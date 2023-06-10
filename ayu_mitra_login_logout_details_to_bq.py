import pdb
import os
import pytz 
import logging
import pymysql
from service.database import InitDatabaseConnetion, make_db_params, fetch_record, DB_Params
import pandas as pd
from datetime import datetime
import boto3 
from google.cloud import bigquery 
#from upload_to_bq import upload_bq,upload_bq_onetime

logger = logging.getLogger(__name__)
    
# Description: AyuMitra Login Details to BQ 
# Created By : Akchansh Kumar 

READ_DB_USER = os.environ.get('READ_DB_USER', '')
READ_DB_PASSWORD = os.environ.get('READ_DB_PASSWORD', '')
READ_DB_DOMAIN_URL = os.environ.get('READ_DB_DOMAIN_URL', '')
SECRETS_FILE_BQ = os.environ.get('SECRETS_FILE_BQ', '')
S3_BUCKET_NAME = os.environ.get('S3_BUCKET_NAME', '')

queries = {
    "main": """Select 
                    mitra.email as ayuMitraEmail,
                    aliasName as 'hospitalName',
                    latLong,
                   case when pcp.cityName = 'Chandigarh' then 'CHD'
                                when pcp.cityName = 'Bangalore' then 'BLR'
                                when pcp.cityName is not null then pcp.cityName
                                else 'NA' end as city,
                    
                    mitra.phone as ayuMitraPhoneNo,
                    case when isAvailable = 1 then 'Available' else 'UnAvailable' end as availability
                from 
                    ayu_personnel_details mitra
                    join ayu_mitra_hospital_mapping mapp on mitra.personnelId = mapp.ayuMitraId
                    join ayu_facility_profile hos on mapp.facilityId = hos.facilityId
                    left join ayu_cities pcp on mitra.cityId = pcp.id
             
                    where
                        mapp.isValid = 1
                        and mapp.tenantName = 'AYU'
                        and personnelType = 'AYU_MITRA' 
                        and mappingType = 'AYUMITRA'
                        and hasLeft = 0
                        
                    """,
    "login_logout": """select mitra.email as ayuMitraEmail,
                              date_add(loginTime, INTERVAL '5:30' HOUR_MINUTE) as 'loginTime',
                             
                             case when pcp.cityName = 'Chandigarh' then 'CHD'
                                when pcp.cityName = 'Bangalore' then 'BLR'
                                when pcp.cityName is not null then pcp.cityName
                                else 'NA' end as city
                        from ayu_mitra_attendance_details att
                            join ayu_personnel_details mitra on (att.ayuMitraId = mitra.personnelId and personnelType = 'AYU_MITRA')
                             left join ayu_cities pcp on mitra.cityId = pcp.id
                    where date(attendanceDate) = curdate() 
                    
                """,
    "location_last": """select ayuMitraEmail,
                                date_add(ald.createdOn, INTERVAL '5:30' HOUR_MINUTE) as 'createdOn',
                                locationDetails->>'$.latitude' as 'latitude',
                                locationDetails->>'$.longitude' as 'longitude',
                                case when pcp.cityName = 'Chandigarh' then 'CHD'
                                when pcp.cityName = 'Bangalore' then 'BLR'
                                when pcp.cityName is not null then pcp.cityName
                                else 'NA' end as city
                                
                        from ayu_mitra_location_details ald
                            join ayu_personnel_details mitra on (ald.ayuMitraEmail = mitra.email and personnelType = 'AYU_MITRA')
                             left join ayu_cities pcp on mitra.cityId = pcp.id
                            where date(date) = curdate() 
                            order by ayuMitraEmail, createdOn desc 
                            """
}



def fetch_data(conn):
    fetched_val = {}
    for lookup, query in queries.items():
        fetched_val[lookup] = fetch_record(conn, query)
    return fetched_val        

def lastActiveTime(ayuMitraEmail,last_active_dict):
    
    if ayuMitraEmail in last_active_dict.keys():
        return last_active_dict[ayuMitraEmail]
        
    return ''

def upload_to_BQ(agent):
    
    boto3.client('s3').download_file(S3_BUCKET_NAME, SECRETS_FILE_BQ, '/tmp/secrets_bq.json')
    client = bigquery.Client.from_service_account_json("/tmp/secrets_bq.json", project='gmb-centralisation')
    table_ref = client.dataset("tables_crons").table("ayu_mitra_login_logout_details")
    
    job_config = bigquery.LoadJobConfig(
    schema=[
        bigquery.SchemaField('ayuMitraEmail','STRING'), 
        bigquery.SchemaField('loginTime','STRING'),
        bigquery.SchemaField('last_ActiveTime','STRING'),
        bigquery.SchemaField('hospitalName','STRING'),
        bigquery.SchemaField('city','STRING')
        ],write_disposition="WRITE_TRUNCATE" 
        )
    job = client.load_table_from_dataframe(
    agent, table_ref, job_config=job_config 
    )  # Make an API request.

def lambda_handler(event, context):
    try:
        aws_session_token = os.environ.get('AWS_SESSION_TOKEN') 
        READ_DB_USER,READ_DB_PASSWORD,READ_DB_DOMAIN_URL=DB_Params(aws_session_token)
        read_host, read_port, read_database = make_db_params(**{'init_from': 'script', 'db_url': READ_DB_DOMAIN_URL})
        read_connection_obj = InitDatabaseConnetion(db_url=read_host, port=int(read_port), username=READ_DB_USER,
                                                    password=READ_DB_PASSWORD, db_name=read_database)

        dataSets = fetch_data(read_connection_obj)
        
        mainData = pd.DataFrame(dataSets['main'])
        login_logout = pd.DataFrame(dataSets['login_logout'])
        last_location = dataSets['location_last']
        
        hospital_dict = {}
        for index,val in mainData.iterrows():
            ayuMitraEmail = val['ayuMitraEmail']
            aliasName = val['hospitalName']
            
            if ayuMitraEmail not in hospital_dict.keys():
                hospital_dict[ayuMitraEmail] = aliasName
        
        login_logout['hospitalName'] = login_logout.apply(lambda x: lastActiveTime(x['ayuMitraEmail'],hospital_dict),axis=1)
        
        last_active_dict = {}
        
        for val in last_location:
            ayuMitraEmail = val['ayuMitraEmail'] 
            createdOn = val['createdOn'].strftime('%b %d, %H:%M:%S')
            if ayuMitraEmail not in last_active_dict.keys():
                
                last_active_dict[ayuMitraEmail] = createdOn
                 
        login_logout['last_ActiveTime'] = login_logout.apply(lambda x: lastActiveTime(x['ayuMitraEmail'],last_active_dict),axis=1) 
        
        login_logout['loginTime'] = login_logout['loginTime'].astype(str)
        login_logout['last_ActiveTime'] = login_logout['last_ActiveTime'].astype(str)  
        

        
        print(login_logout) 
        
        upload_to_BQ(login_logout)  
         
    except Exception as e:
        raise e
