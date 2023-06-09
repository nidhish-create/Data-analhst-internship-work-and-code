import pdb
import os
import logging
from service.database import InitDatabaseConnetion, make_db_params, fetch_record, DB_Params
import pandas as pd
from datetime import datetime, timedelta
from service.base_functions import msg_to
from service.send_mail_client import send_email
import time
from datetime import datetime, timedelta            
from google.cloud import bigquery              
import boto3

logger = logging.getLogger(__name__)  

READ_DB_USER = os.environ.get('READ_DB_USER', '')
READ_DB_PASSWORD = os.environ.get('READ_DB_PASSWORD', '')
READ_DB_DOMAIN_URL = os.environ.get('READ_DB_DOMAIN_URL', '')
SECRETS_FILE_BQ = os.environ.get('SECRETS_FILE_BQ', '')
S3_BUCKET_NAME = os.environ.get('S3_BUCKET_NAME', '')     


queries = {'main':"""Select 
                    mitra.email as ayuMitraEmail,
                    
                    case when pcp.cityName = 'Chandigarh' then 'CHD'
                                when pcp.cityName = 'Bangalore' then 'BLR'
                                when pcp.cityName is not null then pcp.cityName
                                else 'NA' end as city,
                    aliasName as 'hospital'
                    
                    
                    from 
                    ayu_personnel_details mitra
                    join ayu_mitra_hospital_mapping mapp on mitra.personnelId = mapp.ayuMitraId
                    join ayu_facility_profile hos on mapp.facilityId = hos.facilityId
                    left join ayu_cities pcp on mitra.cityId = pcp.id
                    where
                        isAvailable = 1
                        and mapp.isValid = 1
                        and mapp.tenantName = 'AYU'
                        and personnelType = 'AYU_MITRA'
                        and mappingType = 'AYUMITRA'
                        and hasLeft = 0
                    """
}

def fetch_data(conn):
    fetched_val = {}
    for lookup, query in queries.items():
        fetched_val[lookup] = fetch_record(conn, query)    
          
    return fetched_val 

def upload_to_bq(bigdata):
    
    "{'ayuMitraEmail': 'diksha.k@ayu.health', 'city': 'NCR', 'hospitals': 'PSRI HOSPITAL'}"

    boto3.client('s3').download_file(S3_BUCKET_NAME, SECRETS_FILE_BQ, '/tmp/secrets_bq.json')
    client = bigquery.Client.from_service_account_json("/tmp/secrets_bq.json", project='gmb-centralisation')
    table_ref = client.dataset("tables_crons").table("ayuMitra_hospital_mapping")
     
    job_config = bigquery.LoadJobConfig(
    schema=[
        bigquery.SchemaField('ayuMitraEmail','STRING'),
        bigquery.SchemaField('city','STRING'),
        bigquery.SchemaField('hospital','STRING')
        ],
    write_disposition="WRITE_TRUNCATE",
        )  
     
    job = client.load_table_from_dataframe(
    bigdata, table_ref, job_config=job_config    
    )  # Make an API request.
    job.result() 


def lambda_handler(event, context):
    try:
        aws_session_token = os.environ.get('AWS_SESSION_TOKEN')
        READ_DB_USER,READ_DB_PASSWORD,READ_DB_DOMAIN_URL=DB_Params(aws_session_token)
        read_host, read_port, read_database = make_db_params(**{'init_from': 'script', 'db_url': READ_DB_DOMAIN_URL})
        read_connection_obj = InitDatabaseConnetion(db_url=read_host, port=int(read_port), username=READ_DB_USER,
                                                    password=READ_DB_PASSWORD, db_name=read_database)

        dataSets = fetch_data(read_connection_obj)
        main = pd.DataFrame(dataSets['main']) 
        
        # main = dataSets['main'] 
        
        print(main)  
        
        upload_to_bq(main)
        
    except Exception as e:
        raise e
