import pdb 
import os
import pytz 
import re
import pandas as pd
from service.database import InitDatabaseConnetion, make_db_params, fetch_record, DB_Params
import logging
from google.cloud import bigquery
from service.base_functions import msg_to
import numpy as np 
import boto3
import requests
import json
from datetime import datetime, timedelta             
from service.send_mail_client import send_email

logger = logging.getLogger(__name__)

READ_DB_USER = os.environ.get('READ_DB_USER', '')
READ_DB_PASSWORD = os.environ.get('READ_DB_PASSWORD', '')
READ_DB_DOMAIN_URL = os.environ.get('READ_DB_DOMAIN_URL', '')
SECRETS_FILE_BQ = os.environ.get('SECRETS_FILE_BQ', '')
S3_BUCKET_NAME = os.environ.get('S3_BUCKET_NAME', '') 



queries = {"main": '''select * from  ayu_personnel_details
''',
           "agent_hospital": ''' select a.ayuMitraId , b.aliasName , a.facilityType , a.mappingType , a.availabilityType from ayu_mitra_hospital_mapping a 
           left join ayu_facility_profile b on b.facilityId = a.facilityId 
           
           '''
    ,

           "permissions": ''' select * from ayu_personnel_permissions'''
 }
 
citymapping = {
    '1':'CHD',
    '2':'BLR',
    '3':'Jaipur',
    '4':'NCR',
    '5':'Hyderabad'

}


def fetch_data(conn):
    fetched_val = {}
    for lookup, query in queries.items():
        fetched_val[lookup] = fetch_record(conn, query)

    return fetched_val
def func1(ayuMitraId , dict1):
    if ayuMitraId in dict1.keys():
        return dict1[ayuMitraId]['aliasName'],dict1[ayuMitraId]['facilityType'],dict1[ayuMitraId]['mappingType'],dict1[ayuMitraId]['availabilityType']
    return '' , '', '' , ''
def func2(personnnelId , dict2):
    if personnnelId in dict2.keys():
        return dict2[personnnelId]
    return ''




def upload_to_BQ(agent):
    boto3.client('s3').download_file(S3_BUCKET_NAME, SECRETS_FILE_BQ, '/tmp/secrets_bq.json')
    client = bigquery.Client.from_service_account_json("/tmp/secrets_bq.json", project='gmb-centralisation')
    table_ref = client.dataset("tables_crons").table("agent_hospital_details") 

    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField('personnelId', 'STRING'),
            bigquery.SchemaField('personnelType', 'STRING'),
            bigquery.SchemaField('email', 'STRING'),
            bigquery.SchemaField('name', 'STRING'),
            bigquery.SchemaField('phone', 'STRING'),
            bigquery.SchemaField('isAvailable', 'STRING'),
            bigquery.SchemaField('hasLeft', 'STRING'),
            bigquery.SchemaField('createdOn', 'STRING'),
            bigquery.SchemaField('updatedOn', 'STRING'),
            bigquery.SchemaField('isTestAccount', 'STRING'),
            bigquery.SchemaField('additionalDetails', 'STRING'),
            bigquery.SchemaField('city', 'STRING'),
            bigquery.SchemaField('aliasName', 'STRING'),
            bigquery.SchemaField('facilityType', 'STRING'),
            bigquery.SchemaField('mappingType', 'STRING'),
            bigquery.SchemaField('availabilityType', 'STRING'),
            bigquery.SchemaField('permissionType', 'STRING'),

        ], write_disposition="WRITE_TRUNCATE"
    )
    job = client.load_table_from_dataframe(
        agent, table_ref, job_config=job_config
    )


def lambda_handler(event,context):
    try:
        aws_session_token = os.environ.get('AWS_SESSION_TOKEN')
        READ_DB_USER,READ_DB_PASSWORD,READ_DB_DOMAIN_URL=DB_Params(aws_session_token)
        read_host, read_port, read_database = make_db_params(**{'init_from': 'script', 'db_url': READ_DB_DOMAIN_URL})
        read_connection_obj = InitDatabaseConnetion(db_url=read_host, port=int(read_port), username=READ_DB_USER,
                                                    password=READ_DB_PASSWORD, db_name=read_database)



        dataSets = fetch_data(read_connection_obj)


        main = pd.DataFrame(dataSets['main'])
        agent_hospital = pd.DataFrame(dataSets['agent_hospital'])
        permissions = pd.DataFrame(dataSets['permissions'])

        main['city'] = main.apply(lambda x: citymapping[str(x['cityId'])],axis = 1)
        print(main['city'])



        dict1 = {}
        for i , j in agent_hospital.iterrows():
            if j['ayuMitraId'] not in dict1.keys():
                dict1[j['ayuMitraId']] = {}
                dict1[j['ayuMitraId']]['aliasName'] = j['aliasName']
                dict1[j['ayuMitraId']]['facilityType'] = j['facilityType']
                dict1[j['ayuMitraId']]['mappingType'] = j['mappingType']
                dict1[j['ayuMitraId']]['availabilityType'] = j['availabilityType']
        dict2 = {}
        for i , j in permissions.iterrows():
            if j['personnelId'] not in dict2.keys():
                dict2[j['personnelId']] = j['permissionType']

        df = main.apply(lambda x: func1(x['personnelId'],dict1),axis=1 , result_type= 'expand')
        main = pd.concat([main , df], axis=1)
        main = main.rename(columns={0:'aliasName',1:'facilityType',2:'mappingType',3:'availabilityType'})

        main['permissionType'] = main.apply(lambda x: func2(x['personnelId'], dict2), axis = 1)
        main = main.drop(columns = ["messageToken","cityId"])
        main = main.astype(str)
        print(main.info())
        # upload_to_BQ(main) 




    except Exception as e: 
        Subject = 'agent_hospital_details_to_bq ERROR' 
        email_recipient_list = ['Analytics@ayu.health']
        # send_email(None, email_recipient_list, Subject, 'None', e, [])   
        raise e

