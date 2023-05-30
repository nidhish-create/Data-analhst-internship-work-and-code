import pdb 
import os
import pytz 
import re
import pandas as pd
from service.database import InitDatabaseConnetion, make_db_params, fetch_record,DB_Params
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



queries = {"Insurance_lender_to_bq": '''select * from insurance_lender
'''

}

def fetch_data(conn):
    fetched_val = {}
    for lookup, query in queries.items():
        fetched_val[lookup] = fetch_record(conn, query)    
          
    return fetched_val 
    
def upload_to_BQ(agent):
    
    boto3.client('s3').download_file(S3_BUCKET_NAME, SECRETS_FILE_BQ, '/tmp/secrets_bq.json')
    client = bigquery.Client.from_service_account_json("/tmp/secrets_bq.json", project='gmb-centralisation')
    table_ref = client.dataset("tables_crons").table("Insurance_lender")
    
    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField('lenderId', 'INTEGER'),
            bigquery.SchemaField('lenderName', 'STRING'),
            bigquery.SchemaField('additionalDetails', 'STRING'),
            bigquery.SchemaField('createdOn', 'DATETIME'),
            bigquery.SchemaField('updatedOn', 'DATETIME'),

        ], write_disposition="WRITE_TRUNCATE"
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

        Insurance_lender_to_bq = pd.DataFrame(dataSets['Insurance_lender_to_bq'])

        upload_to_BQ(Insurance_lender_to_bq) 
        print(Insurance_lender_to_bq)                                   
     
    except Exception as e:
        Subject = 'Insurance_lender_to_bq ERROR' 
        email_recipient_list = ['Analytics@ayu.health']
        send_email(None, email_recipient_list, Subject, 'None',e,[])
        raise e
        






