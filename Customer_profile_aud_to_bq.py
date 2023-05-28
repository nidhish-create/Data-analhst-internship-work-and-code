import pdb 
import os
import pytz 
import re
import pandas as pd
from service.database import InitDatabaseConnetion, make_db_params, fetch_record,DB_Params
import logging
#from requests.auth import HTTPBasicAuth
from google.cloud import bigquery
from service.base_functions import msg_to
import numpy as np
import boto3
import requests
import json
from datetime import datetime, timedelta
from service.send_mail_client import send_email

logger = logging.getLogger(__name__)

# READ_DB_USER = os.environ.get('READ_DB_USER', '')
# READ_DB_PASSWORD = os.environ.get('READ_DB_PASSWORD', '')
# READ_DB_DOMAIN_URL = os.environ.get('READ_DB_DOMAIN_URL', '')
SECRETS_FILE_BQ = os.environ.get('SECRETS_FILE_BQ', '')
S3_BUCKET_NAME = os.environ.get('S3_BUCKET_NAME', '') 

currentimestamp = datetime.now() + timedelta(hours = 5 , minutes = 30 , days= -2)
currentimestamp =str(currentimestamp.date()) + " 18:30:00"
stimestamp = datetime.now() + timedelta(hours = 5 , minutes = 30 , days =-1 )
stimestamp =str(stimestamp.date()) + " 18:30:00"

queries = {"cusotmer_profile_aud": '''select * from customer_profile_AUD
where createdOn >= "{date}"  and  createdOn < "{time}"  
''' 

 
}

def fetch_data(conn):
    fetched_val = {}
    for lookup, query in queries.items():
        print(query.format(date=currentimestamp,time=stimestamp))
        fetched_val[lookup] = fetch_record(conn, query.format(date=currentimestamp,time=stimestamp))    
          
    return fetched_val 
    
def upload_to_BQ(agent):
    
    boto3.client('s3').download_file(S3_BUCKET_NAME, SECRETS_FILE_BQ, '/tmp/secrets_bq.json')
    client = bigquery.Client.from_service_account_json("/tmp/secrets_bq.json", project='gmb-centralisation')
    table_ref = client.dataset("tables_crons").table("customer_profile_AUD")
    
    job_config = bigquery.LoadJobConfig(
    schema=[
        bigquery.SchemaField('REVTYPE','STRING'), 
        bigquery.SchemaField('REV','STRING'),
        bigquery.SchemaField('customerId','INTEGER'),
        bigquery.SchemaField('customerName','STRING'),
        bigquery.SchemaField('customerEmail','STRING'),
        bigquery.SchemaField('gender','STRING'),
        bigquery.SchemaField('dateOfBirth','STRING'),
        bigquery.SchemaField('customerNumber','STRING'),
        bigquery.SchemaField('secondaryNumber','STRING'),
        bigquery.SchemaField('isSecondaryNumberWhatsappEnabled','STRING'),
        bigquery.SchemaField('referralCode','STRING'),
        bigquery.SchemaField('appToken','STRING'),
        bigquery.SchemaField('tenantName','STRING'),
        bigquery.SchemaField('prefLanguage','STRING'),
        bigquery.SchemaField('leadSource','STRING'),
        bigquery.SchemaField('customerLocation','STRING'),
        bigquery.SchemaField('additionalDetails','STRING'),
        bigquery.SchemaField('createdOn','DATETIME'),
        bigquery.SchemaField('updatedOn','DATETIME'),
        bigquery.SchemaField('customerType','STRING'), 
        
        
        ],#write_disposition="WRITE_TRUNCATE"  
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
        
        customer_profile_aud = pd.DataFrame(dataSets['cusotmer_profile_aud'])
        
        customer_profile_aud[['REVTYPE','REV','customerName','customerEmail','gender','dateOfBirth','customerNumber','secondaryNumber','isSecondaryNumberWhatsappEnabled','referralCode','appToken','tenantName','prefLanguage','leadSource','customerLocation','additionalDetails','customerType']] = customer_profile_aud[['REVTYPE','REV','customerName','customerEmail','gender','dateOfBirth','customerNumber','secondaryNumber','isSecondaryNumberWhatsappEnabled','referralCode','appToken','tenantName','prefLanguage','leadSource','customerLocation','additionalDetails','customerType']].astype(str)
        
        
        upload_to_BQ(customer_profile_aud)
    
    except Exception as e:
        Subject = 'customer_profile_aud_to_bq ERROR' 
        email_recipient_list = ['Analytics@ayu.health']
        send_email(None, email_recipient_list, Subject, 'None',e,[])
        raise e
        


