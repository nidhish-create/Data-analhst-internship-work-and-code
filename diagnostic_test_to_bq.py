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


queries = {"diagnostic_test": """select leadId,
                    ldc.id as appId,
                    appointmentDate,
                    appointmentCreationType,
                    consultationType,
                    ldc.additionalDetails->>'$.isDiagnosticsTestRecommended' as isDiagnosticsTestRecommended, 
                    testNamesRecommended
                from 
                    lead_doctor_consultation ldc
                    left join (select appointmentId, group_concat(name) as testNamesRecommended 
                        from 
                    appointment_diagnostics_outcome ad
                    left join diagnostics_details dd on ad.diagnosticsId = dd.id
                    group by appointmentId) ado on ldc.id = ado.appointmentId
                    where
                        consultationType != 'DIAGNOSTICS'
                        and doctorConsultationStatus = 3
                        and ldc.tenantName = 'AYU'"""
}

def fetch_data(conn):
    fetched_val = {}
    for lookup, query in queries.items():
        fetched_val[lookup] = fetch_record(conn, query)    
          
    return fetched_val 
    
def upload_to_BQ(agent):
    
    boto3.client('s3').download_file(S3_BUCKET_NAME, SECRETS_FILE_BQ, '/tmp/secrets_bq.json')
    client = bigquery.Client.from_service_account_json("/tmp/secrets_bq.json", project='gmb-centralisation')
    table_ref = client.dataset("tables_crons").table("diagnostic_test")
    
    job_config = bigquery.LoadJobConfig(
    schema=[
        bigquery.SchemaField('leadId','STRING'), 
        bigquery.SchemaField('appId','STRING'),
        bigquery.SchemaField('appointmentDate','STRING'),
        bigquery.SchemaField('appointmentCreationType','STRING'),
        bigquery.SchemaField('consultationType','STRING'),
        bigquery.SchemaField('isDiagnosticsTestRecommended','STRING'),
        bigquery.SchemaField('testNamesRecommended','STRING'),
      
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
        
        diagnostic_test = pd.DataFrame(dataSets['diagnostic_test'])
        diagnostic_test[['leadId', 'appId','appointmentDate','appointmentCreationType','consultationType','isDiagnosticsTestRecommended','testNamesRecommended']]= diagnostic_test[['leadId', 'appId','appointmentDate','appointmentCreationType','consultationType','isDiagnosticsTestRecommended','testNamesRecommended']].astype(str)
        
        
        
        
       
        upload_to_BQ(diagnostic_test)
        
    
    except Exception as e:
        Subject = 'agents_data_to_BQ ERROR'
        email_recipient_list = ['nidhish@ayu.health']
        #email_recipient_list = ['anuja.biyani@ayu.health']
        send_email(None, email_recipient_list, Subject, 'None',e,[])
        raise e

