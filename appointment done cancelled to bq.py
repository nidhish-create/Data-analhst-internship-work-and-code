import pdb 
import os
import pytz 
import re 
import pandas as pd 
from service.database import InitDatabaseConnetion, make_db_params, fetch_record, DB_Params
import logging
from service.base_functions import msg_to
from datetime import datetime , timedelta
from service.send_mail_client import send_email
# from s3_utils.utils import upload_to_s3
import numpy as np
from google.cloud import bigquery
import numpy as np 
import boto3
import json



logger = logging.getLogger(__name__)

# READ_DB_USER = os.environ.get('READ_DB_USER', '')
# READ_DB_PASSWORD = os.environ.get('READ_DB_PASSWORD', '')
# READ_DB_DOMAIN_URL = os.environ.get('READ_DB_DOMAIN_URL', '')
SECRETS_FILE_BQ = os.environ.get('SECRETS_FILE_BQ', '')
S3_BUCKET_NAME = os.environ.get('S3_BUCKET_NAME', '')





queries = {
    "appointment_done": '''
                        select * from lead_comments
                             where
                             leadId in (SELECT id from lead_doctor_consultation
                             where doctorConsultationStatus in (3,4))
                             and
                             leadType='APPOINTMENT' 
                             and commentType in ('APPOINTMENT_DONE','APPOINTMENT_CANCELLED','APPOINTMENT_DIRECT_DONE')
                             
                            '''
                
                            
    
    
}





big_query = {
    """SELECT distinct date(date_add(cast(createdOn as datetime), interval 330 minute)) as 
    createdOn,leadId FROM `gmb-centralisation.tables_crons.lead_comments_user`"""
}


today = datetime.now() + timedelta(hours=5,minutes=30) - timedelta(days = 1)
start = today.date()
print(start)


def upload_to_bq(df_list):
    
    boto3.client('s3').download_file(S3_BUCKET_NAME, SECRETS_FILE_BQ, '/tmp/secrets_bq.json')
    client = bigquery.Client.from_service_account_json("/tmp/secrets_bq.json", project='gmb-centralisation')
    table_ref = client.dataset("tables_crons").table("lead_comments_user")
    job_config = bigquery.LoadJobConfig(
    schema=[
        bigquery.SchemaField('commentId','STRING'),
        bigquery.SchemaField('leadId','STRING'),
        bigquery.SchemaField('text','STRING'),
        bigquery.SchemaField('leadType','STRING'),
        bigquery.SchemaField('user','STRING'),
        bigquery.SchemaField('createdOn','STRING'),
        bigquery.SchemaField('commentType','STRING'),
        
        ],
    write_disposition="WRITE_TRUNCATE",
         )
         
    job = client.load_table_from_dataframe(
    df_list, table_ref, job_config=job_config
    )  # Make an API request.
    job.result() 
    
    
def getDataFromBigQ(query):
    
    data2 = main(query)
    return data2
    

def fetch_data(conn):
    fetched_val = {}
    for lookup, query in queries.items():
        fetched_val[lookup] = fetch_record(conn, query.format(start=start))  
    
    return fetched_val 

def lambda_handler(event, context):
    try:
        
        aws_session_token = os.environ.get('AWS_SESSION_TOKEN') 
        READ_DB_USER,READ_DB_PASSWORD,READ_DB_DOMAIN_URL = DB_Params(aws_session_token)
        
        read_host, read_port, read_database = make_db_params(**{'init_from': 'script', 'db_url': READ_DB_DOMAIN_URL})
        read_connection_obj = InitDatabaseConnetion(db_url=read_host, port=int(read_port), username=READ_DB_USER,
                                                    password=READ_DB_PASSWORD, db_name=read_database)
          
          
        
        dataSets = fetch_data(read_connection_obj)
        # data = dataSets['data']
        
        data = pd.DataFrame(dataSets['appointment_done'])
        print(data)
        cols = ['leadId','commentId','text','leadType','user','createdOn','commentType']
        data[cols] = data[cols].astype(str)
        upload_to_bq(data) 
        
        
    except Exception as e:
        Subject = 'appointment_done_cancelled_to_bq | error'
        email_recipient_list = ['analytics@ayu.health']
        send_email(None, email_recipient_list, Subject, e,None,[]) 
        raise e 
    
    
    
