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

READ_DB_USER = os.environ.get('READ_DB_USER', '') 
READ_DB_PASSWORD = os.environ.get('READ_DB_PASSWORD', '')
READ_DB_DOMAIN_URL = os.environ.get('READ_DB_DOMAIN_URL', '')
SECRETS_FILE_BQ = os.environ.get('SECRETS_FILE_BQ', '')
S3_BUCKET_NAME = os.environ.get('S3_BUCKET_NAME', '')

queries = {
"data" : """ select caseId,ad.createdOn,user,ad.tenantName,treatmentType,admissionDate,dischargeDate,doctorId,hospitalId,ad.ayuMitraId,surgeryPackageType,
surgeryStatus,followUpDate,id as surgeryId,treatmentName,promisedPrice,discount,ayuPrice,apd.email,
leadUniquenessStatus,ad.additionalDetails,apd.personnelType, apd.isManager
 from patient_surgery_details_AUD ad
 left join ayu_personnel_details apd on apd.personnelId = ad.ayuMitraId

                        """
                        }

def upload_to_bq(df_list): 
    
    boto3.client('s3').download_file(S3_BUCKET_NAME, SECRETS_FILE_BQ, '/tmp/secrets_bq.json')
    client = bigquery.Client.from_service_account_json("/tmp/secrets_bq.json", project='gmb-centralisation')
    table_ref = client.dataset("tables_crons").table("patient_surgery_details_aud")
    job_config = bigquery.LoadJobConfig( 
    schema=[
        bigquery.SchemaField('caseId','STRING'),
        bigquery.SchemaField('createdOn','STRING'),
        bigquery.SchemaField('user','STRING'),
        bigquery.SchemaField('tenantName','STRING'), 
        bigquery.SchemaField('treatmentType','STRING'),
        bigquery.SchemaField('admissionDate','STRING'),
        bigquery.SchemaField('dischargeDate','STRING'),
        bigquery.SchemaField('doctorId','STRING'),
        bigquery.SchemaField('hospitalId','STRING'),
        bigquery.SchemaField('ayuMitraId','STRING'),
        bigquery.SchemaField('surgeryPackageType','STRING'),
        bigquery.SchemaField('surgeryStatus','STRING'),
        bigquery.SchemaField('followUpDate','STRING'),
        bigquery.SchemaField('surgeryId','STRING'),
        bigquery.SchemaField('treatmentName','STRING'),
        bigquery.SchemaField('promisedPrice','STRING'),
        bigquery.SchemaField('discount','STRING'),
        bigquery.SchemaField('ayuPrice','STRING'),
        bigquery.SchemaField('email','STRING'),
        bigquery.SchemaField('leadUniquenessStatus','STRING'),
        bigquery.SchemaField('additionalDetails','STRING'),
        bigquery.SchemaField('personnelType','STRING'),
        bigquery.SchemaField('isManager','STRING'),
        
        ],
    write_disposition="WRITE_TRUNCATE", 
         )
         
    job = client.load_table_from_dataframe(
    df_list, table_ref, job_config=job_config
    )  # Make an API request.
    job.result()  

def fetch_data(conn):
    fetched_val = {}
    for lookup, query in queries.items():
        fetched_val[lookup] = fetch_record(conn, query)  
    
    return fetched_val  

def lambda_handler(event, context):
    try:
        
        aws_session_token = os.environ.get('AWS_SESSION_TOKEN') 
        READ_DB_USER,READ_DB_PASSWORD,READ_DB_DOMAIN_URL=DB_Params(aws_session_token)
        
        read_host, read_port, read_database = make_db_params(**{'init_from': 'script', 'db_url': READ_DB_DOMAIN_URL})
        read_connection_obj = InitDatabaseConnetion(db_url=read_host, port=int(read_port), username=READ_DB_USER,
                                                    password=READ_DB_PASSWORD, db_name=read_database)
                                                    
        dataSets = fetch_data(read_connection_obj)
        # data = dataSets['data']
         
        data = pd.DataFrame(dataSets['data']) 
        cols = ['caseId','createdOn','admissionDate','dischargeDate','doctorId','hospitalId','ayuMitraId',
                    'followUpDate','surgeryId','promisedPrice','discount','ayuPrice','isManager'] 
        data[cols] = data[cols].astype(str)
        upload_to_bq(data)  
        
        
        # fpath = os.path.join('/tmp','referral_agent.csv')
        # data.to_csv(fpath)
        
        # Subject = 'referral_agent'
        # email_recipient_list = ['nikunj.r@ayu.health']
        # send_email(None, email_recipient_list, Subject, 'None',None,[fpath]) 
        
    except Exception as e:
        Subject = 'patient_surgery_details_AUD'
        email_recipient_list = ['analytics@ayu.health']
        send_email(None, email_recipient_list, Subject, 'None',e,[])
        raise e 
    
