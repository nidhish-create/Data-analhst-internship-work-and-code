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
import re
import csv

logger = logging.getLogger(__name__)

READ_DB_USER = os.environ.get('READ_DB_USER', '')
READ_DB_PASSWORD = os.environ.get('READ_DB_PASSWORD', '')
READ_DB_DOMAIN_URL = os.environ.get('READ_DB_DOMAIN_URL', '') 
SPREAD_SHEET_JSON = os.environ.get('SPREAD_SHEET_JSON', '')
SECRETS_FILE_BQ = os.environ.get('SECRETS_FILE_BQ', '')
S3_BUCKET_NAME = os.environ.get('S3_BUCKET_NAME', '') 

quries = {'big_query':"""select ac.id,lower(email) as 'assignedTo',
                              ac.activity->>'$.newAvailabilityValue' as 'newValue',
                              ac.activity->>'$.perviousAvailabilityValue' as 'previousValue',
                              ac.activity->>'$.reason' as 'reason',
                              date_add(ac.createdOn,interval '5:30' HOUR_MINUTE) as 'createdOn', cs.cityId 
                               
                    from 
                        ayu_personnel_activity ac
                        join ayu_personnel_details cs on ac.ayuPersonnelId = cs.personnelId
                        where date(date_add(ac.createdOn,interval '5:30' HOUR_MINUTE)) <= curdate() - interval 1 day
                        and personnelType = 'CUSTOMER_SUPPORT'
                        order by ac.createdOn 
    """ 
} 

def totalActive(newValue,reason,newValue_lead,createdOn_lead,createdOn):
    if newValue == 'true' or reason == 'Going for a meeting':
        if pd.isna(newValue_lead):
            diff = (datetime.now() + timedelta(hours=5, minutes=30, days=-1)).replace(hour=23, minute=59, second=59) - createdOn
            #time_diff = diff.seconds
            delta = (diff.days * 24 * 3600) + diff.seconds
            
        else:
            diff = createdOn_lead- createdOn
            delta = (diff.days * 24 * 3600) + diff.seconds
        return delta
def fetch_data(conn):
    fetched_val = {}
    for lookup, query in quries.items():
        fetched_val[lookup] = fetch_record(conn, query)
    return fetched_val

def upload_to_bq(bigdata):
    boto3.client('s3').download_file(S3_BUCKET_NAME, SECRETS_FILE_BQ, '/tmp/secrets_bq.json')
    client = bigquery.Client.from_service_account_json("/tmp/secrets_bq.json", project='gmb-centralisation')
    table_ref = client.dataset("tables_crons").table("agent_active")
     
    job_config = bigquery.LoadJobConfig(
    schema=[
        bigquery.SchemaField('id','STRING'),
        bigquery.SchemaField('assignedTo','STRING'),
        bigquery.SchemaField('newValue','STRING'), 
        bigquery.SchemaField('previousValue','STRING'),
        bigquery.SchemaField('reason','STRING'),
        bigquery.SchemaField('createdOn','STRING'),
        bigquery.SchemaField('createdOn_lead','STRING'),
        bigquery.SchemaField('reason_lead','STRING'),
        bigquery.SchemaField('newValue_lead','STRING'),
        bigquery.SchemaField('total_active_sec','STRING'),
        bigquery.SchemaField('cityId','STRING') 
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
        read_connection_obj = InitDatabaseConnetion(db_url=read_host, port=int(read_port), username=READ_DB_USER,password=READ_DB_PASSWORD, db_name=read_database)
              
        dataSets = fetch_data(read_connection_obj) 
        agent_active = pd.DataFrame(dataSets['big_query'])
        
        if len(agent_active):
            agent_active['createdOn_lead'] = agent_active.groupby(['assignedTo'])['createdOn'].shift(-1)
            agent_active['reason_lead'] = agent_active.groupby(['assignedTo'])['reason'].shift(-1)
            agent_active['newValue_lead'] = agent_active.groupby(['assignedTo'])['newValue'].shift(-1)
        
        agent_active['total_active_sec'] = agent_active.apply(lambda x: totalActive(x['newValue'],x['reason'],x['newValue_lead'],x['createdOn_lead'],x['createdOn']),axis=1)
        
        cols = ['id','createdOn','createdOn_lead','total_active_sec','cityId']  
        agent_active[cols] = agent_active[cols].astype(str)
        
        upload_to_bq(agent_active)  
        
        # print(agent_active)
        # fpath = os.path.join('/tmp','ayu_personnel_activity.csv.gz')
        # agent_active.to_csv(fpath, compression = 'gzip')
        # # data.to_csv(fpath)
         
        # Subject = 'cc'
        # email_recipient_list = ['nikunj.r@ayu.health']
        # send_email(None, email_recipient_list, Subject, 'None',None,[fpath])
        
    except Exception as e:
        Subject = 'agent_active_to_bq error'
        email_recipient_list = ['analytics@ayu.health']
        send_email(None, email_recipient_list, Subject, 'None',e,[])
        raise e
        
        
