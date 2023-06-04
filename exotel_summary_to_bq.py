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

READ_DB_USER = os.environ.get('READ_DB_USER', '')
READ_DB_PASSWORD = os.environ.get('READ_DB_PASSWORD', '')
READ_DB_DOMAIN_URL = os.environ.get('READ_DB_DOMAIN_URL', '')
SECRETS_FILE_BQ = os.environ.get('SECRETS_FILE_BQ', '')
S3_BUCKET_NAME = os.environ.get('S3_BUCKET_NAME', '')

today = datetime.now() 
today = today.strftime('%Y-%m-%d')

 
queries = {"exotel": """select 
                       er.*,
                       date(call_created_on) as maskDate, hour(call_created_on) as Hour , MONTH(call_created_on) as Month, YEAR(call_created_on) as Year,
                       case when hour(call_created_on) >= 8 and hour(call_created_on)  <= 20 then 'OFFICE_HOUR'
                        else 'OFF_HOUR' end as 'tagHour'
                       from 
                       exotel_response er
                       where
                        direction = 'inbound'
                        and
                         (
                       time(call_created_on) between DATE_Add(current_time(),interval 320 minute) and 
                       DATE_Add(current_time(),interval 330 minute) 
							);
                            
    """,
    "exotelInsights": """select 
                       callSid,
                       insightData,
                       date(date_add(createdOn, INTERVAL '5:30' HOUR_MINUTE)) as 'createdOn'
                    from 
                       exotel_insights
                    where
                        insightType = 'INBOUND_PATIENT_CALL_LIST'
                        and callSid in ({callsids}) 
            """,
    "exotelCallInsights": """select 
                       callSid,
                       insightData
                    from 
                       exotel_insights
                    where
                        insightType = 'INCOMING_EXOTEL_CALL_DETAILS'
                        and callSid in ({callsids})
            """,
   #"funnelCalls": """select
   #                    entityId as caseId,
   #                    date_add(createdOn,INTERVAL '5:30' HOUR_MINUTE) as 'createdOn',
   #                    additionalDetails,
   #                    additionalDetails->>'$.questionSetType' as 'messageBox',
   #                    additionalDetails->>'$.apiResponse' as 'apiResponse', 
   #                    digitPressed
   #                from exotel_outbound_communication
   #                where 
   #                    communicationType = 'CALL'  
   #                    and entityType = 'PATIENT_CASE'
   #                    and date(date_add(createdOn,INTERVAL '5:30' HOUR_MINUTE)) >= '2022-05-01' 
   #                    and additionalDetails->>'$.questionSetType' in ('CX_ATTEMPT_IVR_CALL_FLOW', 'CX_MULTI_LANGUAGE_IVR_CALL_FLOW', 'CX_MULTI_LEVEL_LANGUAGE_IVR_CALL_FLOW')
   #                order by createdOn
   #                """,
    "mapping": """select calls.entityId, que.question , calls.optionValue, calls.ivrQuestionId
                    from
                    call_question_response calls
                    left join ivr_call_questions que on (calls.ivrQuestionId = que.ivrQuestionId)
                    where 
                    templateId = 3
                    and entityType = 'PATIENT_CASE'
                    order by calls.createdOn
            """,
    "cases": """Select 
                    distinct customerNumber,
                    case when pcp.cityName = 'Chandigarh' then 'CHD'
                         when pcp.cityName = 'Bangalore' then 'BLR'
                         when pcp.cityName is not null then pcp.cityName
                         when ppp.cityName = 'Chandigarh' then 'CHD'
                         when ppp.cityName = 'Bangalore' then 'BLR'
                         when ppp.cityName is not null then ppp.cityName
                         else 'No City' end as city
                from 
                    patient_case pc 
                    join patient_profile pp on pc.patientId = pp.id
                    join customer_profile cp on pp.customerId=cp.customerId 
                    left join ayu_cities pcp on pc.cityId = pcp.id
                    left join ayu_cities ppp on pp.cityId = ppp.id
                    """,
    "ayuMitra": """select email as ayuMitraEmail , phone ,
                    case when pcp.cityName = 'Chandigarh' then 'CHD'
                         when pcp.cityName = 'Bangalore' then 'BLR'
                         when pcp.cityName is not null then pcp.cityName 
                         else 'No City' end as city
                    from ayu_personnel_details apd
                    left join ayu_cities pcp on apd.cityId = pcp.id
                    where
                        personnelType='AYU_MITRA' 
                        and phone is not null and phone != ''
                        """,
    "agents": '''select email , phoneNumber,
                    case when pcp.cityName = 'Chandigarh' then 'CHD'
                         when pcp.cityName = 'Bangalore' then 'BLR'
                         when pcp.cityName is not null then pcp.cityName 
                         else 'No City' end as city
                from customer_support_details csd
                left join ayu_cities pcp on csd.cityId = pcp.id
                where 
                    phoneNumber is not null and phoneNumber != '' 
    '''

    
}

def fetch_data(conn):
    fetched_val = {} 
    for lookup, query in queries.items():
        print(lookup) 
        if lookup == 'exotel':
            fetched_val[lookup] = fetch_record(conn, query.format(start = today , end = today))     
            callsids = ''
            for x in fetched_val[lookup]:
                callsids += "'"+x['sid']+"',"
        elif lookup in ('exotelInsights','exotelCallInsights'):
            fetched_val[lookup] = fetch_record(conn,query.format(callsids = callsids[:-1]))
        else:
            fetched_val[lookup] = fetch_record(conn, query)
    return fetched_val
    
ivr_phones = ['8045681283', '8047185943', '8047186805', '8047185981', '8046801948']

def IVRCheck(exotelNo):
    
    exotel_no = msg_to(exotelNo)
    
    if exotel_no in ivr_phones:
        return 'IVR'
    else:
        return 'Non-IVR'
    
def getCity(cxNo, casesDump):
    
    if cxNo in casesDump.keys():
        return casesDump[cxNo]
        
    return 'No City'
    
def getDigitPresses(digitPressed, mappingSet, messageBox, caseId):
    
    if messageBox == 'CX_ATTEMPT_IVR_CALL_FLOW':
        pressed = 'WhatsApp Flow'
        if digitPressed in [1]:
            pressed = 'Call Transferred'
    
    elif messageBox == 'CX_MULTI_LEVEL_LANGUAGE_IVR_CALL_FLOW':
        pressed = 'WhatsApp Flow'
        if caseId in mappingSet.keys():
            firstLevel, secondLevel = mappingSet[caseId]['firstLevel'], mappingSet[caseId]['secondLevel']    
            if secondLevel is None:
                if firstLevel is not None:
                    pressed = firstLevel
            else:
                pressed = secondLevel
    else:
        pressed = 'WhatsApp Flow'
        if digitPressed in [1, 2, 3]:
            pressed = 'Call Transferred'
            
    return pressed
    
def getIvrTransferStatus(sid, funnelCallDump):
    
    if sid in funnelCallDump.keys():
        status = funnelCallDump[sid]
            
        return 'IVR', status
            
    return 'Non-IVR', ''
    
def getInsightData(sid, exotelInsightsDump):
    
    if sid in exotelInsightsDump.keys():
        return exotelInsightsDump[sid]['noSent'], exotelInsightsDump[sid]['ayuPersonnelType'] , exotelInsightsDump[sid]['requested'], exotelInsightsDump[sid]['agentInSystem'], exotelInsightsDump[sid]['agentAvailable']
        
    return 'Not Requested', 'NA', 'Not Requested', '', '' 
    
def getInsightCallData(sid,leg_1_status,leg_2_status,exotelCallInsightsDump,noSent):
    
    leg1DetailedStatus = 'NO_CALL_INSIGHT'
    leg2DetailedStatus = ''
    leg3DetailedStatus = ''
    
    leg1AgentNumber = ''
    leg2AgentNumber = ''
    leg3AgentNumber = ''
    
    leg1Status = ''
    leg2Status = ''
    leg3Status = ''
    
    finalDetailedStatus = 'NO_CALL_INSIGHT'
    
    if sid in exotelCallInsightsDump.keys():
        insightData = exotelCallInsightsDump[sid]
        
        
        if 'leg1DetailedStatus' in insightData.keys():
            leg1DetailedStatus = insightData['leg1DetailedStatus'].upper()
            
        if 'leg2DetailedStatus' in insightData.keys():
            leg2DetailedStatus = insightData['leg2DetailedStatus'].upper()
            
        if 'leg3DetailedStatus' in insightData.keys():
            leg3DetailedStatus = insightData['leg3DetailedStatus'].upper()
            
        
        
        if 'leg1AgentNumber' in insightData.keys():
            leg1AgentNumber = msg_to(insightData['leg1AgentNumber'])
            
        if 'leg2AgentNumber' in insightData.keys():
            leg2AgentNumber = msg_to(insightData['leg2AgentNumber'])
            
        if 'leg3AgentNumber' in insightData.keys():
            leg3AgentNumber = msg_to(insightData['leg3AgentNumber'])
            
        
        
        if 'leg1Status' in insightData.keys():
            leg1Status = insightData['leg1Status']
            
        if 'leg2Status' in insightData.keys():
            leg2Status = insightData['leg2Status']
            
        if 'leg3Status' in insightData.keys():
            leg3Status = insightData['leg3Status']
        
        if leg1DetailedStatus == 'CALL_COMPLETED':
            finalDetailedStatus = 'CALL_COMPLETED'
            
        elif leg2DetailedStatus == 'CALL_COMPLETED':
            finalDetailedStatus = 'CALL_COMPLETED'
            
        elif leg3DetailedStatus == 'CALL_COMPLETED':
            finalDetailedStatus = 'CALL_COMPLETED'
        else:
            finalDetailedStatus = leg1DetailedStatus
    
 
    if finalDetailedStatus == 'NO_CALL_INSIGHT':
                
        if  leg_2_status == 'completed':
            finalDetailedStatus = 'CALL_COMPLETED'
        else:
            if leg_2_status is not None and leg_2_status != '':
                finalDetailedStatus = leg_2_status.upper()
            else:    
                if leg_1_status == 'completed':
                    finalDetailedStatus = 'LEG_1_COMPLETED'
                else:
                    if leg_1_status is not None and leg_1_status != '':
                        finalDetailedStatus = leg_1_status.upper()
            
    if finalDetailedStatus == 'CANCELED':
        if noSent == True:
            finalDetailedStatus = 'CUSTOMER_CANCELLED'
        elif noSent == False:
            finalDetailedStatus = 'NO_NUMBER_TO_DIALOUT_CANCELED'
                    
    if finalDetailedStatus == 'BUSY':
        if noSent == True:
            finalDetailedStatus = 'BUSY'
        elif noSent == False:
            finalDetailedStatus = 'NO_NUMBER_TO_DIALOUT_BUSY'
    
        
    return leg1DetailedStatus, leg2DetailedStatus, leg3DetailedStatus, finalDetailedStatus, leg1AgentNumber, leg2AgentNumber, leg3AgentNumber , leg1Status , leg2Status , leg3Status

def getagents_info(leg1AgentNumber,leg2AgentNumber,leg3AgentNumber,agentsdump):
    
    leg1Agent_email = ''
    leg2Agent_email = ''
    leg3Agent_email = ''
    
    leg1Agent_city = ''
    leg2Agent_city = ''
    leg3Agent_city = ''
    
    print(leg1AgentNumber,leg2AgentNumber,leg3AgentNumber)
    
    #print(agentsdump[leg2AgentNumber])
    
    if leg1AgentNumber in agentsdump.keys():
        leg1Agent_email = agentsdump[leg1AgentNumber]['email']
        leg1Agent_city = agentsdump[leg1AgentNumber]['city']
        
    if leg2AgentNumber in agentsdump.keys():
        leg2Agent_email = agentsdump[leg2AgentNumber]['email']
        leg2Agent_city = agentsdump[leg2AgentNumber]['city']
        
    if leg3AgentNumber in agentsdump.keys():
        leg3Agent_email = agentsdump[leg3AgentNumber]['email']
        leg3Agent_city = agentsdump[leg3AgentNumber]['city']
    
    print(leg1Agent_email , leg2Agent_email ,leg3Agent_email)
        
    return leg1Agent_email , leg2Agent_email ,leg3Agent_email , leg1Agent_city ,leg2Agent_city , leg3Agent_city
    
    
def upload_to_bq(exotel_summary_bq):
    
    boto3.client('s3').download_file(S3_BUCKET_NAME, SECRETS_FILE_BQ, '/tmp/secrets_bq.json')
    client = bigquery.Client.from_service_account_json("/tmp/secrets_bq.json", project='gmb-centralisation')
    table_ref = client.dataset("tables_crons").table("exotel_summary_current_time")
    
    job_config = bigquery.LoadJobConfig(
    schema=[
        bigquery.SchemaField('sid','STRING'),
        bigquery.SchemaField('call_created_on','DATETIME'),
        bigquery.SchemaField('status','STRING'),
        bigquery.SchemaField('direction','STRING'),
        bigquery.SchemaField('to_no','STRING'),
        bigquery.SchemaField('from_no','STRING'),
        bigquery.SchemaField('exotel_no','STRING'),
        bigquery.SchemaField('duration','STRING'),
        bigquery.SchemaField('price','STRING'),
        bigquery.SchemaField('leg_1_status','STRING'),
        bigquery.SchemaField('leg_1_duration','STRING'),
        bigquery.SchemaField('leg_2_status','STRING'),
        bigquery.SchemaField('leg_2_duration','STRING'),
        bigquery.SchemaField('recording_url','STRING'),
        bigquery.SchemaField('maskDate','DATE'),
        bigquery.SchemaField('Hour','STRING'),
        bigquery.SchemaField('Month','STRING'),
        bigquery.SchemaField('Year','STRING'),
        bigquery.SchemaField('tagHour','STRING'),
        bigquery.SchemaField('cxNo','STRING'),
        bigquery.SchemaField('city','STRING'),
        bigquery.SchemaField('ivrCheck','STRING'),
        bigquery.SchemaField('ivrTransferStatus','STRING'),
        bigquery.SchemaField('noSent','STRING'),
        bigquery.SchemaField('ayuPersonnelType','STRING'),
        bigquery.SchemaField('leg1DetailedStatus','STRING'),
        bigquery.SchemaField('leg2DetailedStatus','STRING'),
        bigquery.SchemaField('leg3DetailedStatus','STRING'),
        bigquery.SchemaField('finalDetailedStatus','STRING'),
        bigquery.SchemaField('leg1AgentNumber','STRING'),
        bigquery.SchemaField('leg2AgentNumber','STRING'),
        bigquery.SchemaField('leg3AgentNumber','STRING'),
        bigquery.SchemaField('leg1Status','STRING'),
        bigquery.SchemaField('leg2Status','STRING'),
        bigquery.SchemaField('leg3Status','STRING'),
        bigquery.SchemaField('leg1Agent_email','STRING'),
        bigquery.SchemaField('leg2Agent_email','STRING'),
        bigquery.SchemaField('leg3Agent_email','STRING'),
        bigquery.SchemaField('leg1Agent_city','STRING'),
        bigquery.SchemaField('leg2Agent_city','STRING'),
        bigquery.SchemaField('leg3Agent_city','STRING'),
        bigquery.SchemaField('requested','STRING'),
        bigquery.SchemaField('agentInSystem','STRING'),
        bigquery.SchemaField('agentAvailable','STRING'),
        ],#write_disposition="WRITE_TRUNCATE"
        )
    job = client.load_table_from_dataframe(
    exotel_summary_bq, table_ref, job_config=job_config
    )  # Make an API request.
    
#    ['sid', , 'status', 'direction',
#, 'to_no', 'from_no', 'exotel_no', 'duration', 'price',
#'leg_1_status', 'leg_1_duration', 'leg_2_status', 'leg_2_duration',
#'recording_url', 'maskDate', 'tagHour', 'cxNo', 'city', 'ivrCheck',
#'ivrTransferStatus', 'noSent', 'ayuPersonnelType', 'leg1DetailedStatus',
#'leg2DetailedStatus', 'leg3DetailedStatus', 'finalDetailedStatus',
#'leg1AgentNumber', 'leg2AgentNumber', 'leg3AgentNumber']

fpath = os.path.join('/tmp','exotel_summary_bq.csv')

def lambda_handler(event, context):
    try:
        aws_session_token = os.environ.get('AWS_SESSION_TOKEN') 
        READ_DB_USER,READ_DB_PASSWORD,READ_DB_DOMAIN_URL=DB_Params(aws_session_token)
        read_host, read_port, read_database = make_db_params(**{'init_from': 'script', 'db_url': READ_DB_DOMAIN_URL})
        read_connection_obj = InitDatabaseConnetion(db_url=read_host, port=int(read_port), username=READ_DB_USER,
                                                    password=READ_DB_PASSWORD, db_name=read_database)

        dataSets = fetch_data(read_connection_obj)
        
        print('connection setup completed')
        
        exotel = pd.DataFrame(dataSets['exotel'])
        exotelInsights = dataSets['exotelInsights']
        exotelCallInsights = dataSets['exotelCallInsights']
        #funnelCalls = pd.DataFrame(dataSets['funnelCalls'])
        mapping = dataSets['mapping']
        cases = pd.DataFrame(dataSets['cases'])
        
        print('Data Extracted') 
        
        ayuMitra = pd.DataFrame(dataSets['ayuMitra'])
        ayuMitra['agent_no'] = ayuMitra.apply(lambda x: msg_to(x['phone']),axis=1)
        
        
        agents = pd.DataFrame(dataSets['agents'])
        agents['agent_no'] = agents.apply(lambda x: msg_to(x['phoneNumber']),axis=1)
        
        agentsdump = {}
        for index,val in agents.iterrows():
            if val['agent_no'] not in agentsdump.keys():
                agentsdump[val['agent_no']] = {}
                agentsdump[val['agent_no']]['email'] = val['email']
                agentsdump[val['agent_no']]['city'] = val['city']
            
                
        for index,val in ayuMitra.iterrows():
            if val['agent_no'] not in agentsdump.keys():
                agentsdump[val['agent_no']] = {}
                agentsdump[val['agent_no']]['email'] = val['ayuMitraEmail']
                agentsdump[val['agent_no']]['city'] = val['city']
        
        print('agentsdump created')
        
        exotel = exotel.drop(['call_updated_on','created_at'],axis =1)
        
        cases['cxNo'] = cases.apply(lambda x: msg_to(x['customerNumber']), axis=1)
        casesDump = {}
        for index, val in cases.iterrows():
            
            if val['cxNo'] not in casesDump.keys():
                casesDump[val['cxNo']] = val['city']
                
            if val['city'] != 'No City':
                casesDump[val['cxNo']] = val['city']
        
        print('casesDump created')
        
        mappingSet = {}
        for val in mapping:
        
            if val['entityId'] not in mappingSet.keys():
                mappingSet[val['entityId']] = {}
                mappingSet[val['entityId']]['firstLevel'] = None
                mappingSet[val['entityId']]['secondLevel'] = None
            
            if str(val['ivrQuestionId']) == '3':
                if str(val['optionValue']) in ['1', '2', '3']:
                    mappingSet[val['entityId']]['firstLevel'] = 'Choose Language'
                else:
                    mappingSet[val['entityId']]['firstLevel'] = 'WhatsApp Flow'
            else:
                if str(val['optionValue']) in ['1']:
                    mappingSet[val['entityId']]['secondLevel'] = 'Call Transferred'
                else:
                    mappingSet[val['entityId']]['secondLevel'] = 'WhatsApp Flow'
        
        print('mappingSet cretated')    

        '''
        funnelCalls['digitMap'] = funnelCalls.apply(lambda x: getDigitPresses(x['digitPressed'], mappingSet, x['messageBox'], x['caseId']), axis=1)
        
        funnelCallDump = {}
        for index, val in funnelCalls.iterrows():
            
            additionalDetails = json.loads(val['additionalDetails'])
            sid = None
        
            #print(additionalDetails)    
            apiResponse = val['apiResponse']
            if apiResponse is not None:
                apiResponseD = json.loads(apiResponse)
                if 'callSid' in apiResponseD.keys():
                    sid = apiResponseD['callSid']
                    
                elif 'Call' in apiResponseD.keys():
                    Call = apiResponseD['Call']
                    if 'Sid' in Call.keys():
                        sid = Call['Sid']
        
            if sid is None or sid == '':
                continue
        
            if sid not in funnelCallDump.keys():
                funnelCallDump[sid] = val['digitMap']
        
        print('funnelCallDump created')
        '''       
        exotelInsightsDump = {}
        for val in exotelInsights:
            
            callSid = val['callSid']
            noSent = False
            requested = False
            agentInSystem = ''
            agentAvailable = ''
            ayuPersonnelType = None 
            if callSid not in exotelInsightsDump.keys():
                exotelInsightsDump[callSid] = {}
                exotelInsightsDump[callSid]['noSent'] = noSent
                exotelInsightsDump[callSid]['ayuPersonnelType'] = ayuPersonnelType
                
            insightsData = json.loads(val['insightData'])
            if 'insightData' not in insightsData.keys():
                continue
            
            insightData = insightsData['insightData']
            
            if 'agents' in insightData.keys():
                agents = insightData['agents']
                if len(agents) > 0:
                    noSent = True
                    requested = True
                    
                elif len(agents) == 0:
                    requested = True
                    
            if 'agentInSystem' in insightData.keys():
                agentInSystem = insightData['agentInSystem']
                
            if 'agentAvailable' in insightData.keys():
               agentAvailable = insightData['agentAvailable']
               
            if 'ayuPersonnelType' in insightData.keys():
                ayuPersonnelType = insightData['ayuPersonnelType']
                
            exotelInsightsDump[callSid]['noSent'] = noSent
            exotelInsightsDump[callSid]['requested'] = requested
            exotelInsightsDump[callSid]['ayuPersonnelType'] = ayuPersonnelType
            exotelInsightsDump[callSid]['agentInSystem'] = agentInSystem
            exotelInsightsDump[callSid]['agentAvailable'] = agentAvailable
        
        print('exotelInsightsDump Created')
        
        exotelCallInsightsDump = {}
        for val in exotelCallInsights:
            
            callSid = val['callSid']
            insightsData = json.loads(val['insightData'])
            
            if 'insightData' not in insightsData.keys():
                continue
            
            insightData = insightsData['insightData']
            
            if callSid not in exotelCallInsightsDump.keys():
                exotelCallInsightsDump[callSid] = insightData
        
        print('exotelCallInsightsDump created')
        
        exotel['cxNo'] = exotel.apply(lambda x: msg_to(x['from_no']), axis=1)
        
        exotel['city'] = exotel.apply(lambda x: getCity(x['cxNo'], casesDump), axis=1)
        '''
        df = exotel.apply(lambda x: getIvrTransferStatus(x['sid'], funnelCallDump), axis=1,result_type = 'expand')
        exotel = pd.concat([exotel, df], axis=1)
        exotel = exotel.rename(columns={0: 'ivrCheck', 1: 'ivrTransferStatus'})
        
        print('ivr applied')
        '''
        df1 = exotel.apply(lambda x: getInsightData(x['sid'], exotelInsightsDump), axis=1, result_type='expand')
        exotel = pd.concat([exotel, df1], axis=1)
        exotel = exotel.rename(columns={0: 'noSent', 1: 'ayuPersonnelType', 2: 'requested', 3: 'agentInSystem', 4: 'agentAvailable'})
        
        print('agent checked')
        
        df2 = exotel.apply(lambda x: getInsightCallData(x['sid'],x['leg_1_status'],x['leg_2_status'],exotelCallInsightsDump,x['noSent']), axis=1, result_type='expand')
        exotel = pd.concat([exotel, df2], axis=1)
        exotel = exotel.rename(columns={0: 'leg1DetailedStatus', 1: 'leg2DetailedStatus', 2: 'leg3DetailedStatus', 3: 'finalDetailedStatus', 4: 'leg1AgentNumber', 5: 'leg2AgentNumber', 6: 'leg3AgentNumber' ,7:'leg1Status' ,8:'leg2Status' ,9:'leg3Status' })
        
        print('detailed status applied')
        
        df3 = exotel.apply(lambda x: getagents_info(x['leg1AgentNumber'],x['leg2AgentNumber'],x['leg3AgentNumber'],agentsdump),axis=1, result_type='expand')
        exotel = pd.concat([exotel,df3],axis=1)
        exotel = exotel.rename(columns = {0:'leg1Agent_email',1:'leg2Agent_email',2:'leg3Agent_email',3:'leg1Agent_city',4:'leg2Agent_city',5:'leg3Agent_city'})
        
        print('leg wise agent data stored')
        
        exotel['leg_1_duration'] = exotel['leg_1_duration'].replace(np.nan, 0)
        exotel['leg_2_duration'] = exotel['leg_2_duration'].replace(np.nan, 0)
        exotel['duration'] = exotel['duration'].replace(np.nan, 0)
        
        print(exotel)
       
        
        exotel_summary = []
        for index,val in exotel.iterrows():
            exotel_summary.append([
                str(val['sid']),
                val['call_created_on'],
                str(val['status']),
                str(val['direction']),
                str(val['to_no']),
                str(val['from_no']),
                str(val['exotel_no']),
                str(val['duration']),
                str(val['price']),
                str(val['leg_1_status']),
                str(val['leg_1_duration']),
                str(val['leg_2_status']),
                str(val['leg_2_duration']),
                str(val['recording_url']),
                val['maskDate'],
                str(val['Hour']),
                str(val['Month']),
                str(val['Year']),
                str(val['tagHour']),
                str(val['cxNo']),
                str(val['city']),
                '',
                '',
                str(val['noSent']),
                str(val['ayuPersonnelType']),
                str(val['leg1DetailedStatus']),
                str(val['leg2DetailedStatus']),
                str(val['leg3DetailedStatus']),
                str(val['finalDetailedStatus']),
                str(val['leg1AgentNumber']),
                str(val['leg2AgentNumber']),
                str(val['leg3AgentNumber']),
                str(val['leg1Status']),
                str(val['leg2Status']),
                str(val['leg3Status']),
                str(val['leg1Agent_email']),
                str(val['leg2Agent_email']),
                str(val['leg3Agent_email']),
                str(val['leg1Agent_city']),
                str(val['leg2Agent_city']),
                str(val['leg3Agent_city']),
                str(val['requested']),
                str(val['agentInSystem']),
                str(val['agentAvailable']),
                ])
        
        #print(exotel_summary)
        
        exotel_summary_bq = pd.DataFrame(exotel_summary,columns = ['sid','call_created_on','status', 'direction','to_no', 'from_no', 'exotel_no', 'duration', 'price',
                                                                    'leg_1_status', 'leg_1_duration', 'leg_2_status', 'leg_2_duration',
                                                                    'recording_url', 'maskDate','Hour','Month','Year','tagHour', 'cxNo', 'city', 'ivrCheck',
                                                                    'ivrTransferStatus', 'noSent', 'ayuPersonnelType', 'leg1DetailedStatus',
                                                                    'leg2DetailedStatus', 'leg3DetailedStatus', 'finalDetailedStatus',
                                                                    'leg1AgentNumber', 'leg2AgentNumber', 'leg3AgentNumber', 'leg1Status',
                                                                    'leg2Status', 'leg3Status','leg1Agent_email','leg2Agent_email','leg3Agent_email',
                                                                    'leg1Agent_city','leg2Agent_city','leg3Agent_city', 'requested', 'agentInSystem', 'agentAvailable'])
        
        print('data prepared to send to bq')
        #print(exotel_summary_bq)
        #exotel_summary_bq.to_csv(fpath)
        print('sending data to bq')
        # upload_to_bq(exotel_summary_bq)  
        print(type(exotel_summary_bq))
        print('data sent')  
        df=exotel_summary_bq.copy()
        
        DF=df[(df.finalDetailedStatus=='FAILED_SUBSCRIBER_UNAVAILABLE') | (df.finalDetailedStatus=='FAILED_OPERATOR_ERROR')
         | (df.finalDetailedStatus=='FAILED_UNKNOWN_ERROR')];
        DF=DF.sort_values(by='call_created_on',ascending=False)
        now=datetime.now()+timedelta(hours=5,minutes=30)
        DF.call_created_on=pd.to_datetime(DF.call_created_on)
        data=DF[DF.call_created_on>(now)]
        data[['leg1Agent_email','finalDetailedStatus']]
        print(data.shape) 
        emails=list(data['leg1Agent_email'].values)
        issues=data['finalDetailedStatus']
        print(emails) 
        
        result_base=result_base = """
            <body width = \"500\"> 
            <h1><b>'Please Follow the below instructions to resolve the isuue.'
                </b></h1>
            <br></br>
            
            <body width = \"500\">
            <table style=\"border-collapse:collapse\" border=\"1\" >
            <tr style="background-color:powderblue;"> 
            <td colspan = 3 width = \"500\"><b>
            1. Turn off the phone 
            </b></td></tr>
            
            <tr style="background-color:powderblue;"> 
            <td colspan = 3 width = \"1200\"><b>
            2. Remove and reinsert the SIM card 
            </b></td></tr>
            
            <tr style="background-color:powderblue;"> 
            <td colspan = 3 width = \"1200\"><b>
            3. Finally, turn on the power 
            </b></td></tr>
            
            <tr style="background-color:powderblue;"> 
            <td colspan = 3 width = \"1200\"><b>
            4. Verify that the network is adequate
            </b></td></tr>
            
            """
            
        Subject='Call_Dropped_due_to_error'    
        text_body='Please Follow the below instructions to resolve the isuue.'
        email_recipient_list=["mrugesh.patel@ayu.health"]
        email_recipient_list.append(emails)
        print(email_recipient_list)
        if len(emails):
            send_email(None, email_recipient_list, Subject,'',result_base,[]) 

 

        '''
        Subject = 'Surgery Quality Response'
        email_recipient_list = ['akchansh@ayu.health'] 
            #email_recipient_list = ['ankur@ayu.health','arjit@ayu.health','anshul@ayu.health','sarang@ayu.health','tanisha@ayu.health','himesh@ayu.health]
        send_email(None, email_recipient_list, Subject, 'None', None,[fpath])
        '''
    except Exception as e:
        # Subject = 'Error!, call_dropped/exotel_summary_to_bq_live'
        # email_recipient_list = ['mrugesh.patel@ayu.health']
        # send_email(None, email_recipient_list, Subject, 'None',e,[])
        raise e

'''
'sid', 'call_created_on', 'call_updated_on', 'status', 'direction',
'created_at', 'to_no', 'from_no', 'exotel_no', 'duration', 'price',
'leg_1_status', 'leg_1_duration', 'leg_2_status', 'leg_2_duration',
'recording_url', 'maskDate', 'tagHour', 'cxNo', 'city', 'ivrCheck',
'ivrTransferStatus', 'noSent', 'ayuPersonnelType', 'leg1DetailedStatus',
'leg2DetailedStatus', 'leg3DetailedStatus', 'finalDetailedStatus',
'leg1AgentNumber', 'leg2AgentNumber', 'leg3AgentNumber', 'leg1Status',
'leg2Status', 'leg3Status'
'''
