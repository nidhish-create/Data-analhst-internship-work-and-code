import pdb                                             
import os
import logging
from service.database import InitDatabaseConnetion,make_db_params,fetch_record, DB_Params
from service.send_mail_client import send_email
import pandas as pd
import csv          
from datetime import datetime, timedelta
    
logger = logging.getLogger(__name__)             

READ_DB_USER = os.environ.get('READ_DB_USER', '')
READ_DB_PASSWORD = os.environ.get('READ_DB_PASSWORD', '')
#READ_DB_DOMAIN_URL = os.environ.get('READ_DB_DOMAIN_URL', '')
READ_DB_DOMAIN_URL_BETA = os.environ.get('READ_DB_DOMAIN_URL_BETA', '')  


queries = {
        "ticketing_am": '''select zone, aliasName , cityName , mappingType from ayu_facility_profile af 
                            left join  ayu_mitra_hospital_mapping mapp on (af.facilityId = mapp.facilityId and mapp.isValid= 1 and mapp.tenantName = 'AYU' and mapp.mappingType = 'TICKET_AM')
                            left join ayu_cities pcp on af.cityId = pcp.Id
                        where 
                            active = 1 
                        and zone != "INS"
                        '''
   
}
result_base = '''<body width = \"1000\"> 
        <table style=\"border-collapse:collapse\" border=\"1\" >
        <tr bgcolor=\"#C16444\">
            <td colspan = 2  width = \"1000\"><b><center>  No Ticket_AM Assigned    </center></b></td>
        </tr> 
        <tr bgcolor=\"#FAB299\">
            <td colspan = 1  width = \"500\"><b><center>  City  </center></b></td>
            <td colspan = 1  width = \"500\"><b><center> Number of hospitals with no Ticket AM mapping  </center></b></td>
        </tr>
        
'''

def fetch_data(conn): 
    fetched_val = {}
    for lookup, query in queries.items():
        # print(lookup)
        fetched_val[lookup] = fetch_record(conn,query)
    return fetched_val
    

def lambda_handler(event, context):
    try:
        
        aws_session_token = os.environ.get('AWS_SESSION_TOKEN') 
        READ_DB_USER,READ_DB_PASSWORD,READ_DB_DOMAIN_URL=DB_Params(aws_session_token) 
        
        read_host, read_port, read_database = make_db_params(**{'init_from': 'script', 'db_url': READ_DB_DOMAIN_URL_BETA})
        read_connection_obj = InitDatabaseConnetion(db_url=read_host, port=int(read_port), username=READ_DB_USER,
                                                    password=READ_DB_PASSWORD, db_name=read_database)
                                                    
        
        dataSets = fetch_data(read_connection_obj)
        ticketing_am = pd.DataFrame(dataSets['ticketing_am'])
        
        citydict = {}
        consideredhospitalnames={}
        data_to_send= []
        table = [[0 for x in range(2)] for x in range(5)]
        rindex = 0
        for index , val in ticketing_am.iterrows():
            if val['cityName'] not in citydict.keys():
                citydict[val['cityName']] = rindex
                table[rindex][0] = val['cityName']
                rindex +=1  
                
            if val['aliasName']  not in consideredhospitalnames.keys():
                consideredhospitalnames[val['aliasName']] = ''
            
                if val['mappingType'] is None:
                          
                    table[citydict[val['cityName']]][1] += 1
            
                    
                    data_to_send.append([
                        val['cityName'],
                        val['aliasName'],
                        val['zone']
                    ])
        
        
        df = pd.DataFrame(data_to_send, columns=['cityName', 'hospitalName','zone'])
        
        fpath = os.path.join('/tmp','No_ticket_am_assigned_hospitals.csv')
        df.to_csv(fpath)
        
        result = result_base
        for i in range(0, rindex):
            
            result = result + "<tr>"
            for j in range(0, 2):
                if j == 0:
                    result = result + "<td colspan = 1 ><center>" + str(table[i][j]) + "</center></td>"
                elif table[i][j] == 0:
                    result = result + "<td colspan = 1 ><center> </center></td>"
                else:
                    result = result + "<td colspan = 1 ><center>" + str(table[i][j]) + "</center></td>"
            result = result + "</tr>"
        result = result + "</table><br>"  
        
        Subject = 'TICKET_AM | NOT_ASSIGNED'
        email_recipient_list = ['vivekanand@ayu.health','jyothi@ayu.health','vaibhav.mishra@ayu.health','sreenadh.a@ayu.health','manpreet@ayu.health',"ankur@ayu.health",'sawan@ayu.health','vijay.g@ayu.health','jay@ayu.health','rohit.chahal@ayu.health','neha@ayu.health','shashank@ayu.health','ncr_ops@ayu.health','jaipur-ops@ayu.health','ops_chd@ayu.health', 'tls@ayu.health','rohit.kothari@ayu.health']
        # email_recipient_list = ['akchanshk.998@gmail.com'] 
        send_email(None, email_recipient_list, Subject, 'None',result,[fpath])  
 
    except Exception as e:
        Subject = 'ticketing_am | Error'
        email_recipient_list = ['analytics@ayu.health']
        send_email(None, email_recipient_list, Subject, 'None',str(e),[])
        raise e
    

