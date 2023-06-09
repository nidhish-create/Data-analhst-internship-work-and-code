import pdb
import os
import logging
from service.database import InitDatabaseConnetion, make_db_params, fetch_record
from service.send_mail_client import send_email
import pandas as pd
from datetime import datetime, timedelta
from service.base_functions import msg_to, exotel_cxNo
import pytz
import locale
from big_query import main as getdatafrombigq
import boto3
from s3_utils.utils import upload_to_s3

logger = logging.getLogger(__name__)

locale.setlocale(locale.LC_ALL, 'en_IN.utf8')

READ_DB_USER = os.environ.get('READ_DB_USER', '')
READ_DB_PASSWORD = os.environ.get('READ_DB_PASSWORD', '')
READ_DB_DOMAIN_URL = os.environ.get('READ_DB_DOMAIN_URL', '')

change_to_indian_time = lambda x: x.astimezone(pytz.timezone('Asia/kolkata')).strftime('%Y-%m-%d') if type(
    x) == datetime else None

yest_date = datetime.now() + timedelta(hours=5,minutes=30,days=-1)
mtd_start = yest_date.replace(day=1)
yest_date = yest_date.strftime('%Y-%m-%d')
start = mtd_start - timedelta(days=1)
start = start.replace(day=1)
#start = '2022-02-01'
lastmonth = str(start.month)
thismonth = str(mtd_start.month)
start = start.strftime('%Y-%m-%d')
print(start, yest_date)
#start = '2022-02-01'

queries = {
    "ayu_cash": """select ayuCashId,
                          ac.customerId,
                          cashbackType,
                          amount/100 as 'amount',
                          issuedEntityType,
                          issuedEntityId,
                          validTill,
                          ac.additionalDetails,
                          date_add(ac.createdOn, INTERVAL '5:30' HOUR_MINUTE) as 'createdOn',
                          date_add(ac.createdOn, INTERVAL '5:30' HOUR_MINUTE) as 'msgSentOn',
                          transactionType,
                          case when date(date_add(ac.createdOn, INTERVAL '5:30' HOUR_MINUTE)) = curdate() - interval 1 day
                            then 'Yest'
                        else 'Outside'
                        end as dateFlag,
                        month(date_add(ac.createdOn, INTERVAL '5:30' HOUR_MINUTE)) as 'month',
                        customerNumber,
                        customerName
                        from ayu_cash ac
                        join customer_profile cp on ac.customerId = cp.customerId
                        where
                            date(date_add(ac.createdOn, INTERVAL '5:30' HOUR_MINUTE)) >= '{start}'
                            and date(date_add(ac.createdOn, INTERVAL '5:30' HOUR_MINUTE)) <= '{end}'
                            order by ac.createdOn
                            """,
    "cashbackType": """select cashbackId, cashbackType from cashback_type where isActive=1 """,
    "appointments": """select id, user, leadId, consultationType from lead_doctor_consultation""",
    "loyalty": """select cardId,collectedBy from lc_payment_details""",
    "ayuMitra": """select email as ayuMitraEmail from ayu_personnel_details 
                        where
                            personnelType='AYU_MITRA' """,
    "agents": "select email from customer_support_details",
    "cases": "select caseId from patient_case pc where leadSource = 'Customer App' ",
    "comms_logs": """
        Select entityTagMapId,communicationDone,date_add(log.communicationDate,interval '5:30' HOUR_MINUTE) as msgSentOn,
         tag.*,pp.id as patientId,pp.patientName,cp.customerNumber,log.communicationType,log.assetId,
         date(date_add(log.communicationDate,interval '5:30' HOUR_MINUTE)) as maskDate,
         tag.additionalDetails->>'$.ayuCashNudgingFlow' as 'ayuCashNudgingFlow',
         case when date(date_add(log.communicationDate, INTERVAL '5:30' HOUR_MINUTE)) = curdate() - interval 1 day
                            then 'Yest'
                        else 'Outside'
                        end as dateFlag,
                        month(date_add(log.communicationDate, INTERVAL '5:30' HOUR_MINUTE)) as 'month',pp.customerId,
                        date_add(pp.createdOn,interval '5:30' HOUR_MINUTE) as 'patientCreatedOn'
         from 
        user_communication_logs log 
        join ayu_assets aa on log.assetId = aa.id
        join entity_tag_map tag on log.entityTagMapId = tag.id
        left join patient_profile pp on (tag.entityId = pp.id and entityType = 'PATIENT_PROFILE')
        left join customer_profile cp on pp.customerId=cp.customerId
        where 
            tag.tagId = 275
        and communicationDone = 1 
        and date(date_add(log.communicationDate,interval '5:30' HOUR_MINUTE)) >="{start}" 
        and date(date_add(log.communicationDate,interval '5:30' HOUR_MINUTE)) <= "{end}" 
        and communicationType = 'SMS'
        order by 3""",
    "comms_logs_diag_hp": """
        Select entityTagMapId,communicationDone,date_add(log.communicationDate,interval '5:30' HOUR_MINUTE) as msgSentOn
         ,pp.id as patientId,pp.patientName,cp.customerNumber,log.communicationType,log.assetId,
         date(date_add(log.communicationDate,interval '5:30' HOUR_MINUTE)) as maskDate,
         tag.additionalDetails->>'$.ayuCashNudgingFlow' as 'ayuCashNudgingFlow',
         case when date(date_add(log.communicationDate, INTERVAL '5:30' HOUR_MINUTE)) = curdate() - interval 1 day
                            then 'Yest'
                        else 'Outside'
                        end as dateFlag,
                        month(date_add(log.communicationDate, INTERVAL '5:30' HOUR_MINUTE)) as 'month',pp.customerId,
        case when log.assetId in (256,255) then 'Home_pick' else 'Diagnostics_test' end as nudgeTag ,
        date_add(pp.createdOn,interval '5:30' HOUR_MINUTE) as 'patientCreatedOn'
            
         from 
        user_communication_logs log 
        join ayu_assets aa on log.assetId = aa.id
        join entity_tag_map tag on log.entityTagMapId = tag.id
        left join patient_profile pp on (tag.entityId = pp.id and entityType = 'PATIENT_PROFILE')
        left join customer_profile cp on pp.customerId=cp.customerId
        where 
            log.assetId in (251,252,253,254,255,256,258)
        and communicationDone = 1 
        and date(date_add(log.communicationDate,interval '5:30' HOUR_MINUTE)) >="{start}" 
        and date(date_add(log.communicationDate,interval '5:30' HOUR_MINUTE)) <= "{end}" 
        and communicationType = 'SMS'
        order by 3""",
    "whatsapp": """select fromId,toMessage,date_add(createdOn,interval '5:30' HOUR_MINUTE) as 'createdOn',message,
                            'inbound' as direction,
                            '' as leg_1_status,
                        '' as leg_2_status,
                        'WhatsApp' as callType,
                        date(date_add(createdOn,interval '5:30' HOUR_MINUTE)) as maskDate
                    from whatsapp_message_details
                    where date(date_add(createdOn,interval '5:30' HOUR_MINUTE))>="{start}" 
                    and date(date_add(createdOn,interval '5:30' HOUR_MINUTE)) <= "{end}"
                    and fromId = toMessage
                    order by createdOn
                    """,
    "exotel": """select to_no as toMessage,
                        from_no as fromId,
                        call_created_on as 'createdOn',
                        'exotel-call' as message,
                        direction,
                        leg_1_status,
                        leg_2_status,
                        'Exotel' as callType,
                        date(call_created_on) as maskDate
                    from exotel_response
                    where date(call_created_on)>="{start}" 
                    and date(call_created_on) <= "{end}"
                    and direction = 'inbound'
                    order by call_created_on
                    """,
    "app_pay" : """select 
                    ldc.id as appId,
                    consultationFee,
                    case when amountPaid is null then 0 else amountPaid end as 'amountPaid',
                    appointmentCreationType
                from 
                    lead_doctor_consultation ldc 
                    left join (
                    select entityId, sum(round(amount/100,2)) as 'amountPaid'
                        from
                    online_payment_link pay 
                    where
                        entityType = 'APPOINTMENT'
                        and status = 'PAID'
                        and entityId in ({appIds}) 
                    group by 1
                    ) pay on pay.entityId = ldc.id 
                    where
                        ldc.id in ({appIds})
                """,
    "loyalty_pay": """Select 
                    l.cardId
                from 
                        lc_payment_details l  
                        join generated_mcards c on l.cardId = c.cardId
                    where
                        status = 'ACTIVE'
                        and l.cardId in ({cardIds})
                        """,
    "ayuCashPromotional_Once": """Select 
                        *,
                        additionalDetails->>'$.promotionalAyuCashAmount' as promotionalAyuCashAmount,
                        date_add(scheduledOn,interval '5:30' HOUR_MINUTE) as msgSentOn,
                        case when date(date_add(scheduledOn, INTERVAL '5:30' HOUR_MINUTE)) = curdate() - interval 1 day
                            then 'Yest'
                        else 'Outside'
                        end as dateFlag,
                        month(date_add(scheduledOn, INTERVAL '5:30' HOUR_MINUTE)) as 'month'
                    from 
                        nudging_one_time 
                        where
                            communicationType = 'SMS' 
                            and status = 'SUCCESS'
                            and additionalDetails->>'$.ayuCashPromoDone' = 'true'
                            and date(date_add(scheduledOn,interval '5:30' HOUR_MINUTE)) >= "{start}"
                            and date(date_add(scheduledOn,interval '5:30' HOUR_MINUTE)) <= "{end}"
                            order by scheduledOn
                    """
}

#Defing csv Files
fpath = os.path.join('/tmp', 'ayu_cash.csv.gz')
fpath1 = os.path.join('/tmp', 'ayu_promotional.csv.gz')
fpath2 = os.path.join('/tmp', 'ayu_cash_nudge.csv.gz')
fpath3 = os.path.join('/tmp', 'ayu_promotional_cc.csv.gz')
fpath4 = os.path.join('/tmp', 'diagnostics_nudge.csv.gz')
fpath6 = os.path.join('/tmp', 'homepick_nudge.csv.gz')
fpath5 = os.path.join('/tmp', 'Transaction_per_user.csv.gz')

def fetch_data(conn):
    fetched_val = {}
    for lookup, query in queries.items():
        print(lookup)
        if lookup == 'ayu_cash':
            fetched_val[lookup] = fetch_record(conn, query.format(start=start, end=yest_date))
            ayuAppIds = []
            loyaltyCardIds = []
            for val in fetched_val[lookup]:
                if val['transactionType'] == 'DEBITED':
                    if val['issuedEntityType'] in ('APPOINTMENT', 'DIAGNOSTICS_APPOINTMENT'):
                        ayuAppIds.append(str(val['issuedEntityId']))
                    elif val['issuedEntityType'] == 'LOYALTY_CARD':
                        loyaltyCardIds.append(str(val['issuedEntityId']))
            
        elif lookup in ('app_pay'):
            fetched_val[lookup] = fetch_record(conn, query.format(appIds = ','.join(ayuAppIds)))
        elif lookup in ('loyalty_pay'):
            fetched_val[lookup] = fetch_record(conn, query.format(cardIds= ','.join(loyaltyCardIds)))
        else:
            fetched_val[lookup] = fetch_record(conn, query.format(start=start, end=yest_date))
    return fetched_val
    

result_base = """
        <body width = \"1200\">
        <table style=\"border-collapse:collapse\" border=\"1\" >
        <tr bgcolor=\"#BDCE90\"> 
            <td colspan = 4  width = \"1200\"><b><center> Overall | Utilisation </center></b></td>
        </tr>
        <tr bgcolor=\"#EEFCF0\"> 
            <td colspan = 1  width = \"300\"><b><center> Cash Amount </center></b></td>
            <td colspan = 1  width = \"300\"><b><center> Yesterday  </center></b></td>
            <td colspan = 1  width = \"300\"><b><center> MTD  </center></b></td>
            <td colspan = 1  width = \"300\"><b><center> Last Month  </center></b></td>
        </tr>
"""

result_base1 = """
        <table style=\"border-collapse:collapse\" border=\"1\" >
        <tr bgcolor=\"#BDCE90\"> 
            <td colspan = 13  width = \"1600\"><b><center> Transactions | Utilisation {1} </center></b></td>
        </tr>
        <tr bgcolor=\"#EEFCF0\"> 
            <td colspan = 1  rowspan=2 width = \"100\"><b><center> Transactions {0} </center></b></td>
            <td colspan = 4  width = \"500\"><b><center> Yesterday  </center></b></td>
            <td colspan = 4  width = \"500\"><b><center> MTD </center></b></td>
            <td colspan = 4  width = \"500\"><b><center> Last Month </center></b></td>
        </tr>
        <tr bgcolor=\"#EEFCF0\"> 
            <td colspan = 1  width = \"100\"><b><center> Appointments  </center></b></td>
            <td colspan = 1  width = \"100\"><b><center> Diagnostics  </center></b></td>
            <td colspan = 1  width = \"100\"><b><center> Surgery  </center></b></td>
            <td colspan = 1  width = \"100\"><b><center> Loyalty  </center></b></td>
            <td colspan = 1  width = \"100\"><b><center> Appointments  </center></b></td>
            <td colspan = 1  width = \"100\"><b><center> Diagnostics  </center></b></td>
            <td colspan = 1  width = \"100\"><b><center> Surgery  </center></b></td>
            <td colspan = 1  width = \"100\"><b><center> Loyalty  </center></b></td>
            <td colspan = 1  width = \"100\"><b><center> Appointments  </center></b></td>
            <td colspan = 1  width = \"100\"><b><center> Diagnostics  </center></b></td>
            <td colspan = 1  width = \"100\"><b><center> Surgery  </center></b></td>
            <td colspan = 1  width = \"100\"><b><center> Loyalty  </center></b></td>
        </tr>
"""

result_base1_1 = """
        <table style=\"border-collapse:collapse\" border=\"1\" >
        <tr bgcolor=\"#BDCE90\"> 
            <td colspan = 16  width = \"1300\"><b><center> Transactions | Utilisation </center></b></td>
        </tr>
        <tr bgcolor=\"#EEFCF0\"> 
            <td colspan = 1  rowspan=2 width = \"100\"><b><center> Transactions </center></b></td>
            <td colspan = 5  width = \"400\"><b><center> Yesterday  </center></b></td>
            <td colspan = 5  width = \"400\"><b><center> MTD </center></b></td>
            <td colspan = 5  width = \"400\"><b><center> Last Month </center></b></td>
        </tr>
        <tr bgcolor=\"#EEFCF0\"> 
            <td colspan = 1  width = \"100\"><b><center> Appointments  </center></b></td>
            <td colspan = 1  width = \"100\"><b><center> Diagnostics  </center></b></td>
            <td colspan = 1  width = \"100\"><b><center> Surgery  </center></b></td>
            <td colspan = 1  width = \"100\"><b><center> Loyalty  </center></b></td>
            <td colspan = 1  width = \"100\"><b><center> Promotional  </center></b></td>
            <td colspan = 1  width = \"100\"><b><center> Appointments  </center></b></td>
            <td colspan = 1  width = \"100\"><b><center> Diagnostics  </center></b></td>
            <td colspan = 1  width = \"100\"><b><center> Surgery  </center></b></td>
            <td colspan = 1  width = \"100\"><b><center> Loyalty  </center></b></td>
            <td colspan = 1  width = \"100\"><b><center> Promotional  </center></b></td>
            <td colspan = 1  width = \"100\"><b><center> Appointments  </center></b></td>
            <td colspan = 1  width = \"100\"><b><center> Diagnostics  </center></b></td>
            <td colspan = 1  width = \"100\"><b><center> Surgery  </center></b></td>
            <td colspan = 1  width = \"100\"><b><center> Loyalty  </center></b></td>
            <td colspan = 1  width = \"100\"><b><center> Promotional  </center></b></td>
        </tr>
"""

result_base2 = """
        <table style=\"border-collapse:collapse\" border=\"1\" >
        <tr bgcolor=\"#BDCE90\"> 
            <td colspan = 4 width = \"1200\"><b><center> Ayu Cash Promotional SMS  </center></b></td>
        </tr>
        <tr bgcolor=\"#EEFCF0\"> 
            <td colspan = 1  width = \"300\"><b><center>  </center></b></td>
            <td colspan = 1  width = \"300\"><b><center> Yesterday </center></b></td>
            <td colspan = 1  width = \"300\"><b><center> MTD  </center></b></td>
            <td colspan = 1  width = \"300\"><b><center> Last Month  </center></b></td>
        </tr>
"""
result_base7 = """
        <table style=\"border-collapse:collapse\" border=\"1\" >
        <tr bgcolor=\"#BDCE90\"> 
            <td colspan = 7 width = \"1200\"><b><center> Nudging | Diagnostics test & HomePick  </center></b></td>
        </tr>
        <tr bgcolor=\"#EEFCF0\"> 
            <td colspan = 1  rowspan=2 width = \"300\"><b><center>  </center></b></td>
            <td colspan = 3  width = \"450\"><b><center> Diagnostics Test </center></b></td>
            <td colspan = 3  width = \"450\"><b><center> HomePick  </center></b></td>
        </tr>
        <tr bgcolor=\"#EEFCF0\"> 
            <td colspan = 1  width = \"150\"><b><center> Yesterday </center></b></td>
            <td colspan = 1  width = \"150\"><b><center> MTD  </center></b></td>
            <td colspan = 1  width = \"150\"><b><center> Last Month  </center></b></td>
            <td colspan = 1  width = \"150\"><b><center> Yesterday </center></b></td>
            <td colspan = 1  width = \"150\"><b><center> MTD  </center></b></td>
            <td colspan = 1  width = \"150\"><b><center> Last Month  </center></b></td>
        </tr>
"""

result_base6 = """
        <table style=\"border-collapse:collapse\" border=\"1\" >
        <tr bgcolor=\"#BDCE90\"> 
            <td colspan = 9 width = \"1200\"><b><center> Ayu Cash Promotional | Last Interaction </center></b></td>
        </tr>
        <tr bgcolor=\"#EEFCF0\"> 
            <td colspan = 1 rowspan=2 width = \"400\"><b><center>  </center></b></td>
            <td colspan = 4  width = \"400\"><b><center> MTD  </center></b></td>
            <td colspan = 4  width = \"400\"><b><center> Last Month  </center></b></td>
        </tr>
        <tr bgcolor=\"#EEFCF0\"> 
            <td colspan = 1  width = \"100\"><b><center> 1-6 Month  </center></b></td>
            <td colspan = 1  width = \"100\"><b><center> 6-12 Month  </center></b></td>
            <td colspan = 1  width = \"100\"><b><center> 12-24 Month  </center></b></td>
            <td colspan = 1  width = \"100\"><b><center> >24 Month  </center></b></td>
            <td colspan = 1  width = \"100\"><b><center> 1-6 Month  </center></b></td>
            <td colspan = 1  width = \"100\"><b><center> 6-12 Month  </center></b></td>
            <td colspan = 1  width = \"100\"><b><center> 12-24 Month  </center></b></td>
            <td colspan = 1  width = \"100\"><b><center> >24 Month  </center></b></td>
        </tr>
"""

result_base3 = """
        <table style=\"border-collapse:collapse\" border=\"1\" >
        <tr bgcolor=\"#BDCE90\"> 
            <td colspan = 7  width = \"1200\"><b><center> Ayu Cash Nudging  </center></b></td>
        </tr>
        <tr bgcolor=\"#EEFCF0\"> 
            <td colspan = 1  rowspan=2 width = \"300\"><b><center> </center></b></td>
            <td colspan = 2  width = \"350\"><b><center> Yesterday </center></b></td>
            <td colspan = 2  width = \"350\"><b><center> MTD  </center></b></td>
            <td colspan = 2  width = \"350\"><b><center> Last Month  </center></b></td>
        </tr>
        <tr bgcolor=\"#EEFCF0\"> 
            <td colspan = 1  width = \"175\"><b><center> CS </center></b></td>
            <td colspan = 1  width = \"175\"><b><center> AYU_MITRA </center></b></td>
            <td colspan = 1  width = \"175\"><b><center> CS </center></b></td>
            <td colspan = 1  width = \"175\"><b><center> AYU_MITRA </center></b></td>
            <td colspan = 1  width = \"175\"><b><center> CS </center></b></td>
            <td colspan = 1  width = \"175\"><b><center> AYU_MITRA </center></b></td>
        </tr>
"""

result_base4 = """
        <table style=\"border-collapse:collapse\" border=\"1\" >
        <tr bgcolor=\"#BDCE90\"> 
            <td colspan = 9  width = \"1200\"><b><center> Ayu Cash | Nudging & Promotional </center></b></td>
        </tr>
        <tr bgcolor=\"#EEFCF0\"> 
            <td colspan = 1  rowspan=2 width = \"300\"><b><center> </center></b></td>
            <td colspan = 3  width = \"350\"><b><center> Nudging </center></b></td>
            <td colspan = 3  width = \"350\"><b><center> Promotional SMS  </center></b></td>
            <td colspan = 2  width = \"350\"><b><center> Promotional SMS (Last Interaction) </center></b></td>
        </tr>
        <tr bgcolor=\"#EEFCF0\"> 
            <td colspan = 1  width = \"175\"><b><center> Yesterday </center></b></td>
            <td colspan = 1  width = \"175\"><b><center> MTD </center></b></td>
            <td colspan = 1  width = \"175\"><b><center> Last Month </center></b></td>
            <td colspan = 1  width = \"175\"><b><center> Yesterday </center></b></td>
            <td colspan = 1  width = \"175\"><b><center> MTD </center></b></td>
            <td colspan = 1  width = \"175\"><b><center> Last Month </center></b></td>
            <td colspan = 1  width = \"175\"><b><center> MTD </center></b></td>
            <td colspan = 1  width = \"175\"><b><center> Last Month </center></b></td>
        </tr>
"""

result_base5 = """
<body width = \"1200\"> 
        <table style=\"border-collapse:collapse\" border=\"1\" >
        <tr bgcolor=\"#D1DCB2\"> 
            <td colspan = 4 width = \"1200\"><b><center> Transactions per user  </center></b></td>
        </tr>
        
<tr bgcolor=\"#EEFCF0\"> 
<td colspan = 1  rowspan =1 width = \"200\"><b><center> category </center></b></td>
  <td colspan = 1  width = \"100\"><b><center>  Ayu_cash </center></b></td>
     
  <td colspan = 1  width = \"100\"><b><center> Non Ayu-cash </center></b></td>
     
  <td colspan = 1  width = \"100\"><b><center> Overall </center></b></td>
  </tr>
  
  """

def calculate_result(result, table, start, rows, cols):

	for i in range(start, rows):
		result = result + "<tr>"
		for j in range(0, cols):
			if j == 0:
				result = result + "<td colspan = 1 ><center>" + str(table[i][j]) + "</center></td>"
			elif table[i][j] == 0:
				result = result + "<td colspan = 1 ><center> </center></td>"
			else:
				result = result + "<td colspan = 1 ><center>" + str(table[i][j]) + "</center></td>"
		result = result + "</tr>"
	result = result + "</table><br>"

	return result

def formatCurrency(price):
    if price:
    	price = locale.currency(float(price), symbol=False, grouping=True)
    else:
	    price = '0.00'
    
    if price.endswith('.00'):
        price = price[:-3]
    
    return price


def summarize_table(row, col, tableD, result):
    # Fill in Values in HTML content:
    for i in range(0, row):
        result = result + "<tr>"
        for j in range(0, col):
            if j == 0:
                result = result + "<td colspan = 1 ><center>" + str(tableD[i][j]) + "</center></td>"
            elif tableD[i][j] == 0:
                result = result + "<td colspan = 1 ><center> </center></td>"
            else:
                result = result + "<td colspan = 1 ><center>" + str(tableD[i][j]) + "</center></td>"
        result = result + "</tr>"
    result = result + "</table><br>"

    return result
    

def rowMap(x) :
    return {
    1: 0,
    2: 1,
    3: 2,
    4: 3
}.get(x, 4)
    

def getCashBackType(cashbackType, cashbackTypeDump):
    
    if cashbackType in cashbackTypeDump.keys():
        return cashbackTypeDump[cashbackType]
        
    return ''
    
rowMap = {
    'AYU_MITRA' : 0,
    'CS_AGENT': 1,
    'CUSTOMER_APP': 2,
    'NA': 3
}
    
def prepare_table(ayu_cash_debited, ayu_cash_credited):
    
    table1 = [[0 for x in range(4)] for x in range(2)]
    table2 = [[0 for x in range(16)] for x in range(2)]
    table3 = [[0 for x in range(13)] for x in range(4)]
    
    table1[0][0] = 'Given'
    table1[1][0] = 'Used'
    
    table2[0][0] = 'Given'
    table2[1][0] = 'Used'
    
    table3[0][0] = 'AYU_MITRA'
    table3[1][0] = 'CS_AGENT'
    table3[2][0] = 'CUSTOMER_APP'
    table3[3][0] = 'NA'
    
    ayuCashDebited = {}
    
    for index, val in ayu_cash_debited.iterrows():
        
        if val['paymentTag'] == 'Partial_Payment':
            continue
        
        if val['Contact_Number'] not in ayuCashDebited.keys():
            ayuCashDebited[val['Contact_Number']] = []
            
        ayuCashDebited[val['Contact_Number']].append(val)
        
        if val['dateFlag'] == 'Yest':
            table1[1][1] += val['amount']
            
            if val['issuedEntityType'] == 'APPOINTMENT':
                table2[1][1] += val['amount']
            elif val['issuedEntityType'] == 'DIAGNOSTICS_APPOINTMENT':
                table2[1][2] += val['amount']
            elif val['issuedEntityType'] == 'PATIENT_SURGERY':
                table2[1][3] += val['amount']
            elif val['issuedEntityType'] == 'LOYALTY_CARD':
                table2[1][4] += val['amount']
        
        if str(val['month']) == thismonth:    
            table1[1][2] += val['amount']
            
            if val['issuedEntityType'] == 'APPOINTMENT':
                table2[1][6] += val['amount']
            elif val['issuedEntityType'] == 'DIAGNOSTICS_APPOINTMENT':
                table2[1][7] += val['amount']
            elif val['issuedEntityType'] == 'PATIENT_SURGERY':
                table2[1][8] += val['amount']
            elif val['issuedEntityType'] == 'LOYALTY_CARD':
                table2[1][9] += val['amount']
                
        if str(val['month']) == lastmonth:    
            table1[1][3] += val['amount']
            
            if val['issuedEntityType'] == 'APPOINTMENT':
                table2[1][11] += val['amount']
            elif val['issuedEntityType'] == 'DIAGNOSTICS_APPOINTMENT':
                table2[1][12] += val['amount']
            elif val['issuedEntityType'] == 'PATIENT_SURGERY':
                table2[1][13] += val['amount']
            elif val['issuedEntityType'] == 'LOYALTY_CARD':
                table2[1][14] += val['amount']
        
        
    for index, val in ayu_cash_credited.iterrows():
        
        if val['dateFlag'] == 'Yest':
            table1[0][1] += val['amount']
            
            if val['issuedEntityType'] == 'APPOINTMENT':
                table2[0][1] += val['amount']
                table3[rowMap[val['tag']]][1] += val['amount']
                
            elif val['issuedEntityType'] == 'DIAGNOSTICS_APPOINTMENT':
                table2[0][2] += val['amount']
                table3[rowMap[val['tag']]][2] += val['amount']
                
            elif val['issuedEntityType'] == 'PATIENT_SURGERY':
                table2[0][3] += val['amount']
                table3[rowMap[val['tag']]][3] += val['amount']
                
            elif val['issuedEntityType'] == 'LOYALTY_CARD':
                table2[0][4] += val['amount']
                table3[rowMap[val['tag']]][4] += val['amount']
            
            elif val['cashbackType'] == 6:
                table2[0][5] += val['amount']
        
        if str(val['month']) == thismonth:        
        
            table1[0][2] += val['amount']
            
            if val['issuedEntityType'] == 'APPOINTMENT':
                table2[0][6] += val['amount']
                table3[rowMap[val['tag']]][5] += val['amount']
                
            elif val['issuedEntityType'] == 'DIAGNOSTICS_APPOINTMENT':
                table2[0][7] += val['amount']
                table3[rowMap[val['tag']]][6] += val['amount']
                
            elif val['issuedEntityType'] == 'PATIENT_SURGERY':
                table2[0][8] += val['amount']
                table3[rowMap[val['tag']]][7] += val['amount']
                
            elif val['issuedEntityType'] == 'LOYALTY_CARD':
                table2[0][9] += val['amount']
                table3[rowMap[val['tag']]][8] += val['amount']
                
            elif val['cashbackType'] == 6:
                table2[0][10] += val['amount']
                
        if str(val['month']) == lastmonth:        
        
            table1[0][3] += val['amount']
            
            if val['issuedEntityType'] == 'APPOINTMENT':
                table2[0][11] += val['amount']
                table3[rowMap[val['tag']]][9] += val['amount']
                
            elif val['issuedEntityType'] == 'DIAGNOSTICS_APPOINTMENT':
                table2[0][12] += val['amount']
                table3[rowMap[val['tag']]][10] += val['amount']
                
            elif val['issuedEntityType'] == 'PATIENT_SURGERY':
                table2[0][13] += val['amount']
                table3[rowMap[val['tag']]][11] += val['amount']
                
            elif val['issuedEntityType'] == 'LOYALTY_CARD':
                table2[0][14] += val['amount']
                table3[rowMap[val['tag']]][12] += val['amount']
                
            elif val['cashbackType'] == 6:
                table2[0][15] += val['amount']
            
    for i in range(0, 2):
        for j in range(1, 4):
            table1[i][j] = formatCurrency(table1[i][j])
            
            
    for i in range(0, 2):
        for j in range(1, 16):
            table2[i][j] = formatCurrency(table2[i][j])
            
    for i in range(0, 4):
        for j in range(1, 13):
            table3[i][j] = formatCurrency(table3[i][j])
    
    result = result_base        
    result = summarize_table(2, 4, table1, result)
    
    result += result_base1_1
    result = summarize_table(2, 16, table2, result)
    
    result += result_base1.format('(Credited)', '| Tagged')        
    result = summarize_table(4, 13, table3, result)
    
    return result, ayuCashDebited
    
def getTag(issuedEntityId, transactionType, issuedEntityType, appointmentsDump, loyaltyDump, customerAppCases):
    
    if transactionType == 'DEBITED':
        return ''
        
    if issuedEntityType == 'PATIENT_SURGERY':
        return 'AYU_MITRA'
    elif issuedEntityType == 'DIAGNOSTICS_APPOINTMENT':
        return 'AYU_MITRA'
    elif issuedEntityType == 'APPOINTMENT':
        if issuedEntityId in appointmentsDump.keys():
            caseId = appointmentsDump[issuedEntityId]['caseId']
            
            if caseId in customerAppCases.keys():
                return 'CUSTOMER_APP'
                
            user = appointmentsDump[issuedEntityId]['user']
            
            return user
            
    else:
        #print(issuedEntityId,loyaltyDump[issuedEntityId])
        if issuedEntityId in loyaltyDump.keys():
            return loyaltyDump[issuedEntityId]
    #print(issuedEntityId, issuedEntityType)        
    return 'NA'
    
    
def getResponseDetails(number, waExDump, msgSentOn, ifInFirstFlag, ifInSecondFlag, firstDump, secondDump, ifInThirdFlag, ifInFourthFlag, thirdDump, fourthDump):
    
    
    if number in waExDump.keys():
        dump = waExDump[number]
        
        for call in dump:
            
            if call['createdOn'] > msgSentOn:
                if ifInFirstFlag:
                    for row1 in firstDump:
                        if row1['msgSentOn'] > call['createdOn']: #Msg from first dump is after tha call
                            break
                        
                        if row1['msgSentOn'] > msgSentOn:
                            return 'No', ''
                            
                if ifInSecondFlag:
                    for row2 in secondDump:
                        if row2['msgSentOn'] > call['createdOn']: #Msg from first dump is after tha call
                            break
                        
                        if row2['msgSentOn'] > msgSentOn:
                            return 'No', ''
                            
                if ifInThirdFlag:
                    for row3 in thirdDump:
                        if row3['msgSentOn'] > call['createdOn']: #Msg from first dump is after tha call
                            break
                        
                        if row3['msgSentOn'] > msgSentOn:
                            return 'No', ''
                            
                if ifInFourthFlag:
                    for row4 in fourthDump:
                        if row4['msgSentOn'] > call['createdOn']: #Msg from first dump is after tha call
                            break
                        
                        if row4['msgSentOn'] > msgSentOn:
                            return 'No', ''
                
                
                return 'Yes', call['callType']
                
    return 'No', ''
    
    
def getayuCashUtilizedDetails(number, msgSentOn, ayuCashDebited, ifInFirstFlag, ifInSecondFlag, firstDump, secondDump, ifInThirdFlag, ifInFourthFlag, thirdDump, fourthDump):
    
    if number in ayuCashDebited.keys():
        dump = ayuCashDebited[number]
        for val in dump:
        
            if val['createdOn'] > msgSentOn:
                if ifInFirstFlag:
                    for row1 in firstDump:
                        if row1['msgSentOn'] > val['createdOn']: #Msg from first dump is after tha call
                            break
                        
                        if row1['msgSentOn'] > msgSentOn:
                            return 'No', '', ''
                            
                if ifInSecondFlag:
                    for row2 in secondDump:
                        if row2['msgSentOn'] > val['createdOn']: #Msg from first dump is after tha call
                            break
                        
                        if row2['msgSentOn'] > msgSentOn:
                            return 'No', '', ''
                            
                if ifInThirdFlag:
                    for row3 in thirdDump:
                        if row3['msgSentOn'] > val['createdOn']: #Msg from first dump is after tha call
                            break
                        
                        if row3['msgSentOn'] > msgSentOn:
                            return 'No', '', ''
                            
                if ifInFourthFlag:
                    for row4 in fourthDump:
                        if row4['msgSentOn'] > val['createdOn']: #Msg from first dump is after tha call
                            break
                        
                        if row4['msgSentOn'] > msgSentOn:
                            return 'No', '', ''
                            
                return 'Yes', val['issuedEntityType'], val['appointmentCreationType']
            
    return 'No', '', ''
    
def response_from_wa_ex_and_ayu_cash_utilization(number, waExDump, msgSentOn, first, second, ayuCashDebited, third, fourth):
    
    ifInFirstFlag = False
    firstDump = []
    if number in first.keys():
        ifInFirstFlag = True
        firstDump = first[number]
        
    secondDump = []    
    ifInSecondFlag = False
    if number in second.keys():
        ifInSecondFlag = True
        secondDump = second[number]
        
    thirdDump = []    
    ifInThirdFlag = False
    if number in third.keys():
        ifInThirdFlag = True
        thirdDump = third[number]
        
    fourthDump = []    
    ifInFourthFlag = False
    if number in fourth.keys():
        ifInFourthFlag = True
        fourthDump = fourth[number]
        
    responseBack, responseThrough = getResponseDetails(number, waExDump, msgSentOn, ifInFirstFlag, ifInSecondFlag, firstDump, secondDump, ifInThirdFlag, ifInFourthFlag, thirdDump, fourthDump)
    
    utilized, usedOn, appointmentCreationType = getayuCashUtilizedDetails(number, msgSentOn, ayuCashDebited, ifInFirstFlag, ifInSecondFlag, firstDump, secondDump, ifInThirdFlag, ifInFourthFlag, thirdDump, fourthDump)            
                
    return responseBack, responseThrough, utilized, usedOn, appointmentCreationType


def wa_dir(toMessage,fromId):
    if toMessage==fromId:
        return 'Inbound'
    else:
        return 'Outbound'
    
def getayuCashUtilized(Contact_Number, msgSentOn, ayuCashDebited):
    
    if Contact_Number in ayuCashDebited.keys():
        dump = ayuCashDebited[Contact_Number]
        for val in dump:
        
            if val['createdOn'] > msgSentOn:
                return 'Yes', val['issuedEntityType'], val['appointmentCreationType']
            
    return 'No', '', ''
    
def getayuCashUtilizedPromotional(Contact_Number, msgSentOn, ayuCashDebited, customerId, commsDump):
    
    # Adding Check if the utilized ayu cash is from promotional msg or nudging msg
    ifNudgingSent = False
    if customerId in commsDump.keys():
        ifNudgingSent = True
        nudgingDump = commsDump[customerId]
        
    if Contact_Number in ayuCashDebited.keys():
        dump = ayuCashDebited[Contact_Number]
        for val in dump:
        
            if val['createdOn'] > msgSentOn:
                if ifNudgingSent:
                    for row in nudgingDump:
                        if row['msgSentOn'] > val['createdOn']:
                            break
                        
                        if row['msgSentOn'] > msgSentOn:
                            return 'No', '', ''
                            
                return 'Yes', val['issuedEntityType'], val['appointmentCreationType']
            
    return 'No', '', ''

def prepare_table2(comms_logs, table3):
    
    table2 = [[0 for x in range(7)] for x in range(6)]
    
    table2[0][0] = 'Msg Sent'
    table2[1][0] = 'Response from Cx'
    table2[2][0] = 'Cash utilized'
    table2[3][0] = 'Unique Customers'
    table2[5][0] = '% Conversion'
    table2[4][0] = '% Response'
    
    customerAlreadyConsidered = {}
    customerAlreadyConsideredYest = {}
    customerAlreadyConsideredLm = {}
    print(comms_logs.columns)
    for index, val in comms_logs.iterrows():
        
        
        if val['ayuCashNudgingFlow'] in ('APP_DONE', 'APP_CONFIRM_CANCEL', 'APP_CANCEL'):
            colIndex = 2
        else:
            colIndex = 1
        
        
        if val['dateFlag'] == 'Yest' :   
            table2[0][colIndex] += 1
            if val['responded'] == 'Yes':
                table2[1][colIndex] += 1
        
            if val['utilized'] == 'Yes':
                table2[2][colIndex] += 1
                table3[5][1] += 1
                
                if val['usedOn'] == 'APPOINTMENT':
                    if val['appointmentCreationType'] != 'FOLLOWUP_APPOINTMENT':
                        table3[0][1] += 1
                    else:    
                        table3[1][1] += 1
                elif val['usedOn'] == 'DIAGNOSTICS_APPOINTMENT':
                    table3[2][1] += 1
                elif val['usedOn'] == 'LOYALTY_CARD':
                    table3[3][1] += 1
                elif val['usedOn'] == 'PATIENT_SURGERY':
                    table3[4][1] += 1
                    
            if val['patientId'] not in customerAlreadyConsideredYest.keys():
                customerAlreadyConsideredYest[val['patientId']] = 'Used'
                table2[3][colIndex] += 1
        
        if str(val['month']) == thismonth:    
            table2[0][colIndex+2] += 1
            if val['responded'] == 'Yes':
                table2[1][colIndex+2] += 1
            
            if val['utilized'] == 'Yes':
                table2[2][colIndex+2] += 1
                table3[5][2] += 1
                if val['usedOn'] == 'APPOINTMENT':
                    if val['appointmentCreationType'] != 'FOLLOWUP_APPOINTMENT':
                        table3[0][2] += 1
                    else:
                        table3[1][2] += 1
                elif val['usedOn'] == 'DIAGNOSTICS_APPOINTMENT':
                    table3[2][2] += 1
                elif val['usedOn'] == 'LOYALTY_CARD':
                    table3[3][2] += 1
                elif val['usedOn'] == 'PATIENT_SURGERY':
                    table3[4][2] += 1
                
            if val['patientId'] not in customerAlreadyConsidered.keys():
                customerAlreadyConsidered[val['patientId']] = 'Used'
                table2[3][colIndex+2] += 1
                
        if str(val['month']) == lastmonth:    
            table2[0][colIndex+4] += 1
            if val['responded'] == 'Yes':
                table2[1][colIndex+4] += 1
            
            if val['utilized'] == 'Yes':
                table2[2][colIndex+4] += 1
                table3[5][3] += 1
                if val['usedOn'] == 'APPOINTMENT':
                    if val['appointmentCreationType'] != 'FOLLOWUP_APPOINTMENT':
                        table3[0][3] += 1
                    else:
                        table3[1][3] += 1
                elif val['usedOn'] == 'DIAGNOSTICS_APPOINTMENT':
                    table3[2][3] += 1
                elif val['usedOn'] == 'LOYALTY_CARD':
                    table3[3][3] += 1
                elif val['usedOn'] == 'PATIENT_SURGERY':
                    table3[4][3] += 1
                
            if val['patientId'] not in customerAlreadyConsideredLm.keys():
                customerAlreadyConsideredLm[val['patientId']] = 'Used'
                table2[3][colIndex+4] += 1
            
            
    for j in range(1,7):
        table2[4][j] = round(((table2[1][j] * 100)/ table2[3][j]),2) if table2[3][j] != 0 else 0
        table2[5][j] = round(((table2[2][j] * 100)/ table2[3][j]),2) if table2[3][j] != 0 else 0
        
    
    resultC = result_base3
    resultC = summarize_table(6, 7, table2, resultC)
    
    return resultC, table3
    
def prepare_table1(ayu_cash_nudge, table3):
    
    table1 = [[0 for x in range(4)] for x in range(6)]
    
    table1[0][0] = 'Msg Sent'
    table1[1][0] = 'Response from Cx'
    table1[2][0] = 'Cash utilized'
    table1[3][0] = 'Unique Customers'
    table1[5][0] = '% Conversion'
    table1[4][0] = '% Response'
    
            
    customerAlreadyConsidered = {}
    customerAlreadyConsideredYest = {}
    customerAlreadyConsideredLm = {}
    for index, val in ayu_cash_nudge.iterrows():
        
        if val['dateFlag'] == 'Yest':
            table1[0][1] += 1
        
            if val['responded'] == 'Yes':
                table1[1][1] += 1
        
            if val['utilized'] == 'Yes':
                table1[2][1] += 1
                table3[5][4] += 1
                
                if val['usedOn'] == 'APPOINTMENT':
                    #print(val['appointmentCreationType'])
                    if val['appointmentCreationType1'] != 'FOLLOWUP_APPOINTMENT':
                        table3[0][4] += 1
                    else:
                        table3[1][4] += 1
                elif val['usedOn'] == 'DIAGNOSTICS_APPOINTMENT':
                    table3[2][4] += 1
                elif val['usedOn'] == 'LOYALTY_CARD':
                    table3[3][4] += 1
                elif val['usedOn'] == 'PATIENT_SURGERY':
                    table3[4][4] += 1
            
            if val['customerId'] not in customerAlreadyConsideredYest.keys():
                customerAlreadyConsideredYest[val['customerId']] = 'Used'
                table1[3][1] += 1    
        
        if str(val['month']) == thismonth:            
            table1[0][2] += 1
            
            if val['responded'] == 'Yes':
                table1[1][2] += 1
            
            if val['utilized'] == 'Yes':
                table1[2][2] += 1
                table3[5][5] += 1
                
                if val['usedOn'] == 'APPOINTMENT':
                    #print(val)
                    if val['appointmentCreationType1'] != 'FOLLOWUP_APPOINTMENT':
                        table3[0][5] += 1
                    else:
                        table3[1][5] += 1
                elif val['usedOn'] == 'DIAGNOSTICS_APPOINTMENT':
                    table3[2][5] += 1
                elif val['usedOn'] == 'LOYALTY_CARD':
                    table3[3][5] += 1
                elif val['usedOn'] == 'PATIENT_SURGERY':
                    table3[4][5] += 1
                
            if val['customerId'] not in customerAlreadyConsidered.keys():
                customerAlreadyConsidered[val['customerId']] = 'Used'
                table1[3][2] += 1
                
        if str(val['month']) == lastmonth:            
            table1[0][3] += 1
            
            if val['responded'] == 'Yes':
                table1[1][3] += 1
            
            if val['utilized'] == 'Yes':
                table1[2][3] += 1
                table3[5][6] += 1
                
                if val['usedOn'] == 'APPOINTMENT':
                    #print(val)
                    if val['appointmentCreationType1'] != 'FOLLOWUP_APPOINTMENT':
                        table3[0][6] += 1
                    else:
                        table3[1][6] += 1
                elif val['usedOn'] == 'DIAGNOSTICS_APPOINTMENT':
                    table3[2][6] += 1
                elif val['usedOn'] == 'LOYALTY_CARD':
                    table3[3][6] += 1
                elif val['usedOn'] == 'PATIENT_SURGERY':
                    table3[4][6] += 1
                
            if val['customerId'] not in customerAlreadyConsideredLm.keys():
                customerAlreadyConsideredLm[val['customerId']] = 'Used'
                table1[3][3] += 1
            
    for j in range(1,4):
        table1[4][j] = round(((table1[1][j] * 100)/ table1[3][j]),2) if table1[3][j] != 0 else 0
        table1[5][j] = round(((table1[2][j] * 100)/ table1[3][j]),2) if table1[3][j] != 0 else 0
        
    resultB = result_base2
    resultB = summarize_table(6, 4, table1, resultB)
    
    
    return resultB, table3
    
def prepare_table4(comms_logs_diagnostics_test, comms_logs_homepick):
    
    table1 = [[0 for x in range(7)] for x in range(6)]
    
    table1[0][0] = 'Msg Sent'
    table1[1][0] = 'Response from Cx'
    table1[2][0] = 'Cash utilized'
    table1[3][0] = 'Unique Customers'
    table1[5][0] = '% Conversion'
    table1[4][0] = '% Response'
    
            
    customerAlreadyConsidered = {}
    customerAlreadyConsideredYest = {}
    customerAlreadyConsideredLm = {}
    
    for index, val in comms_logs_diagnostics_test.iterrows():
        
        if val['dateFlag'] == 'Yest':
            table1[0][1] += 1
        
            if val['responded'] == 'Yes':
                table1[1][1] += 1
        
            if val['utilized'] == 'Yes':
                table1[2][1] += 1
                
            
            if val['customerId'] not in customerAlreadyConsideredYest.keys():
                customerAlreadyConsideredYest[val['customerId']] = 'Used'
                table1[3][1] += 1    
        
        if str(val['month']) == thismonth:            
            table1[0][2] += 1
            
            if val['responded'] == 'Yes':
                table1[1][2] += 1
            
            if val['utilized'] == 'Yes':
                table1[2][2] += 1
                
                
            if val['customerId'] not in customerAlreadyConsidered.keys():
                customerAlreadyConsidered[val['customerId']] = 'Used'
                table1[3][2] += 1
                
        if str(val['month']) == lastmonth:            
            table1[0][3] += 1
            
            if val['responded'] == 'Yes':
                table1[1][3] += 1
            
            if val['utilized'] == 'Yes':
                table1[2][3] += 1
                
            if val['customerId'] not in customerAlreadyConsideredLm.keys():
                customerAlreadyConsideredLm[val['customerId']] = 'Used'
                table1[3][3] += 1
                
    customerAlreadyConsidered = {}
    customerAlreadyConsideredYest = {}
    customerAlreadyConsideredLm = {}
    
    for index, val in comms_logs_homepick.iterrows():
        
        if val['dateFlag'] == 'Yest':
            table1[0][4] += 1
        
            if val['responded'] == 'Yes':
                table1[1][4] += 1
        
            if val['utilized'] == 'Yes':
                table1[2][4] += 1
                
            
            if val['customerId'] not in customerAlreadyConsideredYest.keys():
                customerAlreadyConsideredYest[val['customerId']] = 'Used'
                table1[3][4] += 1    
        
        if str(val['month']) == thismonth:            
            table1[0][5] += 1
            
            if val['responded'] == 'Yes':
                table1[1][5] += 1
            
            if val['utilized'] == 'Yes':
                table1[2][5] += 1
                
                
            if val['customerId'] not in customerAlreadyConsidered.keys():
                customerAlreadyConsidered[val['customerId']] = 'Used'
                table1[3][5] += 1
                
        if str(val['month']) == lastmonth:            
            table1[0][6] += 1
            
            if val['responded'] == 'Yes':
                table1[1][6] += 1
            
            if val['utilized'] == 'Yes':
                table1[2][6] += 1
                
            if val['customerId'] not in customerAlreadyConsideredLm.keys():
                customerAlreadyConsideredLm[val['customerId']] = 'Used'
                table1[3][6] += 1
            
    for j in range(1,7):
        table1[4][j] = round(((table1[1][j] * 100)/ table1[3][j]),2) if table1[3][j] != 0 else 0
        table1[5][j] = round(((table1[2][j] * 100)/ table1[3][j]),2) if table1[3][j] != 0 else 0
        
    resultF = result_base7
    resultF = summarize_table(6, 7, table1, resultF)
    
    
    return resultF
    
    
#[{"additionalDetails->>'$.ayuCashNudgingFlow'": 'APP_DONE'}, {"additionalDetails->>'$.ayuCashNudgingFlow'": 'CASE_CANCEL'}, {"additionalDetails->>'$.ayuCashNudgingFlow'": 'CASE_CLOSED'}, {"additionalDetails->>'$.ayuCashNudgingFlow'": 'APP_CONFIRM_CANCEL'}, {"additionalDetails->>'$.ayuCashNudgingFlow'": 'APP_CANCEL'}]    
#[{'issuedEntityType': 'PATIENT_SURGERY'}, {'issuedEntityType': 'APPOINTMENT'}, {'issuedEntityType': 'DIAGNOSTICS_APPOINTMENT'}, {'issuedEntityType': 'LOYALTY_CARD'}]
    
    
def getPaymentTag(issuedEntityId, transactionType, issuedEntityType, appPayDump, loyaltyPayDump):
    
    if transactionType == 'CREDITED':
        return 'NA', ''
        
    if issuedEntityType == 'PATIENT_SURGERY':
        return 'NA', ''
        
    if issuedEntityType in ('APPOINTMENT', 'DIAGNOSTICS_APPOINTMENT'):
        if issuedEntityId in appPayDump.keys():
            return appPayDump[issuedEntityId]['paymentTag'], appPayDump[issuedEntityId]['appointmentCreationType']
    elif issuedEntityType == 'LOYALTY_CARD':
        if issuedEntityId in loyaltyPayDump.keys():
            return loyaltyPayDump[issuedEntityId], ''
            
    return 'Partial_Payment', ''

promotionalAyuCashAmountMapping = {
    '100': 1,
    '300': 2,
    '500': 3,
    '1000': 4
}

def prepare_table3(ayuCashPromotional_Once, table3):
    
    table1 = [[0 for x in range(9)] for x in range(6)]
    
    table1[0][0] = 'Msg Sent'
    table1[1][0] = 'Response from Cx'
    table1[2][0] = 'Cash utilized'
    table1[3][0] = 'Unique Customers'
    table1[5][0] = '% Conversion'
    table1[4][0] = '% Response'
    
            
    customerAlreadyConsidered = {}
    customerAlreadyConsideredYest = {}
    customerAlreadyConsideredLm = {}
    for index, val in ayuCashPromotional_Once.iterrows():
        
        colIndex = promotionalAyuCashAmountMapping[str(val['promotionalAyuCashAmount'])]
        '''
        if val['dateFlag'] == 'Yest/Mtd':
            table1[0][colIndex] += 1
        
            if val['responded'] == 'Yes':
                table1[1][colIndex] += 1
        
            if val['utilized'] == 'Yes':
                table1[2][colIndex] += 1
                table3[5][5] += 1
                
                if val['usedOn'] == 'APPOINTMENT':
                    
                    if val['appointmentCreationType1'] != 'FOLLOWUP_APPOINTMENT':
                        table3[0][5] += 1
                    else:
                        table3[1][5] += 1
                elif val['usedOn'] == 'DIAGNOSTICS_APPOINTMENT':
                    table3[2][5] += 1
                elif val['usedOn'] == 'LOYALTY_CARD':
                    table3[3][5] += 1
                elif val['usedOn'] == 'PATIENT_SURGERY':
                    table3[4][5] += 1
            
            if val['customerId'] not in customerAlreadyConsideredYest.keys():
                customerAlreadyConsideredYest[val['customerId']] = 'Used'
                table1[3][colIndex] += 1    
        '''
        if str(val['month']) == lastmonth:            
            table1[0][colIndex+4] += 1
            
            if val['responded'] == 'Yes':
                table1[1][colIndex+4] += 1
            
            if val['utilized'] == 'Yes':
                table1[2][colIndex+4] += 1
                table3[5][8] += 1
                
                if val['usedOn'] == 'APPOINTMENT':
                    #print(val)
                    if val['appointmentCreationType1'] != 'FOLLOWUP_APPOINTMENT':
                        table3[0][8] += 1
                    else:
                        table3[1][8] += 1
                elif val['usedOn'] == 'DIAGNOSTICS_APPOINTMENT':
                    table3[2][8] += 1
                elif val['usedOn'] == 'LOYALTY_CARD':
                    table3[3][8] += 1
                elif val['usedOn'] == 'PATIENT_SURGERY':
                    table3[4][8] += 1
                
            if val['Contact_Number'] not in customerAlreadyConsideredLm.keys():
                customerAlreadyConsideredLm[val['Contact_Number']] = 'Used'
                table1[3][colIndex+4] += 1
        
        if str(val['month']) == thismonth:
            table1[0][colIndex] += 1
                
            if val['responded'] == 'Yes':
                table1[1][colIndex] += 1
                
            if val['utilized'] == 'Yes':
                table1[2][colIndex] += 1
                table3[5][7] += 1
                    
                if val['usedOn'] == 'APPOINTMENT':
                    #print(val)
                    if val['appointmentCreationType1'] != 'FOLLOWUP_APPOINTMENT':
                        table3[0][7] += 1
                    else:
                        table3[1][7] += 1
                elif val['usedOn'] == 'DIAGNOSTICS_APPOINTMENT':
                    table3[2][7] += 1
                elif val['usedOn'] == 'LOYALTY_CARD':
                    table3[3][7] += 1
                elif val['usedOn'] == 'PATIENT_SURGERY':
                    table3[4][7] += 1
                   
            if val['Contact_Number'] not in customerAlreadyConsidered.keys():
                customerAlreadyConsidered[val['Contact_Number']] = 'Used'
                table1[3][colIndex] += 1
            
    for j in range(1,9):
        table1[4][j] = round(((table1[1][j] * 100)/ table1[3][j]),2) if table1[3][j] != 0 else 0
        table1[5][j] = round(((table1[2][j] * 100)/ table1[3][j]),2) if table1[3][j] != 0 else 0
        
    resultE = result_base6
    resultE = summarize_table(6, 9, table1, resultE)
    
    return resultE, table3
    

def lambda_handler(event, context):
    try:
        read_host, read_port, read_database = make_db_params(**{'init_from': 'script', 'db_url': READ_DB_DOMAIN_URL})
        read_connection_obj = InitDatabaseConnetion(db_url=read_host, port=int(read_port), username=READ_DB_USER,
                                                    password=READ_DB_PASSWORD, db_name=read_database)
                                                    
        
        query = """ SELECT *,
                  CASE
                    WHEN patientId IN ( SELECT DISTINCT patientId FROM `gmb-centralisation.tables_crons.transactions_monthly` 
                    WHERE paymentMode = 'AYU_CASH' ) THEN 'Ayu-cash'
                  ELSE
                  'non-Ayu-cash'
                END
                  AS userType,
                  EXTRACT(MONTH
                  FROM
                    createdOn) AS month,
                  CAST(week AS INTEGER) AS weekNo
                FROM
                  `gmb-centralisation.tables_crons.transactions_monthly` """
        
        
        data1 = getdatafrombigq(query)
        data1.to_csv(fpath5,compression = 'gzip')
        ayu_dict = {}
        ayu_dict['month'] = {}
        
        ayu_dict['week'] = {}
        ayu_dict['month']['ayu_cash'] = {}
        ayu_dict['month']['non_ayu'] = {}
        ayu_dict['week']['ayu_cash'] = {}
        ayu_dict['week']['non_ayu'] = {}
        ayu_dict['month']['overall'] = {}
        ayu_dict['week']['overall'] = {}
        
        mnt_list1 = {}
        mnt_list2 = {}
        week_list1 = {}
        week_list2  = {}
        monthlist = {}
        weeklist = {}
        

        dataSets = fetch_data(read_connection_obj)
        ayu_cash = pd.DataFrame(dataSets['ayu_cash'])
        appointments = dataSets['appointments']
        loyalty = dataSets['loyalty']
        ayuMitra = dataSets['ayuMitra']
        agents = dataSets['agents']
        cases = dataSets['cases']
        app_pay = dataSets['app_pay']
        loyalty_pay = dataSets['loyalty_pay']
        
        appPayDump = {}
        for val in app_pay:
            appId = str(val['appId'])
            if appId not in appPayDump.keys():
                appPayDump[appId] = {}
                appPayDump[appId]['paymentTag'] = 'Partial_Payment'
                appPayDump[appId]['appointmentCreationType'] = val['appointmentCreationType']
                if float(val['consultationFee']) == float(val['amountPaid']):
                    appPayDump[appId]['paymentTag'] = 'Full_Payment'
                    
        loyaltyPayDump = {}
        for val in loyalty_pay:
            cardId = str(val['cardId'])
            if cardId not in loyaltyPayDump.keys():
                #loyaltyPayDump[cardId] = 'Partial_Payment'
                #if float(500) == float(val['amountPaid']):
                loyaltyPayDump[cardId] = 'Full_Payment'
                
        
        customerAppCases = {}
        for val in cases:
            customerAppCases[str(val['caseId'])] = 'Used'
            
        ayuMitraDump = {}
        for val in ayuMitra:
            ayuMitraDump[val['ayuMitraEmail']] = 'Used'
            
        agentsDump = {}
        for val in agents:
            agentsDump[val['email']] = 'Used'
            
            
        appointmentsDump = {}
        for val in appointments:
            appId = str(val['id'])
            if appId not in appointmentsDump.keys():
                appointmentsDump[appId] = {}
                appointmentsDump[appId]['caseId'] = str(val['leadId'])
                appointmentsDump[appId]['consultationType'] = val['consultationType']
                if val['user'] in ayuMitraDump.keys():
                    appointmentsDump[appId]['user'] = 'AYU_MITRA'
                else:
                    appointmentsDump[appId]['user'] = 'CS_AGENT'
                
        loyaltyDump = {}
        for val in loyalty:
            cardId = str(val['cardId'])
            if cardId not in loyaltyDump.keys():
                if val['collectedBy'] in ayuMitraDump.keys():
                    loyaltyDump[cardId] = 'AYU_MITRA'
                else:
                    loyaltyDump[cardId] = 'CS_AGENT'
        
        #print(loyaltyDump)    
        if len(ayu_cash)>0:
            ayu_cash['tag'] = ayu_cash.apply(lambda x: getTag(str(x['issuedEntityId']), x['transactionType'], x['issuedEntityType'], appointmentsDump, loyaltyDump, customerAppCases), axis=1)
            ayu_cash['Contact_Number'] =  ayu_cash.apply(lambda x: msg_to(x['customerNumber']),axis=1)
            df =  ayu_cash.apply(lambda x: getPaymentTag(str(x['issuedEntityId']), x['transactionType'], x['issuedEntityType'], appPayDump, loyaltyPayDump),axis=1, result_type='expand')
            ayu_cash = pd.concat([ayu_cash, df], axis=1)
            ayu_cash = ayu_cash.rename(columns={0: 'paymentTag', 1: 'appointmentCreationType'})
            
        ayu_cash_debited = ayu_cash[ayu_cash['transactionType'] == 'DEBITED']
        ayu_cash_credited = ayu_cash[ayu_cash['transactionType'] == 'CREDITED']
        cashbackType = dataSets['cashbackType']
        
        cashbackTypeDump = {}
        for val in cashbackType:
            cashbackTypeDump[str(val['cashbackId'])] = val['cashbackType']
        
        
        if len(ayu_cash_credited):
            ayu_cash_credited['cashbackTypeMapped'] = ayu_cash_credited.apply(lambda x: getCashBackType(str(x['cashbackType']), cashbackTypeDump), axis=1)
            ayu_cash_nudge = ayu_cash_credited[ayu_cash_credited['cashbackType'] == 6]
            
        result, ayuCashDebited = prepare_table(ayu_cash_debited, ayu_cash_credited)
        ayu_cash.to_csv(fpath, compression='gzip')
        
        #-------  Nudging Summary --------
        
        comms_logs = pd.DataFrame(dataSets['comms_logs'])
        comms_logs_diag_hp = pd.DataFrame(dataSets['comms_logs_diag_hp'])
        
        exotel = pd.DataFrame(dataSets['exotel'])
        whatsapp = pd.DataFrame(dataSets['whatsapp'])
        ayuCashPromotional_Once = pd.DataFrame(dataSets['ayuCashPromotional_Once'])
        
        if len(exotel) > 0:
            exotel['cxNo'] = exotel.apply(lambda x: exotel_cxNo(x['direction'], x['fromId'], x['toMessage']), axis=1)
        
        if len(whatsapp) > 0:
            whatsapp['cxNo'] = whatsapp.apply(lambda x: msg_to(x['toMessage']), axis=1)
            #whatsapp['direction'] = whatsapp.apply(lambda x:wa_dir(x['toMessage'], x['fromId']) ,axis=1)
            
        wa_ex = pd.concat([exotel, whatsapp])
        wa_ex = wa_ex.sort_values(by = ['createdOn'], ascending=True)
        
        waExDump = {}
        for index, val in wa_ex.iterrows():
            if val['cxNo'] not in waExDump.keys():
                waExDump[val['cxNo']] = []
            waExDump[val['cxNo']].append(val)
        
        if len(comms_logs):
            comms_logs['Contact_Number'] =  comms_logs.apply(lambda x: msg_to(x['customerNumber']),axis=1)
            
        if len(comms_logs_diag_hp):
            comms_logs_diag_hp['Contact_Number'] =  comms_logs_diag_hp.apply(lambda x: msg_to(x['customerNumber']),axis=1)
            
        if len(ayuCashPromotional_Once):
            ayuCashPromotional_Once['Contact_Number'] =  ayuCashPromotional_Once.apply(lambda x: msg_to(x['patientNumber']),axis=1)
            
        commsDump = {}
        for index, val in comms_logs.iterrows():
            if val['Contact_Number'] not in commsDump.keys():
                commsDump[val['Contact_Number']] = []
            
            commsDump[val['Contact_Number']].append(val)
            
        ayuCashPromoFirst = {}
        for index, val in ayu_cash_nudge.iterrows():
            if val['Contact_Number'] not in ayuCashPromoFirst.keys():
                ayuCashPromoFirst[val['Contact_Number']] = []
            
            ayuCashPromoFirst[val['Contact_Number']].append(val)
            
        ayuCashPromoSecond = {}
        for index, val in ayuCashPromotional_Once.iterrows():
            if val['Contact_Number'] not in ayuCashPromoSecond.keys():
                ayuCashPromoSecond[val['Contact_Number']] = []
            
            ayuCashPromoSecond[val['Contact_Number']].append(val)
        
        comms_logs_diagnostics_test = comms_logs_diag_hp[comms_logs_diag_hp['nudgeTag'] == 'Diagnostics_test']
        comms_logs_homepick = comms_logs_diag_hp[comms_logs_diag_hp['nudgeTag'] == 'Home_pick']
        
        commsDumpDiagnosticsTest = {}
        for index, val in comms_logs_diagnostics_test.iterrows():
            if val['Contact_Number'] not in commsDumpDiagnosticsTest.keys():
                commsDumpDiagnosticsTest[val['Contact_Number']] = []
            
            commsDumpDiagnosticsTest[val['Contact_Number']].append(val)
            
        commsDumpHomepick = {}
        for index, val in comms_logs_homepick.iterrows():
            if val['Contact_Number'] not in commsDumpHomepick.keys():
                commsDumpHomepick[val['Contact_Number']] = []
            
            commsDumpHomepick[val['Contact_Number']].append(val)
        
        
        if len(comms_logs):
                
            df = comms_logs.apply(lambda x: response_from_wa_ex_and_ayu_cash_utilization(x['Contact_Number'], waExDump, x['msgSentOn'], ayuCashPromoFirst, ayuCashPromoSecond, ayuCashDebited, commsDumpDiagnosticsTest, commsDumpHomepick),axis=1, result_type='expand')
            comms_logs = pd.concat([comms_logs, df], axis=1)
            comms_logs = comms_logs.rename(columns={0: 'responded', 1: 'responseFrom', 2: 'utilized', 3: 'usedOn', 4: 'appointmentCreationType'})
            
            #df = comms_logs.apply(lambda x: getayuCashUtilized(x['Contact_Number'], x['msgSentOn'], ayuCashDebited),axis=1, result_type='expand')
            #comms_logs = pd.concat([comms_logs, df], axis=1)
            #comms_logs = comms_logs.rename(columns={0: 'utilized', 1: 'usedOn', 2: 'appointmentCreationType'})
        
        
        table3 = [[0 for x in range(9)] for x in range(6)]
        table3[5][0] = 'Grand Total'
        table3[0][0] = 'Appointment (New)'
        table3[1][0] = 'Appointment (FollowUp)'
        table3[2][0] = 'Diagnostics Appointment'
        table3[3][0] = 'Loyalty Card'
        table3[4][0] = 'Surgery'
        
        resultC, table3 = prepare_table2(comms_logs, table3)
        
        
        if len(ayu_cash_nudge) > 0:
            
            df = ayu_cash_nudge.apply(lambda x: response_from_wa_ex_and_ayu_cash_utilization(x['Contact_Number'], waExDump, x['msgSentOn'], commsDump, ayuCashPromoSecond, ayuCashDebited, commsDumpDiagnosticsTest, commsDumpHomepick),axis=1, result_type='expand')
            ayu_cash_nudge = pd.concat([ayu_cash_nudge, df], axis=1)
            ayu_cash_nudge = ayu_cash_nudge.rename(columns={0: 'responded', 1: 'responseFrom', 2: 'utilized', 3: 'usedOn', 4: 'appointmentCreationType1'})
            
            #df = ayu_cash_nudge.apply(lambda x: getayuCashUtilizedPromotional(x['Contact_Number'], x['createdOn'], ayuCashDebited, x['customerId'], commsDump),axis=1, result_type='expand')
            #ayu_cash_nudge = pd.concat([ayu_cash_nudge, df], axis=1)
            #ayu_cash_nudge = ayu_cash_nudge.rename(columns={0: 'utilized', 1: 'usedOn', 2: 'appointmentCreationType1'})
        
        resultB, table3 = prepare_table1(ayu_cash_nudge, table3)
        
        if len(ayuCashPromotional_Once) > 0:
            
            df = ayuCashPromotional_Once.apply(lambda x: response_from_wa_ex_and_ayu_cash_utilization(x['Contact_Number'], waExDump, x['msgSentOn'], commsDump, ayuCashPromoFirst, ayuCashDebited, commsDumpDiagnosticsTest, commsDumpHomepick),axis=1, result_type='expand')
            ayuCashPromotional_Once = pd.concat([ayuCashPromotional_Once, df], axis=1)
            ayuCashPromotional_Once = ayuCashPromotional_Once.rename(columns={0: 'responded', 1: 'responseFrom', 2: 'utilized', 3: 'usedOn', 4: 'appointmentCreationType1'})
        
        resultD, table3 = prepare_table3(ayuCashPromotional_Once, table3)
        resultE = result_base4
        resultE = summarize_table(6, 9, table3, resultE)
        
        if len(comms_logs_diagnostics_test) > 0:
            
            df = comms_logs_diagnostics_test.apply(lambda x: response_from_wa_ex_and_ayu_cash_utilization(x['Contact_Number'], waExDump, x['msgSentOn'], commsDump, ayuCashPromoFirst, ayuCashDebited, ayuCashPromoSecond, commsDumpHomepick),axis=1, result_type='expand')
            comms_logs_diagnostics_test = pd.concat([comms_logs_diagnostics_test, df], axis=1)
            comms_logs_diagnostics_test = comms_logs_diagnostics_test.rename(columns={0: 'responded', 1: 'responseFrom', 2: 'utilized', 3: 'usedOn', 4: 'appointmentCreationType1'})
        
        
        if len(comms_logs_homepick) > 0:
            
            df = comms_logs_homepick.apply(lambda x: response_from_wa_ex_and_ayu_cash_utilization(x['Contact_Number'], waExDump, x['msgSentOn'], commsDump, ayuCashPromoFirst, ayuCashDebited, ayuCashPromoSecond, commsDumpDiagnosticsTest),axis=1, result_type='expand')
            comms_logs_homepick = pd.concat([comms_logs_homepick, df], axis=1)
            comms_logs_homepick = comms_logs_homepick.rename(columns={0: 'responded', 1: 'responseFrom', 2: 'utilized', 3: 'usedOn', 4: 'appointmentCreationType1'})
        
        resultF = prepare_table4(comms_logs_diagnostics_test, comms_logs_homepick)
        resultE = result_base4
        resultE = summarize_table(6, 9, table3, resultE)
        
        ayu_cash_nudge.to_csv(fpath1, compression='gzip')
        comms_logs.to_csv(fpath2, compression='gzip')
        ayuCashPromotional_Once.to_csv(fpath3, compression='gzip')
        comms_logs_diagnostics_test.to_csv(fpath4, compression='gzip')
        comms_logs_homepick.to_csv(fpath6, compression='gzip')
        
        fileNamePath = 'reports/ayu_cash_nudge.csv.gz' 
        upload_to_s3("crons-dumps", fileNamePath, fpath1)
        
        fileNamePath = 'reports/comms_logs.csv.gz' 
        upload_to_s3("crons-dumps", fileNamePath, fpath2)
        
        fileNamePath = 'reports/ayu_cash_promo.csv.gz' 
        upload_to_s3("crons-dumps", fileNamePath, fpath3)
        
        fileNamePath = 'reports/comms_logs_diagnostics_test.csv.gz' 
        upload_to_s3("crons-dumps", fileNamePath, fpath4)
        
        fileNamePath = 'reports/comms_logs_homepick.csv.gz' 
        upload_to_s3("crons-dumps", fileNamePath, fpath6)
        
        print("Uploaded to s3")
        
        #-------------- Transaction Per user (monthly and weekly code) ------------
        
        for key,val in data1.iterrows():
            if val['userType'] == 'Ayu-cash':
              
              
                if val['month'] not in ayu_dict['month']['ayu_cash'].keys():
                          
                    ayu_dict['month']['ayu_cash'][val['month']] = {}
                    ayu_dict['month']['ayu_cash'][val['month']]['total_count'] = 1
                    
                    ayu_dict['month']['overall'][val['month']] = {}
                    ayu_dict['month']['overall'][val['month']]['grandtotal'] = 1
                    monthlist[val['month']]=[]
                    if val['patientId'] not in monthlist[val['month']]:
                        monthlist[val['month']].append(val['patientId'])
                        ayu_dict['month']['overall'][val['month']]['unique_count'] = len(monthlist[val['month']])
                        
                    mnt_list1[val['month']]=[]
                    if val['patientId'] not in mnt_list1[val['month']]:
                        mnt_list1[val['month']].append(val['patientId'])
                        ayu_dict['month']['ayu_cash'][val['month']]['unique_count'] = len(mnt_list1[val['month']])
                   
                    
            
                else:
                
                    ayu_dict['month']['ayu_cash'][val['month']]['total_count'] += 1
                    ayu_dict['month']['overall'][val['month']]['grandtotal'] += 1
                
                    if val['patientId'] not in monthlist[val['month']]:
                        monthlist[val['month']].append(val['patientId'])
                        ayu_dict['month']['overall'][val['month']]['unique_count'] = len(monthlist[val['month']])
                    
                
                    if val['patientId'] not in mnt_list1[val['month']]:
                        mnt_list1[val['month']].append(val['patientId'])
                        ayu_dict['month']['ayu_cash'][val['month']]['unique_count'] = len(mnt_list1[val['month']])
                
                if val['weekNo'] not in ayu_dict['week']['ayu_cash'].keys():

                    ayu_dict['week']['ayu_cash'][val['weekNo']] = {}
                    ayu_dict['week']['ayu_cash'][val['weekNo']]['total_count'] = 1
              
                    ayu_dict['week']['overall'][val['weekNo']] = {}
                    ayu_dict['week']['overall'][val['weekNo']]['grandtotal'] = 1
                
                    weeklist[val['weekNo']]=[]
                    if val['patientId'] not in  weeklist[val['weekNo']]:
                        weeklist[val['weekNo']].append(val['patientId'])
                        ayu_dict['week']['overall'][val['weekNo']]['unique_count'] = len(weeklist[val['weekNo']])
                
                    week_list1[val['weekNo']] = []
                    if val['patientId'] not in week_list1[val['weekNo']]:
                        week_list1[val['weekNo']].append(val['patientId'])
                        ayu_dict['week']['ayu_cash'][val['weekNo']]['unique_count'] = len(week_list1[val['weekNo']])
            
                else:
            
                    ayu_dict['week']['overall'][val['weekNo']]['grandtotal'] +=1
                    ayu_dict['week']['ayu_cash'][val['weekNo']]['total_count'] += 1
                
              
                    if val['patientId'] not in week_list1[val['weekNo']]:
                        week_list1[val['weekNo']].append(val['patientId'])
                        ayu_dict['week']['ayu_cash'][val['weekNo']]['unique_count'] = len(week_list1[val['weekNo']])
                
                    if val['patientId'] not in  weeklist[val['weekNo']]:
                        weeklist[val['weekNo']].append(val['patientId'])
                        ayu_dict['week']['overall'][val['weekNo']]['unique_count'] = len(weeklist[val['weekNo']])
            
            else:
                if val['month'] not in ayu_dict['month']['non_ayu'].keys():
                
                    ayu_dict['month']['non_ayu'][val['month']] = {}
                    ayu_dict['month']['non_ayu'][val['month']]['total_count'] = 1
                    ayu_dict['month']['overall'][val['month']] = {}
                    ayu_dict['month']['overall'][val['month']]['grandtotal']=1
                    mnt_list2[val['month']]=[]
                    if val['patientId'] not in mnt_list2[val['month']]:
                        mnt_list2[val['month']].append(val['patientId'])
                        ayu_dict['month']['non_ayu'][val['month']]['unique_count'] = len(mnt_list2[val['month']])
                        
                    monthlist[val['month']]=[]  
                    if val['patientId'] not in monthlist[val['month']]:
                        monthlist[val['month']].append(val['patientId'])
                        ayu_dict['month']['overall'][val['month']]['unique_count'] = len(monthlist[val['month']])
                  
                  
     
                else:
      
                    ayu_dict['month']['non_ayu'][val['month']]['total_count'] += 1
                    ayu_dict['month']['overall'][val['month']]['grandtotal']+=1
                    
                    if val['patientId'] not in mnt_list2[val['month']]:
                        mnt_list2[val['month']].append(val['patientId'])
                        ayu_dict['month']['non_ayu'][val['month']]['unique_count'] =len(mnt_list2[val['month']])
                  
                    if val['patientId'] not in monthlist[val['month']]:
                        monthlist[val['month']].append(val['patientId'])
                        ayu_dict['month']['overall'][val['month']]['unique_count'] = len(monthlist[val['month']])
                
                if val['weekNo'] not in ayu_dict['week']['non_ayu'].keys():
                
                    ayu_dict['week']['non_ayu'][val['weekNo']] = {} 
                    ayu_dict['week']['non_ayu'][val['weekNo']]['total_count'] = 1
                    ayu_dict['week']['overall'][val['weekNo']] = {}
                    ayu_dict['week']['overall'][val['weekNo']]['grandtotal'] = 1
           
                    week_list2[val['weekNo']] = []
                    if val['patientId'] not in week_list2[val['weekNo']]:
                        week_list2[val['weekNo']].append(val['patientId'])
                        ayu_dict['week']['non_ayu'][val['weekNo']]['unique_count'] = len(week_list2[val['weekNo']])
                
                    weeklist[val['weekNo']] =[]
                    if val['patientId'] not in  weeklist[val['weekNo']]:
                        weeklist[val['weekNo']].append(val['patientId'])
                        ayu_dict['week']['overall'][val['weekNo']]['unique_count'] = len(weeklist[val['weekNo']])
                
                
     
                else:
                    ayu_dict['week']['non_ayu'][val['weekNo']]['total_count'] += 1
                    ayu_dict['week']['overall'][val['weekNo']]['grandtotal'] += 1
                
                    if val['patientId'] not in week_list2[val['weekNo']]:
                        week_list2[val['weekNo']].append(val['patientId'])
                        ayu_dict['week']['non_ayu'][val['weekNo']]['unique_count'] = len(week_list2[val['weekNo']])
                    if val['patientId'] not in  weeklist[val['weekNo']]:
                        weeklist[val['weekNo']].append(val['patientId'])
                        ayu_dict['week']['overall'][val['weekNo']]['unique_count'] = len(weeklist[val['weekNo']])
        
        
        final={}
        final['month']={}
        final['month']['ayu_cash'] = {}
        final['week'] = {}
        final['week']['ayu_cash'] = {}
        final['week']['non_cash'] = {}
        final['month']['non_cash']={}
        final['month']['overall'] = {}
        final['week']['overall'] = {}
    
        for key,value in ayu_dict['month']['ayu_cash'].items():
            final['month']['ayu_cash'][key] = round(value['total_count']/value['unique_count'],2)
          
        for key,value in ayu_dict['month']['non_ayu'].items():
            final['month']['non_cash'][key] = round(value['total_count']/value['unique_count'],2)
          
        for key,value in ayu_dict['week']['ayu_cash'].items():
            final['week']['ayu_cash'][key] = round(value['total_count']/value['unique_count'],2)
          
        for key,value in ayu_dict['week']['non_ayu'].items():
            final['week']['non_cash'][key] = round(value['total_count']/value['unique_count'],2)
          
        for key,value in ayu_dict['month']['overall'].items():
            final['month']['overall'][key] = round(value['grandtotal']/value['unique_count'],2)
        for key,value in ayu_dict['week']['overall'].items():
            final['week']['overall'][key] = round(value['grandtotal']/value['unique_count'],2)  
        
        table7 = [[0 for x in range(4)] for x in range(100)]
        table8 = [[0 for x in range(4)] for x in range(100)]
        
        print(final)
          
        #print(ayu_dict)        
        s=''
        count = 0
        month_dict = {}
        week_dict = {}
        counter = 0
        for key,val in final['month'].items():
          for i,j in sorted(val.items()):
            if i not in month_dict.keys():
              month_dict[i] = count
              table7[count][0] = i
              count+=1
              
            if key == 'ayu_cash':
              table7[month_dict[i]][1] = j
            elif key == 'non_cash':
              table7[month_dict[i]][2] = j
            else:
              table7[month_dict[i]][3] = j
        
        for key,val in final['week'].items():
          for i,j in sorted(val.items()):
            if i not in week_dict.keys():
              week_dict[i] = counter
              table8[counter][0] = i
              counter+=1
            if key == 'ayu_cash':
              table8[week_dict[i]][1] = j
            elif key == 'non_cash':
              table8[week_dict[i]][2] = j
            else:
              table8[week_dict[i]][3] = j
              
            
        result11 = calculate_result(result_base5, table7, 0, len(month_dict),4)
        result12 = calculate_result(result_base5, table8, 0, len(week_dict),4)
        result = result + resultB + resultC + resultD + resultF + resultE + result11 + result12
        
        
        
        Subject = "Ayu Cash Utilisation | Summary"
        email_recipient_list = ["arjit@ayu.health", "sumedha@ayu.health", "rohit.chopra@ayu.health", "anshul@ayu.health", "ankur@ayu.health", "namitha@ayu.health", "susmitha@ayu.health", "himesh@ayu.health", "karan@ayu.health"]
        #email_recipient_list = ["ankur@ayu.health"]
        #email_recipient_list = ['srinivasa@ayu.health']   
        send_email(None, email_recipient_list, Subject, None, result, [])
        
        

    except Exception as e: 
        Text = 'Error Running Cron , response Text: ' + str(e)
        Subject = "ayu_cash_summary | Error"
        email_recipient_list = ["analytics@ayu.health"]
        send_email(None, email_recipient_list, Subject, Text, None, [])
        raise e
