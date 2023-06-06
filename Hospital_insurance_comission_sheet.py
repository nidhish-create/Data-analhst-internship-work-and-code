import json
import pdb              
import os
import pytz  
import re  
import numpy as np
import pandas as pd 
from datetime import datetime , timedelta
import numpy as np  
from service.database import InitDatabaseConnetion, make_db_params, fetch_record,DB_Params
import logging
from service.base_functions import msg_to
from datetime import datetime , timedelta   
from service.send_mail_client import send_email
from write_gsheet import clear_and_write_to_sheet, clear_and_write_to_sheet_city

logger = logging.getLogger(__name__)

READ_DB_USER = os.environ.get('READ_DB_USER', '')
READ_DB_PASSWORD = os.environ.get('READ_DB_PASSWORD', '')
READ_DB_DOMAIN_URL = os.environ.get('READ_DB_DOMAIN_URL', '')  
#READ_DB_DOMAIN_URL_BETA = os.environ.get('READ_DB_DOMAIN_URL_BETA', '')
S3_BUCKET_NAME = os.environ.get('S3_BUCKET_NAME', '')
SECRETS_FILE = os.environ.get('SECRETS_FILE', '')

ipd_transactions_admissions = {
    "case": """
            select 
            date_add(ic.createdOn,INTERVAL '5:30' HOUR_MINUTE) case_created_on
            ,concat(year(date_add(ic.createdOn,INTERVAL '5:30' HOUR_MINUTE)), '-', month(date_add(ic.createdOn,INTERVAL '5:30' HOUR_MINUTE))) as case_created_month
            ,ic.id as Caseid
            ,ic.surgeryid as surgeryid
            ,'Non-Ayu' as Source
            ,'Dashboard' as Patient_source
            ,cp.customername as Patient_name
            ,cp.customernumber as Contact_number
            ,cp.customeremail as email_id
            ,f.aliasName as hospital_name
            ,iv.vendorname
            ,ic.status as case_status
            ,ic.insurancename
            ,ic.additionalDetails->>'$.tpaName' as tpaName
            ,ic.treatmentType
            ,ic.surgeryName
            ,ic.specialityName
            ,city.cityName as city
            from insurance_case ic
            left join insurance_vendor iv on ic.vendorid=iv.vendorid
            left join ayu_facility_profile f on f.facilityId=ic.facilityId
            left join ayu_cities city on city.id=f.cityid
            left join patient_profile pp on ic.patientid=pp.id
            left join customer_profile cp on cp.customerid=pp.customerid
            where date_add(ic.createdOn,INTERVAL '5:30' HOUR_MINUTE)>='2022-10-12 00:00:00'
            """  ,
    "ins_doc":"""select ip.phaseId as ins_phaseid
                        ,ip.insuranceCaseId as ins_caseid
                        # ,ip.status as insurance_doc_status
                        # ,ip.Reason as insurance_doc_Reason
                        # ,apd.email as ins_doc_assigned_to
                        # ,ip.additionalDetails as ins_doc_additional_details
                 from insurance_phase  ip
                 left join ayu_personnel_details apd on apd.personnelId=ip.assignedTo and upper(personneltype)='CENTRAL_INSURANCE_TEAM'
                 where upper(phasetype)="INSURANCE_DOCS_APPROVAL"
               """ ,
    "dis_doc":"""select ip1.phaseId as dis_phaseid
                        ,ip1.insuranceCaseId as dis_caseid
                        # ,ip1.status as dischage_doc_status
                        # ,ip1.reason as discharge_doc_reason
                        # ,apd.email as dis_doc_assigned_to
                        # ,ip1.additionalDetails as dis_doc_additional_details
                        ,ip1.additionalDetails->>'$.surgeryDischargeApprovalInfo.nme' as NonMedicalExpenses
                        ,ip1.additionalDetails->>'$.surgeryDischargeApprovalInfo.totalBillAmount' as totalBillAmount 
                        ,ip1.additionalDetails->>'$.surgeryDischargeApprovalInfo.finalApprovedAmount' as finalApprovedAmount
                        # ,ip1.additionalDetails->>'$.surgeryDischargeApprovalInfo.document.docLink' as docLink
                 from insurance_phase  ip1
                 left join ayu_personnel_details apd on apd.personnelId=ip1.assignedTo and upper(personneltype)='CENTRAL_INSURANCE_TEAM'
                 where upper(phasetype)="DISCHARGE_DOCS_APPROVAL"
               """ ,
    "sur_done":"""select ip2.phaseId as sur_phaseid
                        ,ip2.insuranceCaseId as sur_caseid
                        # ,ip2.status as surgery_done_status
                        # ,ip2.reason as Surgery_Done_Reason
                        # ,apd.email as sur_doc_assigned_to
                        # ,ip2.additionalDetails as sur_done_additional_details
                        # ,ip2.additionalDetails->>'$.surgeryDischargeDoneRequest.surgeryName' as surgeryName
                        ,ip2.additionalDetails->>'$.surgeryDischargeDoneRequest.admissionDate' as admissionDate
                        ,ip2.additionalDetails->>'$.surgeryDischargeDoneRequest.dischargeDate' as dischargeDate
                        # ,ip2.additionalDetails->>'$.surgeryDischargeDoneRequest.tentativePromisedPrice' as tentativePromisedPrice
                  from insurance_phase ip2
                  left join ayu_personnel_details apd on apd.personnelId=ip2.assignedTo and upper(personneltype)='CENTRAL_INSURANCE_TEAM'
                  where upper(phasetype)="MARKING_SURGERY_DONE"
                """,
    "fin_elig":"""select isp.subPhaseId
                        ,isp.phaseId as fin_phaseid
                        # ,isp.status as fin_elig_status
                        # ,isp.reason as fin_elig_reason
                        # ,apd.email as fin_elig_assigned_to
                        # ,isp.additionalDetails as fin_elig_additional_details
                        ,il.lenderName
                  from insurance_sub_phase isp
                  left join ayu_personnel_details apd on apd.personnelId=isp.assignedTo and upper(personneltype)='CENTRAL_INSURANCE_TEAM'
                  left join insurance_lender il on il.lenderid=isp.additionalDetails->>'$.financeEligibilityInfo.lenderId'
                  where upper(isp.subphasetype)="FINANCE_ELIGIBILITY_APPROVAL"
                """
}

import gspread
from oauth2client.service_account import ServiceAccountCredentials

def get_df_from_sheet(workbook, sheet):
    scope = ["https://spreadsheets.google.com/feeds","https://www.googleapis.com/auth/spreadsheets","https://www.googleapis.com/auth/drive.file","https://www.googleapis.com/auth/drive"]
    p={
    "type": "service_account",
    "project_id": "commissionreport",
    "private_key_id": "d6a56e6786f4f5f763494baa0d1dc8e0be893d95",
    "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQCxgSB3GubC1ZhX\nCzickNO8326G42SCNzufSCypI1+RZzUtDSdckjj+Uudz+yDes4Q/f/z3adZrRc13\nbTMSfx0XU2LhNHgWDk3EFQrfW1MyYQjcM2+lnqivHTE/fznQv/vGgiyYwrYbqUJf\naCIcQNYQrPDZkAXVkHxDXlssAaIR0JI2POIgKa09uxuiqL7WawPRLb970+2ZO+su\np798vMqxaeywm2dCV2wY9RO9lMIyrvXIBVDRT6QI6QuseMoL2U9UUVgBt0chsMhD\nY1+XxDYsB2s+eYPLd44zGXNZD4Uru5m6sqopWGIuCyx9AXUgvaD2QQmduNRMXqqi\nj5+hMC/HAgMBAAECggEACj6oJ4ercupOCRQSN+MZ/Wgt4mYkammSrN5HCZoLjYJV\n7Nw5/dtIo9EYH7cKTiAxTVLvQD5hDD7ynWZ1kKc6gBqlddcH6Un5Dgyi3IJSWnGE\nR+VGF9qA5N579ay7owM5nlrsBQMT7KPHu2bwtQ2+7SZ2HjB0Tb+xlFfRTkhn2LxG\nvoaxwpEcswn9RGlEwk0q8XR6kT5NazQPpAclSyZKvqdrczIW90JEIOcwT+37VXGu\nDS8Nnq/Uluh2eBBfWfmVvZih09f395P7WvIrItQvyX+r0YqKnfHDe6yt4BA4I3bq\njMuLfn67v7xMzMqluZskPGkT+nnxlH3O1PFrscnnbQKBgQDsk2nhR99FH4lVqzGo\nl2l3hjzQp7TCcUMh0PPyMoKjHGzY7dIkXOwb/gwrqyAWKMEJX+I//wu+kRrMW/tN\ng4Av5TEIplrAegBELYUXdUpJbHNqgjxRvqYClMw+M6arXKCLS4RxQcNog+28eTX0\nYVM/Hzr6+ASF/n7iZPJCmJfl0wKBgQDAFBdOl3Y2KOixIL9kPiGT91PhNbmr2XbZ\n8Q5wCmYnYfJXxYZH6izIu8OQ/l5jL6j91/ikwPnv6I6i52TJ+A728vnFddaQ58k5\nRRt+LWsppdzOIUt/yBjz6ueMeIxV2qzfR+LF8n8zF9rfLFbA7NaDCJP9a0SP7KjF\nLZddSkWRvQKBgF+Hp1mxDBd9hJdzaboKaiw5qJUZI4Tg95rQJbHHc7kp4Uo3voOw\ngidLjt6TW4GXM1v1vAbbloJ9VbTv76p9T2YHxqUXh83xdeoR94xhcH31rSV1MaZQ\ntfiU3WTAtqy72phlBjY1uBKcM4PH7mGga10x3z84p5r0CYih+rGprKzBAoGBAKt0\nQ6m/waFuuucBmFZer5Jo/9LUJjykDVdVudGBNtaIs85tXwPqoLc+A7/1j0NyU6Lj\nmetW5sOkD06SxoESkCkXkqUUHseSXyhj67qhyDqQ95x4U/BoKP3x/WaCZKJuZEma\n3W5cm/Z7oL/90CK+Rm0IxzE7AySF19DKdYasuZTxAoGBAJ1aMb+za19w0u7rW8rm\njxBVibm9xsNWzAn+od1q8DqkMRYKx3ZL2VGMJv7/uS/L+VOyFfRGZorTopcU8tTU\nylozCAV4Ywdr1KybnUb39q0+Xh3vXx/4zI/dP36vrneJkuRLzSnKzIYYdxGkLd+g\nm+maqGn0pKaSnEGemx+ANO6A\n-----END PRIVATE KEY-----\n",
    "client_email": "vivektestapi@commissionreport.iam.gserviceaccount.com",
    "client_id": "111965347606204986243",
    "auth_uri": "https://accounts.google.com/o/oauth2/auth",
    "token_uri": "https://oauth2.googleapis.com/token",
    "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
    "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/vivektestapi%40commissionreport.iam.gserviceaccount.com"
    }

    creds = ServiceAccountCredentials.from_json_keyfile_dict(p,scope)
    client = gspread.authorize(creds)

    sh = client.open(workbook)
    attend_sheet = sh.worksheet(sheet)
    attend_sheet1 = attend_sheet.get_all_values()
    df = pd.DataFrame(attend_sheet1)
    return df

workbook='Hospital Insurance Cases Master Data'
sheet = 'commission_hi'
commission_sheet = get_df_from_sheet(workbook,sheet)

commission_sheet.columns = commission_sheet.iloc[0]
commission_sheet=commission_sheet.reindex(commission_sheet.index.drop(0)) 
commission_sheet.drop(['Hospital Name','Source','Category'],axis=1,inplace=True)

commission_sheet.rename(columns={'Ayu Share':'Ayu Share %'},inplace=True)
commission_sheet.rename(columns={'Net Ayu Share':'Net Ayu Share %'},inplace=True)

workbook='Hospital Insurance Cases Master Data'
sheet = 'Non-Ayu'
NonAyuExcel = get_df_from_sheet(workbook,sheet)

NonAyuExcel.columns = NonAyuExcel.iloc[0]
NonAyuExcel=NonAyuExcel.reindex(NonAyuExcel.index.drop(0))

NonAyuExcel.drop(['Nach Processed Date','Enach done/ not','Discharge Spoc','Intimation Number','Loan ID','Card Number','Policy Number','Policy Holder Name','Company Name','Employee Id (Patient)','Policy Type ','Planned Tentative Date (Only for Planned Cases)','Reason for PDC AYU','Rejection/Case Lost Reason','Cibil Rejected Reason','Cibil Status','KYP Processed By','Sales SPOC','KYP-Spoc','Month','Week','Date Filter'], axis=1, inplace=True)

NonAyuExcel['surgeryName']=[None]*len(NonAyuExcel)
NonAyuExcel['specialityName']=[None]*len(NonAyuExcel)
NonAyuExcel['GST']=[None]*len(NonAyuExcel)
NonAyuExcel['Net Ayu Share']=[None]*len(NonAyuExcel)
NonAyuExcel['Agreement Starting Date']=[None]*len(NonAyuExcel)
NonAyuExcel['Agreement Termination date']=[None]*len(NonAyuExcel)
NonAyuExcel['commission_sys_comments']=[None]*len(NonAyuExcel)

workbook='Hospital Insurance Cases Master Data'
sheet = 'Ayu'
AyuExcel = get_df_from_sheet(workbook,sheet)

AyuExcel.columns = AyuExcel.iloc[0]
AyuExcel=AyuExcel.reindex(AyuExcel.index.drop(0)) 

AyuExcel.drop(['Nach Processed Date','Enach done/ not','Discharge Spoc','Intimation Number','Loan ID','Card Number','Policy Number','Policy Holder Name','Company Name','Employee Id (Patient)','Policy Type ','Planned Tentative Date (Only for Planned Cases)','Reason for PDC AYU','Rejection/Case Lost Reason','Cibil Rejected Reason','Cibil Status','KYP Processed By','Sales SPOC','KYP-Spoc','Month','Week','Date Filter'], axis=1, inplace=True)
AyuExcel.drop(['Remarks','Final Settlement Status','Mail Sent to Lender Date','Settled Date','Settled Amount','Query Replied POD Number','Latest Query Replied Date','Query 2 Reply Date','Query 2 Shared with Hospital','Query 2 Received Date','Query 1 Reply Date','Query 1 Shared with Hospital','Query 1 Received Date','Additional Remarks of Claim Status','Claim Status','Claim Number','Latest follow up Date','Follow-Up 3','Follow-Up 2','Follow-up Date with Insurance Company','Dispatch Ageing','Scrutiny Ageing','Hard Copy Dispatch To (IC/TPA Name)','Courier Partner','POD Number','Hard Copy Dispatch Date','Query in Details','Hard Copy Dispatch Status (Done or Not Done)','Hardcopy Dispatch Spoc','Soft Copy Scrutiny Completion Date','Follow Up Date for Scrutiny Query','Soft Copy Scrutiny Query Response Date(Hospital)','Soft Copy Scrutiny Query Raised Date','Soft Copy Scrutiny Status (Done/Under Query)','Soft Copy Scrutiny Assigned to','Soft Copy Scrutiny Received Date','Scrutiny Scanned file status','Amount Paid to Hospital Date','Payment Status','Amount Paid to Hospital'], axis=1, inplace=True)
AyuExcel.drop(['Payment Pending Ageing ','Post Discharged (Approved and Settled Ageing)','Overall Ageing Split','Overall Ageing','Post Discharge (CUP) Ageing','E Nach Ageing','Ageing (Admitted)','Ageing (Planned and KYP)','Amount Received from Lender Date','Amount Received from Lender'], axis=1, inplace=True)

AyuExcel['surgeryName']=[None]*len(AyuExcel)
AyuExcel['specialityName']=[None]*len(AyuExcel)
AyuExcel['GST']=[None]*len(AyuExcel)
AyuExcel['Net Ayu Share']=[None]*len(AyuExcel)
AyuExcel['Agreement Starting Date']=[None]*len(AyuExcel)
AyuExcel['Agreement Termination date']=[None]*len(AyuExcel)
AyuExcel['commission_sys_comments']=[None]*len(AyuExcel)

workbook='Hospital Insurance Cases Master Data'
sheet = 'whatsapp'
Whatsapp = get_df_from_sheet(workbook,sheet)

Whatsapp.columns = Whatsapp.iloc[0]
Whatsapp=Whatsapp.reindex(Whatsapp.index.drop(0)) 

Whatsapp.drop(['Nach Processed Date','Enach done/ not','Discharge Spoc','Intimation Number','Loan ID','Card Number','Policy Number','Policy Holder Name','Company Name','Employee Id (Patient)','Policy Type ','Planned Tentative Date (Only for Planned Cases)','Reason for PDC AYU','Rejection/Case Lost Reason','Cibil Rejected Reason','Cibil Status','KYP Processed By','Sales SPOC','KYP-Spoc','Month','Week','Date Filter'], axis=1, inplace=True)
Whatsapp.drop(['Remarks','Final Settlement Status','Mail Sent to Lender Date','Settled Date','Settled Amount','Query Replied POD Number','Latest Query Replied Date','Query 2 Reply Date','Query 2 Shared with Hospital','Query 2 Received Date','Query 1 Reply Date','Query 1 Shared with Hospital','Query 1 Received Date','Additional Remarks of Claim Status','Claim Status','Claim Number','Latest follow up Date','Follow-Up 3','Follow-Up 2','Follow-up Date with Insurance Company','Dispatch Ageing','Scrutiny Ageing','Hard Copy Dispatch To (IC/TPA Name)','Courier Partner','POD Number','Hard Copy Dispatch Date','Query in Details','Hard Copy Dispatch Status (Done or Not Done)','Hardcopy Dispatch Spoc','Soft Copy Scrutiny Completion Date','Follow Up Date for Scrutiny Query','Soft Copy Scrutiny Query Response Date(Hospital)','Soft Copy Scrutiny Query Raised Date','Soft Copy Scrutiny Status (Done/Under Query)','Soft Copy Scrutiny Assigned to','Soft Copy Scrutiny Received Date','Scrutiny Scanned file status','Amount Paid to Hospital Date','Payment Status','Amount Paid to Hospital'], axis=1, inplace=True)
Whatsapp.drop(['Payment Pending Ageing ','Post Discharged (Approved and Settled Ageing)','Overall Ageing Split','Overall Ageing','Post Discharge (CUP) Ageing','E Nach Ageing','Ageing (Admitted)','Ageing (Planned and KYP)','Amount Received from Lender Date','Amount Received from Lender'], axis=1, inplace=True)

Whatsapp['surgeryName']=[None]*len(Whatsapp)
Whatsapp['specialityName']=[None]*len(Whatsapp)
Whatsapp['GST']=[None]*len(Whatsapp)
Whatsapp['Net Ayu Share']=[None]*len(Whatsapp)
Whatsapp['Agreement Starting Date']=[None]*len(Whatsapp)
Whatsapp['Agreement Termination date']=[None]*len(Whatsapp)
Whatsapp['commission_sys_comments']=[None]*len(Whatsapp)

ayu_nonayu=pd.concat([NonAyuExcel,AyuExcel],axis=0)
insurance_dump=pd.concat([ayu_nonayu,Whatsapp],axis=0)

insurance_dump=insurance_dump.reset_index()
insurance_dump=insurance_dump.drop(['index'],axis=1)

insurance_dump.rename(columns={'Date':'case_created_on'},inplace=True)
insurance_dump.rename(columns={'Admission Month':'case_created_month'},inplace=True)
insurance_dump.rename(columns={'Case ID':'Caseid'},inplace=True)
insurance_dump.rename(columns={'Surgery ID':'surgeryid'},inplace=True)
insurance_dump.rename(columns={'Patient Source':'Patient_source'},inplace=True)
insurance_dump.rename(columns={'Patient Name':'Patient_name'},inplace=True)
insurance_dump.rename(columns={'Contact Number (As per the Insurance Copy)':'Contact_number'},inplace=True)
insurance_dump.rename(columns={'E-Mail id (As per the Insurance Copy)':'email_id'},inplace=True)
insurance_dump.rename(columns={'Hospital Name':'hospital_name'},inplace=True)
insurance_dump.rename(columns={'Partner':'lenderName'},inplace=True)
insurance_dump.rename(columns={'Case Status':'case_status'},inplace=True)
insurance_dump.rename(columns={'Admission Date':'admissionDate'},inplace=True)
insurance_dump.rename(columns={'Discharge Date':'dischargeDate'},inplace=True)
insurance_dump.rename(columns={'Insurance':'insurancename'},inplace=True)
insurance_dump.rename(columns={'Treatment Type':'treatmentType'},inplace=True)
insurance_dump.rename(columns={'TPA':'tpaName'},inplace=True)
# insurance_dump.rename(columns={'Discharge Case Processed by':'dis_doc_assigned_to'},inplace=True)
insurance_dump.rename(columns={'Total Bill Amount':'totalBillAmount'},inplace=True)
insurance_dump.rename(columns={'Approved \n(Chargeable)':'finalApprovedAmount'},inplace=True)
insurance_dump.rename(columns={'NME (Collected by Hospital)':'NME: Collected by Hospital'},inplace=True)
insurance_dump.rename(columns={'NME (Collected by AYU)':'NME: Collected by AYU'},inplace=True)
# insurance_dump.rename(columns={'Ayu Share %':'Ayu Share'},inplace=True)
insurance_dump.rename(columns={'Ayu Share Rs.':'Ayu Share'},inplace=True)
insurance_dump.rename(columns={'Net Payable':'Net_Payable'},inplace=True)
insurance_dump.rename(columns={'Commission Remarks':'Remarks'},inplace=True)

insurance_dump['finalApprovedAmount']=insurance_dump['finalApprovedAmount'].str.split(",").map(lambda x: x[0]).astype(str)+insurance_dump['finalApprovedAmount'].str.split(",").map(lambda x: x[1] if len(x)>=2 else 0).astype(str)
insurance_dump['GST']='18.00%'
insurance_dump["Net Ayu Share %"]=(pd.to_numeric(insurance_dump["Ayu Share %"].str.replace("%","").replace(" ",""))/100)*0.18+(pd.to_numeric(insurance_dump["Ayu Share %"].str.replace("%","").replace(" ",""))/100)
insurance_dump['totalBillAmount']=insurance_dump['totalBillAmount'].str.split(",").map(lambda x: x[0]).astype(str)+insurance_dump['totalBillAmount'].str.split(",").map(lambda x: x[1] if len(x)>=2 else 0).astype(str)
insurance_dump.loc[(insurance_dump['Remarks']=='On Approved Amount'),'Total Ayu Share']=(pd.to_numeric(insurance_dump['finalApprovedAmount']))*(pd.to_numeric(insurance_dump["Net Ayu Share %"]))
insurance_dump.loc[(insurance_dump['Remarks']=='On Total Bill Amount'),'Total Ayu Share']=(pd.to_numeric(insurance_dump['totalBillAmount']))*(pd.to_numeric(insurance_dump["Net Ayu Share %"]))
insurance_dump['GST Amount']=pd.to_numeric(insurance_dump['Total Ayu Share'])-pd.to_numeric(insurance_dump['Ayu Share'])
insurance_dump['AYU Discount']=insurance_dump['AYU Discount'].str.split(",").map(lambda x: x[0]).astype(str)+insurance_dump['AYU Discount'].str.split(",").map(lambda x: x[1] if len(x)>=2 else 0).astype(str)
insurance_dump['NME: Collected by AYU']=insurance_dump['NME: Collected by AYU'].str.split(",").map(lambda x: x[0]).astype(str)+insurance_dump['NME: Collected by AYU'].str.split(",").map(lambda x: x[1] if len(x)>=2 else 0).astype(str)
insurance_dump['Net_Payable']=pd.to_numeric(insurance_dump['finalApprovedAmount'])-pd.to_numeric(insurance_dump['Total Ayu Share'])-pd.to_numeric(insurance_dump['TDS@1%'])+pd.to_numeric(insurance_dump['AYU Discount'])+pd.to_numeric(insurance_dump['NME: Collected by AYU'])

def fetch_data(conn):
    fetched_val = {}
    for lookup, query in ipd_transactions_admissions.items():
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
        case_details =pd.DataFrame(dataSets['case']) 
        insurance_doc =pd.DataFrame(dataSets['ins_doc'])
        discharge_doc =pd.DataFrame(dataSets['dis_doc'])
        Surgery_done =pd.DataFrame(dataSets['sur_done'])
        fin_eligible =pd.DataFrame(dataSets['fin_elig'])
        
        insurance_doc['Rank1'] = insurance_doc.groupby('ins_caseid')['ins_phaseid'].rank(method='first',ascending=False)
        discharge_doc['Rank2'] = discharge_doc.groupby('dis_caseid')['dis_phaseid'].rank(method='first',ascending=False)
        Surgery_done['Rank3'] = Surgery_done.groupby('sur_caseid')['sur_phaseid'].rank(method='first',ascending=False)
        fin_eligible['Rank4'] = fin_eligible.groupby('fin_phaseid')['subPhaseId'].rank(method='first',ascending=False)
        
        insurance_doc=insurance_doc[insurance_doc['Rank1']==1]
        discharge_doc=discharge_doc[discharge_doc['Rank2']==1]
        Surgery_done=Surgery_done[Surgery_done['Rank3']==1]
        fin_eligible=fin_eligible[fin_eligible['Rank4']==1]
        
        df1 = pd.merge(case_details,insurance_doc, how='left', left_on='Caseid',right_on='ins_caseid')
        df2 = pd.merge(df1,discharge_doc, how='left', left_on='Caseid',right_on='dis_caseid')
        df3 = pd.merge(df2,Surgery_done, how='left', left_on='Caseid',right_on='sur_caseid')
        df4 = pd.merge(df3,fin_eligible, how='left', left_on='ins_phaseid',right_on='fin_phaseid')
        df4.drop(['Rank1','Rank2','Rank3','Rank4','ins_caseid','sur_caseid','dis_caseid','ins_phaseid','dis_phaseid','sur_phaseid','fin_phaseid','subPhaseId'], axis=1, inplace=True)
        
        a=['Ayu PDC','Hospital PDC']
        df4.loc[(df4['lenderName'].isna()),'Category']="None"
        df4.loc[(df4['lenderName']=='Ayu PDC'),'Category']="PDC-Ayu"
        df4.loc[(df4['lenderName']=='Hospital PDC'),'Category']="PDC Hospital"
        df4.loc[(~df4['lenderName'].isin(a)) & (~df4['lenderName'].isna()),'Category']="24HR"
        
        df4['Key']=df4['hospital_name']+df4['Source']+df4['Category']
        
        df = pd.merge(df4,commission_sheet, on='Key',how='left')
        df.drop(['Key'],axis=1,inplace=True)
        
        df['admissionDate']=pd.to_datetime(df['admissionDate'])
        df['Agreement Starting Date']=pd.to_datetime(df['Agreement Starting Date'])
        df['Agreement Termination date']=pd.to_datetime(df['Agreement Termination date'])
        
        df.loc[(df['admissionDate']>=df['Agreement Starting Date']) & (df['admissionDate']<=df['Agreement Termination date']),'commission_sys_comments']='commission_calculated'
        df.loc[df['lenderName'].isna(),'commission_sys_comments']='lender_not_updated'
        df.loc[(df['admissionDate'].isna()) & (~df['lenderName'].isna()),'commission_sys_comments']='admission_date_not_updated'   
        
        df=df[~(((df['admissionDate']<df['Agreement Starting Date']) | (df['admissionDate']>df['Agreement Termination date'])) & df['admissionDate'].notna())]
        df=df[~(((df['case_created_on']<df['Agreement Starting Date']) | (df['case_created_on']>df['Agreement Termination date'])) & df['admissionDate'].isna())]
        
        df.loc[(df['Remarks']=='On Approved amount'),'Ayu Share']=pd.to_numeric(df['finalApprovedAmount'])*(pd.to_numeric(df["Ayu Share %"].str.replace("%",""))/100)
        df.loc[(df['Remarks']=='On Total Bill Amount'),'Ayu Share']=pd.to_numeric(df['totalBillAmount'])*(pd.to_numeric(df["Ayu Share %"].str.replace("%",""))/100)
        
        df.loc[(df['Remarks']=='On Approved amount'),'Total Ayu Share']=pd.to_numeric(df['finalApprovedAmount'])*(pd.to_numeric(df["Net Ayu Share %"].str.replace("%",""))/100)
        df.loc[(df['Remarks']=='On Total Bill Amount'),'Total Ayu Share']=pd.to_numeric(df['totalBillAmount'])*(pd.to_numeric(df["Net Ayu Share %"].str.replace("%",""))/100)
        
        df['TDS@1%']=len(df)*[None]
        df['NME: Collected by AYU']=len(df)*[None]
        df['AYU Discount']=len(df)*[None]
        df.rename(columns={'NonMedicalExpenses':'NME: Collected by Hospital'},inplace=True)
        df[['TDS@1%','AYU Discount','NME: Collected by AYU']]=df[['TDS@1%','AYU Discount','NME: Collected by AYU']].replace(np.NaN,0)
        df['Net_Payable']=pd.to_numeric(df['finalApprovedAmount'])-pd.to_numeric(df['Total Ayu Share']) -pd.to_numeric(df['TDS@1%'])+pd.to_numeric(df['AYU Discount'])+pd.to_numeric(df['NME: Collected by AYU'])
        df['GST Amount']=df['Total Ayu Share']-pd.to_numeric(df['Ayu Share'])
        final_stack=pd.concat([insurance_dump,df],axis=0)
        final_stack=final_stack.replace(np.NaN,"")
        final_stack[['case_created_on','admissionDate','Agreement Starting Date','Agreement Termination date','dischargeDate']]=final_stack[['case_created_on','admissionDate','Agreement Starting Date','Agreement Termination date','dischargeDate']].astype(str)
        final_stack.drop(['Agreement Starting Date','Agreement Termination date'], axis=1, inplace=True)
        final_stack.rename(columns={'Remarks':'Chargeable On'},inplace=True)
        final_stack['Chargeable Amount']=final_stack['finalApprovedAmount']
        final_stack=final_stack[['case_created_on', 'case_created_month', 'Caseid', 'surgeryid', 'hospital_name', 'Patient_name', 
                              'Contact_number', 'email_id','city', 'lenderName', 'Category', 'Source','Patient_source' ,'vendorname',
                              'case_status', 'admissionDate', 'dischargeDate', 'insurancename',
                              'tpaName', 'treatmentType','surgeryName','specialityName',
                              'totalBillAmount', 'finalApprovedAmount',  'NME: Collected by Hospital',
                              'NME: Collected by AYU', 'AYU Discount', 'Ayu Share %','GST','Net Ayu Share %',
                              'Ayu Share','GST Amount','Total Ayu Share', 'TDS@1%','Net_Payable','Chargeable Amount','Chargeable On',
                              'commission_sys_comments' ]]

        # fpath = os.path.join("/tmp","subject.csv") 
        # final_stack.to_csv(fpath)
         
        hi_commission = [list(final_stack.columns)]
        
        values = final_stack.values.tolist()
        hi_commission.extend(values) 
        # print(hi_commission)

        clear_and_write_to_sheet_city('18Pi9TrHNalZDOi1V-jGjsu_KtiX_rp2Mvop9TnFRXsU', 'HI_Commission', 'A1:AN', hi_commission)
        
        # Subject = "Non Ayu Hospital insurance cases data"
        # email_recipient_list = ['vivek.kb@ayu.health']
        # send_email(None, email_recipient_list, Subject, 'Non-Ayu Hospital insurance case automation data','None',[fpath])
           
    except Exception as e: 
        raise e      
         
 
