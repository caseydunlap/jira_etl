import pandas as pd
import numpy as np
import pytz
import time
import urllib
import math
import boto3
import json
import snowflake.connector
from sqlalchemy.sql import text
from datetime import datetime,timedelta,timezone,time,date
from dateutil.relativedelta import relativedelta
import requests
from sqlalchemy import create_engine
from decimal import Decimal
from requests.auth import HTTPBasicAuth
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.serialization import load_pem_private_key, load_der_private_key

def get_secrets(secret_names, region_name="us-east-1"):
    secrets = {}
    
    client = boto3.client(
        service_name='secretsmanager',
        region_name=region_name
    )
    
    for secret_name in secret_names:
        try:
            get_secret_value_response = client.get_secret_value(
                SecretId=secret_name)
        except Exception as e:
                raise e
        else:
            if 'SecretString' in get_secret_value_response:
                secrets[secret_name] = get_secret_value_response['SecretString']
            else:
                secrets[secret_name] = base64.b64decode(get_secret_value_response['SecretBinary'])

    return secrets
    
def extract_secret_value(data):
    if isinstance(data, str):
        return json.loads(data)
    return data

secrets = ['snowflake_bizops_user','snowflake_account','snowflake_fivetran_db','snowflake_bizops_role','snowflake_key_pass','snowflake_bizops_wh','jira_api_token','email']

fetch_secrets = get_secrets(secrets)

extracted_secrets = {key: extract_secret_value(value) for key, value in fetch_secrets.items()}

snowflake_user = extracted_secrets['snowflake_bizops_user']['snowflake_bizops_user']
snowflake_account = extracted_secrets['snowflake_account']['snowflake_account']
snowflake_key_pass = extracted_secrets['snowflake_key_pass']['snowflake_key_pass']
snowflake_bizops_wh = extracted_secrets['snowflake_bizops_wh']['snowflake_bizops_wh']
snowflake_fivetran_db = extracted_secrets['snowflake_fivetran_db']['snowflake_fivetran_db']
snowflake_role = extracted_secrets['snowflake_bizops_role']['snowflake_bizops_role']
jira_api_key = extracted_secrets['jira_api_token']['jira_api_token']
email = extracted_secrets['email']['email']

password = snowflake_key_pass.encode()

s3_bucket = 'aws-glue-assets-bianalytics'
s3_key = 'BIZ_OPS_ETL_USER.p8'

def download_from_s3(bucket, key):
    s3_client = boto3.client('s3')
    try:
        response = s3_client.get_object(Bucket=bucket, Key=key)
        return response['Body'].read()
    except Exception as e:
        print(f"Error downloading from S3: {e}")
        return None

key_data = download_from_s3(s3_bucket, s3_key)

private_key = load_pem_private_key(key_data, password=password)

private_key_bytes = private_key.private_bytes(
    encoding=serialization.Encoding.DER,
    format=serialization.PrivateFormat.PKCS8,
    encryption_algorithm=serialization.NoEncryption()
)

jira_url = "https://hhaxsupport.atlassian.net"
api_endpoint = "/rest/api/3/search/"

jql_query = f'(project in (HHA) and (updated >=-2h or created >= -2h))'

jql_query_encoded = urllib.parse.quote(jql_query)

startAt = 0
maxResults = 100

all_issues = []

while True:
    api_url = f"{jira_url}{api_endpoint}?jql={jql_query_encoded}&startAt={startAt}&maxResults={maxResults}"

    response = requests.get(
        api_url,
        auth=HTTPBasicAuth(email, jira_api_key),
        headers={
            "Accept": "application/json"
        }
    )

    json_response = response.json()

    if response.status_code == 200:
        all_issues.extend(json_response['issues'])

        if json_response['total'] == len(all_issues):
            break
        else:
            startAt += maxResults
    else:
        print("Request failed. Error message:")
        print(json_response)
        break

if isinstance(json_response, str):
    json_response = json.loads(json_response)

issues = all_issues

if isinstance(issues, list):
    data = []

    for issue in issues:

        key = issue['key']
        customfield_10035_obj = issue['fields'].get('customfield_10035', None)
        customfield_10035 = customfield_10035_obj['value'] if customfield_10035_obj else None
        customfield_10036_obj = issue['fields'].get('customfield_10036',None)
        customfield_10036 = customfield_10036_obj['value'] if customfield_10036_obj else None
        customfield_10036_child = customfield_10036_obj['child']['value'] if customfield_10036_obj and 'child' in customfield_10036_obj else None
        created = issue['fields'].get('created', None)
        summary = issue['fields'].get('summary', None)
        resolved = issue['fields'].get('resolutiondate',None)
        assignee_obj = issue['fields'].get('assignee', None)
        assignee_name = assignee_obj['displayName'] if assignee_obj and 'displayName' in assignee_obj else None
        customfield_10300 = issue['fields'].get('customfield_10300', None)
        customfield_10201 = issue['fields'].get('customfield_10201', None)
        priority_obj = issue['fields'].get('priority', {})
        priority_name = priority_obj.get('name', None)
        resolution_obj = issue['fields'].get('resolution')
        if resolution_obj:
            resolution_name = resolution_obj.get('name', None)
        else:
            resolution_name = None
        customfield_10010_obj = issue['fields'].get('customfield_10010')
        if customfield_10010_obj:
            request_type_obj = customfield_10010_obj.get('requestType', {})
            request_type = request_type_obj.get('name', None)
        else:
            request_type = None
        custom_field = issue['fields'].get('customfield_10010', {})
        customfield_12757_obj = issue['fields'].get('customfield_12757', {})
        if customfield_12757_obj:
            completed_cycles = customfield_12757_obj.get('completedCycles', [])
            if completed_cycles:
                remaining_time = completed_cycles[0].get('elapsedTime', {}).get('millis', None)
            else:
                remaining_time = None
        else:
            completed_cycles = []
            remaining_time = None

        customfield_12756_obj = issue['fields'].get('customfield_12756', {})
        if customfield_12756_obj:
            completed_cycles_2 = customfield_12756_obj.get('completedCycles', [])
            if completed_cycles_2:
                remaining_time_2 = completed_cycles_2[0].get('elapsedTime', {}).get('millis', None)
            else:
                remaining_time_2 = None
        else:
            completed_cycles_2 = []
            remaining_time_2 = None
        status_obj = issue['fields'].get('status', {})
        customfield_10202 = issue['fields'].get('customfield_10202', None)
        description = status_obj.get('description', None)
        customfield_10030_obj = issue['fields'].get('customfield_10030', {})
        if customfield_10030_obj and isinstance(customfield_10030_obj, dict):
            completed_cycles_resolved = customfield_10030_obj.get('completedCycles', [])
            if completed_cycles_resolved and completed_cycles_resolved[0] is not None:
                completed_cycles_resolved_adjusted = completed_cycles_resolved[0].get('elapsedTime', {}).get('millis', None)
            else:
                completed_cycles_resolved_adjusted = None
        else:
            completed_cycles_resolved_adjusted = None

        customfield_10031_obj = issue['fields'].get('customfield_10031', {})
        if customfield_10031_obj and isinstance(customfield_10031_obj, dict):
            completed_cycles_response = customfield_10031_obj.get('completedCycles', [])
            if completed_cycles_response and completed_cycles_response[0] is not None:
                completed_cycles_response_adjusted = completed_cycles_response[0].get('elapsedTime', {}).get('millis', None)
            else:
                completed_cycles_response_adjusted = None
        else:
            completed_cycles_response_adjusted = None
        customfield_10056_obj = issue['fields'].get('customfield_10056', {})
        completed_cycles_resolved_vip = []
        if customfield_10056_obj:
            completed_cycles_resolved_vip = customfield_10056_obj.get('completedCycles', [])
        if completed_cycles_resolved_vip:
            completed_cycles_resolved_adjusted_vip = completed_cycles_resolved_vip[0].get('elapsedTime', {}).get('millis', None)
        else:
            completed_cycles_resolved_adjusted_vip = None
        customfield_10057_obj = issue['fields'].get('customfield_10057', {})
        completed_cycles_response_vip = []
        if customfield_10057_obj:
            completed_cycles_response_vip = customfield_10057_obj.get('completedCycles', [])
        if completed_cycles_response_vip:
            completed_cycles_response_adjusted_vip = completed_cycles_response_vip[0].get('elapsedTime', {}).get('millis', None)
        else:
            completed_cycles_response_adjusted_vip = None
        customfield_12793_obj = issue['fields'].get('customfield_12793', {})
        if customfield_12793_obj:
            customfield_12793_value = customfield_12793_obj.get('value', None)
        else:
            customfield_12793_value = None
        customfield_12782_obj = issue['fields'].get('customfield_12782', {})
        if customfield_12782_obj:
            customfield_12782_value = customfield_12782_obj.get('value', None)
        else:
            customfield_12782_value = None
        customfield_12787_obj = issue['fields'].get('customfield_12787', {})
        if customfield_12787_obj:
            customfield_12787_value = customfield_12787_obj.get('value', None)
        else:
            customfield_12787_value = None
        customfield_12791_obj = issue['fields'].get('customfield_12791', {})
        if customfield_12791_obj:
            customfield_12791_value = customfield_12791_obj.get('value', None)
        else:
            customfield_12791_value = None
        customfield_12798_obj = issue['fields'].get('customfield_12798', {})
        if customfield_12798_obj:
            customfield_12798_value = customfield_12798_obj.get('value', None)
        else:
            customfield_12798_value = None
        customfield_12784_obj = issue['fields'].get('customfield_12784', {})
        if customfield_12784_obj:
            customfield_12784_value = customfield_12784_obj.get('value', None)
        else:
            customfield_12784_value = None
        customfield_12795_obj = issue['fields'].get('customfield_12795', {})
        if customfield_12795_obj:
            customfield_12795_value = customfield_12795_obj.get('value', None)
        else:
            customfield_12795_value = None
        customfield_12792_obj = issue['fields'].get('customfield_12792', {})
        if customfield_12792_obj:
            customfield_12792_value = customfield_12792_obj.get('value', None)
        else:
            customfield_12792_value = None
        customfield_12786_obj = issue['fields'].get('customfield_12786', {})
        if customfield_12786_obj:
            customfield_12786_value = customfield_12786_obj.get('value', None)
        else:
            customfield_12786_value = None
        customfield_12796_obj = issue['fields'].get('customfield_12796', {})
        if customfield_12796_obj:
            customfield_12796_value = customfield_12796_obj.get('value', None)
        else:
            customfield_12796_value = None
        customfield_12785_obj = issue['fields'].get('customfield_12785', {})
        if customfield_12785_obj:
            customfield_12785_value = customfield_12785_obj.get('value', None)
        else:
            customfield_12785_value = None
        customfield_12780_obj = issue['fields'].get('customfield_12780', {})
        if customfield_12780_obj:
            customfield_12780_value = customfield_12780_obj.get('value', None)
        else:
            customfield_12780_value = None
        customfield_12799_obj = issue['fields'].get('customfield_12799', {})
        if customfield_12799_obj:
            customfield_12799_value = customfield_12799_obj.get('value', None)
        else:
            customfield_12799_value = None
        customfield_12788_obj = issue['fields'].get('customfield_12788', {})
        if customfield_12788_obj:
            customfield_12788_value = customfield_12788_obj.get('value', None)
        else:
            customfield_12788_value = None
        customfield_12797_obj = issue['fields'].get('customfield_12797', {})
        if customfield_12797_obj:
            customfield_12797_value = customfield_12797_obj.get('value', None)
        else:
            customfield_12797_value = None
        customfield_11540_obj = issue['fields'].get('customfield_11540', {})
        if customfield_11540_obj:
            customfield_11540 = customfield_11540_obj.get('value', None)
        else:
            customfield_11540 = None
        customfield_10236_obj = issue['fields'].get('customfield_10236', {})
        if customfield_10236_obj and 'content' in customfield_10236_obj:
            content_list = customfield_10236_obj['content']
            if content_list and 'content' in content_list[0]:
                text_content = content_list[0]['content']
                if text_content and 'text' in text_content[0]:
                    customfield_10236 = text_content[0]['text']
                else:
                    customfield_10236 = None
            else:
                customfield_10236 = None
        else:
            customfield_10236 = None
        customfield_12789 = issue['fields'].get('customfield_12789', None)
        customfield_12779 = issue['fields'].get('customfield_12779', None)
        customfield_12710 = issue['fields'].get('customfield_12710', None)
        customfield_12632 = issue['fields'].get('customfield_12632', None)
        customfield_12652 = issue['fields'].get('customfield_12652', None)
        customfield_12781 = issue['fields'].get('customfield_12781', None)
        customfield_10916 = issue['fields'].get('customfield_10916', None)
        customfield_12803 = issue['fields'].get('customfield_12803', None)
        customfield_12802 = issue['fields'].get('customfield_12802', None)
        customfield_10206 = issue['fields'].get('customfield_10206', None)
        customfield_12755 = issue['fields'].get('customfield_12755', None)
        customfield_10203 = issue['fields'].get('customfield_10203', None)

        status_snapshot = issue['fields'].get('status', {}).get('name', None)

        data.append([key, customfield_12755,customfield_10206,customfield_11540,customfield_10236,customfield_12802,customfield_12803,customfield_10916,customfield_12781,customfield_12652,customfield_12632,customfield_12710,customfield_12779,customfield_12789,description,customfield_12797_value,customfield_12788_value,customfield_12799_value,customfield_12780_value,customfield_12785_value,customfield_12796_value,customfield_12786_value,customfield_12792_value,customfield_12795_value,customfield_12784_value,customfield_12791_value,customfield_12798_value,customfield_12787_value,customfield_12782_value,customfield_12793_value,completed_cycles_resolved_adjusted_vip,completed_cycles_response_adjusted_vip,customfield_10202,completed_cycles_response_adjusted,completed_cycles_resolved_adjusted,summary,resolved,resolution_name,request_type,customfield_10036,customfield_10036_child,customfield_10035, remaining_time,remaining_time_2,customfield_10201,created, assignee_name, customfield_10300, status_snapshot,priority_name,customfield_10203])

    update_df = pd.DataFrame(data, columns=['key','primary location','hhax market','state','HHAX regional platform tag','requester name','request username','agency name','customer id','contact name','caregiver code','patient','patient id','Report Needed Help With','description','Visit / Action Help Type Needed','Visit Functionality Help Needed','Type of Help Needed Free','Patient Profile Help Needed','Mobile Device Type','Caregiver Mobile App Help Type Free','Caregiver Mobile App Help Type','Type of Incident','Caregiver Profile Help Type Needed Free','Caregiver Profile Help Type Needed','Admin Help Type Needed','Admin Help Type Needed free','Action Functionality Help Needed','Environment Type','Type of Help Needed','elapsed time resolved - vip','elapsed time response - vip','ent/free','eas-response','eas-resolved','summary','resolved','resolution_name','request_type','sub issue_1','sub issue_2','customfield_10035','elapsedTime-Resolved','elapsedTime-response','customfield_10201', 'created', 'assignee', 'customfield_10300', 'current_status','priority','hhax-vip'])

column_mapping = {'key':'KEY',
'primary location': 'PRIMARY_LOCATION',
'hhax market': 'HHAX_MARKET',
'state':'STATE',
'HHAX regional platform tag':'HHAX_REGIONAL_PLATFORM_TAG',
'requester name':'REQUESTER_NAME',
'request username':'REQUESTER_USERNAME',
'agency name':'AGENCY_NAME',
'customer id':'CUSTOMER_ID',
'contact name':'CONTACT_NAME',
'caregiver code':'CAREGIVER_CODE',
'patient':'PATIENT',
'patient id':'PATIENT_ID',
'Report Needed Help With':'REPORT_NEEDED_HELP_WITH',
'description':'DESCRIPTION',
'Visit / Action Help Type Needed':'VISIT_ACTION_HELP_TYPE_NEEDED',
'Visit Functionality Help Needed':'VISIT_FUNCTIONALITY_HELP_NEEDED',
'Type of Help Needed Free':'TYPE_OF_HELP_NEEDED_FREE',
'Patient Profile Help Needed':'PATIENT_PROFILE_HELP_NEEDED',
'Mobile Device Type':'MOBILE_DEVICE_TYPE',
'Caregiver Mobile App Help Type Free':'CAREGIVER_MOBILE_APP_HELP_TYPE_FREE',
'Caregiver Mobile App Help Type':'CAREGIVER_MOBILE_APP_HELP_TYPE',
'Type of Incident':'TYPE_OF_INCIDENT',
'Caregiver Profile Help Type Needed Free':'CAREGIVER_PROFILE_HELP_TYPE_NEEDED_FREE',
'Caregiver Profile Help Type Needed':'CAREGIVER_PROFILE_HELP_TYPE_NEEDED',
'Admin Help Type Needed':'ADMIN_HELP_TYPE_NEEDED',
'Admin Help Type Needed free':'ADMIN_HELP_TYPE_NEEDED_FREE',
'Action Functionality Help Needed':'ACTION_FUNCTIONALITY_HELP_NEEDED',
'Environment Type':'ENVIRONMENT_TYPE',
'Type of Help Needed':'TYPE_OF_HELP_NEEDED',
'elapsed time resolved - vip':'ELAPSED_TIME_RESOLVED_VIP',
'elapsed time response - vip':'ELAPSED_TIME_RESPONSE_VIP',
'ent/free':'ENT_OR_FREE',
'eas-response':'ELAPSED_TIME_RESPONSE',
'eas-resolved':'ELAPSED_TIME_RESOLVED',
'summary':'SUMMARY',
'resolved':'RESOLVED_DATE',
'resolution_name':'RESOLUTION_TYPE',
'request_type':'REQUEST_TYPE',
'sub issue_1':'SUB_ISSUE_1',
'sub issue_2':'SUB_ISSUE_2',
'customfield_10035':'REQUEST_CHANNEL',
'elapsedTime-Resolved':'ELAPSED_TIME_RESOLVED_TEXAS',
'elapsedTime-response':'ELAPSED_TIME_RESPONSE_TEXAS',
'customfield_10201':'SALESFORCE_ACCOUNT_ID',
'created':'CREATE_DATE',
'assignee':'ASSIGNEE',
'customfield_10300':'T1_ESCALATION_DATE',
'current_status':'STATUS',
'priority':'PRIORITY',
'hhax-vip':'IS_VIP'
} 

ctx = snowflake.connector.connect(
    user=snowflake_user,
    account=snowflake_account,
    private_key=private_key_bytes,
    role=snowflake_role,
    warehouse=snowflake_bizops_wh)
    
cs = ctx.cursor()

script = """
select * 
from "PC_FIVETRAN_DB"."JIRA"."CUSTOM_ISSUE"
"""
payload = cs.execute(script)
original_df = pd.DataFrame.from_records(iter(payload), columns=[x[0] for x in payload.description])

delete = """
delete from "PC_FIVETRAN_DB"."JIRA"."CUSTOM_ISSUE_STAGING"
"""
cs.execute(delete)

original_df = original_df.drop('UPDATED_TIMESTAMP', axis=1)
update_df_final = update_df.rename(columns=column_mapping)
update_df_final = update_df_final.drop_duplicates(subset='KEY', keep='last')

def escape_single_quotes(s):
    if isinstance(s, str):
        return s.replace("'", "''")
    return s

def add_timezone_to_timestamp(timestamp):
    return timestamp + " -0400"

def convert_timestamp_format(timestamp_str):
    if timestamp_str is None:
        return None
    return timestamp_str.replace('T', ' ')

# Create copies to work on
df3 = original_df.copy()
update_temp = update_df_final.copy()

update_temp.set_index('KEY', inplace=True)
df3.set_index('KEY', inplace=True)

common_indices = df3.index.intersection(update_temp.index)
common_rows_from_df3 = df3.loc[common_indices]
common_rows_from_update = update_temp.loc[common_indices]

updated_rows = common_rows_from_update[~(common_rows_from_update == common_rows_from_df3).all(axis=1)]

new_rows = update_temp.loc[update_temp.index.difference(df3.index)]

if 'index' in new_rows.columns:
    new_rows.drop('index', axis=1, inplace=True)

if 'index' in updated_rows.columns:
    updated_rows.drop('index', axis=1, inplace=True)

updated_rows = updated_rows.applymap(escape_single_quotes)
new_rows = new_rows.applymap(escape_single_quotes)

current_timestamp = datetime.now()

updated_rows['UPDATED_TIMESTAMP'] = current_timestamp
updated_rows['UPDATED_TIMESTAMP'] = updated_rows['UPDATED_TIMESTAMP'].astype(str)
updated_rows['UPDATED_TIMESTAMP']= updated_rows['UPDATED_TIMESTAMP'].apply(add_timezone_to_timestamp)
updated_rows['CREATE_DATE'] = updated_rows['CREATE_DATE'].astype(str)
updated_rows['RESOLVED_DATE'] = updated_rows['RESOLVED_DATE'].astype(str)
updated_rows['RESOLVED_DATE']=updated_rows['RESOLVED_DATE'].apply(convert_timestamp_format)
updated_rows['CREATE_DATE']=updated_rows['CREATE_DATE'].apply(convert_timestamp_format)

new_rows['UPDATED_TIMESTAMP'] = current_timestamp
new_rows['UPDATED_TIMESTAMP'] = new_rows['UPDATED_TIMESTAMP'].astype(str)
new_rows['UPDATED_TIMESTAMP']= new_rows['UPDATED_TIMESTAMP'].apply(add_timezone_to_timestamp)
new_rows['CREATE_DATE'] = new_rows['CREATE_DATE'].astype(str)
new_rows['RESOLVED_DATE'] = new_rows['RESOLVED_DATE'].astype(str)
new_rows['RESOLVED_DATE']=new_rows['RESOLVED_DATE'].apply(convert_timestamp_format)
new_rows['CREATE_DATE']=new_rows['CREATE_DATE'].apply(convert_timestamp_format)

connection_string = f"snowflake://{snowflake_user}@{snowflake_account}/{snowflake_fivetran_db}/JIRA?warehouse={snowflake_bizops_wh}&role={snowflake_role}&authenticator=externalbrowser"

engine = create_engine(
    connection_string,
    connect_args={
        "private_key": private_key_bytes
    }
)

engine2 = create_engine(
    connection_string,
    connect_args={
        "private_key": private_key_bytes
    }
)

table_name = '"PC_FIVETRAN_DB"."JIRA"."CUSTOM_ISSUE"'

updated_rows.reset_index(inplace=True)
new_rows.reset_index(inplace=True)

staging_table_name = 'custom_issue_staging'

updated_rows.to_sql(staging_table_name, engine, if_exists='append', index=False, method='multi', chunksize=10000)

if len(new_rows) > 0:
    new_rows.to_sql(staging_table_name, engine2, if_exists='append', index=False, method='multi', chunksize=1000)
else:
    pass

merge_sql = f"""
MERGE INTO {table_name} AS target
USING "PC_FIVETRAN_DB"."JIRA"."CUSTOM_ISSUE_STAGING" AS staging
ON target.KEY = staging.KEY
WHEN MATCHED THEN
    UPDATE SET
target.KEY=staging.KEY,
target.PRIMARY_LOCATION=staging.PRIMARY_LOCATION,
target.HHAX_MARKET=staging.HHAX_MARKET,
target.STATE=staging.STATE,
target.HHAX_REGIONAL_PLATFORM_TAG=staging.HHAX_REGIONAL_PLATFORM_TAG,
target.REQUESTER_NAME=staging.REQUESTER_NAME,
target.REQUESTER_USERNAME=staging.REQUESTER_USERNAME,
target.AGENCY_NAME=staging.AGENCY_NAME,
target.CUSTOMER_ID=staging.CUSTOMER_ID,
target.CONTACT_NAME=staging.CONTACT_NAME,
target.CAREGIVER_CODE=staging.CAREGIVER_CODE,
target.PATIENT=staging.PATIENT,
target.PATIENT_ID=staging.PATIENT_ID,
target.REPORT_NEEDED_HELP_WITH=staging.REPORT_NEEDED_HELP_WITH,
target.DESCRIPTION=staging.DESCRIPTION,
target.VISIT_ACTION_HELP_TYPE_NEEDED=staging.VISIT_ACTION_HELP_TYPE_NEEDED,
target.VISIT_FUNCTIONALITY_HELP_NEEDED=staging.VISIT_FUNCTIONALITY_HELP_NEEDED,
target.TYPE_OF_HELP_NEEDED_FREE=staging.TYPE_OF_HELP_NEEDED_FREE,
target.PATIENT_PROFILE_HELP_NEEDED=staging.PATIENT_PROFILE_HELP_NEEDED,
target.MOBILE_DEVICE_TYPE=staging.MOBILE_DEVICE_TYPE,
target.CAREGIVER_MOBILE_APP_HELP_TYPE_FREE=staging.CAREGIVER_MOBILE_APP_HELP_TYPE_FREE,
target.CAREGIVER_MOBILE_APP_HELP_TYPE=staging.CAREGIVER_MOBILE_APP_HELP_TYPE,
target.TYPE_OF_INCIDENT=staging.TYPE_OF_INCIDENT,
target.CAREGIVER_PROFILE_HELP_TYPE_NEEDED_FREE=staging.CAREGIVER_PROFILE_HELP_TYPE_NEEDED_FREE,
target.CAREGIVER_PROFILE_HELP_TYPE_NEEDED=staging.CAREGIVER_PROFILE_HELP_TYPE_NEEDED,
target.ADMIN_HELP_TYPE_NEEDED=staging.ADMIN_HELP_TYPE_NEEDED,
target.ADMIN_HELP_TYPE_NEEDED_FREE=staging.ADMIN_HELP_TYPE_NEEDED_FREE,
target.ACTION_FUNCTIONALITY_HELP_NEEDED=staging.ACTION_FUNCTIONALITY_HELP_NEEDED,
target.ENVIRONMENT_TYPE=staging.ENVIRONMENT_TYPE,
target.TYPE_OF_HELP_NEEDED=staging.TYPE_OF_HELP_NEEDED,
target.ELAPSED_TIME_RESOLVED_VIP=staging.ELAPSED_TIME_RESOLVED_VIP,
target.ELAPSED_TIME_RESPONSE_VIP=staging.ELAPSED_TIME_RESPONSE_VIP,
target.ENT_OR_FREE=staging.ENT_OR_FREE,
target.ELAPSED_TIME_RESPONSE=staging.ELAPSED_TIME_RESPONSE,
target.ELAPSED_TIME_RESOLVED=staging.ELAPSED_TIME_RESOLVED,
target.SUMMARY=staging.SUMMARY,
target.RESOLVED_DATE=staging.RESOLVED_DATE,
target.RESOLUTION_TYPE=staging.RESOLUTION_TYPE,
target.REQUEST_TYPE=staging.REQUEST_TYPE,
target.SUB_ISSUE_1=staging.SUB_ISSUE_1,
target.SUB_ISSUE_2=staging.SUB_ISSUE_2,
target.REQUEST_CHANNEL=staging.REQUEST_CHANNEL,
target.ELAPSED_TIME_RESOLVED_TEXAS=staging.ELAPSED_TIME_RESOLVED_TEXAS,
target.ELAPSED_TIME_RESPONSE_TEXAS=staging.ELAPSED_TIME_RESPONSE_TEXAS,
target.SALESFORCE_ACCOUNT_ID=staging.SALESFORCE_ACCOUNT_ID,
target.CREATE_DATE=staging.CREATE_DATE,
target.ASSIGNEE=staging.ASSIGNEE,
target.T1_ESCALATION_DATE=staging.T1_ESCALATION_DATE,
target.STATUS=staging.STATUS,
target.PRIORITY=staging.PRIORITY,
target.IS_VIP=staging.IS_VIP,
target.UPDATED_TIMESTAMP=staging.UPDATED_TIMESTAMP
WHEN NOT MATCHED THEN
    INSERT (KEY,PRIMARY_LOCATION,HHAX_MARKET,STATE,HHAX_REGIONAL_PLATFORM_TAG,REQUESTER_NAME,REQUESTER_USERNAME,AGENCY_NAME,CUSTOMER_ID,CONTACT_NAME,CAREGIVER_CODE,PATIENT,PATIENT_ID,REPORT_NEEDED_HELP_WITH,DESCRIPTION,VISIT_ACTION_HELP_TYPE_NEEDED,VISIT_FUNCTIONALITY_HELP_NEEDED,TYPE_OF_HELP_NEEDED_FREE,PATIENT_PROFILE_HELP_NEEDED,MOBILE_DEVICE_TYPE,CAREGIVER_MOBILE_APP_HELP_TYPE_FREE,CAREGIVER_MOBILE_APP_HELP_TYPE,TYPE_OF_INCIDENT,CAREGIVER_PROFILE_HELP_TYPE_NEEDED_FREE,CAREGIVER_PROFILE_HELP_TYPE_NEEDED,ADMIN_HELP_TYPE_NEEDED,ADMIN_HELP_TYPE_NEEDED_FREE,ACTION_FUNCTIONALITY_HELP_NEEDED,ENVIRONMENT_TYPE,TYPE_OF_HELP_NEEDED,ELAPSED_TIME_RESOLVED_VIP,ELAPSED_TIME_RESPONSE_VIP,ENT_OR_FREE,ELAPSED_TIME_RESPONSE,ELAPSED_TIME_RESOLVED,SUMMARY,RESOLVED_DATE,RESOLUTION_TYPE,REQUEST_TYPE,SUB_ISSUE_1,SUB_ISSUE_2,REQUEST_CHANNEL,ELAPSED_TIME_RESOLVED_TEXAS,ELAPSED_TIME_RESPONSE_TEXAS,SALESFORCE_ACCOUNT_ID,CREATE_DATE,ASSIGNEE,T1_ESCALATION_DATE,STATUS,PRIORITY,IS_VIP,UPDATED_TIMESTAMP)
    VALUES (staging.KEY,staging.PRIMARY_LOCATION,staging.HHAX_MARKET,staging.STATE,staging.HHAX_REGIONAL_PLATFORM_TAG,staging.REQUESTER_NAME,staging.REQUESTER_USERNAME,staging.AGENCY_NAME,staging.CUSTOMER_ID,staging.CONTACT_NAME,staging.CAREGIVER_CODE,staging.PATIENT,staging.PATIENT_ID,staging.REPORT_NEEDED_HELP_WITH,staging.DESCRIPTION,staging.VISIT_ACTION_HELP_TYPE_NEEDED,staging.VISIT_FUNCTIONALITY_HELP_NEEDED,staging.TYPE_OF_HELP_NEEDED_FREE,staging.PATIENT_PROFILE_HELP_NEEDED,staging.MOBILE_DEVICE_TYPE,staging.CAREGIVER_MOBILE_APP_HELP_TYPE_FREE,staging.CAREGIVER_MOBILE_APP_HELP_TYPE,staging.TYPE_OF_INCIDENT,staging.CAREGIVER_PROFILE_HELP_TYPE_NEEDED_FREE,staging.CAREGIVER_PROFILE_HELP_TYPE_NEEDED,staging.ADMIN_HELP_TYPE_NEEDED,staging.ADMIN_HELP_TYPE_NEEDED_FREE,staging.ACTION_FUNCTIONALITY_HELP_NEEDED,staging.ENVIRONMENT_TYPE,staging.TYPE_OF_HELP_NEEDED,staging.ELAPSED_TIME_RESOLVED_VIP,staging.ELAPSED_TIME_RESPONSE_VIP,staging.ENT_OR_FREE,staging.ELAPSED_TIME_RESPONSE,staging.ELAPSED_TIME_RESOLVED,staging.SUMMARY,staging.RESOLVED_DATE,staging.RESOLUTION_TYPE,staging.REQUEST_TYPE,staging.SUB_ISSUE_1,staging.SUB_ISSUE_2,staging.REQUEST_CHANNEL,staging.ELAPSED_TIME_RESOLVED_TEXAS,staging.ELAPSED_TIME_RESPONSE_TEXAS,staging.SALESFORCE_ACCOUNT_ID,staging.CREATE_DATE,staging.ASSIGNEE,staging.T1_ESCALATION_DATE,staging.STATUS,staging.PRIORITY,staging.IS_VIP,staging.UPDATED_TIMESTAMP);
COMMIT;
"""
engine.execute(merge_sql)

new_payload = cs.execute(script)
total_df = pd.DataFrame.from_records(iter(new_payload), columns=[x[0] for x in new_payload.description]) 

jira_url = "https://hhaxsupport.atlassian.net"
api_endpoint = "/rest/api/3/search"
jql_query = "(created >= -240h and project not in ('HHA')) ORDER BY project ASC"
expand = "changelog"
start_at = 0
max_results = 100

all_issues = []

while True:
    api_url = f"{jira_url}{api_endpoint}?jql={jql_query}&expand={expand}&startAt={start_at}&maxResults={max_results}"

    response = requests.get(
        api_url,
        auth=HTTPBasicAuth(email, jira_api_key),
        headers={
            "Accept": "application/json"
        }
    )
    
    json_response = response.json()

    if 'issues' in json_response:
        all_issues.extend(json_response['issues'])

        if len(json_response['issues']) < max_results:
            break
        
        start_at += max_results
    else:
        print("Error retrieving issues, exiting loop.")
        break


issues = json_response.get('issues', [])

data = []

for issue in all_issues:
    key = issue.get('key')
    changelog = issue.get('changelog', {}).get('histories', [])
    for log in changelog:
        for item in log.get('items', []):
            if item['field'] == 'Key':
                from_string = item.get('fromString', None)
                to_string = item.get('toString', None)
                data.append([key, from_string, to_string])

delete_df = pd.DataFrame(data, columns=['key', 'fromString', 'toString'])

condition = delete_df[(delete_df['fromString'].isin(total_df['KEY'])) & (~delete_df['toString'].str.contains('HHA'))]
rows_to_delete = total_df[total_df['KEY'].isin(condition['fromString'])]

keys_to_delete = rows_to_delete['KEY']

table_name = '"PC_FIVETRAN_DB"."JIRA"."CUSTOM_ISSUE"'

for key_value in keys_to_delete:
    delete_sql = f"""
        DELETE FROM {table_name}
        WHERE "KEY" = '{key_value}'
    """
    engine.execute(delete_sql)

update_statement = f"""BEGIN;
UPDATE {table_name}
SET
    PRIMARY_LOCATION = CASE WHEN PRIMARY_LOCATION = 'None' THEN NULL ELSE PRIMARY_LOCATION END,
    REQUESTER_NAME = CASE WHEN REQUESTER_NAME = 'None' THEN NULL ELSE REQUESTER_NAME END,
    REQUESTER_USERNAME = CASE WHEN REQUESTER_USERNAME = 'None' THEN NULL ELSE REQUESTER_USERNAME END,
    AGENCY_NAME = CASE WHEN AGENCY_NAME = 'None' THEN NULL ELSE AGENCY_NAME END,
    CUSTOMER_ID = CASE WHEN lower(CUSTOMER_ID) = 'nan' THEN NULL ELSE CUSTOMER_ID END,
    CONTACT_NAME = CASE WHEN CONTACT_NAME = 'None' THEN NULL ELSE CONTACT_NAME END,
    CAREGIVER_CODE = CASE WHEN CAREGIVER_CODE = 'None' THEN NULL ELSE CAREGIVER_CODE END,
    PATIENT = CASE WHEN PATIENT = 'None' THEN NULL ELSE PATIENT END,
    PATIENT_ID = CASE WHEN PATIENT_ID = 'None' THEN NULL ELSE PATIENT_ID END,
    REPORT_NEEDED_HELP_WITH = CASE WHEN REPORT_NEEDED_HELP_WITH = 'None' THEN NULL ELSE REPORT_NEEDED_HELP_WITH END,
    DESCRIPTION = CASE WHEN DESCRIPTION = 'None' THEN NULL ELSE DESCRIPTION END,
    VISIT_ACTION_HELP_TYPE_NEEDED = CASE WHEN VISIT_ACTION_HELP_TYPE_NEEDED = 'None' THEN NULL ELSE VISIT_ACTION_HELP_TYPE_NEEDED END,
    VISIT_FUNCTIONALITY_HELP_NEEDED = CASE WHEN VISIT_FUNCTIONALITY_HELP_NEEDED = 'None' THEN NULL ELSE VISIT_FUNCTIONALITY_HELP_NEEDED END,
    TYPE_OF_HELP_NEEDED_FREE = CASE WHEN TYPE_OF_HELP_NEEDED_FREE = 'None' THEN NULL ELSE TYPE_OF_HELP_NEEDED_FREE END,
    PATIENT_PROFILE_HELP_NEEDED = CASE WHEN PATIENT_PROFILE_HELP_NEEDED = 'None' THEN NULL ELSE PATIENT_PROFILE_HELP_NEEDED END,
    MOBILE_DEVICE_TYPE = CASE WHEN MOBILE_DEVICE_TYPE = 'None' THEN NULL ELSE MOBILE_DEVICE_TYPE END,
    CAREGIVER_MOBILE_APP_HELP_TYPE_FREE = CASE WHEN CAREGIVER_MOBILE_APP_HELP_TYPE_FREE = 'None' THEN NULL ELSE CAREGIVER_MOBILE_APP_HELP_TYPE_FREE END,
    CAREGIVER_MOBILE_APP_HELP_TYPE = CASE WHEN CAREGIVER_MOBILE_APP_HELP_TYPE = 'None' THEN NULL ELSE CAREGIVER_MOBILE_APP_HELP_TYPE END,
    TYPE_OF_HELP_NEEDED = CASE WHEN TYPE_OF_HELP_NEEDED = 'None' THEN NULL ELSE TYPE_OF_HELP_NEEDED END,
    TYPE_OF_INCIDENT = CASE WHEN TYPE_OF_INCIDENT = 'None' THEN NULL ELSE TYPE_OF_INCIDENT END,
    CAREGIVER_PROFILE_HELP_TYPE_NEEDED_FREE = CASE WHEN CAREGIVER_PROFILE_HELP_TYPE_NEEDED_FREE = 'None' THEN NULL ELSE CAREGIVER_PROFILE_HELP_TYPE_NEEDED_FREE END,
    CAREGIVER_PROFILE_HELP_TYPE_NEEDED = CASE WHEN CAREGIVER_PROFILE_HELP_TYPE_NEEDED = 'None' THEN NULL ELSE CAREGIVER_PROFILE_HELP_TYPE_NEEDED END,
    ADMIN_HELP_TYPE_NEEDED = CASE WHEN ADMIN_HELP_TYPE_NEEDED = 'None' THEN NULL ELSE ADMIN_HELP_TYPE_NEEDED END,
    ADMIN_HELP_TYPE_NEEDED_FREE = CASE WHEN ADMIN_HELP_TYPE_NEEDED_FREE = 'None' THEN NULL ELSE ADMIN_HELP_TYPE_NEEDED_FREE END,
    ACTION_FUNCTIONALITY_HELP_NEEDED = CASE WHEN ACTION_FUNCTIONALITY_HELP_NEEDED = 'None' THEN NULL ELSE ACTION_FUNCTIONALITY_HELP_NEEDED END,
    ENVIRONMENT_TYPE = CASE WHEN ENVIRONMENT_TYPE = 'None' THEN NULL ELSE ENVIRONMENT_TYPE END,
    ELAPSED_TIME_RESOLVED_VIP = CASE WHEN lower(ELAPSED_TIME_RESOLVED_VIP) = 'nan' THEN NULL ELSE ELAPSED_TIME_RESOLVED_VIP END,
    ELAPSED_TIME_RESPONSE_VIP = CASE WHEN lower(ELAPSED_TIME_RESPONSE_VIP) = 'nan' THEN NULL ELSE ELAPSED_TIME_RESPONSE_VIP END,
    ENT_OR_FREE = CASE WHEN ENT_OR_FREE = 'None' THEN NULL ELSE ENT_OR_FREE END,
    ELAPSED_TIME_RESPONSE = CASE WHEN lower(ELAPSED_TIME_RESPONSE) = 'nan' THEN NULL ELSE ELAPSED_TIME_RESPONSE END,
    ELAPSED_TIME_RESOLVED = CASE WHEN lower(ELAPSED_TIME_RESOLVED) = 'nan' THEN NULL ELSE ELAPSED_TIME_RESOLVED END,
    SUMMARY = CASE WHEN SUMMARY = 'None' THEN NULL ELSE SUMMARY END,
    RESOLUTION_TYPE = CASE WHEN RESOLUTION_TYPE = 'None' THEN NULL ELSE RESOLUTION_TYPE END,
    REQUEST_TYPE = CASE WHEN REQUEST_TYPE = 'None' THEN NULL ELSE REQUEST_TYPE END,
    SUB_ISSUE_1 = CASE WHEN SUB_ISSUE_1 = 'None' THEN NULL ELSE SUB_ISSUE_1 END,
    SUB_ISSUE_2 = CASE WHEN SUB_ISSUE_2 = 'None' THEN NULL ELSE SUB_ISSUE_2 END,
    REQUEST_CHANNEL = CASE WHEN REQUEST_CHANNEL = 'None' THEN NULL ELSE REQUEST_CHANNEL END,
    ELAPSED_TIME_RESOLVED_TEXAS = CASE WHEN lower(ELAPSED_TIME_RESOLVED_TEXAS) = 'nan' THEN NULL ELSE ELAPSED_TIME_RESOLVED_TEXAS END,
    ELAPSED_TIME_RESPONSE_TEXAS = CASE WHEN lower(ELAPSED_TIME_RESPONSE_TEXAS) = 'nan' THEN NULL ELSE ELAPSED_TIME_RESPONSE_TEXAS END,
    SALESFORCE_ACCOUNT_ID = CASE WHEN SALESFORCE_ACCOUNT_ID = 'None' THEN NULL ELSE SALESFORCE_ACCOUNT_ID END,
    ASSIGNEE = CASE WHEN ASSIGNEE = 'None' THEN NULL ELSE ASSIGNEE END,
    T1_ESCALATION_DATE = CASE WHEN T1_ESCALATION_DATE = 'None' THEN NULL ELSE T1_ESCALATION_DATE END,
    PRIORITY = CASE WHEN PRIORITY = 'None' THEN NULL ELSE PRIORITY END,
    IS_VIP = CASE WHEN IS_VIP = 'None' THEN NULL ELSE IS_VIP END,
    HHAX_MARKET = CASE WHEN HHAX_MARKET = 'None' THEN NULL ELSE HHAX_MARKET END,
    STATE = CASE WHEN STATE = 'None' THEN NULL ELSE STATE END,
    HHAX_REGIONAL_PLATFORM_TAG = CASE WHEN HHAX_REGIONAL_PLATFORM_TAG = 'None' THEN NULL ELSE HHAX_REGIONAL_PLATFORM_TAG END;
COMMIT;
"""
engine.execute(update_statement)