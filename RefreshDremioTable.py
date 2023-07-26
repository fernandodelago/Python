# Databricks notebook source
import requests, urllib3, time
from pyspark.sql.functions import trim

# COMMAND ----------

# DBTITLE 1,Set variables
BASE_URL = '''https://dremio.mycompany.net'''
headers = {'Content-Type': 'application/json'}
v_service_id = dbutils.secrets.get(scope = 'name_of_scope', key = 'service-id')
v_service_pwd = dbutils.secrets.get(scope = 'name_of_scope', key = 'service-secret')
data = '{"userName": "' + str(v_service_id) + '", "password": "' + str(v_service_pwd) + '" }'

# COMMAND ----------

# DBTITLE 1,Set configuration to SSL conexion
# used to disable SSL
requests.packages.urllib3.disable_warnings()
requests.packages.urllib3.util.ssl_.DEFAULT_CIPHERS += ':HIGH:!DH:!aNULL'
try:
    requests.packages.urllib3.contrib.pyopenssl.util.ssl_.DEFAULT_CIPHERS += ':HIGH:!DH:!aNULL'
except:
    pass

# COMMAND ----------

# DBTITLE 1,UDF to getting job status
def udf_get_job_results(job_id):
    print('Waiting for the job to complete...')
    headers = {'Content-Type': 'application/json', 'Authorization':authorization_code}

    job_status = requests.request("GET", BASE_URL + "/api/v3/job/" +job_id, headers=headers,verify=False).json()['jobState']
    
    while job_status != 'COMPLETED':
        
        if job_status == 'FAILED':
            response = requests.request("GET", BASE_URL + "/api/v3/job/"+job_id+"/results", headers=headers,verify=False)
            rt_msg = '#E# Status ' + job_status + ' | ' + str(response.text)
            dbutils.notebook.exit(rt_msg)
        else:
            time.sleep(1)
            job_status = requests.request("GET", BASE_URL + "/api/v3/job/" +job_id, headers=headers,verify=False).json()['jobState']
    
    response = requests.request("GET", BASE_URL + "/api/v3/job/"+job_id+"/results", headers=headers,verify=False)
    rt_msg = '# Status ' + job_status + ' | ' + str(response.text)
    return rt_msg

# COMMAND ----------

# DBTITLE 1,UDF to send refresh to Dremio
def udf_send_refresh(v_schema, v_table):
    '''
    IMPORTANT: table names are case-sensitive in Dremio
    '''
    sql_data = '"ALTER PDS \\"pds_filename.folder_name.{v_schema}.{v_table} REFRESH METADATA FORCE UPDATE"'.format(v_schema = v_schema, v_table = v_table)
    data = '{"sql": ' + sql_data + '}'
    print(data)
    headers = {'Content-Type': 'application/json', 'Authorization':authorization_code}
    response = requests.post(BASE_URL + '/api/v3/sql', headers=headers, data=data,verify=False)

    job_id = response.json()['id']
    if response.status_code == 200:
        print ('Job creation successful. Job id is: ' + job_id)
    else:
        print('Job creation failed.')
        print(response.text)
        dbutils.notebook.exit(udf_get_job_results(job_id))
    return job_id

# COMMAND ----------

# DBTITLE 1,Do the connection
response = requests.post(BASE_URL + '/apiv2/login', headers=headers, data=data, verify=False)

if response.status_code != 200:
    v_msg = '#E# Status_code ' + str(response.status_code) + ' | ' + str(response.text)
    raise Exception(v_msg)

authorization_code = '_dremio' + response.json()['token'] # _dremio is prepended to the token

print('# Token received => ' + str(authorization_code))

# COMMAND ----------

# DBTITLE 1,Send refresh to fact table
job_id = udf_send_refresh('schema_name','table_name')

# COMMAND ----------

# DBTITLE 1,Verify job status of fact table
rt_msg = udf_get_job_results(job_id)
print(rt_msg)
