import json
import ast
import re
import pytz
from pyspark.sql.functions import *
from pyspark.sql.types import *
from concurrent.futures import ThreadPoolExecutor
from datetime import *
from dateutil import parser

#MAGIC %run ./utilsShared
#MAGIC %run ./customLogger

#Fetching Widget Values
domain = dbutils.widgets.get('domain')
job_run_id = int(dbutils.widgets.get('job_run_id'))
task_run_id = int(dbutils.widgets.get('task_run_id'))
job_name = dbutils.widgets.get('job_name')
task_name = dbutils.widgets.get('task_name')

#Fetching Runtime Task Values
env = dbutils.job.taskValues.get('setRunTimeParams','env')
load_type = dbutils.job.taskValues.get('setRunTimeParams','load_type')
stage_catalog = dbutils.job.taskValues.get('setRunTimeParams','stage_catalog')
stage_schema = dbutils.job.taskValues.get('setRunTimeParams','stage_schema')
transform_schema = dbutils.job.taskValues.get('setRunTimeParams','transform_schema')
gentable_schema = dbutils.job.taskValues.get('setRunTimeParams','gentable_schema')
source_target_config = dbutils.job.taskValues.get('setRunTimeParams','source_target_config')
balance_control_path = dbutils.job.taskValues.get('setRunTimeParams','balance_control_path')
min_src_batch_id = dbutils.job.taskValues.get('setRunTimeParams','min_src_batch_id')
max_src_batch_id = dbutils.job.taskValues.get('setRunTimeParams','max_src_batch_id')
SRC_SYS_CD = dbutils.job.taskValues.get('setRunTimeParams','SRC_SYS_CD')
refined_catalog = dbutils.job.taskValues.get('setRunTimeParams','refined_catalog')
event_schema = dbutils.job.taskValues.get('setRunTimeParams','event_schema')
load_batch_id = dbutils.job.taskValues.get('setRunTimeParams','load_batch_id')

#Predefined Values
interm_schema = f"interm_nlink_{str(min_src_batch_id)}_{str(max_src_batch_id)}"
source_name = "northlink"

#Create Source Temp Views

try:
    df_config = spark.read.table(source_target_config)
    df_filtered = df_config.where(f"source_name == '{source_name}' and domain == '{domain}'")

    driving_entity_list = df_filtered.select('driving_entity_list').first()[0]
    source_entity_list = df_filtered.select('source_entity_list').first()[0]
    all_src_drv_list = driving_entity_list + source_entity_list

    for table in all_src_drv_list:
        spark.table(f"{stage_catalog}.{interm_schema}.{table}_view").createOrReplaceTempView(f"{table}_view")

except Exception as e:

    err_msg = f"Error Occured while reading the table: {str(e)}"
    exit_status = {
        "status":"failure",
        "layer":f"{domain}",
        "table_list":[],
        "balance_control_tbl":''
    }
    dbutils.job.taskValues.set(f"{domain}_exit_status",exit_status)
    raise ValueError(err_msg)

#OMG Mapping

#MAGIC %run ./domainProcess/omgMapping

#A dictionary with key being the table name and value being the global temp view name
table_dict = dict(zip(entity_list,gtv_list))

#MAGIC %run ./domainProcess/policyValidation

#OMG Writing
def run_notebook(table, view_name):
    params = {"table":table, "view_name":view_name, "stage_catalog":stage_catalog, "transform_schema":transform_schema, "env":env}
    status = dbutils.notebook.run("./domainProcess/omgWriter",0,params)
    return status

with ThreadPoolExecutor() as executor:
    futures = [executor.submit(run_notebook, k, v) for k, v in table_dict.items()]

#Failure Checks
failed_tables = []
try:
    failed_tables = [ast.literal_eval(future.result())['table_name'] for future in futures if ast.literal_eval(future.result())['status'].lower() in ['fail','failure']]
    if len(failed_tables) != 0:
        err_msg = f"The following tables failed to process: {failed_tables}"
        raise ValueError(err_msg)
except Exception as e:
    err_msg = f"An error occured in Failure Checks: {str(e)}"
    exit_status = {
        "status":"failure",
        "layer":f"{domain}",
        "table_list":list(table_dict.keys()),
        "balance_control_tbl":''
    }
    dbutils.job.taskValues.set(f"{domain}_exit_status",exit_status)
    raise ValueError(err_msg)

#MAGIC %run ./domainProcess/balanceControl

#Delete the global and temp views created

for table in all_src_drv_list:
    spark.catalog.dropTempView(f"{table}_view")

for global_view in gtv_list:
    spark.catalog.dropGlobalTempView(global_view)

exit_status = {
     "status":"success",
        "layer":f"{domain}",
        "table_list":list(table_dict.keys()),
        "balance_control_tbl":balance_control_tbl
}
