
#Importing Libraries

import json
import pytz
import ast
from datetime import *
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Get Widgets
load_type = dbutils.widgets.get('load_type')
job_run_id = int(dbutils.widgets.get('job_run_id'))
task_run_id = int(dbutils.widgets.get('task_run_id'))
job_name = dbutils.widgets.get('job_name')
task_name = dbutils.widgets.get('task_name')
transform_check_flag = dbutils.widgets.get('transform_check_flag')

#Get Input Config Params

workspace = spark.conf.get("spark.databricks.workspaceUrl")
env = utils.get_env(workspace)
ic = ast.literal_eval(dbutils.notebook.run("./inputConfig",0,{"env":env}))
batch_control_table = ic["batch_control_table"]
audit_table = ic["audit_table"]
stage_catalog = ic["stage_catalog"]
stage_schema = ic["stage_schema"]
bnr_table = ic["bnr_table"]
transform_schema = ic["transform_schema"]

dbutils.job.taskValues.set("no_batch_flag", "false")

#Function to fetch the rows from a table based on year and month

def generate_min_max_src_batch_id(year, month):

    month_list = month.split(',')
    month = str(tuple([i for i in month_list])).replace(",)",")")

    if year == "" and month == "":

        filter_condition = " "

    else:

        filter_condition = f"and year(insert_timestamp) = {year} and month(insert_timestamp) in {month}"
        query = (f' SELECT insert_timestamp, min_max_src_batch_id, load_batch_id FROM {batch_control_table} where stage_completion_flag = "Y" and transformation_completion_flag = "{transform_check_flag}" and refine_completion_flag = "N" and load_batch_id not like "%_hist_%" {filter_condition}')
        print(query)

    df = spark.sql(query)

    return df

#Fetch Batch Control Metadata where stage is completed and transform and refine is not completed.

if load_type == 'hist':
    batch_control = spark.sql(F''' select load_batch_id, min_max_src_batch_id from {batch_control_table} 
                              where stage_completion_flag = 'Y' and transformation_completion_flag = '{transform_check_flag}' and load_batch_id like '%hist%' 
                              and CAST(SUBSTRING(load_batch_id,1,4) as INT) >=2016 order by SUBSTRING(load_batch_id,1,4) limit 1''')
    
elif load_type == 'incr':
    batch_control = spark.sql(F''' select load_batch_id, min_max_src_batch_id from {batch_control_table} 
                              where stage_completion_flag = 'Y' and transformation_completion_flag = '{transform_check_flag}' and load_batch_id like '%hist%'  limit 1''')
    
elif load_type == 'catchup':
    year = dbutils.widgets.get('year')
    month = dbutils.widgets.get('month')
    batch_control = generate_min_max_src_batch_id(year,month)

else:
    raise ValueError("Load Type is not Defined")

if not batch_control.isEmpty():

    dbutils.widgets.set("no_batch_flag", "false")

    if load_type != 'catchup':

        row = batch_control.orderBy("insert_timestamp").first()
        min_max_impt_btch_id = row["min_max_src_batch_id"]
        load_batch_id = row["load_batch_id"]
        min_src_batch_id = __builtins__.min(min_max_impt_btch_id)
        max_src_batch_id = __builtins__.max(min_max_impt_btch_id)
    
    else:

        timestamp_range = batch_control.agg(
            min("insert_timestamp").alias("min_ts"),
            max("insert_timestamp").alias("max_ts")
        ).collect()[0]

        min_ts = timestamp_range["min_ts"]
        max_ts = timestamp_range["max_ts"]

        #Get the batch IDs from records with min and max timestamps

        min_src_batch_id = batch_control.filter(col("insert_timestamp") == min_ts).select("min_max_src_batch_id").first()[0][0]
        max_src_batch_id = batch_control.filter(col("insert_timestamp") == max_ts).select("min_max_src_batch_id").first()[0][1]

        load_batch_id = [row[0] for row in batch_control.select(col("load_batch_id")).collect()]

        print(f"Processing Range : {min_src_batch_id} to {max_src_batch_id}")

else:

    min_src_batch_id = 0
    max_src_batch_id = 0
    load_batch_id = []

    dbutils.job.taskValues.set("no_batch_flag", "true")

#Get the value of the BNR Flag
bnr_table_name = f"{stage_catalog}.{transform_schema}.{bnr_table}"
bnr_flag_df = spark.sql(f" select bnr_flag from {bnr_table_name}")
flag_value = bnr_flag_df.collect()[0]['bnr_flag']

if flag_value == 'ENABLED':
    bnr_check = 'Yes'
else:
    bnr_check = 'No'

task_values = {
    'env':env,
    'stage_catalog':stage_catalog,
    'stage_schema':stage_schema,
    'transform_schema': transform_schema,
    'gentable_schema':ic['gentable_schema'],
    'source_target_config': ic['source_target_config'],
    'mapping_config':ic['mapping_config'],
    'batch_control_table':batch_control_table,
    'load_batch_id': load_batch_id,
    'balance_control_path': ic['balance_control'],
    'min_src_batch_id': min_src_batch_id,
    'max_src_batch_id': max_src_batch_id,
    'SRC_SYS_CD':ic['src_sys_cd'],
    'load_type':load_type,
    'refined_catalog':ic['refined_catalog'],
    'event_schema':ic['event_schema'],
    'policy_schema':ic['policy_schema'],
    'bnr_check':bnr_check,
    'domain_details':ic['domaind_details']
}

for key,value in task_values.items():
    dbutils.job.taskValues.set(key=key, value=value)
