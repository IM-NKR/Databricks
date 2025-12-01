import json
import ast
import re
import pytz
import sys
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import *
from dateutil import parser


no_batch_flag = dbutils.jobs.taskValues.get('SetRunTimeParams','no_batch_flag')
if no_batch_flag == "true":
    dbutils.notebook.exit("No batch to load")

#MAGIC %run ./utilsShared
#MAGIC %run ./customLogger

#Get Task Values
update_type = dbutils.widgets.get('update_type')
stage_flag = dbutils.widgets.get('stage_flag')
transform_flag = dbutils.widgets.get('transform_flag')
refine_flag = dbutils.widgets.get('refine_flag')
job_run_id = dbutils.widgets.get('job_run_id')
task_run_id = dbutils.widgets.get('task_run_id')
job_name = dbutils.widgets.get('job_name')
task_name = dbutils.widgets.get('task_name')

load_batch_id = dbutils.jobs.taskValues.get('setRunTimeParams','load_batch_id')
batch_control_table = dbutils.jobs.taskValues.get('setRunTimeParams','batch_control_table')

#Update Batch Control Table

try:
    if update_type.lower() == "success":
        error_msg = ""
    elif update_type.lower() == "no_data":
        error_msg = "No valid data available in complete batch to process for domain"
    else:
        error_msg = "Main Code Failed"
    
    if isinstance(load_batch_id, list):

        (spark.table(batch_control_table).filter(col("load_batch_id").isin(load_batch_id))
                    .withColumn("transformation_completion_flag",lit(transform_flag).cast(StringType()))
                    .withColumn("stage_completion_flag", lit(stage_flag).cast(StringType()))
                    .withColumn("refine_completion_flag",lit(refine_flag).cast(StringType()))
                    .withColumn("update_timestamp",current_timestamp().cast(TimestampType()))
                    .withColumn("error_details", concat_ws(" ", col("error_details").cast(StringType()), lit(error_msg)))).createOrReplaceTempView(f"temp_view")

        spark.sql(f'''MERGE INTO {batch_control_table} a
                USING temp_view b
                on a.load_batch_id = b.load_batch_id
                AND a.stage_load_ts = b.stage_load_ts
                WHEN MATCHED THEN UPDATE SET *
                WHEN NOT MATCHED THEN INSERT *''')
        
    else:

        (spark.table(batch_control_table).filter(col("load_batch_id") == load_batch_id)
                    .withColumn("transformation_completion_flag",lit(transform_flag).cast(StringType()))
                    .withColumn("stage_completion_flag", lit(stage_flag).cast(StringType()))
                    .withColumn("refine_completion_flag",lit(refine_flag).cast(StringType()))
                    .withColumn("update_timestamp",current_timestamp().cast(TimestampType()))
                    .withColumn("error_details", concat_ws(" ", col("error_details").cast(StringType()), lit(error_msg)))).createOrReplaceTempView(f"temp_view")

        spark.sql(f'''MERGE INTO {batch_control_table} a
                USING temp_view b
                on a.load_batch_id = b.load_batch_id
                AND a.stage_load_ts = b.stage_load_ts
                WHEN MATCHED THEN UPDATE SET *
                WHEN NOT MATCHED THEN INSERT *''')
        
except Exception as e:

    err_msg = f"Error Occured while Updating Batch Control Metadata {str(e)}"
    raise ValueError(err_msg)
