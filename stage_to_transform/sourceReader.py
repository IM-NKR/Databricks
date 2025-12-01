import json
import ast
import pytz
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import *
from concurrent.futures import ThreadPoolExecutor

no_batch_flag = dbutils.jobs.taskValues.get('SetRunTimeParams','no_batch_flag')
if no_batch_flag == "true":
    dbutils.notebook.exit("No batch to load")

#MAGIC %run ./utilsShared
#MAGIC %run ./customLogger

utils = UtilsShared(spark=spark)

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
source_target_config = dbutils.job.taskValues.get('setRunTimeParams','source_target_config')
mapping_config = dbutils.job.taskValues.get('setRunTimeParams','mapping_config')
min_src_batch_id = dbutils.job.taskValues.get('setRunTimeParams','min_src_batch_id')
max_src_batch_id = dbutils.job.taskValues.get('setRunTimeParams','max_src_batch_id')

#Predefined Values
interm_schema = f"interm_nlink_{str(min_src_batch_id)}_{str(max_src_batch_id)}"
source_name = "northlink"

#Get the Driving and Source Entity List 
def get_driving_source_list(source_target_config, source_name, domains):

    try:
        df_config = spark.read.table(source_target_config)

        all_domains = ','.join([f' "{domain}"' for domain in domains])

        df_filtered = df_config.where(
            f"source_name = '{source_name}' and domain in {all_domains})"
        )

        driving_entity_list = set(chain.from_iterable([d.driving_entity_list for d in df_filtered.select('driving_entity_list').collect()]))
        
        source_entity_list = set(chain.from_iterable([d.source_entity_list for d in df_filtered.select('source_entity_list').collect()]))
        
        return (driving_entity_list, source_entity_list)

    except Exception as e:

        err_msg = f"Error in function get_driving_source_list - {str(e)}"
        sr_exit_status = {
            "status":"failure",
            "layer":"SourceReader"
        }
        dbutils.jobs.taskValues.set("sr_exit_status", sr_exit_status)
        raise ValueError(err_msg)
    
#Handle DatetimeFormats

def handle_date_time_formats(date_cols, timestamp_cols, table_df_casted):

    try:

        formats = ["dd-MM-yyyy HH:mm:ss", "yyy-MM-dd'T'HH:mm:ss", "yyyy-MM-dd", "yyyy/MM/dd", "MM-dd-yyyy", "yyyy.MM.dd HH:mm:ss", "yyyy-MM-dd HH:mm:ss.SSS"]

        if len(date_cols) != 0:
            for d in date_cols:
                table_df_casted = table_df_casted.withColumn(d, to_date(coalesce(*[to_timestamp(d, fmt) for fmt in formats])))
        
        if len(timestamp_cols) != 0:
            for d in date_cols:
                table_df_casted = table_df_casted.withColumn(d, to_timestamp(coalesce(*[to_timestamp(d, fmt) for fmt in formats])))

        return table_df_casted

    except Exception as e:

        err_msg = f"Error in function get_driving_source_list - {str(e)}"
        sr_exit_status = {
            "status":"failure",
            "layer":"SourceReader"
        }
        dbutils.jobs.taskValues.set("sr_exit_status", sr_exit_status)
        raise ValueError(err_msg)
    
#Apply Cast and Create Views
def cast_and_create_view(table_df, source_name, mapping_config, table_name):

    try:
        df_map = spark.table(mapping_config)

        map_filtered = df_map(f"source_name == '{source_name}' and table_name = '{table_name}' ").select('omg_mapping').first()[0]
        column_cast_mapping = {k.lower():v for k,v in map_filtered.items()}

        expressions = []
        date_cols = []
        timestamp_cols = []

        for col_name in table_df.columns:

            if col_name() in column_cast_mapping.keys() and column_cast_mapping[col_name.lower()].lower() == "string":
                expressions.append(trim(col(col_name)).cast(column_cast_mapping[col_name.lower()]).alias(col_name))
            
            elif col_name.lower() in column_cast_mapping.keys() and column_cast_mapping[col_name.lower()].lower() == "date":
                date_cols.append(col_name)
                expressions.append(col(col_name))
            elif col_name.lower() in column_cast_mapping.keys() and column_cast_mapping[col_name.lower()].lower() == "timestamp":
                date_cols.append(col_name)
                expressions.append(col(col_name))
            elif col_name.lower() in column_cast_mapping.keys() and column_cast_mapping[col_name.lower()].lower() not in ['string', 'date', 'timestamp']:
                expressions.append(col(col_name)).cast(column_cast_mapping[col_name.lower()].alias(col_name))
            else:
                expressions.append(col(col_name))
        
        table_df_type_casted = table_df.select(*expressions)

        table_df_type_casted = handle_date_time_formats(date_cols, timestamp_cols, table_df_type_casted)

        view_name = f"{table_name}_view"

        table_df_type_casted.createOrReplaceTempView(view_name)

        return view_name
    
    except Exception as e:

        err_msg = f"Error in function cast_and_create_view: {str(e)}"
        sr_exit_status = {
            "status":"failure",
            "layer":"SourceReader"
        }
        dbutils.jobs.taskValues.set("sr_exit_status", sr_exit_status)
        raise ValueError(err_msg) 

driving_entity_list, source_entity_list = get_driving_source_list(source_target_config, source_name, domain)

#Pull Forward Logic
try:

    ol_min_src_batch_id = min_src_batch_id
    if load_type != "hist":
        min_src_batch_id = int(str(int(str(min_src_batch_id)[:4]) -2) + "010100")

except Exception as e:
    err_msg = f"Error in Pull Forward Logic {str(e)}"
    sr_exit_status = {
            "status":"failure",
            "layer":"SourceReader"
        }
    dbutils.jobs.taskValues.set("sr_exit_status", sr_exit_status)
    raise ValueError(err_msg) 

#Creating Driving Entity Temp Views
driving_view_list = list()

try:
    for dri in driving_entity_list:

        query = F""" SELECT * FROM {stage_catalog}.{stage_schema}.{dri} a where a.src_batch_id >= {ol_min_src_batch_id} and a.src_batch_id <= {max_src_batch_id} """

        driving_df = spark.sql(query)

        driving_view_list.append(cast_and_create_view(driving_df, source_name, mapping_config, dri))

except Exception as e:

    err_msg = f"Error processing driving entity list: {str(e)}"
    sr_exit_status = {
            "status":"failure",
            "layer":"SourceReader"
        }
    dbutils.jobs.taskValues.set("sr_exit_status", sr_exit_status)
    raise ValueError(err_msg) 

#Creating Source Entity Temp Views
source_view_list = list()

try:
    for src in source_entity_list:

        query = F""" SELECT * FROM {stage_catalog}.{stage_schema}.{src} a where a.src_batch_id >= {ol_min_src_batch_id} and a.src_batch_id <= {max_src_batch_id} """

        source_df = spark.sql(query)

        source_view_list.append(cast_and_create_view(source_df, source_name, mapping_config, src))

except Exception as e:

    err_msg = f"Error processing source entity list: {str(e)}"
    sr_exit_status = {
            "status":"failure",
            "layer":"SourceReader"
        }
    dbutils.jobs.taskValues.set("sr_exit_status", sr_exit_status)
    raise ValueError(err_msg) 

view_list = list(set(driving_view_list + source_view_list))

#Issued data Count Check

try:
    dbutils.jobs.taskValues.set("no_data_flag","false")
    data_check_query = f""" SELE count(*) from driving_entity_view"""
    data_count = spark.sql(data_check_query).collect()[0][0]

    if data_count == 0:
        dbutils.jobs.taskValues.set("no_data_flag","true")
        exit_status = {
            "error": "no_data",
            "status":"fail"
        }
        dbutils.notebook.exit(json.dumps(exit_status))

except Exception as e:
    err_msg = f"Error processing source entity list: {str(e)}"
    sr_exit_status = {
            "status":"failure",
            "layer":"SourceReader"
        }
    dbutils.jobs.taskValues.set("sr_exit_status", sr_exit_status)
    raise ValueError(err_msg) 

#write Intrm Temp Views
def write_intrm_view(item):
    
    try:
        df = spark.sql(F"""select * from {item} """)
        df.write.format("delta").mode("overwrite").saveAsTable(f"{interm_schema}.{item}")
        spark.sql(f"Optimize {interm_schema}.{item}")

    except Exception as e:
        err_msg = f"Error processing source entity list: {str(e)}"
    sr_exit_status = {
            "status":"failure",
            "layer":"SourceReader"
        }
    dbutils.jobs.taskValues.set("sr_exit_status", sr_exit_status)
    raise ValueError(err_msg) 

spark.sql(f"USE CATALOG {stage_catalog}")
spark.sql(f" CREATE SCHEMA IF NOT EXISTS {interm_schema}")

with ThreadPoolExecutor(max_worker=5) as executor:
    executor.map(write_Intrm_view, view_list)

sr_exit_status = {
            "status":"failure",
            "layer":"SourceReader"
        }
dbutils.jobs.taskValues.set("sr_exit_status", sr_exit_status)
