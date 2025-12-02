import json
import sys
from pyspark.sql.functions import *
from pyspark.sql.types import *

#MAGIC %run ./utilsShared
#MAGIC %run ./customLogger

utils = UtilsShared(spark=spark)

#Get Widgets
table = dbutils.widgets.get("table")
view_name = dbutils.widgets.get("view_name")
stage_catalog = dbutils.widgets.get("stage_catalog")
transform_schema = dbutils.widgets.get("transform_schema")
env = dbutils.widgets.get("env")

table_name = f'{table}_LOAD'

df_load = spark.sql(f"SELECT * from global_temp.{view_name}")

required_schema = spark.table(f"{stage_catalog}.{transform_schema}.{table_name}").schema

if utils.validate_df_load(df_load, table_name, required_schema):
    print("Writing")

try:

    utils.write_delta(df_load, f"{stage_catalog}.{transform_schema}.{table_name}", mode="append", merge_schema="False",table_dest_path="")
    spark.sql(f"OPTIMIZE {stage_catalog}.{transform_schema}.{table_name}")
    exit_status = {
        'status':'Success',
        'table_name': table_name,
        'env':env
    }

except Exception as e:

    exit_status = {
    'status':'fail',
    'env':env,
    'table_name':table_name,
    'error':f"{str(e) - {table_name}}"
}

dbutils.notebook.exit(json.dumps(exit_status))
