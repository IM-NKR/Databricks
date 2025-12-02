import uuid
import json
from functools import reduce
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import *
from pyspark.dbutils import DBUtils
from delta.tables import DeltaTable
from datetime import datetime, timedelta
from pyspark.testing import assertDataFrameEqual, assertSchemaEqual

class UtilsShared:

    def __init__(self,spark: SparkSession):

        self.spark = spark
        self.dbutils = DBUtils(self.spark)
    
    def get_env(self, workspace_url):

        try:
            env = workspace_url.split('.')[0].split('-')[1]
            return env
        except Exception as e:
            raise ValueError(f"Error Retrieving env: {str(e)}")
    
    def validate_input_config(self, config, mandatory_keys):

        try:
            missing_keys = [key for key in mandatory_keys if key not in config]
            if missing_keys:
                raise ValueError(f"Missing Mandatory Keys: {','.join(missing_keys)}")
        except Exception as e:
            raise ValueError(f"Error validating input config: {str(e)}")
        
    def standardize_input_df(self, input_df, load_type):

        try:

            df_standard = (input_df.withColumn("temp_batch_date", concat_ws("-",col("year"),lpad(col("month"), 2, '0'),lpad(col("day"), 2, '0')))
                                .withColumn("temp_hour", when(lit(load_type) == "hist", lit(99)).otherwise(lpad(col("hour"), 2, '0')))
                                .withColumn("src_batch_date", to_date(col("temp_batch_date"), 'yyyy-MM-dd'))
                                .withColumn("src_batch_id", concat_ws("", regexp_replace("temp_batch_date", "-",""), col("temp_hour")).cast("int"))
                        )
            return df_standard

        except Exception as e:

            err_msg = f"Error standardizing input Dataframe: {str(e)}"
            raise ValueError(err_msg)
    
    def write_delta(self, Dataframe, table_name:str, mode:str='append', merge_schema:str="false", table_dest_path:str=""):
        
        try:
            if table_dest_path == "":
                print("Managed Table")
                
                if mode == "overwrite":
                    df.write.option("autoOptimize","true").option("mergeSchema",merge_schema).option("overwriteSchema","true").mode(mode).saveAsTable(table_name)

                elif mode == "append":
                    df.write.option("autoOptimize","true").option("mergeSchema",merge_schema).mode(mode).saveAsTable(table_name)

                else:
                    err_msg = f"write mode {mode} not defined"
                    raise ValueError(err_msg)
            
            else:
                print("External Tables")

                if mode == "overwrite":
                    df.write.option("autoOptimize","true").option("mergeSchema",merge_schema).option("overwriteSchema","true").option("path",table_dest_path).mode(mode).saveAsTable(table_name)

                elif mode == "append":
                    df.write.option("autoOptimize","true").option("mergeSchema",merge_schema).option("path",table_dest_path).mode(mode).saveAsTable(table_name)

                else:
                    err_msg = f"write mode {mode} not defined"
                    raise ValueError(err_msg)

        except Exception as e:
            err_msg = f"Error writing data to Delta Table {table_name}: {str(e)}"
            raise ValueError(err_msg)
        
    def writeStream_delta(self, df:Dataframe, table_name:str, table_check_point_path:str, mode:str='append',table_dest_path:str=""):

        try:

            if table_dest_path == "":
                print("Writing Managed table {table_name}")

                if mode == "overwrite":
                    self.dbutils.ds.rm(f"{table_dest_path}",recurse=True)
                    self.spark.sql(f"DROP TABLE EXISTS {table_name}")
                
                (df.writeStream
                .queryName(table_name)
                .option("checkpointLocation",f"{table_check_point_path}")
                .option("mergeSchema", "true")
                .option("autoOptimize","true")
                .trigger(availableNow=True)
                .toTable(table_name) 
                )

            else:
                print("Writing External table {table_name}")

                if mode == "overwrite":
                    self.spark.sql(f"DROP TABLE EXISTS {table_name}")
                    self.dbutils.ds.rm(f"{table_dest_path}",recurse=True)
                
                (df.writeStream
                .queryName(table_name)
                .option("checkpointLocation",f"{table_check_point_path}")
                .option("mergeSchema", "true")
                .option("autoOptimize","true")
                .trigger(availableNow=True)
                .toTable(table_name) 
                )
        
        except Exception as e:
            err_msg = f"Error writing stream to Delta table: {str(e)}"
            raise ValueError(err_msg)

    def check_table_exists(self, table_name:str):

        try:

            DeltaTable.forName(self.spark, table_name)
            print(f"Table '{table_name}' exists..")
            return True
        
        except Exception as e:

            print(f"Table '{table_name}' does not exists..")
            return False
        
    def alter_table_with_clustering(self, table_name:str, cluster_columns:list):

        try:

            print(f"Fetching table details for '{table_name}")
            detail_df = self.spark.sql(f"DESCRIBE DETAIL {table_name}")
            properties = detail_df.select("properties").first()["properties"]
            print("Check if clustering is already applied or not")
            liquid_clustered_enabled = properties.get('liquid.clustered.enabled','false') == 'true'
            print(f"liquid clustering enabled:{liquid_clustered_enabled}")
            
            if liquid_clustered_enabled:
                print("Setting liquid Cluster for already created table")
                self.spark.sql(f"ALTER TABLE {table_name} CLUSTER BY ({','.join(cluster_columns)})")
                self.spark.sql(f"ALTER TABLE {table_name} SE TBLPROPERTIES ('liquid.clustered.enabled' = 'true')")

            else:
                print("Fetch existing clustering columns")
                existing_cluster_colns = detail_df.select("clusteringColumns").first()["clusteringColumns"]
                missing_clusterkeys = list(set(existing_cluster_columns) - set(cluster_columns))
                extra_clusterkeys = list(set(cluster_columns) - set(existing_cluster_columns))

                if len(missing_clusterkeys) != 0 or len(extra_clusterkeys) !=0:
                    print("Modifying Liquid Cluster")
                    self.spark.sql(f"ALTER TABLE {table_name} CLUSTER BY ({','.join(cluster_columns)})")
        
        except Exception as e:
            err_msg = f"Error applying clustering: {str(e)}"
            raise ValueError(err_msg)
        
    def create_table_with_property(self, table_name:str, clustercols:dict, df_schema, table_dest_path:str, vector_flag:str):

        try:
            cluster_columns = list(clustercols.keys())

            if len(cluster_columns) != 0:

                if not self.check_table_exists(table_name):

                    print("Creating and setting liquid cluster with property")
                    DeltaTable.create(self.spark).tableName(table_name).addColumns(df_schema).clusterBy(cluster_columns).location(table_dest_path).property("liquid.clustered.enabled","true").property("delta.enableDeletionVectors",vector_flag).execute()

                else:
                    
                    self.alter_table_with_clustering(table_name, cluster_columns)
            
            else:

                if not self.check_table_exists(table_name):

                    print("Creating Table")
                    DeltaTable.create(self.spark).tableName(table_name).addColumns(df_schema).location(table_dest_path).property("liquid.clustered.enabled","true").property("delta.enableDeletionVectors",vector_flag).execute()

                else:
                    
                    print("Removing Liquid Cluster and Changing Property")
                    self.spark.sql(f"ALTER TABLE {table_name} CLUSTER BY NONE")
                    self.spark.sql(f"ALTER TABLE {table_name} SET TBLPROPERTIES ('liquid.clustered.enabled' = 'false' )")
                    self.spark.sql(f"ALTER TABLE {table_name} SET TBLPROPERTIES ('delta.enableDeletionVectors' = '{vector_flag}' )")
                    self.alter_table_with_clustering(table_name, cluster_columns)

        except Exception as e:
            err_msg = f"Error creating or altering table: {str(e)}"
            raise ValueError(err_msg)
        
    def write_batch_control(self, load_batch_id:str, s3_dest_path:str, batch_Control_table:str, source:str, stage_status:str, transform_status:str, refine_status:str, stage_load_ts:str, min_max_src_batch_id, error_message:str, mode:str):

        try:

            columns = ["source","stage_completion_flag","transformation_completion_flag","refine_completion_flag","load_batch_id"]
            data = [(source, stage_status, transform_status, refine_status, load_batch_id)]
            min_max_src_batch_id = [lit(mn) for mn in min_max_src_batch_id]
            base_df = (self.spark.createDataFrame(data, columns))

            batch_Control_df = (base_df.withColumn("batch_number", lit(None).cast(StringType()))
                                .withColumn("update_timestamp",current_timestamp().cast(TimestampType()))
                                .withColumn("insert_timestamp",current_timestamp().cast(TimestampType()))
                                .withColumn("error_details",lit(error_message).cast(StringType()))
                                .withColumn("stage_load_ts",lit(stage_load_ts).cast(TimestampType()))
                                .withColumn("min_max_src_batch_id",array(min_max_src_batch_id))
                                )
            
            if not sef.check_table_exists(f"{batch_Control_table}"):
                batch_Control_df.write.option("mergeSchem","true").option("path",s3_dest_path+batch_Control_table.split('.')[-1]).option("autoOptimize","true").mode(mode).saveAstable(f"{batch_Control_table}")
            
            else:
                batch_Control_df.createOrReplaceTempView(f"batch_control_view")
                self.spark.sql(f'''
                MERGE INTO {batch_Control_table} a
                USING batch_control_view b
                ON a.stage_load_ts = b.stage_load_ts
                and a.load_batch_id = b.load_batch_id
                WHEN MATCHED THEN UPDATE SET *
                WHEN NOT MATCHED THEn INSERT *
                ''')
        
        except Exception as e:
            raise ValueError(f"Error during batch control entry: {str(e)}")
        
    def validate_df_load(self,df_load:Dataframe, table_name:str, required_schema):

        try:

            df_load_cols = set([col.lower() for col in df_load.columns])
            require_cols = set([col.lower() for col in required_schema.names])

            print("Identify extra columns in Dataframe and missing columns required by the schema")

            extra_load_cols = df_load_cols.difference(required_cols)
            missing_req_cols = required_cols.difference(df_load_cols)

            diff_schema = {col.name: (col.dataType, df_load.schema[col.name].dataType for col in required_schema if col.name in df_load.schema.fieldNames() and col.dataType != df_load.schema[col.name].dataType)}
            
            print("Check if there are any extra or missing columns")

            if len(extra_load_cols) != 0 or len(missing_req_cols) != 0 or len(diff_schema) != 0:

                print(f"Found Extra Columns if df load - {str(extra_load_cols)} or Found missing columns in df load - {str(missing_req_cols)} or schema mismatch in df load {str(diff_schema)}")
                return False
            
            else:

                print(F"Validation successfull for table: {table_name}")
                return True
        

        except Exception as e:

            exit_status = {
                'status':'fail',
                'error':str(e)
            }
            print(f"Validating failed for table: {table_name}")
            raise ValueError(json.dumps(exit_status)) 
