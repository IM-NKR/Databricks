def OMG_entity_global_view(query_dict):

    try:
        entity = []
        gtv = []

        for key,value in query_dict.items():
            entity.append(key)
            create_view = f"spark.sql('''{value}''').createOrReplaceGlobalTempView('{key}_view')"
            exec(create_view)
            gtv.append(f'{key}_view')
        
        return entity,gtv

    except Exception as e:
        err_msg = f"Error Occured in OMG_entity_global_view: {str(e)}"
        exit_status = {
        "status":"failure",
        "layer":f"{domain}",
        "table_list":[],
        "balance_control_tbl":''
        }
        dbutils.job.taskValues.set(f"{domain}_exit_status",exit_status)
        raise ValueError(err_msg)

#MAGIC %run ../sqlLogic/sql_policy_logic
#MAGIC %run ../sqlLogic/sql_party_logic
#MAGIC %run ../sqlLogic/sql_event_logic

sname = source_name.replace('-','')
s = f'{sname}_{domain}_queries'

#MAGIC %run ../udf/gentable_event

if domain == 'event':

    try:
        dict_event = northlink_event_df(stage_catalog, gentable_schema)
        spark.udf.register("eventgen", eventgen)
    
    except Exception as e:
        err_msg = f"Error Occured in OMG_entity_global_view: {str(e)}"
        exit_status = {
        "status":"failure",
        "layer":f"{domain}",
        "table_list":[],
        "balance_control_tbl":''
        }
        dbutils.job.taskValues.set(f"{domain}_exit_status",exit_status)
        raise ValueError(err_msg)

elif domain == 'policy':

    try:
        dict_event = northlink_event_df(stage_catalog, gentable_schema)
        spark.udf.register("policygen", policygen)
    
    except Exception as e:
        err_msg = f"Error Occured in OMG_entity_global_view: {str(e)}"
        exit_status = {
        "status":"failure",
        "layer":f"{domain}",
        "table_list":[],
        "balance_control_tbl":''
        }
        dbutils.job.taskValues.set(f"{domain}_exit_status",exit_status)
        raise ValueError(err_msg)

else:
    print(f"There are no gen tables for domain {domain}")

entity_list, gtv_list = OMG_entity_global_view(locals()[s])
