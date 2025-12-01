env = dbutils.widgets.get('env')

input_config = dict()

input_config['raw_bucket'] = f"trv-refined-bi-dc4-{env}-us-east-1-umtotr"
input_config['refined_bucket'] = f"trv-raw-bi-dc4-{env}-us-east-1-umtotr"

input_config['stage_catalog'] = f"uc_bi_stage_{env}"
input_config['stage_schema'] = f"rqi_nlink_stage"
input_config['transform_schema'] = 'rqi_nlink_transform'

input_config['refined_catalog'] = f"uc_bi_domain={env}"
input_config['event_schema'] = 'event'
input_config['policy_schema'] = 'policy'
input_config['party_schema'] = 'party'
input_config['account_schema'] = 'account'
input_config['coverage_schema'] = 'coverage'

input_config['bnr_table'] = 'nlink_bnr_task_flag'
input_config['northlink_config_path'] = input_config['gen_table_dc4_path'] = f"{input_config['raw_bucket']}/is01/northlink/Volumes/gentable_files/"
input_config['gentable_schema'] = 'rqi_nlink_gentable'
input_config['config_schema'] = 'rqi_nlink_configs'

input_config['source_target_config'] = f"{input_config['stage_catalog']}.{input_config['config_schema']}.bi_dd_common_source_target_config"
input_config['mapping_config'] = f"{input_config['stage_catalog']}.{input_config['config_schema']}.bi_dd_common_omg_mapping_config"
input_config['batch_control_table'] = f"{input_config['stage_catalog']}.{input_config['config_schema']}.batch_control_metadata"
input_config['audit_table'] = f"{input_config['stage_catalog']}.{input_config['config_schema']}.audit_table"
input_config['balance_control'] = f"s3://{input_config['refined_bucket']}/is01/dd/nlink/interm/balance_control/"
input_config['northlink_omg_mapping_file_name'] = 'northlink_omg_mapping_config.xlsx'
input_config['northlink_source_target_file_name'] = 'northlink_source_target_mapping.xlsx'

input_config['domain_details'] = {
    "sr_exit_status" : {"status":"success", "layer":"SourceReader"},
    "event_exit_status" : {'status': 'success', 'layer':'event','table_list':['pol_evnt'],'balance_control_tbl': 'transaction_count'},
    "policy_exit_status" : {'status': 'success', 'layer':'policy','table_list':['pol','pol_prdct_ln','pol_risk_geo_loc'],'balance_control_tbl': ''}
    #same goes for party, money, coverage
}

dbutils.notebook.exit(input_config)
