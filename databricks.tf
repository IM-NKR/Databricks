#Iterator Workflow
resource "databricks_job" "domains_iterator"
{
    name = var.jobname
    description = var.job_description
    max_concurrent_runs = 1
    task
    {
        task_key = "northlink_domains_iterator"
        for_each_task 
        {
            inputs = "[1,2,3,4,5,6,7,8,9]"
            concurrency = 1
            task 
            {
                task_key = "northlink_domains"
                run_if = "ALL_SUCCESS"
                run_job_task
                {
                    job_id = databricks_job.job_name.id
                }
            }
        }
    }
    tags = module.tags.common_tags
}


#Code to generate a single workflow

resource "databricks_job" "bnr_toggle" 
{

    name = var.job_name
    description = var.job_description
    timeout_seconds = var.job_timeout_seconds

    job_cluster 
    {
        job_cluster_key = var.cluster_name

        new_cluster 
        {
            num_workers = var.num_workers
            spark.version = var.spark.version
            node_type_id = var.worker_instance_type
            driver_node_type_id = var.driver_instance_type
            policy_id = var.cluster_policy_id
            runtime_engine = "STANDARD"
            data_security_mode = "SINGLE_USER"

            autoscale 
            {
                min_workers = var.min_workers
                max_workers = var.max_workers
            }

            aws_attributes 
            {
                instance_profile_arn = var.instance_profile_arn
                first_on_demand = var.first_on_demand
                availability = var.availability
                zone_id = var.zone_id
                spot_bid_price_percent = var.spot_bid_price_percent
            }

            spark_conf 
            {
                "spark.dtabricks.hive.metastore.glueCatalog.enabled":"true",
                "spark.hadoop.hive.metastore.glue.catalogid": local.catalogid,
                "s[ark.hadoop.aws.region":"us-east-1",
                "spark.hadoop.fs.s3a.acl.default":"BucketOwnerFullControl"
            }

            custom_tags = local.cluster_tags
        }
    }
    email_notifications
    {
        on_start = var.email_on_start
        on_success = var.email_on_success
        on_failure = var.email_on_failure
    }
    task 
    {
        task_key = "bnr_toggle"
        job_cluster_key = var.cluster_name
        notebook_task 
        {
            notebook_path = "/apps/${var.env_name}/BIModDomainNorthlink/python/stage_to_transform/BNR/bnr_toggle"
            base_parameters = {
                env = var.env_name
            }
        }
        timeout_seconds = 1800
    }
    tags = module.tags.common_tags
}


#Permissions to Job

resource "databricks_permissions" "bnr_toggle_usage" {
    job_id = databricks_job.bnr_toggle.id
    access_control {
        group_name = "users"
        permission_level = var.job_permission_level
    }
}

#Publish Notebooks to Workspace

resource "databricks_notebook" "bnr_toggle" {
    source = "${path.cwd}/notebooks.stage_to_transform/BNR.bnr_toggle.py"
    path = "/apps/${var.env_name}/BIModDomainNorthlink/python/stage_to_transform/BNR/bnr_toggle"
}

#################################################################################################

#StageToLoad Workflow

resource "databricks_job" "bnr_toggle" {

    name = var.job_name1
    description = var.job_description1
    timeout_seconds = var.job_timeout_seconds

    job_cluster 
    {
        job_cluster_key = var.cluster_name

        new_cluster 
        {
            num_workers = var.num_workers
            spark.version = var.spark.version
            node_type_id = var.worker_instance_type
            driver_node_type_id = var.driver_instance_type
            policy_id = var.cluster_policy_id
            runtime_engine = "STANDARD"
            data_security_mode = "SINGLE_USER"

            autoscale 
            {
                min_workers = var.min_workers
                max_workers = var.max_workers
            }

            aws_attributes 
            {
                instance_profile_arn = var.instance_profile_arn
                first_on_demand = var.first_on_demand
                availability = var.availability
                zone_id = var.zone_id
                spot_bid_price_percent = var.spot_bid_price_percent
            }

            spark_conf 
            {
                "spark.dtabricks.hive.metastore.glueCatalog.enabled":"true",
                "spark.hadoop.hive.metastore.glue.catalogid": local.catalogid,
                "s[ark.hadoop.aws.region":"us-east-1",
                "spark.hadoop.fs.s3a.acl.default":"BucketOwnerFullControl"
            }

            custom_tags = local.cluster_tags
        }
    }
    email_notifications
    {
        on_start = var.email_on_start
        on_success = var.email_on_success
        on_failure = var.email_on_failure
    }
    task 
    {
        task_key = "setRunTimeParams"
        job_cluster_key = var.cluster_name
        notebook_task 
        {
            notebook_path = "/apps/${var.env_name}/BIModDomainNorthlink/python/stage_to_transform/setRunTimeParams"
            base_parameters = {
                env = var.env_name
                job_name = "{{job.name}}"
                job_run_id = "{{job.run_id}}"
                load_type = "hist"
                task_name = "{{task.name}}"
                task_run_id = "{{task.run_id}}"
                transform_check_flag = "N"
            }
        }
        library
        {
            pypi {
                package = "bi-mod-p2p-sdk==1.0.6"
            }
        }
        timeout_seconds = 1800
    }

    task 
    {
        task_key = "sourceReader"
        depends_on
        {
            task_key = "setRunTimeParams"
        }
        job_cluster_key = var.cluster_name
        notebook_task 
        {
            notebook_path = "/apps/${var.env_name}/BIModDomainNorthlink/python/stage_to_transform/sourceReader"
            base_parameters = {
                job_name = "{{job.name}}"
                job_run_id = "{{job.run_id}}"
                task_name = "{{task.name}}"
                task_run_id = "{{task.run_id}}"
            }
        }
        library
        {
            pypi {
                package = "bi-mod-p2p-sdk==1.0.6"
            }
        }
        timeout_seconds = 1800
    }

    task 
    {
        task_key = "sourceReaderDataCheck"

        depends_on
        {
            task_key = "sourceReader"
        }

        run_if = "ALL_DONE"

        condition_task 
        {
            op = "EQUAL_TO"
            left = "{{task.sourceReader.values.no_data_flag}}"
            right = "true"
        }
    }

    task 
    {
        task_key = "batchControl-NoDataUpdate"

        depends_on
        {
            task_key = "sourceReaderDataCheck"
            outcome = "true"
        }
        run_if = "ALL_SUCCESS"

        job_cluster_key = var.cluster_name
        notebook_task 
        {
            notebook_path = "/apps/${var.env_name}/BIModDomainNorthlink/python/stage_to_transform/batchControlUpdate"
            base_parameters = {
                job_name = "{{job.name}}"
                job_run_id = "{{job.run_id}}"
                task_name = "{{task.name}}"
                task_run_id = "{{task.run_id}}"
                refine_flag = "Y"
                stage_flag = "Y"
                transform_flag = "Y"
                update_type = "no_data"
            }
        }
        timeout_seconds = 1800
    }

    task 
    {
        task_key = "event"

        depends_on
        {
            task_key = "sourceReaderDataCheck"
            outcome = "false"
        }
        run_if = "ALL_SUCCESS"

        job_cluster_key = var.cluster_name
        notebook_task 
        {
            notebook_path = "/apps/${var.env_name}/BIModDomainNorthlink/python/stage_to_transform/main"
            base_parameters = {
                job_name = "{{job.name}}"
                job_run_id = "{{job.run_id}}"
                task_name = "{{task.name}}"
                task_run_id = "{{task.run_id}}"
            }
        }
        timeout_seconds = 1800
    }

    task 
    {
        task_key = "stageBnrConditionCheck"

        depends_on
        {
            task_key = "event"
        }

        run_if = "ALL_SUCCESS"

        condition_task 
        {
            op = "EQUAL_TO"
            left = "{{task.setRunTimeParams.values.bnr_check}}"
            right = "Yes"
        }
    }

    task 
    {
        task_key = "stageBNR_SDK"

        depends_on
        {
            task_key = "stageBnrConditionCheck"
            outcome = "true"
        }
        run_if = "ALL_SUCCESS"

        job_cluster_key = var.cluster_name
        notebook_task 
        {
            notebook_path = "/apps/${var.env_name}/BIModDomainNorthlink/python/stage_to_transform/BNR/bnr_code"
            base_parameters = {
                job_name = "{{job.name}}"
                job_run_id = "{{job.run_id}}"
                task_name = "{{task.name}}"
                task_run_id = "{{task.run_id}}"
                assumeFlag = "Y"
                controlPointCategory_name = "DATA_DOMAIN"
                dataLayer = "STAGE"
                dataSourceAppsCode = "NLINK"
                env = var.env_name
                source_nm = "NLINK"
                frequency = "HISTORY"
            }
        }

        library
        {
            pypi {
                package = "pyarrow"
            }
        }
        library
        {
            pypi {
                package = "snowflake-connector-python"
            }
        }
        library
        {
            pypi {
                package = "openpyxl"
            }
        }
        timeout_seconds = 1800
    }

    task 
    {
        task_key = "policy"

        depends_on
        {
            task_key = "event"
        }
        run_if = "ALL_SUCCESS"

        job_cluster_key = var.cluster_name
        notebook_task 
        {
            notebook_path = "/apps/${var.env_name}/BIModDomainNorthlink/python/stage_to_transform/main"
            base_parameters = {
                domain = "policy"
                job_name = "{{job.name}}"
                job_run_id = "{{job.run_id}}"
                task_name = "{{task.name}}"
                task_run_id = "{{task.run_id}}"
            }
        }
        timeout_seconds = 1800
    }

    task 
    {
        task_key = "party"

        depends_on
        {
            task_key = "policy"
        }
        run_if = "ALL_SUCCESS"

        job_cluster_key = var.cluster_name
        notebook_task 
        {
            notebook_path = "/apps/${var.env_name}/BIModDomainNorthlink/python/stage_to_transform/main"
            base_parameters = {
                domain = "party"
                job_name = "{{job.name}}"
                job_run_id = "{{job.run_id}}"
                task_name = "{{task.name}}"
                task_run_id = "{{task.run_id}}"
            }
        }
        timeout_seconds = 1800
    }

    task 
    {
        task_key = "sourceReaderFailure-cleanUp"

        depends_on
        {
            task_key = "sourcereader"
        }
        run_if = "ALL_FAILED"

        job_cluster_key = var.cluster_name
        notebook_task 
        {
            notebook_path = "/apps/${var.env_name}/BIModDomainNorthlink/python/stage_to_transform/cleanup"
            base_parameters = {
                cleanup_layer = "sourcereader_cleanup"
                job_name = "{{job.name}}"
                job_run_id = "{{job.run_id}}"
                task_name = "{{task.name}}"
                task_run_id = "{{task.run_id}}"
            }
        }
        timeout_seconds = 1800
    }
    
    task 
    {
        task_key = "updateBalanceControl-LoadLayer"

        depends_on
        {
            task_key = "party"
        }
        run_if = "ALL_SUCCESS"

        job_cluster_key = var.cluster_name
        notebook_task 
        {
            notebook_path = "/apps/${var.env_name}/BIModDomainNorthlink/python/stage_to_transform/updateBalanceControl"
            base_parameters = {
                layer = "load"
                job_name = "{{job.name}}"
                job_run_id = "{{job.run_id}}"
                task_name = "{{task.name}}"
                task_run_id = "{{task.run_id}}"
            }
        }
        timeout_seconds = 1800
    }

    task 
    {
        task_key = "cleanUp"

        depends_on
        {
            task_key = "updateBalanceControl-LoadLayer"
        }
        run_if = "ALL_SUCCESS"

        job_cluster_key = var.cluster_name
        notebook_task 
        {
            notebook_path = "/apps/${var.env_name}/BIModDomainNorthlink/python/stage_to_transform/cleanup"
            base_parameters = {
                cleanup_layer = "intrm_cleanup"
                job_name = "{{job.name}}"
                job_run_id = "{{job.run_id}}"
                task_name = "{{task.name}}"
                task_run_id = "{{task.run_id}}"
            }
        }
        timeout_seconds = 1800
    }

    task 
    {
        task_key = "batchControl-FailureUpdate"

        depends_on
        {
            task_key = "cleanUp"
        }
        run_if = "AT_LEAST_ONE_FAILED"

        job_cluster_key = var.cluster_name
        notebook_task 
        {
            notebook_path = "/apps/${var.env_name}/BIModDomainNorthlink/python/stage_to_transform/batchControlUpdate"
            base_parameters = {
                job_name = "{{job.name}}"
                job_run_id = "{{job.run_id}}"
                task_name = "{{task.name}}"
                task_run_id = "{{task.run_id}}"
                refine_flag = "N"
                stage_flag = "N"
                transform_flag = "N"
                update_type = "failure"
            }
        }
        timeout_seconds = 1800
    }

    task 
    {
        task_key = "loadCleanUp"

        depends_on
        {
            task_key = "batchControl-FailureUpdate"
        }
        run_if = "ALL_SUCCESS"

        job_cluster_key = var.cluster_name
        notebook_task 
        {
            notebook_path = "/apps/${var.env_name}/BIModDomainNorthlink/python/stage_to_transform/cleanup"
            base_parameters = {
                cleanup_layer = "domain_cleanup"
                job_name = "{{job.name}}"
                job_run_id = "{{job.run_id}}"
                task_name = "{{task.name}}"
                task_run_id = "{{task.run_id}}"
            }
        }
        timeout_seconds = 1800
    }

    task 
    {
        task_key = "batchControl-SuccessUpdate"

        depends_on
        {
            task_key = "cleanUp"
        }
        run_if = "ALL_SUCCESS"

        job_cluster_key = var.cluster_name
        notebook_task 
        {
            notebook_path = "/apps/${var.env_name}/BIModDomainNorthlink/python/stage_to_transform/batchControlUpdate"
            base_parameters = {
                job_name = "{{job.name}}"
                job_run_id = "{{job.run_id}}"
                task_name = "{{task.name}}"
                task_run_id = "{{task.run_id}}"
                refine_flag = "N"
                stage_flag = "Y"
                transform_flag = "M"
                update_type = "success"
            }
        }
        timeout_seconds = 1800
    }

    task 
    {
        task_key = "LoadtoTrans"
        depends_on 
        {
            task_key = "batchControl-SuccessUpdate"
        }
        run_if = "ALL_SUCCESS"
        run_job_task {
            job_id = databricks.Northlink-Hist-LoadToTrans.id
        }
    }
    tags = module.tags.common_tags

}

#LoadToTrans Workflow


#TransToRefine Workflow
