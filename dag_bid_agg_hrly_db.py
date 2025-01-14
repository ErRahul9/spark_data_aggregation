import json

from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
from airflow.utils.dates import days_ago

DATABRICKS_CLUSTER_JSON = {
    "access_control_list": [
        {"user_name": "rparashar@mountain.com", "permission_level": "CAN_MANAGE"},
    ],
    "tasks": [
        {
            "task_key": "bidder_log_aggregation_hrly",
            "run_if": "ALL_SUCCESS",
            "spark_python_task": {
                "python_file": "bid_price_agg_hrly.py",
                "parameters": [
                    "-e",
                    "prod",
                    "-dd",
                    "{{ params.dd }}",
                    "-hh",
                    "{{ params.hh }}",
                    "-mods",
                    "{{ params.mods }}",
                ],
                "source": "GIT",
            },
            "new_cluster": {
                "spark_version": "15.4.x-scala2.12",
                "spark_conf": {"spark.sql.shuffle.partitions": "auto"},
                "aws_attributes": {
                    "first_on_demand": 1,
                    "availability": "SPOT_WITH_FALLBACK",
                    "zone_id": "auto",
                    "instance_profile_arn": "arn:aws:iam::077854988703:instance-profile/mntn-databricks-prod-instance-role",
                    "spot_bid_price_percent": 100,
                    "ebs_volume_type": "GENERAL_PURPOSE_SSD",
                    "ebs_volume_count": 3,
                    "ebs_volume_size": 100,
                },
                "node_type_id": "r8g.8xlarge",
                "driver_node_type_id": "r8g.2xlarge",
                "custom_tags": {"team": "bidder", "project": "rahul-ad-hoc"},
                "enable_elastic_disk": False,
                "enable_local_disk_encryption": False,
                "autoscale": {"min_workers": 2, "max_workers": 10},
            },
        }
    ],
    "git_source": {
        "git_url": "https://github.com/ErRahul9/spark_data_aggregation.git",
        "git_provider": "gitHub",
        "git_branch": "main",
    },
}


default_args = {"owner": "airflow"}

with DAG(
    "dag_databricks_bidder_log_aggregation_hour",
    start_date=days_ago(2),
    schedule_interval="15 16 * * *",
    default_args=default_args,
    params={
        "mods": "15 18 55 58 44 65 24 44 78 87 63 98 71 72 78 57 59 16 86 76 66 25 67 94 32 75",
        "hh": None,
        "dd": None,
    },
) as dag:
    submit_databricks_job = DatabricksSubmitRunOperator(
        task_id="submit_databricks_job",
        databricks_conn_id="databricks_bidder",  # Connection ID configured in Airflow
        json=DATABRICKS_CLUSTER_JSON,  # Pass the job configuration
    )
    submit_databricks_job
