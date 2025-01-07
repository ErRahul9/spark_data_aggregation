import json

from airflow import DAG
from airflow.providers.databricks.operators.databricks import ( DatabricksSubmitRunOperator,
)
from airflow.utils.dates import days_ago


DATABRICKS_CLUSTER_JSON = {
"settings": {
"name": "[test] bid_aggregation_job",
"email_notifications": {
  "no_alert_for_skipped_runs": False
}
},
    "tasks": [

        {
            "task_key": "bid_agg_hrly",
            "run_if": "ALL_SUCCESS",
            "spark_python_task": {
                "python_file": "bid_price_agg_hrly.py",
                "parameters": ["-e", "prod", "-dd", "2024-11-11", "-hh", "14"],
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
                "node_type_id": "r8g.xlarge",
                "driver_node_type_id": "r8g.xlarge",
                "custom_tags": {"team": "bidder", "project": "rahul-ad-hoc"},
                "enable_elastic_disk": False,
                "single_user_name": "rparashar@mountain.com",
                "enable_local_disk_encryption": False,
                "data_security_mode": "SINGLE_USER",
                "autoscale": {"min_workers": 2, "max_workers": 10},
            },
        }
    ],
    "git_source": {
        "git_url": "https://github.com/ErRahul9/spark_data_aggregation.git",
        "git_provider": "gitHub",
        "git_branch": "main",
    }
}




default_args = {"owner": "airflow"}

with DAG(
    "dag_databricks_log_agg",
    start_date=days_ago(2),
    schedule_interval=None,
    default_args=default_args,
) as dag:
    submit_databricks_job = DatabricksSubmitRunOperator(
        task_id="submit_databricks_job",
        databricks_conn_id="databricks_default",  # Connection ID configured in Airflow
        json=DATABRICKS_CLUSTER_JSON,  # Pass the job configuration
    )

    submit_databricks_job
