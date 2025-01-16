from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
from datetime import datetime, timedelta

# Default arguments for the DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

#
# def generate_hours_and_date(execution_date):
#     """
#     Generate the date and hours array based on the execution time.
#
#     Args:
#         execution_date (datetime): The execution date of the DAG.
#
#     Returns:
#         tuple: A tuple containing the process_date (str) and hours (list of int).
#     """
#     run_hour = execution_date.hour
#     if run_hour == 0:
#         # For hour 0, set the date to the previous day and hours to 18-23
#         process_date = (execution_date - timedelta(days=1)).strftime("%Y-%m-%d")
#         hours = [18, 19, 20, 21, 22, 23]
#     else:
#         # For other hours, calculate the date and last 6 hours
#         process_date = execution_date.strftime("%Y-%m-%d")
#         hours = [(run_hour - i - 1) % 24 for i in range(6)][::-1]  # Generate last 6 hours in ascending order
#
#     return process_date, hours


with DAG(
        dag_id="spark_job_last_6_hours",
        default_args=default_args,
        description="Run a Spark job for the last 6 hours of data.",
        schedule_interval="0 */6 * * *",  # Runs at 00:00, 06:00, 12:00, 18:00
        start_date=datetime(2023, 1, 1),
        catchup=False,
) as dag:
    # Use Jinja template to dynamically generate date and hours
    submit_spark_job = DatabricksSubmitRunOperator(
        task_id="submit_databricks_job",
        databricks_conn_id="databricks_default",
        json={
            "new_cluster": {
                "spark_version": "11.3.x-scala2.12",
                "node_type_id": "i3.xlarge",
                "num_workers": 2,
            },
            "spark_jar_task": {
                "main_class_name": "com.example.SparkJob",
                "parameters": [
                    "--date",
                    "{{ macros.ds_add(ds, -1) if execution_date.hour == 0 else ds }}",  # Date for processing
                    "--hours",
                    "{{ '[18, 19, 20, 21, 22, 23]' if execution_date.hour == 0 else [(execution_date.hour - i - 1) % 24 for i in range(6)][::-1] }}"
                ],
            },
            "run_name": "Spark Job - Last 6 Hours",
        },
    )

    submit_spark_job
