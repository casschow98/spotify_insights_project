
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from google.cloud import storage
import pendulum
import datetime
import os
from ingestion import get_recent_tracks
import re
import shutil
from gcp_fn import gcs_bq_upload

# Define bucket and home path environment variables
home_path = os.environ.get("AIRFLOW_HOME","/opt/airflow/")
BUCKET = os.environ.get("GCP_STORAGE_BUCKET")
DATASET = "de_zoomcamp_cchow_dataset"
PROJECT_ID = os.environ.get("GCP_PROJECT_ID")


# Default arguments for the DAG
default_args = {
    "owner": "airflow",
    "start_date": pendulum.datetime(2024, 1, 1, tz="America/Vancouver"),
    "end_date": pendulum.datetime(2025, 2, 1, tz="America/Vancouver"),
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": datetime.timedelta(minutes=0.5)
}


def delete_contents(home_dir, names, **kwargs):
    # Loop through names
    try:
        for name in names:
            path = os.path.join(home_dir,name)
            if os.path.isfile(path):
                os.remove(path)
                print(f"Successfully removed file {path}")
            elif os.path.isdir(path):
                shutil.rmtree(path)
                print(f"Successfully removed directory and contents of {path}")
    except Exception as e:
            print(f"Error removing {path}: {e}")


# Define the DAG
dag = DAG(
    'spotify_dag',
    default_args=default_args,
    description='Pipeline for spotify data insights',
    schedule='@hourly',
    catchup=False
)

# Define tasks

# Task to retrieve new api tokens, send api requests, and download data to .csv
get_recent_tracks_task = PythonOperator(
    task_id='get_recent_tracks_task',
    python_callable=get_recent_tracks.retrieve_songs(),
    dag=dag
)

# Task to upload to Google Cloud Storage
upload_gcs_task = PythonOperator(
    task_id='upload_gcs_task',
    python_callable=gcs_bq_upload.process_csv(),
    dag=dag
)

# Task to delete the /opt/airflow/tmp/ directory and contents
delete_local_task = PythonOperator(
    task_id='delete_local_task',
    python_callable=delete_contents,
    op_kwargs={
        "home_dir": home_path,
        "names": 'tmp'
    }
)

# # Task to submit the Spark job
# spark_submit_task = SparkSubmitOperator(
#     task_id='spark_submit_task',
#     application='/path/to/spark_job.py',  # Path to your Spark job script
#     conn_id='spark_default',  # Connection ID for Spark
#     application_args=[
#         '--project_id', 'your_project_id',
#         '--bq_table_input', 'your_dataset.your_table',
#         '--bucket', 'your_bucket',
#     ],
#     dag=dag,
# )


# Dependencies between the tasks
get_recent_tracks_task >> upload_gcs_task >> delete_local_task