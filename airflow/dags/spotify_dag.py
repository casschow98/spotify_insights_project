
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
import pendulum
import datetime
import os
import re
import shutil
from operators.gcp_fn import gcs_bq_upload
from operators.ingestion import get_recent_tracks


# Define bucket and home path environment variables
home_path = os.environ.get("AIRFLOW_HOME","/opt/airflow/")
BUCKET = os.environ.get("GCP_STORAGE_BUCKET")
DATASET = os.environ.get("BQ_DATASET")
TABLE = os.environ.get("BQ_TABLE")
PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
GCP_CREDS= os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
SPARK_AIRFLOW_CONN_ID= os.environ.get("SPARK_AIRFLOW_CONN_ID")


# Default arguments for the DAG
default_args = {
    "owner": "airflow",
    "start_date": pendulum.datetime(2024, 1, 1, tz="America/Vancouver"),
    "end_date": pendulum.datetime(2025, 2, 1, tz="America/Vancouver"),
    "depends_on_past": False,
    "retries": 0
}


def get_songs_callable():
    grt = get_recent_tracks()
    grt.retrieve_songs()

def gcp_upload_callable():
    gbu = gcs_bq_upload()
    gbu.process_csv()

# Function deletes local files/directories
def delete_contents(home_dir, names):
    try:
        for name in names:
            path = os.path.join(home_dir,name)
            print(f"Attempting to remove {path}...")
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
    python_callable=get_songs_callable,
    dag=dag
)

# Task to upload to Google Cloud Storage then to BigQuery
upload_gcs_task = PythonOperator(
    task_id='upload_gcs_task',
    python_callable=gcp_upload_callable,
    dag=dag
)

# Task to delete the /opt/airflow/working/ directory and contents
delete_local_task = PythonOperator(
    task_id='delete_local_task',
    python_callable=delete_contents,
    op_kwargs={
        "home_dir": home_path,
        "names": ["working"]
    },
    provide_context=True,
    dag=dag
)


# Task to submit the Spark job
spark_submit_task = SparkSubmitOperator(
    task_id='spark_submit_task',
    application='/opt/airflow/dags/spark/spark_job.py',
    conn_id=SPARK_AIRFLOW_CONN_ID,
    executor_memory='2g',
    total_executor_cores=2,
    application_args=[
        '--project_id', PROJECT_ID,
        '--dataset', DATASET,
        '--table', TABLE,
        '--bucket', BUCKET
    ],
    verbose = True,
    conf={
        "spark.executor.memory": "2G",
        "spark.executor.cores": "1",
        "spark.driver.memory": "2G",
        "spark.driver.cores": "1",
        "spark.executor.userClassPathFirst": "true",
        "spark.driver.userClassPathFirst": "true",
        "spark.jars.packages": "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.22.2,com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.10,com.google.guava:guava:30.1-jre",
        "spark.hadoop.google.cloud.auth.service.account.enable": "true",
        "spark.hadoop.google.cloud.auth.service.account.json.keyfile": GCP_CREDS,
        "spark.hadoop.fs.gs.impl": "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem",
        "spark.hadoop.fs.AbstractFileSystem.gs.impl": "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS"
    },
    dag=dag
)


# Dependencies between the tasks
get_recent_tracks_task >> upload_gcs_task >> delete_local_task >> spark_submit_task