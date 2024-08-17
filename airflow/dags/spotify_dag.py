
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
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
DATASET = os.environ.get("BQ_DATASET")
TABLE = os.environ.get("BQ_TABLE")
PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
JAVA_HOME = os.environ.get("JAVA_HOME","/opt/bitnami/java")
GCP_CREDS= os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")




# Default arguments for the DAG
default_args = {
    "owner": "airflow",
    "start_date": pendulum.datetime(2024, 1, 1, tz="America/Vancouver"),
    "end_date": pendulum.datetime(2025, 2, 1, tz="America/Vancouver"),
    "depends_on_past": False,
    "retries": 0,
    "retry_delay": datetime.timedelta(minutes=0.5)
}


def get_songs_callable():
    grt = get_recent_tracks()
    grt.retrieve_songs()

def gcp_upload_callable():
    guc = gcs_bq_upload()
    guc.process_csv()


def delete_contents(home_dir, names):
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
    python_callable=get_songs_callable,
    dag=dag
)

# Task to upload to Google Cloud Storage
upload_gcs_task = PythonOperator(
    task_id='upload_gcs_task',
    python_callable=gcp_upload_callable,
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

# spark_submit_task = BashOperator(
#     task_id='spark_submit_task',
#     bash_command="""
#         spark-submit \
#         --master spark://spark-master:7077 \
#         --conf spark.jars.packages=com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.24.1,com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.5 \
#         --conf spark.hadoop.google.cloud.auth.service.account.enable=true \
#         --conf spark.hadoop.google.cloud.auth.service.account.json.keyfile={GCP_CREDS} \
#         --conf spark.hadoop.fs.AbstractFileSystem.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS \
#         --conf spark.hadoop.fs.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem \
#         --conf spark.hadoop.fs.gs.auth.service.account.json.keyfile={GCP_CREDS} \
#         --conf spark.hadoop.fs.gs.auth.service.account.enable=true \
#         '/opt/airflow/dags/spark/spark_job.py' \
#         --project_id {PROJECT_ID} \
#         --dataset {DATASET} \
#         --table {TABLE} \
#         --bucket {BUCKET}
#     """
# )


# Task to submit the Spark job
# spark_submit_task = SparkSubmitOperator(
#     task_id='spark_submit_task',
#     application='dags/spark/spark_job.py',
#     conn_id='spark-conn',
#     executor_memory='2g',
#     total_executor_cores=2,
#     application_args=[
#         '--project_id', PROJECT_ID,
#         '--dataset', DATASET,
#         '--table', TABLE,
#         '--bucket', BUCKET
#     ],
#     verbose = True,
#     conf={
#         "spark.jars.packages":"com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.22.2,com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.10,com.google.guava:guava:31.1-jre",
#         # "spark.jars":"dags/spark/spark-bigquery-with-dependencies_2.12-0.34.0.jar,dags/spark/gcs-connector-hadoop3-2.2.10.jar",
#         "spark.hadoop.google.cloud.auth.service.account.enable":"true",
#         "spark.hadoop.google.cloud.auth.service.account.json.keyfile":GCP_CREDS,
#         "spark.hadoop.fs.gs.impl":"com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem",
#         "spark.hadoop.fs.AbstractFileSystem.gs.impl":"com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS"
#     },
#     dag=dag
# )


# Dependencies between the tasks
get_recent_tracks_task >> upload_gcs_task >> delete_local_task