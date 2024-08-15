
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
    instance = get_recent_tracks()
    instance.retrieve_songs()

def gcp_upload_callable():
    instance = gcs_bq_upload()
    instance.process_csv()


def delete_contents(home_dir, names):
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

submit_spark_job_task = BashOperator(
    task_id='submit_spark_job_task',
    bash_command="""
        spark-submit \
        --master local \
        /opt/bitnami/spark/spark_job.py \
        --project_id {PROJECT_ID} \
        --dataset {DATASET} \
        --table {TABLE}
    """
)


# Task to submit the Spark job
# spark_submit_task = SparkSubmitOperator(
#     task_id='spark_submit_task',
#     application='/opt/spark/jobs/spark_job.py',
#     conn_id='spark-conn',
#     executor_memory='2g',
#     total_executor_cores=2,
#     conf={
#         'spark.driver.extraJavaOptions': '-Dlog4j.logLevel=ERROR',
#         'spark.executor.extraJavaOptions': f'-Djava.home={JAVA_HOME}',
#         'spark.master': 'local[*]' 
#     },
#     application_args=[
#         '--project_id', PROJECT_ID,
#         '--dataset', DATASET,
#         '--table', TABLE
#     ],
#     dag=dag
# )


# Dependencies between the tasks
get_recent_tracks_task >> upload_gcs_task >> delete_local_task >> submit_spark_job_task