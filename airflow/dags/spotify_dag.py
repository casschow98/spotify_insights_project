
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.sensors.external_task import ExternalTaskSensor
import pendulum
import datetime
import os
import requests

# Define bucket and home path environment variables
home_path = os.environ.get("AIRFLOW_HOME","/opt/airflow/")
BUCKET = os.environ.get("GCP_STORAGE_BUCKET")
DATASET = "de_zoomcamp_cchow_dataset"
PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
API_URL = 


# Default arguments for the DAG
default_args = {
    "owner": "airflow",
    "start_date": pendulum.datetime(2024, 1, 1, tz="America/Vancouver"),
    "end_date": pendulum.datetime(2025, 2, 1, tz="America/Vancouver"),
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": datetime.timedelta(minutes=0.5)
}

# Define the DAG
dag = DAG(
    'spotify_dag',
    default_args=default_args,
    description='Pipeline for spotify data insights',
    schedule='@daily',
    catchup=False
)

# Define the tasks

# Task to upload the shapefile .zip of wildfire polygons to Google Cloud Storage
upload_zip_task_fire = PythonOperator(
    task_id='upload_zip_task_fire',
    python_callable=upload_to_gcs,
    op_kwargs={
        "home_dir": home_path,
        "bucket_name": BUCKET,
        "rel_path": f"{fire_poly_file}.zip"
    },
    dag=dag
)

# Task to convert the geojson to a newline-delimited geojson format
geojsonl_task_fire = BashOperator(
    task_id='geojsonl_task_fire',
    bash_command="geojson2ndjson {{ params.in_geojson }} > {{ params.out_geojsonl }}",
    params = {
        "in_geojson" : f"{home_path}/tmp/{fire_poly_file}.geojson",
        "out_geojsonl" : f"{home_path}/tmp/{fire_poly_file}_nl.geojsonl"
    },
    dag=dag
)

# Task to upload the newline-delimited geojson to Google Cloud Storage
upload_geojsonl_task_fire = PythonOperator(
    task_id='upload_geojsonl_task_fire',
    python_callable=upload_to_gcs,
    op_kwargs={
        "home_dir": home_path,
        "bucket_name": BUCKET,
        "rel_path": f"tmp/{fire_poly_file}_nl.geojsonl"
    },
    dag=dag
)


# Task to trigger the start of this dag once the rec_data_dag successfully finishes its final task
wait_for_rec_data = ExternalTaskSensor(
    task_id='wait_for_rec_data',
    external_dag_id='rec_data_dag',
    external_task_id='delete_contents_task',  # Task ID to check in the parent DAG
    mode='poke',
    timeout=600,
    poke_interval=30,
    allowed_states=['success'],
    dag=dag,
)


# Dependencies between the tasks
