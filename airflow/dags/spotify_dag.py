
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.sensors.external_task import ExternalTaskSensor
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

def run_ingestion():
    # sp_tracks = get_recent_tracks()
    # sp_tracks.retrieve_songs()
    get_recent_tracks.retrieve_songs()

def run_gcp_fn():
    # to_gcp = gcs_bq_upload()
    # to_gcp.process_csv()
    gcs_bq_upload.process_csv()

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

# Task to refresh the spotify api token to obtain a new access token
get_recent_tracks_task = PythonOperator(
    task_id='get_recent_tracks_task',
    python_callable=run_ingestion,
    dag=dag
)

# Task to upload to Google Cloud Storage
upload_gcs_task = PythonOperator(
    task_id='upload_gcs_task',
    python_callable=run_gcp_fn,
    dag=dag
)

delete_local_task = PythonOperator(
    task_id='delete_local_task',
    python_callable=delete_contents,
    op_kwargs={
        "home_dir": home_path,
        "names": 'tmp'
    }
)

# # Task to convert the geojson to a newline-delimited geojson format
# geojsonl_task_fire = BashOperator(
#     task_id='geojsonl_task_fire',
#     bash_command="geojson2ndjson {{ params.in_geojson }} > {{ params.out_geojsonl }}",
#     params = {
#         "in_geojson" : f"{home_path}/tmp/{fire_poly_file}.geojson",
#         "out_geojsonl" : f"{home_path}/tmp/{fire_poly_file}_nl.geojsonl"
#     },
#     dag=dag
# )




# # Task to trigger the start of this dag once the rec_data_dag successfully finishes its final task
# wait_for_rec_data = ExternalTaskSensor(
#     task_id='wait_for_rec_data',
#     external_dag_id='rec_data_dag',
#     external_task_id='delete_contents_task',  # Task ID to check in the parent DAG
#     mode='poke',
#     timeout=600,
#     poke_interval=30,
#     allowed_states=['success'],
#     dag=dag,
# )


# Dependencies between the tasks
get_recent_tracks_task >> upload_gcs_task >> delete_local_task