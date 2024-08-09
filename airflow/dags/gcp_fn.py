import os
from google.cloud import bigquery
from google.cloud import storage
from datetime import datetime
import re
from airflow.exceptions import AirflowException


class gcs_bq_upload:
    def __init__(self):
        self.HOME_PATH = os.environ.get("AIRFLOW_HOME","/opt/airflow/")
        self.BUCKET = os.environ.get("GCP_STORAGE_BUCKET")
        self.DATASET = "de_zoomcamp_cchow_dataset"
        self.PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
        self.TABLE = os.environ.get("BQ_TABLE")
        self.SOURCE_DIR = os.path.join(self.HOME_PATH,"tmp")

    # Function to upload files to Google Cloud Storage
    def upload_to_gcs(self, filename):
        # Initialize a storage client
        storage_client = storage.Client()
        bucket = storage_client.bucket(self.BUCKET)

        # Get GCS destination path using year, month, and day from the filename
        self.gcs_path = self.get_gcs_path(filename)
    
        # Create a blob object
        blob = bucket.blob(self.gcs_path)
        
        # Upload the file to Google Cloud Storage
        source_path = os.path.join(self.SOURCE_DIR, filename)

        print(f"Uploading to storage bucket as {blob} from {source_path}...")                
        try:
            blob.upload_from_filename(source_path)
            print(f"Successfully uploaded file from {source_path} to {blob}")
        except Exception as e:
            print(f"Error uploading file {source_path} to {blob}: {str(e)}")
            # Automatically fails airflow task if there was an error in uploading the file
            raise AirflowException("Task failed due to an exception")


    def load_data_to_bigquery(self):
            # Reference to the dataset and table in BigQuery
            dataset_ref = self.bq_client.dataset(self.DATASET)
            table_ref = dataset_ref.table(self.TABLE)

            # Construct the URI for the file in GCS
            uri = f"gs://{self.BUCKET}/{self.gcs_path}"

            # Define the BigQuery load job configuration
            job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.CSV,
                skip_leading_rows=1,
                autodetect=True,
            )

            # Load data from GCS to BigQuery
            try:
                load_job = self.bq_client.load_table_from_uri(
                    uri,
                    table_ref,
                    job_config=job_config
                )
                load_job.result()
                print(f"Successfully loaded data from {uri} to {self.table_name} table in BigQuery")
            except Exception as e:
                print(f"Error loading data from {uri} to {self.table_name}: {str(e)}")
                raise AirflowException("Task failed due to an exception")
                    

    def get_gcs_path(self, filename):
        # Use regexpressions to search for matches for date values in the filename string
        match = re.search(r'(\d{4})-(\d{2})-(\d{2})', filename)
        if match:
            year, month, day = match.groups()
            gcs_path = f"{year}/{month}/{day}/{filename}"
        
        return gcs_path
    

    def process_csv(self):
        # Technically should only contain one .csv in the folder if the delete_contents task was successful
        for filename in os.listdir(self.SOURCE_DIR):
            if filename.endswith('.csv'):
                self.upload_to_gcs(filename)
                self.load_data_to_bigquery()




