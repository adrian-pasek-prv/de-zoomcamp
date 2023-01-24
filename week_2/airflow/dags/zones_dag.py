import os
from datetime import datetime

from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator

from google.cloud import storage  # This was specified in Dockerfile

import pandas as pd

# Import enviroment variable within context of Docker image from Dockerfile
PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET")

URL_PREFIX = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/'
# Airflow makes use of Jinja so we can access "logical_date" (execution_date) variable and format it to YYYY-MM format
# in order to make this job dynamic
DOWNLOAD_FILENAME = 'taxi_zone_lookup.csv'
URL_TEMPLATE = URL_PREFIX + DOWNLOAD_FILENAME
PATH_TO_LOCAL_HOME = os.environ.get("AIRFLOW_HOME")
GCS_PATH_TEMPLATE = 'raw/zones/'


default_args = {
    "owner": "airflow",
    "start_date": datetime(2023, 1, 24),
    "retries": 1,
}

@dag(
    dag_id="zones_dag",
    default_args=default_args,
    tags=['dtc-de'],
)
def dag():

    # Not all operators can be used with @task decorator, thus they are used in classic manner
    download_dataset = BashOperator(task_id='download_dataset',
                                    bash_command=f'curl -sSLf {URL_TEMPLATE} > {PATH_TO_LOCAL_HOME}/{DOWNLOAD_FILENAME}'
    )

    @task(task_id='format_to_parquet', )
    def format_to_parquet_func(src_file):

        df = pd.read_csv(src_file)
        parquet_file_name = src_file.replace('.csv', '.parquet').split('/')[-1]
        df.to_parquet(src_file.replace('.csv', '.parquet'))
        return parquet_file_name

    # We have assigned a decorated task to a variable in order to add arguments
    format_to_parquet = format_to_parquet_func(src_file=f"{PATH_TO_LOCAL_HOME}/{DOWNLOAD_FILENAME}")

    @task(task_id='upload_to_gcs')
    # NOTE: takes 20 mins, at an upload speed of 800kbps. Faster if your internet has a better upload speed
    def upload_to_gcs_func(bucket, object_name, local_file):
        """
        Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
        :param bucket: GCS bucket name
        :param object_name: target path & file-name
        :param local_file: source path & file-name
        :return:
        """
        # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
        # (Ref: https://github.com/googleapis/python-storage/issues/74)
        storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
        storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
        # End of Workaround

        client = storage.Client()
        bucket = client.bucket(bucket)

        # Creating a file object (blob) 
        # https://stackoverflow.com/questions/43264664/creating-uploading-new-file-at-google-cloud-storage-bucket-using-python
        blob = bucket.blob(object_name)
        blob.upload_from_filename(local_file)

    # format_to_parquet variable is the returned string from decorated task format_to_parquet_func
    upload_to_gcs = upload_to_gcs_func(bucket=BUCKET,
                                       object_name=f"{GCS_PATH_TEMPLATE}{format_to_parquet}",
                                       local_file=f"{PATH_TO_LOCAL_HOME}/{format_to_parquet}",)

    # Remove downloaded files .csv.gz and .parquet to keep memory clean
    remove_temp_files = BashOperator(
        task_id='remove_temp_files',
        bash_command=f'rm {PATH_TO_LOCAL_HOME}/{DOWNLOAD_FILENAME} {PATH_TO_LOCAL_HOME}/{format_to_parquet}'
    )

    # If we explicitly assigned each task to a variable we can use bitwise operators to create DAGs.
    download_dataset >> format_to_parquet >> upload_to_gcs >> remove_temp_files

# We call the main DAG function at the end for it to work
dag()