import os

from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator

from google.cloud import storage  # This was specified in Dockerfile
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator

import pandas as pd

# Import enviroment variable within context of Docker image from Dockerfile
PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET")

dataset_file = "yellow_tripdata_2021-01.csv.gz"
dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/{dataset_file}"
path_to_local_home = os.environ.get("AIRFLOW_HOME")


default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
}

@dag(
    dag_id="data_ingestion_gcs_dag",
    schedule_interval="@daily",
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=['dtc-de'],
)
def data_ingestion_gcs_dag():

    # Not all operators can be used with @task decorator, thus they are used in classic manner
    download_dataset = BashOperator(task_id='download_dataset',
                                    bash_command=f"curl -sSL {dataset_url} > {path_to_local_home}/{dataset_file}"
    )

    @task(task_id='format_to_parquet', )
    def format_to_parquet_func(src_file):

        df = pd.read_csv(src_file)
        parquet_file_name = src_file.replace('.csv.gz', '.parquet').split('/')[-1]
        df.to_parquet(src_file.replace('.csv.gz', '.parquet'))
        return parquet_file_name

    # We have assigned a decorated task to a variable in order to add arguments
    format_to_parquet = format_to_parquet_func(src_file=f"{path_to_local_home}/{dataset_file}")

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
                                       object_name=f"raw/{format_to_parquet}",
                                       local_file=f"{path_to_local_home}/{format_to_parquet}",)

    bigquery_external_table_insert = BigQueryCreateExternalTableOperator(
        task_id="bigquery_external_table_insert",
        table_resource={
            "tableReference": {
                "projectId": PROJECT_ID,
                "datasetId": BIGQUERY_DATASET,
                "tableId": "external_table",
            },
            "externalDataConfiguration": {
                "sourceFormat": "PARQUET",
                "sourceUris": [f"gs://{BUCKET}/raw/{format_to_parquet}"],
            },
        },
    )

    # If we explicitly assigned each task to a variable we can use bitwise operators to create DAGs.
    download_dataset >> format_to_parquet >> upload_to_gcs >> bigquery_external_table_insert

# We call the main DAG function at the end for it to work
data_ingestion_gcs_dag()