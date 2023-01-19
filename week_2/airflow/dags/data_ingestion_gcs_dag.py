import os
from datetime import datetime

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
EXT_TABLE_NAME = 'ext_yellow_taxi'

URL_PREFIX = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/'
# Airflow makes use of Jinja so we can access "logical_date" (execution_date) variable and format it to YYYY-MM format
# in order to make this job dynamic
DOWNLOAD_FILENAME = 'yellow_tripdata_{{ logical_date.strftime(\'%Y-%m\') }}.csv.gz'
URL_TEMPLATE = URL_PREFIX + DOWNLOAD_FILENAME
PATH_TO_LOCAL_HOME = os.environ.get("AIRFLOW_HOME")




default_args = {
    "owner": "airflow",
    "start_date": datetime(2021, 1, 1),
    "end_date": datetime(2021, 8, 2),
    "depends_on_past": True,
    "retries": 1,
}

@dag(
    dag_id="data_ingestion_gcs_dag",
    schedule_interval="0 6 2 * *",
    default_args=default_args,
    catchup=True,
    tags=['dtc-de'],
)
def data_ingestion_gcs_dag():

    # Not all operators can be used with @task decorator, thus they are used in classic manner
    download_dataset = BashOperator(task_id='download_dataset',
                                    bash_command=f'curl -sSL {URL_TEMPLATE} > {PATH_TO_LOCAL_HOME}/{DOWNLOAD_FILENAME}'
    )

    @task(task_id='format_to_parquet', )
    def format_to_parquet_func(src_file):

# Using "Int64" not int64, allows integers to be nullable in Pandas
# ref: https://stackoverflow.com/questions/11548005/numpy-or-pandas-keeping-array-type-as-integer-while-having-a-nan-value
# Datetime columns are first assigned a string and then a date_parser is used
# Parquet file will retain column datatypes as assigned in Pandas DataFrame

        schema = {
            "VendorID": "Int64",
            "tpep_pickup_datetime": "str",
            "tpep_dropoff_datetime": "str",
            "passenger_count": "Int64",
            "trip_distance": "float32",
            "RatecodeID": "Int64",
            "store_and_fwd_flag": "str",
            "PULocationID": "Int64",
            "DOLocationID": "Int64",
            "payment_type": "Int64",
            "fare_amount": "float32",
            "extra": "float32",
            "mta_tax": "float32",
            "tip_amount": "float32",
            "tolls_amount": "float32",
            "improvement_surcharge": "float32",
            "total_amount": "float32",
            "congestion_surcharge": "float32",
        }

        df = pd.read_csv(src_file, 
                         dtype=schema,
                         parse_dates=["tpep_pickup_datetime", "tpep_dropoff_datetime"],
                         date_parser=pd.to_datetime)
        parquet_file_name = src_file.replace('.csv.gz', '.parquet').split('/')[-1]
        df.to_parquet(src_file.replace('.csv.gz', '.parquet'))
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
                                       object_name=f"raw/yellow_taxi/{format_to_parquet}",
                                       local_file=f"{PATH_TO_LOCAL_HOME}/{format_to_parquet}",)

    bigquery_create_external_table = BigQueryCreateExternalTableOperator(
        task_id="bigquery_create_external_table",
        table_resource={
            "tableReference": {
                "projectId": PROJECT_ID,
                "datasetId": BIGQUERY_DATASET,
                "tableId": EXT_TABLE_NAME,
            },
            "externalDataConfiguration": {
                "sourceFormat": "PARQUET",
                # Specify source of this external table. Using wildcard to fetch data from all parquet files in this directory
                "sourceUris": [f"gs://{BUCKET}/raw/yellow_taxi/*.parquet"],
            },
        }
    )

    # If we explicitly assigned each task to a variable we can use bitwise operators to create DAGs.
    download_dataset >> format_to_parquet >> upload_to_gcs >> bigquery_create_external_table

# We call the main DAG function at the end for it to work
data_ingestion_gcs_dag()