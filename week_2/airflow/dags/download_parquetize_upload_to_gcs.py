import os
from datetime import datetime

from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator

from google.cloud import storage  # This was specified in Dockerfile

import pandas as pd

# Import enviroment variable within context of Docker image from Dockerfile
PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET")
PATH_TO_LOCAL_HOME = os.environ.get("AIRFLOW_HOME")


def dag_template(
    dag_id: str,
    url_prefix: str,
    download_filename_prefix: str,
    gcs_path_template: str,
    date_columns_for_parsing: list
):
    '''
    DAG template that allows to customize what files should be downloaded (only .csv.gz),
    from where, and where should they be put in the GCS bucket. The files should follow 
    this patter: [download_filename_prefix]_YYYY_MM.csv.gz
    '''
    
    URL_TEMPLATE = url_prefix + download_filename_prefix

    default_args = {
        "owner": "airflow",
        "depends_on_past": True,
        "retries": 1,
    }

    @dag(
        dag_id=dag_id,
        start_date = datetime(2019, 1, 1),
        end_date = datetime(2021, 8, 2),
        schedule="0 6 2 * *",
        default_args=default_args,
        catchup=True,
        tags=['dtc-de'],
    )
    def ingestion_dag():

        # Not all operators can be used with @task decorator, thus they are used in classic manner
        download_dataset = BashOperator(task_id='download_dataset',
                                        bash_command=f'curl -sSLf {URL_TEMPLATE} > {PATH_TO_LOCAL_HOME}/{download_filename_prefix}'
        )

        @task(task_id='format_to_parquet', )
        def format_to_parquet_func(src_file):

    # Using "Int64" not int64, allows integers to be nullable in Pandas
    # ref: https://stackoverflow.com/questions/11548005/numpy-or-pandas-keeping-array-type-as-integer-while-having-a-nan-value
    # Datetime columns are first assigned a string and then a date_parser is used
    # Parquet file will retain column datatypes as assigned in Pandas DataFrame

            df = pd.read_csv(src_file, 
                            parse_dates=date_columns_for_parsing,
                            date_parser=pd.to_datetime)
            parquet_file_name = src_file.replace('.csv.gz', '.parquet').split('/')[-1]
            df.to_parquet(src_file.replace('.csv.gz', '.parquet'))
            return parquet_file_name

        # We have assigned a decorated task to a variable in order to add arguments
        format_to_parquet = format_to_parquet_func(src_file=f"{PATH_TO_LOCAL_HOME}/{download_filename_prefix}")

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
                                        object_name=f"{gcs_path_template}{format_to_parquet}",
                                        local_file=f"{PATH_TO_LOCAL_HOME}/{format_to_parquet}",)

        # Remove downloaded files .csv.gz and .parquet to keep memory clean
        remove_temp_files = BashOperator(
            task_id='remove_temp_files',
            bash_command=f'rm {PATH_TO_LOCAL_HOME}/{download_filename_prefix} {PATH_TO_LOCAL_HOME}/{format_to_parquet}'
        )

        # If we explicitly assigned each task to a variable we can use bitwise operators to create DAGs.
        download_dataset >> format_to_parquet >> upload_to_gcs >> remove_temp_files

    return ingestion_dag()

    
