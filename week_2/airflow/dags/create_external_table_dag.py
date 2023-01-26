import os
from datetime import datetime

from airflow.decorators import dag
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator

# Import enviroment variable within context of Docker image from Dockerfile
PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET")
TABLE_NAME_GCS_PATH_MAPPING = {
    'raw/yellow_tripdata/': 'ext_yellow_tripdata',
    'raw/green_tripdata/': 'ext_green_tripdata',
    'raw/fhv_tripdata/': 'ext_fhv_tripdata',
    'raw/zones/': 'ext_zones'
}


default_args = {
    "owner": "airflow",
    "start_date": datetime(2023, 1, 24),
    "retries": 1,
}

@dag(
    dag_id="create_external_table_dag",
    schedule_interval="0 6 2 * *",
    default_args=default_args,
    tags=['dtc-de'],
)
def create_external_table_dag():
# Looping is possible but you need to make sure that task_id is unique
    for path, table_name in TABLE_NAME_GCS_PATH_MAPPING.items():
        bigquery_create_external_table = BigQueryCreateExternalTableOperator(
            task_id=f"create_{table_name}",
            table_resource={
                "tableReference": {
                    "projectId": PROJECT_ID,
                    "datasetId": BIGQUERY_DATASET,
                    "tableId": table_name,
                },
                "externalDataConfiguration": {
                    "sourceFormat": "PARQUET",
                    # Specify source of this external table. Using wildcard to fetch data from all parquet files in this directory
                    "sourceUris": [f"gs://{BUCKET}/{path}*"],
                },
            }
        )

# We call the main DAG function at the end for it to work
dag = create_external_table_dag()