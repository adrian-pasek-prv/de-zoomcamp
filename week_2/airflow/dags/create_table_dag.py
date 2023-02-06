import os
from datetime import datetime

from airflow.decorators import dag
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

# Import enviroment variable within context of Docker image from Dockerfile
PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET")
TABLE_NAME_GCS_PATH_PART_MAPPING = {
    'partitions':
        {
            'yellow_tripdata': 'DATE(tpep_pickup_datetime)',
            'green_tripdata': 'DATE(lpep_pickup_datetime)',
            'fhv_tripdata': 'DATE(pickup_datetime)'
        },
    'clusters':
        {
            'yellow_tripdata': 'VendorID',
            'green_tripdata': 'VendorID',
            'fhv_tripdata': 'dispatching_base_num'
        }
}


default_args = {
    "owner": "airflow",
    "start_date": datetime(2023, 1, 30),
    "retries": 1,
}

@dag(
    dag_id="create_table_dag",
    schedule_interval="0 6 2 * *",
    default_args=default_args,
    tags=['dtc-de'],
)
def create_table_dag():
# Looping is possible but you need to make sure that task_id is unique

    for table_name in TABLE_NAME_GCS_PATH_PART_MAPPING['partitions']:

        if table_name == 'zones':
            SQL = f'''
        
            CREATE OR REPLACE TABLE {BIGQUERY_DATASET}.{table_name} AS
            SELECT * FROM {BIGQUERY_DATASET}.ext_{table_name};
                    
        '''
        elif table_name == 'green_tripdata':
            SQL = f'''
            
                CREATE OR REPLACE TABLE {BIGQUERY_DATASET}.{table_name}
                PARTITION BY {TABLE_NAME_GCS_PATH_PART_MAPPING['partitions'][table_name]}
                CLUSTER BY {TABLE_NAME_GCS_PATH_PART_MAPPING['clusters'][table_name]} AS
                SELECT * EXCEPT (ehail_fee) FROM {BIGQUERY_DATASET}.ext_{table_name}
                JOIN (SELECT CAST(0 as numeric) as ehail_fee FROM (SELECT SESSION_USER())) ON 1=1;
                        
            '''          
        else:
            SQL = f'''
            
                CREATE OR REPLACE TABLE {BIGQUERY_DATASET}.{table_name}
                PARTITION BY {TABLE_NAME_GCS_PATH_PART_MAPPING['partitions'][table_name]}
                CLUSTER BY {TABLE_NAME_GCS_PATH_PART_MAPPING['clusters'][table_name]} AS
                SELECT * FROM {BIGQUERY_DATASET}.ext_{table_name};
                        
            '''
        bigquery_create_table = BigQueryInsertJobOperator(
            task_id=f"create_{table_name}",
            project_id = PROJECT_ID,
            location = 'europe-west1',
            configuration = {
                'query': {
                    'query': SQL,
                    'useLegacySql': False
                }
            }
        )

# We call the main DAG function at the end for it to work
dag = create_table_dag()