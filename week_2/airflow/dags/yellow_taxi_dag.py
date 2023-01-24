#airflow dag <- Make sure this these words appear _somewhere_ in the file.

from download_parquetize_upload_to_gcs import dag_template

dag_template(dag_id='yellow_taxi_dag',
             url_prefix='https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/',
             download_filename_prefix='yellow_tripdata_{{ logical_date.strftime(\'%Y-%m\') }}.csv.gz',
             gcs_path_template='raw/yellow_tripdata/{{ logical_date.strftime(\'%Y\') }}', # Create year folder
             date_columns_for_parsing=['tpep_pickup_datetime', 'tpep_dropoff_datetime']
                            )

