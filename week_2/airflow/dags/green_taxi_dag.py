#airflow dag <- Make sure this these words appear _somewhere_ in the file.

from download_parquetize_upload_to_gcs import dag_template

dag_template(dag_id='green_taxi_dag',
             url_prefix='https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/',
             download_filename_prefix='green_tripdata_{{ logical_date.strftime(\'%Y-%m\') }}.csv.gz',
             gcs_path_template='raw/green_tripdata/{{ logical_date.strftime(\'%Y\') }}/' # Create year folder
                            )

