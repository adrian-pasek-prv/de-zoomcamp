#airflow dag <- Make sure this these words appear _somewhere_ in the file.

from download_parquetize_upload_to_gcs import dag_template

dag_template(dag_id='zones_dag',
             url_prefix='https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/',
             download_filename_prefix='taxi_zone_lookup.csv',
             gcs_path_template='raw/zones/' # Create year folder
                            )