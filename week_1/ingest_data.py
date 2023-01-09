import pandas as pd
from sqlalchemy import create_engine
import time
import argparse
import os

def main(params):
    # Assign argparse parameters to variables
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    database = params.database
    table_name = params.table_name
    url = params.url

    # Pandas read_csv() can read gzipped files as long as they contain one csv file
    if url.endswith('.csv.gz'):
        csv_name = 'output.csv.gz'
    else:
        csv_name = 'output.csv'

    os.system(f"wget {url} -O {csv_name}")

    # Create engine for postgres databased run by docker
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{database}')

    # use Pandas to_sql function to create a table in postgres, with the headers only
    df_headers = pd.read_csv(csv_name, nrows=1)
    df_headers.head(n=0).to_sql(table_name, con=engine, if_exists='replace')

    # Split df into chunks using iterator=True parameter and specifying chunks so that we will not overload the database
    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100_000)
    for df in df_iter:
        t_start = time.time()
        df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
        df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
        df.to_sql(table_name, con=engine, if_exists='append')
        t_end = time.time()
        print(f'Inserted chunk. Took {t_end - t_start:.2f} sec.')
    print('Finished insertion')

if __name__ == '__main__':
    # Add CLI arguments with argparse
    parser = argparse.ArgumentParser(description='Insert CSV data to Postgres')
    parser.add_argument('--user', help='user for Postgres DB')
    parser.add_argument('--password', help='password for Postgres DB')
    parser.add_argument('--host', help='host for Postgres DB')
    parser.add_argument('--port', help='port for Postgres DB')
    parser.add_argument('--database', help='Postgres DB')
    parser.add_argument('--table_name', help='table name for Postgres DB')
    parser.add_argument('--url', help='URL of csv or gz file containing single CSV file')

    # Parse them to be used later as an argument to main() function
    args = parser.parse_args()

    main(args)