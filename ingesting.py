import pandas as pd
from time import time
import pyarrow.parquet as pq
from sqlalchemy import create_engine
import argparse

def main(params):
    pg_user = params.user
    pg_password = params.password
    pg_host = params.host
    pg_port = params.port
    pg_db = params.db
    pg_table = params.table
    parquet_path = params.parquet_path   

    # Create Postgres connection
    engine = create_engine(f"postgresql://{pg_user}:{pg_password}@{pg_host}:{pg_port}/{pg_db}")
    con = engine.connect()

    # Output compressed CSV file
    output_file = "green_tripdata_2019-01.csv.gz"

    # Read Parquet file
    df = pd.read_parquet(parquet_path)
    df.to_csv(output_file, index=False, compression="gzip")
    print(f"Converted {parquet_path} to {output_file}")
    df_iter = pd.read_csv(output_file, iterator=True, chunksize=10000)
    df = next(df_iter)
    
    # Convert datetime columns
    df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
    df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)
    
    df.head(0).to_sql(con=con, name=pg_table, if_exists="replace", index=False)
    df.to_sql(con=con, name=pg_table, if_exists="append", index=False)

    try:
        while True:
            t_start = time()
            df = next(df_iter)

            # Reconvert datetime columns
            df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
            df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)

            # Append data to SQL table
            df.to_sql(con=con, name=pg_table, if_exists="append", index=False)

            t_end = time()
            print(f"New records added in {t_end - t_start:.2f} seconds")

    except StopIteration:
        print("All records imported successfully.")


#Entry point
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Ingest parquet data into Postgres')
    parser.add_argument('--user', help="Database user")
    parser.add_argument('--password', help="Database password")
    parser.add_argument('--host', help="Database host")
    parser.add_argument('--port', help="Database port")
    parser.add_argument('--db', help="Database name")
    parser.add_argument('--table', help="Database table")
    parser.add_argument('--parquet_path', help="Path to the Parquet file")

    

    params = parser.parse_args()
    main(params)
