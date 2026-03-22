#!/usr/bin/env python
# coding: utf-8

import click
import pandas as pd
from sqlalchemy import create_engine
from tqdm.auto import tqdm


dtype = {
    "VendorID": "Int64",
    "passenger_count": "Int64",
    "trip_distance": "float64",
    "RatecodeID": "Int64",
    "store_and_fwd_flag": "string",
    "PULocationID": "Int64",
    "DOLocationID": "Int64",
    "payment_type": "Int64",
    "fare_amount": "float64",
    "extra": "float64",
    "mta_tax": "float64",
    "tip_amount": "float64",
    "tolls_amount": "float64",
    "improvement_surcharge": "float64",
    "total_amount": "float64",
    "congestion_surcharge": "float64"
}

parse_dates = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime"
]



@click.command()
@click.option('--pg_user', default='root', help='PostgreSQL user')
@click.option('--pg_password', default='root', help='PostgreSQL password')
@click.option('--pg_host', default='localhost', help='PostgreSQL host')
@click.option('--pg_port', default=5432, type=int, help='PostgreSQL port')
@click.option('--pg_db', default='ny_taxi', help='PostgreSQL database name')
@click.option('--target_table', default='yellow_taxi_data', help='Target table name')
@click.option('--target_table1', default='taxi_zones', help='Target table name for zones')
@click.option('--chunksize', default=100000, type=int, help='Chunk size for reading CSV')
def run(pg_user, pg_password, pg_host, pg_port, pg_db, target_table, target_table1, chunksize):
    """Ingest NYC taxi data into PostgreSQL database."""

    prefix = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/'
    prefix1 = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/'
    engine = create_engine(f'postgresql+psycopg://{pg_user}:{pg_password}@{pg_host}:{pg_port}/{pg_db}')


    df_iter = pd.read_csv(
        prefix + 'yellow_tripdata_2021-01.csv.gz',
        dtype=dtype,
        parse_dates=parse_dates,
        iterator=True,
        chunksize=chunksize,
    )
   
    df_zones = pd.read_csv(
        prefix1 + 'taxi_zone_lookup.csv'
    )


    first = True
    for df_chunk in tqdm(df_iter):
        if first:
            df_chunk.head(n=0).to_sql(
                name=target_table, 
                con=engine, 
                if_exists='replace')
            first = False
        df_chunk.to_sql(
            name=target_table, 
            con=engine, 
            if_exists='append')
        
    df_zones = pd.read_csv(prefix1 + 'taxi_zone_lookup.csv')
    df_zones.to_sql(name=target_table1, con=engine, if_exists='replace')


if __name__ == '__main__':
    run()


