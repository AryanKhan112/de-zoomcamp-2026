import pandas as pd
from sqlalchemy import create_engine
from time import time

engine = create_engine('postgresql://root:root@localhost:5432/ny_taxi')

# Note: Using compression='gzip' because we verified the file is a binary GZ
df_iter = pd.read_csv('yellow_tripdata_2021-01.csv', iterator=True, chunksize=100000, compression='gzip')

# Create the table schema (First chunk only) - use 'replace' to start fresh
df = next(df_iter)
df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
df.head(n=0).to_sql(name='yellow_taxi_data', con=engine, if_exists='replace')

# Insert the first chunk
df.to_sql(name='yellow_taxi_data', con=engine, if_exists='append')

# LOOP: Process the remaining millions of rows
while True:
    try:
        t_start = time()
        df = next(df_iter)
        
        df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
        df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
        
        df.to_sql(name='yellow_taxi_data', con=engine, if_exists='append')
        
        t_end = time()
        print(f'Inserted another chunk, took {(t_end - t_start):.3f} seconds')
    except StopIteration:
        print("Finished ingesting all data.")
        break