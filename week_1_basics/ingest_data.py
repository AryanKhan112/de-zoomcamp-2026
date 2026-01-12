import pandas as pd
from sqlalchemy import create_engine
from time import time

engine = create_engine('postgresql://root:root@localhost:5432/ny_taxi')

df_iter = pd.read_csv('yellow_tripdata_2021-01.csv', iterator=True, chunksize=100000, compression='gzip')

df = next(df_iter)

df.columns = [c.lower() for c in df.columns]

df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

df.to_sql(name='yellow_taxi_data', con=engine, if_exists='replace', index=False)

while True:
    try:
        t_start = time()
        df = next(df_iter)
        
        df.columns = [c.lower() for c in df.columns]
        df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
        df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
        
        df.to_sql(name='yellow_taxi_data', con=engine, if_exists='append', index=False)
        
        t_end = time()
        print(f'Inserted another chunk, took {(t_end - t_start):.3f} seconds')
    except StopIteration:
        print("Finished ingesting all data.")
        break