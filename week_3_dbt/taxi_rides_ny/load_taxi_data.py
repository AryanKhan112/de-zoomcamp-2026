import pandas as pd
from sqlalchemy import create_engine

engine = create_engine('postgresql://root:root@localhost:5432/ny_taxi')

def load_data(url, table_name):
    print(f"Downloading {table_name}...")
    df_iter = pd.read_csv(url, iterator=True, chunksize=100000)
    
    df = next(df_iter)
    
    df.columns = [x.lower() for x in df.columns]
    
    if 'lpep_pickup_datetime' in df.columns:
        df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
        df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)
    if 'tpep_pickup_datetime' in df.columns:
        df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
        df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')
    
    df.to_sql(name=table_name, con=engine, if_exists='append')
    print(f"Inserted first chunk into {table_name}...")

    for chunk in df_iter:
        chunk.columns = [x.lower() for x in chunk.columns]
        
        if 'lpep_pickup_datetime' in chunk.columns:
            chunk.lpep_pickup_datetime = pd.to_datetime(chunk.lpep_pickup_datetime)
            chunk.lpep_dropoff_datetime = pd.to_datetime(chunk.lpep_dropoff_datetime)
        if 'tpep_pickup_datetime' in chunk.columns:
            chunk.tpep_pickup_datetime = pd.to_datetime(chunk.tpep_pickup_datetime)
            chunk.tpep_dropoff_datetime = pd.to_datetime(chunk.tpep_dropoff_datetime)
            
        chunk.to_sql(name=table_name, con=engine, if_exists='append')
        print(f"Inserted chunk...")

green_url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-09.csv.gz"
yellow_url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2019-09.csv.gz"

print("Starting ingestion...")
load_data(green_url, "green_tripdata")
print("Green Taxi Loaded.")
load_data(yellow_url, "yellow_tripdata")
print("Yellow Taxi Loaded.")
print("DONE.")