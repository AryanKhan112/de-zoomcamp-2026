import pandas as pd
import os

url = "https://d37ci6vzurychx.cloudfront.net/misc/taxi+_zone_lookup.csv"
file_path = os.path.join("seeds", "taxi_zone_lookup.csv")

df = pd.read_csv(url)

df.columns = [x.lower() for x in df.columns]

df.to_csv(file_path, index=False)

print(f"Downloaded seed file to {file_path}")