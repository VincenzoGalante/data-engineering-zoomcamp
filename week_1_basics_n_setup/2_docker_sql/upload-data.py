#!/usr/bin/env python
# coding: utf-8

import pandas as pd
from time import time
from sqlalchemy import create_engine


engine = create_engine('postgresql://root:root@localhost:5432/ny_taxi')

df_iter = pd.read_csv('green_tripdata_2019-01.csv.gz', iterator=True, chunksize=100000, low_memory=False)
df = next(df_iter)

df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)

df.head(n=0).to_sql(name='green_taxi_data', con=engine, if_exists='replace')
df.to_sql(name='green_taxi_data', con=engine, if_exists='append')

while True: 
    t_start = time()

    df = next(df_iter)

    df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
    df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)
    
    df.to_sql(name='green_taxi_data', con=engine, if_exists='append')

    t_end = time()

    print('inserted another chunk, took %.3f second' % (t_end - t_start))