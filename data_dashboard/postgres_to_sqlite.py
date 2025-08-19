# Third Party
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv

# Native
import os
import sqlite3

# PostgreSQL credentials and database details
load_dotenv() # take environment variables from .env.
db_user = os.getenv("POSTGRESQL_USERNAME")
db_password = os.getenv("POSTGRESQL_PWD")
db_host = 'localhost'
db_port = '5432'
db_name = 'tile_db'
try:
    conn = create_engine(f'postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}')
except:
    print('Connection Failed')

# Connect to SQLite3
sqlite_conn = sqlite3.connect('data_dashboard/data/dashboard_data.sqlite')

# Query Postgress and Save to SQLite
limit = 100000

query = f"""
SELECT
    datetime,
    date,
    time,
    latitude,
    longitude,
    cluster_label
FROM tile_data_john
LIMIT {limit};
"""
df = pd.read_sql(query, con=conn)
df.to_sql('tile_data_john', sqlite_conn, if_exists='replace', index=True)
print('saved tile_data_john to sqlite')

query = f"""
SELECT
    tag,
    cluster_label
FROM tags
LIMIT {limit};
"""
df = pd.read_sql(query, con=conn)
df.to_sql('tags', sqlite_conn, if_exists='replace', index=True)
print('saved tags to sqlite')

query = f"""
SELECT
    date,
    temperature_2m,
    relative_humidity_2m,
    precipitation,
    elevation_meters_asl,
    cloud_cover
FROM weather
LIMIT {limit};
"""
df = pd.read_sql(query, con=conn)
df.to_sql('weather', sqlite_conn, if_exists='replace', index=True)
print('saved weather to sqlite')