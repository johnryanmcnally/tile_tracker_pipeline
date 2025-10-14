# Third Party
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv

# Native
import os
import sqlite3
import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# PostgreSQL credentials and database details
load_dotenv() # take environment variables from .env.
db_user = os.getenv("POSTGRESQL_USERNAME")
db_password = os.getenv("POSTGRESQL_PWD")
db_host = 'host.docker.internal'
db_port = '5432'
db_name = 'tile_db'
try:
    conn = create_engine(f'postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}')
except:
    print('Connection Failed')

# Connect to SQLite3
DASHBOARDDATAPATH = "./data_dashboard/data/"
sqlite_conn = sqlite3.connect(DASHBOARDDATAPATH + 'dashboard_data.sqlite')

# Query Postgress and Save to SQLite
query = f"""
SELECT
    datetime,
    date,
    time,
    latitude,
    longitude,
    cluster_label
FROM tile_data_john
;
"""
logger.info("Loading data from 'tile_data_john'")
df = pd.read_sql(query, con=conn)
df.to_sql('tile_data_john', sqlite_conn, if_exists='replace', index=True)
logger.info('saved tile_data_john to sqlite')

query = f"""
SELECT
    tag,
    cluster_label
FROM tags
;
"""
logger.info("Loading data from 'tags'")
df = pd.read_sql(query, con=conn)
df.to_sql('tags', sqlite_conn, if_exists='replace', index=True)
logger.info('saved tags to sqlite')

query = f"""
SELECT
    date,
    temperature_2m,
    relative_humidity_2m,
    precipitation,
    elevation_meters_asl,
    cloud_cover
FROM weather
;
"""
logger.info("Loading data from 'weather'")
df = pd.read_sql(query, con=conn)
df.to_sql('weather', sqlite_conn, if_exists='replace', index=True)
logger.info('saved weather to sqlite')

query = f"""
SELECT
    cluster_label,
    country
FROM cluster_address
;
"""
logger.info("Loading data from 'cluster_address'")
df = pd.read_sql(query, con=conn)
df.to_sql('cluster_address', sqlite_conn, if_exists='replace', index=True)
logger.info('saved cluster_address to sqlite')