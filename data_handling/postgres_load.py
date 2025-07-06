# Third Party
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv

# Native
import os

TEMPPATH = '/opt/data/temp/'

# PostgreSQL credentials and database details
load_dotenv() # take environment variables from .env.
db_user = os.getenv("POSTGRESQL_USERNAME")
db_password = os.getenv("POSTGRESQL_PWD")
db_host = 'localhost' # Or your PostgreSQL server IP/hostname
db_port = '5432'      # Default PostgreSQL port
db_name = 'tile_db'

# Create the SQLAlchemy engine
# The format is: 'postgresql+psycopg2://user:password@host:port/database'
engine = create_engine(f'postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}')
files_to_load = ['temp_cluster.parquet', # result of extract, process, cluster, and refine
                 # reverse geocoding results
                 'addresses.parquet',
                 'cluster_address.parquet',
                 'place_ids.parquet',
                 'tags.parquet',
                 # weather api results
                 'weather.parquet']
for fname in files_to_load:
    df = pd.read_parquet(TEMPPATH + fname)
    remove_cols = [col for col in df if 'unnamed' in col.lower()]
    df = df.drop(columns=remove_cols)
    df['tile_name'] = 'John'
    table_name = fname.replace('.csv','').lower() # The name you want for your PostgreSQL table
    try:
        df.to_sql(table_name, engine, if_exists='replace', index=True)
        print(f"DataFrame successfully loaded into table '{table_name}' in PostgreSQL.")

    except Exception as e:
        print(f"Error loading DataFrame: {e}")


if 'engine' in locals() and engine:
    engine.dispose()