# Third Party
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# Native
import os
import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


TEMPPATH = '/opt/data/temp/'

# PostgreSQL credentials and database details
load_dotenv() # take environment variables from .env.
db_user = os.getenv("POSTGRESQL_USERNAME")
db_password = os.getenv("POSTGRESQL_PWD")
db_host = 'host.docker.internal'
db_port = '5432'
db_name = 'tile_db'

# Create the SQLAlchemy engine
engine = create_engine(f'postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}')
files_to_load = [
                'feature_engineering.parquet', # result of extract, process, cluster, and refine
                 # reverse geocoding results
                 'addresses.parquet',
                 'cluster_address.parquet',
                 'place_ids.parquet',
                 'tags.parquet',
                 # weather api results
                 'weather.parquet']

# keys to remove duplicates in each table
conflict_keys_map = {
    'tile_data_john':['location_timestamp'],
    'addresses':['cluster_label', 'address'],
    'cluster_address':['cluster_label'],
    'place_ids':['cluster_label','place_id'],
    'tags':['cluster_label','tag'],
    'weather':['date','time']
}

for fname in files_to_load:
    # read and clean data
    df = pd.read_parquet(TEMPPATH + fname)
    remove_cols = [col for col in df if 'unnamed' in col.lower()]
    df = df.drop(columns=remove_cols)
    df['tile_name'] = 'John'

    # set up sql variables
    table_name = fname.replace('.parquet','').lower()
    if table_name == 'feature_engineering':
        table_name = 'tile_data_john'
    conflict_keys = conflict_keys_map[table_name]

    # append to database
    try:
        df.to_sql(table_name, engine, if_exists='append')
        logger.info(f'Data appended to {table_name}')
    except:
        logger.error(f'Unable to append to {table_name}')

    # remove duplicates in table
    try:
        conflict_string = ', '.join([key for key in conflict_keys])
        duplicate_query = f"""
WITH duplicate_finder AS (
    SELECT index, {conflict_string},
        ROW_NUMBER() OVER(
            PARTITION BY {conflict_string}
            ORDER BY index DESC
        ) as rn
    FROM
        {table_name}
)
DELETE FROM {table_name}
WHERE index IN (
    SELECT index
    FROM duplicate_finder
    WHERE rn > 1
);
"""
        with engine.begin() as connection:
            result = connection.execute(text(duplicate_query))
            # The transaction is committed automatically upon exiting the 'with engine.begin()' block.
            print(f"Successfully deleted {result.rowcount} duplicate rows.")

    except Exception as e:
        logger.error(f'Unable to remove duplicates: {e}')
    
if 'engine' in locals() and engine:
    engine.dispose()