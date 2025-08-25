# Third Party
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import psycopg2

# Native
import os

STAGEDDATAPATH = 'data/staged/'

# PostgreSQL credentials and database details

"""
FUTURE JOHN, This is Past John.

You were working on getting an AWS RDS Database set up so that you could
use AWS Bedrock to set up a RAG for the dashboard. There was some error
when trying to load the 'staged' data into the database. I think its something
to do with the permissions on AWS. Just run this code and it might work. The error:
psycopg2.OperationalError: connection to server at "tile-db.c98o2iuaknzp.us-east-2.rds.amazonaws.com" (172.31.40.65), port 5432 failed: Connection timed out (0x0000274C/10060)
        Is the server running on that host and accepting TCP/IP connections?

I also tried loading the data into Google BigQuery but it the vector store didnt want
to accept the data.

The RAG api running with Ollama on docker locally all works. To have it hosted
though, there were a bunch of hoops to jump through on AWS. Didn't try on GCP.

"""


load_dotenv() # take environment variables from .env.
db_user = "postgres"
db_password = "!PoKKgneJ0#PwZ-XSw3ZL(dI<i*t"
db_host = 'tile-db.c98o2iuaknzp.us-east-2.rds.amazonaws.com'
db_port = '5432'
db_name = 'tile_db'

connection = psycopg2.connect(host=db_host, database=db_name,
                              user=db_user, password=db_password,
                              port=db_port)
print('connected')

cursor = connection.cursor()
cursor.execute("SELECT version()")
db_version=cursor.fetchone()
print(db_version)

cursor.close()

# Create the SQLAlchemy engine
# print('connecting to database')
# engine = create_engine(f'postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}')
# files_to_load = ['tile_data_john.csv', # result of extract, process, cluster, and refine
#                  # reverse geocoding results
#                  'addresses.csv',
#                  'cluster_address.csv',
#                  'place_ids.csv',
#                  'tags.csv',
#                  # weather api results
#                  'weather.csv']
# for fname in files_to_load:
#     print(f"loading file {fname}")
#     df = pd.read_csv(STAGEDDATAPATH + fname)
#     remove_cols = [col for col in df if 'unnamed' in col.lower()]
#     df = df.drop(columns=remove_cols)
#     df['tile_name'] = 'John'
#     table_name = fname.replace('.csv','').lower()
#     try:
#         df.to_sql(table_name, engine, if_exists='replace', index=True)
#         print(f"DataFrame successfully loaded into table '{table_name}' in PostgreSQL.")

#     except Exception as e:
#         print(f"Error loading DataFrame: {e}")


# if 'engine' in locals() and engine:
#     engine.dispose()