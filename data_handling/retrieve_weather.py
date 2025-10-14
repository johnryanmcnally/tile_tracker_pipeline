# Third Party Imports
import pandas as pd

# Native Imports
import os
import time
import logging
logging.basicConfig(level=logging.INFO) # makes the logs appear in Airflow
logger = logging.getLogger(__name__)

# Custom Imports
from data_utils.weather_api import Weather_API

# Variables
TEMPPATH = '/opt/data/temp/'
# if os.getenv('AIRFLOW_CONTEXT_DAG_ID'):
#     # Running inside an Airflow task
#     TEMPPATH = '/opt/data/temp/'
# else:
#     # Running locally or outside Airflow
#     TEMPPATH = './data/temp/'

if __name__ == '__main__':
    testing = True

    if testing:
        # Load Data
        df = pd.read_parquet(TEMPPATH + 'feature_engineering.parquet')

        # Retrieve Weather Data
        logger.info("Getting weather data from Open-Meteo...")
        start = time.time()
        weather_api = Weather_API()
        weather_api.get_weather(df[['date','time','latitude','longitude']])
        logger.info('Weather data successfully retrieved.')
        logger.info(f"Took {time.time() - start:.3f} seconds")

        weather_api.weather_df.to_parquet(TEMPPATH + 'weather.parquet')
        logger.info(f"Successfully saved processed weather data: '{TEMPPATH + 'weather.parquet'}'")
    else:
        # Load Data
        df = pd.read_parquet(TEMPPATH + 'feature_engineering.parquet')
        df.to_parquet(TEMPPATH + 'weather.parquet')
        logger.info(f"Successfully saved processed weather data: '{TEMPPATH + 'weather.parquet'}'")
