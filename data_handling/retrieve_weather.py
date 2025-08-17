# Third Party Imports
import pandas as pd

# Native Imports
import time

# Custom Imports
from data_utils.weather_api import Weather_API

# Variables
TEMPPATH = '/opt/data/temp/'

if __name__ == '__main__':
    testing = False

    if testing:
        # Load Data
        df = pd.read_parquet(TEMPPATH + 'feature_engineering.parquet')

        # Retrieve Weather Data
        print("Getting weather data from Open-Meteo...")
        start = time.time()
        weather_api = Weather_API()
        weather_api.get_weather(df[['date','time','latitude','longitude']])
        print('Weather data successfully retrieved.')
        print(f"Took {time.time() - start:.3f} seconds")

        weather_api.weather_df.to_parquet(TEMPPATH + 'weather.parquet')
        print(f"Successfully saved processed weather data: '{TEMPPATH + 'weather.parquet'}'")
    else:
        # Load Data
        df = pd.read_parquet(TEMPPATH + 'feature_engineering.parquet')
        df.to_parquet(TEMPPATH + 'weather.parquet')
        print(f"Successfully saved processed weather data: '{TEMPPATH + 'weather.parquet'}'")