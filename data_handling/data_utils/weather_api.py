# Third Party
import pandas as pd
import numpy as np
import openmeteo_requests
import requests_cache
from retry_requests import retry

# Native
import time


class Weather_API():
    
    def __init__(self, weather_df: pd.DataFrame = None):
        self.cache_session = requests_cache.CachedSession('.cache', expire_after = -1)
        self.retry_session = retry(self.cache_session, retries = 5, backoff_factor = 0.2)
        self.openmeteo = openmeteo_requests.Client(session = self.retry_session)
        self.url = "https://archive-api.open-meteo.com/v1/archive"
        self.rate_limiter = .1 # s/call -- limit of 600 calls/min

        if weather_df is not None:
            self.weather_df = weather_df


    def get_weather(self, df):
        """
        Returns the dataframe with weather data based on datetime, latitude, longitude
        """
        # make dataframe for just dates
        day_df = df[['date','latitude','longitude']].groupby('date').mean().reset_index()
        total_len = day_df['date'].nunique()
        print(f"Requesting data for {total_len} days")
        hourly_df = [] # list to collect hourly dataframes
        for i, row in day_df.iterrows(): # not too inefficient since there are only like 200 days
            if i%10 == 0:
                print(f"{100*(i/total_len):.1f}% Complete")
            date, lat, lon = row.values
            params = {
                "latitude": lat,
                "longitude": lon,
                "start_date": date,
                "end_date": date,
                "hourly": ["temperature_2m", "relative_humidity_2m", "rain", "weather_code", "wind_speed_10m", "cloud_cover", "cloud_cover_low", "cloud_cover_high", "cloud_cover_mid", "is_day", "sunshine_duration", "precipitation", "snowfall", "apparent_temperature", "pressure_msl", "surface_pressure", "wind_direction_10m", "wind_gusts_10m"],
                "timezone": "GMT"
            }
            response = self.openmeteo.weather_api(self.url, params=params)[0] # returns a list of 1
            hourly_df.append(self.get_hourly(response)) # turn response into hourly_df
            time.sleep(self.rate_limiter) # avoid rate limits

        # combine all hourly dataframes
        self.hourly_df = pd.concat(hourly_df, ignore_index=True)
        # merge hourly data into main dataframe
        self.weather_df = self.make_weather_df(df)
    
    def get_hourly(self, response):
        """
        Code from Open-Meteo to build hourly dataframe from response
        """
        # Process hourly data. The order of variables needs to be the same as requested.
        hourly = response.Hourly()
        hourly_temperature_2m = hourly.Variables(0).ValuesAsNumpy()
        hourly_relative_humidity_2m = hourly.Variables(1).ValuesAsNumpy()
        hourly_rain = hourly.Variables(2).ValuesAsNumpy()
        hourly_weather_code = hourly.Variables(3).ValuesAsNumpy()
        hourly_wind_speed_10m = hourly.Variables(4).ValuesAsNumpy()
        hourly_cloud_cover = hourly.Variables(5).ValuesAsNumpy()
        hourly_cloud_cover_low = hourly.Variables(6).ValuesAsNumpy()
        hourly_cloud_cover_high = hourly.Variables(7).ValuesAsNumpy()
        hourly_cloud_cover_mid = hourly.Variables(8).ValuesAsNumpy()
        hourly_is_day = hourly.Variables(9).ValuesAsNumpy()
        hourly_sunshine_duration = hourly.Variables(10).ValuesAsNumpy()
        hourly_precipitation = hourly.Variables(11).ValuesAsNumpy()
        hourly_snowfall = hourly.Variables(12).ValuesAsNumpy()
        hourly_apparent_temperature = hourly.Variables(13).ValuesAsNumpy()
        hourly_pressure_msl = hourly.Variables(14).ValuesAsNumpy()
        hourly_surface_pressure = hourly.Variables(15).ValuesAsNumpy()
        hourly_wind_direction_10m = hourly.Variables(16).ValuesAsNumpy()
        hourly_wind_gusts_10m = hourly.Variables(17).ValuesAsNumpy()

        hourly_data = {"datetime": pd.date_range(
            start = pd.to_datetime(hourly.Time(), unit = "s", utc = True),
            end = pd.to_datetime(hourly.TimeEnd(), unit = "s", utc = True),
            freq = pd.Timedelta(seconds = hourly.Interval()),
            inclusive = "left"
        )}

        hourly_data['date_hour'] = pd.to_datetime(hourly_data['datetime'].date.astype(str) + ' ' +  hourly_data['datetime'].hour.astype(str), format="%Y-%m-%d %H") # addition to map to date 
        hourly_data['hour'] = hourly_data['datetime'].hour # addition to map to hours
        hourly_data['elevation_meters_asl'] = response.Elevation() # addition to map to elevation
        hourly_data["temperature_2m"] = hourly_temperature_2m
        hourly_data["relative_humidity_2m"] = hourly_relative_humidity_2m
        hourly_data["rain"] = hourly_rain
        hourly_data["weather_code"] = hourly_weather_code
        hourly_data["wind_speed_10m"] = hourly_wind_speed_10m
        hourly_data["cloud_cover"] = hourly_cloud_cover
        hourly_data["cloud_cover_low"] = hourly_cloud_cover_low
        hourly_data["cloud_cover_high"] = hourly_cloud_cover_high
        hourly_data["cloud_cover_mid"] = hourly_cloud_cover_mid
        hourly_data["is_day"] = hourly_is_day
        hourly_data["sunshine_duration"] = hourly_sunshine_duration
        hourly_data["precipitation"] = hourly_precipitation
        hourly_data["snowfall"] = hourly_snowfall
        hourly_data["apparent_temperature"] = hourly_apparent_temperature
        hourly_data["pressure_msl"] = hourly_pressure_msl
        hourly_data["surface_pressure"] = hourly_surface_pressure
        hourly_data["wind_direction_10m"] = hourly_wind_direction_10m
        hourly_data["wind_gusts_10m"] = hourly_wind_gusts_10m

        hourly_dataframe = pd.DataFrame(data = hourly_data)
        return hourly_dataframe
    
    def make_weather_df(self, df):
        # Make column to merge on
        cdf = df.copy()
        cdf.loc[:,'hour'] = pd.to_datetime(cdf['time'], format="%H:%M:%S").dt.hour
        cdf.loc[:,'weather_hour'] = pd.to_datetime(cdf['date'].astype(str) + ' ' + cdf['hour'].astype(str), format="%Y-%m-%d %H")

        # Merge with hourly weather data
        cdf = pd.merge(cdf, self.hourly_df, how='left', left_on='weather_hour', right_on='date_hour', suffixes=[None,'_right'])

        # Remove and Organize columns
        remove_cols = ['date_hour', 'datetime','hour'] + [col for col in cdf if '_right' in col.lower()]
        cdf = cdf.drop(columns=remove_cols)
        col_order = ['date','time','weather_hour','latitude','longitude']
        col_order = col_order + [col for col in cdf.columns if col not in col_order]
        cdf = cdf[col_order]
        return cdf