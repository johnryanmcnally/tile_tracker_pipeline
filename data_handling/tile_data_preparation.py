# Third Party Imports
import pandas as pd

# Native Imports
import time
import pickle
import json

# Custom Imports
from data_utils.utils import *
from data_utils.geocoder import Geocoder
from data_utils.weather_api import Weather_API

# Variables
RAWDATAPATH = r'data\raw\\'
STAGEDATAPATH = r'data\staged\\'
tilenames = { # From pytile
    '0287c8181aa557e7': 'Maya', # On Maya's Camera
    '02df4813aa180c3a': "Maya's Backpack",
    '06c5863b0ea97d00': 'John', # On Sling Backpack
    '06e9828702df2f1f': "John's Backpack",
    'p!0028e4d51b64dafa7db22c75e373903b': "John's iPhone", # No location recorded
    'p!27a7386a743b1de5fd19cf5c3873dea8': "Maya's iPhone", # No location recorded
    }
tilenames_reverse = {val:key for key,val in tilenames.items()}
tile_name = "John"
tile_uuid = tilenames_reverse[tile_name]


if __name__ == "__main__":
    # set this flag to test new code at the bottom, then move tested code to 'else' to save new save state
    testing = False
    # Option to rerun the api calls
    rerun_geocode_api = True
    rerun_weather_api = True
    
    if testing:
        # start from save state
        df = pd.read_csv('data_handling/save_state.csv')
        print('Loaded data state from data_handling/save_state.csv')
    else:
        # Start from Raw Data
        print("Combining raw data...")
        start = time.time()
        df = combine_data(datapath=RAWDATAPATH, tile_uuid=tile_uuid, tile_name=tile_name)
        print("Data successfully combined.")
        print(f"Took {time.time() - start:.3f} seconds")

        # Cluster Data using HDBSCAN
        print('Clustering data...')
        start = time.time()
        db, df['cluster_label'] = cluster_data(df[['latitude','longitude']])
        print('Data successfully clustered.')
        print(f"Took {time.time() - start:.3f} seconds")

        # Add Bearing
        print('Adding bearing column...')
        start = time.time()
        df['bearing'] = add_bearing_column(df[['latitude','longitude']])
        print('Data successfully added.')
        print(f"Took {time.time() - start:.3f} seconds")

        # Add Direction Similarity
        print('Adding direction similarity column...')
        start = time.time()
        df['direction_similarity'] = add_direction_similarity(df[['latitude','longitude']])
        print('Data successfully added.')
        print(f"Took {time.time() - start:.3f} seconds") 

        # Reduce clusters by labelling some as transit (-3) using direction similarity
        print('Reassigning clusters based on direction similarity...')
        start = time.time()
        prev_len = df['cluster_label'].nunique()
        df['cluster_label'] = reduce_clusters(df=df)
        print(f"reduced clusters by {prev_len - df['cluster_label'].nunique()} from {prev_len} to {df['cluster_label'].nunique()}")
        print(f"Took {time.time() - start:.3f} seconds") 
        
        if rerun_geocode_api:
            # request reverse geocode information from googlemaps api
            # *** Must have Google Cloud SDK Shell running and authenticated ***
            geocoder = Geocoder()
            geocoder.check_state()
            print(f"Requestion reverse geocoding from GoogleMaps API...")
            start = time.time()
            geocode_results = geocoder.geocode_clusters(df[['cluster_label','latitude','longitude']])
            print("Done.")
            print(f"Took {time.time() - start:.3f} seconds") 
            # Save Result immediately so we dont have to do it again
            with open(STAGEDATAPATH + 'geocode_results.json','w+') as f:
                json.dump(geocode_results, f)
            print(f"Successfully saved geocoding data: 'geocode_results.json'")
        else:
            with open(STAGEDATAPATH + 'geocode_results.json','r') as f:
                geocode_results = json.load(f)
            print("loaded saved geocode results from 'geocode_results.json'")

            # initialize with pre-saved data
            geocoder = Geocoder(geocode_results = geocode_results, df = df[['cluster_label','latitude','longitude']])

        print("Processing geocode results...")
        start = time.time()
        df_tags, df_place_ids, df_addresses, df_cluster_address, norm_cluster_map = geocoder.process_geocode()
        print(f"Took {time.time() - start:.3f} seconds")

        print("Saving all of the hard work...")
        # Save Data From Tile
        df.loc[:,'norm_cluster_label'] = df['cluster_label'].map(norm_cluster_map)
        df.to_csv(STAGEDATAPATH + f'tile_data_{tile_name}.csv')
        print(f"Successfully saved Tile data: 'tile_data_{tile_name}.csv'")

        # Save HDBSCAN model
        with open(f"models/tile_hdbscan.pkl",'wb+') as f:
            pickle.dump(db, f)
        print(f"Successfully saved model: 'tile_hdbscan.pkl'")

        # Save processed geocode data
        df_tags.to_csv(STAGEDATAPATH + 'tags.csv')
        df_place_ids.to_csv(STAGEDATAPATH + 'place_ids.csv')
        df_addresses.to_csv(STAGEDATAPATH + 'addresses.csv')
        df_cluster_address.to_csv(STAGEDATAPATH + 'cluster_address.csv')
        print("Geocode results saved to their own dataframes: 'tags.csv', 'place_ids.csv', 'addresses.csv', 'cluster_address.csv'")

        if rerun_weather_api:
            print("Getting weather data from Open-Meteo...")
            start = time.time()
            weather_api = Weather_API()
            weather_api.get_weather(df[['date','time','latitude','longitude']])
            print('Weather data successfully retrieved.')
            print(f"Took {time.time() - start:.3f} seconds")

            weather_api.weather_df.to_csv(STAGEDATAPATH + 'weather.csv')
            print(f"Successfully saved processed weather data: 'weather.csv'")
        else:
            weather_df = pd.read_csv(STAGEDATAPATH + 'weather.csv')
            # initialize with pre-saved data
            weather_api = Weather_API(weather_df = weather_df)
            print("Loaded saved weather data from 'weather.csv'")
     
    # *** Testing Area ***

    

