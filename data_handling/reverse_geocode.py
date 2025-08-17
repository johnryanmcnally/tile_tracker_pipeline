# Third Party Imports
import pandas as pd

# Native Imports
import time
import json

# Custom Imports
from data_utils.geocoder import Geocoder

# Variables
TEMPPATH = '/opt/data/temp/'

if __name__ == '__main__':
    # Load Data
    df = pd.read_parquet(TEMPPATH + 'feature_engineering.parquet')

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
    with open(TEMPPATH + 'geocode_results.json','w+') as f:
        json.dump(geocode_results, f)
    print(f"Successfully saved geocoding data: 'geocode_results.json'")

    print("Processing geocode results...")
    start = time.time()
    df_tags, df_place_ids, df_addresses, df_cluster_address, norm_cluster_map = geocoder.process_geocode()
    print(f"Took {time.time() - start:.3f} seconds")

    # Save processed geocode data
    df_tags.to_parquet(TEMPPATH + 'tags.parquet')
    df_place_ids.to_parquet(TEMPPATH + 'place_ids.parquet')
    df_addresses.to_parquet(TEMPPATH + 'addresses.parquet')
    df_cluster_address.to_parquet(TEMPPATH + 'cluster_address.parquet')
    print("Geocode results saved to their own dataframes:\n'tags.parquet'\n'place_ids.parquet'\n'addresses.parquet'\n'cluster_address.parquet'")