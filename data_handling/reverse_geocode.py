# Third Party Imports
import pandas as pd

# Native Imports
import os
import time
import json
import logging
logging.basicConfig(level=logging.INFO) # makes the logs appear in Airflow
logger = logging.getLogger(__name__)

# Custom Imports
from data_utils.geocoder import Geocoder

# Variables
TEMPPATH = '/opt/data/temp/'
# if os.getenv('AIRFLOW_CONTEXT_DAG_ID'):
#     # Running inside an Airflow task
#     TEMPPATH = '/opt/data/temp/'
# else:
#     # Running locally or outside Airflow
#     TEMPPATH = './data/temp/'

if __name__ == '__main__':
    # Load Data
    df = pd.read_parquet(TEMPPATH + 'feature_engineering.parquet')
    df = df.dropna(subset=['latitude','longitude'])

    # get previous results
    with open(TEMPPATH + 'geocode_results.json', 'r') as f:
        cache = json.load(f)

    # request reverse geocode information from googlemaps api
    # *** Must have Google Cloud SDK Shell running and authenticated ***
    geocoder = Geocoder(cache=cache)
    geocoder.check_state()
    logger.info("Requesting reverse geocoding from GoogleMaps API...")
    start = time.time()
    clusters_to_try = list(df['cluster_label'].unique())
    geocode_results, cluster_errors = geocoder.geocode_clusters(df[['cluster_label','latitude','longitude']], clusters_to_try)
    
    # retry once on only cluster errors
    if len(cluster_errors) > 0:
        logger.info(f"Retrying on {len(cluster_errors)} clusters")
        geocode_results, cluster_errors = geocoder.geocode_clusters(df[['cluster_label','latitude','longitude']], list(cluster_errors))


    logger.info(f"Done.")
    logger.info(f"Took {time.time() - start:.3f} seconds")

    # Save Result immediately so we dont have to do it again
    with open(TEMPPATH + 'geocode_results.json','w+') as f:
        cache |= geocode_results # in-place update of old values, append new
        json.dump(cache, f)
    logger.info(f"Successfully saved geocoding data: 'geocode_results.json'")

    logger.info(f"Processing geocode results...")
    start = time.time()
    df_tags, df_place_ids, df_addresses, df_cluster_address, norm_cluster_map = geocoder.process_geocode()
    logger.info(f"Took {time.time() - start:.3f} seconds")

    # Save processed geocode data
    df_tags.to_parquet(TEMPPATH + 'tags.parquet')
    df_place_ids.to_parquet(TEMPPATH + 'place_ids.parquet')
    df_addresses.to_parquet(TEMPPATH + 'addresses.parquet')
    df_cluster_address.to_parquet(TEMPPATH + 'cluster_address.parquet')
    logger.info("Geocode results saved to their own dataframes:\n'tags.parquet'\n'place_ids.parquet'\n'addresses.parquet'\n'cluster_address.parquet'")
