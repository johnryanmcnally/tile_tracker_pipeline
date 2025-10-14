# Third Party Imports
import pandas as pd
import numpy as np

# Native Imports
import os
import sys
import time
import pickle
import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Custom Imports
from data_utils.utils import combine_data, add_bearing_column, add_direction_similarity, cluster_data, reduce_clusters, get_db_info

# Variables
RAWDATAPATH = '/opt/data/raw/'
STAGEDATAPATH = '/opt/data/staged/'
TEMPPATH = '/opt/data/temp/'
# if os.getenv('AIRFLOW_CONTEXT_DAG_ID'):
#     # Running inside an Airflow task
#     RAWDATAPATH = '/opt/data/raw/'
#     STAGEDATAPATH = '/opt/data/staged/'
#     TEMPPATH = '/opt/data/temp/'
# else:
#     # Running locally or outside Airflow
#     RAWDATAPATH = './data/raw/'
#     STAGEDATAPATH = './data/staged/'
#     TEMPPATH = './data/temp/'

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
    # Start from Raw Data
    logger.info("Combining raw data...")
    start = time.time()
    # Check data that has already been processed
    most_recent_date, most_recent_timestamp, cluster_labels = get_db_info()
    # combine new data
    df = combine_data(datapath=RAWDATAPATH, tile_uuid=tile_uuid, tile_name=tile_name,
                      most_recent_date=most_recent_date, most_recent_timestamp=most_recent_timestamp)
    if len(df) == 0:
        logger.info("Database is up to date")
        sys.exit(0)
    else:
        logger.info("Data successfully combined.")
        logger.info(f"Took {time.time() - start:.3f} seconds")

    # Add Bearing (DEPRECATED - column no longer used, but could be useful for visualizations)
    logger.info('Adding bearing column...')
    start = time.time()
    df['bearing'] = add_bearing_column(df[['latitude','longitude']])
    logger.info('Data successfully added.')
    logger.info(f"Took {time.time() - start:.3f} seconds")

    # Add Direction Similarity
    logger.info('Adding direction similarity column...')
    start = time.time()
    df['direction_similarity'] = add_direction_similarity(df[['latitude','longitude']])
    logger.info('Data successfully added.')
    logger.info(f"Took {time.time() - start:.3f} seconds")

    testing = True # flag for making debugging easier
    if testing:
        # Cluster Data using HDBSCAN
        logger.info('Clustering data...')
        start = time.time()
        db, df['cluster_label'] = cluster_data(df[['latitude','longitude']])
        logger.info('Data successfully clustered.')
        logger.info(f"Took {time.time() - start:.3f} seconds")

        # Reduce clusters by labelling some as transit (-3) using direction similarity
        logger.info('Reassigning clusters based on direction similarity...')
        start = time.time()
        prev_len = df['cluster_label'].nunique()
        df['cluster_label'] = reduce_clusters(df=df)
        df = df.dropna(subset=['direction_similarity'])
        # update cluster labels to be higher than the existing labels in DB
        df.loc[:,'cluster_label'] = np.where((df['cluster_label'] != -1) & (df['cluster_label'] != -3),
                                        df['cluster_label'] + cluster_labels[-1],
                                        df['cluster_label'])
        logger.info(f"reduced clusters by {prev_len - df['cluster_label'].nunique()} from {prev_len} to {df['cluster_label'].nunique()}")
        logger.info(f"Took {time.time() - start:.3f} seconds")

        # Save HDBSCAN model
        with open(TEMPPATH + f"temp_hdbscan.pkl",'wb+') as f:
            pickle.dump(db, f)
        logger.info(f"Successfully saved model: '{TEMPPATH + 'temp_hdbscan.pkl'}'")

        # Save df to parquet
        df.to_parquet(TEMPPATH + 'feature_engineering.parquet', index=False)
        logger.info(TEMPPATH + 'feature_engineering.parquet')
    else:
        # just reload previous data
        df = pd.read_parquet(TEMPPATH + 'feature_engineering.parquet')
        df.to_parquet(TEMPPATH + 'feature_engineering.parquet', index=False)
        logger.info(TEMPPATH + 'feature_engineering.parquet')
