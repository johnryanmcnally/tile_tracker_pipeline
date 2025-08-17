# Third Party Imports
import pandas as pd

# Native Imports
import time
import pickle

# Custom Imports
from data_utils.utils import combine_data, add_bearing_column, add_direction_similarity, cluster_data, reduce_clusters

# Variables
RAWDATAPATH = '/opt/data/raw/'
STAGEDATAPATH = '/opt/data/staged/'
TEMPPATH = '/opt/data/temp/'
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
    print("Combining raw data...")
    start = time.time()
    df = combine_data(datapath=RAWDATAPATH, tile_uuid=tile_uuid, tile_name=tile_name)
    print("Data successfully combined.")
    print(f"Took {time.time() - start:.3f} seconds")
    
    # Add Bearing (DEPRECATED - column no longer used, but could be useful for visualizations)
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

    testing = True # flag for making debugging easier
    if testing:
        # Cluster Data using HDBSCAN
        print('Clustering data...')
        start = time.time()
        db, df['cluster_label'] = cluster_data(df[['latitude','longitude']])
        print('Data successfully clustered.')
        print(f"Took {time.time() - start:.3f} seconds")

        # Reduce clusters by labelling some as transit (-3) using direction similarity
        print('Reassigning clusters based on direction similarity...')
        start = time.time()
        prev_len = df['cluster_label'].nunique()
        df['cluster_label'] = reduce_clusters(df=df)
        print(f"reduced clusters by {prev_len - df['cluster_label'].nunique()} from {prev_len} to {df['cluster_label'].nunique()}")
        print(f"Took {time.time() - start:.3f} seconds")

        # Save HDBSCAN model
        with open(TEMPPATH + f"temp_hdbscan.pkl",'wb+') as f:
            pickle.dump(db, f)
        print(f"Successfully saved model: '{TEMPPATH + 'temp_hdbscan.pkl'}'")

        # Save df to parquet
        df.to_parquet(TEMPPATH + 'feature_engineering.parquet', index=False)
        print(TEMPPATH + 'feature_engineering.parquet')
    else:
        # just reload previous data
        df = pd.read_parquet(TEMPPATH + 'feature_engineering.parquet')
        df.to_parquet(TEMPPATH + 'feature_engineering.parquet', index=False)
        print(TEMPPATH + 'feature_engineering.parquet')

    df.to_parquet(TEMPPATH + 'feature_engineering.parquet', index=False)
    print(TEMPPATH + 'feature_engineering.parquet')