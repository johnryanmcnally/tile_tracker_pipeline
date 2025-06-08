# Third Party Imports
import pandas as pd
import numpy as np

# Native Imports
import time

# Custom Imports
from data_utils.utils import *

# Combine data from raw
RAWDATAPATH = r'C:\Users\joyam\Documents\JohnProjects\TileTracking\data\raw\\'
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
    testing = True

    if testing:
        df = pd.read_csv('data_handling/save_state.csv')
        print('Loaded data state from data_handling/save_state.csv')
    else:
        print("Combining raw data...")
        start = time.time()
        df = combine_data(datapath=RAWDATAPATH, tile_uuid=tile_uuid, tile_name=tile_name)
        print("Data successfully combined.")
        print(f"Took {time.time() - start:.3f} seconds")

        print('Clustering data...')
        start = time.time()
        db, df['place_cluster_label'] = cluster_data(df[['latitude','longitude']])
        print('Data successfully clustered.')
        print(f"Took {time.time() - start:.3f} seconds")

        print('Adding bearing column...')
        start = time.time()
        df['bearing'] = add_bearing_column(df[['latitude','longitude']])
        print('Data successfully added.')
        print(f"Took {time.time() - start:.3f} seconds")

        print('Adding direction similarity column...')
        start = time.time()
        df['direction_similarity'] = add_direction_similarity(df[['latitude','longitude']])
        print('Data successfully added.')
        print(f"Took {time.time() - start:.3f} seconds") 
        print(df.head(5))

        print('Saving state to data_handling/save_state.csv')
        df.to_csv('data_handling/save_state.csv')

    print('Adding bearing column...')
    start = time.time()
    df['bearing'] = add_bearing_column(df[['latitude','longitude']])
    print('Data successfully added.')
    print(f"Took {time.time() - start:.3f} seconds")

    print('Adding direction similarity column...')
    start = time.time()
    df['direction_similarity'] = add_direction_similarity(df[['latitude','longitude']])
    print('Data successfully added.')
    print(f"Took {time.time() - start:.3f} seconds") 
    print(df.head(5))


    print('Reassigning clusters based on direction similarity...')
    start = time.time()

    prev_len = df['place_cluster_label'].nunique()
    for cluster_label in list(df['place_cluster_label'].unique()):
        if (cluster_label == -1) | (cluster_label == -2): continue
        cluster = df[df['place_cluster_label'] == cluster_label]
        
        if cluster['direction_similarity'].iloc[1:].mean() > .25: #remove first point because it will reference a point not in the cluster
            cluster_idx = df[df['place_cluster_label'] == cluster_label].index
            df.loc[cluster_idx,'place_cluster_label'] = -3
    print(f"{len(df[df['place_cluster_label']==-3])} points labelled as transit (-3)")
    print(f"reduced clusters by {prev_len - df['place_cluster_label'].nunique()} from {prev_len} to {df['place_cluster_label'].nunique()}")

    print('Data successfully added.')
    print(f"Took {time.time() - start:.3f} seconds") 
    print(df.head(5))    
