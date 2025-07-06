# Third Party Imports
import pandas as pd

# Native Imports
import time

# Custom Imports
from data_utils.utils import combine_data, add_bearing_column, add_direction_similarity

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

    df.to_parquet(TEMPPATH + 'temp_extract_and_process.parquet', index=False)
    print(TEMPPATH + 'temp_extract_and_process.parquet')