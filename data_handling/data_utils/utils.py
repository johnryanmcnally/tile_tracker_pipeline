# Third Party Imports
import pandas as pd
import numpy as np
from sklearn.cluster import HDBSCAN
from sklearn.preprocessing import StandardScaler
from sklearn.metrics.pairwise import cosine_similarity

# Native Imports
from pathlib import Path
from functools import lru_cache, cache
import json

# caching doesn't help here because we are not calling the function repeatedly with the same arguments
# @lru_cache(maxsize=None) 

def combine_data(datapath: str, tile_uuid: str, tile_name: str):
    """
    Function to combine all data from the raw jsons to a dataframe, optimized for speed.

    Parameters
    -----------
        datapath (str): the file path pointing to the directory holding the raw data in json format
        tile_uuid (str): unique ID for the tile
        tile_name (str): human readable name of the tile

    Returns
    ----------
        df (dataframe): dataframe containing the combined data
    """
    all_location_updates = []
    files = Path(datapath).glob('*.json')

    for file in files:
        with open(file, 'r') as f:
            data = json.load(f)
            # Safely access nested data, handle potential missing keys if your JSONs vary
            if tile_uuid in data and 'result' in data[tile_uuid] and \
               'location_updates' in data[tile_uuid]['result']:
                all_location_updates.extend(data[tile_uuid]['result']['location_updates'])

    if not all_location_updates:
        return pd.DataFrame() # Return empty DataFrame if no data found

    df = pd.DataFrame(all_location_updates)

    # Convert 'location_timestamp' to datetime directly
    df['datetime'] = pd.to_datetime(df['location_timestamp'], unit='ms', utc=True)
    df['date'] = df['datetime'].dt.date
    df['time'] = df['datetime'].dt.strftime("%H:%M:%S")

    # Add tile_name column
    df['tile_name'] = tile_name

    # Add tile_uuid column
    df['tile_uuid'] = tile_uuid # Assuming tile_uuid is constant for all data in this combination process

    # Remove Duplicates
    df = df.groupby('location_timestamp', as_index=False).last()
    df = df.sort_values(by='datetime')

    # Ensure all columns exist before reordering to avoid KeyError
    required_cols = ['tile_name', 'tile_uuid', 'location_timestamp', 'datetime', 'date', 'time',
                     'latitude', 'longitude', 'raw_precision', 'precision']
    final_cols = [col for col in required_cols if col in df.columns]
    df = df[final_cols] # Reorder columns

    return df

def numpy_to_hashable_bytes(arr):
    """Converts a NumPy array to hashable bytes, including dtype and shape."""
    # how to rebuild the array:
    # coords = np.frombuffer(bytes_data, dtype=dtype).reshape(shape)
    return arr.tobytes(), arr.dtype, arr.shape

# caching doesn't help here because we are not calling the function repeatedly with the same arguments
# @lru_cache(maxsize=None)
def cluster_data(df):
    """
    Fits an HDBSCAN clustering model to the data and returns the labels

    Parameters
    -----------
        df (dataframe): dataframe that contains the columns to be fit on (should only be ['latitude', 'longitude'])

    Returns
    -----------
        db (sklearn HDBSCAN class): fit model
        db.labels_ (array): array containing the labels
    """
    # scaler = StandardScaler()
    # coords = scaler.fit_transform(df)
    # consider using metric='haversine' in future versions, which also need to remove standard scaler
    coords_radians = np.deg2rad(df[['latitude', 'longitude']].values)
    db = HDBSCAN(metric='haversine', min_cluster_size=5, n_jobs=-1).fit(coords_radians) 
    return db, db.labels_

def add_bearing_column(df):
    """
    Function to calculate bearing for a DataFrame using vectorized operations.

    Parameters
    -----------
        df (DataFrame): dataframe containing ['latitude','longitude']

    Returns
    -----------
        bearing (Series): pandas series containing the bearing for every row
    """
    # Create a copy to avoid SettingWithCopyWarning if df is a slice
    df_copy = df.copy()

    # Shift latitude and longitude to get previous points -- will create NaN value for first row
    df_copy['prev_latitude'] = df_copy['latitude'].shift(1)
    df_copy['prev_longitude'] = df_copy['longitude'].shift(1)

    # Convert degrees to radians for trigonometric functions
    lat1_rad = np.deg2rad(df_copy['prev_latitude'])
    lon1_rad = np.deg2rad(df_copy['prev_longitude'])
    lat2_rad = np.deg2rad(df_copy['latitude'])
    lon2_rad = np.deg2rad(df_copy['longitude'])

    # Calculate differences in longitude
    dLon = lon2_rad - lon1_rad

    # Apply the bearing formula using vectorized NumPy functions
    y = np.sin(dLon) * np.cos(lat2_rad)
    x = np.cos(lat1_rad) * np.sin(lat2_rad) - np.sin(lat1_rad) * np.cos(lat2_rad) * np.cos(dLon)

    # Calculate bearing in radians, then convert to degrees
    brng_rad = np.arctan2(y, x)
    brng_deg = np.rad2deg(brng_rad)

    # Normalize bearing to be between 0 and 360 degrees
    bearing = (brng_deg + 360) % 360

    # Convert the NumPy array result back to a Pandas Series, preserving the original index
    return pd.Series(bearing, index=df.index)


def add_direction_similarity(df):
    """
    Function to calculate direction similarity for a DataFrame using vectorized operations.

    Parameters
    -----------
        df (DataFrame): dataframe containing ['latitude','longitude']

    Returns
    -----------
        direction_similarity (Series): pandas series containing the direction similarity for every row
    """
    # Create a copy to avoid SettingWithCopyWarning if df is a slice
    df_copy = df.copy()

    # ** Below will create NaN value for two rows **
    # Calculate differences for current and previous steps
    df_copy['diff_lat'] = df_copy['latitude'] - df_copy['latitude'].shift(1)
    df_copy['diff_lon'] = df_copy['longitude'] - df_copy['longitude'].shift(1)
    # Shift these differences to get 'previous' differences
    df_copy['prev_diff_lat'] = df_copy['diff_lat'].shift(1)
    df_copy['prev_diff_lon'] = df_copy['diff_lon'].shift(1)

    # Stack the columns to form 2D arrays where each row is a vector
    vector_A = df_copy[['prev_diff_lat', 'prev_diff_lon']].values
    vector_B = df_copy[['diff_lat', 'diff_lon']].values

    # Calculate dot product: x1*x2 + y1*y2
    dot_product = np.sum(vector_A * vector_B, axis=1)

    # Calculate magnitude of vector A: sqrt(x1^2 + y1^2)
    magnitude_A = np.sqrt(np.sum(vector_A**2, axis=1))

    # Calculate magnitude of vector B: sqrt(x2^2 + y2^2)
    magnitude_B = np.sqrt(np.sum(vector_B**2, axis=1))

    # Calculate cosine similarity
    # Handle division by zero: if magnitudes are zero, cosine similarity is undefined (often treated as 0 or NaN)
    # Using np.divide with where clause handles this gracefully
    direction_similarity = np.divide(dot_product,
                                     (magnitude_A * magnitude_B),
                                     out=np.zeros_like(dot_product, dtype=float), # Output array for results
                                     where=(magnitude_A * magnitude_B) != 0)

    # Convert to Pandas Series and ensure the index aligns with the original df
    return pd.Series(direction_similarity, index=df.index)

# prev_len = df['place_cluster_label'].nunique()
# for cluster_label in list(df['place_cluster_label'].unique()):
#     if (cluster_label == -1) | (cluster_label == -2): continue
#     cluster = df[df['place_cluster_label'] == cluster_label]
    
#     if cluster['direction_similarity'].iloc[1:].mean() > .25: #remove first point because it will reference a point not in the cluster
#         cluster_idx = df[df['place_cluster_label'] == cluster_label].index
#         df.loc[cluster_idx,'place_cluster_label'] = -3
# print(f"{len(df[df['place_cluster_label']==-3])} points labelled as transit (-3)")
# print(f"reduced clusters by {prev_len - df['place_cluster_label'].nunique()} from {prev_len} to {df['place_cluster_label'].nunique()}")

# # drop unneccessary columns
# unnamed = [col for col in df.columns if 'unnamed' in col.lower()]
# df = df.drop(columns=['prev_latitude','prev_longitude','diff_lat','prev_diff_lat','diff_lon','prev_diff_lon'] + unnamed)