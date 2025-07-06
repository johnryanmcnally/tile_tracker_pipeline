# Third Party Imports
import pandas as pd

# Native Imports
import time
import pickle

# Custom Imports
from data_utils.utils import cluster_data, reduce_clusters

# Variables
TEMPPATH = '/opt/data/temp/'

if __name__ == '__main__':
    testing = False

    if testing:
        # Load data from extract_and_process
        df = pd.read_parquet(TEMPPATH + 'temp_extract_and_process.parquet')

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
        df.to_parquet(TEMPPATH + 'temp_cluster.parquet', index=False)
        print(TEMPPATH + 'temp_cluster.parquet') # Print the output path to stdout for Airflow XCom capture
    else:
        # just reload previous data
        df = pd.read_parquet(TEMPPATH + 'temp_cluster.parquet')
        df.to_parquet(TEMPPATH + 'temp_cluster.parquet', index=False)
        print(TEMPPATH + 'temp_cluster.parquet') # Print the output path to stdout for Airflow XCom capture