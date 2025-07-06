# Third Party
import googlemaps
from dotenv import load_dotenv
import pandas as pd
import numpy as np
from anyascii import anyascii

# Native
import time
import os

# Custom



"""
In order to use this script the Google Cloud SDK Shell needs to be running

1. Open 'Google Cloud SDK Shell'
2. Run command: 'gcloud init'
3. Follow prompts to sign in
4. Choose 'crafty-sound-462212-u1' as the project

"""

class Geocoder():
    """
    Class to facilitate data to and from Google Maps API

    Attributes
    ----------
    google_api_key : str
        API Key for Google Maps
    client : googlemaps.Client
        client for GoogleMaps API
    geocode_results : dict
        dict containing {cluster label: api_response}
    df : pandas DataFrame
        has columns ['cluster_label','latitude','longitude']

    Methods
    -------
    check_state()
        ensure client is running correctly
    geocode_clusters(df)
        make the API call to GoogleMaps API and save result
    process_geocode()
        process the results from the API responses

    """
    def __init__(self, geocode_results = None, df = None):
        """
        Initialize Geocoder

        Parameters
        -----------
        geocode_results : str [optional]
            load pre-saved results from API call
        df : pandas DataFrame [optional]
            load a dataframe

        Returns
        -----------
        None
        """
        # retrieve api key
        load_dotenv() # take environment variables from .env.
        self.google_api_key = os.environ.get("GOOGLE_API_KEY")
        # Set up client that includes Reverse Geocoding
        self.client = googlemaps.Client(key=f"{self.google_api_key}")

        if geocode_results is not None:
            self.geocode_results = geocode_results
        if df is not None:
            self.df = df

    def check_state(self):
        """
        Check state of GoogleMaps API client
        """
        try:
            state = self.client.__getstate__()
            return state
        except Exception as e:
            print(e)

    # Request reverse geocoding from google api
    def geocode_clusters(self, df: pd.DataFrame) -> dict:
        """
        Request reverse geocoding from GoogleMaps API

        Parameters
        -----------
        df : pandas DataFrame
            contains columns ['cluster_label','latitude','longitude']

        Returns
        -----------
        geocode_results : dict
            dictionary containing {cluster_label: api_response}
        """
        self.df = df
        total_len = df['cluster_label'].nunique()
        self.geocode_results = {}
        for i, cluster_label in enumerate(list(df['cluster_label'].unique())):
            if i%50 == 0:
                print(f"{100*(i/total_len):.1f}% Complete")
            # reverse geocode the mean lat and lon of the cluster
            lat, lon = df[df['cluster_label'] == cluster_label][['latitude','longitude']].mean().values
            self.geocode_results[str(cluster_label)] = self.client.reverse_geocode((lat, lon))
            time.sleep(.02) # to stay under the 3000 requests per minute ~ .02 sec per request

        return self.geocode_results

    
    def process_geocode(self):
        """
        Process responses from GoogleMaps API

        Parameters
        -----------
        None

        Returns
        -----------
        df_tags : pandas DataFrame
            contains columns ['cluster_label','tag'] one row per possible tag
        df_place_ids : pandas DataFrame
            contains columns ['cluster_label','place_id'] one row per possible place_id
        df_addresses : pandas DataFrame
            contains columns ['cluster_label','address'] one row per possible address

        """
        # Initialize empty lists to collect data for our new DataFrames
        all_tags = []
        all_place_ids = []
        all_addresses = []

        # --- Process special clusters (-3 and -1) ---
        # For transit clusters (-3)
        label = -3
        all_tags.append({'cluster_label': label, 'tag': 'transit'})
        all_place_ids.append({'cluster_label': label, 'place_id': 'none'})
        all_addresses.append({'cluster_label': label, 'address': 'none'})

        # For outlier clusters (-1)
        label = -1
        all_tags.append({'cluster_label': label, 'tag': 'outlier'})
        all_place_ids.append({'cluster_label': label, 'place_id': 'none'})
        all_addresses.append({'cluster_label': label, 'address': 'none'})

        # --- Process regular clusters (not -1, -2, or -3) ---
        # Get labels that are not special cases and exist in geocode_results
        regular_cluster_labels = [
            label for label in self.df['cluster_label'].unique()
            if label not in [-1, -2, -3] and str(label) in self.geocode_results
        ]

        for cluster_label in regular_cluster_labels:
            geocoded_items = self.geocode_results[str(cluster_label)]
            
            # Filter geocode results once per cluster for valid location types
            filtered_results = [
                item for item in geocoded_items
                if item['geometry']['location_type'] not in ['RANGE_INTERPOLATED', 'APPROXIMATE']
            ]

            for item in filtered_results:
                # Tags: handle nested list of types
                if 'types' in item and isinstance(item['types'], list):
                    for tag in item['types']:
                        all_tags.append({'cluster_label': cluster_label, 'tag': tag})
                
                # Place IDs
                if 'place_id' in item:
                    all_place_ids.append({'cluster_label': cluster_label, 'place_id': item['place_id']})
                
                # Addresses
                if 'formatted_address' in item:
                    all_addresses.append({'cluster_label': cluster_label, 'address': item['formatted_address']})

        # Create the new DataFrames
        self.df_tags = pd.DataFrame(all_tags)
        self.df_place_ids = pd.DataFrame(all_place_ids)
        self.df_possible_addresses = pd.DataFrame(all_addresses)
        # Further process geocode results for city, country, etc
        # contains info for top address in cluster, so save to new frame
        self.df_cluster_address = self.add_address_info()

        # Use address information to further normalize cluster_labels
        # Many clusters have the same primary address -- normalize based on that
        self.norm_cluster_map = self.get_normalized_cluster_mapping()
        self.df_tags.loc[:,'norm_cluster_label'] = self.df_tags['cluster_label'].map(self.norm_cluster_map)
        self.df_place_ids.loc[:,'norm_cluster_label'] = self.df_place_ids['cluster_label'].map(self.norm_cluster_map)
        self.df_possible_addresses.loc[:,'norm_cluster_label'] = self.df_possible_addresses['cluster_label'].map(self.norm_cluster_map)
        self.df_cluster_address.loc[:,'norm_cluster_label'] = self.df_cluster_address['cluster_label'].astype(int).map(self.norm_cluster_map)      


        return self.df_tags, self.df_place_ids, self.df_possible_addresses, self.df_cluster_address, self.norm_cluster_map
    
    def add_address_info(self):
        """
        Further process geocode results to extract the primary address's compenents

        Parameters
        -----------
        None

        Returns
        -----------
        df_cluster_address : pd.DataFrame
            dataframe containing additional information for the primary address in a cluster
        """
        address_components = ['cluster_label',
                              'administrative_area_level_1','administrative_area_level_2',
                              'administrative_area_level_3','administrative_area_level_4',
                              'street_number','route','neighborhood','locality',
                              'country','postal_code','postal_code_suffix','plus_code']
        address_info = []
        for key in self.geocode_results.keys():
            # print(key, type(key))
            if key in ['-1','-3']:
                loc_info = {comp:np.nan for comp in address_components}
                loc_info['cluster_label'] = key
                address_info.append(loc_info)
            else:
                parsed_components = {}
                for comp in self.geocode_results[key][0]['address_components']:
                    component_type = comp['types'][0]
                    if component_type in address_components:
                        long_name_value = comp['long_name']
                        
                        # Check if the value is not None and convert it to string
                        if long_name_value is not None:
                            # Use str() to convert integers, floats, etc., to strings
                            parsed_components[component_type] = anyascii(str(long_name_value))
                        else:
                            # If it's None, you might want to keep it as None or treat it as NaN
                            parsed_components[component_type] = np.nan # Or None, depending on preference

                # Now, build the final loc_info dictionary
                loc_info = {
                    tag: parsed_components.get(tag, np.nan)
                    for tag in address_components
                }
                loc_info['cluster_label'] = key
                address_info.append(loc_info)
        # print(address_info)
        df_cluster_address = pd.DataFrame(address_info)
        return df_cluster_address
    
    def get_normalized_cluster_mapping(self):
        """
        Reduce clusters using the primary address in cluster

        Parameters
        -----------
        None

        Returns
        -----------
        cluster_map : dict
            dictionary that maps {cluster_label: norm_cluster_label}. Contains an entry for every cluster and maps to
            the normalized version of that cluster
        """
        # Mapping address to most frequent cluster_label
        tdf = self.df_possible_addresses.groupby('address')['cluster_label'].agg(pd.Series.mode).to_frame().reset_index()
        tdf.rename(columns={'cluster_label': 'norm_cluster_label'}, inplace=True)
        tdf = tdf.explode('norm_cluster_label')
        # This df contains the mapping of cluster_label --> most_common_cluster_label
        tdf = pd.merge(self.df_possible_addresses, tdf, on='address', how='left')

        # Convert to dictionary
        cluster_map = pd.Series(tdf['norm_cluster_label'].values, index=tdf['cluster_label']).to_dict()
        cluster_map[-1] = -1
        cluster_map[-3] = -3
        return cluster_map
