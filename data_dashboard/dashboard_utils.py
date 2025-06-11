# Third Party
import pandas as pd
import plotly.graph_objects as go
import psycopg2
from sqlalchemy import create_engine
from dotenv import load_dotenv
import streamlit as st

# Native
import os

@st.cache_resource # Cache the connection object to avoid re-establishing on every rerun
def get_db_connection():
    load_dotenv() # take environment variables from .env.
    db_user = os.getenv("POSTGRESQL_USERNAME")
    db_password = os.getenv("POSTGRESQL_PWD")
    db_host = 'localhost' # Or your PostgreSQL server IP/hostname
    db_port = '5432'      # Default PostgreSQL port
    db_name = 'tile_db'
    try:
        conn = create_engine(f'postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}')
        return conn
    except psycopg2.Error as e:
        st.error(f"Error connecting to the database: {e}")
        st.stop() # Stop the Streamlit app if connection fails

# Function to fetch data
@st.cache_data(ttl=600) # Cache data for 10 minutes (adjust as needed)
def fetch_data(start, end):
    rawquery = f"""
                SELECT date, latitude, longitude
                FROM tile_data_john
                WHERE date::date BETWEEN '{start}' AND '{end}'
                ;
            """
    clusterquery = f"""
                SELECT DISTINCT ON (cluster_label) date, latitude, longitude
                FROM tile_data_john
                WHERE date::date BETWEEN '{start}' AND '{end}'
                ;
            """
    normalizedquery = f"""
                SELECT DISTINCT ON (norm_cluster_label) date, latitude, longitude
                FROM tile_data_john
                WHERE date::date BETWEEN '{start}' AND '{end}'
                ;
            """
    engine = get_db_connection()
    raw = pd.read_sql(rawquery, engine)
    cluster = pd.read_sql(clusterquery, engine)
    norm = pd.read_sql(normalizedquery, engine)
    engine.dispose()

    return raw, cluster, norm

@st.cache_data(ttl=600)
def make_map(plotdf, filter_selection):
    load_dotenv() # take environment variables from .env.
    MAPBOX_TOKEN = os.getenv("MAPBOX_TOKEN")
    color = {'Raw':'date','Clustered':'cluster_label','Normalized':'norm_cluster_label'}
    color = color[filter_selection]
    fig = go.Figure(go.Scattermapbox(
        lat=plotdf['latitude'],
        lon=plotdf['longitude'],
        mode='markers',
    # --- Marker configuration is done here ---
        marker=go.scattermapbox.Marker(
            color='red',
            # colorscale='Plasma', # Choose a colorscale (e.g., 'Viridis', 'Plasma', 'Jet', 'Portland')
            # cmin=plotdf[color].min(), # Min value for colorscale
            # cmax=plotdf[color].max(), # Max value for colorscale
            # showscale=True, # Show the color bar legend
            # size=plotdf[color] / 1e6, # Size based on population (adjust divisor for appropriate visual size)
            # sizemode='area', # 'diameter' or 'area' (area scales by the square of the size value)
            # sizeref=2. * max(plotdf[color]) / (60.**2), # Adjust this to control overall marker size scaling, default is usually fine
            # opacity=0.7, # Transparency of markers
        )
        # text=df['city'] + '<br>Population: ' + df['population'].astype(str) + 'M',
        # hoverinfo='text'
    ))

    fig.update_layout(
        autosize=True,
        # width=800,  # pixels
        # height=600, # pixels
        hovermode='closest',
        mapbox=dict(
            accesstoken=MAPBOX_TOKEN,
            bearing=0,
            center=dict(lat=plotdf['latitude'].mean(),lon=plotdf['longitude'].mean()),
            pitch=0,
            zoom=1,
            style="streets" # Or 'mapbox://styles/your_username/your_style_id'
        ),
        margin=dict(
        l=0, # left margin
        r=0, # right margin
        b=0, # bottom margin
        t=0, # top margin
        pad=0 # padding around plot (usually zero for tight fit)
    )

    )

    return fig