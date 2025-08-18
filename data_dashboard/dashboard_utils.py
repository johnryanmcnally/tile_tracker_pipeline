# Third Party
import pandas as pd
import plotly.graph_objects as go
import psycopg2
from sqlalchemy import create_engine
from dotenv import load_dotenv
import streamlit as st

# Native
import os
import datetime

@st.cache_resource # Cache the connection object to avoid re-establishing on every rerun
def get_db_connection():
    load_dotenv() # take environment variables from .env.
    db_user = os.getenv("POSTGRESQL_USERNAME")
    db_password = os.getenv("POSTGRESQL_PWD")
    db_host = 'localhost'
    db_port = '5432'
    db_name = 'tile_db'
    try:
        conn = create_engine(f'postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}')
        return conn
    except psycopg2.Error as e:
        st.error(f"Error connecting to the database: {e}")
        st.stop() # Stop the Streamlit app if connection fails

# Function to fetch data health
def tile_data_health(period):
    end = datetime.date.today()
    start = end - datetime.timedelta(days=period)
    tile_total_count_query = f"""
                SELECT COUNT(*)
                FROM tile_data_john;
                """
    tile_delta_count_query = f"""
                SELECT COUNT(*)
                FROM tile_data_john
                WHERE date::date BETWEEN '{start}' AND '{end}';
                """

    engine = get_db_connection()
    # raw = pd.read_sql(rawquery, con=engine)
    tile_total_count = pd.read_sql(tile_total_count_query, con=engine).values[0,0]
    tile_delta_count = pd.read_sql(tile_delta_count_query, con=engine).values[0,0]
    
    return tile_total_count, tile_delta_count

def google_data_health(period):
    end = datetime.date.today()
    start = end - datetime.timedelta(days=period)
    tag_count_query = f"""
WITH date_range_counts AS (
    SELECT
        t.tag,
        COUNT(t.tag) AS date_range_count
    FROM tags AS t
    JOIN tile_data_john AS tdj
        ON t.cluster_label = tdj.cluster_label
    WHERE
        tdj.date BETWEEN '{start}' AND '{end}'
    GROUP BY t.tag
),
all_time_counts AS (
    SELECT
        t.tag,
        COUNT(t.tag) AS all_time_count
    FROM tags AS t
    JOIN tile_data_john AS tdj -- This JOIN is the key correction.
        ON t.cluster_label = tdj.cluster_label
    GROUP BY t.tag
)
SELECT
    atc.tag,
    atc.all_time_count as tag_count,
    COALESCE(drc.date_range_count, 0) as delta
FROM all_time_counts AS atc
LEFT JOIN date_range_counts AS drc
    ON atc.tag = drc.tag
WHERE
    atc.tag NOT IN (
        'street_address', 'plus_code', 'route', 'establishment', 'premise', 'subpremise',
        'sublocality', 'sublocality_level_4', 'outlier', 'point_of_interest', 'transit'
    )
ORDER BY tag_count DESC
;
"""

    engine = get_db_connection()
    # raw = pd.read_sql(rawquery, con=engine)
    tag_count = pd.read_sql(tag_count_query, con=engine)    
    return tag_count

def get_weather(period):
    end = datetime.date.today()
    start = end - datetime.timedelta(days=period)
    weather_query = f"""
SELECT
	date,
	AVG(temperature_2m * 5/9 + 32) as Temperature_F,
	AVG(relative_humidity_2m) as RH,
	AVG(precipitation) as precipitation_mm
FROM weather
WHERE date::date BETWEEN '{start}' AND '{end}'
GROUP BY date
ORDER BY date DESC
                """

    engine = get_db_connection()
    # raw = pd.read_sql(rawquery, con=engine)
    weather = pd.read_sql(weather_query, con=engine)
    
    return weather

# Function to fetch data
@st.cache_data(ttl=600) # Cache data for 10 minutes
def fetch_data(start, end):
    rawquery = f"""
                SELECT date, latitude, longitude
                FROM tile_data_john
                WHERE date::date BETWEEN '{start}' AND '{end}'
                ;
            """
    clusterquery = f"""
                SELECT DISTINCT ON (t.cluster_label)
                    tdj.date,
                    tdj.time,
                    tdj.latitude,
                    tdj.longitude,
                    w.elevation_meters_asl AS elevation,
                    w.temperature_2m AS temperature,
                    w.relative_humidity_2m AS relative_humidity,
                    w.cloud_cover,
                    w.precipitation,
                    tdj.norm_cluster_label,
                    t.tag,
                    a.address
                FROM tags AS t 
                INNER JOIN tile_data_john AS tdj ON t.cluster_label = tdj.cluster_label
                INNER JOIN addresses AS a ON t.cluster_label = a.cluster_label
                INNER JOIN weather AS w ON t.index = w.index
                WHERE 
                    t.tag NOT IN ('street_address','plus_code','route','premise','subpremise','establishment','point_of_interest')
                    AND
                    tdj.date::date BETWEEN '{start}' AND '{end}'
                ;
            """
    normalizedquery = f"""
                SELECT
                    tdj.date,
                    tdj.latitude,
                    tdj.longitude,
                    w.elevation_meters_asl AS elevation,
                    w.temperature_2m AS temperature,
                    w.relative_humidity_2m AS relative_humidity,
                    w.cloud_cover,
                    w.precipitation,
                    tdj.norm_cluster_label,
                    t.tag,
                    a.address	
                FROM tags AS t 
                INNER JOIN tile_data_john AS tdj ON t.norm_cluster_label = tdj.norm_cluster_label
                INNER JOIN addresses AS a ON t.norm_cluster_label = a.norm_cluster_label
                INNER JOIN weather AS w ON t.index = w.index
                WHERE 
                    t.tag NOT IN ('street_address','plus_code','route','premise','subpremise','establishment','point_of_interest')
                    AND
                    tdj.date::date BETWEEN '{start}' AND '{end}'
                ;
            """
                #     SELECT DISTINCT ON (norm_cluster_label) norm_cluster_label, date, latitude, longitude
                # FROM tile_data_john
                # WHERE date::date BETWEEN '{start}' AND '{end}'
    engine = get_db_connection()
    # raw = pd.read_sql(rawquery, con=engine)
    cluster = pd.read_sql(clusterquery, con=engine)
    # norm = pd.read_sql(normalizedquery, con=engine)
    raw, norm = pd.DataFrame(), pd.DataFrame()

    return raw, cluster, norm

# @st.cache_data(ttl=600)
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
    if filter_selection != 'Raw':
        fig.update_traces(
            # Find the Scattermapbox trace (usually the first one if you only have one)
            # You might need to specify selector=dict(type='scattermapbox') if you have other trace types
            selector=dict(type='scattermapbox'),
            text=
            '<br>Tag: ' + plotdf['tag'] + \
            '<br>Latitude: ' + plotdf['latitude'].round(3).astype(str) + \
            '<br>Longitude: ' + plotdf['longitude'].round(3).astype(str) + \
            '<br>Time: ' + plotdf['time'] +\
            '<br>Temperature: ' + plotdf['temperature'].round(1).astype(str) + 'C' +\
            '<br>Rel Hum: ' + plotdf['relative_humidity'].round(2).astype(str),
            hoverinfo='text', # Ensure hoverinfo is set to 'text' to use your custom text
        )
    else:
        fig.update_traces(
            # Find the Scattermapbox trace (usually the first one if you only have one)
            # You might need to specify selector=dict(type='scattermapbox') if you have other trace types
            selector=dict(type='scattermapbox'),
            text='<br>Latitude: ' + plotdf['latitude'].round(3).astype(str) + \
                '<br>Longitude: ' + plotdf['longitude'].round(3).astype(str),
            hoverinfo='text', # Ensure hoverinfo is set to 'text' to use your custom text
        )        

    return fig