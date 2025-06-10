# Third Party
import streamlit as st
import pandas as pd
import plotly.express as px
from plotly.subplots import make_subplots
from plotly.graph_objects import Scattergeo
import psycopg2
from dotenv import load_dotenv

# Native
import os
import datetime

@st.cache_resource # Cache the connection object to avoid re-establishing on every rerun
def get_db_connection():
    load_dotenv() # take environment variables from .env.
    db_user = os.getenv("POSTGRESQL_USERNAME")
    db_password = os.getenv("POSTGRESQL_PWD")
    db_host = 'localhost' # Or your PostgreSQL server IP/hostname
    db_port = '5432'      # Default PostgreSQL port
    db_name = 'tile_db'
    try:
        conn = psycopg2.connect(
            host=db_host,
            database=db_name,
            user=db_user,
            password=db_password,
            port=db_port
        )
        return conn
    except psycopg2.Error as e:
        st.error(f"Error connecting to the database: {e}")
        st.stop() # Stop the Streamlit app if connection fails

# Function to fetch data
@st.cache_data(ttl=600) # Cache data for 10 minutes (adjust as needed)
def fetch_data(query):
    conn = get_db_connection()
    df = pd.read_sql(query, conn)
    # conn.close() # Close connection after fetching data
    return df

def make_map(plotdf, pointcolor):
    fig = make_subplots(rows=1, cols=2) 
    fig = px.scatter_geo(plotdf, 
                        lat="latitude",
                        lon='longitude',
                        color=pointcolor,
                        scope='world',
                        fitbounds='locations',
                        #  hover_data=['direction_similarity']
                        )
    bounds_add = 10
    fig.add_traces(Scattergeo(lat=[plotdf.latitude.min()-bounds_add, plotdf.latitude.max()+bounds_add],
                                                lon=[plotdf.longitude.min()-bounds_add, plotdf.longitude.max()+bounds_add],
                                                mode = 'markers', marker = dict(size = 2,color = 'rgba(0, 0, 0, 0)')))
    fig.update_geos(resolution=50)
    return fig

st.set_page_config(layout="wide", page_title="Tile Dashboard")
title = 'Tile Tracker'
st.markdown(f"<h1 style='text-align: center; color: black;'>{title}</h1>", unsafe_allow_html=True)

# # Add a refresh button
if st.button("Refresh Data"):
    # This explicitly clears the cache for the fetch_data function
    # It will force fetch_data to rerun the query next time it's called
    fetch_data.clear() # Clears ALL cached results for fetch_data
    # Or, if you use query as a parameter:
    # fetch_data.clear(query) # Clears only for that specific query

    st.success("Data refreshed!")


start = st.date_input('Start Date', datetime.date(2024, 11, 15))
end = st.date_input('End Date', 'today')

filter_map = {'Raw':'date',
              'Clustered':'cluster_label',
              'Normalized':'norm_cluster_label'}
filter_selection = st.radio(label='Select Data Filter',options=['Raw','Clustered','Normalized'], horizontal=True, on_change=fetch_data)
filter_selection = filter_map[filter_selection]
if filter_selection == 'date':
    query = f"""
            SELECT date, latitude, longitude
            FROM tile_data_john
            WHERE date::date BETWEEN '{start}' AND '{end}'
            ;
        """
    plotdf = fetch_data(query)
else:
    query = f"""
            SELECT DISTINCT ON ({filter_selection}) date, latitude, longitude
            FROM tile_data_john
            WHERE date::date BETWEEN '{start}' AND '{end}'
            ;
        """
    plotdf = fetch_data(query)

# query = f"""
#             SELECT date, norm_cluster_label, latitude, longitude
#             FROM tile_data_john
#             WHERE date BETWEEN {start} AND {end}
#             ;
#         """
# plotdf = fetch_data(query)

fig = make_subplots(rows=1, cols=1) 
fig = px.scatter_geo(plotdf, 
                    lat="latitude",
                    lon='longitude',
                    color=filter_selection,
                    scope='world',
                    fitbounds='locations',
                    #  hover_data=['direction_similarity']
                    )
bounds_add = 10
fig.add_traces(Scattergeo(lat=[plotdf.latitude.min()-bounds_add, plotdf.latitude.max()+bounds_add],
                                            lon=[plotdf.longitude.min()-bounds_add, plotdf.longitude.max()+bounds_add],
                                            mode = 'markers', marker = dict(size = 2,color = 'rgba(0, 0, 0, 0)')))
fig.update_geos(resolution=50)
# fig = make_map(plotdf, filter_selection)

st.plotly_chart(fig)