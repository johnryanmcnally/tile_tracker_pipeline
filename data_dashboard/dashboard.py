# Third Party
import streamlit as st
import pandas as pd


# Native
import datetime

# Custom
from dashboard_utils import *

st.set_page_config(layout="wide", page_title="Tile Dashboard")
# title = 'Tile Tracker'
# st.markdown(f"<h1 style='text-align: center; color: white;'>{title}</h1>", unsafe_allow_html=True)
col1, col2 = st.columns([1, 1])

# Add a refresh button
with col1:
    col11, col12, col13 = st.columns([1, 1, 1])
    with col11:
        start = st.date_input('Start Date', datetime.date(2024, 11, 15), min_value=datetime.date(2024, 11, 15))
    with col12:
        end = st.date_input('End Date', 'today')
    with col13:    
        filter_selection = st.radio(label='Select Data Filter', key='data_filter',
                                options=['Raw','Clustered','Normalized'],
                                horizontal=False)
    # if st.button("Refresh Data for New Dates"):
    #     fetch_data.clear() # Clears ALL cached results for fetch_data
    #     st.success("Data refreshed!")

    
    
    # Get Data
    raw, clustered, normed = fetch_data(start, end)
    data_map = {'Raw':raw, 'Clustered':clustered, 'Normalized':normed}
    
    config = {
        'scrollZoom': True,  # Enables scroll-to-zoom
        'displayModeBar': False # Optional: Shows the modebar with zoom/pan buttons
    }
    st.plotly_chart(make_map(data_map[filter_selection], filter_selection), config=config, use_container_width=True)
    st.text('here to push the map up \n\n')
    

with col2:
    st.text(f"{raw['date'].nunique()} Days")
    if filter_selection == 'Clustered':
        st.text(f"{data_map[filter_selection]['cluster_label'].nunique()} Clusters")
    elif filter_selection == 'Normalized':
        st.text(f"{data_map[filter_selection]['norm_cluster_label'].nunique()} Clusters")
    else:
        st.text("hey")



