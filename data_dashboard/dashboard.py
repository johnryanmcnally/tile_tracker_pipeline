# Third Party
import streamlit as st
import pandas as pd
import altair as alt
# import folium as fol
# from streamlit_folium import st_folium
# import geopandas as gpd
# from shapely.geometry import Point
import numpy as np


# Native
import datetime

# Custom
from dashboard_utils import * 

st.set_page_config(layout="wide", page_title="Tile Dashboard", page_icon="data_dashboard/images/tile_logo.png")
title = '**Location Tracker Dashboard**'

# title columns
t1, t2, t3, t4 = st.columns([.6, .15, 1.25, .6])
t2.image("data_dashboard/images/tile_logo.png", width = 75)
t3.title(title, anchor='right')

# metric columns
mt1, mt2, mt3 = st.columns([1,1.75,7])
period = mt2.number_input(label='Period (Days)', min_value=0, step=1, value=30)
mt3.write(f"<br><br>---------------------------------------------------------------------- Last {period} Days ----------------------------------------------------------------------", unsafe_allow_html=True)
m1, m2, m3, m4, m5, m6, m7 = st.columns(7)

# graph columns
col1, col2, col3, col4, col5 = st.columns([.01, .5, .5, 1.25, .25], gap='medium')
title_font_size = 15

# Retrieve data based on period
tile_total_count, tile_delta_count = tile_data_health(period)
tag_count = google_data_health(period).head(10)
tag_count['prev_value'] = tag_count['tag_count'] - tag_count['delta']
weather = get_weather(period)

# Arrange Data on dashboard
# Tile Data
m2.metric(label='**Total Record Count**', value=tile_total_count, delta=f'{tile_delta_count} in last {period} days', border=True) # , delta_color='inverse'
# col2.write(f"----------- Last {period} Days -----------")
m3.metric(label=f'**Most Visited Tag**', value=tag_count['tag'].values[0], border=True)
m4.metric(label='**Average Temperature**', value = f"{weather['temperature_f'].mean():.1f} F", border=True)
m5.metric(label='**Average RH**', value = f"{weather['rh'].mean():.1f}%", border=True)
m6.metric(label='**Average Precipitation**', value = f"{weather['precipitation_mm'].mean():.2f} mm", border=True)

# Make Graphs
weather = weather.melt('date')
tag_chart, delta_tag_chart, weather_chart = make_dashboard_graphs(period, tag_count, weather)
# Google Data
col2.altair_chart(tag_chart)
col3.altair_chart(delta_tag_chart)
# Weather Data
col4.altair_chart(weather_chart.properties(height=300, width=600, padding={'bottom':45,'right':10,'left':10,'top':0}), use_container_width=False)


# Make Map
slider1, slider2, slider3 = st.columns([1,.85,1])
slider2.subheader('Interactive Map w/ Histograms', anchor='middle')
mapdata = fetch_data(period)
df = mapdata[['longitude','latitude']].copy().rename(columns={'latitude':'Latitude','longitude':'Longitude'})

# Native Altair slider was causing rendering issues - streamlit slider has significantly slower performance... but it works
rotate_value = slider2.slider("Rotate Longitude", min_value=-180, max_value=180, value=-int(df['Longitude'].mean()), step=45)

map_chart = make_altair_map(df, rotate_value)
latitude_hist, longitude_hist = make_lat_lon_hist(df, rotate_value)

# columns for formating graphs and metrics
m1, m2, m3, m4, m5 = st.columns([.35,.25,.5,.25,.25], vertical_alignment='top', gap=None)
md1, md2, md3 = st.columns([1,.5,1], vertical_alignment='top')

height = 350
width = 350

# Formatting map and graphs
combine = alt.hconcat(map_chart.properties(height=height, width=width), 
                      latitude_hist.properties(height=height), 
                      padding={'bottom':0,'right':0,'left':0,'top':0}, spacing = 10) # autosize='fit', bounds='flush'
m3.altair_chart(combine, use_container_width=False)
m3.altair_chart(longitude_hist.properties(width=width), use_container_width=False)

top_country = mapdata[(-1*rotate_value - 90 < mapdata['longitude']) & (mapdata['longitude'] < -1*rotate_value + 90)]['country'].value_counts().index[0]
m2.metric(label='Most Points in', value=f"{top_country}")
m2.metric(label=f'Clusters in {top_country}', value=mapdata[mapdata['country']==top_country]['cluster_label'].nunique())
m2.metric(label=f'Top Label in {top_country}', value=mapdata[mapdata['country']==top_country]['tag'].value_counts().index[0])


# Joya Chatbot section
# ** Just load Vertex AI App into Streamlit **
gradio_interface_url = "https://genai-app-travelstorygeneration-1-1756046827551-965790274455.us-central1.run.app/?key=kkosf48reph1b6f4"

chat1, chat2, chat3 = st.columns([.35,1,.25])
# Load the Gradio interface using an iframe
chat2.write(f'<iframe src="{gradio_interface_url}" width="800" height="600"></iframe>',
         unsafe_allow_html=True) 





# Attempt at folium map
# m = fol.Map([mean_lat, mean_lon], zoom_start=5)

# for i, data in mapdata.iterrows():
#     tooltip = fol.features.Tooltip(
#     f"Cluster: {data['cluster_label']}",
#     style="font-size: 12px;" # background-color: lightblue;  border: 1px solid blue;
#     )
#     popup = fol.features.Popup(
#         f"""
#         Cluster: {data['cluster_label']}<br>
#         Lat: {data['latitude']:.5f}<br>
#         Lon: {data['longitude']:.5f}
#         """,
#         style="font-size: 12px;"
#     )
#     fol.CircleMarker(
#         location=[data['latitude'], data['longitude']],
#         fill = True,
#         radius = 3,
#         tooltip= tooltip,
#         popup= popup
#     ).add_to(m)
# map1, map2, map3 = st.columns([.2,1,.1])
# with map2:
#     st_folium(m, use_container_width=True) 