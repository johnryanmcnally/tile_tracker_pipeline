# Third Party
import streamlit as st
import pandas as pd
import numpy as np
import altair as alt
# import folium as fol
# from streamlit_folium import st_folium
# import geopandas as gpd
# from shapely.geometry import Point

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

# Joya Chatbot section
st.markdown('<div style="text-align: center;"><h3/>Retrieval Augmented Generation (RAG) LLM</h3></div>', unsafe_allow_html=True)
chat1, chat2, chat3 = st.columns([.25,.85,.25])
with chat2.container(height=250, border=False):
    messages = st.container()
if prompt := chat2.chat_input("Hi, I'm Joya. Ask me about my trip."):
    messages.chat_message("user").write(prompt)
    messages.chat_message("Joya").write(f"{joya_chat(prompt)}")

# date range columns
st.markdown('<div style="text-align: center;"><h3/>Dashboard</h3></div>', unsafe_allow_html=True)
dr1, dr2, dr3, dr4 = st.columns([.8,.5,.5,1])
start_date = dr2.date_input(label="Start Date", value=datetime.date.today() - datetime.timedelta(days=30), # datetime.date(year=2024, month=11, day=15)
                            min_value=datetime.date(year=2024, month=11, day=15))
end_date = dr3.date_input(label="End Date", value = datetime.date.today(),
                            min_value=datetime.date(year=2024, month=11, day=16))
period = (end_date - start_date).days
# metric columns
mt1, mt2, mt3 = st.columns([1,1.75,7])
# period = mt2.number_input(label='Period (Days)', min_value=0, step=1, value=30)
mt3.write(f"|---------------------------------------------------------------------- {period} Days ----------------------------------------------------------------------|", unsafe_allow_html=True)
m1, m2, m3, m4, m5, m6, m7 = st.columns(7)

# graph columns
col1, col2, col3, col4, col5 = st.columns([.2, .4, .4, 1, .3], gap='medium')
title_font_size = 15

# Retrieve data based on period
tile_total_count, tile_delta_count = tile_data_health(start_date, end_date)
tag_count = google_data_health(start_date, end_date).head(10)
tag_count['prev_value'] = tag_count['tag_count'] - tag_count['delta']
weather = get_weather(start_date, end_date)

# Arrange Data on dashboard
# Tile Data
m2.metric(label='**Total Record Count**', value=tile_total_count, delta=f'{tile_delta_count} in date range', border=True) # , delta_color='inverse'
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
st.markdown('<div style="text-align: center;"><h3/>Interactive Map w/ Histograms</h3></div>', unsafe_allow_html=True)
slider1, slider2, slider3 = st.columns([1,.85,1])
mapdata = fetch_data(start_date, end_date)
df = mapdata[['longitude','latitude']].copy().rename(columns={'latitude':'Latitude','longitude':'Longitude'})

# Native Altair slider was causing rendering issues - streamlit slider has significantly slower performance... but it works
rotate_value = slider2.slider("Rotate Longitude", min_value=-180, max_value=180, value=-int(df['Longitude'].mean()), step=45)

map_chart = make_altair_map(df, rotate_value)
latitude_hist, longitude_hist = make_lat_lon_hist(df, rotate_value)

# columns for formating graphs and metrics
m1, m2, m3, m4, m5 = st.columns([.35,.25,.5,.25,.25], vertical_alignment='top', gap='small')
md1, md2, md3 = st.columns([1,.5,1], vertical_alignment='top')

# Formatting map and graphs
height = 350
width = 350
combine = alt.hconcat(map_chart.properties(height=height, width=width), 
                      latitude_hist.properties(height=height), 
                      padding={'bottom':0,'right':0,'left':0,'top':0}, spacing = 10) # autosize='fit', bounds='flush'
m3.altair_chart(combine, use_container_width=False)
m3.altair_chart(longitude_hist.properties(width=width), use_container_width=False)

try:
    top_country = mapdata[(-1*rotate_value - 90 < mapdata['longitude']) & (mapdata['longitude'] < -1*rotate_value + 90)]['country'].value_counts().index[0]
    m2.metric(label='Most Points in', value=f"{top_country}")
    m2.metric(label=f'Clusters in {top_country}', value=mapdata[mapdata['country']==top_country]['cluster_label'].nunique())
    m2.metric(label=f'Top Label in {top_country}', value=mapdata[mapdata['country']==top_country]['tag'].value_counts().index[0])
except:
    m2.metric(label='Try Rotating the Map', value=f"")
    m2.metric(label=f'No Points Available', value=f"")
    m2.metric(label=f'No Points Available', value=f"")
