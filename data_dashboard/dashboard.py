# Third Party
import streamlit as st
import pandas as pd
import altair as alt
import folium as fol
from streamlit_folium import st_folium
from vega_datasets import data
import geopandas as gpd
from shapely.geometry import Point
import numpy as np


# Native
import datetime

# Custom
from dashboard_utils import * 

st.set_page_config(layout="wide", page_title="Tile Dashboard", page_icon="data_dashboard/images/tile_logo.png")
title = '**Location Tracker Dashboard**'
# st.markdown(f"<h1 style='text-align: center; color: grey;'>{title}</h1>", unsafe_allow_html=True)
t1, t2, t3, t4 = st.columns([.6, .15, 1.25, .6])
t2.image("data_dashboard/images/tile_logo.png", width = 75)
t3.title(title, anchor='right')
# metric columns
mt1, mt2, mt3 = st.columns([1,1.75,7])
period = mt2.number_input(label='Period (Days)', min_value=0, step=1, value=7)
mt3.write(f":grey[.]\n\n---------------------------------------------------------------------- Last {period} Days ----------------------------------------------------------------------")
m1, m2, m3, m4, m5, m6, m7 = st.columns(7)
# graph columns
col1, col2, col3, col4, col5 = st.columns([.01, .5, .5, 1.25, .25])
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
col4.altair_chart(weather_chart)


# Make Map
map1, map2, map3 = st.columns([2,1,2])
map2.subheader('Map', anchor='middle')
mapdata = fetch_data(period)

df = mapdata[['longitude','latitude']].copy().rename(columns={'latitude':'Latitude','longitude':'Longitude'})

@st.cache_data
def make_altair_map(df, rotate_value):
    # background for map
    sphere = alt.Chart(alt.sphere()).mark_geoshape(
        fill="aliceblue", stroke="black", strokeWidth=1.5
    )
    # country projections
    countries = alt.topo_feature(data.world_110m.url, 'countries')
    world = alt.Chart(countries).mark_geoshape(
        fill="lightgray", stroke="black"
    )
    # tile tracker points - the filter only renders the points that are on the sphere facing the user
    points = alt.Chart(df).mark_circle(opacity=0.35, tooltip=True, stroke="black").transform_filter(
        (rotate_value * -1 - 90 < alt.datum.Longitude) & (alt.datum.Longitude < rotate_value * -1 + 90)
    ).encode(
            longitude="Longitude:Q",
            latitude="Latitude:Q",
        )

    map_chart = alt.layer(sphere+world+points).project(
        type="orthographic", rotate=alt.expr(f"[{rotate_value}, 0, 0]"))
    return map_chart

# Native Altair slider was causing rendering issues - streamlit slider has significantly slower performance... but it works
rotate_param = map2.slider("Rotate Longitude", min_value=-180, max_value=180, value=-int(df['Longitude'].mean()), step=90)

map_chart = make_altair_map(df, rotate_param)
st.altair_chart(map_chart)


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