# Third Party
import streamlit as st
import pandas as pd
import altair as alt


# Native
import datetime

# Custom
from dashboard_utils import * 

st.set_page_config(layout="wide", page_title="Tile Dashboard", page_icon="images/tile_logo.png")
title = '**Location Tracker Dashboard**'
# st.markdown(f"<h1 style='text-align: center; color: grey;'>{title}</h1>", unsafe_allow_html=True)
t1, t2, t3, t4 = st.columns([.6, .15, 1.25, .6])
t2.image("images/tile_logo.png", width = 75)
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

# Google Data
bar = alt.Chart(tag_count).mark_bar().encode(
    y = alt.Y('tag:N', sort='-x', axis=alt.Axis(title=None)),
    x = alt.X('tag_count', axis=alt.Axis(title='Counts')),
    tooltip=['tag','tag_count','delta'],
    color = alt.value('grey')
)
delta_bar = alt.Chart(tag_count).mark_rect(height=15).encode(
    y = alt.Y('tag:N', sort='-x'),
    x = alt.X('prev_value'),
    x2 = 'tag_count',
    color=alt.value('green')
)
chart = (bar + delta_bar).properties(
    title='Total Tag Counts vs. Delta'
).configure_title(
    fontSize=title_font_size,
    # font='serif',
    color='darkgray',
    anchor='middle',
    dy=20
)
col2.altair_chart(chart)

delta_chart = alt.Chart(tag_count).mark_bar().encode(
    y = alt.Y('tag:N', sort='-x', axis=alt.Axis(title=None)),
    x = alt.X('delta', axis=alt.Axis(title='Counts')),
    tooltip=['tag','delta'],
    color = alt.value('green')    
).properties(
    title=f'Tag Deltas (Last {period} Days)'
).configure_title(
    fontSize=title_font_size,
    # font='serif',
    color='darkgray',
    anchor='middle',
    dy=20
)
col3.altair_chart(delta_chart)


# Weather Data
weather = weather.melt('date')
# col4.write(weather)
temperature = alt.Chart(weather[weather['variable']!='precipitation_mm']).mark_line().encode(
    x=alt.X('date:O', axis=alt.Axis(title='Date')),
    y = alt.Y('value', axis=alt.Axis(title='Temperature (F), RH (%)')),
    color=alt.Color('variable', scale=alt.Scale(domain=['temperature_f', 'rh', 'precipitation'],
                                                range=['orange', 'lightblue', 'grey'])).legend(orient='top', title=None),
    tooltip = []
)

precipitation = alt.Chart(weather[weather['variable']=='precipitation_mm']).mark_bar().encode(
    x=alt.X('date:O'),
    y = alt.Y('value', axis=alt.Axis(title='Precipitation (mm)')),
    color= alt.value('grey'),
    tooltip=[]
)

weather_chart = (precipitation + temperature).resolve_scale(y='independent').properties(
    title=f'Weather (Last {period} Days)'
).configure_title(
    fontSize=title_font_size,
    # font='serif',
    color='darkgray',
    anchor='middle',
    dy=20
)
col4.altair_chart(weather_chart)

