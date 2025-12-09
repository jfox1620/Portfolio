# Databricks notebook source
"""
USGS Earthquakes Visualization Notebook

This notebook extracts recent earthquake data from a Databricks Delta table and generates an interactive visualization to explore earthquake activity over time. The workflow includes:

1. Data Extraction and Filtering:
   - Queries the `silver_earthquakes` table for the last 90 days.
   - Filters earthquakes by magnitude (default >= 3.0).
   - Orders data chronologically.

2. Feature Preparation for Mapping:
   - Converts Spark DataFrame to Pandas DataFrame.
   - Constructs GeoJSON features with earthquake location, magnitude, and timestamp.
   - Sets visual styling for map markers (size proportional to magnitude).

3. Interactive Map Creation:
   - Generates a Folium map with `TimestampedGeoJson` to animate earthquake events over time.
   - Configures animation parameters (duration, period, looping, and playback).

4. Plotly Scatter Geo Visualization:
   - Creates an animated Plotly scatter geo plot to visualize earthquake locations daily.
   - Marker size and color represent earthquake magnitude.
   - Exports the interactive visualization as an HTML file.

Usage:
- Run each cell sequentially to query, process, and visualize earthquake data.
- Adjust magnitude threshold or date range as needed for different analyses.
"""

# COMMAND ----------

import pandas as pd
import folium
from folium.plugins import TimestampedGeoJson
import IPython.display as ipd
import geopandas as gpd
from datetime import datetime
import plotly.express as px

# COMMAND ----------

# -----------------------------
# 1. Read and process data
# -----------------------------

df = spark.sql("""
SELECT latitude, longitude, mag as magnitude, time
FROM silver_earthquakes
WHERE time >= date_sub(current_date(), 90)
ORDER BY time ASC
""")

df = df.orderBy('time')

# Optional: filter for earthquakes above a certain magnitude
df = df[df['magnitude'] >= 3.0]

display(df)

# COMMAND ----------

# -----------------------
# 2. Prepare features for Folium TimestampedGeoJson
# -----------------------

# Convert Spark DataFrame to Pandas DataFrame
pdf = df.toPandas()

features = [
    {
        'type': 'Feature',
        'geometry': {
            'type': 'Point',
            'coordinates': [row['longitude'], row['latitude']]
        },
        'properties': {
            'time': row['time'].isoformat(),
            'style': {
                'radius': row['magnitude'] ** 2,
                'color': 'black',
                'fillColor': 'red',
                'fillOpacity': 0.6
            },
            'icon': 'circle'
        }
    }
    for _, row in pdf.iterrows()
]

# COMMAND ----------

# -----------------------
# 3. Create the map
# -----------------------

m = folium.Map(location=[0,0], zoom_start=2)

TimestampedGeoJson(
    {
        'type': 'FeatureCollection',
        'features': features,
    },
    period='P1D',         # each frame = 1 day
    add_last_point=True,   # keeps points for last frame
    duration='PT3D',       # points last 3 days
    auto_play=True,
    loop=True,
    max_speed=10,
    loop_button=True,
    date_options='YYYY-MM-DD',
).add_to(m)

# COMMAND ----------

# -----------------------
# 4. Save and display the animation
# -----------------------

# Ensure 'time' is datetime
pdf['day'] = pdf['time'].dt.date

fig = px.scatter_geo(
    pdf,
    lon='longitude',
    lat='latitude',
    size='magnitude',
    color='magnitude',
    animation_frame='day',
    projection='natural earth',
    title='Earthquake Locations Over Time',
    width=1200,
    height=700
)

# Improve date label readability
fig.update_xaxes(
    tickangle=45,
    nticks=10,
    tickformat="%Y-%m-%d"
)

# Save figure as HTML
fig.write_html(
    "earthquake_map.html",
    include_plotlyjs="cdn"
)

# Display animation
with open("earthquake_map.html", "r") as f:
    html_content = f.read()

displayHTML(html_content)