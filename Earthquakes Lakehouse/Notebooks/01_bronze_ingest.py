# Databricks notebook source
"""
Bronze Layer USGS Earthquake Data Ingestion
--------------------------------

This notebook ingests raw earthquake data from the USGS GeoJSON API and stores it in a Delta Lake Bronze table without applying any transformations. The Bronze layer serves as the immutable source of truth for downstream processing.

Key responsibilities:
- Requests the live USGS “past hour” GeoJSON feed.
- Stores the full raw JSON payload along with an ingest timestamp.
- Appends data into the `bronze_earthquakes` Delta table.
- Updates the `pipeline_metadata` table with the latest ingest time.
- Displays a preview of the most recent raw record.

Outputs:
- Delta table: `bronze_earthquakes`
- Delta table: `pipeline_metadata`

The Bronze layer enables incremental processing, auditability, and reliable downstream parsing in the Silver and Gold stages.
"""

# COMMAND ----------

# -------------------------------
# CONFIG
# -------------------------------

import requests
import json
from datetime import datetime
from pyspark.sql import Row
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, ArrayType
from pyspark.sql.functions import substring

bronze_table = "bronze_earthquakes"
metadata_table = "pipeline_metadata"

# COMMAND ----------

# -------------------------------
# STEP 0: Ensure metadata table exists
# -------------------------------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {metadata_table} (
    table_name STRING,
    last_ingest_ts TIMESTAMP,
    last_update_ts TIMESTAMP
)
""")

# COMMAND ----------

# -------------------------------
# STEP 1: API Request
# -------------------------------

# USGS GeoJSON feed
url = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_hour.geojson"  # past hour
url_month = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_month.geojson"   # past month

# Fetch data
response = requests.get(url)
response.raise_for_status()  # throws error if request failed

data = response.json()

# Inspect basic stats
print("Number of earthquakes:", len(data["features"]))

# For display
json.dumps(data, indent=2)[:500]  # preview only

# COMMAND ----------

# -------------------------------
# STEP 2: Save as table
# -------------------------------

raw_json = json.dumps(data)
df_bronze = spark.createDataFrame(
    [(raw_json, datetime.utcnow())],
    ["raw_geojson", "ingest_timestamp"]
)

df_bronze.write.format("delta").mode("append").saveAsTable(bronze_table)

# COMMAND ----------

# -------------------------------
# STEP 3: Read it back
# -------------------------------

df_bronze_recent = spark.table(bronze_table)[
    ["ingest_timestamp", substring("raw_geojson", 1, 500).alias("raw_geojson_preview")]
].orderBy(
    F.col("ingest_timestamp").desc()
).limit(1)

display(df_bronze_recent)

# COMMAND ----------

# -------------------------------
# STEP 4: Update Metadata
# -------------------------------

max_ingest_ts = df_bronze.agg(F.max("ingest_timestamp")).first()[0]

spark.sql(f"""
MERGE INTO {metadata_table} AS meta
USING (
    SELECT
        '{bronze_table}' AS table_name,
        TIMESTAMP'{max_ingest_ts}' AS last_ingest_ts,
        current_timestamp() AS last_update_ts
) AS src
ON meta.table_name = src.table_name
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
""")

print(f"Updated metadata table for {bronze_table} with last_ingest_ts={max_ingest_ts}")