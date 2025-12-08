# Databricks notebook source
"""
Silver Layer USGS Earthquake Data Transformation
--------------------------------------

This notebook processes raw Bronze-layer GeoJSON into clean, structured, analytics-ready Silver data. It incrementally loads new Bronze records using the metadata table, explodes the GeoJSON features array, and applies normalization to produce a flattened earthquake event schema.

Key responsibilities:
- Detect and load only new Bronze ingestions (incremental processing)
- Extract and explode GeoJSON "features" into individual earthquake events
- Flatten nested geometry and properties fields
- Convert epoch millisecond timestamps to proper Spark timestamps
- Upsert cleaned records into the Silver Delta table (MERGE INTO)
- Update pipeline metadata for downstream incremental loads
- Perform optional VACUUM retention on Silver data

Outputs:
- Delta table: `silver_earthquakes`
- Delta table: `pipeline_metadata`

The Silver layer produces a normalized, deduplicated event-level dataset that serves as the reliable source for all downstream Gold aggregations.
"""

# COMMAND ----------

# -------------------------------
# CONFIG
# -------------------------------
 
from pyspark.sql import functions as F
from pyspark.sql import Row
from pyspark.sql.types import MapType, StringType, ArrayType
from delta.tables import DeltaTable
from pyspark.sql import Window
import json
import pandas as pd

bronze_table = "bronze_earthquakes"
silver_table = "silver_earthquakes"
metadata_table = "pipeline_metadata"

# COMMAND ----------

# -------------------------------
# STEP 1: Get last processed ingest_timestamp
# -------------------------------

last_processed = spark.sql(f"""
SELECT last_ingest_ts
FROM {metadata_table}
WHERE table_name = '{silver_table}'
ORDER BY last_ingest_ts DESC
LIMIT 1
""").collect()

if last_processed:
    last_ingest_ts = last_processed[0][0]
else:
    last_ingest_ts = None  # first run

# COMMAND ----------

# -------------------------------
# STEP 2: Read new Bronze records
# -------------------------------

df_bronze = spark.table(bronze_table)

if last_ingest_ts:
    df_new = df_bronze.filter(F.col("ingest_timestamp") > F.lit(last_ingest_ts))
else:
    df_new = df_bronze  # first run, take all

new_count = df_new.count()

if new_count == 0:
    print("No new Bronze records to process.")
else:
    print(f"Processing {new_count} new Bronze record(s)...")

# COMMAND ----------

# -------------------------------
# STEP 3: Convert raw_geojson → array of features
# -------------------------------

def extract_features(raw_json_str):
    """
    Extract the list of earthquake feature objects from a raw USGS JSON payload.

    This function receives a raw JSON string from the Bronze table, parses it, retrieves the "features" array, and returns each feature as its own JSON string. If the payload cannot be parsed, an empty list is returned.

    Args:
        raw_json_str (str): Raw JSON string containing the USGS GeoJSON response.

    Returns:
        list[str]: A list of JSON-formatted strings, each representing a single earthquake feature. Returns an empty list if parsing fails.
    """

    try:
        data = json.loads(raw_json_str)
        features = data.get("features", [])
        # Convert each feature dict to a valid JSON string
        return [json.dumps(f) for f in features]
    except:
        return []

extract_features_udf = F.udf(
    extract_features,
    ArrayType(StringType())  # now each string is valid JSON
)

df_features = (
    df_new
    .withColumn("feature_json", extract_features_udf("raw_geojson"))
    .select("ingest_timestamp", F.explode("feature_json").alias("feature_json"))
)

# COMMAND ----------

# -------------------------------
# STEP 4: Flatten
# -------------------------------

def flatten_feature(batch_iter):
    """
    Convert a single USGS GeoJSON feature into a flat dictionary suitable for Spark ingestion.

    This function extracts the nested 'properties' and 'geometry' fields, renames certain keys, and normalizes the structure into a flattened dictionary of simple key/value pairs.

    Args:
        feature (dict): Raw USGS feature object from the GeoJSON feed.

    Returns:
        dict: A flattened earthquake record with keys such as:
              id, time, mag, place, depth, longitude, latitude, etc.
    """

    for pdf in batch_iter:
        rows = []

        for _, row in pdf.iterrows():
            ingest_ts = row["ingest_timestamp"]
            f_raw = row["feature_json"]

            # Parse feature JSON into dict
            try:
                f = json.loads(f_raw)
            except Exception as e:
                print("Failed to parse JSON:", e)
                continue

            # --- Geometry ---
            geometry = f.get("geometry", {})
            coords = geometry.get("coordinates", [None, None, None])

            # --- Properties ---
            props = f.get("properties", {})

            rows.append({
                "id": f.get("id"),
                "ingest_timestamp": ingest_ts,
                "longitude": float(coords[0]) if coords[0] is not None else None,
                "latitude": float(coords[1]) if coords[1] is not None else None,
                "depth": float(coords[2]) if coords[2] is not None else None,
                "mag": props.get("mag"),
                "place": props.get("place"),
                "time": props.get("time"),
                "updated": props.get("updated"),
                "tz": props.get("tz"),
                "url": props.get("url"),
                "status": props.get("status"),
                "tsunami": props.get("tsunami"),
                "sig": props.get("sig"),
                "magType": props.get("magType"),
                "event_type": f.get("type")
            })

        yield pd.DataFrame(rows)

# Define output schema
silver_schema = """
    id STRING,
    ingest_timestamp TIMESTAMP,
    longitude DOUBLE,
    latitude DOUBLE,
    depth DOUBLE,
    mag DOUBLE,
    place STRING,
    time BIGINT,
    updated BIGINT,
    tz INT,
    url STRING,
    status STRING,
    tsunami INT,
    sig INT,
    magType STRING,
    event_type STRING
"""

df_flat = df_features.mapInPandas(flatten_feature, schema=silver_schema)

# COMMAND ----------

# -------------------------------
# STEP 5: Additional Transformations
# -------------------------------

# Convert 'time' and 'updated' to datetime values
df_flat = (
    df_flat
    .withColumn("time", F.from_unixtime(F.col("time") / 1000).cast("timestamp"))
    .withColumn("updated", F.from_unixtime(F.col("updated") / 1000).cast("timestamp"))
)

window = Window.partitionBy("id").orderBy(F.col("updated").desc())

# There shouldn't be any duplicates, but remove any just in case, keeping the most recently updated record.
df_flat = (
    df_flat
    .withColumn("row_num", F.row_number().over(window))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

# COMMAND ----------

# -------------------------------
# STEP 6: Write to Silver table
# -------------------------------

# Ensure silver table exists
if spark.catalog.tableExists(silver_table):
    # Perform upsert (merge)
    delta_silver = DeltaTable.forName(spark, silver_table)

    (
        delta_silver.alias("silver")
        .merge(
            df_flat.alias("updates"),
            "silver.id = updates.id"
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
else:
    # Create table
    df_flat.write.format("delta").saveAsTable(silver_table)

print(f"Upserted {df_flat.count()} Silver records.")


# COMMAND ----------

# -------------------------------
# STEP 7: Read it back
# -------------------------------

df_silver_recent = spark.table(silver_table).orderBy(
    F.col("ingest_timestamp").desc()
).limit(10)

display(df_silver_recent)

# COMMAND ----------

# -------------------------------
# STEP 8: Update metadata
# -------------------------------

max_ingest_ts = df_new.agg(F.max("ingest_timestamp")).first()[0]

# Get current version of the silver Delta table
delta_silver = DeltaTable.forName(spark, silver_table)
current_version = (
    delta_silver.history(1)
    .select("version")
    .first()[0]
)

spark.sql(f"""
MERGE INTO {metadata_table} AS meta
USING (
    SELECT
        '{silver_table}' AS table_name,
        {current_version} AS last_version,
        TIMESTAMP'{max_ingest_ts}' AS last_ingest_ts,
        current_timestamp() AS last_update_ts
) AS src
ON meta.table_name = src.table_name
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
""")

print(f"Updated metadata table for {silver_table} with last_ingest_ts={max_ingest_ts}")

# COMMAND ----------

# Vacuum each gold table, retaining 7 days
spark.sql(f"VACUUM {silver_table} RETAIN 168 HOURS")