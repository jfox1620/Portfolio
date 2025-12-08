# Databricks notebook source
"""
Gold Layer USGS Earthquake Data Analytics
------------------------------------

This notebook produces business-ready Gold datasets from the cleaned Silver earthquake events. It applies a 7-day lookback to capture late-arriving data, runs table-specific transformations, and writes results using either partition-overwrite (aggregated tables) or MERGE-based upserts (event-level tables). The Gold layer serves as the final analytical output for reporting, dashboards, and downstream data products.

Key responsibilities:
- Incrementally load Silver data using a 7-day lookback window
- Apply table-specific transformation functions
- Write Gold tables via either partition overwrite or MERGE INTO on key columns
- Apply Z-Order optimization for event-level performance
- Update metadata to track the last processed ingest_timestamp
- Perform optional VACUUM retention on Gold data

Outputs:
- Delta table: `gold_earthquakes_daily_summary`
   - Daily aggregates: counts, magnitude stats, depth stats, significance stats  
   - Computes the most active region per day using a window function  
- Delta table: `gold_earthquakes_big`
   - Event-level table of magnitude ≥ 5 earthquakes  
   - Parses the “place” field into region, distance_from_region, and direction_from_region

The Gold layer contains clean, high-value datasets purpose-built for analytics and real-time insights.
"""

# COMMAND ----------

# -------------------------------
# CONFIG
# -------------------------------

from pyspark.sql import functions as F
from pyspark.sql import Row
from pyspark.sql.types import MapType, StringType, ArrayType
from pyspark.sql.functions import current_date, date_sub, col, to_date, from_unixtime, current_timestamp
from pyspark.sql.window import Window
from delta.tables import DeltaTable
import json
import pandas as pd

silver_table = "silver_earthquakes"
gold_table_1 = "gold_earthquakes_daily_summary"
gold_table_2 = "gold_earthquakes_big"
metadata_table = "pipeline_metadata"

# COMMAND ----------

# -------------------------------
# STEP 1: Get last processed ingest_timestamp for each gold table
# -------------------------------

def get_last_ingest_ts(table_name, metadata_table):
    rows = spark.sql(f"""
        SELECT last_ingest_ts
        FROM {metadata_table}
        WHERE table_name = '{table_name}'
        ORDER BY last_ingest_ts DESC
        LIMIT 1
    """).collect()
    
    return rows[0][0] if rows else None


# Gold table 1
last_ingest_ts_1 = get_last_ingest_ts(gold_table_1, metadata_table)

# Gold table 2
last_ingest_ts_2 = get_last_ingest_ts(gold_table_2, metadata_table)

# COMMAND ----------

# -------------------------------
# STEP 2: Pull Silver records for Gold (with lookback)
# -------------------------------

# How many days back to include for possible late data
LOOKBACK_DAYS = 7

# Calculate start date for lookback window
today = current_date()
start_date = date_sub(today, LOOKBACK_DAYS)

# Dictionary of gold tables → DataFrames of rows to process
gold_tables = [gold_table_1, gold_table_2]
new_gold_data = {}

for gold_table in gold_tables:
    # Convert 'time' (epoch in ms) to timestamp, then to date
    df_silver = (
        spark.table(silver_table)
        .withColumn("event_date", to_date(col("time")))
        .filter((col("event_date") >= start_date) & (col("event_date") < today))
    )
    
    count_rows = df_silver.count()
    new_gold_data[gold_table] = df_silver

    if count_rows == 0:
        print(f"No Silver records to process for {gold_table} in the last {LOOKBACK_DAYS} days.")
    else:
        print(f"Processing {count_rows} Silver rows for {gold_table} (last {LOOKBACK_DAYS} days)...")

# COMMAND ----------

# -------------------------------
# STEP 3: Apply Gold Transformations
# -------------------------------

# Gold transformation functions
def transform_gold_table_1(df):
    """
    Transform Silver earthquake data into a daily summary Gold table.
    
    This function aggregates earthquake events by day (event_date) to produce a business-ready summary table for analytics and reporting.

    Aggregations performed per day:
    - daily_count: total number of earthquakes
    - avg_mag: average magnitude
    - max_mag: maximum magnitude
    - avg_depth: average depth
    - max_depth: maximum depth
    - avg_sig: average significance
    - max_sig: maximum significance
    - most_active_region: the region/place with the highest number of earthquakes on that day

    Additional notes:
    - Input DataFrame must have at least the following columns:
      'time' (timestamp in ms), 'mag', 'depth', 'sig', 'place'
    - Converts 'time' to 'event_date' internally if not already present
    - Output DataFrame is ready to write to a Gold table, partitioned by event_date

    Args:
        df (DataFrame): Silver-level earthquakes DataFrame

    Returns:
        DataFrame: Daily summary Gold-level DataFrame
    """

    # Step 1: Basic numeric aggregates
    agg_df = df.groupBy("event_date").agg(
        F.count("*").alias("daily_count"),
        F.avg("mag").alias("avg_mag"),
        F.max("mag").alias("max_mag"),
        F.avg("depth").alias("avg_depth"),
        F.max("depth").alias("max_depth"),
        F.avg("sig").alias("avg_sig"),
        F.max("sig").alias("max_sig")
    )

    # Step 2: Most active region per day
    region_count_df = (
    df.groupBy(
        "event_date",
        F.when(F.col("place").contains(" of "), F.expr("split(place, ' of ')[1]"))
         .otherwise(F.col("place")).alias("region")
    )
    .agg(F.count("*").alias("place_count"))
    )

    # Step 3: Window to rank by count per day
    w = Window.partitionBy("event_date").orderBy(F.desc("place_count"))
    region_ranked = region_count_df.withColumn("rank", F.rank().over(w))
    most_active_region_df = region_ranked.filter(F.col("rank") == 1).select(
        "event_date", F.col("region").alias("most_active_region")
    )

    # Step 4: Join aggregates + most active region
    df_summary = agg_df.join(most_active_region_df, on="event_date", how="left")

    # Optional: reorder columns
    df_summary = df_summary.select(
        "event_date",
        "daily_count",
        "avg_mag", "max_mag",
        "avg_depth", "max_depth",
        "avg_sig", "max_sig",
        "most_active_region"
    )

    return df_summary


def transform_gold_table_2(df):
    """
    Transform Silver earthquake data into a Gold table containing only high-magnitude earthquakes (mag >= 5), splitting the place column into separate region, distance_from_region, and direction_from_region columns, and selecting only the desired columns for analysis.

    Args:
        df_silver (DataFrame): The Silver-level earthquakes DataFrame with columns including id, longitude, latitude, depth, mag, magType, place, event_date, tsunami, sig, etc.

    Returns:
        DataFrame: Transformed Gold-level DataFrame for high-magnitude earthquakes with relevant, clean data:
        
    """
    
    df_summary = (
        df_silver
        .filter(F.col("mag") >= 5)
        .withColumn(
            "distance_from_region",
            F.when(
                F.col("place").contains(" of "),
                F.when(
                    F.size(F.split(F.split(F.col("place"), " of ")[0], " ")) >= 2,
                    F.concat(
                        F.split(F.split(F.col("place"), " of ")[0], " ")[0],
                        F.lit(" "),
                        F.split(F.split(F.col("place"), " of ")[0], " ")[1]
                    )
                ).otherwise(F.split(F.split(F.col("place"), " of ")[0], " ")[0])
            ).otherwise(F.lit(None))
        )
        .withColumn(
            "direction_from_region",
            F.when(
                F.col("place").contains(" of "),
                F.when(
                    F.size(F.split(F.split(F.col("place"), " of ")[0], " ")) >= 3,
                    F.split(F.split(F.col("place"), " of ")[0], " ")[2]
                ).otherwise(F.lit(None))
            ).otherwise(F.lit(None))
        )
        .withColumn(
            "region",
            F.when(F.col("place").contains(" of "),
                F.trim(F.expr("split(place, ' of ')[1]")))
            .otherwise(F.col("place"))
        )
        .select(
            "id",
            "longitude",
            "latitude",
            "depth",
            "mag",
            "magType",
            "region",
            "distance_from_region",
            "direction_from_region",
            "event_date",
            "tsunami",
            "sig"
        )
    )
    
    return df_summary


# Map transformations to tables
gold_transformations = {
    gold_table_1: transform_gold_table_1,
    gold_table_2: transform_gold_table_2
}

# Dictionary to store transformed DFs for Step 4 (Upsert)
gold_ready_dfs = {}

for gold_table, transform_fn in gold_transformations.items():

    df_new = new_gold_data[gold_table]   # from Step 2
    if df_new.count() == 0:
        continue

    df_transformed = transform_fn(df_new)

    gold_ready_dfs[gold_table] = df_transformed
    print(f"Gold transformation complete for {gold_table}")


# COMMAND ----------

# -------------------------------
# STEP 4: Write/Upsert Gold Tables
# -------------------------------

# Define table-specific configuration
gold_table_configs = {
    gold_table_1: {  # e.g., daily summary table
        "type": "aggregated",
        "partition_col": "event_date",  # used for overwrite
        "key_col": None,  # no unique ID for merge
        "zorder_cols": None
    },
    gold_table_2: {  # e.g., event-level table
        "type": "event_level",
        "partition_col": None,
        "key_col": "id",  # unique ID for merge
        "zorder_cols": ["event_date", "region"]  # specify Z-Order columns here
    }
}

for gold_table, df_gold in gold_ready_dfs.items():
    cfg = gold_table_configs[gold_table]
    print(f"Processing {gold_table} ({cfg['type']})...")

    if not spark.catalog.tableExists(gold_table):
        # First-time table creation
        writer = df_gold.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
        if cfg["type"] == "aggregated" and cfg["partition_col"]:
            writer = writer.partitionBy(cfg["partition_col"])
        writer.saveAsTable(gold_table)
        print(f"Created {gold_table}.")
        continue

    # Table exists → handle differently by type
    if cfg["type"] == "aggregated":
        # Overwrite partitions
        df_gold.write.format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .option("replaceWhere", f"{cfg['partition_col']} IS NOT NULL") \
            .partitionBy(cfg["partition_col"]) \
            .saveAsTable(gold_table)
        print(f"Overwrote partitions for {gold_table} by {cfg['partition_col']}.")

    elif cfg["type"] == "event_level":
        # Merge/Upsert on key
        delta_gold = DeltaTable.forName(spark, gold_table)
        merge_condition = f"gold.{cfg['key_col']} = updates.{cfg['key_col']}"
        delta_gold.alias("gold") \
            .merge(df_gold.alias("updates"), merge_condition) \
            .whenMatchedUpdateAll() \
            .whenNotMatchedInsertAll() \
            .execute()
        print(f"Performed MERGE UPSERT into {gold_table}.")
        
        # Z-Order for performance
        if cfg.get("zorder_cols"):
            cols_str = ", ".join(cfg["zorder_cols"])
            spark.sql(f"OPTIMIZE {gold_table} ZORDER BY ({cols_str})")
            print(f"Optimized {gold_table} with Z-Order on {cols_str}.")


# COMMAND ----------

# -------------------------------
# STEP 5: Update metadata table
# -------------------------------

for gold_table, df_gold in gold_ready_dfs.items():
    if df_gold.count() == 0:
        continue

    # Determine reference column
    if "event_date" in df_gold.columns:
        last_ts = df_gold.agg(F.max("event_date")).collect()[0][0]
    elif "time" in df_gold.columns:
        last_ts = df_gold.agg(F.max("time")).collect()[0][0]
    else:
        # fallback: take max ingest_timestamp from silver
        last_ts = new_gold_data[gold_table].agg(F.max("ingest_timestamp")).collect()[0][0]

    # Get current version of the gold Delta table
    delta_gold = DeltaTable.forName(spark, gold_table)
    current_version = delta_gold.history(1).select("version").collect()[0][0]
    
    spark.sql(f"""
        MERGE INTO {metadata_table} AS meta
        USING (
            SELECT
                '{gold_table}' AS table_name,
                {current_version} AS last_version,
                '{last_ts}' AS last_ingest_ts,
                current_timestamp() AS last_update_ts
        ) AS src
        ON meta.table_name = src.table_name
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)

    print(f"Updated metadata table for {gold_table} with last_ingest_ts={last_ts}")


# COMMAND ----------

# Vacuum each gold table, retaining 7 days
for gold_table in gold_tables:
    spark.sql(f"VACUUM {gold_table} RETAIN 168 HOURS")