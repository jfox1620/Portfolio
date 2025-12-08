# USGS Earthquake Lakehouse — End-to-End ETL Pipeline (Bronze → Silver → Gold)

## Project Overview

This project demonstrates a full production-grade lakehouse pipeline using PySpark and Delta Lake.
It ingests real-time earthquake data from the USGS API, lands the raw JSON into a structured Bronze → Silver → Gold architecture, and produces analytical tables suitable for dashboards, reporting, and machine learning.

The goal is to show mastery of modern data engineering fundamentals, including:
+ Bronze/Silver/Gold medallion architecture
+ Incremental processing
+	Schema management
+	Merge vs. Overwrite-by-Partition
+	Incremental metadata tracking
+	Z-ORDER optimization
+	Time Travel
+	Data quality handling (parsing, normalization, error handling)

________________________________________

## Data Source

(USGS Earthquake GeoJSON Feed (Past 30 days))[https://earthquake.usgs.gov/earthquakes/feed/v1.0/geojson.php]

API used:
+	Past Hour All Earthquakes
+	Updated every minute
+	Returns 60–120 recent earthquakes
+	Endpoint returns JSON containing earthquake metadata, geometry, and properties

The pipeline pulls this JSON, normalizes it, and outputs high-value tables used for analysis.

________________________________________

## Architecture

This project follows the Medallion Architecture widely used in lakehouse systems like Databricks and Delta Lake environments:

<img width="941" height="451" alt="architecture" src="https://github.com/user-attachments/assets/c36b4e0d-128f-4a39-a16a-14ebbc2944fc" />

________________________________________

## Key Features Demonstrated

+ Bronze Layer — Raw Landing (Append-Only)
  *	Stores raw API response
  *	Adds ingest_timestamp
  *	No transformations (audit-ready)
+ Silver Layer — Standardization & Normalization
  *	Parses nested JSON (properties, geometry)
  *	Converts epoch milliseconds to proper timestamps
  *	Extracts event_date
  *	Normalizes text fields
  *	Ensures schema consistency
  *	Removes malformed entries gracefully
+ Gold Layer — Analytics Tables
  + Gold Table 1 — Daily Summary
    *	Aggregate data (daily count, average/max magnitude, etc.)
    * Partitioned by event_date for fast reads.
  + Gold Table 2 — mag 5+ Events
    * Event-level data
    *	Clean region parsing (including distance, direction)
    *	Z-ORDER optimization on (event_date, region)
    *	Demonstrates advanced Delta Lake performance optimization
+ Metadata Tracking Table
  *	Last processed timestamp per table
  *	Last update timestamp
  * Used for incremental logic.
+ Merge vs. Overwrite-by-Partition
  *	MERGE INTO → event-level tables (dedupe + upsert)
  *	Overwrite by partition → static daily aggregates
+ Delta Lake Time Travel
  * Rollback, audit, or compare previous versions:
    SELECT * FROM gold_earthquakes_daily_summary VERSION AS OF 3;
  * Time Travel is referenced in this README only and is not used in the notebooks.
+ Z-ORDER Optimization
  * Improves locality of data:
    spark.sql("OPTIMIZE gold_earthquakes_mag5_plus ZORDER BY (event_date, region)")
    
________________________________________

## Repository Structure

/
├── notebooks/
│   ├── 01_bronze_ingest.ipynb
│   ├── 02_silver_transform.ipynb
│   ├── 03_gold_tables.ipynb
│
├── README.md  ← (this file)
│── architecture.png
└── requirements.txt

________________________________________

## Skills Demonstrated

This project shows proficiency in:
+	PySpark (DataFrames, Window functions, JSON processing)
+	Delta Lake (MERGE, OPTIMIZE, ZORDER, Time Travel)
+	Medallion Architecture
+	Incremental & partition-aware processing
+	Data quality handling
+	Real-time data ingestion simulation
+	Metadata frameworks
+	Designing pipeline notebooks

