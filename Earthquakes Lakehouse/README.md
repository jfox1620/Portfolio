# USGS Earthquake Lakehouse — End-to-End ETL Pipeline (Bronze → Silver → Gold)

## Project Overview

This project demonstrates a full production-grade lakehouse pipeline using PySpark and Delta Lake in Databricks.
It ingests real-time earthquake data from the USGS API, lands the raw JSON into a structured [Bronze](https://github.com/jfox1620/Portfolio/blob/main/Earthquakes%20Lakehouse/Notebooks/01_bronze_ingest.py) -> [Silver](https://github.com/jfox1620/Portfolio/blob/main/Earthquakes%20Lakehouse/Notebooks/02_silver_cleaning.py) -> [Gold](https://github.com/jfox1620/Portfolio/blob/main/Earthquakes%20Lakehouse/Notebooks/03_gold_analytics.py) architecture, and produces analytical tables suitable for dashboards, reporting, and machine learning.

The goal is to show understanding of modern data engineering fundamentals, including:
+ Architecture & Pipeline Design
  * Bronze → Silver → Gold medallion architecture
  * Modular notebook pipeline design
  * Metadata tracking framework
  * Incremental loads
+ PySpark Data Engineering
  * DataFrame transformations
  * JSON parsing & normalization
  * Schema management
+ Delta Lake Capabilities
  * MERGE INTO and Overwrite-by-Partition Upserts
  * Z-ORDER optimization
  * Time Travel queries
+ Data Quality & Reliability
  * Parsing and error-tolerant ingestion
  * Normalization of nested/semi-structured data

________________________________________

## Repository Structure

```bash
/
├── notebooks/
│   ├── 01_bronze_ingest.py
│   ├── 02_silver_transform.py
│   ├── 03_gold_tables.py
│   ├── generate_map_animation.py
│
├── README.md  ← (this file)
│── architecture.png
│── requirements.txt
└── earthquake_map.html
```
________________________________________

## Data Source

[USGS Earthquake GeoJSON Feed](https://earthquake.usgs.gov/earthquakes/feed/v1.0/geojson.php)

+	All Earthquakes past hour
+	Updated every minute
+	Returns 60–120 recent earthquakes
+	Endpoint returns JSON containing earthquake metadata, geometry, and properties

________________________________________

## Architecture

This project follows the Medallion Architecture widely used in lakehouse systems like Databricks and Delta Lake environments:

<img width="941" height="451" alt="architecture" src="https://github.com/user-attachments/assets/c36b4e0d-128f-4a39-a16a-14ebbc2944fc" />

________________________________________

## Key Features Demonstrated

+ [Bronze Layer](https://github.com/jfox1620/Portfolio/blob/main/Earthquakes%20Lakehouse/Notebooks/01_bronze_ingest.py) — Raw Landing (Append-Only)
  *	Stores raw API response
  *	Adds ingest_timestamp
  *	No transformations (audit-ready)
+ [Silver Layer](https://github.com/jfox1620/Portfolio/blob/main/Earthquakes%20Lakehouse/Notebooks/02_silver_cleaning.py) — Standardization & Normalization
  *	Parses nested JSON (properties, geometry)
  *	Converts epoch milliseconds to proper timestamps
  *	Extracts event_date
  *	Normalizes text fields
  *	Ensures schema consistency
+ [Gold Layer](https://github.com/jfox1620/Portfolio/blob/main/Earthquakes%20Lakehouse/Notebooks/03_gold_analytics.py) — Analytics Tables
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
    ```SQL
    SELECT * FROM gold_earthquakes_daily_summary VERSION AS OF 3;
    ```
  * Time Travel is referenced in this README only and is not used in the notebooks.
+ Z-ORDER Optimization
  * Improves locality of data:
    ```python
    spark.sql("OPTIMIZE gold_earthquakes_mag5_plus ZORDER BY (event_date, region)")
    ```

 ________________________________________

## Interactive Earthquakes Visualization

In addition to the ETL pipeline, this project includes an interactive visualization of earthquake events over time.

+ [Generate Animation Notebook](https://github.com/jfox1620/Portfolio/blob/main/Earthquakes%20Lakehouse/Notebooks/generate_map_animation.py)
  * Generates interactive Folium and Plotly visualizations of recent earthquake activity.
  * Converts the Silver layer earthquake data into GeoJSON features for animation.
  * Folium `TimestampedGeoJson` map for animated timeline view.
  * Plotly `scatter_geo` map for animated daily earthquake locations.

+ [Interactive Map HTML](https://github.com/jfox1620/Portfolio/blob/main/Earthquakes%20Lakehouse/earthquake_map.html)
  * Fully interactive Plotly map exported as HTML.
  * Can be downloaded and opened in a browser to explore earthquake locations, magnitude, and timelines.
  * Marker size represents earthquake magnitude; animation frames show daily events.

**Viewing Notes:**
- To view the HTML map online, you can use [GitHub Pages](https://pages.github.com/) or [HTML Preview](https://htmlpreview.github.io/) for interactive rendering.
- Locally, simply download `earthquake_map.html` and open it in a browser.

________________________________________
