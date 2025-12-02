##Etsy + Gelato Data Pipeline (Databricks)

This project is an end-to-end data ingestion pipeline that syncs Etsy shop data and Gelato fulfillment data into Databricks SQL. It fetches receipts, transactions, item attributes, and fulfillment records from each API, restructures the nested JSON into analytic-ready tables, and loads them into a managed Databricks SQL Warehouse.

The pipeline is written entirely in a Databricks Community Edition notebook and follows a clean, production-style ETL design. Although CE cannot run external orchestrators such as Prefect or Airflow, the code intentionally mirrors typical orchestration structure—clear task boundaries, modular functions, error handling, and deterministic load behavior—making it representative of real workflow engineering practices.

##What this project demonstrates

-Practical third-party API integration (Etsy + Gelato)
-Paginated extraction and refresh-token OAuth authentication
-JSON normalization and schema design for analytics
-Idempotent warehouse loading (full table replacement)
-Modular, readable engineering conventions with strong docstrings
-Databricks SQL Warehouse connectivity and loading patterns

##Technologies used

Python
-Databricks Community Edition
-Databricks SQL Warehouse
-Etsy API + OAuth workflow
-Gelato Fulfillment API
-pandas

This project serves as a portfolio example of designing a reliable, well-structured data pipeline under real constraints, emphasizing clarity, maintainability, and production-minded engineering even in a notebook environment.
