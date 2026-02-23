# Etsy Shop ELT Pipeline (AWS + Snowflake + dbt)

## Project Overview
This project is a fully cloud-native end-to-end ELT data pipeline that ingests e-commerce transactional data from the Etsy and Gelato APIs using AWS Lambda, persists it in S3, loads it into Snowflake, and transforms it into a production-style dimensional model using dbt.

Etsy is an online e-commerce marketplace where I sell framed and unframed artwork, and Gelato is my print-on-demand fulfillment partner that produces and ships the physical products directly to customers after an order is placed.

The goal is to simulate a real-world analytics engineering architecture with separation of raw and clean analytics layers.

This project was built using modern data engineering best practices:

**ELT Architecture:**
- Transformation occurs inside Snowflake after raw ingestion.

**Separation of Concerns:**
- Raw ingestion isolated from transformation
- Staging isolated from analytics
- Clear layer boundaries

**Incremental API Extraction:**
- Lambda pulls only new records using timestamp logic
- Prevents reprocessing historical data

**Data Lake Partitioning Strategy:**
- Partitioned folder structure by API source and ingestion date.

**Star Schema Modeling:**
- Fact tables at business event grain
- Conformed dimensions
- Optimized for BI tools

**Snowflake-Native Ingestion:**
- External stage
- Snowpipe
- Automated loading

**dbt Best Practices:**
- ref() for dependency management
- Schema-based layering
- Generic tests (not_null, unique, relationships)
- Custom business logic tests
- Orchestrated builds via Snowflake Task

**Data Quality Enforcement:**
- Referential integrity checks
- Business rule validation
- Pipeline-level validation via dbt build

  
## Repository Structure

```bash
/
├── README.md
├── Architecture.png
│
├── AWS_Lambda/
│   ├── etsy_gelato_ingest.py
│
├── dbt_project/
│   ├── dbt_project.yml
│   │
│   ├── models/
│   │   │
│   │   ├── staging/
│   │   │   ├── receipts.sql
│   │   │   ├── transactions.sql
│   │   │   ├── fulfillments.sql
│   │   │   └── staging_schema.yml
│   │   │
│   │   ├── dimensions/
│   │   │   ├── dim_customers.sql
│   │   │   ├── dim_products.sql
│   │   │   └── dim_schema.yml
│   │   │
│   │   └── facts/
│   │       ├── fact_orders.sql
│   │       ├── fact_transactions.sql
│   │       └── fact_schema.yml
│   │
│   ├── tests/
│   │   ├── shipped_with_timestamp.sql
│   │   └── custom_tests.sql
│   │
│   ├── macros/
│   │   └── generate_schema_name.sql
```

## Architecture & Data Flow

**Diagram:**
```bash
Etsy API      Gelato API
     │             │
     ▼             ▼
AWS Lambda (daily incremental extraction)
     │
     ▼
Amazon S3 (partitioned raw JSON storage)
     │
     ▼
Snowflake External Stage
     │
     ▼
Snowpipe
     │
     ▼
Raw Tables (receipts, transactions, fulfillments)
     │
     ▼
dbt Staging Models (views)
     │
     ▼
dbt Dimensions & Facts (analytics schema)
     │
     ▼
Snowflake Tasks (scheduled dbt build)
```

**Components:**
- *Sources (API):* The system begins with two external data sources: the Etsy API and the Gelato API. Etsy provides marketplace transaction data including orders, customers, product details, pricing, and financial information. Gelato provides fulfillment data such as production costs, shipping status, taxes, and order completion details. Together, these APIs supply both revenue-side and cost-side data necessary for full profitability analysis.
- *AWS Lambda (Daily Trigger):* An AWS Lambda function runs on a scheduled daily trigger to extract incremental data from both APIs. The function uses last-run timestamps to ensure only new or updated records are retrieved, preventing duplicate processing. After extraction, the Lambda function writes the raw JSON responses to Amazon S3. This design enables automated, event-driven ingestion without manual intervention.
- *AWS S3 (Partitioned Data Lake):* All raw API responses are stored in Amazon S3 using a partitioned folder structure, typically organized by ingestion date. This partitioning strategy improves scalability, simplifies incremental processing, and allows for targeted reprocessing of specific time periods. S3 serves as the durable raw data layer and preserves the full original payload for traceability, auditing, and potential re-ingestion.
- *Snowflake (Ingestion via Snowpipe):* Snowflake connects to the S3 bucket using an external stage and Snowpipe. Snowpipe automatically detects newly added files and loads them into raw Snowflake tables. This provides continuous, fully automated ingestion directly into the data warehouse without requiring manual loads or external ETL jobs.
- *dbt Models:* dbt is used to transform the raw JSON data into structured analytical tables inside Snowflake. The staging layer flattens and standardizes the raw data into clean views. From there, a star schema is built consisting of fact tables and dimension tables within the analytics schema. This design separates business events (facts) from descriptive attributes (dimensions), enabling efficient analytical queries and scalable reporting.
- *dbt Tests:* dbt tests enforce data quality and integrity throughout the pipeline. Generic schema tests validate uniqueness, non-null constraints, and referential relationships between facts and dimensions. Custom tests enforce business logic rules, ensuring the transformed data remains consistent with expected system behavior. Tests are executed as part of the automated dbt build process to validate the pipeline continuously.

### Snowflake Data Model

This data model was designed using a star schema because it optimizes analytical performance, simplifies querying for BI tools, and clearly separates business events (facts) from descriptive attributes (dimensions), enabling scalable and intuitive reporting.

ERD Diagram TBD

**Fact Tables:**
- `fact_orders`
  + Grain: 1 row per receipt (order-level)
  + Contains revenue metrics
  + Contains fulfillment cost metrics
  + Includes calculated profit fields
- `fact_transactions`
  + Grain: 1 row per line item (transaction-level)
  + Contains unit price and quantity
  + Enables product-level performance analysis
  + Linked to orders via receipt_id 

**Dimension Tables:**
- `dim_customers`
  + 1 row per buyer
- `dim_products`
  + 1 row per SKU/listing
  + Includes size, thickness, frame, product type

**Schema Separation:**
| Layer      | Schema     | Materialization   |
| --------   | -------    | ---------------   |
| Raw        | RAW_INGEST | Tables (Snowpipe) |
| Staging    | ANALYTICS  | Views             |
| Dimensions | ANALYTICS  | Tables            |
| Facts      | ANALYTICS  | Tables            |

DBT project object is deployed inside Snowflake and executed via Task scheduling.


## Future Enhancements

This architecture can be extended in production by:
- Adding Etsy fee modeling for true net profit
- Implementing Slowly Changing Dimensions (SCD Type 2)
- Introducing dbt snapshots
- Adding incremental fact tables for large-scale growth
- Adding monitoring & alerting
- Integrating BI dashboards (Power BI / Tableau)
- Implementing CI/CD for dbt deployments
- Adding automated data observability checks
