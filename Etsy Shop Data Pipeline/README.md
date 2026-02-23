# Etsy Shop ELT Pipeline (AWS + Snowflake + dbt)

## Project Overview
This project is a fully cloud-native end-to-end ELT data pipeline that ingests e-commerce transactional data from the Etsy and Gelato APIs using AWS Lambda, persists it in S3, loads it into Snowflake, and transforms it into a production-style dimensional model using dbt.
Etsy is an online e-commerce marketplace where I sell framed and unframed artwork, and Gelato is my print-on-demand fulfillment partner that produces and ships the physical products directly to customers after an order is placed.

The goal is to simulate a real-world analytics engineering architecture with separation of raw and clean analytics layers.


## Repository Structure

```bash
/
├── README.md
├── architecture.png
│
├── aws_lambda/
│   ├── etsy_ingestion.py
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

### Snowflake Data Model

This data model was designed using a star schema because it optimizes analytical performance, simplifies querying for BI tools, and clearly separates business events (facts) from descriptive attributes (dimensions), enabling scalable and intuitive reporting.

**Fact Tables:**
- fact_orders
  + Grain: 1 row per receipt (order-level)
  + Contains revenue metrics
  + Contains fulfillment cost metrics
  + Includes calculated profit fields
- fact_transactions
  + Grain: 1 row per line item (transaction-level)
  + Contains unit price and quantity
  + Enables product-level performance analysis
  + Linked to orders via receipt_id 

**Dimension Tables:**
- dim_customers
  + 1 row per buyer
- dim_products
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


## Design Principles & Concepts

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
