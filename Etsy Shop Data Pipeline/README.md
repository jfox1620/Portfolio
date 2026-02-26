# Etsy Shop ELT Pipeline (AWS + Snowflake + dbt)

## Project Overview
This project is a fully cloud-native end-to-end ELT data pipeline that ingests e-commerce transactional data from the Etsy and Gelato APIs using AWS Lambda, persists it in S3, loads it into Snowflake, and transforms it into a production-style dimensional model using dbt.

Etsy is an online e-commerce marketplace where I sell framed and unframed artwork, and Gelato is my print-on-demand fulfillment partner that produces and ships the physical products directly to customers after an order is placed.

The goal is to simulate a real-world analytics engineering architecture with separation of raw and clean analytics layers.

### Design Principles & Concepts

This project was built using modern data engineering best practices:

**ELT Architecture:**
All transformations occur inside Snowflake after raw ingestion into cloud storage. This leverages the warehouse for scalable compute while preserving raw source data in its original form.

**Separation of Concerns:**
The system enforces clear layer boundaries between raw ingestion, staging transformations, and analytics modeling, ensuring modularity, maintainability, and logical data flow.

**Incremental API Extraction:**
AWS Lambda retrieves only new or updated records using timestamp-based logic, preventing unnecessary reprocessing and enabling efficient daily updates.

**Data Lake Partitioning Strategy:**
Raw JSON files are stored in Amazon S3 using a partitioned folder structure organized by API source and ingestion date, supporting scalable storage and targeted reprocessing.

**Snowflake-Native Ingestion:**
An external stage and Snowpipe automate continuous file ingestion from S3 into Snowflake raw tables, creating a fully managed, event-driven loading process.

**Star Schema Modeling:**
Data is modeled using a dimensional star schema with fact tables at defined business-event grains and conformed dimensions, optimizing performance and usability for analytical workloads.

**dbt Best Practices:**
dbt is used for dependency-aware transformations via ref(), schema-based model organization, and automated execution through Snowflake Tasks, with both generic and custom tests enforcing model integrity.

**Data Quality Enforcement:**
Data integrity is validated through referential checks, business-rule tests, and pipeline-level validation executed as part of dbt build, ensuring reliable analytical outputs.

  
## Repository Structure

```bash
/
├── README.md
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
│   │   ├── fulfilled_orders_have_cost_data.sql
│   │   └── revenue_not_negative.sql
│   │
│   ├── macros/
│   │   └── generate_schema_name.sql
```

## Architecture & Data Flow

**Data Flow Diagram:**

<img width="794" height="391" alt="TrueFormDesignsELTFlow" src="https://github.com/user-attachments/assets/f12ac938-cd41-4a7b-925b-87bd552109b0" />

**Components:**
- *Sources (API):* The system begins with two external data sources: the Etsy API and the Gelato API. Etsy provides marketplace transaction data including orders, customers, product details, pricing, and financial information. Gelato provides fulfillment data such as production costs, shipping status, taxes, and order completion details. Together, these APIs supply both revenue-side and cost-side data necessary for full profitability analysis.
- *AWS Lambda (Daily Trigger):* An [AWS Lambda function](https://github.com/jfox1620/Portfolio/blob/main/Etsy%20Shop%20Data%20Pipeline/AWS_Lambda/etsy_gelato_ingest.py) runs on a scheduled daily trigger via Cloudwatch to extract incremental data from both APIs. For dependencies, the function uses appropriate layers, including a publicly available ARN (Klayers). The function uses last-run timestamps to ensure only new or updated records are retrieved, preventing duplicate processing. After extraction, the Lambda function writes the raw JSON responses to Amazon S3. This design enables automated, event-driven ingestion without manual intervention.
- *AWS S3 (Partitioned Data Lake):* All raw API responses are stored in Amazon S3 using a partitioned folder structure, organized by soruce type and ingestion date. This partitioning strategy improves scalability, simplifies incremental processing, and allows for targeted reprocessing of specific time periods. S3 serves as the durable raw data layer and preserves the full original payload for traceability, auditing, and potential re-ingestion.
- *Snowflake (Ingestion via Snowpipe):* Snowflake connects to the S3 bucket using an external stage and Snowpipe. Snowpipe automatically detects newly added JSON files and separates them into raw Snowflake tables, adding on metadata (source file name and ingestion timestamp). This provides continuous, fully automated ingestion directly into the data warehouse without requiring manual loads or external ETL jobs.
- *dbt Models:* dbt is used to flatten and transform the raw JSON data into structured analytical tables inside Snowflake. The [staging layer](https://github.com/jfox1620/Portfolio/blob/main/Etsy%20Shop%20Data%20Pipeline/dbt%20Project/Models/Staging/) flattens and standardizes (which includes type casting) the raw data into clean views. From there, a star schema is built consisting of [fact tables](https://github.com/jfox1620/Portfolio/blob/main/Etsy%20Shop%20Data%20Pipeline/dbt%20Project/Models/Facts/) and [dimension tables](https://github.com/jfox1620/Portfolio/blob/main/Etsy%20Shop%20Data%20Pipeline/dbt%20Project/Models/Dimensions/) within the analytics schema. This design separates business events (facts) from descriptive attributes (dimensions), enabling efficient analytical queries and scalable reporting. Both models and tests are run on a schedule via a Snowflake task.
- *dbt Tests:* dbt tests enforce data quality and integrity throughout the pipeline. Generic schema tests validate uniqueness, non-null constraints, and referential relationships between facts and dimensions. [Custom tests](https://github.com/jfox1620/Portfolio/blob/main/Etsy%20Shop%20Data%20Pipeline/dbt%20Project/Tests/) enforce business logic rules, ensuring the transformed data remains consistent with expected system behavior. Tests are executed as part of the automated dbt build process to validate the pipeline continuously.

### Snowflake Data Model

This data model was designed using a star schema because it optimizes analytical performance, simplifies querying for BI tools, and clearly separates business events (facts) from descriptive attributes (dimensions), enabling scalable and intuitive reporting.

**ERD:**

<img width="1515" height="824" alt="TRUE_FORM_DESIGNS ANALYTICS ERD" src="https://github.com/user-attachments/assets/17c0055e-ef08-4410-b5e7-c2158c553ed6" />


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
