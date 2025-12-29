# JSG Clickstream Analytics

## Overview

**JSG Clickstream Analytics** is a real-time clickstream data pipeline designed to capture anonymized user engagement events from a live website. The website belongs to the Christian nonprofit [Jesus Said Go](https://www.jesus-said-go.com), where I volunteer.  

The pipeline streams events through a messaging system, stores raw data in a NoSQL database, and delivers curated analytics in a cloud data warehouse. This project demonstrates real-world data engineering patterns while remaining cost-effective and portfolio-ready.

**Problem Statement & Design Principles:**

Nonprofits often lack tools to understand how users engage with their websites. Clickstream data—including page views, button clicks, and navigation—provides actionable insights for content and outreach.  
This project addresses that need with a lightweight, privacy-aware pipeline using streaming and cloud-native components.  

**Key Principles:**

- Separation of concerns: ingestion, streaming, raw storage, analytics  
- Cost awareness: free-tier friendly  
- Privacy by design: no PII collected  
- Production-inspired architecture: demonstrates multiple layers and trade-offs

---

## Repository Structure

```bash
/
├── README.md
├── architecture.png
├── ingestion/
│   └── cloud_run_ingest.py
├── kafka/
│   ├── docker-compose.yml
│   ├── create_topics.sh
│   └── kafka_consumer.py
├── nosql/
│   └── mongo_schema.md
├── warehouse/
│   ├── snowflake_schema.sql
│   └── analytics_queries.sql
└── website/
    └── masterpage.js

```

---

## Architecture & Technology Stack

**Data Flow:**

```
Wix Website (masterpage.js tracking)
↓
GCP Cloud Run HTTP Endpoint
↓
Kafka (Streaming)
↓
MongoDB (Raw Event Storage)
↓
Snowflake (Analytics Warehouse)
↓
Analytics & Reporting
```

**Components:**

- **Website JavaScript (masterpage.js):** client-side code embedded in the website that captures page views, button clicks, and navigation events, then sends them to the ingestion endpoint.
- **Cloud Run HTTP Endpoint:** serverless ingestion, always available, handles click events.  
- **Apache Kafka (local via Docker):** buffer layer that decouples ingestion from downstream consumers, supports replay and backpressure.
- **MongoDB:** raw, append-only event storage with flexible schema for evolving formats.  
- **Snowflake:** analytics-ready warehouse optimized for aggregations and reporting.  

**Hybrid Architecture Rationale:**  

Using multiple technologies highlights **architectural decision-making**: each layer chosen for workload characteristics rather than vendor consolidation.

---

## Event Data Model & Tracking

**Tracked Events:**

- Page views (page_view)
- Navigation clicks via menu (navigation_click)
- Subscribe button clicks (subscribe_submit)
- Donate link clicks (donate_outbound_click)

**Example anonymized click event:**

```json
{
"event_type": "page_view",
"page": "/donations",
"timestamp": "2025-12-29T17:59:42.182Z"
}
```

**Privacy Considerations:**

- No PII is collected
- No IP addresses, emails, or user identifiers
- Session IDs are anonymous
- Data used only for aggregate analytics

**Analytics Use Cases:**

- Page views per day
- Most visited pages
- Conversions by page
- Navigation depth and flows
