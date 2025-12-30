# JSG Clickstream Analytics

## Project Overview

JSG Clickstream Analytics is a real-time clickstream data pipeline designed to capture anonymized user engagement events from a live website. The website belongs to the Christian nonprofit [Jesus Said Go](https://www.jesus-said-go.com), where I volunteer.

The pipeline streams events through a messaging system, stores raw data in a NoSQL database, and delivers curated analytics in a cloud data warehouse. This project demonstrates real-world data engineering patterns while remaining cost-effective and portfolio-ready.

**Problem Statement & Design Principles:**

Nonprofits often lack tools to understand how users engage with their websites. Clickstream data, including page views, button clicks, and navigation, provides actionable insights for content and outreach.
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
├── Cloud Functions/
│   ├── clickstream_ingest.py
│   ├── pubsub_to_firestore.py
│   └── firestore_to_bigquery.py
├── BigQuery/
│   └── schema.md
└── Website/
    └── masterpage.js
```

---

## Architecture

**Data Flow:**

<img width="1211" height="631" alt="architecture" src="https://github.com/user-attachments/assets/baf5057d-078b-4d8d-9921-b2a90c275609" />

**Components:**

- **Website JavaScript (masterpage.js):** client-side code embedded in the website that captures page views, button clicks, and navigation events, then sends them to the ingestion endpoint.
- **Cloud Run Function (clickstream_ingestion.py):** serverless ingestion, always available, handles click events.  
- **Pub/Sub (click-events):** buffer layer that decouples ingestion from downstream consumers, supports replay and backpressure.
- **Cloud Run Function (pubsub_to_firestore):** [description needed]
- **Firestore (click_events):** [description needed]
- **Cloud Run Function (firestore_to_bigquery):** [description needed]
- **BigQuery (clickstream_analytics.click_events):** [description needed]
- **Looker Dashboard:** [description needed]

---

## Event Data Model & Tracking

**Tracked Events:**

- Page views (page_view)
- Navigation clicks via menu (navigation_click)
- Subscribe button clicks (subscribe_submit)
- Donate link clicks (donate_outbound_click)

**Example click event:**

```json
{
"event_type": "page_view",
"session_id": "62995dbb-fb5e-411d-b6ca-78fb71e87a84",
"page": "/donations",
"timestamp": "2025-12-29T17:59:42.182Z"
}
```

**Privacy Considerations:**

- No PII is collected
- No IP addresses, emails, or user identifiers
- Data used only for aggregate analytics

**Analytics:**

[Pending]
