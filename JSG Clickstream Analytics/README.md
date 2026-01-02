# JSG Clickstream Analytics

## Project Overview

JSG Clickstream Analytics is a real-time clickstream data pipeline designed to capture anonymized user engagement events from a live production website. The website belongs to the Christian nonprofit [Jesus Said Go](https://www.jesus-said-go.com), where I volunteer, allowing this project to operate on real user traffic rather than synthetic or demo data.

The pipeline collects client-side interaction events, streams them through a messaging layer, persists raw events in a NoSQL datastore, and delivers curated, query-optimized analytics to a cloud data warehouse for reporting and visualization. The system is designed to reflect real-world data engineering patterns while remaining cost-effective, serverless, and suitable for a portfolio project.

This project emphasizes reliability, scalability, and analytical usefulness over raw volume, demonstrating how modern cloud-native components can be composed into a practical analytics platform.

**Problem Statement:**

Many nonprofits lack visibility into how users interact with their websites beyond basic page view counts. Without structured clickstream data, it is difficult to understand content engagement, navigation behavior, or the effectiveness of calls to action such as subscriptions or donations.

This project addresses that gap by implementing a lightweight, privacy-aware clickstream pipeline that captures meaningful interaction data and transforms it into actionable analytics using modern streaming and data warehouse technologies.

**Design Principles:**

- **Separation of concerns** - ingestion, streaming, raw storage, transformation, and analytics are handled by distinct components
- **Cost awareness** - designed to operate within free-tier or low-cost cloud limits
- **Privacy by design** - no personally identifiable information (PII) is collected
- **Production-inspired architecture** - reflects real-world trade-offs in reliability, extensibility, and maintainability
- **Analytics-first modeling** - events and schema are designed with downstream analysis in mind

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
└── Website/
    └── masterpage.js
```

---

## Architecture

**Data Flow:**

<img width="1231" height="631" alt="architecture" src="https://github.com/user-attachments/assets/a5074592-5037-4bac-ad99-1ea2f72b0924" />


**Components:**

- **[Website JavaScript (masterpage.js)](https://github.com/jfox1620/Portfolio/blob/main/JSG%20Clickstream%20Analytics/Website/masterpage.js):** Client-side tracking script embedded in the website’s master page. It initializes a session identifier, captures user interactions (page views, navigation clicks, form submissions, and outbound donate clicks), enriches events with page and session context, and sends structured JSON events to the ingestion endpoint.
- **[Cloud Run Function (clickstream_ingest.py)](https://github.com/jfox1620/Portfolio/blob/main/JSG%20Clickstream%20Analytics/Cloud%20Functions/clickstream_ingest.py):** Public-facing serverless ingestion service that receives clickstream events from the website, performs lightweight validation, and publishes events to Pub/Sub. Designed to be highly available and stateless.
- **[Cloud Run Function (pubsub_to_firestore.py)](https://github.com/jfox1620/Portfolio/blob/main/JSG%20Clickstream%20Analytics/Cloud%20Functions/pubsub_to_firestore.py):** Event consumer triggered by Pub/Sub messages. This function persists clickstream events into Firestore, using the Pub/Sub message payload as the source of truth. Firestore serves as a durable, low-latency event store.
- **[Cloud Run Function (firestore_to_bigquery.py)](https://github.com/jfox1620/Portfolio/blob/main/JSG%20Clickstream%20Analytics/Cloud%20Functions/firestore_to_bigquery.py):** Batch processing function that extracts recent clickstream events from Firestore, maps and cleans fields, loads data into a BigQuery staging table, and merges into the main analytics table using event_id as a deduplication key. This enables reliable, idempotent ingestion into the analytics warehouse. Runs daily via Cloud Scheduler.
- **Pub/Sub (click-events):** Messaging layer that decouples the website ingestion tier from downstream storage and analytics. Provides buffering, fault tolerance, and replay capability while smoothing traffic spikes from client-side event bursts.
- **Firestore (click_events):** Operational event store for raw clickstream data. Stores individual user interaction events with flexible schema support, low write latency, and strong durability. Acts as an intermediate system between real-time ingestion and batch analytics processing.
- **BigQuery (clickstream_analytics.click_events):** Central analytics warehouse optimized for querying and reporting. Stores cleaned, structured clickstream events with enforced schema, enabling aggregation, session analysis, funnel tracking, and dashboarding at scale.
- **[Looker Dashboard](https://lookerstudio.google.com/u/0/reporting/5357902a-fda9-4f78-a87f-9433cf2d6a0e):** Lightweight analytics and visualization layer built on top of BigQuery data. Presents core engagement metrics such as total sessions, page views, navigation clicks, subscription clicks, and outbound donation clicks, along with derived KPIs (e.g., average page views per session). Designed to provide a clear, high-level view of user behavior and site engagement.

**BigQuery Schema:**

**`clickstream_analytics.click_events` table**

| Field Name   | Type      | Mode      | Description                                                                                             |
|--------------|-----------|-----------|---------------------------------------------------------------------------------------------------------|
| event_id     | STRING    | REQUIRED  | Unique identifier for the event (matches Firestore Document ID).                                        |
| event_type   | STRING    | REQUIRED  | Type of the event (e.g., "page_view", "navigation_click", "subscribe_submit", "donate_outbound_click"). |
| session_id   | STRING    | REQUIRED  | Identifier for the user session during which this event occurred.                                       |
| from_page    | STRING    | NULLABLE  | The URL of the page the user navigated from (for navigation events).                                    |
| to_page      | STRING    | NULLABLE  | The URL of the page the user navigated to (for navigation events).                                      |
| page         | STRING    | NULLABLE  | The URL of the current page where the event occurred (for page views or other events).                  |
| timestamp    | TIMESTAMP | REQUIRED  | The UTC timestamp when the event occurred.                                                              |

---

## Event Data Model & Tracking

This project uses an event-based clickstream model designed to capture user engagement while remaining lightweight, privacy-conscious, and analytics-friendly.

**Tracked Events:**

- `page_view` : triggered once per page load to track content engagement
- `navigation_click` : tracks menu-based navigation between pages
- `subscribe_submit` : captures subscription form submissions (exists on almost every page)
- `donate_outbound_click` : tracks outbound clicks to the external donation platform (exists on a single page)

Each event is enriched with session context and page-level metadata to enable session analysis and funnel-style reporting in BigQuery.

**Example Event Payload:**

```json
{
"event_type": "page_view",
"session_id": "62995dbb-fb5e-411d-b6ca-78fb71e87a84",
"page": "/donations",
"timestamp": "2025-12-29T17:59:42.182Z"
}
```

**Privacy Considerations:**

- No personally identifiable information (PII) is collected
- No IP addresses, emails, cookies, or persistent user identifiers
- All analytics are performed at an aggregate level for site engagement insights

## Future Enhancements

Planned future enhancements aim to improve structure, analysis, and data reliability for the pipeline.

**Heartbeat events** – Track user activity more precisely to measure time spent on pages and overall session duration.
**Referrer information** – Capture where traffic is coming from to better understand user sources and navigation paths.
**Logging** – Implement structured logging for easier debugging, monitoring, and auditing of pipeline processes.
**Data quality checks & alerts** – Automatically detect anomalies or missing data and create notifications of any failures or data abnormalities.
