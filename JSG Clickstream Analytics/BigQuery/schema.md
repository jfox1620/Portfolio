## BigQuery Schema

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
