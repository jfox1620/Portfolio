from google.cloud import firestore, bigquery
from datetime import datetime, timedelta, timezone
import json
import tempfile

# Initialize clients
db = firestore.Client()
bq = bigquery.Client()

# Configuration
PROJECT_ID = "jsg-clickstream"
DATASET = "clickstream_analytics"
MAIN_TABLE = f"{DATASET}.click_events"
STAGING_TABLE = f"{DATASET}.click_events_staging"
# Map Firestore fields to BigQuery fields
field_mapping = {
    "event_id": "event_id",
    "event_type": "event_type",
    "session_id": "session_id",
    "from_page": "from_page",
    "to_page": "to_page",
    "page": "page",
    "timestamp": "timestamp"
}

def firestore_to_bigquery(request: Request):
    """
    Pulls the last day's click events from Firestore into a BigQuery staging table, then merges into the main table using event_id as the unique key.
    Ignores unexpected fields automatically.
    """
  
    # 1. Time window
    now = datetime.now(timezone.utc)
    yesterday = now - timedelta(days=1)

    # 2. Query Firestore
    events_ref = db.collection("click_events")
    query = events_ref.where("timestamp", ">=", yesterday.isoformat())
    docs = query.stream()

    rows_to_insert = []
    for doc in docs:
        data = doc.to_dict()
        data["event_id"] = doc.id

        # Map Firestore field names to BigQuery column names
        mapped = {bq_field: data.get(fs_field, None) for fs_field, bq_field in field_mapping.items()}
        rows_to_insert.append(mapped)

    if not rows_to_insert:
        print("No new events to process.")
        return "OK", 200

    # 3. Load into staging table
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",  # overwrite staging table each run
        schema=[
            bigquery.SchemaField("event_id", "STRING"),
            bigquery.SchemaField("event_type", "STRING"),
            bigquery.SchemaField("session_id", "STRING"),
            bigquery.SchemaField("from_page", "STRING"),
            bigquery.SchemaField("to_page", "STRING"),
            bigquery.SchemaField("page", "STRING"),
            bigquery.SchemaField("timestamp", "TIMESTAMP"),
        ],
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        autodetect=False,
    )

    with tempfile.NamedTemporaryFile("w", delete=False) as temp_file:
        for row in rows_to_insert:
            temp_file.write(json.dumps(row) + "\n")
        temp_file_path = temp_file.name

    with open(temp_file_path, "rb") as source_file:
        load_job = bq.load_table_from_file(
            source_file, STAGING_TABLE, job_config=job_config
        )
    load_job.result()
    print(f"Loaded {len(rows_to_insert)} rows into staging table.")

    # 4. Merge into main table
    merge_sql = f"""
    MERGE `{PROJECT_ID}.{MAIN_TABLE}` AS main
    USING `{PROJECT_ID}.{STAGING_TABLE}` AS staging
    ON main.event_id = staging.event_id
    WHEN MATCHED THEN
      UPDATE SET
        event_type = staging.event_type,
        page = staging.page,
        session_id = staging.session_id,
        from_page = staging.from_page,
        to_page = staging.to_page,
        timestamp = staging.timestamp
    WHEN NOT MATCHED THEN
      INSERT (event_id, event_type, page, session_id, from_page, to_page, timestamp)
      VALUES (staging.event_id, staging.event_type, staging.page, staging.session_id, staging.from_page, staging.to_page, staging.timestamp)
    """
    query_job = bq.query(merge_sql)
    query_job.result()
    print(f"Merged {len(rows_to_insert)} rows into main table.")

    return "OK", 200