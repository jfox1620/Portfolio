import json
from google.cloud import pubsub_v1

# Initialize Pub/Sub publisher
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path("jsg-clickstream", "click-events")

def ingest_event(request: Request):
    # Handle preflight CORS request
    if request.method == "OPTIONS":
        # Required headers for preflight
        headers = {
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "POST, OPTIONS",
            "Access-Control-Allow-Headers": "Content-Type",
        }
        return ("", 204, headers)

    # Only allow POST from here
    if request.method != "POST":
        return ("Only POST allowed", 405)

    # Handle POST event
    event = request.get_json(silent=True)
    if not event:
        return ("No JSON received", 400)

    print("Received event:",json.dumps(event))

    # Publish to Pub/Sub
    publisher.publish(topic_path, json.dumps(event).encode("utf-8"))

    # Add CORS header so Wix can receive response
    headers = {"Access-Control-Allow-Origin": "*"}
    return ("OK", 200, headers)
