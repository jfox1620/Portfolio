import base64
import json
from google.cloud import firestore

# Initialize Firestore client
db = firestore.Client()

def pubsub_to_firestore(event, context):
    """
    Cloud Function triggered by Pub/Sub messages.
    
    This function decodes the incoming Pub/Sub message, which should be a JSON string representing a clickstream event, and writes it into Firestore.
    
    Args:
        event (dict): The dictionary with data specific to this type of event.
                      The `data` field contains the Pub/Sub message in base64.
        context (google.cloud.functions.Context): Metadata for the event.
    """
    try:
        envelope = request.get_json()
        if not envelope or "message" not in envelope:
            return "Invalid Pub/Sub message format", 400

        pubsub_message = envelope["message"]
        data_str = base64.b64decode(pubsub_message.get("data", "")).decode("utf-8")
        event_data = json.loads(data_str)

        # Skip messages without an 'event_type'
        if "event_type" not in event_data:
            print("No event_type found, skipping.")
            return "Ignored message", 200

        # Optional: add timestamp if missing
        if "timestamp" not in event_data:
            from datetime import datetime
            event_data["timestamp"] = datetime.utcnow().isoformat()

        # Write to Firestore
        db.collection("click_events").add(event_data)
        print(f"Event written to Firestore: {event_data}")
        return "OK", 200

    except Exception as e:
        print(f"Error processing Pub/Sub message: {e}")
        return "Error", 500