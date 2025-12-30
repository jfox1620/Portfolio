import base64
import json
from google.cloud import firestore

# Initialize Firestore client
db = firestore.Client()

def pubsub_to_firestore(request: Request):
    """
    Cloud Run / Eventarc Pub/Sub handler.
    Expects Pub/Sub messages via HTTP POST.
    """
    try:
        envelope = request.get_json()
        if not envelope:
            print("No JSON body found")
            return "Bad Request", 400

        # Pub/Sub message is inside 'message' key
        if 'message' not in envelope:
            print("No message field in request")
            return "Bad Request", 400

        pubsub_message = envelope['message']

        # Decode the data from base64
        data_str = base64.b64decode(pubsub_message['data']).decode('utf-8')
        event_data = json.loads(data_str)

        if 'event_type' not in event_data:
            print("No event_type, skipping")
            return "Ignored", 200

        db.collection('click_events').add(event_data)
        print(f"Event written: {event_data}")
        return "OK", 200

    except Exception as e:
        print(f"Error: {e}")
        return "Error", 500