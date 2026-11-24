import json
import os
import functions_framework
from google.cloud import pubsub_v1

PROJECT_ID = os.environ.get("PROJECT_ID", "project-df614751-5062-4d69-80f")
TOPIC_ID = "file-upload-events"

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)


@functions_framework.cloud_event
def publish_file_event(cloud_event):

    data = cloud_event.data

    bucket_name = data.get("bucket")
    file_name = data.get("name")
    generation = data.get("generation")

    print("GCS upload event received")
    print("Bucket:", bucket_name)
    print("File:", file_name)

    # Process only text files from the input folder
    if not file_name.startswith("input/") or not file_name.lower().endswith(".txt"):
        print("Ignoring file:", file_name)
        return

    message = {
        "bucket": bucket_name,
        "file_name": file_name,
        "generation": str(generation)
    }

    future = publisher.publish(
        topic_path,
        json.dumps(message).encode("utf-8")
    )

    message_id = future.result()

    print("Message published successfully!")
    print("Message ID:", message_id)
    print("Metadata:", json.dumps(message))
