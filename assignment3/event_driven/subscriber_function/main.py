import base64
import json
import os
import functions_framework

from google.cloud import storage

storage_client = storage.Client()


def process_file(bucket_name, file_name, generation, subscriber_name):
    print("=" * 60)
    print("SUBSCRIBER:", subscriber_name)
    print("INPUT FILE:", file_name)

    bucket = storage_client.bucket(bucket_name)

    # Read the exact GCS object generation from the Pub/Sub message
    blob = bucket.blob(file_name, generation=int(generation))
    content = blob.download_as_text(encoding="utf-8")

    # Split into lines and remove only line-ending characters
    lines = content.splitlines()

    if lines:
        max_length = max(len(line) for line in lines)
        longest_lines = [
            line for line in lines
            if len(line) == max_length
        ]
    else:
        max_length = 0
        longest_lines = []

    output = (
        f"Input File: {file_name}\n"
        f"Maximum Line Length: {max_length}\n"
        f"Longest Lines:\n"
    )

    for line in longest_lines:
        output += line + "\n"

    output_path = (
        f"output/{subscriber_name}/"
        f"{os.path.basename(file_name).rsplit('.', 1)[0]}_result.txt"
    )

    output_blob = bucket.blob(output_path)
    output_blob.upload_from_string(
        output,
        content_type="text/plain; charset=utf-8"
    )

    print(output)
    print("OUTPUT LOCATION:", f"gs://{bucket_name}/{output_path}")
    print("=" * 60)


@functions_framework.cloud_event
def process_pubsub_message(cloud_event):
    message = cloud_event.data["message"]

    payload = json.loads(
        base64.b64decode(message["data"]).decode("utf-8")
    )

    print("Received Pub/Sub message:", payload)

    process_file(
        bucket_name=payload["bucket"],
        file_name=payload["file_name"],
        generation=payload["generation"],
        subscriber_name="cloud_function"
    )
