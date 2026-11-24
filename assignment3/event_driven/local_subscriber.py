import os
import json
from google.cloud import pubsub_v1, storage

PROJECT_ID = os.getenv("PROJECT_ID", "project-df614751-5062-4d69-80f")
SUBSCRIPTION_ID = "local-sub"
SUBSCRIBER_NAME = "local"

subscriber = pubsub_v1.SubscriberClient()
storage_client = storage.Client()

subscription_path = subscriber.subscription_path(
    PROJECT_ID, SUBSCRIPTION_ID
)


def process_file(payload):
    bucket_name = payload["bucket"]
    file_name = payload["file_name"]
    generation = int(payload["generation"])

    print("=" * 60)
    print("SUBSCRIBER:", SUBSCRIBER_NAME)
    print("INPUT FILE:", file_name)

    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_name, generation=generation)

    content = blob.download_as_text(encoding="utf-8")
    lines = content.splitlines()

    if lines:
        max_length = max(len(line) for line in lines)
        longest_lines = [
            line for line in lines if len(line) == max_length
        ]
    else:
        max_length = 0
        longest_lines = []

    output = (
        f"Input File: {file_name}\n"
        f"Maximum Line Length: {max_length}\n"
        f"Longest Lines:\n"
        + "".join(line + "\n" for line in longest_lines)
    )

    output_path = (
        f"output/local/{os.path.basename(file_name).rsplit('.', 1)[0]}_result.txt"
    )

    bucket.blob(output_path).upload_from_string(
        output,
        content_type="text/plain; charset=utf-8"
    )

    print(output)
    print("GCS OUTPUT:", f"gs://{bucket_name}/{output_path}")
    print("=" * 60)


def callback(message):
    try:
        payload = json.loads(message.data.decode("utf-8"))
        print("Received Pub/Sub message:", payload)

        process_file(payload)
        message.ack()
        print("Message acknowledged successfully.")

    except Exception as error:
        print("Processing failed:", str(error))
        message.nack()


if __name__ == "__main__":
    print("Local subscriber started.")
    print("Listening on:", subscription_path)

    streaming_pull = subscriber.subscribe(
        subscription_path, callback=callback
    )

    try:
        streaming_pull.result()
    except KeyboardInterrupt:
        streaming_pull.cancel()
        print("Local subscriber stopped.")
