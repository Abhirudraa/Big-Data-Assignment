import csv
import io
import logging
import functions_framework
from google.cloud import storage

logging.basicConfig(level=logging.INFO, force=True)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

@functions_framework.cloud_event
def process_station_results(cloud_event):
    try:
        data = cloud_event.data
        bucket_name = data.get("bucket")
        file_name = data.get("name")

        print(f"Received file: {file_name} in bucket: {bucket_name}", flush=True)

        if not file_name or not file_name.endswith("station_aggregates.csv"):
            print(f"Ignoring unrelated file: {file_name}", flush=True)
            return

        client = storage.Client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(file_name)
        content = blob.download_as_text()

        reader = csv.DictReader(io.StringIO(content))
        rows = list(reader)

        if not rows:
            print("WARNING: CSV contains no station records.", flush=True)
            return

        valid_rows = [
            row for row in rows
            if row.get("avg_stop_min") not in (None, "")
        ]

        if not valid_rows:
            print(f"WARNING: No valid average duration values found. Columns: {reader.fieldnames}", flush=True)
            return

        highest = max(
            valid_rows,
            key=lambda row: float(row["avg_stop_min"])
        )

        print("========== RAILWAY ANALYSIS RESULT ==========", flush=True)
        print(f"Station Code: {highest['Station Code']}", flush=True)
        print(f"Station Name: {highest['Station Name']}", flush=True)
        print(f"Highest Average Stop Duration: {float(highest['avg_stop_min']):.2f} minutes", flush=True)
        print("==============================================", flush=True)

    except Exception as e:
        print(f"ERROR processing railway results CSV: {e}", flush=True)
        raise
