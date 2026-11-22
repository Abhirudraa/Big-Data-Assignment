# OPPE Assignment 1 – Railway Stoppage Analysis

## 1. Problem Statement

The objective is to analyze railway train arrival and departure data using PySpark on Google Cloud Platform (GCP).

The analysis calculates train stoppage durations, station-level statistics, identifies unusual stoppage patterns, and calculates the maximum number of train arrivals within a two-hour sliding window.

A Google Cloud Function is also used to process the aggregated CSV automatically when it is uploaded to Google Cloud Storage.

## 2. GCP Architecture

- **Google Cloud Storage (GCS):** Stores the input dataset and aggregated output.
- **Google Cloud Dataproc:** Runs the PySpark analysis.
- **PySpark:** Performs data cleaning, duration calculations and aggregations.
- **Eventarc:** Detects CSV upload events in the GCS bucket.
- **Google Cloud Function (Gen 2):** Reads the uploaded CSV and logs the station with the highest average stoppage duration.

## 3. Repository Structure

```text
OPPE1/
├── code/
│   ├── railway_analysis.py
│   ├── main.py
│   └── requirements.txt
├── output/
│   └── station_aggregates.csv
├── screenshots/
├── recording/
├── documentation/
└── README.md
4. Files Description
code/railway_analysis.py
PySpark application executed on Google Cloud Dataproc. It:
- Reads railway data from GCS.
- Validates required fields and handles invalid time values.
- Calculates stoppage duration in minutes.
- Calculates station-level count, minimum, maximum, average, total and 90th percentile.
- Identifies the top five stations by stoppage count.
- Identifies stations where maximum stoppage duration exceeds three times the average.
- Calculates maximum arrivals in a two-hour sliding window for a selected top-five station.
- Writes aggregated results to GCS as CSV.
code/main.py
Google Cloud Function triggered by a finalized GCS object upload. It:
- Reads the uploaded CSV.
- Identifies the station with the highest average stoppage duration.
- Prints the station code, station name and average duration in Cloud Logging.
code/requirements.txt
Lists the Python dependency required by the Cloud Function.
output/station_aggregates.csv
Contains the aggregated station-level analysis results produced by the PySpark job.
5. GCP Configuration
- Project ID: project-df614751-5062-4d69-80f
- Region: asia-south1
- Dataproc cluster: oppe1-cluster
- Cluster machine type: e2-standard-2
- Boot disk: 50 GB
- GCS bucket: week6-dataproc-2026-project-df614751
- Cloud Function: railway-result-processor
- Runtime: Python 3.12
- Function generation: Gen 2
6. Execution
Submit the PySpark application from Cloud Shell:
gcloud dataproc jobs submit pyspark \
  code/railway_analysis.py \
  --cluster=oppe1-cluster \
  --region=asia-south1


The final aggregated CSV is stored at:
gs://week6-dataproc-2026-project-df614751/OPPE1/output/station_aggregates.csv


Uploading the CSV triggers the Cloud Function:
gcloud storage cp output/station_aggregates.csv \
  gs://week6-dataproc-2026-project-df614751/OPPE1/output/station_aggregates.csv


View Cloud Function logs:
gcloud functions logs read railway-result-processor \
  --gen2 \
  --region=asia-south1 \
  --limit=20


7. Results
Input records: 186,124

Valid stoppages used for duration analysis: 182,147

Records excluded from duration analysis: 3,977

Top five stations by stoppage count:
Station	Stoppage Count
MSB – CHENNAI BEAC	735
SDAH – SEALDAH	729
HWH – HOWRAH JN.	676
KYN – KALYAN JN	611
DR – DADAR	507

The two-hour sliding-window analysis selected MSB – CHENNAI BEAC and found a maximum of 112 train arrivals in a two-hour window.

The Cloud Function successfully identified:
- Station Code: MDB
- Station Name: MANDOR
- Highest Average Stop Duration: 47.20 minutes
8. Screenshots and Recording
The screenshots/ directory is intended for the Cloud Function Logs Explorer screenshot.

The recording/ directory is intended for the assignment screencast.

The documentation/ directory is intended for the completed IBD OPPE LLM reference document.
