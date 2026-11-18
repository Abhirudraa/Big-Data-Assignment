# Week 6 - Customer Analytics using Dataproc and SparkSQL

## 1. Objective

The objective is to analyze customer transaction data using Apache SparkSQL
on Google Cloud Dataproc and demonstrate Slowly Changing Dimensions (SCD)
Type I and Type II.

All processing was performed on GCP Dataproc. Input data was stored in
Google Cloud Storage (GCS), and output datasets were written back to GCS.

## 2. Input Datasets

The data directory contains:

- customer_master.csv: Existing customer master records.
- customer_updates.csv: Customer records containing updated attributes
  and their change dates.
- transactions.csv: Customer transaction records, including transaction
  ID, customer ID, date, amount, payment mode, and city.

## 3. Implementation

The SparkSQL application is scripts/main.py.

### Transaction Analytics

1. Top 5 customers by total transaction amount.
2. Number of customers per city.
3. Overall average transaction value.

### SCD Type I

Updates changed customer attributes in place. Previous values are
overwritten, and historical versions are not retained.

### SCD Type II

Preserves customer history. When attributes change, the old record is
expired and a new current record is inserted with the change date as
its effective date.

Before-and-after table previews and validation results are printed
in the driver logs.

## 4. GCP Infrastructure

- Project ID: project-df614751-5062-4d69-80f
- Region: asia-south1
- Zone: asia-south1-a
- Dataproc cluster: week6-spark-cluster
- GCS bucket: week6-dataproc-2026-project-df614751

Input location:
gs://week6-dataproc-2026-project-df614751/input/

Output location:
gs://week6-dataproc-2026-project-df614751/output/

## 5. Execution

The application was submitted using:

gcloud dataproc jobs submit pyspark scripts/main.py \
  --cluster=week6-spark-cluster \
  --region=asia-south1

## 6. Output Files

The output directory contains:

- top_5_customers: Top five customers by total transaction amount.
- customers_per_city: Customer count grouped by city.
- average_transaction_value: Overall transaction count and average value.
- scd_type_1/before: Customer records before Type I updates.
- scd_type_1/after: Customer records after Type I updates.
- scd_type_2/before: Customer records before Type II updates.
- scd_type_2/after: Customer history after Type II updates.

Each output directory contains a CSV part file and a Spark _SUCCESS marker.

## 7. Execution Verification

- Dataproc Job ID: 00211b00e69c47e19abdcdd5153adbc9
- Job status: DONE
- YARN application status: FINISHED
- Application: Week6-Customer-Analytics-SCD
- All seven output datasets were verified in GCS.
- Driver output logs were inspected.

## 8. Repository Structure

Week_6/
  README.md
  data/
    customer_master.csv
    customer_updates.csv
    transactions.csv
  scripts/
    main.py
  output/
    top_5_customers/
    customers_per_city/
    average_transaction_value/
    scd_type_1/
      before/
      after/
    scd_type_2/
      before/
      after/

## 9. Learning Outcomes

This assignment demonstrates running SparkSQL workloads on Google Cloud
Dataproc, querying data stored in GCS, performing transaction aggregations,
and implementing SCD Type I and Type II to manage changing customer
attributes and preserve historical records.
