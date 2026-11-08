from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType
from pyspark.sql.window import Window

# --------------------------------------------------
# 1. INITIALIZATION
# --------------------------------------------------

spark = SparkSession.builder.appName(
    "Week4_Customer_Transaction_Analysis"
).getOrCreate()

spark.sparkContext.setLogLevel("WARN")

BUCKET = "gs://assignment-2-week3-abhirudraa-2026"

CUSTOMER_INPUT = f"{BUCKET}/input/customers.csv"
TRANSACTION_INPUT = f"{BUCKET}/input/transactions.csv"

OUTPUT = f"{BUCKET}/output"

print("\n========== WEEK 4 PYSPARK ASSIGNMENT ==========\n")

# --------------------------------------------------
# 2. LOAD DATASETS
# --------------------------------------------------

customers_raw = (
    spark.read
    .option("header", True)
    .option("inferSchema", False)
    .csv(CUSTOMER_INPUT)
)

transactions_raw = (
    spark.read
    .option("header", True)
    .option("inferSchema", False)
    .csv(TRANSACTION_INPUT)
)

print("CUSTOMER SCHEMA:")
customers_raw.printSchema()
customers_raw.show(5, truncate=False)

print("TRANSACTION SCHEMA:")
transactions_raw.printSchema()
transactions_raw.show(5, truncate=False)

CUSTOMER_COLS = [
    "customer_id", "customer_name", "city", "status"
]

TRANSACTION_COLS = [
    "transaction_id", "customer_id", "transaction_date",
    "transaction_amount", "payment_mode"
]

# Validate expected input columns
assert set(CUSTOMER_COLS).issubset(customers_raw.columns)
assert set(TRANSACTION_COLS).issubset(transactions_raw.columns)

# --------------------------------------------------
# 3. CUSTOMER DATA CLEANING
# --------------------------------------------------

print("\n========== CUSTOMER DATA CLEANING ==========\n")

# Assign row identifiers to distinguish exact duplicates
customer_window = (
    Window.partitionBy(*CUSTOMER_COLS)
    .orderBy("_row_id")
)

customers = (
    customers_raw
    .select(*CUSTOMER_COLS)
    .withColumn("_row_id", F.monotonically_increasing_id())
    .withColumn("_duplicate_rank", F.row_number().over(customer_window))
)

customer_duplicate = F.col("_duplicate_rank") > 1

customer_missing = [
    F.col(c).isNull() | (F.trim(F.col(c)) == "")
    for c in CUSTOMER_COLS
]

customer_reason = F.concat_ws(
    "; ",
    F.when(customer_missing[0], "Missing customer_id"),
    F.when(customer_missing[1], "Missing customer_name"),
    F.when(customer_missing[2], "Missing city"),
    F.when(customer_missing[3], "Missing status"),
    F.when(customer_duplicate, "Duplicate record")
)

customers = customers.withColumn(
    "invalid_reason", customer_reason
)

# Save invalid customer records
invalid_customers = (
    customers
    .filter(F.col("invalid_reason") != "")
    .select(*CUSTOMER_COLS, "invalid_reason")
)

# Remove invalid records and duplicates
clean_customers = (
    customers
    .filter(F.col("invalid_reason") == "")
    .drop("_row_id", "_duplicate_rank", "invalid_reason")
)

# Standardize city names
city_key = F.lower(F.trim(F.col("city")))

clean_customers = (
    clean_customers
    .withColumn(
        "city",
        F.when(city_key.isin("kochi", "cochin"), "Kochi")
         .when(city_key.isin("trivandrum", "trivandrm"), "Trivandrum")
         .when(city_key.isin("cmbt", "coimbatore"), "Coimbatore")
         .when(city_key.isin("hyd", "hyderabad"), "Hyderabad")
         .when(city_key.isin("poona", "pune"), "Pune")
         .when(city_key.isin("bangalore", "bengaluru"), "Bengaluru")
         .otherwise(F.initcap(F.trim(F.col("city"))))
    )
    .withColumn("customer_id", F.trim(F.col("customer_id")))
    .withColumn("customer_name", F.trim(F.col("customer_name")))
    .withColumn("status", F.lower(F.trim(F.col("status"))))
)
print("INVALID CUSTOMER RECORDS:")
invalid_customers.show(10, truncate=False)

print("CLEANED CUSTOMER RECORDS:")
clean_customers.show(10, truncate=False)

print("Customer count:", clean_customers.count())
print("Invalid customer count:", invalid_customers.count())

# --------------------------------------------------
# 4. TRANSACTION DATA CLEANING
# --------------------------------------------------

print("\n========== TRANSACTION DATA CLEANING ==========\n")

transaction_window = (
    Window.partitionBy(*TRANSACTION_COLS)
    .orderBy("_row_id")
)

transactions = (
    transactions_raw
    .select(*TRANSACTION_COLS)
    .withColumn("_row_id", F.monotonically_increasing_id())
    .withColumn("_duplicate_rank", F.row_number().over(transaction_window))
)

# Convert and validate transaction amount
transactions = transactions.withColumn(
    "_parsed_amount",
    F.trim(F.col("transaction_amount")).cast(DecimalType(18, 2))
)

# Safely parse dates in YYYY-MM-DD format
transactions = transactions.withColumn(
    "_parsed_timestamp",
    F.try_to_timestamp(
        F.trim(F.col("transaction_date")),
        F.lit("yyyy-MM-dd")
    )
)

transactions = transactions.withColumn(
    "_parsed_date",
    F.to_date(F.col("_parsed_timestamp"))
)

# Identify duplicate and invalid records
transaction_duplicate = F.col("_duplicate_rank") > 1

transaction_reason = F.concat_ws(
    "; ",
    F.when(
        F.col("transaction_id").isNull() |
        (F.trim(F.col("transaction_id")) == ""),
        "Missing transaction_id"
    ),
    F.when(
        F.col("customer_id").isNull() |
        (F.trim(F.col("customer_id")) == ""),
        "Missing customer_id"
    ),
    F.when(
        F.col("payment_mode").isNull() |
        (F.trim(F.col("payment_mode")) == ""),
        "Missing payment_mode"
    ),
    F.when(
        F.col("_parsed_amount").isNull() |
        (F.col("_parsed_amount") <= 0),
        "Invalid transaction_amount"
    ),
    F.when(
        F.col("_parsed_timestamp").isNull() |
        (
            F.date_format(
                F.col("_parsed_timestamp"), "yyyy-MM-dd"
            ) != F.trim(F.col("transaction_date"))
        ),
        "Invalid transaction_date"
    ),
    F.when(transaction_duplicate, "Duplicate record")
)

transactions = transactions.withColumn(
    "invalid_reason", transaction_reason
)

# Initially invalid transaction records
invalid_transactions_initial = (
    transactions
    .filter(F.col("invalid_reason") != "")
    .select(*TRANSACTION_COLS, "invalid_reason")
)

# Syntactically valid transactions
valid_transactions = transactions.filter(
    F.col("invalid_reason") == ""
)

# Prepare correctly typed columns
valid_transactions = (
    valid_transactions
    .withColumn("customer_id", F.trim(F.col("customer_id")))
    .withColumn("transaction_id", F.trim(F.col("transaction_id")))
    .withColumn("payment_mode", F.lower(F.trim(F.col("payment_mode"))))
    .withColumn("transaction_date", F.col("_parsed_date"))
    .withColumn("transaction_amount", F.col("_parsed_amount"))
)

# --------------------------------------------------
# 5. REFERENTIAL INTEGRITY VALIDATION
# --------------------------------------------------

print("\n========== REFERENTIAL INTEGRITY CHECK ==========\n")

valid_customer_ids = clean_customers.select("customer_id").distinct()

# Transactions with unknown customer IDs are invalid
unknown_customer_transactions = (
    valid_transactions
    .join(valid_customer_ids, on="customer_id", how="left_anti")
)

invalid_transactions_referential = (
    unknown_customer_transactions
    .select(
        *TRANSACTION_COLS,
        F.lit("Customer ID not found in cleaned customer dataset")
        .alias("invalid_reason")
    )
)

# Retain only transactions with valid customer references
clean_transactions = (
    valid_transactions
    .join(valid_customer_ids, on="customer_id", how="left_semi")
    .select(
        "transaction_id",
        "customer_id",
        "transaction_date",
        "transaction_amount",
        "payment_mode"
    )
)

# Combine all invalid transaction records
invalid_transactions = (
    invalid_transactions_initial
    .unionByName(invalid_transactions_referential)
)

print("INVALID TRANSACTION RECORDS:")
invalid_transactions.show(10, truncate=False)

print("CLEANED TRANSACTION RECORDS:")
clean_transactions.show(10, truncate=False)

print("Clean transaction count:", clean_transactions.count())
print("Invalid transaction count:", invalid_transactions.count())

# --------------------------------------------------
# 6. SAVE CLEANED AND INVALID DATA
# --------------------------------------------------

print("\n========== SAVING CLEANING OUTPUTS ==========\n")

def save_csv(dataframe, path):
    (
        dataframe.write
        .mode("overwrite")
        .option("header", True)
        .csv(path)
    )

save_csv(
    clean_customers,
    f"{OUTPUT}/cleaned_data/customers"
)

save_csv(
    clean_transactions,
    f"{OUTPUT}/cleaned_data/transactions"
)

save_csv(
    invalid_customers,
    f"{OUTPUT}/invalid_rows/customers"
)

save_csv(
    invalid_transactions,
    f"{OUTPUT}/invalid_rows/transactions"
)

# --------------------------------------------------
# 7. COLUMN TRANSFORMATIONS
# --------------------------------------------------

print("\n========== COLUMN TRANSFORMATIONS ==========\n")

transformed_transactions = (
    clean_transactions
    .withColumn(
        "transaction_category",
        F.when(
            F.col("transaction_amount") <= 2000, "Low"
        )
        .when(
            F.col("transaction_amount") <= 5000, "Medium"
        )
        .otherwise("High")
    )
    .withColumn(
        "transaction_month",
        F.month(F.col("transaction_date"))
    )
)

transformed_transactions.show(10, truncate=False)

# --------------------------------------------------
# 8. JOIN CUSTOMER AND TRANSACTION DATA
# --------------------------------------------------

print("\n========== CUSTOMER TRANSACTION JOIN ==========\n")

joined_data = (
    transformed_transactions
    .join(clean_customers, on="customer_id", how="inner")
)

print("JOINED DATA:")
joined_data.show(10, truncate=False)

print("Joined record count:", joined_data.count())

save_csv(
    joined_data,
    f"{OUTPUT}/joined_data"
)

# --------------------------------------------------
# 9. AGGREGATIONS
# --------------------------------------------------

print("\n========== AGGREGATIONS ==========\n")

# Total and average transaction amount per customer
customer_summary = (
    joined_data
    .groupBy("customer_id", "customer_name")
    .agg(
        F.sum("transaction_amount").alias("total_amount"),
        F.avg("transaction_amount").alias("average_amount")
    )
    .orderBy(F.desc("total_amount"))
)

print("CUSTOMER SUMMARY:")
customer_summary.show(10, truncate=False)

# Total transaction amount per city
city_summary = (
    joined_data
    .groupBy("city")
    .agg(
        F.sum("transaction_amount").alias("total_amount")
    )
    .orderBy(F.desc("total_amount"))
)

print("CITY SUMMARY:")
city_summary.show(10, truncate=False)

# Top three customers by total transaction amount
top_three_customers = customer_summary.limit(3)

print("TOP THREE CUSTOMERS:")
top_three_customers.show(3, truncate=False)

# Save aggregation outputs
save_csv(
    customer_summary,
    f"{OUTPUT}/aggregates/customer_summary"
)

save_csv(
    city_summary,
    f"{OUTPUT}/aggregates/city_summary"
)

save_csv(
    top_three_customers,
    f"{OUTPUT}/aggregates/top_3_customers"
)

# --------------------------------------------------
# 10. FINAL VERIFICATION
# --------------------------------------------------

print("\n========== FINAL OUTPUT SUMMARY ==========\n")

print("Clean customers:", clean_customers.count())
print("Invalid customers:", invalid_customers.count())
print("Clean transactions:", clean_transactions.count())
print("Invalid transactions:", invalid_transactions.count())
print("Joined records:", joined_data.count())

print("\nAll outputs saved successfully to:")
print(f"{OUTPUT}/")

print("\n========== WEEK 4 JOB COMPLETED ==========\n")

spark.stop()
