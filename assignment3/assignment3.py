from pyspark.sql import SparkSession
from pyspark.sql.functions import hour, when, col

# ---------------------------------------------------------
# Assignment 3 - Click Counts by Time of Day
# RDD and DataFrame implementations
# ---------------------------------------------------------

# GCS input and output locations
INPUT_PATH = "gs://assignment-2-week3-abhirudraa-2026/assignment3/input/click_file.txt"
OUTPUT_PATH = "gs://assignment-2-week3-abhirudraa-2026/assignment3/output"

# Create Spark session
spark = SparkSession.builder \
    .appName("Assignment3_ClickCounts") \
    .getOrCreate()

sc = spark.sparkContext

# =========================================================
# PART 1 - RDD IMPLEMENTATION
# =========================================================

# Read the input file
raw_rdd = sc.textFile(INPUT_PATH)

# Remove header
header = raw_rdd.first()

data_rdd = raw_rdd.filter(lambda line: line != header)


# Function to determine time-of-day bin
def get_time_bin(timestamp):
    hour_value = int(timestamp.split(" ")[1].split(":")[0])

    if 0 <= hour_value < 6:
        return "0-6"
    elif 6 <= hour_value < 12:
        return "6-12"
    elif 12 <= hour_value < 18:
        return "12-18"
    else:
        return "18-24"


# Convert each record into (time_bin, 1)
rdd_bins = data_rdd.map(
    lambda line: (
        get_time_bin(line.split(",", 1)[1]),
        1
    )
)

# Count clicks in each time bin
rdd_counts = rdd_bins.reduceByKey(lambda a, b: a + b)

# Sort results by the beginning of the time interval
bin_order = {
    "0-6": 0,
    "6-12": 1,
    "12-18": 2,
    "18-24": 3
}

rdd_sorted = rdd_counts.sortBy(
    lambda x: bin_order[x[0]]
)

rdd_results = rdd_sorted.collect()


# =========================================================
# PART 2 - DATAFRAME IMPLEMENTATION
# =========================================================

# Read CSV file using Spark DataFrame
df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv(INPUT_PATH)

# Extract hour from timestamp
df_with_hour = df.withColumn(
    "hour",
    hour(col("timestamp"))
)

# Create time-of-day bins
df_with_bin = df_with_hour.withColumn(
    "time_bin",
    when((col("hour") >= 0) & (col("hour") < 6), "0-6")
    .when((col("hour") >= 6) & (col("hour") < 12), "6-12")
    .when((col("hour") >= 12) & (col("hour") < 18), "12-18")
    .otherwise("18-24")
)

# Count clicks in each time bin
df_counts = df_with_bin.groupBy("time_bin") \
    .count()

# Add sorting column and sort the results
df_sorted = df_counts.withColumn(
    "sort_order",
    when(col("time_bin") == "0-6", 0)
    .when(col("time_bin") == "6-12", 1)
    .when(col("time_bin") == "12-18", 2)
    .otherwise(3)
).orderBy("sort_order")

df_results = df_sorted.select(
    "time_bin",
    "count"
).collect()


# =========================================================
# DISPLAY RESULTS
# =========================================================

print("=" * 50)
print("RDD IMPLEMENTATION RESULTS")
print("=" * 50)

for time_bin, count in rdd_results:
    print(f"{time_bin}: {count} clicks")


print()
print("=" * 50)
print("DATAFRAME IMPLEMENTATION RESULTS")
print("=" * 50)

for row in df_results:
    print(f"{row['time_bin']}: {row['count']} clicks")


# =========================================================
# WRITE OUTPUT
# =========================================================

output_lines = [
    "Assignment 3 - Click Counts by Time of Day",
    "",
    "RDD Implementation:",
]

for time_bin, count in rdd_results:
    output_lines.append(f"{time_bin}: {count} clicks")

output_lines.extend([
    "",
    "DataFrame Implementation:"
])

for row in df_results:
    output_lines.append(
        f"{row['time_bin']}: {row['count']} clicks"
    )

output_rdd = sc.parallelize(output_lines)

output_rdd.coalesce(1).saveAsTextFile(OUTPUT_PATH)

print()
print("Output successfully written to:")
print(OUTPUT_PATH)

# Stop Spark
spark.stop()
