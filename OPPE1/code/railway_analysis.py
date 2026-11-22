from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# --------------------------------------------------
# 1. Configuration
# --------------------------------------------------

BUCKET = "week6-dataproc-2026-project-df614751"

INPUT_PATH = f"gs://{BUCKET}/OPPE1/input/railway_data.csv"
OUTPUT_PATH = f"gs://{BUCKET}/OPPE1/output/station_aggregates"

spark = SparkSession.builder \
    .appName("Railway-Stop-Analysis-OPPE1") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# --------------------------------------------------
# 2. Read CSV from Google Cloud Storage
# --------------------------------------------------

df = spark.read \
    .option("header", True) \
    .option("inferSchema", False) \
    .option("mode", "PERMISSIVE") \
    .csv(INPUT_PATH)

print("\n========== INPUT DATA ==========")
print("Total input records:", df.count())
df.printSchema()

# --------------------------------------------------
# 3. Handle missing and malformed records
# --------------------------------------------------

required = [
    "Train No", "Station Code", "Station Name",
    "Arrival time", "Departure Time"
]

valid_required = F.lit(True)

for c in required:
    valid_required = valid_required & (
        F.col(c).isNotNull() &
        (F.trim(F.col(c)) != "")
    )

clean = df.filter(valid_required)

# Strict HH:mm:ss validation
time_pattern = r"^(?:[01]\d|2[0-3]):[0-5]\d:[0-5]\d$"

clean = clean.withColumn(
    "arrival_valid",
    F.col("Arrival time").rlike(time_pattern) &
    (F.col("Arrival time") != "00:00:00")
).withColumn(
    "departure_valid",
    F.col("Departure Time").rlike(time_pattern) &
    (F.col("Departure Time") != "00:00:00")
)

# A valid stoppage requires both arrival and departure.
stops = clean.filter(
    F.col("arrival_valid") & F.col("departure_valid")
)

print("\n========== DATA QUALITY ==========")
print("Records with required fields:", clean.count())
print("Valid stoppages:", stops.count())
print("Excluded records:", df.count() - stops.count())

# --------------------------------------------------
# 4. Calculate stop duration in minutes
# --------------------------------------------------

stops = stops.withColumn(
    "arrival_sec",
    F.unix_timestamp(F.col("Arrival time"), "HH:mm:ss")
).withColumn(
    "departure_sec",
    F.unix_timestamp(F.col("Departure Time"), "HH:mm:ss")
)

# Handle overnight time differences.
stops = stops.withColumn(
    "duration_sec",
    F.when(
        F.col("departure_sec") >= F.col("arrival_sec"),
        F.col("departure_sec") - F.col("arrival_sec")
    ).otherwise(
        F.col("departure_sec") - F.col("arrival_sec") + 86400
    )
)

stops = stops.withColumn(
    "stop_duration_min",
    F.col("duration_sec") / 60.0
)

# --------------------------------------------------
# SECTION A: STOP DURATION STATISTICS
# --------------------------------------------------

station_stats = stops.groupBy(
    "Station Code", "Station Name"
).agg(
    F.count("*").alias("stoppage_count"),
    F.min("stop_duration_min").alias("min_stop_min"),
    F.max("stop_duration_min").alias("max_stop_min"),
    F.avg("stop_duration_min").alias("avg_stop_min"),
    F.expr(
        "percentile_approx(stop_duration_min, 0.90, 10000)"
    ).alias("p90_stop_min"),
    F.sum("stop_duration_min").alias("total_stop_min")
)

print("\n========== SECTION A: STATION STATISTICS ==========")

station_stats.orderBy(
    F.desc("stoppage_count")
).show(20, truncate=False)

print("\n========== TOP 5 STATIONS BY STOPPAGES ==========")

top5 = station_stats.orderBy(
    F.desc("stoppage_count"),
    F.asc("Station Code")
).limit(5)

top5.show(5, truncate=False)

# --------------------------------------------------
# SECTION B: STATION LOAD ANALYSIS
# --------------------------------------------------

print("\n========== SECTION B: TOTAL AND AVERAGE DURATION ==========")

station_stats.select(
    "Station Code",
    "Station Name",
    "total_stop_min",
    "avg_stop_min"
).orderBy(
    F.desc("total_stop_min")
).show(20, truncate=False)

print("\n========== STATIONS WITH MAX > 3 TIMES AVERAGE ==========")

outliers = station_stats.filter(
    F.col("max_stop_min") > 3 * F.col("avg_stop_min")
)

outliers.orderBy(
    F.desc("max_stop_min")
).show(50, truncate=False)

# --------------------------------------------------
# 5. Two-hour sliding window for busiest station
# --------------------------------------------------

top_station = top5.first()

top_station_code = top_station["Station Code"]
top_station_name = top_station["Station Name"]

print("\n========== TWO-HOUR SLIDING WINDOW ==========")
print("Selected station:", top_station_code, top_station_name)

arrivals = stops.filter(
    (F.col("Station Code") == top_station_code) &
    (F.col("arrival_valid"))
).select(
    "Train No",
    "arrival_sec"
)

# Duplicate each day's events at +24 hours.
# This handles windows crossing midnight.
original = arrivals.withColumn("shifted", F.lit(False))

shifted = arrivals.withColumn(
    "arrival_sec", F.col("arrival_sec") + 86400
).withColumn(
    "shifted", F.lit(True)
)

events = original.unionByName(shifted)

window_2h = Window.orderBy(
    "arrival_sec"
).rangeBetween(-7200, 0)

window_counts = events.withColumn(
    "trains_in_2h",
    F.count("*").over(window_2h)
)

max_arrivals_2h = window_counts.filter(
    F.col("shifted")
).agg(
    F.max("trains_in_2h").alias("max_trains_2h")
).first()["max_trains_2h"]

print("Maximum trains arriving in any 2-hour window:",
      max_arrivals_2h)

# --------------------------------------------------
# 6. Prepare final aggregated output
# --------------------------------------------------

final_result = station_stats.withColumn(
    "is_outlier",
    F.col("max_stop_min") > 3 * F.col("avg_stop_min")
).withColumn(
    "two_hour_window_station",
    F.lit(top_station_code)
).withColumn(
    "max_trains_in_2h",
    F.when(
        F.col("Station Code") == top_station_code,
        F.lit(max_arrivals_2h)
    )
)

# --------------------------------------------------
# 7. Save aggregated CSV to GCS
# --------------------------------------------------

final_result.coalesce(1).write \
    .mode("overwrite") \
    .option("header", True) \
    .csv(OUTPUT_PATH)

print("\n========== OUTPUT SAVED ==========")
print("GCS location:", OUTPUT_PATH)

print("\nRailway analysis completed successfully.")

spark.stop()
