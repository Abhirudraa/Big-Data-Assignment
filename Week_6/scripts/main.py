
import logging
from pyspark.sql import SparkSession

# --------------------------------------------------
# 1. INITIALIZE SPARK
# --------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s: %(message)s"
)

logger = logging.getLogger("Week6-Dataproc-SparkSQL")

spark = (
    SparkSession.builder
    .appName("Week6-Customer-Analytics-SCD")
    .getOrCreate()
)

BUCKET = "gs://week6-dataproc-2026-project-df614751"
INPUT = f"{BUCKET}/input"
OUTPUT = f"{BUCKET}/output"

logger.info("Starting Week 6 SparkSQL assignment")
logger.info("Input location: %s", INPUT)
logger.info("Output location: %s", OUTPUT)


# --------------------------------------------------
# 2. LOAD INPUT FILES USING SPARK SQL
# --------------------------------------------------

spark.sql(f"""
CREATE OR REPLACE TEMP VIEW transactions
USING csv
OPTIONS (
    path '{INPUT}/transactions.csv',
    header 'true',
    inferSchema 'true'
)
""")

spark.sql(f"""
CREATE OR REPLACE TEMP VIEW customer_master
USING csv
OPTIONS (
    path '{INPUT}/customer_master.csv',
    header 'true',
    inferSchema 'true'
)
""")

spark.sql(f"""
CREATE OR REPLACE TEMP VIEW customer_updates
USING csv
OPTIONS (
    path '{INPUT}/customer_updates.csv',
    header 'true',
    inferSchema 'true'
)
""")

logger.info("All three input datasets loaded successfully")


# --------------------------------------------------
# 3. AGGREGATION QUERY 1:
# TOP 5 CUSTOMERS BY TRANSACTION AMOUNT
# --------------------------------------------------

top_customers_sql = """
SELECT
    t.customer_id,
    m.customer_name,
    SUM(t.transaction_amount) AS total_transaction_amount
FROM transactions t
LEFT JOIN customer_master m
    ON t.customer_id = m.customer_id
GROUP BY t.customer_id, m.customer_name
ORDER BY total_transaction_amount DESC
LIMIT 5
"""

logger.info("TOP 5 CUSTOMERS BY TOTAL TRANSACTION AMOUNT")
spark.sql(top_customers_sql).show(100, False)

spark.sql(f"""
INSERT OVERWRITE DIRECTORY '{OUTPUT}/top_5_customers'
USING csv
OPTIONS (header 'true')
{top_customers_sql}
""")


# --------------------------------------------------
# 4. AGGREGATION QUERY 2:
# NUMBER OF CUSTOMERS PER CITY
# --------------------------------------------------

customers_per_city_sql = """
SELECT
    city,
    COUNT(DISTINCT customer_id) AS customer_count
FROM customer_master
GROUP BY city
ORDER BY customer_count DESC, city
"""

logger.info("NUMBER OF CUSTOMERS PER CITY")
spark.sql(customers_per_city_sql).show(100, False)

spark.sql(f"""
INSERT OVERWRITE DIRECTORY '{OUTPUT}/customers_per_city'
USING csv
OPTIONS (header 'true')
{customers_per_city_sql}
""")


# --------------------------------------------------
# 5. AGGREGATION QUERY 3:
# OVERALL AVERAGE TRANSACTION VALUE
# --------------------------------------------------

average_transaction_sql = """
SELECT
    COUNT(*) AS total_transactions,
    ROUND(AVG(transaction_amount), 2)
        AS average_transaction_value
FROM transactions
"""

logger.info("OVERALL AVERAGE TRANSACTION VALUE")
spark.sql(average_transaction_sql).show(100, False)

spark.sql(f"""
INSERT OVERWRITE DIRECTORY '{OUTPUT}/average_transaction_value'
USING csv
OPTIONS (header 'true')
{average_transaction_sql}
""")


# --------------------------------------------------
# 6. SCD TYPE I - BEFORE STATE
# --------------------------------------------------

logger.info("SCD TYPE I - BEFORE UPDATE")

scd1_before_sql = """
SELECT
    customer_id,
    customer_name,
    city,
    dob,
    effective_date,
    expiry_date,
    current_flag
FROM customer_master
ORDER BY customer_id
"""

spark.sql(scd1_before_sql).show(100, False)

spark.sql(f"""
INSERT OVERWRITE DIRECTORY '{OUTPUT}/scd_type_1/before'
USING csv
OPTIONS (header 'true')
{scd1_before_sql}
""")


# --------------------------------------------------
# 7. SCD TYPE I - AFTER STATE
# OVERWRITE CHANGED CUSTOMER ATTRIBUTES
# --------------------------------------------------

spark.sql("""
CREATE OR REPLACE TEMP VIEW scd1_after AS
SELECT
    m.customer_id,
    COALESCE(u.customer_name, m.customer_name)
        AS customer_name,
    COALESCE(u.city, m.city)
        AS city,
    COALESCE(u.dob, m.dob)
        AS dob,
    m.effective_date,
    m.expiry_date,
    m.current_flag
FROM customer_master m
LEFT JOIN customer_updates u
    ON m.customer_id = u.customer_id
""")

scd1_after_sql = """
SELECT *
FROM scd1_after
ORDER BY customer_id
"""

logger.info("SCD TYPE I - AFTER UPDATE")
spark.sql(scd1_after_sql).show(100, False)

spark.sql(f"""
INSERT OVERWRITE DIRECTORY '{OUTPUT}/scd_type_1/after'
USING csv
OPTIONS (header 'true')
{scd1_after_sql}
""")


# --------------------------------------------------
# 8. SCD TYPE II - BEFORE STATE
# --------------------------------------------------

logger.info("SCD TYPE II - BEFORE UPDATE")

scd2_before_sql = """
SELECT
    customer_id,
    customer_name,
    city,
    dob,
    effective_date,
    expiry_date,
    current_flag
FROM customer_master
ORDER BY customer_id
"""

spark.sql(scd2_before_sql).show(100, False)

spark.sql(f"""
INSERT OVERWRITE DIRECTORY '{OUTPUT}/scd_type_2/before'
USING csv
OPTIONS (header 'true')
{scd2_before_sql}
""")


# --------------------------------------------------
# 9. SCD TYPE II - CREATE HISTORICAL VERSIONS
# EXPIRE OLD RECORDS AND INSERT NEW VERSIONS
# --------------------------------------------------

spark.sql("""
CREATE OR REPLACE TEMP VIEW scd2_after AS

-- Preserve old records.
-- Changed records are expired on the day before the update.

SELECT
    m.customer_id,
    m.customer_name,
    m.city,
    CAST(m.dob AS STRING) AS dob,
    TO_DATE(CAST(m.effective_date AS STRING))
        AS effective_date,

    CASE
        WHEN u.customer_id IS NOT NULL
         AND NOT (
             m.customer_name <=> u.customer_name
             AND m.city <=> u.city
             AND CAST(m.dob AS STRING) <=> CAST(u.dob AS STRING)
         )
        THEN DATE_SUB(TO_DATE(CAST(u.change_date AS STRING)), 1)
        ELSE TO_DATE(CAST(m.expiry_date AS STRING))
    END AS expiry_date,

    CASE
        WHEN u.customer_id IS NOT NULL
         AND NOT (
             m.customer_name <=> u.customer_name
             AND m.city <=> u.city
             AND CAST(m.dob AS STRING) <=> CAST(u.dob AS STRING)
         )
        THEN 0
        ELSE CAST(m.current_flag AS INT)
    END AS current_flag

FROM customer_master m
LEFT JOIN customer_updates u
    ON m.customer_id = u.customer_id

UNION ALL

-- Insert new versions for changed customers.

SELECT
    u.customer_id,
    u.customer_name,
    u.city,
    CAST(u.dob AS STRING) AS dob,
    TO_DATE(CAST(u.change_date AS STRING)) AS effective_date,
    CAST(NULL AS DATE) AS expiry_date,
    1 AS current_flag

FROM customer_updates u
INNER JOIN customer_master m
    ON u.customer_id = m.customer_id

WHERE NOT (
    m.customer_name <=> u.customer_name
    AND m.city <=> u.city
    AND CAST(m.dob AS STRING) <=> CAST(u.dob AS STRING)
)
""")

scd2_after_sql = """
SELECT *
FROM scd2_after
ORDER BY customer_id, effective_date
"""

logger.info("SCD TYPE II - AFTER UPDATE WITH HISTORY")
spark.sql(scd2_after_sql).show(100, False)

spark.sql(f"""
INSERT OVERWRITE DIRECTORY '{OUTPUT}/scd_type_2/after'
USING csv
OPTIONS (header 'true')
{scd2_after_sql}
""")


# --------------------------------------------------
# 10. VALIDATION QUERIES
# --------------------------------------------------

logger.info("SCD TYPE II VALIDATION - CURRENT RECORD COUNTS")

spark.sql("""
SELECT
    current_flag,
    COUNT(*) AS record_count
FROM scd2_after
GROUP BY current_flag
ORDER BY current_flag
""").show(100, False)

logger.info("SCD TYPE II VALIDATION - CUSTOMER HISTORY")

spark.sql("""
SELECT
    customer_id,
    COUNT(*) AS versions,
    SUM(CASE WHEN current_flag = 1 THEN 1 ELSE 0 END)
        AS current_versions
FROM scd2_after
GROUP BY customer_id
ORDER BY customer_id
""").show(100, False)


# --------------------------------------------------
# 11. JOB COMPLETION
# --------------------------------------------------

logger.info("All aggregation and SCD queries completed")
logger.info("All output CSV datasets saved to GCS")
logger.info("Output location: %s", OUTPUT)

spark.stop()
