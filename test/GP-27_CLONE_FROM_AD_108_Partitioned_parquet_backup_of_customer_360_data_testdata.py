# PySpark script for Databricks customer_360_raw backup and vacuum operation
# Purpose: Generate comprehensive test data for customer_360_raw table and test backup/vacuum logic
# Author: Giang Nguyen
# Date: 2025-07-22
# Description: This script generates diverse test data for the customer_360_raw table (including happy path, edge cases, errors, NULLs, and special characters), writes it as partitioned/parquet/snappy files, and runs an example of the vacuum logic, with robust commented documentation. All file I/O is safely handled, and all data types match Databricks conventions.

# Imports for schema and PySpark DataFrame operations
from pyspark.sql import Row  
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType, LongType, TimestampType, DateType
)  # built-in
from pyspark.sql import functions as F  

import datetime  

# Sample test data generation

def get_sample_customer_360_raw_data():
    """
    Generate diverse test data for customer_360_raw table.
    Covers happy path, edge, error, NULL, and special character scenarios.

    Returns:
        list of dict: Each dict is a test row.
    """
    now = datetime.datetime(2024, 6, 30, 12, 0, 0)
    # Helper for timestamps
    def ts(dt):
        # Match Databricks timestamp standard: 'yyyy-MM-dd\'T\'HH:mm:ss.SSS+0000'
        return dt.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + '+0000'
    # Basic test set
    test_data = [
        # Happy path, typical data
        {
            "customer_id": 1,  # BIGINT
            "name": "Alice Smith",  # STRING
            "email": "alice.smith@email.com",  # STRING
            "state": "CA",  # STRING, partition col
            "zip": "94105",  # STRING
            "balance": 1050.75,  # DOUBLE
            "credit_score": 720,  # INTEGER
            "signup_date": "2021-01-15",  # STRING (could be DateType)
            "last_purchase": ts(now - datetime.timedelta(days=1)),  # TIMESTAMP
            "updated_at": "2024-06-29 18:00:00",  # STRING as source format
        },
        # Edge case: earliest allowed updated_at (boundary for vacuum)
        {
            "customer_id": 2,
            "name": "Bob Zhang",
            "email": "bob.zhang@email.com",
            "state": "TX",
            "zip": "73301",
            "balance": 0.0,
            "credit_score": 0,  # Minimum
            "signup_date": "2022-12-31",
            "last_purchase": ts(now - datetime.timedelta(days=30)),
            "updated_at": "2024-05-31 12:00:00",  # Exactly 30 days ago
        },
        # Edge: future updated_at, high balance
        {
            "customer_id": 3,
            "name": "Clara O’Hara",
            "email": "clara.ohara@email.com",
            "state": "NY",
            "zip": "10001",
            "balance": 1000000.99,
            "credit_score": 850,  # Max in standard score
            "signup_date": "2023-06-15",
            "last_purchase": ts(now + datetime.timedelta(days=1)),  # in future
            "updated_at": "2024-07-02 11:59:59",
        },
        # Edge: multi-byte char, special char, and NULL email
        {
            "customer_id": 4,
            "name": "Nguyễn Văn ☺",
            "email": None,
            "state": "WA",
            "zip": "98052",
            "balance": 502.4,
            "credit_score": 640,
            "signup_date": "2022-08-05",
            "last_purchase": ts(now - datetime.timedelta(hours=12)),
            "updated_at": "2024-06-30 02:30:00",
        },
        # Error: Invalid credit_score / negative balance
        {
            "customer_id": 5,
            "name": "Invalid User",
            "email": "bad@example.com",
            "state": "IL",
            "zip": "60601",
            "balance": -100.0,  # Out-of-range
            "credit_score": -20,
            "signup_date": "2020-05-12",
            "last_purchase": ts(now - datetime.timedelta(days=60)),
            "updated_at": "2024-05-10 09:00:00",  # Older than 30 days
        },
        # Error: invalid updated_at format
        {
            "customer_id": 6,
            "name": "Mismatch Date",
            "email": "mix@error.com",
            "state": "NV",
            "zip": "89501",
            "balance": 300.50,
            "credit_score": 600,
            "signup_date": "2023-03-22",
            "last_purchase": ts(now - datetime.timedelta(days=45)),
            "updated_at": "2024/05/31T12:00Z",  # Bad format error test
        },
        # Error: NULL updated_at
        {
            "customer_id": 7,
            "name": "Null Time",
            "email": "nullerror@email.com",
            "state": "FL",
            "zip": "33101",
            "balance": 80,
            "credit_score": 450,
            "signup_date": "2021-10-10",
            "last_purchase": ts(now - datetime.timedelta(days=100)),
            "updated_at": None,
        },
        # Edge: MAX values
        {
            "customer_id": 9223372036854775807,
            "name": "Big Id",
            "email": "maxid@big.com",
            "state": "TX",
            "zip": "79999",
            "balance": 1.7976931348623157e+308,  # max double
            "credit_score": 850,
            "signup_date": "1900-01-01",
            "last_purchase": ts(now),
            "updated_at": "2024-06-30 12:00:00",
        },
        # Edge: unicode, emoji
        {
            "customer_id": 8,
            "name": "Emoji 👩‍💻🦄",
            "email": "emoji@fun.com",
            "state": "HI",
            "zip": "96801",
            "balance": 2000,
            "credit_score": 700,
            "signup_date": "2022-11-23",
            "last_purchase": ts(now),
            "updated_at": "2024-06-30 06:30:00",
        },
        # Edge: blanks, special char in zip, weird state
        {
            "customer_id": 9,
            "name": "",
            "email": "",
            "state": "ZZ",  # unknown/fake state
            "zip": "!!!!!!",
            "balance": 9.99,
            "credit_score": 500,
            "signup_date": "",
            "last_purchase": ts(now - datetime.timedelta(days=2)),
            "updated_at": "",  # blank string
        },
        # Happy path: another region
        {
            "customer_id": 10,
            "name": "Juan Pérez",
            "email": "juan.perez@correo.mx",
            "state": "NM",
            "zip": "87501",
            "balance": 333.33,
            "credit_score": 710,
            "signup_date": "2024-01-01",
            "last_purchase": ts(now - datetime.timedelta(days=20)),
            "updated_at": "2024-06-15 08:30:20",
        },
        # Error: missing state (required partition col)
        {
            "customer_id": 11,
            "name": "Missing State",
            "email": "nostate@fail.com",
            "state": None,
            "zip": "00000",
            "balance": 120,
            "credit_score": 555,
            "signup_date": "2022-02-02",
            "last_purchase": ts(now),
            "updated_at": "2024-06-10 17:00:00",
        },
        # Edge: special char in name (quotes, escapes)
        {
            "customer_id": 12,
            "name": "O'Reilly \"The 3rd\"",
            "email": "oreilly3@pub.com",
            "state": "CA",
            "zip": "90210",
            "balance": 7200,
            "credit_score": 710,
            "signup_date": "2020-10-10",
            "last_purchase": ts(now - datetime.timedelta(days=60)),
            "updated_at": "2024-06-01 10:15:35",
        },
        # More happy/edge path rows...
    ]
    # Pad to at least 22 test records, with formulaic variants for further coverage:
    base_states = ["CA", "TX", "NY", "WA"]
    for i in range(13, 23):
        test_data.append({
            "customer_id": i,
            "name": f"User {i}",
            "email": f"test{i}@test.com",
            "state": base_states[i % len(base_states)],
            "zip": f"{90000 + i}",
            "balance": 100.0 * (i % 5),
            "credit_score": 700 + (i % 3) * 10,
            "signup_date": f"2023-0{(i%9)+1}-01",
            "last_purchase": ts(now - datetime.timedelta(days=i)),
            "updated_at": (now - datetime.timedelta(days=i*2)).strftime('%Y-%m-%d %H:%M:%S'),
        })
    return test_data

# Define customer_360_raw schema for Unity catalog consistency
customer_360_raw_schema = StructType([
    StructField("customer_id", LongType(), False),
    StructField("name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("state", StringType(), True),
    StructField("zip", StringType(), True),
    StructField("balance", DoubleType(), True),
    StructField("credit_score", IntegerType(), True),
    StructField("signup_date", StringType(), True),  # Could be DateType but using STRING to match sample
    StructField("last_purchase", StringType(), True),  # Store as string for cross-format test; could be TIMESTAMP
    StructField("updated_at", StringType(), True),  # Important for backup/vacuum - time as STRING
])

def create_test_customer_360_raw_dataframe(spark):
    """
    Create DataFrame of test customer_360_raw data with proper schema.

    Args:
        spark (SparkSession): Databricks SparkSession (provided).

    Returns:
        DataFrame: Test data as per customer_360_raw_schema.
    """
    sample = get_sample_customer_360_raw_data()
    rows = [Row(**row) for row in sample]
    df = spark.createDataFrame(rows, schema=customer_360_raw_schema)
    return df

# Create sample test DataFrame (simulate loading from Unity catalog)
customer_360_raw_test_df = create_test_customer_360_raw_dataframe(spark)

# Sample backup Parquet write (partitioned by 'state', compressed by snappy)
try:
    # Run backup, with all required options
    customer_360_raw_test_df.write.mode("overwrite") \
        .partitionBy("state") \
        .option("compression", "snappy") \
        .parquet("/Volumes/customer_360_raw_backup")
    print("Backup completed successfully.")
except Exception as e:
    print(f"Backup error: {e}")

# -- Validation: Check that Parquet output rowcount matches input (simulate partition validation)
try:
    backup_df = spark.read.parquet("/Volumes/customer_360_raw_backup")
    source_count = customer_360_raw_test_df.count()
    backup_count = backup_df.count()
    assert backup_count == source_count, f"Row count mismatch: source={source_count}, backup={backup_count}"
    print("Validation passed: row counts match.")
except Exception as e:
    print(f"Backup validation error: {e}")

def vacuum_customer_360_raw_table(retention_days=30):
    """
    Simulate a vacuum (delete) operation to remove records older than given retention.

    Args:
        retention_days (int): Retain only records updated in last `retention_days`.

    Returns:
        DataFrame: Filtered DataFrame retaining only recent records.
    """
    # Keep rows with valid updated_at only, and in correct format
    cutoff = datetime.datetime(2024, 6, 30, 12, 0, 0) - datetime.timedelta(days=retention_days)
    cutoff_str = cutoff.strftime('%Y-%m-%d %H:%M:%S')
    df = customer_360_raw_test_df.withColumn(
        "updated_at_ts",
        F.to_timestamp("updated_at", "yyyy-MM-dd HH:mm:ss")
    )
    # filter: updated_at >= cutoff
    filtered = df.filter(
        (F.col("updated_at_ts").isNotNull()) &
        (F.col("updated_at_ts") >= F.lit(cutoff_str))
    )
    return filtered

# -- Run vacuum simulation: delete records older than 2024-05-31 12:00:00
old_count = customer_360_raw_test_df.count()
vacuumed_df = vacuum_customer_360_raw_table(retention_days=30)
remaining_count = vacuumed_df.count()
print(f"Vacuum simulated: {old_count-remaining_count} records would be deleted; {remaining_count} remain.")

# -- Edge case: test error handling for partition column missing
def test_missing_partition_column():
    """
    Test backup error when partition column 'state' is missing.

    Raises:
        Exception: To simulate PARTITION_COLUMN_MISSING error.
    """
    temp_schema = StructType([f for f in customer_360_raw_schema if f.name != "state"])
    temp_df = spark.createDataFrame(
        customer_360_raw_test_df.drop("state").rdd, schema=temp_schema
    )
    try:
        temp_df.write.partitionBy("state").parquet("/Volumes/customer_360_raw_backup_error")
    except Exception as e:
        print(f"Expected error when partitionBy column missing: {e}")

# -- Uncomment to test missing partition column error scenario
# test_missing_partition_column()

# -- Edge case: test error handling for bad updated_at format during vacuum
def test_bad_updated_at_format_vacuum():
    """
    Test vacuum error when 'updated_at' has illegal format.

    Raises:
        ValueError: On format parsing error.
    """
    # Simulate
    try:
        df = customer_360_raw_test_df.withColumn(
            "updated_at_ts",
            F.to_timestamp("updated_at", "yyyy-MM-dd HH:mm:ss")
        )
        bad_df = df.filter(
            F.col("updated_at_ts").isNull() & (F.col("updated_at").isNotNull())
        )
        bad_rows = bad_df.select("customer_id", "updated_at").collect()
        if bad_rows:
            raise ValueError("INVALID_DATETIME_FORMAT: 'updated_at' is not yyyy-MM-dd HH:mm:ss")
        print("All 'updated_at' values are valid.")
    except Exception as e:
        print(f"Vacuum format error: {e}")

# -- Run validation for format error
test_bad_updated_at_format_vacuum()

# -- NULL updated_at error test for vacuum
def test_null_updated_at_vacuum():
    """
    Test vacuum error when 'updated_at' is NULL.

    Raises:
        Exception: On NULL found in 'updated_at'.
    """
    try:
        null_rows = customer_360_raw_test_df.filter(F.col("updated_at").isNull()).count()
        if null_rows > 0:
            raise Exception("NULL_DATETIME_ERROR: 'updated_at' field is null")
        print("No NULL 'updated_at' field found.")
    except Exception as e:
        print(f"Vacuum NULL error: {e}")

test_null_updated_at_vacuum()
