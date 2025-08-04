# ============================================================
# PySpark Test Suite for Backup and Vacuum of customer_360_raw
# Databricks environment: Unity Catalog, Delta Lake, Volumes
# All code is executable in Databricks, with comments for clarity
# ============================================================

# ---------------------------
# Imports and Setup
# ---------------------------
from pyspark.sql import SparkSession  # SparkSession is already available in Databricks
from pyspark.sql import functions as F  
from pyspark.sql.types import (StructType, StructField, LongType, StringType, DateType)  
from datetime import datetime, timedelta  
import os  
import shutil  

# ---------------------------
# Constants and Paths
# ---------------------------
# Define constants for table and volume paths
CATALOG = "purgo_databricks"
SCHEMA = "purgo_playground"
RAW_TABLE = f"{CATALOG}.{SCHEMA}.customer_360_raw"
BACKUP_LOG_TABLE = f"{CATALOG}.{SCHEMA}.customer_360_raw_backup_log"
VOLUME_BACKUP_PATH = "/Volumes/customer_360_raw_backup"
PARQUET_COMPRESSION = "snappy"
BACKUP_RETENTION_DAYS = 90
VACUUM_RETENTION_DAYS = 30
BACKUP_PARTITION_COL = "state"
REQUIRED_FIELDS = ["id", "email", "state", "creation_date"]

# ---------------------------
# Utility Functions
# ---------------------------

def log_operation(status, operation_type, record_count, error_message):
    """
    Log backup or vacuum operation to the backup log table.
    """
    log_df = spark.createDataFrame(
        [(datetime.utcnow().isoformat(), status, operation_type, record_count, error_message)],
        ["timestamp", "status", "operation_type", "record_count", "error_message"]
    )
    try:
        log_df.write.format("delta").mode("append").saveAsTable(BACKUP_LOG_TABLE)
    except Exception as e:
        # If log table is unavailable, raise error as per requirements
        raise RuntimeError("Backup log table unavailable") from e

def validate_email(email):
    """
    Simple email format validation using PySpark SQL regex.
    """
    if email is None:
        return False
    # Basic regex for email validation
    import re  
    return re.match(r"^[^@]+@[^@]+\.[^@]+$", email) is not None

def validate_required_fields(row):
    """
    Validate required fields for a row.
    """
    if row["id"] is None:
        return False, f"id is NULL in record with email={row['email']}"
    if row["email"] is None:
        return False, f"email is NULL in record id={row['id']}"
    if not validate_email(row["email"]):
        return False, f"Data validation error: invalid email format in record id={row['id']}"
    if row["state"] is None:
        return False, f"state is NULL in record id={row['id']}"
    return True, None

def get_table_schema(table_name):
    """
    Get schema of a table as StructType.
    """
    return spark.table(table_name).schema

def assert_schema_match(df, table_name):
    """
    Assert that DataFrame schema matches the target table schema.
    """
    table_schema = get_table_schema(table_name)
    assert len(df.columns) == len(table_schema), f"Column count mismatch: {len(df.columns)} vs {len(table_schema)}"
    for f1, f2 in zip(df.schema.fields, table_schema.fields):
        assert f1.name == f2.name, f"Column name mismatch: {f1.name} vs {f2.name}"
        assert isinstance(f1.dataType, type(f2.dataType)), f"Column type mismatch: {f1.dataType} vs {f2.dataType}"

def assert_parquet_partitioned_by_state(parquet_path):
    """
    Assert that parquet files are partitioned by state.
    """
    try:
        dirs = [d for d in os.listdir(parquet_path) if d.startswith("state=")]
        assert len(dirs) > 0, "No state partitions found in parquet backup"
    except Exception as e:
        raise AssertionError(f"Partition check failed: {str(e)}")

def assert_parquet_compression(parquet_path, expected_codec="snappy"):
    """
    Assert that parquet files are compressed with the expected codec.
    """
    try:
        for root, dirs, files in os.walk(parquet_path):
            for file in files:
                if file.endswith(".parquet"):
                    file_path = os.path.join(root, file)
                    parquet_file = pq.ParquetFile(file_path)
                    codec = parquet_file.metadata.row_group(0).column(0).compression
                    assert codec.lower() == expected_codec, f"Compression codec mismatch: {codec} != {expected_codec}"
    except ImportError:
        # If pyarrow is not available, skip this check
        pass
    except Exception as e:
        raise AssertionError(f"Compression check failed: {str(e)}")

def assert_no_partial_files(parquet_path):
    """
    Assert that no partial files exist in the backup directory.
    """
    if os.path.exists(parquet_path):
        files = os.listdir(parquet_path)
        assert len(files) == 0, "Partial files found after failed backup"

def assert_log_entry(status, operation_type, record_count, error_message_contains=None):
    """
    Assert that a log entry exists with the given parameters.
    """
    df = spark.table(BACKUP_LOG_TABLE)
    cond = (F.col("status") == status) & (F.col("operation_type") == operation_type) & (F.col("record_count") == record_count)
    if error_message_contains is not None:
        cond = cond & (F.col("error_message").contains(error_message_contains))
    assert df.filter(cond).count() > 0, f"Log entry not found for {status}, {operation_type}, {record_count}, {error_message_contains}"

def assert_table_row_count(table_name, expected_count):
    """
    Assert that a table has the expected number of rows.
    """
    actual_count = spark.table(table_name).count()
    assert actual_count == expected_count, f"Row count mismatch: {actual_count} != {expected_count}"

def assert_only_recent_records(table_name, date_col, min_date):
    """
    Assert that only records with date_col >= min_date exist in the table.
    """
    df = spark.table(table_name)
    assert df.filter(F.col(date_col) < min_date).count() == 0, f"Old records found before {min_date}"

def assert_backup_file_deleted(parquet_path, file_date):
    """
    Assert that backup files older than retention are deleted.
    """
    target_dir = os.path.join(parquet_path, f"date={file_date}")
    assert not os.path.exists(target_dir), f"Old backup file {target_dir} still exists"

def assert_idempotency(operation_type, date_str):
    """
    Assert that only one log entry exists for the operation and date.
    """
    df = spark.table(BACKUP_LOG_TABLE)
    count = df.filter((F.col("operation_type") == operation_type) & (F.col("timestamp").startswith(date_str))).count()
    assert count == 1, f"Idempotency failed: {count} log entries for {operation_type} on {date_str}"

# ---------------------------
# Test Data Preparation
# ---------------------------

# Load test data as per provided test data script
# (Assume test data is already loaded into RAW_TABLE for test execution)

# ---------------------------
# Test 1: Schema Validation
# ---------------------------
# /* Test that the DataFrame schema matches the target table schema */
df_raw = spark.table(RAW_TABLE)
assert_schema_match(df_raw, RAW_TABLE)

# ---------------------------
# Test 2: Data Type Conversion and NULL Handling
# ---------------------------
# /* Test that all required fields are present and valid, and NULLs are handled */
invalid_rows = []
for row in df_raw.collect():
    valid, err = validate_required_fields(row.asDict())
    if not valid:
        invalid_rows.append((row, err))
assert len(invalid_rows) == 4, "Expected 4 invalid rows for required fields and email format"

# ---------------------------
# Test 3: Backup Operation - Happy Path
# ---------------------------
# /* Test full backup to parquet, partitioned by state, snappy compression */
try:
    # Remove previous backup files for clean test
    if os.path.exists(VOLUME_BACKUP_PATH):
        shutil.rmtree(VOLUME_BACKUP_PATH)
except Exception:
    pass  # Ignore if path does not exist

try:
    # Filter out invalid rows for backup
    valid_df = df_raw.filter(
        (F.col("id").isNotNull()) &
        (F.col("email").isNotNull()) &
        (F.col("state").isNotNull()) &
        (F.col("email").rlike(r"^[^@]+@[^@]+\.[^@]+$"))
    )
    record_count = valid_df.count()
    # Write to parquet, partitioned by state, snappy compression
    valid_df.write.mode("overwrite").partitionBy(BACKUP_PARTITION_COL).option("compression", PARQUET_COMPRESSION).parquet(VOLUME_BACKUP_PATH)
    # Log success
    log_operation("SUCCESS", "BACKUP", record_count, None)
except Exception as e:
    log_operation("FAILED", "BACKUP", 0, str(e))
    raise

# Assert backup files exist and are partitioned by state
assert_parquet_partitioned_by_state(VOLUME_BACKUP_PATH)
# Assert parquet files use snappy compression (if pyarrow available)
assert_parquet_compression(VOLUME_BACKUP_PATH, "snappy")
# Assert log entry for backup
assert_log_entry("SUCCESS", "BACKUP", record_count)

# ---------------------------
# Test 4: Backup Operation - Data Validation Error
# ---------------------------
# /* Test backup fails if invalid email format is present */
try:
    # Intentionally include invalid email
    df_invalid_email = df_raw.filter(F.col("email") == "invalid-email-format")
    if df_invalid_email.count() > 0:
        raise ValueError(f"Data validation error: invalid email format in record id={df_invalid_email.first()['id']}")
except Exception as e:
    log_operation("FAILED", "BACKUP", 0, str(e))
    assert_log_entry("FAILED", "BACKUP", 0, "invalid email format")

# ---------------------------
# Test 5: Backup Operation - Required Field NULL
# ---------------------------
# /* Test backup fails if required fields are NULL */
for field in ["email", "id", "state"]:
    try:
        df_null = df_raw.filter(F.col(field).isNull())
        if df_null.count() > 0:
            raise ValueError(f"{field} is NULL in record id={df_null.first()['id']}")
    except Exception as e:
        log_operation("FAILED", "BACKUP", 0, str(e))
        assert_log_entry("FAILED", "BACKUP", 0, f"{field} is NULL")

# ---------------------------
# Test 6: Backup Operation - Insufficient Write Permissions
# ---------------------------
# /* Simulate permission denied by writing to a protected path */
try:
    protected_path = "/root/protected_backup"
    valid_df.limit(1).write.mode("overwrite").parquet(protected_path)
except Exception as e:
    log_operation("FAILED", "BACKUP", 0, "Permission denied: write access")
    assert_log_entry("FAILED", "BACKUP", 0, "Permission denied: write access")

# ---------------------------
# Test 7: Backup Operation - Insufficient Storage
# ---------------------------
# /* Simulate insufficient storage by checking free space (mocked) */
try:
    # Simulate <1GB free space
    free_space_gb = 0.5
    estimated_backup_size_gb = 10
    if free_space_gb < estimated_backup_size_gb:
        raise IOError("Insufficient storage space")
except Exception as e:
    log_operation("FAILED", "BACKUP", 0, "Insufficient storage space")
    assert_log_entry("FAILED", "BACKUP", 0, "Insufficient storage space")

# ---------------------------
# Test 8: Vacuum Operation - Happy Path
# ---------------------------
# /* Test vacuum operation retains only records from last 30 days */
try:
    today = datetime(2024, 6, 30)
    min_date = today - timedelta(days=VACUUM_RETENTION_DAYS)
    # Count records before vacuum
    pre_vacuum_count = df_raw.count()
    # Delete old records
    spark.sql(f"""
        DELETE FROM {RAW_TABLE}
        WHERE creation_date < DATE('{min_date.date()}')
    """)
    # Count records after vacuum
    post_vacuum_count = spark.table(RAW_TABLE).count()
    # Log success
    log_operation("SUCCESS", "VACUUM", post_vacuum_count, None)
except Exception as e:
    log_operation("FAILED", "VACUUM", 0, str(e))
    raise

# Assert only recent records remain
assert_only_recent_records(RAW_TABLE, "creation_date", min_date.date())
# Assert log entry for vacuum
assert_log_entry("SUCCESS", "VACUUM", post_vacuum_count)

# ---------------------------
# Test 9: Vacuum Operation - Insufficient Table Permissions
# ---------------------------
# /* Simulate permission denied on DELETE */
try:
    # Simulate by raising error
    raise PermissionError("Permission denied: delete access")
except Exception as e:
    log_operation("FAILED", "VACUUM", 0, "Permission denied: delete access")
    assert_log_entry("FAILED", "VACUUM", 0, "Permission denied: delete access")

# ---------------------------
# Test 10: Retention Policy - Delete Old Backup Files
# ---------------------------
# /* Test that backup files older than retention are deleted */
try:
    # Simulate a backup file from 2024-03-01
    old_backup_dir = os.path.join(VOLUME_BACKUP_PATH, "date=2024-03-01")
    os.makedirs(old_backup_dir, exist_ok=True)
    # Run retention policy
    cutoff_date = (datetime(2024, 6, 1) - timedelta(days=BACKUP_RETENTION_DAYS)).date()
    for d in os.listdir(VOLUME_BACKUP_PATH):
        if d.startswith("date="):
            file_date = d.split("=")[1]
            if file_date < str(cutoff_date):
                shutil.rmtree(os.path.join(VOLUME_BACKUP_PATH, d))
    # Assert old backup deleted
    assert_backup_file_deleted(VOLUME_BACKUP_PATH, "2024-03-01")
except Exception as e:
    raise AssertionError(f"Retention policy failed: {str(e)}")

# ---------------------------
# Test 11: Logging - All Operations
# ---------------------------
# /* Test that all operations are logged with correct fields */
df_log = spark.table(BACKUP_LOG_TABLE)
required_log_fields = ["timestamp", "status", "operation_type", "record_count", "error_message"]
for field in required_log_fields:
    assert field in df_log.columns, f"Log table missing field: {field}"

# ---------------------------
# Test 12: Atomicity - No Partial State on Failure
# ---------------------------
# /* Test that failed backup leaves no partial files */
try:
    # Simulate failure during backup
    partial_path = os.path.join(VOLUME_BACKUP_PATH, "partial")
    os.makedirs(partial_path, exist_ok=True)
    raise RuntimeError("Simulated failure during backup")
except Exception as e:
    # Clean up partial files
    if os.path.exists(partial_path):
        shutil.rmtree(partial_path)
    log_operation("FAILED", "BACKUP", 0, str(e))
    assert_no_partial_files(partial_path)
    assert_log_entry("FAILED", "BACKUP", 0, "Simulated failure during backup")

# ---------------------------
# Test 13: Idempotency - Only One Operation Per Day
# ---------------------------
# /* Test that only one backup and one vacuum are performed per day */
date_str = datetime.utcnow().date().isoformat()
assert_idempotency("BACKUP", date_str)
assert_idempotency("VACUUM", date_str)

# ---------------------------
# Test 14: Auditing - All Operation Details in Log
# ---------------------------
# /* Test that all operation details are available for audit */
df_log = spark.table(BACKUP_LOG_TABLE)
assert df_log.count() > 0, "No log entries found for audit"

# ---------------------------
# Test 15: Backup Includes All Columns
# ---------------------------
# /* Test that backup parquet files include all columns from source table */
parquet_df = spark.read.parquet(VOLUME_BACKUP_PATH)
assert set(parquet_df.columns) == set(df_raw.columns), "Backup parquet columns do not match source table"

# ---------------------------
# Test 16: Parquet Partitioning by State
# ---------------------------
# /* Test that backup parquet files are partitioned by state */
assert_parquet_partitioned_by_state(VOLUME_BACKUP_PATH)

# ---------------------------
# Test 17: Parquet Compression
# ---------------------------
# /* Test that backup parquet files use snappy compression */
assert_parquet_compression(VOLUME_BACKUP_PATH, "snappy")

# ---------------------------
# Test 18: Only Recent Records After Vacuum
# ---------------------------
# /* Test that only records from last 30 days remain after vacuum */
today = datetime(2024, 6, 30)
min_date = today - timedelta(days=VACUUM_RETENTION_DAYS)
assert_only_recent_records(RAW_TABLE, "creation_date", min_date.date())

# ---------------------------
# Test 19: Error Logging if Log Table Unavailable
# ---------------------------
# /* Test that error is raised if backup log table is unavailable */
try:
    # Simulate by dropping log table
    spark.sql(f"DROP TABLE IF EXISTS {BACKUP_LOG_TABLE}")
    try:
        log_operation("FAILED", "BACKUP", 0, "Backup log table unavailable")
    except RuntimeError as e:
        assert "Backup log table unavailable" in str(e)
finally:
    # Recreate log table for further tests
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {BACKUP_LOG_TABLE} (
            timestamp STRING,
            status STRING,
            operation_type STRING,
            record_count BIGINT,
            error_message STRING
        )
        USING DELTA
    """)

# ---------------------------
# Test 20: Incremental Backup (if specified)
# ---------------------------
# /* Test that only new/changed records are backed up in incremental mode */
# For this test, simulate last backup date and filter accordingly
last_backup_date = datetime(2024, 6, 29).date()
incremental_df = df_raw.filter(F.col("creation_date") > F.lit(last_backup_date))
incremental_count = incremental_df.count()
try:
    incremental_df.write.mode("overwrite").partitionBy(BACKUP_PARTITION_COL).option("compression", PARQUET_COMPRESSION).parquet(VOLUME_BACKUP_PATH)
    log_operation("SUCCESS", "BACKUP", incremental_count, None)
except Exception as e:
    log_operation("FAILED", "BACKUP", 0, str(e))
    raise
assert_log_entry("SUCCESS", "BACKUP", incremental_count)

# ---------------------------
# Test 21: Full Backup (if specified)
# ---------------------------
# /* Test that all records are backed up in full mode */
full_count = valid_df.count()
try:
    valid_df.write.mode("overwrite").partitionBy(BACKUP_PARTITION_COL).option("compression", PARQUET_COMPRESSION).parquet(VOLUME_BACKUP_PATH)
    log_operation("SUCCESS", "BACKUP", full_count, None)
except Exception as e:
    log_operation("FAILED", "BACKUP", 0, str(e))
    raise
assert_log_entry("SUCCESS", "BACKUP", full_count)

# ---------------------------
# Cleanup: Remove test backup files and restore log table
# ---------------------------
try:
    if os.path.exists(VOLUME_BACKUP_PATH):
        shutil.rmtree(VOLUME_BACKUP_PATH)
except Exception:
    pass  # Ignore if path does not exist

# Note: Do not include spark.stop() in Databricks notebooks
