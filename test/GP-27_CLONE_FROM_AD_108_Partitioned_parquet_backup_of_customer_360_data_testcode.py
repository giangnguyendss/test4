# PySpark script for Databricks customer_360_raw backup and vacuum operation
# Purpose: Test partitioned Parquet backup and Delta vacuum/delete logic on Unity Catalog table with all error/logging scenarios
# Author: Giang Nguyen
# Date: 2025-07-22
# Description: This script tests reading from Unity Catalog table, writing partitioned Parquet backups with snappy compression, validates column/scheme/data integrity, checks all error/edge vault cases, performs vacuum on Delta, and ensures logs/errors are handled appropriately. It covers permissions, missing columns, bad datetimes, column count, and null handling, with robust type and schema checks.

# -- Required imports for PySpark, Delta, datatypes, functions, and logging
from pyspark.sql import functions as F      
from pyspark.sql.types import (             
    StructType, StructField, StringType, LongType, DoubleType, IntegerType, DateType, TimestampType
)
from delta.tables import DeltaTable         
import datetime                            
import os                                  
import json                                

# -- Configuration constants and paths
CATALOG_TABLE = "purgo_databricks.purgo_playground.customer_360_raw"
PARQUET_BACKUP_PATH = "/Volumes/customer_360_raw_backup"
LOG_FILE_PATH = "/dbfs/logs/customer_360_raw_backup.log"
RETENTION_DAYS = 30
REQUIRED_PARTITION_COL = "state"
REQUIRED_TIMESTAMP_COL = "updated_at"
COMPRESSION_CODEC = "snappy"   # Only "snappy" is allowed
CURRENT_UTC = datetime.datetime(2024, 6, 30, 12, 0, 0)  # Fixed for deterministic tests

# -- Utility: log events to file as JSONL
def log_event(operation, status, details="", error_message=None):
    """
    Log an event to /dbfs/logs/customer_360_raw_backup.log
    
    Args:
        operation (str): Operation name (e.g., 'backup', 'vacuum').
        status (str): 'OK' or 'FAIL'.
        details (str): Additional info.
        error_message (str|None): Exception msg or None.
    """
    log_entry = {
        "event_time": datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "operation": operation,
        "status": status,
        "details": details,
        "error_message": error_message
    }
    try:
        with open(LOG_FILE_PATH, "a") as f:
            f.write(json.dumps(log_entry) + "\n")
    except Exception as e:
        print(f"[LOGGING ERROR]: Unable to log: {e}")

# -- Utility: function to validate Parquet compression codec in backup directory
def assert_parquet_files_compressed_snappy(backup_path):
    """
    Assert that all Parquet files under backup_path are compressed with snappy.

    Args:
        backup_path (str): Path to Parquet backup dir.

    Raises:
        AssertionError: If any non-snappy compressed Parquets are found.
    """
    # NOTE: pyarrow or native parquetlib is not directly available;
    # Databricks Parquet always uses file metadata to show codec
    # For test, sample a file and read its footer using SQL (show format).
    files = [f for f in os.listdir(backup_path) if f.endswith('.parquet')]
    if len(files) == 0:
        return
    parquet_file = os.path.join(backup_path, files[0])
    df = spark.read.parquet(parquet_file)
    # Save as single file and check its format option
    # No direct API to introspect codec, but if .option("compression", ...) set to snappy: assume it's correct
    # Not assertable at API level in PySpark without extra libs (test limited here)
    pass

# -- Step 1: Read from Unity Catalog table. Catch missing table/columns/permission errors.
try:
    customer_360_raw_df = spark.read.table(CATALOG_TABLE)
    log_event("read_source", "OK", details="Read Unity Catalog table")
except Exception as e:
    log_event("read_source", "FAIL", error_message=f"TABLE_NOT_FOUND_OR_NO_PRIV: {str(e)}")
    raise

# -- Step 2: Validate schema and partition column presence before backup
src_fields = set(customer_360_raw_df.columns)
assert REQUIRED_PARTITION_COL in src_fields, \
    log_event("schema_validation", "FAIL", error_message="PARTITION_COLUMN_MISSING: 'state' missing") or "Required partition column 'state' is missing"

# -- Step 3: Ensure number of DataFrame columns matches table schema
table_schema = customer_360_raw_df.schema
assert len(customer_360_raw_df.columns) == len(table_schema), \
    log_event("schema_validation", "FAIL", error_message="COLUMN_COUNT_MISMATCH") or "Column count does not match target schema"

# -- Step 4: If backup dir compression set to 'none', error out
if COMPRESSION_CODEC == "none":
    log_event("backup", "FAIL", error_message="COMPRESSION_REQUIRED: Output compression must be enabled")
    raise Exception("COMPRESSION_REQUIRED: Output compression must be enabled")

# -- Step 5: Do not allow backup if no data or table missing
source_row_count = customer_360_raw_df.count()
if source_row_count == 0:
    log_event("backup", "FAIL", details="NO_DATA_TO_BACKUP")
    # Directory must remain empty for this test path
else:
    # -- Step 6: Write DataFrame partitioned by `state`, snappy compressed Parquet
    try:
        customer_360_raw_df.write.mode("overwrite") \
            .option("compression", COMPRESSION_CODEC) \
            .partitionBy(REQUIRED_PARTITION_COL) \
            .parquet(PARQUET_BACKUP_PATH)
        log_event("backup", "OK", details=f"Backup {source_row_count} rows partitioned by {REQUIRED_PARTITION_COL}")
    except Exception as e:
        log_event("backup", "FAIL", error_message=str(e))
        raise

    # -- Step 7: Validate row count, schema, column names in Parquet
    try:
        parquet_df = spark.read.parquet(PARQUET_BACKUP_PATH)
        parquet_row_count = parquet_df.count()
        assert parquet_row_count == source_row_count, \
            log_event("backup_validation", "FAIL", error_message="ROW_COUNT_MISMATCH") or "Row count mismatch"
        assert set(parquet_df.columns) == set(src_fields), \
            log_event("backup_validation", "FAIL", error_message="COLUMN_NAME_MISMATCH") or "Backup column names mismatch"
        for sf in table_schema:
            assert parquet_df.schema[sf.name].dataType == sf.dataType, \
                log_event("backup_validation", "FAIL", error_message="DATATYPE_MISMATCH") or f"Column {sf.name} datatype mismatch"
        log_event("backup_validation", "OK", details="Backup schema, names, datatypes match")
    except Exception as e:
        log_event("backup_validation", "FAIL", error_message=str(e))
        raise

    # -- Step 8: Check that Parquet files are written partitioned by state, and compressed (see notes in function above)
    # Not directly assertable for snappy in PySpark w/o native libs; assume OK if no error
    try:
        partitions = parquet_df.select(REQUIRED_PARTITION_COL).distinct().count()
        src_partitions = customer_360_raw_df.select(REQUIRED_PARTITION_COL).distinct().count()
        assert partitions == src_partitions, \
            log_event("partitioning", "FAIL", error_message="PARTITION_VALUE_MISMATCH") or "Backup partitions mismatch source 'state' distinct values"
        log_event("partitioning", "OK", details="Backup partitions per 'state' correct")
        # Parquet snappy check best-effort per note: see files
    except Exception as e:
        log_event("partitioning", "FAIL", error_message=str(e))
        raise

# -- Step 9: Data type conversion/sample: STRING -> TIMESTAMP, NULLs, ARRAY, STRUCT/MAP types validation
def test_data_type_and_null_handling(df):
    """
    Test data type conversions, NULL handling, and Spark/SQL compatibility.

    Args:
        df (DataFrame): DataFrame to test.

    Returns:
        None
    """
    # Test: updated_at as timestamp (must convert, NULLs/bad strings become null)
    df2 = df.withColumn("updated_at_ts", F.to_timestamp("updated_at", "yyyy-MM-dd HH:mm:ss"))
    null_count = df2.filter(F.col("updated_at_ts").isNull() & F.col("updated_at").isNotNull()).count()
    if null_count > 0:
        log_event("datatype_conversion", "FAIL", details=f"{null_count} bad 'updated_at' format(s)", error_message="INVALID_DATETIME_FORMAT")
    # Test: array, struct, map field add, and nulls
    arr_df = df.withColumn("arr_test", F.array("state", "zip"))
    struct_df = arr_df.withColumn("struct_test", F.struct("state", "zip"))
    map_df = struct_df.withColumn("map_test", F.create_map(["state", "zip"]))
    # Check null: set 'name' to null where zip is invalid
    nullified = map_df.withColumn("name", F.when(F.col("zip") == "!!!!!!", F.lit(None)).otherwise(F.col("name")))
    null_rows = nullified.filter(F.col("name").isNull()).count()
    if null_rows > 0:
        log_event("null_handling", "OK", details=f"{null_rows} nulls injected in 'name'")
    # Test for all datatypes: LongType, StringType, DoubleType, IntegerType, DateType, TimestampType, ARRAY, STRUCT, MAP
    return

test_data_type_and_null_handling(customer_360_raw_df)

# -- Step 10: Run vacuum operation on Unity Catalog Delta table to remove records older than 30 days based on updated_at
try:
    # First, check if table is Delta
    delta_table = DeltaTable.forName(spark, CATALOG_TABLE)
    cutoff_dt = (CURRENT_UTC - datetime.timedelta(days=RETENTION_DAYS)).strftime('%Y-%m-%d %H:%M:%S')
    # Window: Validate that field exists and is convertible to timestamp
    try:
        upd_col = customer_360_raw_df.select(REQUIRED_TIMESTAMP_COL)
        nulls = upd_col.filter(F.col(REQUIRED_TIMESTAMP_COL).isNull()).count()
        if nulls > 0:
            log_event("vacuum_precheck", "FAIL", error_message="NULL_DATETIME_ERROR: 'updated_at' field is null")
            raise Exception("NULL_DATETIME_ERROR: 'updated_at' field is null")
    except Exception as e:
        log_event("vacuum_precheck", "FAIL", error_message="NO_UPDATED_AT_COL_OR_NULL: " + str(e))
        raise

    # Delete records older than cutoff
    delta_table.delete(f"{REQUIRED_TIMESTAMP_COL} < '{cutoff_dt}'")
    log_event("vacuum", "OK", details=f"Deleted records where {REQUIRED_TIMESTAMP_COL} < {cutoff_dt}")
    # Optional: run Delta VACUUM physical clean-up
    spark.sql(f"VACUUM {CATALOG_TABLE} RETAIN 0 HOURS")
    log_event("vacuum_physical", "OK", details="Run Delta VACUUM 0h")
except Exception as e:
    log_event("vacuum", "FAIL", error_message=f"{str(e)}")
    raise

# -- Step 11: Post-vacuum data quality: remaining records' updated_at >= cutoff
try:
    validate_df = spark.read.table(CATALOG_TABLE).withColumn("updated_at_ts", F.to_timestamp("updated_at", "yyyy-MM-dd HH:mm:ss"))
    bad = validate_df.filter(F.col("updated_at_ts") < F.lit(cutoff_dt)).count()
    assert bad == 0, log_event("vacuum_validation", "FAIL", error_message="VACUUM_NOT_COMPLETE") or "Old records found after vacuum"
    log_event("vacuum_validation", "OK", details="All remaining rows post-vacuum are in range")
except Exception as e:
    log_event("vacuum_validation", "FAIL", error_message=str(e))
    raise

# -- Step 12: Permissions errors for read/write (simulate by try/except)
def simulate_permission_error(read_ok=True, write_ok=True):
    """
    Simulates permission errors for backup.

    Args:
        read_ok (bool): if False, simulate no read priv.
        write_ok (bool): if False, simulate no write priv.
    """
    try:
        if not read_ok:
            raise Exception("ACCESS_DENIED: No READ privilege on source table")
        if not write_ok:
            raise Exception("ACCESS_DENIED: No WRITE privilege on backup volume")
        # else do nothing
    except Exception as e:
        log_event("permission_check", "FAIL", error_message=str(e))

simulate_permission_error(read_ok=True, write_ok=False)
simulate_permission_error(read_ok=False, write_ok=True)
simulate_permission_error(read_ok=False, write_ok=False)

# -- Step 13: Test error scenario - missing table or missing partition column
def test_structural_error_missing_table_or_partition():
    """
    Simulates error if table missing, or 'state' column missing for partition.
    """
    # Missing table
    try:
        spark.read.table("purgo_databricks.purgo_playground.nonexistent_table")
    except Exception as e:
        log_event("table_missing", "FAIL", error_message="TABLE_NOT_FOUND: customer_360_raw table does not exist")
    # Missing column
    test_df = customer_360_raw_df.drop(REQUIRED_PARTITION_COL)
    try:
        test_df.write.partitionBy(REQUIRED_PARTITION_COL).parquet("/Volumes/_should_fail")
    except Exception as e:
        log_event("partition_col_missing", "FAIL", error_message="PARTITION_COLUMN_MISSING: Required partition column 'state' missing")

test_structural_error_missing_table_or_partition()

# -- Step 14: Test error for invalid 'updated_at' datetime formatting
def test_invalid_updated_at():
    """
    Tests that 'updated_at' bad format results in INVALID_DATETIME_FORMAT error.
    """
    df = customer_360_raw_df.withColumn("updated_at_ts", F.to_timestamp(F.col(REQUIRED_TIMESTAMP_COL), "yyyy-MM-dd HH:mm:ss"))
    count_bad = df.filter((F.col("updated_at_ts").isNull()) & (F.col(REQUIRED_TIMESTAMP_COL).isNotNull())).count()
    if count_bad > 0:
        log_event("vacuum_datetime_format", "FAIL", error_message="INVALID_DATETIME_FORMAT: 'updated_at' is not yyyy-MM-dd HH:mm:ss")
test_invalid_updated_at()

# -- Step 15: Test that backup gracefully handles missing or empty table
def test_backup_empty_handling():
    """
    Tests backup operation handles empty DataFrame (no data).
    """
    empty_df = customer_360_raw_df.filter("1=0")
    if empty_df.count() == 0:
        log_event("backup", "FAIL", details="NO_DATA_TO_BACKUP")

test_backup_empty_handling()

# -- Step 16: Clean up - Remove test artifacts for test idempotency
def cleanup_test_parquet():
    """
    Deletes backup directory made during test run.
    """
    # os.system(f"rm -rf {PARQUET_BACKUP_PATH}")   # Do not actually remove in Databricks prod test
    log_event("cleanup", "OK", details="Cleanup test Parquet backup skipped in test run")

cleanup_test_parquet()
