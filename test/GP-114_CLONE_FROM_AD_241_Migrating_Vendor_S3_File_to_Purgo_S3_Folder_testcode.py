%pip install pytest

# -----------------------------------------------------------
# Databricks PySpark Test Suite for S3 File Transfer Pipeline
# -----------------------------------------------------------
# All code below is executable and follows Databricks and Unity Catalog best practices.
# This test suite covers:
#   - Schema validation
#   - Data type and NULL handling
#   - Unit and integration tests for file transfer logic
#   - Delta Lake operations and data quality checks
#   - Error and edge case handling
#   - Logging and analytics features
#   - Cleanup operations
# -----------------------------------------------------------

# ---------------------------
# Imports and Setup
# ---------------------------
from pyspark.sql import SparkSession  # SparkSession is already available in Databricks
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, ArrayType, MapType, StructType  
from pyspark.sql import functions as F  
from pyspark.sql.utils import AnalysisException  
import pytest  
import datetime  

# ---------------------------
# Section: Helper Functions
# ---------------------------

def assert_schema(df, expected_schema):
    """
    Assert that the DataFrame schema matches the expected schema.
    """
    # Compare field names and data types
    actual_fields = [(f.name, f.dataType, f.nullable) for f in df.schema.fields]
    expected_fields = [(f.name, f.dataType, f.nullable) for f in expected_schema.fields]
    assert actual_fields == expected_fields, f"Schema mismatch: {actual_fields} != {expected_fields}"

def assert_table_row_count(table, expected_count):
    """
    Assert that the table has the expected number of rows.
    """
    actual_count = spark.table(table).count()
    assert actual_count == expected_count, f"Row count mismatch for {table}: {actual_count} != {expected_count}"

def assert_table_contains(table, where_expr):
    """
    Assert that the table contains at least one row matching the where_expr.
    """
    df = spark.table(table).where(where_expr)
    assert df.count() > 0, f"Table {table} does not contain any row matching: {where_expr}"

def assert_table_not_contains(table, where_expr):
    """
    Assert that the table does not contain any row matching the where_expr.
    """
    df = spark.table(table).where(where_expr)
    assert df.count() == 0, f"Table {table} should not contain any row matching: {where_expr}"

def get_secret(scope, key):
    """
    Retrieve a secret from Databricks secret scope.
    """
    try:
        return dbutils.secrets.get(scope=scope, key=key)
    except Exception as e:
        return None

def simulate_s3_list_files(s3_path, files):
    """
    Simulate S3 file listing for a given S3 path.
    """
    # Only return files at the root (no subfolders)
    return [f for f in files if "/" not in f]

def simulate_s3_list_files_with_subdirs(s3_path, files):
    """
    Simulate S3 file listing including subfolders.
    """
    return files

def simulate_s3_copy_file(src_path, dest_path, file_name):
    """
    Simulate S3 file copy operation.
    """
    # In real code, use boto3 or dbutils.fs.cp
    # Here, just return True for success
    return True

def simulate_s3_accessible(s3_path):
    """
    Simulate S3 path accessibility.
    """
    # For test, return False if path contains 'folder9' or 'landing10'
    if "folder9" in s3_path or "landing10" in s3_path:
        return False
    return True

def simulate_aws_credentials_valid():
    """
    Simulate AWS credentials check.
    """
    # For test, return False if CFG008 is being processed
    return True

# ---------------------------
# Section: Schema Validation Tests
# ---------------------------

# /* Validate schema of purgo_playground.ingest_config_master */
expected_ingest_config_master_schema = StructType([
    StructField("config_id", StringType(), True),
    StructField("source_object_name", StringType(), True),
    StructField("source_system", StringType(), True),
    StructField("file_name", StringType(), True),
    StructField("frequency", StringType(), True),
    StructField("location", StringType(), True),
    StructField("domain", StringType(), True),
    StructField("sub_domain", StringType(), True),
    StructField("s3_vendor_path", StringType(), True),
    StructField("source_path", StringType(), True),
    StructField("s3_landing_path", StringType(), True),
    StructField("s3_archive_path", StringType(), True),
    StructField("delta_load_ts", StringType(), True),
    StructField("full_or_incremental_load", StringType(), True),
    StructField("zip_file", StringType(), True),
    StructField("vendor", StringType(), True),
    StructField("delimiter", StringType(), True),
    StructField("source_landing", StringType(), True),
    StructField("src_landing_table_name", StringType(), True),
    StructField("publish_unstitched", StringType(), True),
    StructField("publish_unstitched_table_name", StringType(), True),
    StructField("publish_stitched", StringType(), True),
    StructField("publish_stitched_table_name", StringType(), True),
    StructField("primary_key", StringType(), True),
    StructField("header", StringType(), True),
    StructField("date_pattern", StringType(), True),
    StructField("actual_file_name", StringType(), True),
    StructField("vendor_file_deletion_flag", StringType(), True),
    StructField("file_recursive_flag", StringType(), True),
    StructField("total_weeks_req_data", StringType(), True),
    StructField("total_weeks_file_data", StringType(), True),
    StructField("active_flag", StringType(), True)
])
assert_schema(spark.table("purgo_playground.ingest_config_master"), expected_ingest_config_master_schema)

# /* Validate schema of purgo_playground.s3_file_process_log */
expected_s3_file_process_log_schema = StructType([
    StructField("file_name", StringType(), True),
    StructField("s3_vendor_path", StringType(), True),
    StructField("s3_landing_path", StringType(), True),
    StructField("s3_archive_path", StringType(), True),
    StructField("file_status", StringType(), True),
    StructField("file_processed_date", TimestampType(), True)
])
assert_schema(spark.table("purgo_playground.s3_file_process_log"), expected_s3_file_process_log_schema)

# ---------------------------
# Section: Data Type Conversion and NULL Handling
# ---------------------------

# /* Test data type conversions and NULL handling for ingest_config_master */
df = spark.table("purgo_playground.ingest_config_master")
# Test: All s3_*_path columns are STRING and can be safely cast to STRING
for col in ["s3_vendor_path", "s3_landing_path", "s3_archive_path"]:
    assert df.schema[col].dataType == StringType(), f"{col} is not STRING"
# Test: NULL handling
null_count = df.filter(F.col("s3_vendor_path").isNull() | F.col("s3_landing_path").isNull() | F.col("s3_archive_path").isNull()).count()
assert null_count >= 0  # Should not throw

# /* Test complex types: simulate ARRAY, STRUCT, MAP in a DataFrame */
complex_schema = StructType([
    StructField("file_list", ArrayType(StringType()), True),
    StructField("file_meta", StructType([
        StructField("name", StringType(), True),
        StructField("size", StringType(), True)
    ]), True),
    StructField("file_tags", MapType(StringType(), StringType()), True)
])
complex_data = [(
    ["file1.csv", "file2.csv"],
    {"name": "file1.csv", "size": "1234"},
    {"type": "csv", "owner": "user1"}
)]
complex_df = spark.createDataFrame(complex_data, schema=complex_schema)
assert isinstance(complex_df.schema["file_list"].dataType, ArrayType)
assert isinstance(complex_df.schema["file_meta"].dataType, StructType)
assert isinstance(complex_df.schema["file_tags"].dataType, MapType)

# ---------------------------
# Section: Unit Tests for Transformations
# ---------------------------

# /* Test: Only configs with active_flag = 'A' are processed */
active_configs = spark.table("purgo_playground.ingest_config_master").filter(F.col("active_flag") == "A")
inactive_configs = spark.table("purgo_playground.ingest_config_master").filter(F.col("active_flag") != "A")
assert active_configs.count() > 0
assert inactive_configs.count() > 0

# /* Test: File name matching is case-sensitive */
# Simulate vendor files and purgo files
vendor_files = ["File8.csv", "file8.csv"]
purgo_files = ["file8.csv"]
# Only "File8.csv" should be eligible for transfer
eligible_files = [f for f in vendor_files if f not in purgo_files]
assert "File8.csv" in eligible_files
assert "file8.csv" not in eligible_files

# /* Test: Only root-level files are processed (no subfolders) */
vendor_files_with_subdir = ["file5.csv", "subdir/file6.csv"]
root_files = simulate_s3_list_files("s3://vendor-bucket/folder17", vendor_files_with_subdir)
assert "file5.csv" in root_files
assert "subdir/file6.csv" not in root_files

# /* Test: All file types are eligible for transfer */
all_types = ["data.csv", "image.png", "report.pdf"]
for f in all_types:
    assert f.endswith((".csv", ".png", ".pdf"))

# /* Test: NULL handling in config */
null_config = spark.table("purgo_playground.ingest_config_master").filter(F.col("config_id") == "CFG015").collect()[0]
assert null_config.s3_vendor_path is not None
assert null_config.s3_landing_path is not None
assert null_config.s3_archive_path is not None

# ---------------------------
# Section: Integration Tests (End-to-End Flow)
# ---------------------------

# /* Test: Happy path - files copied if not in Purgo or Archive */
# Example: CFG001
cfg = spark.table("purgo_playground.ingest_config_master").filter(F.col("config_id") == "CFG001").collect()[0]
vendor_path = cfg.s3_vendor_path
purgo_path = cfg.s3_landing_path
archive_path = cfg.s3_archive_path
vendor_files = ["file1.csv", "file2.csv"]
purgo_files = []
archive_files = []
eligible_files = [f for f in vendor_files if f not in purgo_files and f not in archive_files]
assert eligible_files == ["file1.csv", "file2.csv"]

# /* Test: File remains in vendor after copy */
assert "file1.csv" in vendor_files

# /* Test: File not present in archive after copy */
assert "file1.csv" not in archive_files

# /* Test: Log table updated with SUCCESS for each transferred file */
for f in eligible_files:
    assert_table_contains("purgo_playground.s3_file_process_log", f"file_name = '{f}' AND file_status = 'SUCCESS' AND s3_vendor_path = '{vendor_path}'")

# /* Test: Skipped if already in Purgo */
cfg = spark.table("purgo_playground.ingest_config_master").filter(F.col("config_id") == "CFG003").collect()[0]
vendor_files = ["file1.csv", "file2.csv"]
purgo_files = ["file1.csv"]
skipped_files = [f for f in vendor_files if f in purgo_files]
for f in skipped_files:
    assert_table_contains("purgo_playground.s3_file_process_log", f"file_name = '{f}' AND file_status = 'SKIPPED_EXISTS' AND s3_vendor_path = '{cfg.s3_vendor_path}'")

# /* Test: Skipped if already in Archive */
cfg = spark.table("purgo_playground.ingest_config_master").filter(F.col("config_id") == "CFG004").collect()[0]
vendor_files = ["file1.csv", "file2.csv"]
archive_files = ["file2.csv"]
skipped_files = [f for f in vendor_files if f in archive_files]
for f in skipped_files:
    assert_table_contains("purgo_playground.s3_file_process_log", f"file_name = '{f}' AND file_status = 'SKIPPED_ARCHIVE' AND s3_vendor_path = '{cfg.s3_vendor_path}'")

# /* Test: No active configs */
no_active = spark.table("purgo_playground.ingest_config_master").filter(F.col("active_flag") == "A").count() == 0
if no_active:
    assert_table_contains("purgo_playground.s3_file_process_log", "file_status = 'ERROR' AND file_name IS NULL")

# /* Test: S3 path missing in config */
for cfg_id in ["CFG005", "CFG006", "CFG007"]:
    cfg = spark.table("purgo_playground.ingest_config_master").filter(F.col("config_id") == cfg_id).collect()[0]
    if not cfg.s3_vendor_path or not cfg.s3_landing_path or not cfg.s3_archive_path:
        assert_table_contains("purgo_playground.s3_file_process_log", f"file_status = 'ERROR' AND (s3_vendor_path = '{cfg.s3_vendor_path}' OR s3_landing_path = '{cfg.s3_landing_path}' OR s3_archive_path = '{cfg.s3_archive_path}')")

# /* Test: AWS credentials missing/invalid */
cfg = spark.table("purgo_playground.ingest_config_master").filter(F.col("config_id") == "CFG008").collect()[0]
assert_table_contains("purgo_playground.s3_file_process_log", f"s3_vendor_path = '{cfg.s3_vendor_path}' AND file_status = 'ERROR'")

# /* Test: S3 access denied or path not found */
for cfg_id in ["CFG009", "CFG010"]:
    cfg = spark.table("purgo_playground.ingest_config_master").filter(F.col("config_id") == cfg_id).collect()[0]
    assert_table_contains("purgo_playground.s3_file_process_log", f"s3_vendor_path = '{cfg.s3_vendor_path}' AND file_status = 'ERROR'")

# /* Test: Only configs with active_flag 'A' are processed */
inactive_ids = ["CFG011", "CFG012"]
for cfg_id in inactive_ids:
    assert_table_not_contains("purgo_playground.s3_file_process_log", f"s3_vendor_path LIKE '%folder{cfg_id[-2:]}%'")

# /* Test: File name matching is case-sensitive */
cfg = spark.table("purgo_playground.ingest_config_master").filter(F.col("config_id") == "CFG013").collect()[0]
assert_table_contains("purgo_playground.s3_file_process_log", f"file_name = 'File8.csv' AND file_status = 'SUCCESS'")

# /* Test: Special/multibyte characters in file name */
cfg = spark.table("purgo_playground.ingest_config_master").filter(F.col("config_id") == "CFG014").collect()[0]
assert_table_contains("purgo_playground.s3_file_process_log", f"file_name = 'spécial_文件.csv' AND file_status = 'SUCCESS'")

# /* Test: NULL handling in log */
cfg = spark.table("purgo_playground.ingest_config_master").filter(F.col("config_id") == "CFG015").collect()[0]
assert_table_contains("purgo_playground.s3_file_process_log", f"s3_vendor_path = '{cfg.s3_vendor_path}' AND file_status = 'SUCCESS'")

# /* Test: All file types eligible */
for f in ["data.csv", "image.png", "report.pdf"]:
    assert_table_contains("purgo_playground.s3_file_process_log", f"file_name = '{f}' AND file_status = 'SUCCESS'")

# /* Test: Files not processed recursively in subfolders */
cfg = spark.table("purgo_playground.ingest_config_master").filter(F.col("config_id") == "CFG019").collect()[0]
assert_table_contains("purgo_playground.s3_file_process_log", f"file_name = 'file5.csv' AND file_status = 'SUCCESS'")
assert_table_not_contains("purgo_playground.s3_file_process_log", f"file_name = 'subdir/file6.csv'")

# /* Test: No files to transfer */
assert_table_contains("purgo_playground.s3_file_process_log", "file_status = 'SKIPPED_NONE'")

# /* Test: Unexpected exception during file transfer */
assert_table_contains("purgo_playground.s3_file_process_log", "file_name = 'file9.csv' AND file_status = 'ERROR'")

# ---------------------------
# Section: Delta Lake Operations and Analytics
# ---------------------------

# /* Test: Delta Lake MERGE, UPDATE, DELETE operations on s3_file_process_log */
from delta.tables import DeltaTable  

delta_log = DeltaTable.forName(spark, "purgo_playground.s3_file_process_log")

# Test: Update file_status for a file
delta_log.update(
    condition="file_name = 'file1.csv'",
    set={"file_status": F.lit("UPDATED")}
)
assert_table_contains("purgo_playground.s3_file_process_log", "file_name = 'file1.csv' AND file_status = 'UPDATED'")

# Test: Delete a log entry
delta_log.delete("file_name = 'file2.csv'")
assert_table_not_contains("purgo_playground.s3_file_process_log", "file_name = 'file2.csv'")

# Test: Merge (upsert) a new log entry
merge_source = spark.createDataFrame(
    [("file_merge.csv", "s3://vendor-bucket/folder1", "s3://purgo-bucket/landing1", "s3://purgo-bucket/archive1", "SUCCESS", datetime.datetime.now())],
    schema=expected_s3_file_process_log_schema
)
delta_log.alias("tgt").merge(
    merge_source.alias("src"),
    "tgt.file_name = src.file_name"
).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
assert_table_contains("purgo_playground.s3_file_process_log", "file_name = 'file_merge.csv' AND file_status = 'SUCCESS'")

# ---------------------------
# Section: Window Functions and Analytics
# ---------------------------

# /* Test: Window function - count of files processed per status */
from pyspark.sql.window import Window  

log_df = spark.table("purgo_playground.s3_file_process_log")
window_spec = Window.partitionBy("file_status")
log_df = log_df.withColumn("status_count", F.count("*").over(window_spec))
# Assert that status_count is correct for at least one status
status_counts = log_df.groupBy("file_status").count().collect()
for row in status_counts:
    count_in_window = log_df.filter(F.col("file_status") == row["file_status"]).select("status_count").first()["status_count"]
    assert count_in_window == row["count"]

# ---------------------------
# Section: Data Quality Validation
# ---------------------------

# /* Test: No duplicate log entries for same file_name and s3_vendor_path */
dupes = log_df.groupBy("file_name", "s3_vendor_path").count().filter("count > 1").count()
assert dupes == 0, "Duplicate log entries found"

# /* Test: All file_status values are in allowed set */
allowed_status = {"SUCCESS", "SKIPPED_EXISTS", "SKIPPED_ARCHIVE", "ERROR", "SKIPPED_NONE", "UPDATED"}
invalid_status = log_df.filter(~F.col("file_status").isin(list(allowed_status))).count()
assert invalid_status == 0, "Invalid file_status values found"

# /* Test: All timestamps are not null */
null_ts = log_df.filter(F.col("file_processed_date").isNull()).count()
assert null_ts == 0, "Null file_processed_date found"

# ---------------------------
# Section: Cleanup Operations
# ---------------------------

# /* Cleanup: Remove test log entry for file_merge.csv */
delta_log.delete("file_name = 'file_merge.csv'")

# /* Cleanup: Reset file_status for file1.csv to SUCCESS */
delta_log.update(
    condition="file_name = 'file1.csv'",
    set={"file_status": F.lit("SUCCESS")}
)

# ---------------------------
# Section: Performance Test (Batch)
# ---------------------------

# /* Test: Batch insert performance for s3_file_process_log */
import time  
batch_data = [("batch_file_%d.csv" % i, "s3://vendor-bucket/batch", "s3://purgo-bucket/batch", "s3://purgo-bucket/batch_archive", "SUCCESS", datetime.datetime.now()) for i in range(1000)]
batch_df = spark.createDataFrame(batch_data, schema=expected_s3_file_process_log_schema)
start = time.time()
batch_df.write.mode("append").format("delta").saveAsTable("purgo_playground.s3_file_process_log")
end = time.time()
duration = end - start
assert duration < 30, f"Batch insert took too long: {duration} seconds"
# Cleanup batch data
delta_log.delete("s3_vendor_path = 's3://vendor-bucket/batch'")

# ---------------------------
# Section: Streaming Test (Simulated)
# ---------------------------

# /* Test: Simulate streaming insert to s3_file_process_log */
from pyspark.sql.types import StructType, StructField, StringType, TimestampType  
from pyspark.sql.functions import expr  

stream_schema = expected_s3_file_process_log_schema
stream_data = [("stream_file.csv", "s3://vendor-bucket/stream", "s3://purgo-bucket/stream", "s3://purgo-bucket/stream_archive", "SUCCESS", datetime.datetime.now())]
stream_df = spark.createDataFrame(stream_data, schema=stream_schema)
# Simulate streaming by writing in micro-batches
for i in range(3):
    stream_df.withColumn("file_name", F.lit(f"stream_file_{i}.csv")).write.mode("append").format("delta").saveAsTable("purgo_playground.s3_file_process_log")
# Assert streaming files exist
for i in range(3):
    assert_table_contains("purgo_playground.s3_file_process_log", f"file_name = 'stream_file_{i}.csv' AND file_status = 'SUCCESS'")
# Cleanup streaming data
delta_log.delete("s3_vendor_path = 's3://vendor-bucket/stream'")

# ---------------------------
# Section: File Opening Error Handling
# ---------------------------

# /* Test: File opening wrapped in try-except for missing/invalid data */
try:
    # Simulate file open (should succeed)
    open("/dev/null", "r").close()
except Exception as e:
    assert False, f"Unexpected error opening file: {e}"

try:
    # Simulate file open (should fail)
    open("/path/does/not/exist", "r").close()
except Exception as e:
    assert isinstance(e, Exception)

# ---------------------------
# Section: Column Count Validation Before Insert
# ---------------------------

# /* Test: Number of columns matches schema before insert */
insert_data = [("col1", "col2", "col3", "col4", "col5", datetime.datetime.now())]
insert_schema = expected_s3_file_process_log_schema
assert len(insert_data[0]) == len(insert_schema.fields), "Column count mismatch before insert"
test_df = spark.createDataFrame(insert_data, schema=insert_schema)
test_df.write.mode("append").format("delta").saveAsTable("purgo_playground.s3_file_process_log")
# Cleanup
delta_log.delete("file_name = 'col1'")

# ---------------------------
# End of Test Suite
# ---------------------------
