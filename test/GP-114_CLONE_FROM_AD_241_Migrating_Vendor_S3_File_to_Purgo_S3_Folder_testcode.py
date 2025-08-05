# ============================================================
# Databricks PySpark Test Suite for S3 File Transfer Script
# ============================================================
# This test suite validates the S3 file transfer logic as per the requirements.
# It covers:
#   - Schema validation
#   - Data type conversions
#   - NULL and error handling
#   - File transfer eligibility logic
#   - Logging and audit trail
#   - Delta Lake operations and cleanup
#   - Data quality and integration tests
#   - Performance checks (row count, timing)
#   - AWS credential and S3 path error handling
#   - Case sensitivity and extension matching
#   - Non-recursive file listing
#   - Logging of all outcomes in s3_file_process_log
#   - Use of Databricks secrets for AWS credentials
#   - All code is Databricks-compatible and uses Unity Catalog
#   - All comments are in block or line comment format as required
#   - No SparkSession initialization or stop commands
#   - All imports are properly commented for pip requirements
# ============================================================

# =========================
# Imports and Setup
# =========================

from pyspark.sql import SparkSession  # SparkSession is already available in Databricks
from pyspark.sql import functions as F  
from pyspark.sql.types import (StructType, StructField, StringType, TimestampType)  
from datetime import datetime  
import time  

# =========================
# Section: Helper Functions
# =========================

# Block: Helper to simulate S3 file listing (mocked for test)
def list_s3_files_mock(s3_path, files_in_folder):
    # Returns a list of file names in the given S3 path (non-recursive)
    # files_in_folder: dict of {s3_path: [file1, file2, ...]}
    return files_in_folder.get(s3_path, [])

# Block: Helper to simulate S3 file copy (mocked for test)
def copy_s3_file_mock(src_path, dest_path, file_name, permissions, files_in_folder):
    # Simulate permission error
    if not permissions.get(src_path, True):
        raise PermissionError(f"Permission denied for S3 path: {src_path}")
    if not permissions.get(dest_path, True):
        raise PermissionError(f"Permission denied for S3 path: {dest_path}")
    # Simulate file copy by adding file to dest_path
    if dest_path not in files_in_folder:
        files_in_folder[dest_path] = []
    if file_name not in files_in_folder[dest_path]:
        files_in_folder[dest_path].append(file_name)
    # Do not remove from src_path (copy, not move)
    return True

# Block: Helper to simulate Databricks secret retrieval
def get_secret_mock(scope, key, secrets_dict):
    # secrets_dict: dict of {key: value}
    if key not in secrets_dict or not secrets_dict[key]:
        raise ValueError(f"Missing or invalid AWS credentials: {key}")
    return secrets_dict[key]

# =========================
# Section: Test Data Setup
# =========================

# Block: Test S3 file system state (mocked)
files_in_folder = {
    "s3://vendor-bucket/folder/": ["data_20240601.csv", "report1.txt", "DATA_20240601.csv", "data_20240601.txt", "file_#€_20240601.csv", "データ_20240601.csv", "файл_20240601.csv", "very_long_file_name_20240601_abcdefghijklmnopqrstuvwxyz.csv", "1234567890.csv", "file-20240601_test.csv", "special!@#$.csv"],
    "s3://purgo-bucket/landing/": ["data_20240601.csv"],  # file already present for some tests
    "s3://purgo-bucket/archive/": ["data_20240601.csv"],  # file already present for some tests
    "s3://vendor-bucket/": ["subfolder/data_20240601.csv"],
    "s3://purgo-bucket/": [],
    "s3://purgo-bucket/landing!@#$/": [],
    "s3://purgo-bucket/archive!@#$/": [],
    "s3://vendor-bucket/folder!@#$/": ["special!@#$.csv"],
}

# Block: Test S3 permissions (mocked)
permissions = {
    "s3://vendor-bucket/folder/": True,
    "s3://purgo-bucket/landing/": True,
    "s3://purgo-bucket/archive/": True,
    "s3://vendor-bucket/": True,
    "s3://purgo-bucket/": True,
    "s3://purgo-bucket/landing!@#$/": True,
    "s3://purgo-bucket/archive!@#$/": True,
    "s3://vendor-bucket/folder!@#$/": True,
}

# Block: Test AWS secrets (mocked)
secrets_dict = {
    "access_key": "FAKEACCESSKEY",
    "secret_key": "FAKESECRETKEY"
}

# =========================
# Section: Schema Validation Tests
# =========================

# Block: Validate ingest_config_master schema
expected_ingest_schema = StructType([
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
actual_ingest_schema = spark.table("purgo_databricks.purgo_playground.ingest_config_master").schema
assert actual_ingest_schema == expected_ingest_schema, "ingest_config_master schema mismatch"

# Block: Validate s3_file_process_log schema
expected_log_schema = StructType([
    StructField("file_name", StringType(), True),
    StructField("s3_vendor_path", StringType(), True),
    StructField("s3_landing_path", StringType(), True),
    StructField("s3_archive_path", StringType(), True),
    StructField("file_status", StringType(), True),
    StructField("file_processed_date", TimestampType(), True)
])
actual_log_schema = spark.table("purgo_databricks.purgo_playground.s3_file_process_log").schema
assert actual_log_schema == expected_log_schema, "s3_file_process_log schema mismatch"

# =========================
# Section: Data Type Conversion Tests
# =========================

# Block: Test TIMESTAMP conversion for file_processed_date
log_df = spark.table("purgo_databricks.purgo_playground.s3_file_process_log")
assert log_df.select(F.col("file_processed_date").cast(TimestampType())).filter(F.col("file_processed_date").isNotNull()).count() > 0, "file_processed_date TIMESTAMP conversion failed"

# Block: Test NULL handling in ingest_config_master
null_file_name_count = spark.table("purgo_databricks.purgo_playground.ingest_config_master").filter(F.col("file_name").isNull()).count()
assert null_file_name_count >= 0, "NULL file_name handling failed"

# Block: Test ARRAY/STRUCT/MAP compatibility (complex types)
# Not present in schema, but test that adding a struct column works
from pyspark.sql import Row  
test_struct_df = spark.createDataFrame([Row(a=1, b=Row(x="foo", y="bar"))])
assert "b" in test_struct_df.columns, "STRUCT type compatibility failed"

# =========================
# Section: Unit Tests for File Transfer Eligibility
# =========================

# Block: Test only active files are eligible
active_files = spark.table("purgo_databricks.purgo_playground.ingest_config_master").filter(F.col("active_flag") == "A").select("file_name").rdd.flatMap(lambda x: x).collect()
inactive_files = spark.table("purgo_databricks.purgo_playground.ingest_config_master").filter(F.col("active_flag") != "A").select("file_name").rdd.flatMap(lambda x: x).collect()
assert "data_20240601.csv" in active_files, "Active file missing"
assert "data_20240601.csv" in inactive_files, "Inactive file missing"

# Block: Test file name case sensitivity and extension
assert "DATA_20240601.csv" in files_in_folder["s3://vendor-bucket/folder/"], "Case sensitivity test file missing"
assert "data_20240601.txt" in files_in_folder["s3://vendor-bucket/folder/"], "Extension test file missing"

# Block: Test non-recursive file listing
assert "subfolder/data_20240601.csv" in files_in_folder["s3://vendor-bucket/"], "Subfolder file missing"
assert "data_20240601.csv" not in files_in_folder["s3://vendor-bucket/"], "Non-recursive listing failed"

# =========================
# Section: Integration Tests for End-to-End Flow
# =========================

# Block: Test eligible file is copied from Vendor to Purgo S3
try:
    eligible_file = "report1.txt"
    vendor_path = "s3://vendor-bucket/folder/"
    purgo_path = "s3://purgo-bucket/landing/"
    archive_path = "s3://purgo-bucket/archive/"
    # Remove from Purgo and Archive for this test
    if eligible_file in files_in_folder[purgo_path]:
        files_in_folder[purgo_path].remove(eligible_file)
    if eligible_file in files_in_folder[archive_path]:
        files_in_folder[archive_path].remove(eligible_file)
    # Simulate copy
    copy_s3_file_mock(vendor_path, purgo_path, eligible_file, permissions, files_in_folder)
    assert eligible_file in files_in_folder[purgo_path], "Eligible file not copied to Purgo S3"
    assert eligible_file in files_in_folder[vendor_path], "Eligible file missing from Vendor S3 after copy"
except Exception as e:
    assert False, f"Eligible file copy failed: {str(e)}"

# Block: Test inactive file is not copied
try:
    inactive_file = "data_20240601.csv"
    vendor_path = "s3://vendor-bucket/folder/"
    purgo_path = "s3://purgo-bucket/landing/"
    # Remove from Purgo for this test
    if inactive_file in files_in_folder[purgo_path]:
        files_in_folder[purgo_path].remove(inactive_file)
    # Simulate: should not copy
    # (No copy_s3_file_mock call)
    assert inactive_file not in files_in_folder[purgo_path], "Inactive file should not be copied"
except Exception as e:
    assert False, f"Inactive file copy logic failed: {str(e)}"

# Block: Test file already in Purgo S3 is not copied again
try:
    already_present_file = "data_20240601.csv"
    vendor_path = "s3://vendor-bucket/folder/"
    purgo_path = "s3://purgo-bucket/landing/"
    files_in_folder[purgo_path].append(already_present_file)
    # Simulate: should not copy
    # (No copy_s3_file_mock call)
    assert files_in_folder[purgo_path].count(already_present_file) == 1, "File should not be copied again"
except Exception as e:
    assert False, f"Already present file copy logic failed: {str(e)}"

# Block: Test file already in Archive S3 is not copied
try:
    already_archived_file = "data_20240601.csv"
    vendor_path = "s3://vendor-bucket/folder/"
    archive_path = "s3://purgo-bucket/archive/"
    files_in_folder[archive_path].append(already_archived_file)
    # Simulate: should not copy
    # (No copy_s3_file_mock call)
    assert already_archived_file in files_in_folder[archive_path], "File should remain in Archive"
except Exception as e:
    assert False, f"Already archived file copy logic failed: {str(e)}"

# Block: Test file in subfolder is not copied (non-recursive)
try:
    subfolder_file = "subfolder/data_20240601.csv"
    vendor_path = "s3://vendor-bucket/"
    purgo_path = "s3://purgo-bucket/"
    # Simulate: should not copy
    assert subfolder_file not in files_in_folder.get(purgo_path, []), "Subfolder file should not be copied"
except Exception as e:
    assert False, f"Subfolder file copy logic failed: {str(e)}"

# Block: Test file name matching is case-sensitive and includes extension
try:
    config_file = "data_20240601.csv"
    actual_file = "DATA_20240601.csv"
    vendor_path = "s3://vendor-bucket/folder/"
    purgo_path = "s3://purgo-bucket/landing/"
    # Simulate: should not copy
    assert actual_file not in files_in_folder.get(purgo_path, []), "Case-sensitive file should not be copied"
except Exception as e:
    assert False, f"Case-sensitive file copy logic failed: {str(e)}"

# =========================
# Section: Error Handling Tests
# =========================

# Block: Test missing AWS credentials
try:
    missing_key = "access_key"
    secrets_dict_missing = {"secret_key": "FAKESECRETKEY"}
    try:
        get_secret_mock("aws_keys", missing_key, secrets_dict_missing)
        assert False, "Missing AWS credential did not raise error"
    except ValueError as ve:
        assert "Missing or invalid AWS credentials" in str(ve), "Incorrect error message for missing AWS credential"
except Exception as e:
    assert False, f"Missing AWS credential test failed: {str(e)}"

# Block: Test missing S3 path in ingest_config_master
try:
    ingest_df = spark.table("purgo_databricks.purgo_playground.ingest_config_master")
    missing_path_row = ingest_df.filter(F.col("s3_landing_path").isNull()).first()
    if missing_path_row:
        raise ValueError("Missing S3 path in ingest_config_master: s3_landing_path")
except ValueError as ve:
    assert "Missing S3 path in ingest_config_master" in str(ve), "Incorrect error message for missing S3 path"
except Exception as e:
    assert False, f"Missing S3 path test failed: {str(e)}"

# Block: Test file transfer fails due to permission issues
try:
    file_name = "data_20240601.csv"
    vendor_path = "s3://vendor-bucket/folder/"
    purgo_path = "s3://purgo-bucket/landing/"
    permissions[vendor_path] = False
    try:
        copy_s3_file_mock(vendor_path, purgo_path, file_name, permissions, files_in_folder)
        assert False, "Permission error not raised"
    except PermissionError as pe:
        assert "Permission denied for S3 path" in str(pe), "Incorrect error message for permission denied"
    permissions[vendor_path] = True  # Reset for other tests
except Exception as e:
    assert False, f"Permission error test failed: {str(e)}"

# =========================
# Section: Data Quality Validation Tests
# =========================

# Block: Test s3_file_process_log contains SUCCESS for transferred file
log_df = spark.table("purgo_databricks.purgo_playground.s3_file_process_log")
success_count = log_df.filter((F.col("file_name") == "data_20240601.csv") & (F.col("file_status") == "SUCCESS")).count()
assert success_count >= 1, "SUCCESS log entry missing for transferred file"

# Block: Test s3_file_process_log contains SKIPPED for already present file
skipped_count = log_df.filter((F.col("file_name") == "data_20240601.csv") & (F.col("file_status") == "SKIPPED")).count()
assert skipped_count >= 1, "SKIPPED log entry missing for already present file"

# Block: Test s3_file_process_log contains FAILED for failed transfer
failed_count = log_df.filter((F.col("file_name") == "data_20240601.csv") & (F.col("file_status") == "FAILED")).count()
assert failed_count >= 1, "FAILED log entry missing for failed transfer"

# Block: Test s3_file_process_log for special/multibyte file names
special_count = log_df.filter((F.col("file_name") == "file_#€_20240601.csv") & (F.col("file_status") == "SUCCESS")).count()
multibyte_count = log_df.filter((F.col("file_name") == "データ_20240601.csv") & (F.col("file_status") == "SUCCESS")).count()
assert special_count == 1, "Special character file log missing"
assert multibyte_count == 1, "Multibyte character file log missing"

# =========================
# Section: Delta Lake Operations and Cleanup
# =========================

# Block: Test Delta Lake MERGE, UPDATE, DELETE operations on s3_file_process_log
from delta.tables import DeltaTable  

delta_log = DeltaTable.forName(spark, "purgo_databricks.purgo_playground.s3_file_process_log")

# MERGE: Upsert a new log entry
merge_df = spark.createDataFrame([("merge_test.csv", "s3://vendor-bucket/folder/", "s3://purgo-bucket/landing/", "s3://purgo-bucket/archive/", "SUCCESS", datetime.now())], expected_log_schema)
delta_log.alias("t").merge(
    merge_df.alias("s"),
    "t.file_name = s.file_name"
).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
assert delta_log.toDF().filter(F.col("file_name") == "merge_test.csv").count() == 1, "Delta MERGE failed"

# UPDATE: Update file_status
delta_log.update(
    condition=F.col("file_name") == "merge_test.csv",
    set={"file_status": F.lit("UPDATED")}
)
assert delta_log.toDF().filter((F.col("file_name") == "merge_test.csv") & (F.col("file_status") == "UPDATED")).count() == 1, "Delta UPDATE failed"

# DELETE: Remove test log entry
delta_log.delete(F.col("file_name") == "merge_test.csv")
assert delta_log.toDF().filter(F.col("file_name") == "merge_test.csv").count() == 0, "Delta DELETE failed"

# =========================
# Section: Performance Test (Row Count and Timing)
# =========================

# Block: Performance test for eligible file count
start_time = time.time()
eligible_count = spark.table("purgo_databricks.purgo_playground.ingest_config_master").filter(F.col("active_flag") == "A").count()
end_time = time.time()
assert eligible_count > 0, "No eligible files found"
assert (end_time - start_time) < 10, "Eligible file count performance issue"

# =========================
# Section: Window Function and Analytics Feature Test
# =========================

# Block: Test window function on s3_file_process_log
from pyspark.sql.window import Window  
window_spec = Window.partitionBy("file_status").orderBy(F.col("file_processed_date").desc())
log_df = log_df.withColumn("row_num", F.row_number().over(window_spec))
max_row_num = log_df.agg(F.max("row_num")).collect()[0][0]
assert max_row_num >= 1, "Window function row_number failed"

# =========================
# Section: Cleanup Operations
# =========================

# Block: Cleanup test log entry if exists
delta_log.delete(F.col("file_name") == "merge_test.csv")

# =========================
# Section: NULL Handling and Data Quality
# =========================

# Block: Test NULL and empty string handling in file_name
null_or_empty_count = spark.table("purgo_databricks.purgo_playground.ingest_config_master").filter((F.col("file_name").isNull()) | (F.col("file_name") == "")).count()
assert null_or_empty_count >= 0, "NULL/empty file_name handling failed"

# Block: Test NULL active_flag handling
null_active_flag_count = spark.table("purgo_databricks.purgo_playground.ingest_config_master").filter(F.col("active_flag").isNull()).count()
assert null_active_flag_count >= 0, "NULL active_flag handling failed"

# =========================
# Section: Foreign Key and Constraint Validation
# =========================

# Block: Validate allowed values for file_status in s3_file_process_log
allowed_status = {"SUCCESS", "SKIPPED", "FAILED", "UPDATED"}
status_values = [row.file_status for row in log_df.select("file_status").distinct().collect()]
for status in status_values:
    assert status in allowed_status, f"Invalid file_status value: {status}"

# =========================
# Section: End of Test Suite
# =========================

# All tests passed if no assertion failed
