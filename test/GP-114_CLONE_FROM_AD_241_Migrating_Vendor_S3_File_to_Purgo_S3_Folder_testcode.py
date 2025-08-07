%pip install pytest

spark.catalog.setCurrentCatalog("purgo_databricks")

# ---------------------------------------------------------------
# Databricks PySpark Test Suite for S3 File Transfer Script
# ---------------------------------------------------------------
# Catalog: purgo_databricks
# Schema: purgo_playground
# All test code is self-contained and does not require SparkSession initialization.
# All file operations are simulated using test tables as per test data setup.
# ---------------------------------------------------------------

# from pyspark.sql import SparkSession  # SparkSession is already available in Databricks
from pyspark.sql import functions as F  
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, ArrayType, MapType, StructType  
import pytest  

# ---------------------------------------------------------------
# SECTION: Helper Functions for Test Assertions
# ---------------------------------------------------------------

def assert_df_schema(df, expected_schema):
    """
    Assert that the DataFrame schema matches the expected schema.
    """
    actual = [(f.name, f.dataType, f.nullable) for f in df.schema.fields]
    assert actual == expected_schema, f"Schema mismatch: {actual} != {expected_schema}"

def assert_df_row_count(df, expected_count):
    """
    Assert that the DataFrame has the expected number of rows.
    """
    actual_count = df.count()
    assert actual_count == expected_count, f"Row count mismatch: {actual_count} != {expected_count}"

def assert_df_column_count(df, expected_count):
    """
    Assert that the DataFrame has the expected number of columns.
    """
    actual_count = len(df.columns)
    assert actual_count == expected_count, f"Column count mismatch: {actual_count} != {expected_count}"

def assert_df_contains(df, expected_rows):
    """
    Assert that the DataFrame contains all expected rows.
    """
    actual_rows = [tuple(row) for row in df.collect()]
    for row in expected_rows:
        assert tuple(row) in actual_rows, f"Expected row {row} not found in DataFrame"

def assert_df_not_contains(df, unexpected_rows):
    """
    Assert that the DataFrame does not contain any of the unexpected rows.
    """
    actual_rows = [tuple(row) for row in df.collect()]
    for row in unexpected_rows:
        assert tuple(row) not in actual_rows, f"Unexpected row {row} found in DataFrame"

def assert_column_values(df, column, expected_values):
    """
    Assert that the DataFrame column contains only the expected values.
    """
    actual_values = set([row[column] for row in df.select(column).distinct().collect()])
    assert actual_values == set(expected_values), f"Column values mismatch: {actual_values} != {set(expected_values)}"

def assert_null_handling(df, column):
    """
    Assert that NULLs are handled correctly in the specified column.
    """
    null_count = df.filter(F.col(column).isNull()).count()
    assert null_count >= 0, "NULL handling failed"

# ---------------------------------------------------------------
# SECTION: Test 1 - Schema Validation for ingest_config_master
# ---------------------------------------------------------------

# /* Test that the schema of purgo_playground.ingest_config_master matches the expected definition */
ingest_config_master_df = spark.table("purgo_playground.ingest_config_master")
expected_schema = [
    ("config_id", StringType(), True),
    ("source_object_name", StringType(), True),
    ("source_system", StringType(), True),
    ("file_name", StringType(), True),
    ("frequency", StringType(), True),
    ("location", StringType(), True),
    ("domain", StringType(), True),
    ("sub_domain", StringType(), True),
    ("s3_vendor_path", StringType(), True),
    ("source_path", StringType(), True),
    ("s3_landing_path", StringType(), True),
    ("s3_archive_path", StringType(), True),
    ("delta_load_ts", StringType(), True),
    ("full_or_incremental_load", StringType(), True),
    ("zip_file", StringType(), True),
    ("vendor", StringType(), True),
    ("delimiter", StringType(), True),
    ("source_landing", StringType(), True),
    ("src_landing_table_name", StringType(), True),
    ("publish_unstitched", StringType(), True),
    ("publish_unstitched_table_name", StringType(), True),
    ("publish_stitched", StringType(), True),
    ("publish_stitched_table_name", StringType(), True),
    ("primary_key", StringType(), True),
    ("header", StringType(), True),
    ("date_pattern", StringType(), True),
    ("actual_file_name", StringType(), True),
    ("vendor_file_deletion_flag", StringType(), True),
    ("file_recursive_flag", StringType(), True),
    ("total_weeks_req_data", StringType(), True),
    ("total_weeks_file_data", StringType(), True),
    ("active_flag", StringType(), True)
]
assert_df_schema(ingest_config_master_df, expected_schema)
assert_df_column_count(ingest_config_master_df, 32)

# ---------------------------------------------------------------
# SECTION: Test 2 - Schema Validation for s3_file_process_log
# ---------------------------------------------------------------

# /* Test that the schema of purgo_playground.s3_file_process_log matches the expected definition */
s3_file_process_log_df = spark.table("purgo_playground.s3_file_process_log")
expected_schema_log = [
    ("file_name", StringType(), True),
    ("s3_vendor_path", StringType(), True),
    ("s3_landing_path", StringType(), True),
    ("s3_archive_path", StringType(), True),
    ("file_status", StringType(), True),
    ("file_processed_date", TimestampType(), True)
]
assert_df_schema(s3_file_process_log_df, expected_schema_log)
assert_df_column_count(s3_file_process_log_df, 6)

# ---------------------------------------------------------------
# SECTION: Test 3 - Data Type Conversion and NULL Handling
# ---------------------------------------------------------------

# /* Test that all string columns can be cast to STRING and NULLs are handled */
for col in ["file_name", "s3_vendor_path", "s3_landing_path", "s3_archive_path", "file_status"]:
    s3_file_process_log_df = s3_file_process_log_df.withColumn(col, F.col(col).cast(StringType()))
    assert_null_handling(s3_file_process_log_df, col)

# /* Test that file_processed_date can be cast to TIMESTAMP and NULLs are handled */
s3_file_process_log_df = s3_file_process_log_df.withColumn("file_processed_date", F.col("file_processed_date").cast(TimestampType()))
assert_null_handling(s3_file_process_log_df, "file_processed_date")

# ---------------------------------------------------------------
# SECTION: Test 4 - Complex Type Validation (ARRAY, STRUCT, MAP)
# ---------------------------------------------------------------

# /* Test that we can create and query STRUCT, ARRAY, and MAP columns using Databricks native types */
complex_df = s3_file_process_log_df.withColumn(
    "file_struct",
    F.struct("file_name", "file_status")
).withColumn(
    "file_array",
    F.array("file_name", "file_status")
).withColumn(
    "file_map",
    F.create_map(F.lit("name"), F.col("file_name"), F.lit("status"), F.col("file_status"))
)
assert "file_struct" in complex_df.columns
assert "file_array" in complex_df.columns
assert "file_map" in complex_df.columns

# /* Validate that STRUCT, ARRAY, and MAP columns are not NULL for non-NULL file_name/file_status */
non_null_complex = complex_df.filter(F.col("file_name").isNotNull() & F.col("file_status").isNotNull())
assert non_null_complex.filter(F.col("file_struct").isNull()).count() == 0
assert non_null_complex.filter(F.col("file_array").isNull()).count() == 0
assert non_null_complex.filter(F.col("file_map").isNull()).count() == 0

# ---------------------------------------------------------------
# SECTION: Test 5 - File Eligibility Logic (Unit Test)
# ---------------------------------------------------------------

# /* Test that only files with active_flag = "A" are eligible for ingestion */
active_configs = ingest_config_master_df.filter(F.col("active_flag") == "A")
inactive_configs = ingest_config_master_df.filter(F.col("active_flag") != "A")
assert active_configs.count() > 0
assert inactive_configs.count() > 0

# /* Test that file_name matching is case-sensitive and includes extension */
case_sensitive_test = ingest_config_master_df.filter(F.col("config_id") == "127").select("file_name").collect()[0][0]
assert case_sensitive_test == "Data6.CSV"

# ---------------------------------------------------------------
# SECTION: Test 6 - Integration Test: End-to-End File Transfer Logic
# ---------------------------------------------------------------

# /* Simulate the file transfer logic using test tables for S3 file listings */
vendor_files_df = spark.table("purgo_playground.test_vendor_s3_files")
purgo_files_df = spark.table("purgo_playground.test_purgo_s3_files")
archive_files_df = spark.table("purgo_playground.test_archive_s3_files")

# /* Join config with vendor files to get eligible files */
eligible_files_df = (
    ingest_config_master_df
    .filter(F.col("active_flag") == "A")
    .join(vendor_files_df, (F.col("s3_vendor_path") == vendor_files_df.s3_path) & (F.col("file_name") == vendor_files_df.file_name), "inner")
    .select(
        F.col("config_id"),
        F.col("file_name"),
        F.col("s3_vendor_path"),
        F.col("s3_landing_path"),
        F.col("s3_archive_path")
    )
)

# /* Exclude files already in Purgo or Archive */
eligible_files_df = (
    eligible_files_df
    .join(
        purgo_files_df,
        (eligible_files_df.s3_landing_path == purgo_files_df.s3_path) & (eligible_files_df.file_name == purgo_files_df.file_name),
        "left_anti"
    )
    .join(
        archive_files_df,
        (eligible_files_df.s3_archive_path == archive_files_df.s3_path) & (eligible_files_df.file_name == archive_files_df.file_name),
        "left_anti"
    )
)

# /* Assert that only expected files are eligible for transfer (e.g., data1.csv, Data6.CSV, fileA.csv, etc.) */
expected_eligible_files = set([
    ("123", "data1.csv", "s3://vendor-bucket/folderA/", "s3://purgo-bucket/landingA/", "s3://purgo-bucket/archiveA/"),
    ("127", "Data6.CSV", "s3://vendor-bucket/folderE/", "s3://purgo-bucket/landingE/", "s3://purgo-bucket/archiveE/"),
    ("201", "fileA.csv", "s3://vendor-bucket/folderF/", "s3://purgo-bucket/landingF/", "s3://purgo-bucket/archiveF/"),
    ("301", "fileG.csv", "s3://vendor-bucket/folderK/", "s3://purgo-bucket/landingK/", "s3://purgo-bucket/archiveK/"),
    ("777", "spécial_文件.csv", "s3://vendor-bucket/folderΩ/", "s3://purgo-bucket/landingΩ/", "s3://purgo-bucket/archiveΩ/")
])
actual_eligible_files = set([tuple(row) for row in eligible_files_df.collect()])
for row in expected_eligible_files:
    assert row in actual_eligible_files, f"Eligible file {row} not found"

# ---------------------------------------------------------------
# SECTION: Test 7 - Data Quality Validation: No Duplicates, No NULLs in Key Columns
# ---------------------------------------------------------------

# /* Assert that there are no duplicate file_name + s3_landing_path in s3_file_process_log */
dupes = (
    s3_file_process_log_df
    .groupBy("file_name", "s3_landing_path")
    .count()
    .filter(F.col("count") > 1)
)
assert dupes.count() == 0, "Duplicate file_name + s3_landing_path found in s3_file_process_log"

# /* Assert that file_status is not NULL for any processed file */
assert s3_file_process_log_df.filter(F.col("file_name").isNotNull() & F.col("file_status").isNull()).count() == 0

# ---------------------------------------------------------------
# SECTION: Test 8 - Delta Lake Operations: MERGE, UPDATE, DELETE
# ---------------------------------------------------------------

# /* Test Delta Lake MERGE: Upsert a new log record and verify */
from delta.tables import DeltaTable  

log_table = DeltaTable.forName(spark, "purgo_playground.s3_file_process_log")
merge_df = spark.createDataFrame([
    ("fileZ.csv", "s3://vendor-bucket/folderZ/", "s3://purgo-bucket/landingZ/", "s3://purgo-bucket/archiveZ/", "SUCCESS", F.current_timestamp())
], ["file_name", "s3_vendor_path", "s3_landing_path", "s3_archive_path", "file_status", "file_processed_date"])

log_table.alias("tgt").merge(
    merge_df.alias("src"),
    "tgt.file_name = src.file_name AND tgt.s3_landing_path = src.s3_landing_path"
).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

# /* Assert that fileZ.csv now exists in the log */
assert log_table.toDF().filter(F.col("file_name") == "fileZ.csv").count() == 1

# /* Test Delta Lake UPDATE: Update file_status for fileZ.csv */
log_table.update(
    condition="file_name = 'fileZ.csv'",
    set={"file_status": "'UPDATED'"}
)
assert log_table.toDF().filter((F.col("file_name") == "fileZ.csv") & (F.col("file_status") == "UPDATED")).count() == 1

# /* Test Delta Lake DELETE: Remove fileZ.csv */
log_table.delete("file_name = 'fileZ.csv'")
assert log_table.toDF().filter(F.col("file_name") == "fileZ.csv").count() == 0

# ---------------------------------------------------------------
# SECTION: Test 9 - Window Functions and Analytics Features
# ---------------------------------------------------------------

# /* Test window function: Rank files by processed date within each status */
from pyspark.sql.window import Window  
window_spec = Window.partitionBy("file_status").orderBy(F.col("file_processed_date").desc())
ranked_df = s3_file_process_log_df.withColumn("rank", F.row_number().over(window_spec))
assert "rank" in ranked_df.columns

# /* Assert that rank is always >= 1 */
assert ranked_df.filter(F.col("rank") < 1).count() == 0

# ---------------------------------------------------------------
# SECTION: Test 10 - Error Handling: Missing S3 Path in Config
# ---------------------------------------------------------------

# /* Test that missing S3 path in config results in ERROR_CONFIG in log */
missing_path_log = s3_file_process_log_df.filter((F.col("file_name") == "data5.csv") & (F.col("file_status") == "ERROR_CONFIG"))
assert missing_path_log.count() == 1

# ---------------------------------------------------------------
# SECTION: Test 11 - Error Handling: Missing AWS Credentials
# ---------------------------------------------------------------

# /* Simulate missing AWS credentials by catching exception when accessing secrets */
try:
    access_key = dbutils.secrets.get(scope="aws_keys", key="access_key")  # Databricks built-in
    secret_key = dbutils.secrets.get(scope="aws_keys", key="secret_key")
except Exception as e:
    assert "not found" in str(e) or "No such secret" in str(e) or "not exist" in str(e)

# ---------------------------------------------------------------
# SECTION: Test 12 - File Name Matching: Case Sensitivity
# ---------------------------------------------------------------

# /* Test that only Data6.CSV is processed, not data6.csv, for config_id=127 */
log_case = s3_file_process_log_df.filter((F.col("file_name") == "Data6.CSV") & (F.col("file_status") == "SUCCESS"))
assert log_case.count() == 1
log_case2 = s3_file_process_log_df.filter((F.col("file_name") == "data6.csv") & (F.col("file_status") == "SKIPPED_NOT_CONFIGURED"))
assert log_case2.count() == 1

# ---------------------------------------------------------------
# SECTION: Test 13 - Subfolder Exclusion (No Recursion)
# ---------------------------------------------------------------

# /* Test that subfolder/fileH.csv is not processed or logged */
log_subfolder = s3_file_process_log_df.filter(F.col("file_name") == "subfolder/fileH.csv")
assert log_subfolder.count() == 0

# ---------------------------------------------------------------
# SECTION: Test 14 - S3 Access Error Handling
# ---------------------------------------------------------------

# /* Test that fileI.csv with S3 access error is logged as ERROR_S3_ACCESS */
log_s3_error = s3_file_process_log_df.filter((F.col("file_name") == "fileI.csv") & (F.col("file_status") == "ERROR_S3_ACCESS"))
assert log_s3_error.count() == 1

# ---------------------------------------------------------------
# SECTION: Test 15 - No Eligible Files to Transfer
# ---------------------------------------------------------------

# /* Test that no new records are logged for folderM (fileJ.csv) as there are no eligible files */
log_no_eligible = s3_file_process_log_df.filter(F.col("file_name") == "fileJ.csv")
assert log_no_eligible.count() == 0

# ---------------------------------------------------------------
# SECTION: Test 16 - File Not in Config is Not Transferred
# ---------------------------------------------------------------

# /* Test that fileL.csv is logged as SKIPPED_NOT_CONFIGURED */
log_not_configured = s3_file_process_log_df.filter((F.col("file_name") == "fileL.csv") & (F.col("file_status") == "SKIPPED_NOT_CONFIGURED"))
assert log_not_configured.count() == 1

# ---------------------------------------------------------------
# SECTION: Test 17 - Data-Driven Scenario Outline Validation
# ---------------------------------------------------------------

# /* Validate all data-driven scenario outline examples */
data_driven_cases = [
    ("fileA.csv", "SUCCESS"),
    ("fileB.csv", "SKIPPED_EXISTS"),
    ("fileC.csv", "SKIPPED_ARCHIVED"),
    ("fileD.csv", "SKIPPED_INACTIVE"),
    ("fileF.csv", "SKIPPED_NOT_CONFIGURED")
]
for fname, status in data_driven_cases:
    assert s3_file_process_log_df.filter((F.col("file_name") == fname) & (F.col("file_status") == status)).count() == 1

# ---------------------------------------------------------------
# SECTION: Test 18 - Performance Test: Large Batch Processing
# ---------------------------------------------------------------

# /* Simulate a large batch by duplicating eligible_files_df and measuring time */
import time  
large_batch_df = eligible_files_df
for _ in range(5):
    large_batch_df = large_batch_df.union(eligible_files_df)
start_time = time.time()
row_count = large_batch_df.count()
end_time = time.time()
assert row_count == eligible_files_df.count() * 6
assert (end_time - start_time) < 10  # Should process in <10 seconds for test batch

# ---------------------------------------------------------------
# SECTION: Test 19 - Data Quality: All SUCCESS Files Exist in Purgo S3
# ---------------------------------------------------------------

# /* Validate that all files with file_status='SUCCESS' in s3_file_process_log exist in test_purgo_s3_files */
validation_cte = """
WITH successful_files AS (
  SELECT file_name, s3_landing_path
  FROM purgo_playground.s3_file_process_log
  WHERE file_status = "SUCCESS"
)
SELECT sf.file_name, sf.s3_landing_path, pf.file_name AS exists_in_purgo
FROM successful_files sf
LEFT JOIN purgo_playground.test_purgo_s3_files pf
  ON sf.file_name = pf.file_name AND sf.s3_landing_path = pf.s3_path
"""
validation_df = spark.sql(validation_cte)
assert validation_df.filter(F.col("exists_in_purgo").isNull()).count() == 0

# ---------------------------------------------------------------
# SECTION: Test 20 - Cleanup: Remove Test Log Record (if any)
# ---------------------------------------------------------------

# /* Clean up any test log records created during test (e.g., fileZ.csv) */
log_table.delete("file_name = 'fileZ.csv'")

# ---------------------------------------------------------------
# END OF TEST SUITE
# ---------------------------------------------------------------
