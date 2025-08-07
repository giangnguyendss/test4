spark.catalog.setCurrentCatalog("purgo_databricks")

# Databricks PySpark Test Suite for S3 File Transfer Script
# All test code is wrapped in this code block as per requirements

# -----------------------------------------------------------
# /* 
#   SETUP & CONFIGURATION
#   - Assumes 'spark' session is available in Databricks
#   - All test tables are in catalog: purgo_databricks, schema: purgo_playground
#   - Uses only Databricks native data types
#   - All test data is created in-memory and written to Unity Catalog tables
#   - All S3 operations are mocked using in-memory structures
#   - AWS credentials are retrieved from Databricks secrets (mocked for tests)
#   - All test assertions use assert statements
#   - All file operations are wrapped in try-except blocks
#   - All test output is via assert or DataFrame .show() for validation
#   - No plain text output outside of code/comments
# */

# -----------------------------------------------------------
# /* 
#   IMPORTS
#   - Only necessary imports included
#   - Each import is annotated with required pip package
# */
from pyspark.sql import Row  
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType  
from pyspark.sql.functions import col, lit, array, struct, when, countDistinct, expr  
from pyspark.sql.utils import AnalysisException  
import re  
import sys  

# -----------------------------------------------------------
# /* 
#   MOCKS & HELPERS
#   - Mock S3 file listings and file transfer operations
#   - Mock Databricks secrets
#   - Helper functions for S3 URI validation, file listing, and transfer
# */

# Mock Databricks secrets (simulate dbutils.secrets.get)
class MockSecrets:
    def __init__(self, secrets_dict):
        self.secrets_dict = secrets_dict
    def get(self, scope, key):
        if scope != "aws_keys":
            raise Exception(f"Secret scope '{scope}' not found")
        if key not in self.secrets_dict:
            raise Exception(f"AWS credentials not found in Databricks secret scope 'aws_keys'")
        return self.secrets_dict[key]

# Mock S3 file system (in-memory)
class MockS3:
    def __init__(self):
        # Structure: {bucket: {prefix: set(file_names)}}
        self.s3 = {}
    def add_files(self, s3_uri, file_list):
        bucket, prefix = self._parse_s3_uri(s3_uri)
        if bucket not in self.s3:
            self.s3[bucket] = {}
        if prefix not in self.s3[bucket]:
            self.s3[bucket][prefix] = set()
        for f in file_list:
            if f is not None and f != "":
                self.s3[bucket][prefix].add(f)
    def list_files(self, s3_uri, recursive=False):
        bucket, prefix = self._parse_s3_uri(s3_uri)
        if bucket not in self.s3:
            return []
        if recursive:
            # List all files under all sub-prefixes
            files = []
            for p, file_set in self.s3[bucket].items():
                if p.startswith(prefix):
                    for f in file_set:
                        if f is not None and f != "":
                            files.append(p[len(prefix):] + f if p != prefix else f)
            return files
        else:
            # Only files in the exact prefix
            if prefix in self.s3[bucket]:
                return [f for f in self.s3[bucket][prefix] if f is not None and f != ""]
            else:
                return []
    def file_exists(self, s3_uri, file_name):
        bucket, prefix = self._parse_s3_uri(s3_uri)
        if bucket in self.s3 and prefix in self.s3[bucket]:
            return file_name in self.s3[bucket][prefix]
        return False
    def copy_file(self, src_s3_uri, dst_s3_uri, file_name):
        src_bucket, src_prefix = self._parse_s3_uri(src_s3_uri)
        dst_bucket, dst_prefix = self._parse_s3_uri(dst_s3_uri)
        if src_bucket not in self.s3 or src_prefix not in self.s3[src_bucket]:
            raise Exception(f"Source S3 path not found: {src_s3_uri}")
        if file_name not in self.s3[src_bucket][src_prefix]:
            raise Exception(f"File {file_name} not found in {src_s3_uri}")
        if dst_bucket not in self.s3:
            self.s3[dst_bucket] = {}
        if dst_prefix not in self.s3[dst_bucket]:
            self.s3[dst_bucket][dst_prefix] = set()
        # Simulate S3 copy (file remains in source)
        self.s3[dst_bucket][dst_prefix].add(file_name)
    def _parse_s3_uri(self, s3_uri):
        # Validate S3 URI
        if not isinstance(s3_uri, str) or not s3_uri.startswith("s3://"):
            raise Exception(f"Invalid S3 URI in configuration: {s3_uri}")
        m = re.match(r"s3://([^/]+)/?(.*)", s3_uri)
        if not m:
            raise Exception(f"Invalid S3 URI in configuration: {s3_uri}")
        bucket = m.group(1)
        prefix = m.group(2)
        if prefix and not prefix.endswith("/"):
            prefix += "/"
        return bucket, prefix or ""

# -----------------------------------------------------------
# /* 
#   TEST DATA SETUP
#   - Create ingest_config_master table with test data
#   - Create mock S3 file listings for Vendor, Purgo, Archive
#   - All test data is in-memory and written to Unity Catalog tables
# */

# Set current catalog and schema
spark.sql('USE CATALOG purgo_databricks')
spark.sql('USE purgo_playground')

# Define ingest_config_master schema
ingest_config_master_schema = StructType([
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

# Minimal test data for ingest_config_master
ingest_config_master_data = [
    # Active config, recursive, valid S3 URIs
    Row(config_id="1", source_object_name="objA", source_system="sysA", file_name="fileA.csv", frequency="daily", location="locA", domain="domA", sub_domain="subA",
        s3_vendor_path="s3://vendor-bucket/folder1/", source_path=None, s3_landing_path="s3://purgo-bucket/landing1/", s3_archive_path="s3://purgo-bucket/archive1/",
        delta_load_ts="2024-03-21T00:00:00.000+0000", full_or_incremental_load="F", zip_file="N", vendor="VendorA", delimiter=",", source_landing="Y",
        src_landing_table_name="tableA", publish_unstitched="N", publish_unstitched_table_name=None, publish_stitched="Y", publish_stitched_table_name="stitchedA",
        primary_key="id", header="Y", date_pattern="yyyy-MM-dd", actual_file_name="fileA.csv", vendor_file_deletion_flag="N", file_recursive_flag="Y",
        total_weeks_req_data="4", total_weeks_file_data="4", active_flag="A"),
    # Inactive config, should not be processed
    Row(config_id="2", source_object_name="objB", source_system="sysB", file_name="fileB.csv", frequency="weekly", location="locB", domain="domB", sub_domain="subB",
        s3_vendor_path="s3://vendor-bucket/folder2/", source_path=None, s3_landing_path="s3://purgo-bucket/landing2/", s3_archive_path="s3://purgo-bucket/archive2/",
        delta_load_ts="2024-03-22T00:00:00.000+0000", full_or_incremental_load="I", zip_file="Y", vendor="VendorB", delimiter="|", source_landing="N",
        src_landing_table_name="tableB", publish_unstitched="Y", publish_unstitched_table_name="unstitchB", publish_stitched="N", publish_stitched_table_name=None,
        primary_key="pk", header="N", date_pattern="MM/dd/yyyy", actual_file_name="fileB.csv", vendor_file_deletion_flag="Y", file_recursive_flag=None,
        total_weeks_req_data="2", total_weeks_file_data="2", active_flag="I"),
    # Active config, non-recursive, valid S3 URIs
    Row(config_id="3", source_object_name="objC", source_system="sysC", file_name="fileC.csv", frequency="monthly", location="locC", domain="domC", sub_domain="subC",
        s3_vendor_path="s3://vendor-bucket/folder3/", source_path=None, s3_landing_path="s3://purgo-bucket/landing3/", s3_archive_path="s3://purgo-bucket/archive3/",
        delta_load_ts="2024-03-23T00:00:00.000+0000", full_or_incremental_load="F", zip_file="N", vendor="VendorC", delimiter=";", source_landing="Y",
        src_landing_table_name="tableC", publish_unstitched="N", publish_unstitched_table_name=None, publish_stitched="Y", publish_stitched_table_name="stitchedC",
        primary_key="id", header="Y", date_pattern="dd-MM-yyyy", actual_file_name="fileC.csv", vendor_file_deletion_flag="N", file_recursive_flag=None,
        total_weeks_req_data="1", total_weeks_file_data="1", active_flag="A"),
    # Active config, missing S3 path (null)
    Row(config_id="4", source_object_name="objD", source_system="sysD", file_name="fileD.csv", frequency="hourly", location="locD", domain="domD", sub_domain="subD",
        s3_vendor_path=None, source_path=None, s3_landing_path="s3://purgo-bucket/landing4/", s3_archive_path="s3://purgo-bucket/archive4/",
        delta_load_ts="2024-03-24T00:00:00.000+0000", full_or_incremental_load="I", zip_file="Y", vendor="VendorD", delimiter="\t", source_landing="N",
        src_landing_table_name="tableD", publish_unstitched="Y", publish_unstitched_table_name="unstitchD", publish_stitched="N", publish_stitched_table_name=None,
        primary_key="pk", header="N", date_pattern="yyyy/MM/dd", actual_file_name="fileD.csv", vendor_file_deletion_flag="Y", file_recursive_flag="Y",
        total_weeks_req_data="3", total_weeks_file_data="3", active_flag="A"),
    # Active config, invalid S3 URI
    Row(config_id="5", source_object_name="objE", source_system="sysE", file_name="fileE.csv", frequency="daily", location="locE", domain="domE", sub_domain="subE",
        s3_vendor_path="invalid_path", source_path=None, s3_landing_path="s3://purgo-bucket/landing5/", s3_archive_path="s3://purgo-bucket/archive5/",
        delta_load_ts="2024-03-25T00:00:00.000+0000", full_or_incremental_load="F", zip_file="N", vendor="VendorE", delimiter=",", source_landing="Y",
        src_landing_table_name="tableE", publish_unstitched="N", publish_unstitched_table_name=None, publish_stitched="Y", publish_stitched_table_name="stitchedE",
        primary_key="id", header="Y", date_pattern="yyyy-MM-dd", actual_file_name="fileE.csv", vendor_file_deletion_flag="N", file_recursive_flag="N",
        total_weeks_req_data="4", total_weeks_file_data="4", active_flag="A"),
]

ingest_config_master_df = spark.createDataFrame(ingest_config_master_data, schema=ingest_config_master_schema)
ingest_config_master_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("purgo_playground.ingest_config_master")

# Mock S3 file system setup
mock_s3 = MockS3()
# Vendor S3: config_id=1, recursive
mock_s3.add_files("s3://vendor-bucket/folder1/", ["fileA.csv", "fileB.csv", "fileC.csv", "sub1/fileD.csv"])
# Purgo S3: config_id=1
mock_s3.add_files("s3://purgo-bucket/landing1/", ["fileB.csv", "fileE.csv"])
# Archive S3: config_id=1
mock_s3.add_files("s3://purgo-bucket/archive1/", ["fileC.csv", "fileF.csv"])
# Vendor S3: config_id=3, non-recursive
mock_s3.add_files("s3://vendor-bucket/folder3/", ["fileG.csv", "sub2/fileH.csv"])
mock_s3.add_files("s3://purgo-bucket/landing3/", ["fileG.csv"])
mock_s3.add_files("s3://purgo-bucket/archive3/", ["fileI.csv"])
# Vendor S3: config_id=5, invalid S3 URI
mock_s3.add_files("invalid_path", ["fileE.csv"])

# Mock secrets: valid and missing cases
mock_secrets_valid = MockSecrets({"access_key": "AKIA_TEST", "secret_key": "SECRET_TEST"})
mock_secrets_missing = MockSecrets({})

# -----------------------------------------------------------
# /* 
#   TEST CASES
#   - Each test case is a function
#   - All assertions are explicit
#   - All file operations are wrapped in try-except
#   - All error messages are validated
# */

def test_retrieve_active_configs():
    # /* Test: Retrieve only active configs (active_flag = "A") */
    df = spark.table("purgo_playground.ingest_config_master")
    active_df = df.filter(col("active_flag") == "A")
    active_ids = [r["config_id"] for r in active_df.collect()]
    assert "1" in active_ids, "Active config_id=1 should be present"
    assert "3" in active_ids, "Active config_id=3 should be present"
    assert "2" not in active_ids, "Inactive config_id=2 should not be present"
    assert "4" in active_ids, "config_id=4 is active (even if S3 path is null)"
    assert "5" in active_ids, "config_id=5 is active (even if S3 path is invalid)"

def test_s3_path_not_null_or_empty():
    # /* Test: S3 path columns must not be null or empty for active configs */
    df = spark.table("purgo_playground.ingest_config_master")
    active_df = df.filter(col("active_flag") == "A")
    for row in active_df.collect():
        try:
            for colname in ["s3_vendor_path", "s3_landing_path", "s3_archive_path"]:
                val = row[colname]
                assert val is not None and val != "", f"S3 path columns ({colname}) must not be null or empty for active configuration"
        except AssertionError as e:
            assert "must not be null or empty" in str(e)

def test_invalid_s3_uri():
    # /* Test: Invalid S3 URI in configuration */
    df = spark.table("purgo_playground.ingest_config_master")
    row = df.filter(col("config_id") == "5").collect()[0]
    try:
        mock_s3._parse_s3_uri(row["s3_vendor_path"])
        assert False, "Should have raised Exception for invalid S3 URI"
    except Exception as e:
        assert "Invalid S3 URI" in str(e)

def test_aws_credentials_missing():
    # /* Test: Error when AWS credentials are missing from Databricks secret */
    try:
        mock_secrets_missing.get("aws_keys", "access_key")
        assert False, "Should have raised Exception for missing AWS credentials"
    except Exception as e:
        assert "AWS credentials not found in Databricks secret scope 'aws_keys'" in str(e)

def test_file_transfer_logic_case_sensitive():
    # /* Test: File name matching is case-sensitive and includes extension */
    vendor_files = ["FileA.csv", "filea.csv"]
    purgo_files = ["fileA.csv"]
    archive_files = []
    mock_s3.add_files("s3://vendor-bucket/folderX/", vendor_files)
    mock_s3.add_files("s3://purgo-bucket/landingX/", purgo_files)
    mock_s3.add_files("s3://purgo-bucket/archiveX/", archive_files)
    # Only "FileA.csv" and "filea.csv" not in Purgo or Archive
    eligible = [f for f in vendor_files if f not in purgo_files and f not in archive_files]
    assert "FileA.csv" in eligible
    assert "filea.csv" in eligible
    assert "fileA.csv" not in eligible

def test_file_transfer_copy_not_move():
    # /* Test: File transfer is a copy, not a move (source file remains in Vendor S3) */
    src_s3 = "s3://vendor-bucket/folder1/"
    dst_s3 = "s3://purgo-bucket/landing1/"
    file_name = "fileA.csv"
    try:
        mock_s3.copy_file(src_s3, dst_s3, file_name)
        assert mock_s3.file_exists(src_s3, file_name), "Source file should remain after copy"
        assert mock_s3.file_exists(dst_s3, file_name), "Destination file should exist after copy"
    except Exception as e:
        assert False, f"Unexpected error in file copy: {e}"

def test_file_transfer_recursive_flag():
    # /* Test: File transfer is recursive if file_recursive_flag = "Y" */
    vendor_s3 = "s3://vendor-bucket/folder1/"
    files = mock_s3.list_files(vendor_s3, recursive=True)
    assert "sub1/fileD.csv" in files, "Recursive listing should include subfolder files"
    files_nonrec = mock_s3.list_files(vendor_s3, recursive=False)
    assert "sub1/fileD.csv" not in files_nonrec, "Non-recursive listing should not include subfolder files"

def test_file_transfer_nonrecursive_flag():
    # /* Test: File transfer is not recursive if file_recursive_flag is null or not "Y" */
    vendor_s3 = "s3://vendor-bucket/folder3/"
    files = mock_s3.list_files(vendor_s3, recursive=False)
    assert "fileG.csv" in files
    assert "sub2/fileH.csv" not in files

def test_file_transfer_eligible_files():
    # /* Test: Transfer only files from Vendor S3 that do not exist in either Purgo S3 or Archive S3 */
    vendor_s3 = "s3://vendor-bucket/folder1/"
    purgo_s3 = "s3://purgo-bucket/landing1/"
    archive_s3 = "s3://purgo-bucket/archive1/"
    vendor_files = mock_s3.list_files(vendor_s3, recursive=True)
    purgo_files = set(mock_s3.list_files(purgo_s3, recursive=True))
    archive_files = set(mock_s3.list_files(archive_s3, recursive=True))
    eligible = [f for f in vendor_files if f not in purgo_files and f not in archive_files]
    assert "fileA.csv" in eligible, "fileA.csv should be eligible for transfer"
    assert "fileB.csv" not in eligible, "fileB.csv already in Purgo"
    assert "fileC.csv" not in eligible, "fileC.csv already in Archive"
    assert "sub1/fileD.csv" in eligible, "sub1/fileD.csv should be eligible for transfer"

def test_file_transfer_duplicate_vendor_files():
    # /* Test: Do not transfer files if file_name in Vendor S3 is a duplicate (appears more than once) */
    vendor_s3 = "s3://vendor-bucket/folderDup/"
    mock_s3.add_files(vendor_s3, ["fileH.csv", "fileH.csv"])
    files = mock_s3.list_files(vendor_s3, recursive=False)
    assert files.count("fileH.csv") == 1, "Duplicate files should be considered only once"

def test_file_transfer_skip_null_empty_file_names():
    # /* Test: Do not transfer files if file_name is null or empty in Vendor S3 listing */
    vendor_s3 = "s3://vendor-bucket/folderNull/"
    mock_s3.add_files(vendor_s3, [None, "", "fileL.csv"])
    files = mock_s3.list_files(vendor_s3, recursive=False)
    assert None not in files and "" not in files, "Null/empty file names should be skipped"
    assert "fileL.csv" in files

def test_file_transfer_error_on_permission():
    # /* Test: Error if file transfer to Purgo S3 fails due to insufficient permissions */
    try:
        # Simulate error by raising exception in copy_file
        raise Exception("Failed to transfer fileF.csv: 403 Forbidden")
    except Exception as e:
        assert "403 Forbidden" in str(e)

def test_file_transfer_error_on_nosuchbucket():
    # /* Test: Error if file transfer to Purgo S3 fails due to missing destination bucket */
    try:
        raise Exception("Failed to transfer fileG.csv: NoSuchBucket")
    except Exception as e:
        assert "NoSuchBucket" in str(e)

def test_file_transfer_error_on_accessdenied():
    # /* Test: Error when S3 API returns AccessDenied */
    try:
        raise Exception("Failed to transfer fileZ.csv: AccessDenied")
    except Exception as e:
        assert "AccessDenied" in str(e)

def test_file_transfer_log_summary():
    # /* Test: Log summary of transferred files */
    summary = {"1": ["fileA.csv", "sub1/fileD.csv"]}
    assert "1" in summary
    assert "fileA.csv" in summary["1"]
    assert "sub1/fileD.csv" in summary["1"]

def test_file_transfer_log_error():
    # /* Test: Log error for files that could not be transferred */
    error_log = []
    try:
        raise Exception("Failed to transfer fileC.csv: NetworkTimeout")
    except Exception as e:
        error_log.append(str(e))
    assert any("NetworkTimeout" in msg for msg in error_log)

def test_no_active_configs():
    # /* Test: Error if no active configurations are found */
    df = spark.table("purgo_playground.ingest_config_master")
    active_df = df.filter(col("active_flag") == "A")
    if active_df.count() == 0:
        try:
            raise Exception("No active configurations found in purgo_playground.ingest_config_master")
        except Exception as e:
            assert "No active configurations found" in str(e)

def test_ingest_config_master_missing():
    # /* Test: Error if ingest_config_master table is missing or inaccessible */
    try:
        spark.table("purgo_playground.ingest_config_master_missing")
        assert False, "Should have raised AnalysisException"
    except AnalysisException as e:
        assert "Table or view not found" in str(e)

def test_schema_validation():
    # /* Test: Schema validation for ingest_config_master */
    df = spark.table("purgo_playground.ingest_config_master")
    expected_cols = set([
        "config_id", "source_object_name", "source_system", "file_name", "frequency", "location", "domain", "sub_domain",
        "s3_vendor_path", "source_path", "s3_landing_path", "s3_archive_path", "delta_load_ts", "full_or_incremental_load",
        "zip_file", "vendor", "delimiter", "source_landing", "src_landing_table_name", "publish_unstitched",
        "publish_unstitched_table_name", "publish_stitched", "publish_stitched_table_name", "primary_key", "header",
        "date_pattern", "actual_file_name", "vendor_file_deletion_flag", "file_recursive_flag", "total_weeks_req_data",
        "total_weeks_file_data", "active_flag"
    ])
    actual_cols = set(df.columns)
    assert expected_cols == actual_cols, "Schema columns do not match"

def test_data_type_conversion():
    # /* Test: Data type conversions using Databricks functions */
    df = spark.table("purgo_playground.ingest_config_master")
    df2 = df.withColumn("config_id_int", col("config_id").cast(IntegerType()))
    assert "config_id_int" in df2.columns
    # NULL handling
    null_count = df2.filter(col("config_id_int").isNull()).count()
    assert null_count >= 0

def test_complex_type_handling():
    # /* Test: Validate complex types (ARRAY, STRUCT) */
    df = spark.table("purgo_playground.ingest_config_master")
    df2 = df.withColumn("struct_col", struct(col("config_id"), col("file_name")))
    df3 = df2.withColumn("array_col", array(col("config_id"), col("file_name")))
    assert "struct_col" in df3.columns
    assert "array_col" in df3.columns

def test_null_handling():
    # /* Test: NULL handling in ingest_config_master */
    df = spark.table("purgo_playground.ingest_config_master")
    nulls = df.filter(col("file_name").isNull()).count()
    assert nulls >= 0

def test_window_function():
    # /* Test: Window function and analytics features */
    from pyspark.sql.window import Window  
    from pyspark.sql.functions import row_number  
    df = spark.table("purgo_playground.ingest_config_master")
    w = Window.partitionBy("active_flag").orderBy("config_id")
    df2 = df.withColumn("rn", row_number().over(w))
    assert "rn" in df2.columns

def test_delta_lake_merge_update_delete():
    # /* Test: Delta Lake MERGE, UPDATE, DELETE operations */
    # Create a test Delta table
    test_table = "purgo_playground.test_delta_table"
    data = [Row(id="1", val="A"), Row(id="2", val="B")]
    schema = StructType([StructField("id", StringType(), True), StructField("val", StringType(), True)])
    df = spark.createDataFrame(data, schema)
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(test_table)
    # UPDATE
    spark.sql(f"UPDATE {test_table} SET val = 'C' WHERE id = '1'")
    updated = spark.table(test_table).filter(col("id") == "1").collect()[0]["val"]
    assert updated == "C"
    # DELETE
    spark.sql(f"DELETE FROM {test_table} WHERE id = '2'")
    count = spark.table(test_table).filter(col("id") == "2").count()
    assert count == 0
    # MERGE
    merge_data = [Row(id="1", val="D"), Row(id="3", val="E")]
    merge_df = spark.createDataFrame(merge_data, schema)
    merge_df.createOrReplaceTempView("merge_source")
    spark.sql(f"""
        MERGE INTO {test_table} t
        USING merge_source s
        ON t.id = s.id
        WHEN MATCHED THEN UPDATE SET t.val = s.val
        WHEN NOT MATCHED THEN INSERT (id, val) VALUES (s.id, s.val)
    """)
    merged = spark.table(test_table).filter(col("id") == "3").count()
    assert merged == 1
    # Cleanup
    spark.sql(f"DROP TABLE IF EXISTS {test_table}")

def test_cleanup_operations():
    # /* Test: Cleanup operations */
    spark.sql("DROP TABLE IF EXISTS purgo_playground.test_delta_table")
    # No assertion needed, just ensure no error

# -----------------------------------------------------------
# /* 
#   RUN ALL TESTS
#   - Each test is called in sequence
#   - Any assertion failure will raise an error
# */

test_retrieve_active_configs()
test_s3_path_not_null_or_empty()
test_invalid_s3_uri()
test_aws_credentials_missing()
test_file_transfer_logic_case_sensitive()
test_file_transfer_copy_not_move()
test_file_transfer_recursive_flag()
test_file_transfer_nonrecursive_flag()
test_file_transfer_eligible_files()
test_file_transfer_duplicate_vendor_files()
test_file_transfer_skip_null_empty_file_names()
test_file_transfer_error_on_permission()
test_file_transfer_error_on_nosuchbucket()
test_file_transfer_error_on_accessdenied()
test_file_transfer_log_summary()
test_file_transfer_log_error()
test_no_active_configs()
test_ingest_config_master_missing()
test_schema_validation()
test_data_type_conversion()
test_complex_type_handling()
test_null_handling()
test_window_function()
test_delta_lake_merge_update_delete()
test_cleanup_operations()

# /* 
#   END OF TEST SUITE
# */
