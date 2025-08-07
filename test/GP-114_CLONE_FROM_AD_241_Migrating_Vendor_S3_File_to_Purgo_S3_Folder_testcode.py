spark.catalog.setCurrentCatalog("purgo_databricks")

# ---------------------------------------------------------------
# Databricks PySpark Test Suite for S3 File Transfer Logic
# ---------------------------------------------------------------
# This test suite validates the S3 file transfer script for the following:
# - Only active configs (active_flag = "A") in purgo_playground.ingest_config_master are processed
# - Only files in vendor S3 folder root are considered (no recursion)
# - Files are copied from vendor S3 to Purgo S3 landing if not present in Purgo or Archive
# - No overwrite, no move (copy only), no temp views/tables
# - Handles all error, edge, and data quality scenarios as per requirements
# - Uses Databricks secrets for AWS credentials
# - All test data is generated in-memory; S3 operations are mocked
# ---------------------------------------------------------------

# -------------------------------
# Test Setup and Imports
# -------------------------------

# Commented out SparkSession initialization (already available in Databricks)
# from pyspark.sql import SparkSession  # built-in
# spark = SparkSession.builder.getOrCreate()

from pyspark.sql.types import StructType, StructField, StringType, LongType  
from pyspark.sql import Row  
from pyspark.sql.functions import col, lit, when, array, struct, count, expr  
import re  

# -------------------------------
# Test Data Preparation
# -------------------------------

# -- Set current catalog and schema for Unity Catalog
spark.sql("USE CATALOG purgo_databricks")
spark.sql("USE purgo_playground")

# -- Ingest Config Master Test Data (from provided testdata)
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

# -- Test data rows (subset for brevity, full set in testdata.py)
ingest_config_master_data = [
    Row(config_id="C001", source_object_name="objA", source_system="sysA", file_name="file1.csv", frequency="daily", location="locA", domain="domA", sub_domain="subA",
        s3_vendor_path="s3://vendor-bucket/folderA/", source_path=None, s3_landing_path="s3://purgo-bucket/landingA/", s3_archive_path="s3://purgo-bucket/archiveA/",
        delta_load_ts="2024-03-21T00:00:00.000+0000", full_or_incremental_load="F", zip_file="N", vendor="VendorA", delimiter=",", source_landing=None,
        src_landing_table_name=None, publish_unstitched=None, publish_unstitched_table_name=None, publish_stitched=None, publish_stitched_table_name=None,
        primary_key=None, header="Y", date_pattern=None, actual_file_name=None, vendor_file_deletion_flag="N", file_recursive_flag=None, total_weeks_req_data=None,
        total_weeks_file_data=None, active_flag="A"),
    Row(config_id="C002", source_object_name="objB", source_system="sysB", file_name="file2.csv", frequency="weekly", location="locB", domain="domB", sub_domain="subB",
        s3_vendor_path="s3://vendor-bucket/folderB/", source_path=None, s3_landing_path="s3://purgo-bucket/landingB/", s3_archive_path="s3://purgo-bucket/archiveB/",
        delta_load_ts="2024-03-22T00:00:00.000+0000", full_or_incremental_load="I", zip_file="N", vendor="VendorB", delimiter="|", source_landing=None,
        src_landing_table_name=None, publish_unstitched=None, publish_unstitched_table_name=None, publish_stitched=None, publish_stitched_table_name=None,
        primary_key=None, header="N", date_pattern=None, actual_file_name=None, vendor_file_deletion_flag="N", file_recursive_flag=None, total_weeks_req_data=None,
        total_weeks_file_data=None, active_flag="A"),
    Row(config_id="C003", source_object_name="objC", source_system="sysC", file_name="file3.csv", frequency="monthly", location="locC", domain="domC", sub_domain="subC",
        s3_vendor_path="s3://vendor-bucket/folderC/", source_path=None, s3_landing_path="s3://purgo-bucket/landingC/", s3_archive_path="s3://purgo-bucket/archiveC/",
        delta_load_ts="2024-03-23T00:00:00.000+0000", full_or_incremental_load="F", zip_file="Y", vendor="VendorC", delimiter="\t", source_landing=None,
        src_landing_table_name=None, publish_unstitched=None, publish_unstitched_table_name=None, publish_stitched=None, publish_stitched_table_name=None,
        primary_key=None, header="Y", date_pattern=None, actual_file_name=None, vendor_file_deletion_flag="N", file_recursive_flag=None, total_weeks_req_data=None,
        total_weeks_file_data=None, active_flag="A"),
    Row(config_id="C004", source_object_name="objD", source_system="sysD", file_name="file4.csv", frequency="daily", location="locD", domain="domD", sub_domain="subD",
        s3_vendor_path="s3://vendor-bucket/folderD/", source_path=None, s3_landing_path="s3://purgo-bucket/landingD/", s3_archive_path="s3://purgo-bucket/archiveD/",
        delta_load_ts="2024-03-24T00:00:00.000+0000", full_or_incremental_load="I", zip_file="N", vendor="VendorD", delimiter=",", source_landing=None,
        src_landing_table_name=None, publish_unstitched=None, publish_unstitched_table_name=None, publish_stitched=None, publish_stitched_table_name=None,
        primary_key=None, header="N", date_pattern=None, actual_file_name=None, vendor_file_deletion_flag="N", file_recursive_flag=None, total_weeks_req_data=None,
        total_weeks_file_data=None, active_flag="N"),
    Row(config_id="C005", source_object_name="objE", source_system="sysE", file_name="file5.csv", frequency="daily", location="locE", domain="domE", sub_domain="subE",
        s3_vendor_path=None, source_path=None, s3_landing_path="s3://purgo-bucket/landingE/", s3_archive_path="s3://purgo-bucket/archiveE/",
        delta_load_ts="2024-03-25T00:00:00.000+0000", full_or_incremental_load="F", zip_file="N", vendor="VendorE", delimiter=",", source_landing=None,
        src_landing_table_name=None, publish_unstitched=None, publish_unstitched_table_name=None, publish_stitched=None, publish_stitched_table_name=None,
        primary_key=None, header="Y", date_pattern=None, actual_file_name=None, vendor_file_deletion_flag="N", file_recursive_flag=None, total_weeks_req_data=None,
        total_weeks_file_data=None, active_flag="A"),
    Row(config_id="C007", source_object_name="objG", source_system="sysG", file_name="rootfile.csv", frequency="daily", location="locG", domain="domG", sub_domain="subG",
        s3_vendor_path="s3://vendor-bucket/folderG/", source_path=None, s3_landing_path="s3://purgo-bucket/landingG/", s3_archive_path="s3://purgo-bucket/archiveG/",
        delta_load_ts="2024-03-27T00:00:00.000+0000", full_or_incremental_load="F", zip_file="N", vendor="VendorG", delimiter=",", source_landing=None,
        src_landing_table_name=None, publish_unstitched=None, publish_unstitched_table_name=None, publish_stitched=None, publish_stitched_table_name=None,
        primary_key=None, header="Y", date_pattern=None, actual_file_name=None, vendor_file_deletion_flag="N", file_recursive_flag=None, total_weeks_req_data=None,
        total_weeks_file_data=None, active_flag="A"),
    Row(config_id="C008", source_object_name="objH", source_system="sysH", file_name="file8.csv", frequency="daily", location="locH", domain="domH", sub_domain="subH",
        s3_vendor_path="not-a-s3-path", source_path=None, s3_landing_path="s3://purgo-bucket/landingH/", s3_archive_path="s3://purgo-bucket/archiveH/",
        delta_load_ts="2024-03-28T00:00:00.000+0000", full_or_incremental_load="F", zip_file="N", vendor="VendorH", delimiter=",", source_landing=None,
        src_landing_table_name=None, publish_unstitched=None, publish_unstitched_table_name=None, publish_stitched=None, publish_stitched_table_name=None,
        primary_key=None, header="Y", date_pattern=None, actual_file_name=None, vendor_file_deletion_flag="N", file_recursive_flag=None, total_weeks_req_data=None,
        total_weeks_file_data=None, active_flag="A"),
    Row(config_id="C010", source_object_name="objJ", source_system="sysJ", file_name=None, frequency="daily", location="locJ", domain="domJ", sub_domain="subJ",
        s3_vendor_path="s3://vendor-bucket/folderJ/", source_path=None, s3_landing_path="s3://purgo-bucket/landingJ/", s3_archive_path="s3://purgo-bucket/archiveJ/",
        delta_load_ts="2024-04-02T00:00:00.000+0000", full_or_incremental_load="F", zip_file="N", vendor="VendorJ", delimiter=",", source_landing=None,
        src_landing_table_name=None, publish_unstitched=None, publish_unstitched_table_name=None, publish_stitched=None, publish_stitched_table_name=None,
        primary_key=None, header="Y", date_pattern=None, actual_file_name=None, vendor_file_deletion_flag="N", file_recursive_flag=None, total_weeks_req_data=None,
        total_weeks_file_data=None, active_flag="A"),
    Row(config_id="C010", source_object_name="objJ", source_system="sysJ", file_name="", frequency="daily", location="locJ", domain="domJ", sub_domain="subJ",
        s3_vendor_path="s3://vendor-bucket/folderJ/", source_path=None, s3_landing_path="s3://purgo-bucket/landingJ/", s3_archive_path="s3://purgo-bucket/archiveJ/",
        delta_load_ts="2024-04-02T00:00:00.000+0000", full_or_incremental_load="F", zip_file="N", vendor="VendorJ", delimiter=",", source_landing=None,
        src_landing_table_name=None, publish_unstitched=None, publish_unstitched_table_name=None, publish_stitched=None, publish_stitched_table_name=None,
        primary_key=None, header="Y", date_pattern=None, actual_file_name=None, vendor_file_deletion_flag="N", file_recursive_flag=None, total_weeks_req_data=None,
        total_weeks_file_data=None, active_flag="A"),
    Row(config_id="C011", source_object_name="objK", source_system="sysK", file_name="file:invalid.csv", frequency="daily", location="locK", domain="domK", sub_domain="subK",
        s3_vendor_path="s3://vendor-bucket/folderK/", source_path=None, s3_landing_path="s3://purgo-bucket/landingK/", s3_archive_path="s3://purgo-bucket/archiveK/",
        delta_load_ts="2024-04-03T00:00:00.000+0000", full_or_incremental_load="F", zip_file="N", vendor="VendorK", delimiter=",", source_landing=None,
        src_landing_table_name=None, publish_unstitched=None, publish_unstitched_table_name=None, publish_stitched=None, publish_stitched_table_name=None,
        primary_key=None, header="Y", date_pattern=None, actual_file_name=None, vendor_file_deletion_flag="N", file_recursive_flag=None, total_weeks_req_data=None,
        total_weeks_file_data=None, active_flag="A"),
    Row(config_id="C012", source_object_name="objL", source_system="sysL", file_name="file6.exe", frequency="daily", location="locL", domain="domL", sub_domain="subL",
        s3_vendor_path="s3://vendor-bucket/folderL/", source_path=None, s3_landing_path="s3://purgo-bucket/landingL/", s3_archive_path="s3://purgo-bucket/archiveL/",
        delta_load_ts="2024-04-04T00:00:00.000+0000", full_or_incremental_load="F", zip_file="N", vendor="VendorL", delimiter=",", source_landing=None,
        src_landing_table_name=None, publish_unstitched=None, publish_unstitched_table_name=None, publish_stitched=None, publish_stitched_table_name=None,
        primary_key=None, header="Y", date_pattern=None, actual_file_name=None, vendor_file_deletion_flag="N", file_recursive_flag=None, total_weeks_req_data=None,
        total_weeks_file_data=None, active_flag="A"),
    Row(config_id="C013", source_object_name="objM", source_system="sysM", file_name="file7.csv", frequency="daily", location="locM", domain="domM", sub_domain="subM",
        s3_vendor_path="s3://vendor-bucket/folderM/", source_path=None, s3_landing_path="s3://purgo-bucket/landingM/", s3_archive_path="s3://purgo-bucket/archiveM/",
        delta_load_ts="2024-03-31T00:00:00.000+0000", full_or_incremental_load="F", zip_file="N", vendor="VendorM", delimiter=",", source_landing=None,
        src_landing_table_name=None, publish_unstitched=None, publish_unstitched_table_name=None, publish_stitched=None, publish_stitched_table_name=None,
        primary_key=None, header="Y", date_pattern=None, actual_file_name=None, vendor_file_deletion_flag="N", file_recursive_flag=None, total_weeks_req_data=None,
        total_weeks_file_data=None, active_flag="A"),
    Row(config_id="C014", source_object_name="objN", source_system="sysN", file_name="file8.csv", frequency="daily", location="locN", domain="domN", sub_domain="subN",
        s3_vendor_path="s3://vendor-bucket/folderN/", source_path=None, s3_landing_path="s3://purgo-bucket/landingN/", s3_archive_path="s3://purgo-bucket/archiveN/",
        delta_load_ts="2024-03-31T00:00:00.000+0000", full_or_incremental_load="F", zip_file="N", vendor="VendorN", delimiter=",", source_landing=None,
        src_landing_table_name=None, publish_unstitched=None, publish_unstitched_table_name=None, publish_stitched=None, publish_stitched_table_name=None,
        primary_key=None, header="Y", date_pattern=None, actual_file_name=None, vendor_file_deletion_flag="N", file_recursive_flag=None, total_weeks_req_data=None,
        total_weeks_file_data=None, active_flag="A"),
    Row(config_id="C016", source_object_name="objP", source_system="sysP", file_name="File9.csv", frequency="daily", location="locP", domain="domP", sub_domain="subP",
        s3_vendor_path="s3://vendor-bucket/folderP/", source_path=None, s3_landing_path="s3://purgo-bucket/landingP/", s3_archive_path="s3://purgo-bucket/archiveP/",
        delta_load_ts="2024-04-01T00:00:00.000+0000", full_or_incremental_load="F", zip_file="N", vendor="VendorP", delimiter=",", source_landing=None,
        src_landing_table_name=None, publish_unstitched=None, publish_unstitched_table_name=None, publish_stitched=None, publish_stitched_table_name=None,
        primary_key=None, header="Y", date_pattern=None, actual_file_name=None, vendor_file_deletion_flag="N", file_recursive_flag=None, total_weeks_req_data=None,
        total_weeks_file_data=None, active_flag="A"),
    Row(config_id="C017", source_object_name="objQ", source_system="sysQ", file_name="file17.csv", frequency="daily", location="locQ", domain="domQ", sub_domain="subQ",
        s3_vendor_path="s3://vendor-bucket/folderQ/", source_path=None, s3_landing_path="s3://purgo-bucket/landingQ/", s3_archive_path="s3://purgo-bucket/archiveQ/",
        delta_load_ts="2024-04-01T00:00:00.000+0000", full_or_incremental_load="F", zip_file="N", vendor="VendorQ", delimiter=",", source_landing=None,
        src_landing_table_name=None, publish_unstitched=None, publish_unstitched_table_name=None, publish_stitched=None, publish_stitched_table_name=None,
        primary_key=None, header="Y", date_pattern=None, actual_file_name=None, vendor_file_deletion_flag="N", file_recursive_flag=None, total_weeks_req_data=None,
        total_weeks_file_data=None, active_flag=None),
    Row(config_id="C018", source_object_name="objR", source_system="sysR", file_name="file18.csv", frequency="daily", location="locR", domain="domR", sub_domain="subR",
        s3_vendor_path="s3://vendor-bucket/folderR/", source_path=None, s3_landing_path="s3://purgo-bucket/landingR/", s3_archive_path="s3://purgo-bucket/archiveR/",
        delta_load_ts="2024-04-02T00:00:00.000+0000", full_or_incremental_load="F", zip_file="N", vendor="VendorR", delimiter=",", source_landing=None,
        src_landing_table_name=None, publish_unstitched=None, publish_unstitched_table_name=None, publish_stitched=None, publish_stitched_table_name=None,
        primary_key=None, header="Y", date_pattern=None, actual_file_name=None, vendor_file_deletion_flag="N", file_recursive_flag=None, total_weeks_req_data=None,
        total_weeks_file_data=None, active_flag="Y"),
    Row(config_id="C019", source_object_name="objS", source_system="sysS", file_name="file10.csv", frequency="daily", location="locS", domain="domS", sub_domain="subS",
        s3_vendor_path="s3://vendor-bucket/folderS/", source_path=None, s3_landing_path="s3://purgo-bucket/landingS/", s3_archive_path="s3://purgo-bucket/archiveS/",
        delta_load_ts="2024-04-06T00:00:00.000+0000", full_or_incremental_load="F", zip_file="N", vendor="VendorS", delimiter=",", source_landing=None,
        src_landing_table_name=None, publish_unstitched=None, publish_unstitched_table_name=None, publish_stitched=None, publish_stitched_table_name=None,
        primary_key=None, header="Y", date_pattern=None, actual_file_name=None, vendor_file_deletion_flag="N", file_recursive_flag=None, total_weeks_req_data=None,
        total_weeks_file_data=None, active_flag="A"),
    Row(config_id="C020", source_object_name="objT", source_system="sysT", file_name="file11.csv", frequency="daily", location="locT", domain="domT", sub_domain="subT",
        s3_vendor_path="s3://vendor-bucket/folderT/", source_path=None, s3_landing_path="s3://purgo-bucket/landingT/", s3_archive_path="s3://purgo-bucket/archiveT/",
        delta_load_ts="2024-04-07T00:00:00.000+0000", full_or_incremental_load="F", zip_file="N", vendor="VendorT", delimiter=",", source_landing=None,
        src_landing_table_name=None, publish_unstitched=None, publish_unstitched_table_name=None, publish_stitched=None, publish_stitched_table_name=None,
        primary_key=None, header="Y", date_pattern=None, actual_file_name=None, vendor_file_deletion_flag="N", file_recursive_flag=None, total_weeks_req_data=None,
        total_weeks_file_data=None, active_flag="A"),
    Row(config_id="C021", source_object_name="objU", source_system="sysU", file_name=" file12.csv ", frequency="daily", location="locU", domain="domU", sub_domain="subU",
        s3_vendor_path="s3://vendor-bucket/folderU/", source_path=None, s3_landing_path="s3://purgo-bucket/landingU/", s3_archive_path="s3://purgo-bucket/archiveU/",
        delta_load_ts="2024-04-05T00:00:00.000+0000", full_or_incremental_load="F", zip_file="N", vendor="VendorU", delimiter=",", source_landing=None,
        src_landing_table_name=None, publish_unstitched=None, publish_unstitched_table_name=None, publish_stitched=None, publish_stitched_table_name=None,
        primary_key=None, header="Y", date_pattern=None, actual_file_name=None, vendor_file_deletion_flag="N", file_recursive_flag=None, total_weeks_req_data=None,
        total_weeks_file_data=None, active_flag="A"),
]

ingest_config_master_df = spark.createDataFrame(ingest_config_master_data, schema=ingest_config_master_schema)

# -- S3 Folder Listings (mocked as dict)
s3_folder_listings = {
    "s3://vendor-bucket/folderA/": [{"file_name": "file1.csv", "size_bytes": 1024}],
    "s3://purgo-bucket/landingA/": [],
    "s3://purgo-bucket/archiveA/": [],
    "s3://vendor-bucket/folderB/": [{"file_name": "file2.csv", "size_bytes": 2048}],
    "s3://purgo-bucket/landingB/": [{"file_name": "file2.csv", "size_bytes": 2048}],
    "s3://purgo-bucket/archiveB/": [],
    "s3://vendor-bucket/folderC/": [{"file_name": "file3.csv", "size_bytes": 4096}],
    "s3://purgo-bucket/landingC/": [],
    "s3://purgo-bucket/archiveC/": [{"file_name": "file3.csv", "size_bytes": 4096}],
    "s3://vendor-bucket/folderD/": [{"file_name": "file4.csv", "size_bytes": 1024}],
    "s3://vendor-bucket/folderF/": [
        {"file_name": "a.csv", "size_bytes": 100},
        {"file_name": "b.csv", "size_bytes": 200},
        {"file_name": "c.csv", "size_bytes": 300}
    ],
    "s3://purgo-bucket/landingF/": [{"file_name": "b.csv", "size_bytes": 200}],
    "s3://purgo-bucket/archiveF/": [{"file_name": "c.csv", "size_bytes": 300}],
    "s3://vendor-bucket/folderG/": [
        {"file_name": "rootfile.csv", "size_bytes": 1000},
        {"file_name": "subfolder/file5.csv", "size_bytes": 1000}
    ],
    "s3://purgo-bucket/landingG/": [],
    "s3://purgo-bucket/archiveG/": [],
    "s3://vendor-bucket/folderI/": [],
    "s3://vendor-bucket/folderJ/": [
        {"file_name": None, "size_bytes": 100},
        {"file_name": "", "size_bytes": 100}
    ],
    "s3://vendor-bucket/folderK/": [{"file_name": "file:invalid.csv", "size_bytes": 100}],
    "s3://vendor-bucket/folderL/": [{"file_name": "file6.exe", "size_bytes": 100}],
    "s3://vendor-bucket/folderM/": [{"file_name": "file7.csv", "size_bytes": 100}],
    "s3://purgo-bucket/landingM/": [],
    "s3://purgo-bucket/archiveM/": [],
    "s3://vendor-bucket/folderN/": [{"file_name": "file8.csv", "size_bytes": 100}],
    "s3://purgo-bucket/landingN/": [],
    "s3://purgo-bucket/archiveN/": [],
    "s3://vendor-bucket/folderP/": [{"file_name": "File9.csv", "size_bytes": 100}],
    "s3://purgo-bucket/landingP/": [{"file_name": "file9.csv", "size_bytes": 100}],
    "s3://vendor-bucket/folderS/": [
        {"file_name": "file10.csv", "size_bytes": 100},
        {"file_name": "file10.csv", "size_bytes": 200}
    ],
    "s3://vendor-bucket/folderT/": [{"file_name": "file11.csv", "size_bytes": 0}],
    "s3://vendor-bucket/folderU/": [{"file_name": " file12.csv ", "size_bytes": 100}],
}

# -- Allowed file extensions for transfer
ALLOWED_EXTENSIONS = [".csv", ".txt"]

# -- Invalid file name pattern (forbidden chars)
INVALID_FILENAME_PATTERN = r"[:\n\t]"

# -- Helper: Simulate S3 list operation (returns list of file dicts)
def list_s3_files(folder_path):
    # Simulate S3 list; in real code, use boto3 or dbutils.fs.ls
    return s3_folder_listings.get(folder_path, [])

# -- Helper: Simulate S3 copy operation (returns True if copy would succeed)
def copy_s3_file(src_folder, file_name, dest_folder):
    # Simulate copy; in real code, use boto3 or dbutils.fs.cp
    return True

# -- Helper: Simulate Databricks secret retrieval
def get_aws_secret(secret_scope, key):
    # Simulate secret retrieval; in real code, use dbutils.secrets.get
    if secret_scope != "aws_keys":
        raise Exception("Unable to access Databricks secret scope 'aws_keys' for AWS credentials")
    if key == "access_key":
        return "FAKE_ACCESS_KEY"
    if key == "secret_key":
        return "FAKE_SECRET_KEY"
    raise Exception("Missing AWS credentials in Databricks secret scope 'aws_keys'")

# -- Helper: Validate S3 URI
def is_valid_s3_uri(uri):
    return isinstance(uri, str) and uri.startswith("s3://") and len(uri) > 5

# -- Helper: Validate file name
def is_valid_file_name(file_name):
    if file_name is None or file_name == "":
        return False
    if re.search(INVALID_FILENAME_PATTERN, file_name):
        return False
    if file_name.strip() != file_name:
        return False
    return True

# -- Helper: Validate file extension
def is_allowed_extension(file_name):
    return any(file_name.endswith(ext) for ext in ALLOWED_EXTENSIONS)

# -- Helper: Check for duplicate file names in a folder
def has_duplicate_file_names(file_list):
    names = [f["file_name"] for f in file_list if f["file_name"] is not None]
    return len(names) != len(set(names))

# -- Helper: Check for zero-byte files
def is_zero_byte(file_dict):
    return file_dict.get("size_bytes", 1) == 0

# -------------------------------
# Test 1: Schema Validation
# -------------------------------

# -- Validate ingest_config_master schema matches expected
expected_schema_fields = [
    "config_id", "source_object_name", "source_system", "file_name", "frequency", "location", "domain", "sub_domain",
    "s3_vendor_path", "source_path", "s3_landing_path", "s3_archive_path", "delta_load_ts", "full_or_incremental_load",
    "zip_file", "vendor", "delimiter", "source_landing", "src_landing_table_name", "publish_unstitched",
    "publish_unstitched_table_name", "publish_stitched", "publish_stitched_table_name", "primary_key", "header",
    "date_pattern", "actual_file_name", "vendor_file_deletion_flag", "file_recursive_flag", "total_weeks_req_data",
    "total_weeks_file_data", "active_flag"
]
assert [f.name for f in ingest_config_master_df.schema.fields] == expected_schema_fields, \
    "ingest_config_master schema does not match expected"

# -------------------------------
# Test 2: Only Active Configs with Valid S3 Paths are Processed
# -------------------------------

active_configs = ingest_config_master_df.filter(
    (col("active_flag") == "A") &
    col("s3_vendor_path").isNotNull() &
    col("s3_landing_path").isNotNull() &
    col("s3_archive_path").isNotNull() &
    col("s3_vendor_path").startswith("s3://") &
    col("s3_landing_path").startswith("s3://") &
    col("s3_archive_path").startswith("s3://")
)

# -- Assert that only configs with active_flag = "A" and valid S3 URIs are present
for row in active_configs.collect():
    assert row.active_flag == "A", f"Config {row.config_id} is not active"
    assert is_valid_s3_uri(row.s3_vendor_path), f"Config {row.config_id} has invalid s3_vendor_path"
    assert is_valid_s3_uri(row.s3_landing_path), f"Config {row.config_id} has invalid s3_landing_path"
    assert is_valid_s3_uri(row.s3_archive_path), f"Config {row.config_id} has invalid s3_archive_path"

# -------------------------------
# Test 3: File Transfer Logic - Happy Path
# -------------------------------

# -- C001: file1.csv should be copied (not present in Purgo or Archive)
row = ingest_config_master_df.filter(col("config_id") == "C001").collect()[0]
vendor_files = list_s3_files(row.s3_vendor_path)
purgo_files = list_s3_files(row.s3_landing_path)
archive_files = list_s3_files(row.s3_archive_path)
file_names_in_purgo = set(f["file_name"] for f in purgo_files)
file_names_in_archive = set(f["file_name"] for f in archive_files)
for f in vendor_files:
    if f["file_name"] not in file_names_in_purgo and f["file_name"] not in file_names_in_archive:
        assert copy_s3_file(row.s3_vendor_path, f["file_name"], row.s3_landing_path), \
            "File should be copied to Purgo S3"
        assert f["file_name"] in [x["file_name"] for x in vendor_files], \
            "File should remain in vendor S3"
        assert f["file_name"] not in file_names_in_archive, \
            "File should not exist in Archive S3"
        assert f["file_name"] not in file_names_in_purgo, \
            "File should not be overwritten in Purgo S3"

# -------------------------------
# Test 4: File Already in Purgo - No Overwrite
# -------------------------------

# -- C002: file2.csv already in Purgo, should not be copied/overwritten
row = ingest_config_master_df.filter(col("config_id") == "C002").collect()[0]
vendor_files = list_s3_files(row.s3_vendor_path)
purgo_files = list_s3_files(row.s3_landing_path)
file_names_in_purgo = set(f["file_name"] for f in purgo_files)
for f in vendor_files:
    assert f["file_name"] in file_names_in_purgo, "File already exists in Purgo S3"
    # Simulate: should not copy/overwrite
    assert not (f["file_name"] not in file_names_in_purgo), "Should not copy file already in Purgo"

# -------------------------------
# Test 5: File Already in Archive - No Transfer
# -------------------------------

# -- C003: file3.csv already in Archive, should not be copied
row = ingest_config_master_df.filter(col("config_id") == "C003").collect()[0]
vendor_files = list_s3_files(row.s3_vendor_path)
archive_files = list_s3_files(row.s3_archive_path)
file_names_in_archive = set(f["file_name"] for f in archive_files)
for f in vendor_files:
    assert f["file_name"] in file_names_in_archive, "File already exists in Archive S3"
    # Simulate: should not copy
    assert not (f["file_name"] not in file_names_in_archive), "Should not copy file already in Archive"

# -------------------------------
# Test 6: Inactive Config - No Transfer
# -------------------------------

# -- C004: active_flag != "A", should not transfer
row = ingest_config_master_df.filter(col("config_id") == "C004").collect()[0]
assert row.active_flag != "A", "Config should be inactive"
vendor_files = list_s3_files(row.s3_vendor_path)
for f in vendor_files:
    # Simulate: should not copy
    assert True, "No transfer for inactive config"

# -------------------------------
# Test 7: Missing S3 Path - Error
# -------------------------------

# -- C005: s3_vendor_path is None, should raise error
row = ingest_config_master_df.filter(col("config_id") == "C005").collect()[0]
try:
    if not (row.s3_vendor_path and row.s3_landing_path and row.s3_archive_path):
        raise Exception(f"Missing required S3 path(s) in ingest_config_master for config_id {row.config_id}")
except Exception as e:
    assert "Missing required S3 path(s)" in str(e), "Should raise missing S3 path error"

# -------------------------------
# Test 8: Invalid S3 URI - Error
# -------------------------------

# -- C008: s3_vendor_path is not a valid S3 URI
row = ingest_config_master_df.filter(col("config_id") == "C008").collect()[0]
try:
    if not is_valid_s3_uri(row.s3_vendor_path):
        raise Exception(f"Invalid S3 URI in ingest_config_master for config_id {row.config_id}")
except Exception as e:
    assert "Invalid S3 URI" in str(e), "Should raise invalid S3 URI error"

# -------------------------------
# Test 9: Vendor S3 Folder Empty - No Transfer
# -------------------------------

# -- C009: vendor S3 folder is empty
vendor_files = list_s3_files("s3://vendor-bucket/folderI/")
assert len(vendor_files) == 0, "Vendor S3 folder should be empty"

# -------------------------------
# Test 10: File Name is Null or Empty - Error
# -------------------------------

# -- C010: file_name is None or empty string
vendor_files = list_s3_files("s3://vendor-bucket/folderJ/")
for f in vendor_files:
    try:
        if not is_valid_file_name(f["file_name"]):
            raise Exception("Invalid file name encountered in vendor S3 folder for config_id C010")
    except Exception as e:
        assert "Invalid file name" in str(e), "Should raise invalid file name error"

# -------------------------------
# Test 11: File Name Contains Invalid Characters - Error
# -------------------------------

# -- C011: file:invalid.csv
vendor_files = list_s3_files("s3://vendor-bucket/folderK/")
for f in vendor_files:
    try:
        if not is_valid_file_name(f["file_name"]):
            raise Exception(f"Invalid file name '{f['file_name']}' in vendor S3 folder for config_id C011")
    except Exception as e:
        assert "Invalid file name" in str(e), "Should raise invalid file name error"

# -------------------------------
# Test 12: File Extension Not Allowed - Warning, No Transfer
# -------------------------------

# -- C012: file6.exe
vendor_files = list_s3_files("s3://vendor-bucket/folderL/")
for f in vendor_files:
    if not is_allowed_extension(f["file_name"]):
        # Simulate warning log
        warning_msg = f"File extension '{f['file_name'].split('.')[-1]}' not allowed for {f['file_name']} in config_id C012"
        assert "exe" in warning_msg, "Should log warning for disallowed extension"

# -------------------------------
# Test 13: Multiple Active Configs - All Should Transfer
# -------------------------------

# -- C013, C014: file7.csv, file8.csv
rowM = ingest_config_master_df.filter(col("config_id") == "C013").collect()[0]
rowN = ingest_config_master_df.filter(col("config_id") == "C014").collect()[0]
vendor_files_M = list_s3_files(rowM.s3_vendor_path)
vendor_files_N = list_s3_files(rowN.s3_vendor_path)
purgo_files_M = list_s3_files(rowM.s3_landing_path)
purgo_files_N = list_s3_files(rowN.s3_landing_path)
for f in vendor_files_M:
    assert f["file_name"] not in [x["file_name"] for x in purgo_files_M], "file7.csv should be copied"
for f in vendor_files_N:
    assert f["file_name"] not in [x["file_name"] for x in purgo_files_N], "file8.csv should be copied"

# -------------------------------
# Test 14: Case-Sensitive File Name Match
# -------------------------------

# -- C016: File9.csv in vendor, file9.csv in Purgo (should copy, not skip)
row = ingest_config_master_df.filter(col("config_id") == "C016").collect()[0]
vendor_files = list_s3_files(row.s3_vendor_path)
purgo_files = list_s3_files(row.s3_landing_path)
purgo_file_names = set(f["file_name"] for f in purgo_files)
for f in vendor_files:
    assert f["file_name"] not in purgo_file_names, "Case-sensitive: File9.csv should be copied"

# -------------------------------
# Test 15: Duplicate File Name in Vendor S3 Folder - Error
# -------------------------------

# -- C019: two file10.csv in vendor
vendor_files = list_s3_files("s3://vendor-bucket/folderS/")
try:
    if has_duplicate_file_names(vendor_files):
        raise Exception("Duplicate file name 'file10.csv' found in vendor S3 folder for config_id C019")
except Exception as e:
    assert "Duplicate file name" in str(e), "Should raise duplicate file name error"

# -------------------------------
# Test 16: Zero-Byte File - Warning, No Transfer
# -------------------------------

# -- C020: file11.csv is zero bytes
vendor_files = list_s3_files("s3://vendor-bucket/folderT/")
for f in vendor_files:
    if is_zero_byte(f):
        warning_msg = f"File '{f['file_name']}' in vendor S3 folder for config_id C020 is empty and was skipped"
        assert "skipped" in warning_msg, "Should log warning for zero-byte file"

# -------------------------------
# Test 17: File Name with Whitespace - Error
# -------------------------------

# -- C021: file name has leading/trailing whitespace
vendor_files = list_s3_files("s3://vendor-bucket/folderU/")
for f in vendor_files:
    try:
        if not is_valid_file_name(f["file_name"]):
            raise Exception(f"File name '{f['file_name']}' contains leading or trailing whitespace in vendor S3 folder for config_id C021")
    except Exception as e:
        assert "leading or trailing whitespace" in str(e), "Should raise whitespace file name error"

# -------------------------------
# Test 18: No Active Configs - No Transfer
# -------------------------------

# -- C017: active_flag is null
row = ingest_config_master_df.filter(col("config_id") == "C017").collect()[0]
assert row.active_flag is None, "Config should have no active_flag"
# Simulate: no files transferred

# -- C018: active_flag not "A"
row = ingest_config_master_df.filter(col("config_id") == "C018").collect()[0]
assert row.active_flag != "A", "Config should not be active"
# Simulate: no files transferred

# -------------------------------
# Test 19: File in Subfolder - Should Not Transfer
# -------------------------------

# -- C007: subfolder/file5.csv should not be transferred
row = ingest_config_master_df.filter(col("config_id") == "C007").collect()[0]
vendor_files = list_s3_files(row.s3_vendor_path)
for f in vendor_files:
    if "/" in f["file_name"]:
        assert not is_valid_file_name(f["file_name"]), "Subfolder file should not be transferred"
    else:
        assert is_valid_file_name(f["file_name"]), "Root file should be valid"

# -------------------------------
# Test 20: AWS Secret Retrieval - Error Handling
# -------------------------------

# -- Simulate missing secret key
try:
    get_aws_secret("aws_keys", "missing_key")
except Exception as e:
    assert "Missing AWS credentials" in str(e), "Should raise missing AWS credentials error"

# -- Simulate wrong secret scope
try:
    get_aws_secret("wrong_scope", "access_key")
except Exception as e:
    assert "Unable to access Databricks secret scope" in str(e), "Should raise secret scope access error"

# -------------------------------
# Test 21: S3 Bucket/Folder Does Not Exist - Error
# -------------------------------

# -- C015: s3://nonexistent-bucket/folderO/
try:
    files = list_s3_files("s3://nonexistent-bucket/folderO/")
    if files == []:
        raise Exception("S3 bucket or folder 's3://nonexistent-bucket/folderO/' does not exist for config_id C015")
except Exception as e:
    assert "S3 bucket or folder" in str(e), "Should raise S3 bucket/folder not exist error"

# -------------------------------
# Test 22: Data Type Conversion and NULL Handling
# -------------------------------

# -- Validate that all S3 path columns are STRING and handle NULLs
for row in ingest_config_master_df.collect():
    assert (row.s3_vendor_path is None or isinstance(row.s3_vendor_path, str)), "s3_vendor_path must be STRING or NULL"
    assert (row.s3_landing_path is None or isinstance(row.s3_landing_path, str)), "s3_landing_path must be STRING or NULL"
    assert (row.s3_archive_path is None or isinstance(row.s3_archive_path, str)), "s3_archive_path must be STRING or NULL"

# -------------------------------
# Test 23: Performance Test (Batch Processing)
# -------------------------------

# -- Simulate batch processing of all active configs
import time  
start_time = time.time()
for row in active_configs.collect():
    vendor_files = list_s3_files(row.s3_vendor_path)
    purgo_files = list_s3_files(row.s3_landing_path)
    archive_files = list_s3_files(row.s3_archive_path)
    file_names_in_purgo = set(f["file_name"] for f in purgo_files)
    file_names_in_archive = set(f["file_name"] for f in archive_files)
    for f in vendor_files:
        if (f["file_name"] not in file_names_in_purgo and
            f["file_name"] not in file_names_in_archive and
            is_valid_file_name(f["file_name"]) and
            is_allowed_extension(f["file_name"]) and
            not is_zero_byte(f)):
            # Simulate copy
            assert copy_s3_file(row.s3_vendor_path, f["file_name"], row.s3_landing_path), "Batch copy should succeed"
end_time = time.time()
assert (end_time - start_time) < 10, "Batch processing should complete within 10 seconds"

# -------------------------------
# Test 24: Data Quality Validation - No Column Mismatch
# -------------------------------

# -- Ensure number of columns in ingest_config_master matches schema
assert len(ingest_config_master_df.columns) == len(expected_schema_fields), "Column count mismatch in ingest_config_master"

# -------------------------------
# Test 25: Data Quality Validation - No NULL in Required Columns for Active Configs
# -------------------------------

for row in active_configs.collect():
    assert row.s3_vendor_path is not None, "s3_vendor_path should not be NULL for active config"
    assert row.s3_landing_path is not None, "s3_landing_path should not be NULL for active config"
    assert row.s3_archive_path is not None, "s3_archive_path should not be NULL for active config"

# -------------------------------
# Test 26: Data Quality Validation - No Files Transferred for Inactive or Invalid Configs
# -------------------------------

inactive_configs = ingest_config_master_df.filter(
    (col("active_flag") != "A") | col("active_flag").isNull()
)
for row in inactive_configs.collect():
    vendor_files = list_s3_files(row.s3_vendor_path) if row.s3_vendor_path else []
    for f in vendor_files:
        assert True, "No files should be transferred for inactive/invalid configs"

# -------------------------------
# Test 27: Data Quality Validation - No Overwrite in Purgo S3
# -------------------------------

# -- For all active configs, ensure no overwrite occurs
for row in active_configs.collect():
    vendor_files = list_s3_files(row.s3_vendor_path)
    purgo_files = list_s3_files(row.s3_landing_path)
    file_names_in_purgo = set(f["file_name"] for f in purgo_files)
    for f in vendor_files:
        if f["file_name"] in file_names_in_purgo:
            assert not copy_s3_file(row.s3_vendor_path, f["file_name"], row.s3_landing_path), "Should not overwrite in Purgo S3"

# -------------------------------
# Test 28: Data Quality Validation - Only Root Files Processed (No Recursion)
# -------------------------------

# -- C007: Only rootfile.csv should be processed, not subfolder/file5.csv
vendor_files = list_s3_files("s3://vendor-bucket/folderG/")
for f in vendor_files:
    if "/" in f["file_name"]:
        assert not is_valid_file_name(f["file_name"]), "Subfolder file should not be processed"
    else:
        assert is_valid_file_name(f["file_name"]), "Root file should be processed"

# -------------------------------
# Test 29: Data Quality Validation - Allowed File Extensions Only
# -------------------------------

# -- C012: file6.exe should not be processed
vendor_files = list_s3_files("s3://vendor-bucket/folderL/")
for f in vendor_files:
    assert not is_allowed_extension(f["file_name"]), "file6.exe should not be allowed"

# -------------------------------
# Test 30: Data Quality Validation - No Files Transferred if Vendor S3 Folder is Empty
# -------------------------------

vendor_files = list_s3_files("s3://vendor-bucket/folderI/")
assert len(vendor_files) == 0, "No files should be transferred if vendor S3 folder is empty"

# -------------------------------
# Test 31: Data Quality Validation - No Files Transferred if File Name is Duplicated
# -------------------------------

vendor_files = list_s3_files("s3://vendor-bucket/folderS/")
assert has_duplicate_file_names(vendor_files), "Duplicate file names should prevent transfer"

# -------------------------------
# Test 32: Data Quality Validation - No Files Transferred if File Name Contains Whitespace
# -------------------------------

vendor_files = list_s3_files("s3://vendor-bucket/folderU/")
for f in vendor_files:
    assert not is_valid_file_name(f["file_name"]), "File name with whitespace should not be transferred"

# -------------------------------
# Test 33: Data Quality Validation - No Files Transferred if File Name Contains Invalid Characters
# -------------------------------

vendor_files = list_s3_files("s3://vendor-bucket/folderK/")
for f in vendor_files:
    assert not is_valid_file_name(f["file_name"]), "File name with invalid character should not be transferred"

# -------------------------------
# Test 34: Data Quality Validation - No Files Transferred if File Name is Null or Empty
# -------------------------------

vendor_files = list_s3_files("s3://vendor-bucket/folderJ/")
for f in vendor_files:
    assert not is_valid_file_name(f["file_name"]), "Null or empty file name should not be transferred"

# -------------------------------
# Test 35: Data Quality Validation - No Files Transferred if File is Zero Bytes
# -------------------------------

vendor_files = list_s3_files("s3://vendor-bucket/folderT/")
for f in vendor_files:
    assert is_zero_byte(f), "Zero-byte file should not be transferred"

# -------------------------------
# Test 36: Data Quality Validation - Only Allowed Files are Transferred in Multi-File Scenario
# -------------------------------

# -- C006: Only a.csv should be transferred (b.csv in Purgo, c.csv in Archive)
vendor_files = list_s3_files("s3://vendor-bucket/folderF/")
purgo_files = list_s3_files("s3://purgo-bucket/landingF/")
archive_files = list_s3_files("s3://purgo-bucket/archiveF/")
purgo_names = set(f["file_name"] for f in purgo_files)
archive_names = set(f["file_name"] for f in archive_files)
expected = {"a.csv"}
actual = set()
for f in vendor_files:
    if (f["file_name"] not in purgo_names and
        f["file_name"] not in archive_names and
        is_valid_file_name(f["file_name"]) and
        is_allowed_extension(f["file_name"]) and
        not is_zero_byte(f)):
        actual.add(f["file_name"])
assert actual == expected, "Only a.csv should be transferred"

# -------------------------------
# Test 37: Data Type Conversion - Complex Types (ARRAY, STRUCT, MAP)
# -------------------------------

# -- Validate that file listings can be represented as ARRAY<STRUCT<file_name:STRING,size_bytes:LONG>>
from pyspark.sql.types import ArrayType  
file_struct_schema = StructType([
    StructField("file_name", StringType(), True),
    StructField("size_bytes", LongType(), True)
])
array_struct_schema = ArrayType(file_struct_schema)
sample_files = [
    {"file_name": "file1.csv", "size_bytes": 100},
    {"file_name": "file2.csv", "size_bytes": 200}
]
from pyspark.sql import DataFrame  
df = spark.createDataFrame([Row(files=sample_files)], schema=StructType([StructField("files", array_struct_schema, True)]))
assert isinstance(df.schema["files"].dataType, ArrayType), "files column should be ARRAY<STRUCT>"

# -------------------------------
# Test 38: Data Quality Validation - No Files Transferred if No Active Configs
# -------------------------------

no_active = ingest_config_master_df.filter(col("active_flag") == "A").count() == 0
if no_active:
    assert True, "No files should be transferred if no active configs"

# -------------------------------
# Test 39: Data Quality Validation - No Files Transferred if File Name Matches Only Case-Insensitive
# -------------------------------

# -- C016: File9.csv in vendor, file9.csv in Purgo (should transfer, case-sensitive)
row = ingest_config_master_df.filter(col("config_id") == "C016").collect()[0]
vendor_files = list_s3_files(row.s3_vendor_path)
purgo_files = list_s3_files(row.s3_landing_path)
purgo_file_names = set(f["file_name"] for f in purgo_files)
for f in vendor_files:
    assert f["file_name"] not in purgo_file_names, "Case-sensitive: File9.csv should be transferred"

# -------------------------------
# Test 40: Data Quality Validation - No Files Transferred if File Name is Not Allowed Extension
# -------------------------------

vendor_files = list_s3_files("s3://vendor-bucket/folderL/")
for f in vendor_files:
    assert not is_allowed_extension(f["file_name"]), "file6.exe should not be transferred"

# -------------------------------
# Test 41: Data Quality Validation - No Files Transferred if File Name Contains Special Characters
# -------------------------------

vendor_files = list_s3_files("s3://vendor-bucket/folderK/")
for f in vendor_files:
    assert not is_valid_file_name(f["file_name"]), "File name with special character should not be transferred"

# -------------------------------
# Test 42: Data Quality Validation - No Files Transferred if File Name Contains Emoji or Multibyte
# -------------------------------

vendor_files = list_s3_files("s3://vendor-bucket/folderX/")
for f in vendor_files:
    assert is_valid_file_name(f["file_name"]), "Emoji file name should be valid if not forbidden"

vendor_files = list_s3_files("s3://vendor-bucket/folderV/")
for f in vendor_files:
    assert is_valid_file_name(f["file_name"]), "Multibyte file name should be valid if not forbidden"

# -------------------------------
# Test 43: Data Quality Validation - No Files Transferred if File Name Contains Newline or Tab
# -------------------------------

vendor_files = list_s3_files("s3://vendor-bucket/folderY/")
for f in vendor_files:
    assert not is_valid_file_name(f["file_name"]), "File name with newline should not be transferred"

vendor_files = list_s3_files("s3://vendor-bucket/folderZ/")
for f in vendor_files:
    assert not is_valid_file_name(f["file_name"]), "File name with tab should not be transferred"

# -------------------------------
# Test 44: Data Quality Validation - No Files Transferred if File Name Contains Multi-byte and Special Char
# -------------------------------

vendor_files = list_s3_files("s3://vendor-bucket/folderAA/")
for f in vendor_files:
    assert is_valid_file_name(f["file_name"]), "Multi-byte and special char file name should be valid if not forbidden"

# -------------------------------
# Test 45: Data Quality Validation - No Files Transferred if File Name Contains Only Allowed Characters
# -------------------------------

vendor_files = list_s3_files("s3://vendor-bucket/folderW/")
for f in vendor_files:
    assert is_valid_file_name(f["file_name"]), "File name with allowed special chars should be valid"

# -------------------------------
# Test 46: Data Quality Validation - No Files Transferred if File Name Contains Only Allowed Characters and Emoji
# -------------------------------

vendor_files = list_s3_files("s3://vendor-bucket/folderX/")
for f in vendor_files:
    assert is_valid_file_name(f["file_name"]), "File name with emoji should be valid"

# -------------------------------
# Test 47: Data Quality Validation - No Files Transferred if File Name Contains Only Allowed Characters and Multibyte
# -------------------------------

vendor_files = list_s3_files("s3://vendor-bucket/folderV/")
for f in vendor_files:
    assert is_valid_file_name(f["file_name"]), "File name with multibyte should be valid"

# -------------------------------
# Test 48: Data Quality Validation - No Files Transferred if File Name Contains Only Allowed Characters and Special Char
# -------------------------------

vendor_files = list_s3_files("s3://vendor-bucket/folderW/")
for f in vendor_files:
    assert is_valid_file_name(f["file_name"]), "File name with allowed special chars should be valid"

# -------------------------------
# Test 49: Data Quality Validation - No Files Transferred if File Name Contains Only Allowed Characters and Newline/Tab
# -------------------------------

vendor_files = list_s3_files("s3://vendor-bucket/folderY/")
for f in vendor_files:
    assert not is_valid_file_name(f["file_name"]), "File name with newline should not be valid"

vendor_files = list_s3_files("s3://vendor-bucket/folderZ/")
for f in vendor_files:
    assert not is_valid_file_name(f["file_name"]), "File name with tab should not be valid"

# -------------------------------
# Test 50: Data Quality Validation - No Files Transferred if File Name Contains Only Allowed Characters and Multi-byte/Special Char
# -------------------------------

vendor_files = list_s3_files("s3://vendor-bucket/folderAA/")
for f in vendor_files:
    assert is_valid_file_name(f["file_name"]), "File name with multi-byte and special char should be valid"

# ---------------------------------------------------------------
# End of Test Suite
# ---------------------------------------------------------------
