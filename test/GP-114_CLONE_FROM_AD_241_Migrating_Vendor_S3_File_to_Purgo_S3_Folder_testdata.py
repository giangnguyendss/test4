spark.catalog.setCurrentCatalog("purgo_databricks")

# Test Data Generation for purgo_playground.ingest_config_master and S3 file listings

# Commented out SparkSession initialization (already available in Databricks)
# from pyspark.sql import SparkSession  # built-in
# spark = SparkSession.builder.getOrCreate()

from pyspark.sql.types import StructType, StructField, StringType, TimestampType  
from pyspark.sql import Row  
from datetime import datetime  

# -------------------------------
# 1. Test Data for ingest_config_master
# -------------------------------

# Define schema for ingest_config_master (Databricks native types)
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

# Happy path, edge, error, and special character test records
ingest_config_master_data = [
    # Happy path: active config, all S3 paths present
    Row(config_id="C001", source_object_name="objA", source_system="sysA", file_name="file1.csv", frequency="daily", location="locA", domain="domA", sub_domain="subA",
        s3_vendor_path="s3://vendor-bucket/folderA/", source_path=None, s3_landing_path="s3://purgo-bucket/landingA/", s3_archive_path="s3://purgo-bucket/archiveA/",
        delta_load_ts="2024-03-21T00:00:00.000+0000", full_or_incremental_load="F", zip_file="N", vendor="VendorA", delimiter=",", source_landing=None,
        src_landing_table_name=None, publish_unstitched=None, publish_unstitched_table_name=None, publish_stitched=None, publish_stitched_table_name=None,
        primary_key=None, header="Y", date_pattern=None, actual_file_name=None, vendor_file_deletion_flag="N", file_recursive_flag=None, total_weeks_req_data=None,
        total_weeks_file_data=None, active_flag="A"),
    # File already in Purgo
    Row(config_id="C002", source_object_name="objB", source_system="sysB", file_name="file2.csv", frequency="weekly", location="locB", domain="domB", sub_domain="subB",
        s3_vendor_path="s3://vendor-bucket/folderB/", source_path=None, s3_landing_path="s3://purgo-bucket/landingB/", s3_archive_path="s3://purgo-bucket/archiveB/",
        delta_load_ts="2024-03-22T00:00:00.000+0000", full_or_incremental_load="I", zip_file="N", vendor="VendorB", delimiter="|", source_landing=None,
        src_landing_table_name=None, publish_unstitched=None, publish_unstitched_table_name=None, publish_stitched=None, publish_stitched_table_name=None,
        primary_key=None, header="N", date_pattern=None, actual_file_name=None, vendor_file_deletion_flag="N", file_recursive_flag=None, total_weeks_req_data=None,
        total_weeks_file_data=None, active_flag="A"),
    # File already in Archive
    Row(config_id="C003", source_object_name="objC", source_system="sysC", file_name="file3.csv", frequency="monthly", location="locC", domain="domC", sub_domain="subC",
        s3_vendor_path="s3://vendor-bucket/folderC/", source_path=None, s3_landing_path="s3://purgo-bucket/landingC/", s3_archive_path="s3://purgo-bucket/archiveC/",
        delta_load_ts="2024-03-23T00:00:00.000+0000", full_or_incremental_load="F", zip_file="Y", vendor="VendorC", delimiter="\t", source_landing=None,
        src_landing_table_name=None, publish_unstitched=None, publish_unstitched_table_name=None, publish_stitched=None, publish_stitched_table_name=None,
        primary_key=None, header="Y", date_pattern=None, actual_file_name=None, vendor_file_deletion_flag="N", file_recursive_flag=None, total_weeks_req_data=None,
        total_weeks_file_data=None, active_flag="A"),
    # Inactive config (active_flag != "A")
    Row(config_id="C004", source_object_name="objD", source_system="sysD", file_name="file4.csv", frequency="daily", location="locD", domain="domD", sub_domain="subD",
        s3_vendor_path="s3://vendor-bucket/folderD/", source_path=None, s3_landing_path="s3://purgo-bucket/landingD/", s3_archive_path="s3://purgo-bucket/archiveD/",
        delta_load_ts="2024-03-24T00:00:00.000+0000", full_or_incremental_load="I", zip_file="N", vendor="VendorD", delimiter=",", source_landing=None,
        src_landing_table_name=None, publish_unstitched=None, publish_unstitched_table_name=None, publish_stitched=None, publish_stitched_table_name=None,
        primary_key=None, header="N", date_pattern=None, actual_file_name=None, vendor_file_deletion_flag="N", file_recursive_flag=None, total_weeks_req_data=None,
        total_weeks_file_data=None, active_flag="N"),
    # Missing s3_vendor_path (error)
    Row(config_id="C005", source_object_name="objE", source_system="sysE", file_name="file5.csv", frequency="daily", location="locE", domain="domE", sub_domain="subE",
        s3_vendor_path=None, source_path=None, s3_landing_path="s3://purgo-bucket/landingE/", s3_archive_path="s3://purgo-bucket/archiveE/",
        delta_load_ts="2024-03-25T00:00:00.000+0000", full_or_incremental_load="F", zip_file="N", vendor="VendorE", delimiter=",", source_landing=None,
        src_landing_table_name=None, publish_unstitched=None, publish_unstitched_table_name=None, publish_stitched=None, publish_stitched_table_name=None,
        primary_key=None, header="Y", date_pattern=None, actual_file_name=None, vendor_file_deletion_flag="N", file_recursive_flag=None, total_weeks_req_data=None,
        total_weeks_file_data=None, active_flag="A"),
    # Invalid S3 URI (error)
    Row(config_id="C008", source_object_name="objH", source_system="sysH", file_name="file8.csv", frequency="daily", location="locH", domain="domH", sub_domain="subH",
        s3_vendor_path="not-a-s3-path", source_path=None, s3_landing_path="s3://purgo-bucket/landingH/", s3_archive_path="s3://purgo-bucket/archiveH/",
        delta_load_ts="2024-03-28T00:00:00.000+0000", full_or_incremental_load="F", zip_file="N", vendor="VendorH", delimiter=",", source_landing=None,
        src_landing_table_name=None, publish_unstitched=None, publish_unstitched_table_name=None, publish_stitched=None, publish_stitched_table_name=None,
        primary_key=None, header="Y", date_pattern=None, actual_file_name=None, vendor_file_deletion_flag="N", file_recursive_flag=None, total_weeks_req_data=None,
        total_weeks_file_data=None, active_flag="A"),
    # No active configs
    Row(config_id="C017", source_object_name="objQ", source_system="sysQ", file_name="file17.csv", frequency="daily", location="locQ", domain="domQ", sub_domain="subQ",
        s3_vendor_path="s3://vendor-bucket/folderQ/", source_path=None, s3_landing_path="s3://purgo-bucket/landingQ/", s3_archive_path="s3://purgo-bucket/archiveQ/",
        delta_load_ts="2024-04-01T00:00:00.000+0000", full_or_incremental_load="F", zip_file="N", vendor="VendorQ", delimiter=",", source_landing=None,
        src_landing_table_name=None, publish_unstitched=None, publish_unstitched_table_name=None, publish_stitched=None, publish_stitched_table_name=None,
        primary_key=None, header="Y", date_pattern=None, actual_file_name=None, vendor_file_deletion_flag="N", file_recursive_flag=None, total_weeks_req_data=None,
        total_weeks_file_data=None, active_flag=None),
    # active_flag not equal to "A" (e.g., "Y")
    Row(config_id="C018", source_object_name="objR", source_system="sysR", file_name="file18.csv", frequency="daily", location="locR", domain="domR", sub_domain="subR",
        s3_vendor_path="s3://vendor-bucket/folderR/", source_path=None, s3_landing_path="s3://purgo-bucket/landingR/", s3_archive_path="s3://purgo-bucket/archiveR/",
        delta_load_ts="2024-04-02T00:00:00.000+0000", full_or_incremental_load="F", zip_file="N", vendor="VendorR", delimiter=",", source_landing=None,
        src_landing_table_name=None, publish_unstitched=None, publish_unstitched_table_name=None, publish_stitched=None, publish_stitched_table_name=None,
        primary_key=None, header="Y", date_pattern=None, actual_file_name=None, vendor_file_deletion_flag="N", file_recursive_flag=None, total_weeks_req_data=None,
        total_weeks_file_data=None, active_flag="Y"),
    # File recursive flag present (should not recurse)
    Row(config_id="C007", source_object_name="objG", source_system="sysG", file_name="rootfile.csv", frequency="daily", location="locG", domain="domG", sub_domain="subG",
        s3_vendor_path="s3://vendor-bucket/folderG/", source_path=None, s3_landing_path="s3://purgo-bucket/landingG/", s3_archive_path="s3://purgo-bucket/archiveG/",
        delta_load_ts="2024-03-27T00:00:00.000+0000", full_or_incremental_load="F", zip_file="N", vendor="VendorG", delimiter=",", source_landing=None,
        src_landing_table_name=None, publish_unstitched=None, publish_unstitched_table_name=None, publish_stitched=None, publish_stitched_table_name=None,
        primary_key=None, header="Y", date_pattern=None, actual_file_name=None, vendor_file_deletion_flag="N", file_recursive_flag=None, total_weeks_req_data=None,
        total_weeks_file_data=None, active_flag="A"),
    # Multiple active configs
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
    # Edge: file name with whitespace
    Row(config_id="C021", source_object_name="objU", source_system="sysU", file_name=" file12.csv ", frequency="daily", location="locU", domain="domU", sub_domain="subU",
        s3_vendor_path="s3://vendor-bucket/folderU/", source_path=None, s3_landing_path="s3://purgo-bucket/landingU/", s3_archive_path="s3://purgo-bucket/archiveU/",
        delta_load_ts="2024-04-05T00:00:00.000+0000", full_or_incremental_load="F", zip_file="N", vendor="VendorU", delimiter=",", source_landing=None,
        src_landing_table_name=None, publish_unstitched=None, publish_unstitched_table_name=None, publish_stitched=None, publish_stitched_table_name=None,
        primary_key=None, header="Y", date_pattern=None, actual_file_name=None, vendor_file_deletion_flag="N", file_recursive_flag=None, total_weeks_req_data=None,
        total_weeks_file_data=None, active_flag="A"),
    # Edge: file name with invalid character
    Row(config_id="C011", source_object_name="objK", source_system="sysK", file_name="file:invalid.csv", frequency="daily", location="locK", domain="domK", sub_domain="subK",
        s3_vendor_path="s3://vendor-bucket/folderK/", source_path=None, s3_landing_path="s3://purgo-bucket/landingK/", s3_archive_path="s3://purgo-bucket/archiveK/",
        delta_load_ts="2024-04-03T00:00:00.000+0000", full_or_incremental_load="F", zip_file="N", vendor="VendorK", delimiter=",", source_landing=None,
        src_landing_table_name=None, publish_unstitched=None, publish_unstitched_table_name=None, publish_stitched=None, publish_stitched_table_name=None,
        primary_key=None, header="Y", date_pattern=None, actual_file_name=None, vendor_file_deletion_flag="N", file_recursive_flag=None, total_weeks_req_data=None,
        total_weeks_file_data=None, active_flag="A"),
    # Edge: file extension not allowed
    Row(config_id="C012", source_object_name="objL", source_system="sysL", file_name="file6.exe", frequency="daily", location="locL", domain="domL", sub_domain="subL",
        s3_vendor_path="s3://vendor-bucket/folderL/", source_path=None, s3_landing_path="s3://purgo-bucket/landingL/", s3_archive_path="s3://purgo-bucket/archiveL/",
        delta_load_ts="2024-04-04T00:00:00.000+0000", full_or_incremental_load="F", zip_file="N", vendor="VendorL", delimiter=",", source_landing=None,
        src_landing_table_name=None, publish_unstitched=None, publish_unstitched_table_name=None, publish_stitched=None, publish_stitched_table_name=None,
        primary_key=None, header="Y", date_pattern=None, actual_file_name=None, vendor_file_deletion_flag="N", file_recursive_flag=None, total_weeks_req_data=None,
        total_weeks_file_data=None, active_flag="A"),
    # Edge: file name is null
    Row(config_id="C010", source_object_name="objJ", source_system="sysJ", file_name=None, frequency="daily", location="locJ", domain="domJ", sub_domain="subJ",
        s3_vendor_path="s3://vendor-bucket/folderJ/", source_path=None, s3_landing_path="s3://purgo-bucket/landingJ/", s3_archive_path="s3://purgo-bucket/archiveJ/",
        delta_load_ts="2024-04-02T00:00:00.000+0000", full_or_incremental_load="F", zip_file="N", vendor="VendorJ", delimiter=",", source_landing=None,
        src_landing_table_name=None, publish_unstitched=None, publish_unstitched_table_name=None, publish_stitched=None, publish_stitched_table_name=None,
        primary_key=None, header="Y", date_pattern=None, actual_file_name=None, vendor_file_deletion_flag="N", file_recursive_flag=None, total_weeks_req_data=None,
        total_weeks_file_data=None, active_flag="A"),
    # Edge: file name is empty string
    Row(config_id="C010", source_object_name="objJ", source_system="sysJ", file_name="", frequency="daily", location="locJ", domain="domJ", sub_domain="subJ",
        s3_vendor_path="s3://vendor-bucket/folderJ/", source_path=None, s3_landing_path="s3://purgo-bucket/landingJ/", s3_archive_path="s3://purgo-bucket/archiveJ/",
        delta_load_ts="2024-04-02T00:00:00.000+0000", full_or_incremental_load="F", zip_file="N", vendor="VendorJ", delimiter=",", source_landing=None,
        src_landing_table_name=None, publish_unstitched=None, publish_unstitched_table_name=None, publish_stitched=None, publish_stitched_table_name=None,
        primary_key=None, header="Y", date_pattern=None, actual_file_name=None, vendor_file_deletion_flag="N", file_recursive_flag=None, total_weeks_req_data=None,
        total_weeks_file_data=None, active_flag="A"),
    # Edge: duplicate file name in vendor S3 folder (simulate by config, see S3 listing below)
    Row(config_id="C019", source_object_name="objS", source_system="sysS", file_name="file10.csv", frequency="daily", location="locS", domain="domS", sub_domain="subS",
        s3_vendor_path="s3://vendor-bucket/folderS/", source_path=None, s3_landing_path="s3://purgo-bucket/landingS/", s3_archive_path="s3://purgo-bucket/archiveS/",
        delta_load_ts="2024-04-06T00:00:00.000+0000", full_or_incremental_load="F", zip_file="N", vendor="VendorS", delimiter=",", source_landing=None,
        src_landing_table_name=None, publish_unstitched=None, publish_unstitched_table_name=None, publish_stitched=None, publish_stitched_table_name=None,
        primary_key=None, header="Y", date_pattern=None, actual_file_name=None, vendor_file_deletion_flag="N", file_recursive_flag=None, total_weeks_req_data=None,
        total_weeks_file_data=None, active_flag="A"),
    # Edge: file size zero bytes (simulate in S3 listing)
    Row(config_id="C020", source_object_name="objT", source_system="sysT", file_name="file11.csv", frequency="daily", location="locT", domain="domT", sub_domain="subT",
        s3_vendor_path="s3://vendor-bucket/folderT/", source_path=None, s3_landing_path="s3://purgo-bucket/landingT/", s3_archive_path="s3://purgo-bucket/archiveT/",
        delta_load_ts="2024-04-07T00:00:00.000+0000", full_or_incremental_load="F", zip_file="N", vendor="VendorT", delimiter=",", source_landing=None,
        src_landing_table_name=None, publish_unstitched=None, publish_unstitched_table_name=None, publish_stitched=None, publish_stitched_table_name=None,
        primary_key=None, header="Y", date_pattern=None, actual_file_name=None, vendor_file_deletion_flag="N", file_recursive_flag=None, total_weeks_req_data=None,
        total_weeks_file_data=None, active_flag="A"),
    # Edge: special/multibyte characters in file name
    Row(config_id="C022", source_object_name="objV", source_system="sysV", file_name="ファイル13.csv", frequency="daily", location="locV", domain="domV", sub_domain="subV",
        s3_vendor_path="s3://vendor-bucket/folderV/", source_path=None, s3_landing_path="s3://purgo-bucket/landingV/", s3_archive_path="s3://purgo-bucket/archiveV/",
        delta_load_ts="2024-04-08T00:00:00.000+0000", full_or_incremental_load="F", zip_file="N", vendor="VendorV", delimiter=",", source_landing=None,
        src_landing_table_name=None, publish_unstitched=None, publish_unstitched_table_name=None, publish_stitched=None, publish_stitched_table_name=None,
        primary_key=None, header="Y", date_pattern=None, actual_file_name=None, vendor_file_deletion_flag="N", file_recursive_flag=None, total_weeks_req_data=None,
        total_weeks_file_data=None, active_flag="A"),
    # Edge: file name with special characters
    Row(config_id="C023", source_object_name="objW", source_system="sysW", file_name="file_!@#$.csv", frequency="daily", location="locW", domain="domW", sub_domain="subW",
        s3_vendor_path="s3://vendor-bucket/folderW/", source_path=None, s3_landing_path="s3://purgo-bucket/landingW/", s3_archive_path="s3://purgo-bucket/archiveW/",
        delta_load_ts="2024-04-09T00:00:00.000+0000", full_or_incremental_load="F", zip_file="N", vendor="VendorW", delimiter=",", source_landing=None,
        src_landing_table_name=None, publish_unstitched=None, publish_unstitched_table_name=None, publish_stitched=None, publish_stitched_table_name=None,
        primary_key=None, header="Y", date_pattern=None, actual_file_name=None, vendor_file_deletion_flag="N", file_recursive_flag=None, total_weeks_req_data=None,
        total_weeks_file_data=None, active_flag="A"),
    # Edge: file name with emoji
    Row(config_id="C024", source_object_name="objX", source_system="sysX", file_name="file😊.csv", frequency="daily", location="locX", domain="domX", sub_domain="subX",
        s3_vendor_path="s3://vendor-bucket/folderX/", source_path=None, s3_landing_path="s3://purgo-bucket/landingX/", s3_archive_path="s3://purgo-bucket/archiveX/",
        delta_load_ts="2024-04-10T00:00:00.000+0000", full_or_incremental_load="F", zip_file="N", vendor="VendorX", delimiter=",", source_landing=None,
        src_landing_table_name=None, publish_unstitched=None, publish_unstitched_table_name=None, publish_stitched=None, publish_stitched_table_name=None,
        primary_key=None, header="Y", date_pattern=None, actual_file_name=None, vendor_file_deletion_flag="N", file_recursive_flag=None, total_weeks_req_data=None,
        total_weeks_file_data=None, active_flag="A"),
    # Edge: file name with newline
    Row(config_id="C025", source_object_name="objY", source_system="sysY", file_name="file14\n.csv", frequency="daily", location="locY", domain="domY", sub_domain="subY",
        s3_vendor_path="s3://vendor-bucket/folderY/", source_path=None, s3_landing_path="s3://purgo-bucket/landingY/", s3_archive_path="s3://purgo-bucket/archiveY/",
        delta_load_ts="2024-04-11T00:00:00.000+0000", full_or_incremental_load="F", zip_file="N", vendor="VendorY", delimiter=",", source_landing=None,
        src_landing_table_name=None, publish_unstitched=None, publish_unstitched_table_name=None, publish_stitched=None, publish_stitched_table_name=None,
        primary_key=None, header="Y", date_pattern=None, actual_file_name=None, vendor_file_deletion_flag="N", file_recursive_flag=None, total_weeks_req_data=None,
        total_weeks_file_data=None, active_flag="A"),
    # Edge: file name with tab
    Row(config_id="C026", source_object_name="objZ", source_system="sysZ", file_name="file15\t.csv", frequency="daily", location="locZ", domain="domZ", sub_domain="subZ",
        s3_vendor_path="s3://vendor-bucket/folderZ/", source_path=None, s3_landing_path="s3://purgo-bucket/landingZ/", s3_archive_path="s3://purgo-bucket/archiveZ/",
        delta_load_ts="2024-04-12T00:00:00.000+0000", full_or_incremental_load="F", zip_file="N", vendor="VendorZ", delimiter=",", source_landing=None,
        src_landing_table_name=None, publish_unstitched=None, publish_unstitched_table_name=None, publish_stitched=None, publish_stitched_table_name=None,
        primary_key=None, header="Y", date_pattern=None, actual_file_name=None, vendor_file_deletion_flag="N", file_recursive_flag=None, total_weeks_req_data=None,
        total_weeks_file_data=None, active_flag="A"),
    # Edge: file name with multi-byte and special char
    Row(config_id="C027", source_object_name="objAA", source_system="sysAA", file_name="文件16_ß.csv", frequency="daily", location="locAA", domain="domAA", sub_domain="subAA",
        s3_vendor_path="s3://vendor-bucket/folderAA/", source_path=None, s3_landing_path="s3://purgo-bucket/landingAA/", s3_archive_path="s3://purgo-bucket/archiveAA/",
        delta_load_ts="2024-04-13T00:00:00.000+0000", full_or_incremental_load="F", zip_file="N", vendor="VendorAA", delimiter=",", source_landing=None,
        src_landing_table_name=None, publish_unstitched=None, publish_unstitched_table_name=None, publish_stitched=None, publish_stitched_table_name=None,
        primary_key=None, header="Y", date_pattern=None, actual_file_name=None, vendor_file_deletion_flag="N", file_recursive_flag=None, total_weeks_req_data=None,
        total_weeks_file_data=None, active_flag="A"),
]

ingest_config_master_df = spark.createDataFrame(ingest_config_master_data, schema=ingest_config_master_schema)

# -------------------------------
# 2. Test Data for S3 File Listings (simulate S3 folder contents)
# -------------------------------

# Each S3 folder listing is a dict: {folder_path: [ {file_name, size_bytes}, ... ]}
# For edge/error cases, include special/invalid/duplicate/empty/zero-byte files

s3_folder_listings = {
    # C001: Happy path, file not in Purgo or Archive
    "s3://vendor-bucket/folderA/": [
        {"file_name": "file1.csv", "size_bytes": 1024}
    ],
    "s3://purgo-bucket/landingA/": [],
    "s3://purgo-bucket/archiveA/": [],
    # C002: File already in Purgo
    "s3://vendor-bucket/folderB/": [
        {"file_name": "file2.csv", "size_bytes": 2048}
    ],
    "s3://purgo-bucket/landingB/": [
        {"file_name": "file2.csv", "size_bytes": 2048}
    ],
    "s3://purgo-bucket/archiveB/": [],
    # C003: File already in Archive
    "s3://vendor-bucket/folderC/": [
        {"file_name": "file3.csv", "size_bytes": 4096}
    ],
    "s3://purgo-bucket/landingC/": [],
    "s3://purgo-bucket/archiveC/": [
        {"file_name": "file3.csv", "size_bytes": 4096}
    ],
    # C004: Inactive config, file present
    "s3://vendor-bucket/folderD/": [
        {"file_name": "file4.csv", "size_bytes": 1024}
    ],
    # C005: Missing s3_vendor_path, no files
    # C006: Multiple files, some in Purgo/Archive
    "s3://vendor-bucket/folderF/": [
        {"file_name": "a.csv", "size_bytes": 100},
        {"file_name": "b.csv", "size_bytes": 200},
        {"file_name": "c.csv", "size_bytes": 300}
    ],
    "s3://purgo-bucket/landingF/": [
        {"file_name": "b.csv", "size_bytes": 200}
    ],
    "s3://purgo-bucket/archiveF/": [
        {"file_name": "c.csv", "size_bytes": 300}
    ],
    # C007: file_recursive_flag, root and subfolder file
    "s3://vendor-bucket/folderG/": [
        {"file_name": "rootfile.csv", "size_bytes": 1000},
        {"file_name": "subfolder/file5.csv", "size_bytes": 1000}
    ],
    "s3://purgo-bucket/landingG/": [],
    "s3://purgo-bucket/archiveG/": [],
    # C008: Invalid S3 URI, no files
    # C009: Vendor S3 folder empty
    "s3://vendor-bucket/folderI/": [],
    # C010: File name is null and empty string
    "s3://vendor-bucket/folderJ/": [
        {"file_name": None, "size_bytes": 100},
        {"file_name": "", "size_bytes": 100}
    ],
    # C011: File name with invalid character
    "s3://vendor-bucket/folderK/": [
        {"file_name": "file:invalid.csv", "size_bytes": 100}
    ],
    # C012: File extension not allowed
    "s3://vendor-bucket/folderL/": [
        {"file_name": "file6.exe", "size_bytes": 100}
    ],
    # C013: Multiple active configs
    "s3://vendor-bucket/folderM/": [
        {"file_name": "file7.csv", "size_bytes": 100}
    ],
    "s3://purgo-bucket/landingM/": [],
    "s3://purgo-bucket/archiveM/": [],
    "s3://vendor-bucket/folderN/": [
        {"file_name": "file8.csv", "size_bytes": 100}
    ],
    "s3://purgo-bucket/landingN/": [],
    "s3://purgo-bucket/archiveN/": [],
    # C015: Nonexistent bucket (simulate error)
    # C016: Case-sensitive file name match
    "s3://vendor-bucket/folderP/": [
        {"file_name": "File9.csv", "size_bytes": 100}
    ],
    "s3://purgo-bucket/landingP/": [
        {"file_name": "file9.csv", "size_bytes": 100}
    ],
    # C019: Duplicate file name in vendor S3 folder
    "s3://vendor-bucket/folderS/": [
        {"file_name": "file10.csv", "size_bytes": 100},
        {"file_name": "file10.csv", "size_bytes": 200}
    ],
    # C020: Zero-byte file
    "s3://vendor-bucket/folderT/": [
        {"file_name": "file11.csv", "size_bytes": 0}
    ],
    # C021: File name with whitespace
    "s3://vendor-bucket/folderU/": [
        {"file_name": " file12.csv ", "size_bytes": 100}
    ],
    # C022: Multibyte/Unicode file name
    "s3://vendor-bucket/folderV/": [
        {"file_name": "ファイル13.csv", "size_bytes": 100}
    ],
    # C023: Special characters
    "s3://vendor-bucket/folderW/": [
        {"file_name": "file_!@#$.csv", "size_bytes": 100}
    ],
    # C024: Emoji in file name
    "s3://vendor-bucket/folderX/": [
        {"file_name": "file😊.csv", "size_bytes": 100}
    ],
    # C025: Newline in file name
    "s3://vendor-bucket/folderY/": [
        {"file_name": "file14\n.csv", "size_bytes": 100}
    ],
    # C026: Tab in file name
    "s3://vendor-bucket/folderZ/": [
        {"file_name": "file15\t.csv", "size_bytes": 100}
    ],
    # C027: Multi-byte and special char
    "s3://vendor-bucket/folderAA/": [
        {"file_name": "文件16_ß.csv", "size_bytes": 100}
    ],
}

# -------------------------------
# 3. Create DataFrames for S3 listings (for test validation)
# -------------------------------

from pyspark.sql.types import LongType  

s3_listing_schema = StructType([
    StructField("folder_path", StringType(), True),
    StructField("file_name", StringType(), True),
    StructField("size_bytes", LongType(), True)
])

s3_listing_rows = []
for folder, files in s3_folder_listings.items():
    for f in files:
        s3_listing_rows.append(Row(folder_path=folder, file_name=f["file_name"], size_bytes=f["size_bytes"]))

s3_listing_df = spark.createDataFrame(s3_listing_rows, schema=s3_listing_schema)

# -------------------------------
# 4. Validation Query (CTE) Example: Find all active configs with valid S3 paths
# -------------------------------

# This CTE selects all active configs with non-null S3 paths and valid S3 URIs
validation_query = """
WITH valid_active_configs AS (
    SELECT
        config_id,
        s3_vendor_path,
        s3_landing_path,
        s3_archive_path,
        active_flag
    FROM purgo_playground.ingest_config_master
    WHERE active_flag = 'A'
      AND s3_vendor_path IS NOT NULL
      AND s3_landing_path IS NOT NULL
      AND s3_archive_path IS NOT NULL
      AND s3_vendor_path LIKE 's3://%'
      AND s3_landing_path LIKE 's3://%'
      AND s3_archive_path LIKE 's3://%'
)
SELECT * FROM valid_active_configs
"""

# -------------------------------
# 5. Show test data (for test validation only)
# -------------------------------

# Show ingest_config_master test data
ingest_config_master_df.show(truncate=False)

# Show S3 listing test data
s3_listing_df.show(truncate=False)

# Show validation query result (CTE)
# spark.sql(validation_query).show(truncate=False)
