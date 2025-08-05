# Databricks PySpark test data generation for purgo_playground.ingest_config_master and s3_file_process_log

from pyspark.sql import SparkSession  # SparkSession is already available in Databricks
from pyspark.sql.types import (  
    StructType, StructField, StringType, TimestampType
)
from pyspark.sql import Row  
from datetime import datetime  

# =========================
# Test Data for ingest_config_master
# =========================

# Define schema for ingest_config_master (all columns as per catalog, using StringType for all except delta_load_ts)
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

# Test records for ingest_config_master
ingest_config_master_data = [
    # Happy path: eligible active file, all S3 paths present
    Row(config_id="1", source_object_name="obj1", source_system="sys1", file_name="data_20240601.csv", frequency="DAILY", location="loc1", domain="dom1", sub_domain="sub1",
        s3_vendor_path="s3://vendor-bucket/folder/", source_path=None, s3_landing_path="s3://purgo-bucket/landing/", s3_archive_path="s3://purgo-bucket/archive/",
        delta_load_ts="2024-06-01T00:00:00.000+0000", full_or_incremental_load="FULL", zip_file="N", vendor="VendorA", delimiter=",", source_landing="Y",
        src_landing_table_name="table1", publish_unstitched="N", publish_unstitched_table_name=None, publish_stitched="N", publish_stitched_table_name=None,
        primary_key="id", header="Y", date_pattern="yyyy-MM-dd", actual_file_name="data_20240601.csv", vendor_file_deletion_flag="N", file_recursive_flag="N",
        total_weeks_req_data="4", total_weeks_file_data="4", active_flag="A"),
    # Happy path: another eligible file
    Row(config_id="2", source_object_name="obj2", source_system="sys2", file_name="report1.txt", frequency="WEEKLY", location="loc2", domain="dom2", sub_domain="sub2",
        s3_vendor_path="s3://vendor-bucket/folder/", source_path=None, s3_landing_path="s3://purgo-bucket/landing/", s3_archive_path="s3://purgo-bucket/archive/",
        delta_load_ts="2024-06-02T00:00:00.000+0000", full_or_incremental_load="INCR", zip_file="N", vendor="VendorB", delimiter="|", source_landing="Y",
        src_landing_table_name="table2", publish_unstitched="N", publish_unstitched_table_name=None, publish_stitched="N", publish_stitched_table_name=None,
        primary_key="pk", header="N", date_pattern="yyyyMMdd", actual_file_name="report1.txt", vendor_file_deletion_flag="N", file_recursive_flag="N",
        total_weeks_req_data="2", total_weeks_file_data="2", active_flag="A"),
    # Edge: inactive file (should not be ingested)
    Row(config_id="3", source_object_name="obj3", source_system="sys3", file_name="data_20240601.csv", frequency="DAILY", location="loc3", domain="dom3", sub_domain="sub3",
        s3_vendor_path="s3://vendor-bucket/folder/", source_path=None, s3_landing_path="s3://purgo-bucket/landing/", s3_archive_path="s3://purgo-bucket/archive/",
        delta_load_ts="2024-06-03T00:00:00.000+0000", full_or_incremental_load="FULL", zip_file="N", vendor="VendorC", delimiter=",", source_landing="Y",
        src_landing_table_name="table3", publish_unstitched="N", publish_unstitched_table_name=None, publish_stitched="N", publish_stitched_table_name=None,
        primary_key="id", header="Y", date_pattern="yyyy-MM-dd", actual_file_name="data_20240601.csv", vendor_file_deletion_flag="N", file_recursive_flag="N",
        total_weeks_req_data="4", total_weeks_file_data="4", active_flag="I"),
    # Edge: inactive file (flag N)
    Row(config_id="4", source_object_name="obj4", source_system="sys4", file_name="report1.txt", frequency="WEEKLY", location="loc4", domain="dom4", sub_domain="sub4",
        s3_vendor_path="s3://vendor-bucket/folder/", source_path=None, s3_landing_path="s3://purgo-bucket/landing/", s3_archive_path="s3://purgo-bucket/archive/",
        delta_load_ts="2024-06-04T00:00:00.000+0000", full_or_incremental_load="INCR", zip_file="N", vendor="VendorD", delimiter="|", source_landing="Y",
        src_landing_table_name="table4", publish_unstitched="N", publish_unstitched_table_name=None, publish_stitched="N", publish_stitched_table_name=None,
        primary_key="pk", header="N", date_pattern="yyyyMMdd", actual_file_name="report1.txt", vendor_file_deletion_flag="N", file_recursive_flag="N",
        total_weeks_req_data="2", total_weeks_file_data="2", active_flag="N"),
    # Edge: file already in Purgo S3
    Row(config_id="5", source_object_name="obj5", source_system="sys5", file_name="data_20240601.csv", frequency="DAILY", location="loc5", domain="dom5", sub_domain="sub5",
        s3_vendor_path="s3://vendor-bucket/folder/", source_path=None, s3_landing_path="s3://purgo-bucket/landing/", s3_archive_path="s3://purgo-bucket/archive/",
        delta_load_ts="2024-06-05T00:00:00.000+0000", full_or_incremental_load="FULL", zip_file="N", vendor="VendorE", delimiter=",", source_landing="Y",
        src_landing_table_name="table5", publish_unstitched="N", publish_unstitched_table_name=None, publish_stitched="N", publish_stitched_table_name=None,
        primary_key="id", header="Y", date_pattern="yyyy-MM-dd", actual_file_name="data_20240601.csv", vendor_file_deletion_flag="N", file_recursive_flag="N",
        total_weeks_req_data="4", total_weeks_file_data="4", active_flag="A"),
    # Edge: file already in Archive S3
    Row(config_id="6", source_object_name="obj6", source_system="sys6", file_name="data_20240601.csv", frequency="DAILY", location="loc6", domain="dom6", sub_domain="sub6",
        s3_vendor_path="s3://vendor-bucket/folder/", source_path=None, s3_landing_path="s3://purgo-bucket/landing/", s3_archive_path="s3://purgo-bucket/archive/",
        delta_load_ts="2024-06-06T00:00:00.000+0000", full_or_incremental_load="FULL", zip_file="N", vendor="VendorF", delimiter=",", source_landing="Y",
        src_landing_table_name="table6", publish_unstitched="N", publish_unstitched_table_name=None, publish_stitched="N", publish_stitched_table_name=None,
        primary_key="id", header="Y", date_pattern="yyyy-MM-dd", actual_file_name="data_20240601.csv", vendor_file_deletion_flag="N", file_recursive_flag="N",
        total_weeks_req_data="4", total_weeks_file_data="4", active_flag="A"),
    # Error: missing s3_landing_path
    Row(config_id="7", source_object_name="obj7", source_system="sys7", file_name="data_20240601.csv", frequency="DAILY", location="loc7", domain="dom7", sub_domain="sub7",
        s3_vendor_path="s3://vendor-bucket/folder/", source_path=None, s3_landing_path=None, s3_archive_path="s3://purgo-bucket/archive/",
        delta_load_ts="2024-06-07T00:00:00.000+0000", full_or_incremental_load="FULL", zip_file="N", vendor="VendorG", delimiter=",", source_landing="Y",
        src_landing_table_name="table7", publish_unstitched="N", publish_unstitched_table_name=None, publish_stitched="N", publish_stitched_table_name=None,
        primary_key="id", header="Y", date_pattern="yyyy-MM-dd", actual_file_name="data_20240601.csv", vendor_file_deletion_flag="N", file_recursive_flag="N",
        total_weeks_req_data="4", total_weeks_file_data="4", active_flag="A"),
    # Error: missing s3_vendor_path
    Row(config_id="8", source_object_name="obj8", source_system="sys8", file_name="data_20240601.csv", frequency="DAILY", location="loc8", domain="dom8", sub_domain="sub8",
        s3_vendor_path=None, source_path=None, s3_landing_path="s3://purgo-bucket/landing/", s3_archive_path="s3://purgo-bucket/archive/",
        delta_load_ts="2024-06-08T00:00:00.000+0000", full_or_incremental_load="FULL", zip_file="N", vendor="VendorH", delimiter=",", source_landing="Y",
        src_landing_table_name="table8", publish_unstitched="N", publish_unstitched_table_name=None, publish_stitched="N", publish_stitched_table_name=None,
        primary_key="id", header="Y", date_pattern="yyyy-MM-dd", actual_file_name="data_20240601.csv", vendor_file_deletion_flag="N", file_recursive_flag="N",
        total_weeks_req_data="4", total_weeks_file_data="4", active_flag="A"),
    # Error: missing s3_archive_path
    Row(config_id="9", source_object_name="obj9", source_system="sys9", file_name="data_20240601.csv", frequency="DAILY", location="loc9", domain="dom9", sub_domain="sub9",
        s3_vendor_path="s3://vendor-bucket/folder/", source_path=None, s3_landing_path="s3://purgo-bucket/landing/", s3_archive_path=None,
        delta_load_ts="2024-06-09T00:00:00.000+0000", full_or_incremental_load="FULL", zip_file="N", vendor="VendorI", delimiter=",", source_landing="Y",
        src_landing_table_name="table9", publish_unstitched="N", publish_unstitched_table_name=None, publish_stitched="N", publish_stitched_table_name=None,
        primary_key="id", header="Y", date_pattern="yyyy-MM-dd", actual_file_name="data_20240601.csv", vendor_file_deletion_flag="N", file_recursive_flag="N",
        total_weeks_req_data="4", total_weeks_file_data="4", active_flag="A"),
    # Error: file in subfolder (should not be ingested)
    Row(config_id="10", source_object_name="obj10", source_system="sys10", file_name="subfolder/data_20240601.csv", frequency="DAILY", location="loc10", domain="dom10", sub_domain="sub10",
        s3_vendor_path="s3://vendor-bucket/", source_path=None, s3_landing_path="s3://purgo-bucket/", s3_archive_path="s3://purgo-bucket/",
        delta_load_ts="2024-06-10T00:00:00.000+0000", full_or_incremental_load="FULL", zip_file="N", vendor="VendorJ", delimiter=",", source_landing="Y",
        src_landing_table_name="table10", publish_unstitched="N", publish_unstitched_table_name=None, publish_stitched="N", publish_stitched_table_name=None,
        primary_key="id", header="Y", date_pattern="yyyy-MM-dd", actual_file_name="subfolder/data_20240601.csv", vendor_file_deletion_flag="N", file_recursive_flag="N",
        total_weeks_req_data="4", total_weeks_file_data="4", active_flag="A"),
    # Edge: file name case sensitivity (should only match exact)
    Row(config_id="11", source_object_name="obj11", source_system="sys11", file_name="data_20240601.csv", frequency="DAILY", location="loc11", domain="dom11", sub_domain="sub11",
        s3_vendor_path="s3://vendor-bucket/folder/", source_path=None, s3_landing_path="s3://purgo-bucket/landing/", s3_archive_path="s3://purgo-bucket/archive/",
        delta_load_ts="2024-06-11T00:00:00.000+0000", full_or_incremental_load="FULL", zip_file="N", vendor="VendorK", delimiter=",", source_landing="Y",
        src_landing_table_name="table11", publish_unstitched="N", publish_unstitched_table_name=None, publish_stitched="N", publish_stitched_table_name=None,
        primary_key="id", header="Y", date_pattern="yyyy-MM-dd", actual_file_name="DATA_20240601.csv", vendor_file_deletion_flag="N", file_recursive_flag="N",
        total_weeks_req_data="4", total_weeks_file_data="4", active_flag="A"),
    # Edge: file name extension mismatch
    Row(config_id="12", source_object_name="obj12", source_system="sys12", file_name="data_20240601.csv", frequency="DAILY", location="loc12", domain="dom12", sub_domain="sub12",
        s3_vendor_path="s3://vendor-bucket/folder/", source_path=None, s3_landing_path="s3://purgo-bucket/landing/", s3_archive_path="s3://purgo-bucket/archive/",
        delta_load_ts="2024-06-12T00:00:00.000+0000", full_or_incremental_load="FULL", zip_file="N", vendor="VendorL", delimiter=",", source_landing="Y",
        src_landing_table_name="table12", publish_unstitched="N", publish_unstitched_table_name=None, publish_stitched="N", publish_stitched_table_name=None,
        primary_key="id", header="Y", date_pattern="yyyy-MM-dd", actual_file_name="data_20240601.txt", vendor_file_deletion_flag="N", file_recursive_flag="N",
        total_weeks_req_data="4", total_weeks_file_data="4", active_flag="A"),
    # Error: special/multibyte characters in file name
    Row(config_id="13", source_object_name="obj13", source_system="sys13", file_name="データ_20240601.csv", frequency="DAILY", location="loc13", domain="dom13", sub_domain="sub13",
        s3_vendor_path="s3://vendor-bucket/folder/", source_path=None, s3_landing_path="s3://purgo-bucket/landing/", s3_archive_path="s3://purgo-bucket/archive/",
        delta_load_ts="2024-06-13T00:00:00.000+0000", full_or_incremental_load="FULL", zip_file="N", vendor="VendorM", delimiter=",", source_landing="Y",
        src_landing_table_name="table13", publish_unstitched="N", publish_unstitched_table_name=None, publish_stitched="N", publish_stitched_table_name=None,
        primary_key="id", header="Y", date_pattern="yyyy-MM-dd", actual_file_name="データ_20240601.csv", vendor_file_deletion_flag="N", file_recursive_flag="N",
        total_weeks_req_data="4", total_weeks_file_data="4", active_flag="A"),
    # Error: special characters in S3 path
    Row(config_id="14", source_object_name="obj14", source_system="sys14", file_name="special!@#$.csv", frequency="DAILY", location="loc14", domain="dom14", sub_domain="sub14",
        s3_vendor_path="s3://vendor-bucket/folder!@#$/", source_path=None, s3_landing_path="s3://purgo-bucket/landing!@#$/", s3_archive_path="s3://purgo-bucket/archive!@#$/",
        delta_load_ts="2024-06-14T00:00:00.000+0000", full_or_incremental_load="FULL", zip_file="N", vendor="VendorN", delimiter=",", source_landing="Y",
        src_landing_table_name="table14", publish_unstitched="N", publish_unstitched_table_name=None, publish_stitched="N", publish_stitched_table_name=None,
        primary_key="id", header="Y", date_pattern="yyyy-MM-dd", actual_file_name="special!@#$.csv", vendor_file_deletion_flag="N", file_recursive_flag="N",
        total_weeks_req_data="4", total_weeks_file_data="4", active_flag="A"),
    # Error: NULL file_name
    Row(config_id="15", source_object_name="obj15", source_system="sys15", file_name=None, frequency="DAILY", location="loc15", domain="dom15", sub_domain="sub15",
        s3_vendor_path="s3://vendor-bucket/folder/", source_path=None, s3_landing_path="s3://purgo-bucket/landing/", s3_archive_path="s3://purgo-bucket/archive/",
        delta_load_ts="2024-06-15T00:00:00.000+0000", full_or_incremental_load="FULL", zip_file="N", vendor="VendorO", delimiter=",", source_landing="Y",
        src_landing_table_name="table15", publish_unstitched="N", publish_unstitched_table_name=None, publish_stitched="N", publish_stitched_table_name=None,
        primary_key="id", header="Y", date_pattern="yyyy-MM-dd", actual_file_name=None, vendor_file_deletion_flag="N", file_recursive_flag="N",
        total_weeks_req_data="4", total_weeks_file_data="4", active_flag="A"),
    # Error: NULL active_flag
    Row(config_id="16", source_object_name="obj16", source_system="sys16", file_name="data_20240601.csv", frequency="DAILY", location="loc16", domain="dom16", sub_domain="sub16",
        s3_vendor_path="s3://vendor-bucket/folder/", source_path=None, s3_landing_path="s3://purgo-bucket/landing/", s3_archive_path="s3://purgo-bucket/archive/",
        delta_load_ts="2024-06-16T00:00:00.000+0000", full_or_incremental_load="FULL", zip_file="N", vendor="VendorP", delimiter=",", source_landing="Y",
        src_landing_table_name="table16", publish_unstitched="N", publish_unstitched_table_name=None, publish_stitched="N", publish_stitched_table_name=None,
        primary_key="id", header="Y", date_pattern="yyyy-MM-dd", actual_file_name="data_20240601.csv", vendor_file_deletion_flag="N", file_recursive_flag="N",
        total_weeks_req_data="4", total_weeks_file_data="4", active_flag=None),
    # Error: empty string S3 path
    Row(config_id="17", source_object_name="obj17", source_system="sys17", file_name="data_20240601.csv", frequency="DAILY", location="loc17", domain="dom17", sub_domain="sub17",
        s3_vendor_path="", source_path=None, s3_landing_path="s3://purgo-bucket/landing/", s3_archive_path="s3://purgo-bucket/archive/",
        delta_load_ts="2024-06-17T00:00:00.000+0000", full_or_incremental_load="FULL", zip_file="N", vendor="VendorQ", delimiter=",", source_landing="Y",
        src_landing_table_name="table17", publish_unstitched="N", publish_unstitched_table_name=None, publish_stitched="N", publish_stitched_table_name=None,
        primary_key="id", header="Y", date_pattern="yyyy-MM-dd", actual_file_name="data_20240601.csv", vendor_file_deletion_flag="N", file_recursive_flag="N",
        total_weeks_req_data="4", total_weeks_file_data="4", active_flag="A"),
    # Error: empty string file_name
    Row(config_id="18", source_object_name="obj18", source_system="sys18", file_name="", frequency="DAILY", location="loc18", domain="dom18", sub_domain="sub18",
        s3_vendor_path="s3://vendor-bucket/folder/", source_path=None, s3_landing_path="s3://purgo-bucket/landing/", s3_archive_path="s3://purgo-bucket/archive/",
        delta_load_ts="2024-06-18T00:00:00.000+0000", full_or_incremental_load="FULL", zip_file="N", vendor="VendorR", delimiter=",", source_landing="Y",
        src_landing_table_name="table18", publish_unstitched="N", publish_unstitched_table_name=None, publish_stitched="N", publish_stitched_table_name=None,
        primary_key="id", header="Y", date_pattern="yyyy-MM-dd", actual_file_name="", vendor_file_deletion_flag="N", file_recursive_flag="N",
        total_weeks_req_data="4", total_weeks_file_data="4", active_flag="A"),
    # Happy path: file with special characters in name
    Row(config_id="19", source_object_name="obj19", source_system="sys19", file_name="file_#€_20240601.csv", frequency="DAILY", location="loc19", domain="dom19", sub_domain="sub19",
        s3_vendor_path="s3://vendor-bucket/folder/", source_path=None, s3_landing_path="s3://purgo-bucket/landing/", s3_archive_path="s3://purgo-bucket/archive/",
        delta_load_ts="2024-06-19T00:00:00.000+0000", full_or_incremental_load="FULL", zip_file="N", vendor="VendorS", delimiter=",", source_landing="Y",
        src_landing_table_name="table19", publish_unstitched="N", publish_unstitched_table_name=None, publish_stitched="N", publish_stitched_table_name=None,
        primary_key="id", header="Y", date_pattern="yyyy-MM-dd", actual_file_name="file_#€_20240601.csv", vendor_file_deletion_flag="N", file_recursive_flag="N",
        total_weeks_req_data="4", total_weeks_file_data="4", active_flag="A"),
    # Happy path: file with multi-byte characters in name
    Row(config_id="20", source_object_name="obj20", source_system="sys20", file_name="файл_20240601.csv", frequency="DAILY", location="loc20", domain="dom20", sub_domain="sub20",
        s3_vendor_path="s3://vendor-bucket/folder/", source_path=None, s3_landing_path="s3://purgo-bucket/landing/", s3_archive_path="s3://purgo-bucket/archive/",
        delta_load_ts="2024-06-20T00:00:00.000+0000", full_or_incremental_load="FULL", zip_file="N", vendor="VendorT", delimiter=",", source_landing="Y",
        src_landing_table_name="table20", publish_unstitched="N", publish_unstitched_table_name=None, publish_stitched="N", publish_stitched_table_name=None,
        primary_key="id", header="Y", date_pattern="yyyy-MM-dd", actual_file_name="файл_20240601.csv", vendor_file_deletion_flag="N", file_recursive_flag="N",
        total_weeks_req_data="4", total_weeks_file_data="4", active_flag="A"),
    # Error: file name with only extension
    Row(config_id="21", source_object_name="obj21", source_system="sys21", file_name=".csv", frequency="DAILY", location="loc21", domain="dom21", sub_domain="sub21",
        s3_vendor_path="s3://vendor-bucket/folder/", source_path=None, s3_landing_path="s3://purgo-bucket/landing/", s3_archive_path="s3://purgo-bucket/archive/",
        delta_load_ts="2024-06-21T00:00:00.000+0000", full_or_incremental_load="FULL", zip_file="N", vendor="VendorU", delimiter=",", source_landing="Y",
        src_landing_table_name="table21", publish_unstitched="N", publish_unstitched_table_name=None, publish_stitched="N", publish_stitched_table_name=None,
        primary_key="id", header="Y", date_pattern="yyyy-MM-dd", actual_file_name=".csv", vendor_file_deletion_flag="N", file_recursive_flag="N",
        total_weeks_req_data="4", total_weeks_file_data="4", active_flag="A"),
    # Error: file name with whitespace
    Row(config_id="22", source_object_name="obj22", source_system="sys22", file_name="   ", frequency="DAILY", location="loc22", domain="dom22", sub_domain="sub22",
        s3_vendor_path="s3://vendor-bucket/folder/", source_path=None, s3_landing_path="s3://purgo-bucket/landing/", s3_archive_path="s3://purgo-bucket/archive/",
        delta_load_ts="2024-06-22T00:00:00.000+0000", full_or_incremental_load="FULL", zip_file="N", vendor="VendorV", delimiter=",", source_landing="Y",
        src_landing_table_name="table22", publish_unstitched="N", publish_unstitched_table_name=None, publish_stitched="N", publish_stitched_table_name=None,
        primary_key="id", header="Y", date_pattern="yyyy-MM-dd", actual_file_name="   ", vendor_file_deletion_flag="N", file_recursive_flag="N",
        total_weeks_req_data="4", total_weeks_file_data="4", active_flag="A"),
    # Happy path: file with dash and underscore
    Row(config_id="23", source_object_name="obj23", source_system="sys23", file_name="file-20240601_test.csv", frequency="DAILY", location="loc23", domain="dom23", sub_domain="sub23",
        s3_vendor_path="s3://vendor-bucket/folder/", source_path=None, s3_landing_path="s3://purgo-bucket/landing/", s3_archive_path="s3://purgo-bucket/archive/",
        delta_load_ts="2024-06-23T00:00:00.000+0000", full_or_incremental_load="FULL", zip_file="N", vendor="VendorW", delimiter=",", source_landing="Y",
        src_landing_table_name="table23", publish_unstitched="N", publish_unstitched_table_name=None, publish_stitched="N", publish_stitched_table_name=None,
        primary_key="id", header="Y", date_pattern="yyyy-MM-dd", actual_file_name="file-20240601_test.csv", vendor_file_deletion_flag="N", file_recursive_flag="N",
        total_weeks_req_data="4", total_weeks_file_data="4", active_flag="A"),
    # Happy path: file with long name
    Row(config_id="24", source_object_name="obj24", source_system="sys24", file_name="very_long_file_name_20240601_abcdefghijklmnopqrstuvwxyz.csv", frequency="DAILY", location="loc24", domain="dom24", sub_domain="sub24",
        s3_vendor_path="s3://vendor-bucket/folder/", source_path=None, s3_landing_path="s3://purgo-bucket/landing/", s3_archive_path="s3://purgo-bucket/archive/",
        delta_load_ts="2024-06-24T00:00:00.000+0000", full_or_incremental_load="FULL", zip_file="N", vendor="VendorX", delimiter=",", source_landing="Y",
        src_landing_table_name="table24", publish_unstitched="N", publish_unstitched_table_name=None, publish_stitched="N", publish_stitched_table_name=None,
        primary_key="id", header="Y", date_pattern="yyyy-MM-dd", actual_file_name="very_long_file_name_20240601_abcdefghijklmnopqrstuvwxyz.csv", vendor_file_deletion_flag="N", file_recursive_flag="N",
        total_weeks_req_data="4", total_weeks_file_data="4", active_flag="A"),
    # Happy path: file with numbers only
    Row(config_id="25", source_object_name="obj25", source_system="sys25", file_name="1234567890.csv", frequency="DAILY", location="loc25", domain="dom25", sub_domain="sub25",
        s3_vendor_path="s3://vendor-bucket/folder/", source_path=None, s3_landing_path="s3://purgo-bucket/landing/", s3_archive_path="s3://purgo-bucket/archive/",
        delta_load_ts="2024-06-25T00:00:00.000+0000", full_or_incremental_load="FULL", zip_file="N", vendor="VendorY", delimiter=",", source_landing="Y",
        src_landing_table_name="table25", publish_unstitched="N", publish_unstitched_table_name=None, publish_stitched="N", publish_stitched_table_name=None,
        primary_key="id", header="Y", date_pattern="yyyy-MM-dd", actual_file_name="1234567890.csv", vendor_file_deletion_flag="N", file_recursive_flag="N",
        total_weeks_req_data="4", total_weeks_file_data="4", active_flag="A"),
]

ingest_config_master_df = spark.createDataFrame(ingest_config_master_data, schema=ingest_config_master_schema)

# =========================
# Test Data for s3_file_process_log
# =========================

# Define schema for s3_file_process_log
s3_file_process_log_schema = StructType([
    StructField("file_name", StringType(), True),
    StructField("s3_vendor_path", StringType(), True),
    StructField("s3_landing_path", StringType(), True),
    StructField("s3_archive_path", StringType(), True),
    StructField("file_status", StringType(), True),
    StructField("file_processed_date", TimestampType(), True)
])

now = datetime.now()

s3_file_process_log_data = [
    # Happy path: file transferred successfully
    Row(file_name="data_20240601.csv", s3_vendor_path="s3://vendor-bucket/", s3_landing_path="s3://purgo-bucket/", s3_archive_path="s3://purgo-bucket/", file_status="SUCCESS", file_processed_date=now),
    # Skipped: file already in Purgo
    Row(file_name="data_20240601.csv", s3_vendor_path="s3://vendor-bucket/", s3_landing_path="s3://purgo-bucket/", s3_archive_path="s3://purgo-bucket/", file_status="SKIPPED", file_processed_date=now),
    # Failed: permission denied
    Row(file_name="data_20240601.csv", s3_vendor_path="s3://vendor-bucket/", s3_landing_path="s3://purgo-bucket/", s3_archive_path="s3://purgo-bucket/", file_status="FAILED", file_processed_date=now),
    # Happy path: file with special characters
    Row(file_name="file_#€_20240601.csv", s3_vendor_path="s3://vendor-bucket/folder/", s3_landing_path="s3://purgo-bucket/landing/", s3_archive_path="s3://purgo-bucket/archive/", file_status="SUCCESS", file_processed_date=now),
    # Happy path: file with multi-byte characters
    Row(file_name="データ_20240601.csv", s3_vendor_path="s3://vendor-bucket/folder/", s3_landing_path="s3://purgo-bucket/landing/", s3_archive_path="s3://purgo-bucket/archive/", file_status="SUCCESS", file_processed_date=now),
    # Edge: file with empty name
    Row(file_name="", s3_vendor_path="s3://vendor-bucket/folder/", s3_landing_path="s3://purgo-bucket/landing/", s3_archive_path="s3://purgo-bucket/archive/", file_status="FAILED", file_processed_date=now),
    # Edge: file with whitespace name
    Row(file_name="   ", s3_vendor_path="s3://vendor-bucket/folder/", s3_landing_path="s3://purgo-bucket/landing/", s3_archive_path="s3://purgo-bucket/archive/", file_status="FAILED", file_processed_date=now),
    # Edge: file with only extension
    Row(file_name=".csv", s3_vendor_path="s3://vendor-bucket/folder/", s3_landing_path="s3://purgo-bucket/landing/", s3_archive_path="s3://purgo-bucket/archive/", file_status="FAILED", file_processed_date=now),
]

s3_file_process_log_df = spark.createDataFrame(s3_file_process_log_data, schema=s3_file_process_log_schema)

# =========================
# Write test data to Unity Catalog tables
# =========================

# Write ingest_config_master test data
ingest_config_master_df.write.mode("overwrite").format("delta").option("overwriteSchema", "true").saveAsTable("purgo_databricks.purgo_playground.ingest_config_master")

# Write s3_file_process_log test data
s3_file_process_log_df.write.mode("overwrite").format("delta").option("overwriteSchema", "true").saveAsTable("purgo_databricks.purgo_playground.s3_file_process_log")
