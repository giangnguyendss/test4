spark.catalog.setCurrentCatalog("purgo_databricks")

# Test Data Generation for purgo_playground.ingest_config_master, s3_file_process_log, and S3 folder/file simulation

# from pyspark.sql import SparkSession  # SparkSession is already available in Databricks
from pyspark.sql.types import (StructType, StructField, StringType, TimestampType)  
from pyspark.sql import Row  
from pyspark.sql.functions import lit, current_timestamp  

# -------------------------------
# 1. Test Data for ingest_config_master
# -------------------------------

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

ingest_config_master_data = [
    # Happy path
    Row(config_id="123", source_object_name="objA", source_system="sysA", file_name="data1.csv", frequency="DAILY", location="locA", domain="domA", sub_domain="subA",
        s3_vendor_path="s3://vendor-bucket/folderA/", source_path=None, s3_landing_path="s3://purgo-bucket/landingA/", s3_archive_path="s3://purgo-bucket/archiveA/",
        delta_load_ts="2024-03-21T00:00:00.000+0000", full_or_incremental_load="FULL", zip_file="N", vendor="VendorA", delimiter=",", source_landing=None,
        src_landing_table_name=None, publish_unstitched=None, publish_unstitched_table_name=None, publish_stitched=None, publish_stitched_table_name=None,
        primary_key=None, header="Y", date_pattern=None, actual_file_name=None, vendor_file_deletion_flag=None, file_recursive_flag="N", total_weeks_req_data=None,
        total_weeks_file_data=None, active_flag="A"),
    # Inactive config
    Row(config_id="124", source_object_name="objB", source_system="sysB", file_name="data3.csv", frequency="WEEKLY", location="locB", domain="domB", sub_domain="subB",
        s3_vendor_path="s3://vendor-bucket/folderB/", source_path=None, s3_landing_path="s3://purgo-bucket/landingB/", s3_archive_path="s3://purgo-bucket/archiveB/",
        delta_load_ts="2024-03-22T00:00:00.000+0000", full_or_incremental_load="INCR", zip_file="N", vendor="VendorB", delimiter="|", source_landing=None,
        src_landing_table_name=None, publish_unstitched=None, publish_unstitched_table_name=None, publish_stitched=None, publish_stitched_table_name=None,
        primary_key=None, header="N", date_pattern=None, actual_file_name=None, vendor_file_deletion_flag=None, file_recursive_flag="N", total_weeks_req_data=None,
        total_weeks_file_data=None, active_flag="I"),
    # File in archive
    Row(config_id="125", source_object_name="objC", source_system="sysC", file_name="data4.csv", frequency="MONTHLY", location="locC", domain="domC", sub_domain="subC",
        s3_vendor_path="s3://vendor-bucket/folderC/", source_path=None, s3_landing_path="s3://purgo-bucket/landingC/", s3_archive_path="s3://purgo-bucket/archiveC/",
        delta_load_ts="2024-03-23T00:00:00.000+0000", full_or_incremental_load="FULL", zip_file="Y", vendor="VendorC", delimiter=";", source_landing=None,
        src_landing_table_name=None, publish_unstitched=None, publish_unstitched_table_name=None, publish_stitched=None, publish_stitched_table_name=None,
        primary_key=None, header="Y", date_pattern=None, actual_file_name=None, vendor_file_deletion_flag=None, file_recursive_flag="N", total_weeks_req_data=None,
        total_weeks_file_data=None, active_flag="A"),
    # Missing S3 path (error case)
    Row(config_id="126", source_object_name="objD", source_system="sysD", file_name="data5.csv", frequency="DAILY", location="locD", domain="domD", sub_domain="subD",
        s3_vendor_path=None, source_path=None, s3_landing_path="s3://purgo-bucket/landingD/", s3_archive_path="s3://purgo-bucket/archiveD/",
        delta_load_ts="2024-03-24T00:00:00.000+0000", full_or_incremental_load="FULL", zip_file="N", vendor="VendorD", delimiter=",", source_landing=None,
        src_landing_table_name=None, publish_unstitched=None, publish_unstitched_table_name=None, publish_stitched=None, publish_stitched_table_name=None,
        primary_key=None, header="Y", date_pattern=None, actual_file_name=None, vendor_file_deletion_flag=None, file_recursive_flag="N", total_weeks_req_data=None,
        total_weeks_file_data=None, active_flag="A"),
    # Case-sensitive file name
    Row(config_id="127", source_object_name="objE", source_system="sysE", file_name="Data6.CSV", frequency="DAILY", location="locE", domain="domE", sub_domain="subE",
        s3_vendor_path="s3://vendor-bucket/folderE/", source_path=None, s3_landing_path="s3://purgo-bucket/landingE/", s3_archive_path="s3://purgo-bucket/archiveE/",
        delta_load_ts="2024-03-25T00:00:00.000+0000", full_or_incremental_load="FULL", zip_file="N", vendor="VendorE", delimiter=",", source_landing=None,
        src_landing_table_name=None, publish_unstitched=None, publish_unstitched_table_name=None, publish_stitched=None, publish_stitched_table_name=None,
        primary_key=None, header="Y", date_pattern=None, actual_file_name=None, vendor_file_deletion_flag=None, file_recursive_flag="N", total_weeks_req_data=None,
        total_weeks_file_data=None, active_flag="A"),
    # Data-driven test cases (201-205)
    Row(config_id="201", source_object_name="objF", source_system="sysF", file_name="fileA.csv", frequency="DAILY", location="locF", domain="domF", sub_domain="subF",
        s3_vendor_path="s3://vendor-bucket/folderF/", source_path=None, s3_landing_path="s3://purgo-bucket/landingF/", s3_archive_path="s3://purgo-bucket/archiveF/",
        delta_load_ts="2024-03-26T00:00:00.000+0000", full_or_incremental_load="FULL", zip_file="N", vendor="VendorF", delimiter=",", source_landing=None,
        src_landing_table_name=None, publish_unstitched=None, publish_unstitched_table_name=None, publish_stitched=None, publish_stitched_table_name=None,
        primary_key=None, header="Y", date_pattern=None, actual_file_name=None, vendor_file_deletion_flag=None, file_recursive_flag="N", total_weeks_req_data=None,
        total_weeks_file_data=None, active_flag="A"),
    Row(config_id="202", source_object_name="objG", source_system="sysG", file_name="fileB.csv", frequency="DAILY", location="locG", domain="domG", sub_domain="subG",
        s3_vendor_path="s3://vendor-bucket/folderG/", source_path=None, s3_landing_path="s3://purgo-bucket/landingG/", s3_archive_path="s3://purgo-bucket/archiveG/",
        delta_load_ts="2024-03-27T00:00:00.000+0000", full_or_incremental_load="FULL", zip_file="N", vendor="VendorG", delimiter=",", source_landing=None,
        src_landing_table_name=None, publish_unstitched=None, publish_unstitched_table_name=None, publish_stitched=None, publish_stitched_table_name=None,
        primary_key=None, header="Y", date_pattern=None, actual_file_name=None, vendor_file_deletion_flag=None, file_recursive_flag="N", total_weeks_req_data=None,
        total_weeks_file_data=None, active_flag="A"),
    Row(config_id="203", source_object_name="objH", source_system="sysH", file_name="fileC.csv", frequency="DAILY", location="locH", domain="domH", sub_domain="subH",
        s3_vendor_path="s3://vendor-bucket/folderH/", source_path=None, s3_landing_path="s3://purgo-bucket/landingH/", s3_archive_path="s3://purgo-bucket/archiveH/",
        delta_load_ts="2024-03-28T00:00:00.000+0000", full_or_incremental_load="FULL", zip_file="N", vendor="VendorH", delimiter=",", source_landing=None,
        src_landing_table_name=None, publish_unstitched=None, publish_unstitched_table_name=None, publish_stitched=None, publish_stitched_table_name=None,
        primary_key=None, header="Y", date_pattern=None, actual_file_name=None, vendor_file_deletion_flag=None, file_recursive_flag="N", total_weeks_req_data=None,
        total_weeks_file_data=None, active_flag="A"),
    Row(config_id="204", source_object_name="objI", source_system="sysI", file_name="fileD.csv", frequency="DAILY", location="locI", domain="domI", sub_domain="subI",
        s3_vendor_path="s3://vendor-bucket/folderI/", source_path=None, s3_landing_path="s3://purgo-bucket/landingI/", s3_archive_path="s3://purgo-bucket/archiveI/",
        delta_load_ts="2024-03-29T00:00:00.000+0000", full_or_incremental_load="FULL", zip_file="N", vendor="VendorI", delimiter=",", source_landing=None,
        src_landing_table_name=None, publish_unstitched=None, publish_unstitched_table_name=None, publish_stitched=None, publish_stitched_table_name=None,
        primary_key=None, header="Y", date_pattern=None, actual_file_name=None, vendor_file_deletion_flag=None, file_recursive_flag="N", total_weeks_req_data=None,
        total_weeks_file_data=None, active_flag="I"),
    Row(config_id="205", source_object_name="objJ", source_system="sysJ", file_name="fileE.csv", frequency="DAILY", location="locJ", domain="domJ", sub_domain="subJ",
        s3_vendor_path="s3://vendor-bucket/folderJ/", source_path=None, s3_landing_path="s3://purgo-bucket/landingJ/", s3_archive_path="s3://purgo-bucket/archiveJ/",
        delta_load_ts="2024-03-30T00:00:00.000+0000", full_or_incremental_load="FULL", zip_file="N", vendor="VendorJ", delimiter=",", source_landing=None,
        src_landing_table_name=None, publish_unstitched=None, publish_unstitched_table_name=None, publish_stitched=None, publish_stitched_table_name=None,
        primary_key=None, header="Y", date_pattern=None, actual_file_name=None, vendor_file_deletion_flag=None, file_recursive_flag="N", total_weeks_req_data=None,
        total_weeks_file_data=None, active_flag="A"),
    # Special characters, multi-byte, NULLs, edge cases
    Row(config_id="301", source_object_name="objK", source_system="sysK", file_name="fileG.csv", frequency="DAILY", location="locK", domain="domK", sub_domain="subK",
        s3_vendor_path="s3://vendor-bucket/folderK/", source_path=None, s3_landing_path="s3://purgo-bucket/landingK/", s3_archive_path="s3://purgo-bucket/archiveK/",
        delta_load_ts="2024-03-31T00:00:00.000+0000", full_or_incremental_load="FULL", zip_file="N", vendor="VendorK", delimiter=",", source_landing=None,
        src_landing_table_name=None, publish_unstitched=None, publish_unstitched_table_name=None, publish_stitched=None, publish_stitched_table_name=None,
        primary_key=None, header="Y", date_pattern=None, actual_file_name=None, vendor_file_deletion_flag=None, file_recursive_flag="N", total_weeks_req_data=None,
        total_weeks_file_data=None, active_flag="A"),
    Row(config_id="401", source_object_name="objL", source_system="sysL", file_name="fileI.csv", frequency="DAILY", location="locL", domain="domL", sub_domain="subL",
        s3_vendor_path="s3://vendor-bucket/folderL/", source_path=None, s3_landing_path="s3://purgo-bucket/landingL/", s3_archive_path="s3://purgo-bucket/archiveL/",
        delta_load_ts="2024-04-01T00:00:00.000+0000", full_or_incremental_load="FULL", zip_file="N", vendor="VendorL", delimiter=",", source_landing=None,
        src_landing_table_name=None, publish_unstitched=None, publish_unstitched_table_name=None, publish_stitched=None, publish_stitched_table_name=None,
        primary_key=None, header="Y", date_pattern=None, actual_file_name=None, vendor_file_deletion_flag=None, file_recursive_flag="N", total_weeks_req_data=None,
        total_weeks_file_data=None, active_flag="A"),
    Row(config_id="501", source_object_name="objM", source_system="sysM", file_name="fileJ.csv", frequency="DAILY", location="locM", domain="domM", sub_domain="subM",
        s3_vendor_path="s3://vendor-bucket/folderM/", source_path=None, s3_landing_path="s3://purgo-bucket/landingM/", s3_archive_path="s3://purgo-bucket/archiveM/",
        delta_load_ts="2024-04-02T00:00:00.000+0000", full_or_incremental_load="FULL", zip_file="N", vendor="VendorM", delimiter=",", source_landing=None,
        src_landing_table_name=None, publish_unstitched=None, publish_unstitched_table_name=None, publish_stitched=None, publish_stitched_table_name=None,
        primary_key=None, header="Y", date_pattern=None, actual_file_name=None, vendor_file_deletion_flag=None, file_recursive_flag="N", total_weeks_req_data=None,
        total_weeks_file_data=None, active_flag="A"),
    Row(config_id="601", source_object_name="objN", source_system="sysN", file_name="fileK.csv", frequency="DAILY", location="locN", domain="domN", sub_domain="subN",
        s3_vendor_path="s3://vendor-bucket/folderN/", source_path=None, s3_landing_path="s3://purgo-bucket/landingN/", s3_archive_path="s3://purgo-bucket/archiveN/",
        delta_load_ts="2024-04-03T00:00:00.000+0000", full_or_incremental_load="FULL", zip_file="N", vendor="VendorN", delimiter=",", source_landing=None,
        src_landing_table_name=None, publish_unstitched=None, publish_unstitched_table_name=None, publish_stitched=None, publish_stitched_table_name=None,
        primary_key=None, header="Y", date_pattern=None, actual_file_name=None, vendor_file_deletion_flag=None, file_recursive_flag="N", total_weeks_req_data=None,
        total_weeks_file_data=None, active_flag="A"),
    # Special/multibyte chars
    Row(config_id="777", source_object_name="objΩ", source_system="sysΩ", file_name="spécial_文件.csv", frequency="DAILY", location="locΩ", domain="domΩ", sub_domain="subΩ",
        s3_vendor_path="s3://vendor-bucket/folderΩ/", source_path=None, s3_landing_path="s3://purgo-bucket/landingΩ/", s3_archive_path="s3://purgo-bucket/archiveΩ/",
        delta_load_ts="2024-04-04T00:00:00.000+0000", full_or_incremental_load="FULL", zip_file="N", vendor="VendorΩ", delimiter=";", source_landing=None,
        src_landing_table_name=None, publish_unstitched=None, publish_unstitched_table_name=None, publish_stitched=None, publish_stitched_table_name=None,
        primary_key=None, header="Y", date_pattern=None, actual_file_name=None, vendor_file_deletion_flag=None, file_recursive_flag="N", total_weeks_req_data=None,
        total_weeks_file_data=None, active_flag="A"),
    # NULLs and edge
    Row(config_id=None, source_object_name=None, source_system=None, file_name=None, frequency=None, location=None, domain=None, sub_domain=None,
        s3_vendor_path=None, source_path=None, s3_landing_path=None, s3_archive_path=None,
        delta_load_ts=None, full_or_incremental_load=None, zip_file=None, vendor=None, delimiter=None, source_landing=None,
        src_landing_table_name=None, publish_unstitched=None, publish_unstitched_table_name=None, publish_stitched=None, publish_stitched_table_name=None,
        primary_key=None, header=None, date_pattern=None, actual_file_name=None, vendor_file_deletion_flag=None, file_recursive_flag=None, total_weeks_req_data=None,
        total_weeks_file_data=None, active_flag=None),
]

ingest_config_master_df = spark.createDataFrame(ingest_config_master_data, schema=ingest_config_master_schema)
ingest_config_master_df.write.mode("overwrite").format("delta").saveAsTable("purgo_playground.ingest_config_master")

# -------------------------------
# 2. Test Data for s3_file_process_log
# -------------------------------

s3_file_process_log_schema = StructType([
    StructField("file_name", StringType(), True),
    StructField("s3_vendor_path", StringType(), True),
    StructField("s3_landing_path", StringType(), True),
    StructField("s3_archive_path", StringType(), True),
    StructField("file_status", StringType(), True),
    StructField("file_processed_date", TimestampType(), True)
])

s3_file_process_log_data = [
    # Happy path: file copied
    Row(file_name="data1.csv", s3_vendor_path="s3://vendor-bucket/folderA/", s3_landing_path="s3://purgo-bucket/landingA/", s3_archive_path="s3://purgo-bucket/archiveA/", file_status="SUCCESS", file_processed_date=None),
    # Skipped: already exists in Purgo
    Row(file_name="data2.csv", s3_vendor_path="s3://vendor-bucket/folderA/", s3_landing_path="s3://purgo-bucket/landingA/", s3_archive_path="s3://purgo-bucket/archiveA/", file_status="SKIPPED_EXISTS", file_processed_date=None),
    # Skipped: inactive config
    Row(file_name="data3.csv", s3_vendor_path="s3://vendor-bucket/folderB/", s3_landing_path="s3://purgo-bucket/landingB/", s3_archive_path="s3://purgo-bucket/archiveB/", file_status="SKIPPED_INACTIVE", file_processed_date=None),
    # Skipped: in archive
    Row(file_name="data4.csv", s3_vendor_path="s3://vendor-bucket/folderC/", s3_landing_path="s3://purgo-bucket/landingC/", s3_archive_path="s3://purgo-bucket/archiveC/", file_status="SKIPPED_ARCHIVED", file_processed_date=None),
    # Error: missing config
    Row(file_name="data5.csv", s3_vendor_path=None, s3_landing_path="s3://purgo-bucket/landingD/", s3_archive_path="s3://purgo-bucket/archiveD/", file_status="ERROR_CONFIG", file_processed_date=None),
    # Case-sensitive: only Data6.CSV copied
    Row(file_name="Data6.CSV", s3_vendor_path="s3://vendor-bucket/folderE/", s3_landing_path="s3://purgo-bucket/landingE/", s3_archive_path="s3://purgo-bucket/archiveE/", file_status="SUCCESS", file_processed_date=None),
    Row(file_name="data6.csv", s3_vendor_path="s3://vendor-bucket/folderE/", s3_landing_path="s3://purgo-bucket/landingE/", s3_archive_path="s3://purgo-bucket/archiveE/", file_status="SKIPPED_NOT_CONFIGURED", file_processed_date=None),
    # Data-driven
    Row(file_name="fileA.csv", s3_vendor_path="s3://vendor-bucket/folderF/", s3_landing_path="s3://purgo-bucket/landingF/", s3_archive_path="s3://purgo-bucket/archiveF/", file_status="SUCCESS", file_processed_date=None),
    Row(file_name="fileB.csv", s3_vendor_path="s3://vendor-bucket/folderG/", s3_landing_path="s3://purgo-bucket/landingG/", s3_archive_path="s3://purgo-bucket/archiveG/", file_status="SKIPPED_EXISTS", file_processed_date=None),
    Row(file_name="fileC.csv", s3_vendor_path="s3://vendor-bucket/folderH/", s3_landing_path="s3://purgo-bucket/landingH/", s3_archive_path="s3://purgo-bucket/archiveH/", file_status="SKIPPED_ARCHIVED", file_processed_date=None),
    Row(file_name="fileD.csv", s3_vendor_path="s3://vendor-bucket/folderI/", s3_landing_path="s3://purgo-bucket/landingI/", s3_archive_path="s3://purgo-bucket/archiveI/", file_status="SKIPPED_INACTIVE", file_processed_date=None),
    Row(file_name="fileF.csv", s3_vendor_path="s3://vendor-bucket/folderJ/", s3_landing_path="s3://purgo-bucket/landingJ/", s3_archive_path="s3://purgo-bucket/archiveJ/", file_status="SKIPPED_NOT_CONFIGURED", file_processed_date=None),
    # Logging scenario
    Row(file_name="file1.csv", s3_vendor_path="s3://vendor-bucket/folderX/", s3_landing_path="s3://purgo-bucket/landingX/", s3_archive_path="s3://purgo-bucket/archiveX/", file_status="SUCCESS", file_processed_date=None),
    Row(file_name="file2.csv", s3_vendor_path="s3://vendor-bucket/folderX/", s3_landing_path="s3://purgo-bucket/landingX/", s3_archive_path="s3://purgo-bucket/archiveX/", file_status="SKIPPED_EXISTS", file_processed_date=None),
    Row(file_name="file3.csv", s3_vendor_path="s3://vendor-bucket/folderX/", s3_landing_path="s3://purgo-bucket/landingX/", s3_archive_path="s3://purgo-bucket/archiveX/", file_status="SKIPPED_INACTIVE", file_processed_date=None),
    # Subfolder edge
    Row(file_name="fileG.csv", s3_vendor_path="s3://vendor-bucket/folderK/", s3_landing_path="s3://purgo-bucket/landingK/", s3_archive_path="s3://purgo-bucket/archiveK/", file_status="SUCCESS", file_processed_date=None),
    # S3 access error
    Row(file_name="fileI.csv", s3_vendor_path="s3://vendor-bucket/folderL/", s3_landing_path="s3://purgo-bucket/landingL/", s3_archive_path="s3://purgo-bucket/archiveL/", file_status="ERROR_S3_ACCESS", file_processed_date=None),
    # No eligible files
    # Special/multibyte
    Row(file_name="spécial_文件.csv", s3_vendor_path="s3://vendor-bucket/folderΩ/", s3_landing_path="s3://purgo-bucket/landingΩ/", s3_archive_path="s3://purgo-bucket/archiveΩ/", file_status="SUCCESS", file_processed_date=None),
    # NULLs
    Row(file_name=None, s3_vendor_path=None, s3_landing_path=None, s3_archive_path=None, file_status=None, file_processed_date=None),
]

s3_file_process_log_df = spark.createDataFrame(s3_file_process_log_data, schema=s3_file_process_log_schema)
s3_file_process_log_df = s3_file_process_log_df.withColumn("file_processed_date", current_timestamp())
s3_file_process_log_df.write.mode("overwrite").format("delta").saveAsTable("purgo_playground.s3_file_process_log")

# -------------------------------
# 3. Simulated S3 Folder/File Listings (as DataFrames for test logic)
# -------------------------------

# Vendor S3 folder simulation
vendor_s3_files = [
    # folderA
    Row(s3_path="s3://vendor-bucket/folderA/", file_name="data1.csv"),
    Row(s3_path="s3://vendor-bucket/folderA/", file_name="data2.csv"),
    # folderB
    Row(s3_path="s3://vendor-bucket/folderB/", file_name="data3.csv"),
    # folderC
    Row(s3_path="s3://vendor-bucket/folderC/", file_name="data4.csv"),
    # folderE (case-sensitive)
    Row(s3_path="s3://vendor-bucket/folderE/", file_name="data6.csv"),
    Row(s3_path="s3://vendor-bucket/folderE/", file_name="Data6.CSV"),
    # folderF-J
    Row(s3_path="s3://vendor-bucket/folderF/", file_name="fileA.csv"),
    Row(s3_path="s3://vendor-bucket/folderG/", file_name="fileB.csv"),
    Row(s3_path="s3://vendor-bucket/folderH/", file_name="fileC.csv"),
    Row(s3_path="s3://vendor-bucket/folderI/", file_name="fileD.csv"),
    Row(s3_path="s3://vendor-bucket/folderJ/", file_name="fileF.csv"),
    # folderK (subfolder edge)
    Row(s3_path="s3://vendor-bucket/folderK/", file_name="fileG.csv"),
    Row(s3_path="s3://vendor-bucket/folderK/", file_name="subfolder/fileH.csv"),
    # folderL (S3 access error)
    Row(s3_path="s3://vendor-bucket/folderL/", file_name="fileI.csv"),
    # folderM (no eligible files)
    # folderN (not in config)
    Row(s3_path="s3://vendor-bucket/folderN/", file_name="fileL.csv"),
    # folderΩ (special/multibyte)
    Row(s3_path="s3://vendor-bucket/folderΩ/", file_name="spécial_文件.csv"),
]

vendor_s3_files_schema = StructType([
    StructField("s3_path", StringType(), True),
    StructField("file_name", StringType(), True)
])
vendor_s3_files_df = spark.createDataFrame(vendor_s3_files, schema=vendor_s3_files_schema)
vendor_s3_files_df.write.mode("overwrite").format("delta").saveAsTable("purgo_playground.test_vendor_s3_files")

# Purgo S3 folder simulation
purgo_s3_files = [
    # folderA: data2.csv exists
    Row(s3_path="s3://purgo-bucket/landingA/", file_name="data2.csv"),
    # folderG: fileB.csv exists
    Row(s3_path="s3://purgo-bucket/landingG/", file_name="fileB.csv"),
    # folderH: empty
    # folderJ: empty
    # folderE: empty
    # folderF: empty
    # folderK: empty
    # folderL: empty
    # folderN: empty
]

purgo_s3_files_schema = StructType([
    StructField("s3_path", StringType(), True),
    StructField("file_name", StringType(), True)
])
purgo_s3_files_df = spark.createDataFrame(purgo_s3_files, schema=purgo_s3_files_schema)
purgo_s3_files_df.write.mode("overwrite").format("delta").saveAsTable("purgo_playground.test_purgo_s3_files")

# Archive S3 folder simulation
archive_s3_files = [
    # folderC: data4.csv exists
    Row(s3_path="s3://purgo-bucket/archiveC/", file_name="data4.csv"),
    # folderH: fileC.csv exists
    Row(s3_path="s3://purgo-bucket/archiveH/", file_name="fileC.csv"),
]

archive_s3_files_schema = StructType([
    StructField("s3_path", StringType(), True),
    StructField("file_name", StringType(), True)
])
archive_s3_files_df = spark.createDataFrame(archive_s3_files, schema=archive_s3_files_schema)
archive_s3_files_df.write.mode("overwrite").format("delta").saveAsTable("purgo_playground.test_archive_s3_files")

# -------------------------------
# 4. Simulated Databricks Secret Scope for AWS Keys (for error case)
# -------------------------------

# This is a placeholder for secret scope simulation; in real tests, secret access is handled by Databricks.
# For error case, you can simulate missing keys by not setting them in the test environment.

# -------------------------------
# 5. Edge/NULL/Special Character Handling
# -------------------------------

# Already included in above data: NULLs, special/multibyte chars, edge cases, boundary values.

# -------------------------------
# 6. Validation Query CTE Example (for test validation)
# -------------------------------

# Example: Validate that all files with file_status='SUCCESS' in s3_file_process_log exist in test_purgo_s3_files

from pyspark.sql import functions as F  

validation_cte = """
WITH successful_files AS (
  SELECT file_name, s3_landing_path
  FROM purgo_playground.s3_file_process_log
  WHERE file_status = 'SUCCESS'
)
SELECT sf.file_name, sf.s3_landing_path, pf.file_name AS exists_in_purgo
FROM successful_files sf
LEFT JOIN purgo_playground.test_purgo_s3_files pf
  ON sf.file_name = pf.file_name AND sf.s3_landing_path = pf.s3_path
"""

validation_df = spark.sql(validation_cte)
validation_df.show(truncate=False)
