# PySpark test data generation for Databricks environment
# Target tables: purgo_playground.ingest_config_master, purgo_playground.s3_file_process_log

from pyspark.sql import SparkSession  # SparkSession is already available in Databricks
from pyspark.sql.types import StructType, StructField, StringType, TimestampType  
from pyspark.sql import Row  
from pyspark.sql.functions import current_timestamp, lit  

# -------------------------------
# Test Data for ingest_config_master
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
    # Happy path: all S3 paths present, active_flag = 'A'
    Row(config_id="CFG001", source_object_name="obj1", source_system="SYS1", file_name="file1.csv", frequency="DAILY", location="US", domain="FIN", sub_domain="INV", s3_vendor_path="s3://vendor-bucket/folder1", source_path=None, s3_landing_path="s3://purgo-bucket/landing1", s3_archive_path="s3://purgo-bucket/archive1", delta_load_ts="2024-03-21T00:00:00.000+0000", full_or_incremental_load="FULL", zip_file="N", vendor="VendorA", delimiter=",", source_landing="Y", src_landing_table_name="tbl1", publish_unstitched="N", publish_unstitched_table_name=None, publish_stitched="N", publish_stitched_table_name=None, primary_key="id", header="Y", date_pattern="yyyy-MM-dd", actual_file_name=None, vendor_file_deletion_flag="N", file_recursive_flag="N", total_weeks_req_data="4", total_weeks_file_data="4", active_flag="A"),
    Row(config_id="CFG002", source_object_name="obj2", source_system="SYS2", file_name="a.txt", frequency="WEEKLY", location="EU", domain="HR", sub_domain="PAY", s3_vendor_path="s3://vendor-bucket/folder2", source_path=None, s3_landing_path="s3://purgo-bucket/landing2", s3_archive_path="s3://purgo-bucket/archive2", delta_load_ts="2024-03-22T00:00:00.000+0000", full_or_incremental_load="INCR", zip_file="Y", vendor="VendorB", delimiter="|", source_landing="N", src_landing_table_name="tbl2", publish_unstitched="Y", publish_unstitched_table_name="tbl2_unstitched", publish_stitched="N", publish_stitched_table_name=None, primary_key="emp_id", header="N", date_pattern="dd/MM/yyyy", actual_file_name=None, vendor_file_deletion_flag="Y", file_recursive_flag="N", total_weeks_req_data="2", total_weeks_file_data="2", active_flag="A"),
    # Error: file already exists in Purgo S3
    Row(config_id="CFG003", source_object_name="obj3", source_system="SYS3", file_name="file1.csv", frequency="MONTHLY", location="APAC", domain="OPS", sub_domain="SUP", s3_vendor_path="s3://vendor-bucket/folder3", source_path=None, s3_landing_path="s3://purgo-bucket/landing3", s3_archive_path="s3://purgo-bucket/archive3", delta_load_ts="2024-03-23T00:00:00.000+0000", full_or_incremental_load="FULL", zip_file="N", vendor="VendorC", delimiter=";", source_landing="Y", src_landing_table_name="tbl3", publish_unstitched="N", publish_unstitched_table_name=None, publish_stitched="Y", publish_stitched_table_name="tbl3_stitched", primary_key="order_id", header="Y", date_pattern="MM-dd-yyyy", actual_file_name=None, vendor_file_deletion_flag="N", file_recursive_flag="N", total_weeks_req_data="1", total_weeks_file_data="1", active_flag="A"),
    # Error: file already exists in Archive S3
    Row(config_id="CFG004", source_object_name="obj4", source_system="SYS4", file_name="file2.csv", frequency="DAILY", location="US", domain="FIN", sub_domain="INV", s3_vendor_path="s3://vendor-bucket/folder4", source_path=None, s3_landing_path="s3://purgo-bucket/landing4", s3_archive_path="s3://purgo-bucket/archive4", delta_load_ts="2024-03-24T00:00:00.000+0000", full_or_incremental_load="INCR", zip_file="N", vendor="VendorD", delimiter=",", source_landing="Y", src_landing_table_name="tbl4", publish_unstitched="N", publish_unstitched_table_name=None, publish_stitched="N", publish_stitched_table_name=None, primary_key="id", header="Y", date_pattern="yyyy-MM-dd", actual_file_name=None, vendor_file_deletion_flag="N", file_recursive_flag="N", total_weeks_req_data="4", total_weeks_file_data="4", active_flag="A"),
    # Error: S3 path missing in config (vendor_path missing)
    Row(config_id="CFG005", source_object_name="obj5", source_system="SYS5", file_name="file3.csv", frequency="DAILY", location="US", domain="FIN", sub_domain="INV", s3_vendor_path=None, source_path=None, s3_landing_path="s3://purgo-bucket/landing5", s3_archive_path="s3://purgo-bucket/archive5", delta_load_ts="2024-03-25T00:00:00.000+0000", full_or_incremental_load="FULL", zip_file="N", vendor="VendorE", delimiter=",", source_landing="Y", src_landing_table_name="tbl5", publish_unstitched="N", publish_unstitched_table_name=None, publish_stitched="N", publish_stitched_table_name=None, primary_key="id", header="Y", date_pattern="yyyy-MM-dd", actual_file_name=None, vendor_file_deletion_flag="N", file_recursive_flag="N", total_weeks_req_data="4", total_weeks_file_data="4", active_flag="A"),
    # Error: S3 path missing in config (purgo_path missing)
    Row(config_id="CFG006", source_object_name="obj6", source_system="SYS6", file_name="file4.csv", frequency="DAILY", location="US", domain="FIN", sub_domain="INV", s3_vendor_path="s3://vendor-bucket/folder6", source_path=None, s3_landing_path=None, s3_archive_path="s3://purgo-bucket/archive6", delta_load_ts="2024-03-26T00:00:00.000+0000", full_or_incremental_load="FULL", zip_file="N", vendor="VendorF", delimiter=",", source_landing="Y", src_landing_table_name="tbl6", publish_unstitched="N", publish_unstitched_table_name=None, publish_stitched="N", publish_stitched_table_name=None, primary_key="id", header="Y", date_pattern="yyyy-MM-dd", actual_file_name=None, vendor_file_deletion_flag="N", file_recursive_flag="N", total_weeks_req_data="4", total_weeks_file_data="4", active_flag="A"),
    # Error: S3 path missing in config (archive_path missing)
    Row(config_id="CFG007", source_object_name="obj7", source_system="SYS7", file_name="file5.csv", frequency="DAILY", location="US", domain="FIN", sub_domain="INV", s3_vendor_path="s3://vendor-bucket/folder7", source_path=None, s3_landing_path="s3://purgo-bucket/landing7", s3_archive_path=None, delta_load_ts="2024-03-27T00:00:00.000+0000", full_or_incremental_load="FULL", zip_file="N", vendor="VendorG", delimiter=",", source_landing="Y", src_landing_table_name="tbl7", publish_unstitched="N", publish_unstitched_table_name=None, publish_stitched="N", publish_stitched_table_name=None, primary_key="id", header="Y", date_pattern="yyyy-MM-dd", actual_file_name=None, vendor_file_deletion_flag="N", file_recursive_flag="N", total_weeks_req_data="4", total_weeks_file_data="4", active_flag="A"),
    # Error: AWS credentials missing/invalid (simulate by config, not actual secret)
    Row(config_id="CFG008", source_object_name="obj8", source_system="SYS8", file_name="file6.csv", frequency="DAILY", location="US", domain="FIN", sub_domain="INV", s3_vendor_path="s3://vendor-bucket/folder8", source_path=None, s3_landing_path="s3://purgo-bucket/landing8", s3_archive_path="s3://purgo-bucket/archive8", delta_load_ts="2024-03-28T00:00:00.000+0000", full_or_incremental_load="FULL", zip_file="N", vendor="VendorH", delimiter=",", source_landing="Y", src_landing_table_name="tbl8", publish_unstitched="N", publish_unstitched_table_name=None, publish_stitched="N", publish_stitched_table_name="tbl8_stitched", primary_key="id", header="Y", date_pattern="yyyy-MM-dd", actual_file_name=None, vendor_file_deletion_flag="N", file_recursive_flag="N", total_weeks_req_data="4", total_weeks_file_data="4", active_flag="A"),
    # Error: S3 access denied/path not found (simulate by config)
    Row(config_id="CFG009", source_object_name="obj9", source_system="SYS9", file_name="file7.csv", frequency="DAILY", location="US", domain="FIN", sub_domain="INV", s3_vendor_path="s3://vendor-bucket/folder9", source_path=None, s3_landing_path="s3://purgo-bucket/landing9", s3_archive_path="s3://purgo-bucket/archive9", delta_load_ts="2024-03-29T00:00:00.000+0000", full_or_incremental_load="FULL", zip_file="N", vendor="VendorI", delimiter=",", source_landing="Y", src_landing_table_name="tbl9", publish_unstitched="N", publish_unstitched_table_name=None, publish_stitched="N", publish_stitched_table_name=None, primary_key="id", header="Y", date_pattern="yyyy-MM-dd", actual_file_name=None, vendor_file_deletion_flag="N", file_recursive_flag="N", total_weeks_req_data="4", total_weeks_file_data="4", active_flag="A"),
    Row(config_id="CFG010", source_object_name="obj10", source_system="SYS10", file_name="file8.csv", frequency="DAILY", location="US", domain="FIN", sub_domain="INV", s3_vendor_path="s3://vendor-bucket/folder10", source_path=None, s3_landing_path="s3://purgo-bucket/landing10", s3_archive_path="s3://purgo-bucket/archive10", delta_load_ts="2024-03-30T00:00:00.000+0000", full_or_incremental_load="FULL", zip_file="N", vendor="VendorJ", delimiter=",", source_landing="Y", src_landing_table_name="tbl10", publish_unstitched="N", publish_unstitched_table_name=None, publish_stitched="N", publish_stitched_table_name="tbl10_stitched", primary_key="id", header="Y", date_pattern="yyyy-MM-dd", actual_file_name=None, vendor_file_deletion_flag="N", file_recursive_flag="N", total_weeks_req_data="4", total_weeks_file_data="4", active_flag="A"),
    # Validation: only active_flag = 'A' processed
    Row(config_id="CFG011", source_object_name="obj11", source_system="SYS11", file_name="file9.csv", frequency="DAILY", location="US", domain="FIN", sub_domain="INV", s3_vendor_path="s3://vendor-bucket/folder11", source_path=None, s3_landing_path="s3://purgo-bucket/landing11", s3_archive_path="s3://purgo-bucket/archive11", delta_load_ts="2024-03-31T00:00:00.000+0000", full_or_incremental_load="FULL", zip_file="N", vendor="VendorK", delimiter=",", source_landing="Y", src_landing_table_name="tbl11", publish_unstitched="N", publish_unstitched_table_name=None, publish_stitched="N", publish_stitched_table_name=None, primary_key="id", header="Y", date_pattern="yyyy-MM-dd", actual_file_name=None, vendor_file_deletion_flag="N", file_recursive_flag="N", total_weeks_req_data="4", total_weeks_file_data="4", active_flag="I"),
    Row(config_id="CFG012", source_object_name="obj12", source_system="SYS12", file_name="file10.csv", frequency="DAILY", location="US", domain="FIN", sub_domain="INV", s3_vendor_path="s3://vendor-bucket/folder12", source_path=None, s3_landing_path="s3://purgo-bucket/landing12", s3_archive_path="s3://purgo-bucket/archive12", delta_load_ts="2024-04-01T00:00:00.000+0000", full_or_incremental_load="FULL", zip_file="N", vendor="VendorL", delimiter=",", source_landing="Y", src_landing_table_name="tbl12", publish_unstitched="N", publish_unstitched_table_name=None, publish_stitched="N", publish_stitched_table_name=None, primary_key="id", header="Y", date_pattern="yyyy-MM-dd", actual_file_name=None, vendor_file_deletion_flag="N", file_recursive_flag="N", total_weeks_req_data="4", total_weeks_file_data="4", active_flag="D"),
    # Validation: file name case sensitivity
    Row(config_id="CFG013", source_object_name="obj13", source_system="SYS13", file_name="File8.csv", frequency="DAILY", location="US", domain="FIN", sub_domain="INV", s3_vendor_path="s3://vendor-bucket/folder13", source_path=None, s3_landing_path="s3://purgo-bucket/landing13", s3_archive_path="s3://purgo-bucket/archive13", delta_load_ts="2024-04-02T00:00:00.000+0000", full_or_incremental_load="FULL", zip_file="N", vendor="VendorM", delimiter=",", source_landing="Y", src_landing_table_name="tbl13", publish_unstitched="N", publish_unstitched_table_name=None, publish_stitched="N", publish_stitched_table_name=None, primary_key="id", header="Y", date_pattern="yyyy-MM-dd", actual_file_name=None, vendor_file_deletion_flag="N", file_recursive_flag="N", total_weeks_req_data="4", total_weeks_file_data="4", active_flag="A"),
    # Validation: special/multibyte characters
    Row(config_id="CFG014", source_object_name="obj14", source_system="SYS14", file_name="spécial_文件.csv", frequency="DAILY", location="US", domain="FIN", sub_domain="INV", s3_vendor_path="s3://vendor-bucket/folder14", source_path=None, s3_landing_path="s3://purgo-bucket/landing14", s3_archive_path="s3://purgo-bucket/archive14", delta_load_ts="2024-04-03T00:00:00.000+0000", full_or_incremental_load="FULL", zip_file="N", vendor="VendorN", delimiter=",", source_landing="Y", src_landing_table_name="tbl14", publish_unstitched="N", publish_unstitched_table_name=None, publish_stitched="N", publish_stitched_table_name=None, primary_key="id", header="Y", date_pattern="yyyy-MM-dd", actual_file_name=None, vendor_file_deletion_flag="N", file_recursive_flag="N", total_weeks_req_data="4", total_weeks_file_data="4", active_flag="A"),
    # Validation: NULL handling (some nullable fields)
    Row(config_id="CFG015", source_object_name=None, source_system=None, file_name=None, frequency=None, location=None, domain=None, sub_domain=None, s3_vendor_path="s3://vendor-bucket/folder15", source_path=None, s3_landing_path="s3://purgo-bucket/landing15", s3_archive_path="s3://purgo-bucket/archive15", delta_load_ts=None, full_or_incremental_load=None, zip_file=None, vendor=None, delimiter=None, source_landing=None, src_landing_table_name=None, publish_unstitched=None, publish_unstitched_table_name=None, publish_stitched=None, publish_stitched_table_name=None, primary_key=None, header=None, date_pattern=None, actual_file_name=None, vendor_file_deletion_flag=None, file_recursive_flag=None, total_weeks_req_data=None, total_weeks_file_data=None, active_flag="A"),
    # Validation: all file types eligible
    Row(config_id="CFG016", source_object_name="obj16", source_system="SYS16", file_name="data.csv", frequency="DAILY", location="US", domain="FIN", sub_domain="INV", s3_vendor_path="s3://vendor-bucket/folder16", source_path=None, s3_landing_path="s3://purgo-bucket/landing16", s3_archive_path="s3://purgo-bucket/archive16", delta_load_ts="2024-04-04T00:00:00.000+0000", full_or_incremental_load="FULL", zip_file="N", vendor="VendorO", delimiter=",", source_landing="Y", src_landing_table_name="tbl16", publish_unstitched="N", publish_unstitched_table_name=None, publish_stitched="N", publish_stitched_table_name=None, primary_key="id", header="Y", date_pattern="yyyy-MM-dd", actual_file_name=None, vendor_file_deletion_flag="N", file_recursive_flag="N", total_weeks_req_data="4", total_weeks_file_data="4", active_flag="A"),
    Row(config_id="CFG017", source_object_name="obj17", source_system="SYS17", file_name="image.png", frequency="DAILY", location="US", domain="FIN", sub_domain="INV", s3_vendor_path="s3://vendor-bucket/folder16", source_path=None, s3_landing_path="s3://purgo-bucket/landing16", s3_archive_path="s3://purgo-bucket/archive16", delta_load_ts="2024-04-04T00:00:00.000+0000", full_or_incremental_load="FULL", zip_file="N", vendor="VendorO", delimiter=",", source_landing="Y", src_landing_table_name="tbl16", publish_unstitched="N", publish_unstitched_table_name=None, publish_stitched="N", publish_stitched_table_name="tbl16_stitched", primary_key="id", header="Y", date_pattern="yyyy-MM-dd", actual_file_name=None, vendor_file_deletion_flag="N", file_recursive_flag="N", total_weeks_req_data="4", total_weeks_file_data="4", active_flag="A"),
    Row(config_id="CFG018", source_object_name="obj18", source_system="SYS18", file_name="report.pdf", frequency="DAILY", location="US", domain="FIN", sub_domain="INV", s3_vendor_path="s3://vendor-bucket/folder16", source_path=None, s3_landing_path="s3://purgo-bucket/landing16", s3_archive_path="s3://purgo-bucket/archive16", delta_load_ts="2024-04-04T00:00:00.000+0000", full_or_incremental_load="FULL", zip_file="N", vendor="VendorO", delimiter=",", source_landing="Y", src_landing_table_name="tbl16", publish_unstitched="N", publish_unstitched_table_name=None, publish_stitched="N", publish_stitched_table_name=None, primary_key="id", header="Y", date_pattern="yyyy-MM-dd", actual_file_name=None, vendor_file_deletion_flag="N", file_recursive_flag="N", total_weeks_req_data="4", total_weeks_file_data="4", active_flag="A"),
    # Validation: file not processed recursively in subfolders
    Row(config_id="CFG019", source_object_name="obj19", source_system="SYS19", file_name="file5.csv", frequency="DAILY", location="US", domain="FIN", sub_domain="INV", s3_vendor_path="s3://vendor-bucket/folder17", source_path=None, s3_landing_path="s3://purgo-bucket/landing17", s3_archive_path="s3://purgo-bucket/archive17", delta_load_ts="2024-04-05T00:00:00.000+0000", full_or_incremental_load="FULL", zip_file="N", vendor="VendorP", delimiter=",", source_landing="Y", src_landing_table_name="tbl17", publish_unstitched="N", publish_unstitched_table_name=None, publish_stitched="N", publish_stitched_table_name=None, primary_key="id", header="Y", date_pattern="yyyy-MM-dd", actual_file_name=None, vendor_file_deletion_flag="N", file_recursive_flag="N", total_weeks_req_data="4", total_weeks_file_data="4", active_flag="A"),
    # Error: No active configs
    # (No row with active_flag='A' for this scenario, handled by test logic)
    # Error: No files to transfer (simulate by test logic)
    # Error: Unexpected exception during file transfer (simulate by test logic)
]

ingest_config_master_df = spark.createDataFrame(ingest_config_master_data, schema=ingest_config_master_schema)

# -------------------------------
# Test Data for s3_file_process_log
# -------------------------------

s3_file_process_log_schema = StructType([
    StructField("file_name", StringType(), True),
    StructField("s3_vendor_path", StringType(), True),
    StructField("s3_landing_path", StringType(), True),
    StructField("s3_archive_path", StringType(), True),
    StructField("file_status", StringType(), True),
    StructField("file_processed_date", TimestampType(), True)
])

from datetime import datetime  

s3_file_process_log_data = [
    # Happy path: successful transfer
    Row(file_name="file1.csv", s3_vendor_path="s3://vendor-bucket/folder1", s3_landing_path="s3://purgo-bucket/landing1", s3_archive_path="s3://purgo-bucket/archive1", file_status="SUCCESS", file_processed_date=datetime.strptime("2024-04-10T10:00:00.000+0000", "%Y-%m-%dT%H:%M:%S.%f%z")),
    Row(file_name="file2.csv", s3_vendor_path="s3://vendor-bucket/folder1", s3_landing_path="s3://purgo-bucket/landing1", s3_archive_path="s3://purgo-bucket/archive1", file_status="SUCCESS", file_processed_date=datetime.strptime("2024-04-10T10:01:00.000+0000", "%Y-%m-%dT%H:%M:%S.%f%z")),
    # Skipped: already exists in Purgo
    Row(file_name="file1.csv", s3_vendor_path="s3://vendor-bucket/folder3", s3_landing_path="s3://purgo-bucket/landing3", s3_archive_path="s3://purgo-bucket/archive3", file_status="SKIPPED_EXISTS", file_processed_date=datetime.strptime("2024-04-10T10:02:00.000+0000", "%Y-%m-%dT%H:%M:%S.%f%z")),
    # Skipped: already exists in Archive
    Row(file_name="file2.csv", s3_vendor_path="s3://vendor-bucket/folder4", s3_landing_path="s3://purgo-bucket/landing4", s3_archive_path="s3://purgo-bucket/archive4", file_status="SKIPPED_ARCHIVE", file_processed_date=datetime.strptime("2024-04-10T10:03:00.000+0000", "%Y-%m-%dT%H:%M:%S.%f%z")),
    # Error: S3 path missing in config
    Row(file_name=None, s3_vendor_path=None, s3_landing_path="s3://purgo-bucket/landing5", s3_archive_path="s3://purgo-bucket/archive5", file_status="ERROR", file_processed_date=datetime.strptime("2024-04-10T10:04:00.000+0000", "%Y-%m-%dT%H:%M:%S.%f%z")),
    Row(file_name=None, s3_vendor_path="s3://vendor-bucket/folder6", s3_landing_path=None, s3_archive_path="s3://purgo-bucket/archive6", file_status="ERROR", file_processed_date=datetime.strptime("2024-04-10T10:05:00.000+0000", "%Y-%m-%dT%H:%M:%S.%f%z")),
    Row(file_name=None, s3_vendor_path="s3://vendor-bucket/folder7", s3_landing_path="s3://purgo-bucket/landing7", s3_archive_path=None, file_status="ERROR", file_processed_date=datetime.strptime("2024-04-10T10:06:00.000+0000", "%Y-%m-%dT%H:%M:%S.%f%z")),
    # Error: AWS credentials missing/invalid
    Row(file_name=None, s3_vendor_path="s3://vendor-bucket/folder8", s3_landing_path="s3://purgo-bucket/landing8", s3_archive_path="s3://purgo-bucket/archive8", file_status="ERROR", file_processed_date=datetime.strptime("2024-04-10T10:07:00.000+0000", "%Y-%m-%dT%H:%M:%S.%f%z")),
    # Error: S3 access denied/path not found
    Row(file_name=None, s3_vendor_path="s3://vendor-bucket/folder9", s3_landing_path="s3://purgo-bucket/landing9", s3_archive_path="s3://purgo-bucket/archive9", file_status="ERROR", file_processed_date=datetime.strptime("2024-04-10T10:08:00.000+0000", "%Y-%m-%dT%H:%M:%S.%f%z")),
    Row(file_name=None, s3_vendor_path="s3://vendor-bucket/folder10", s3_landing_path="s3://purgo-bucket/landing10", s3_archive_path="s3://purgo-bucket/archive10", file_status="ERROR", file_processed_date=datetime.strptime("2024-04-10T10:09:00.000+0000", "%Y-%m-%dT%H:%M:%S.%f%z")),
    # Validation: only active configs processed (no log for inactive)
    # Validation: file name case sensitivity
    Row(file_name="File8.csv", s3_vendor_path="s3://vendor-bucket/folder13", s3_landing_path="s3://purgo-bucket/landing13", s3_archive_path="s3://purgo-bucket/archive13", file_status="SUCCESS", file_processed_date=datetime.strptime("2024-04-10T10:10:00.000+0000", "%Y-%m-%dT%H:%M:%S.%f%z")),
    # Validation: special/multibyte characters
    Row(file_name="spécial_文件.csv", s3_vendor_path="s3://vendor-bucket/folder14", s3_landing_path="s3://purgo-bucket/landing14", s3_archive_path="s3://purgo-bucket/archive14", file_status="SUCCESS", file_processed_date=datetime.strptime("2024-04-10T10:11:00.000+0000", "%Y-%m-%dT%H:%M:%S.%f%z")),
    # Validation: NULL handling
    Row(file_name=None, s3_vendor_path="s3://vendor-bucket/folder15", s3_landing_path="s3://purgo-bucket/landing15", s3_archive_path="s3://purgo-bucket/archive15", file_status="SUCCESS", file_processed_date=datetime.strptime("2024-04-10T10:12:00.000+0000", "%Y-%m-%dT%H:%M:%S.%f%z")),
    # Validation: all file types eligible
    Row(file_name="data.csv", s3_vendor_path="s3://vendor-bucket/folder16", s3_landing_path="s3://purgo-bucket/landing16", s3_archive_path="s3://purgo-bucket/archive16", file_status="SUCCESS", file_processed_date=datetime.strptime("2024-04-10T10:13:00.000+0000", "%Y-%m-%dT%H:%M:%S.%f%z")),
    Row(file_name="image.png", s3_vendor_path="s3://vendor-bucket/folder16", s3_landing_path="s3://purgo-bucket/landing16", s3_archive_path="s3://purgo-bucket/archive16", file_status="SUCCESS", file_processed_date=datetime.strptime("2024-04-10T10:14:00.000+0000", "%Y-%m-%dT%H:%M:%S.%f%z")),
    Row(file_name="report.pdf", s3_vendor_path="s3://vendor-bucket/folder16", s3_landing_path="s3://purgo-bucket/landing16", s3_archive_path="s3://purgo-bucket/archive16", file_status="SUCCESS", file_processed_date=datetime.strptime("2024-04-10T10:15:00.000+0000", "%Y-%m-%dT%H:%M:%S.%f%z")),
    # Validation: file not processed recursively in subfolders (no log for subdir/file6.csv)
    Row(file_name="file5.csv", s3_vendor_path="s3://vendor-bucket/folder17", s3_landing_path="s3://purgo-bucket/landing17", s3_archive_path="s3://purgo-bucket/archive17", file_status="SUCCESS", file_processed_date=datetime.strptime("2024-04-10T10:16:00.000+0000", "%Y-%m-%dT%H:%M:%S.%f%z")),
    # Error: No active configs
    Row(file_name=None, s3_vendor_path=None, s3_landing_path=None, s3_archive_path=None, file_status="ERROR", file_processed_date=datetime.strptime("2024-04-10T10:17:00.000+0000", "%Y-%m-%dT%H:%M:%S.%f%z")),
    # Error: No files to transfer
    Row(file_name=None, s3_vendor_path="s3://vendor-bucket/folder18", s3_landing_path="s3://purgo-bucket/landing18", s3_archive_path="s3://purgo-bucket/archive18", file_status="SKIPPED_NONE", file_processed_date=datetime.strptime("2024-04-10T10:18:00.000+0000", "%Y-%m-%dT%H:%M:%S.%f%z")),
    # Error: Unexpected exception during file transfer
    Row(file_name="file9.csv", s3_vendor_path="s3://vendor-bucket/folder19", s3_landing_path="s3://purgo-bucket/landing19", s3_archive_path="s3://purgo-bucket/archive19", file_status="ERROR", file_processed_date=datetime.strptime("2024-04-10T10:19:00.000+0000", "%Y-%m-%dT%H:%M:%S.%f%z")),
]

s3_file_process_log_df = spark.createDataFrame(s3_file_process_log_data, schema=s3_file_process_log_schema)

# -------------------------------
# Write test data to Unity Catalog tables
# -------------------------------

# Write ingest_config_master test data
ingest_config_master_df.write.mode("overwrite").format("delta").option("overwriteSchema", "true").saveAsTable("purgo_playground.ingest_config_master")

# Write s3_file_process_log test data
s3_file_process_log_df.write.mode("overwrite").format("delta").option("overwriteSchema", "true").saveAsTable("purgo_playground.s3_file_process_log")
