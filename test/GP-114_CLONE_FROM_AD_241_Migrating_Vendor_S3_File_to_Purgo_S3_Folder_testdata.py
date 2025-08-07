spark.catalog.setCurrentCatalog("purgo_databricks")

# Test Data Generation for purgo_playground.ingest_config_master
# PySpark code for Databricks

# from pyspark.sql import SparkSession  # SparkSession is already available in Databricks
from pyspark.sql.types import (  
    StructType, StructField, StringType, IntegerType, LongType, TimestampType
)
from pyspark.sql import Row  
from datetime import datetime  

# Test data covers: happy path, edge, error, null, special/multibyte chars

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
    # Happy path: active config, valid S3 URIs, non-null, non-empty, recursive
    Row(config_id="1", source_object_name="objA", source_system="sysA", file_name="fileA.csv", frequency="daily", location="locA", domain="domA", sub_domain="subA",
        s3_vendor_path="s3://vendor-bucket/folder1/", source_path=None, s3_landing_path="s3://purgo-bucket/landing1/", s3_archive_path="s3://purgo-bucket/archive1/",
        delta_load_ts="2024-03-21T00:00:00.000+0000", full_or_incremental_load="F", zip_file="N", vendor="VendorA", delimiter=",", source_landing="Y",
        src_landing_table_name="tableA", publish_unstitched="N", publish_unstitched_table_name=None, publish_stitched="Y", publish_stitched_table_name="stitchedA",
        primary_key="id", header="Y", date_pattern="yyyy-MM-dd", actual_file_name="fileA.csv", vendor_file_deletion_flag="N", file_recursive_flag="Y",
        total_weeks_req_data="4", total_weeks_file_data="4", active_flag="A"),
    # Happy path: active config, non-recursive
    Row(config_id="2", source_object_name="objB", source_system="sysB", file_name="fileB.csv", frequency="weekly", location="locB", domain="domB", sub_domain="subB",
        s3_vendor_path="s3://vendor-bucket/folder2/", source_path=None, s3_landing_path="s3://purgo-bucket/landing2/", s3_archive_path="s3://purgo-bucket/archive2/",
        delta_load_ts="2024-03-22T00:00:00.000+0000", full_or_incremental_load="I", zip_file="Y", vendor="VendorB", delimiter="|", source_landing="N",
        src_landing_table_name="tableB", publish_unstitched="Y", publish_unstitched_table_name="unstitchB", publish_stitched="N", publish_stitched_table_name=None,
        primary_key="pk", header="N", date_pattern="MM/dd/yyyy", actual_file_name="fileB.csv", vendor_file_deletion_flag="Y", file_recursive_flag=None,
        total_weeks_req_data="2", total_weeks_file_data="2", active_flag="A"),
    # Edge: inactive config, should not be processed
    Row(config_id="3", source_object_name="objC", source_system="sysC", file_name="fileC.csv", frequency="monthly", location="locC", domain="domC", sub_domain="subC",
        s3_vendor_path="s3://vendor-bucket/folder3/", source_path=None, s3_landing_path="s3://purgo-bucket/landing3/", s3_archive_path="s3://purgo-bucket/archive3/",
        delta_load_ts="2024-03-23T00:00:00.000+0000", full_or_incremental_load="F", zip_file="N", vendor="VendorC", delimiter=";", source_landing="Y",
        src_landing_table_name="tableC", publish_unstitched="N", publish_unstitched_table_name=None, publish_stitched="Y", publish_stitched_table_name="stitchedC",
        primary_key="id", header="Y", date_pattern="dd-MM-yyyy", actual_file_name="fileC.csv", vendor_file_deletion_flag="N", file_recursive_flag="N",
        total_weeks_req_data="1", total_weeks_file_data="1", active_flag="I"),
    # Edge: active_flag lower-case, should not be processed (case-sensitive)
    Row(config_id="4", source_object_name="objD", source_system="sysD", file_name="fileD.csv", frequency="hourly", location="locD", domain="domD", sub_domain="subD",
        s3_vendor_path="s3://vendor-bucket/folder4/", source_path=None, s3_landing_path="s3://purgo-bucket/landing4/", s3_archive_path="s3://purgo-bucket/archive4/",
        delta_load_ts="2024-03-24T00:00:00.000+0000", full_or_incremental_load="I", zip_file="Y", vendor="VendorD", delimiter="\t", source_landing="N",
        src_landing_table_name="tableD", publish_unstitched="Y", publish_unstitched_table_name="unstitchD", publish_stitched="N", publish_stitched_table_name=None,
        primary_key="pk", header="N", date_pattern="yyyy/MM/dd", actual_file_name="fileD.csv", vendor_file_deletion_flag="Y", file_recursive_flag="Y",
        total_weeks_req_data="3", total_weeks_file_data="3", active_flag="a"),
    # Error: missing S3 path (null)
    Row(config_id="5", source_object_name="objE", source_system="sysE", file_name="fileE.csv", frequency="daily", location="locE", domain="domE", sub_domain="subE",
        s3_vendor_path=None, source_path=None, s3_landing_path="s3://purgo-bucket/landing5/", s3_archive_path="s3://purgo-bucket/archive5/",
        delta_load_ts="2024-03-25T00:00:00.000+0000", full_or_incremental_load="F", zip_file="N", vendor="VendorE", delimiter=",", source_landing="Y",
        src_landing_table_name="tableE", publish_unstitched="N", publish_unstitched_table_name=None, publish_stitched="Y", publish_stitched_table_name="stitchedE",
        primary_key="id", header="Y", date_pattern="yyyy-MM-dd", actual_file_name="fileE.csv", vendor_file_deletion_flag="N", file_recursive_flag="N",
        total_weeks_req_data="4", total_weeks_file_data="4", active_flag="A"),
    # Error: missing S3 path (empty string)
    Row(config_id="6", source_object_name="objF", source_system="sysF", file_name="fileF.csv", frequency="weekly", location="locF", domain="domF", sub_domain="subF",
        s3_vendor_path="", source_path=None, s3_landing_path="s3://purgo-bucket/landing6/", s3_archive_path="s3://purgo-bucket/archive6/",
        delta_load_ts="2024-03-26T00:00:00.000+0000", full_or_incremental_load="I", zip_file="Y", vendor="VendorF", delimiter="|", source_landing="N",
        src_landing_table_name="tableF", publish_unstitched="Y", publish_unstitched_table_name="unstitchF", publish_stitched="N", publish_stitched_table_name=None,
        primary_key="pk", header="N", date_pattern="MM/dd/yyyy", actual_file_name="fileF.csv", vendor_file_deletion_flag="Y", file_recursive_flag=None,
        total_weeks_req_data="2", total_weeks_file_data="2", active_flag="A"),
    # Error: invalid S3 URI
    Row(config_id="7", source_object_name="objG", source_system="sysG", file_name="fileG.csv", frequency="monthly", location="locG", domain="domG", sub_domain="subG",
        s3_vendor_path="invalid_path", source_path=None, s3_landing_path="s3://purgo-bucket/landing7/", s3_archive_path="s3://purgo-bucket/archive7/",
        delta_load_ts="2024-03-27T00:00:00.000+0000", full_or_incremental_load="F", zip_file="N", vendor="VendorG", delimiter=";", source_landing="Y",
        src_landing_table_name="tableG", publish_unstitched="N", publish_unstitched_table_name=None, publish_stitched="Y", publish_stitched_table_name="stitchedG",
        primary_key="id", header="Y", date_pattern="dd-MM-yyyy", actual_file_name="fileG.csv", vendor_file_deletion_flag="N", file_recursive_flag="N",
        total_weeks_req_data="1", total_weeks_file_data="1", active_flag="A"),
    # Happy path: recursive flag null, should not process subfolders
    Row(config_id="8", source_object_name="objH", source_system="sysH", file_name="fileH.csv", frequency="hourly", location="locH", domain="domH", sub_domain="subH",
        s3_vendor_path="s3://vendor-bucket/folder8/", source_path=None, s3_landing_path="s3://purgo-bucket/landing8/", s3_archive_path="s3://purgo-bucket/archive8/",
        delta_load_ts="2024-03-28T00:00:00.000+0000", full_or_incremental_load="I", zip_file="Y", vendor="VendorH", delimiter="\t", source_landing="N",
        src_landing_table_name="tableH", publish_unstitched="Y", publish_unstitched_table_name="unstitchH", publish_stitched="N", publish_stitched_table_name=None,
        primary_key="pk", header="N", date_pattern="yyyy/MM/dd", actual_file_name="fileH.csv", vendor_file_deletion_flag="Y", file_recursive_flag=None,
        total_weeks_req_data="3", total_weeks_file_data="3", active_flag="A"),
    # Happy path: special/multibyte chars in file_name and S3 path
    Row(config_id="9", source_object_name="objI", source_system="sysI", file_name="ファイルI.csv", frequency="daily", location="locI", domain="domI", sub_domain="subI",
        s3_vendor_path="s3://vendor-bucket/特殊フォルダ/", source_path=None, s3_landing_path="s3://purgo-bucket/ランディング9/", s3_archive_path="s3://purgo-bucket/アーカイブ9/",
        delta_load_ts="2024-03-29T00:00:00.000+0000", full_or_incremental_load="F", zip_file="N", vendor="VendorI", delimiter=",", source_landing="Y",
        src_landing_table_name="tableI", publish_unstitched="N", publish_unstitched_table_name=None, publish_stitched="Y", publish_stitched_table_name="stitchedI",
        primary_key="id", header="Y", date_pattern="yyyy-MM-dd", actual_file_name="ファイルI.csv", vendor_file_deletion_flag="N", file_recursive_flag="Y",
        total_weeks_req_data="4", total_weeks_file_data="4", active_flag="A"),
    # Happy path: special chars in file_name
    Row(config_id="10", source_object_name="objJ", source_system="sysJ", file_name="file!@#$.csv", frequency="weekly", location="locJ", domain="domJ", sub_domain="subJ",
        s3_vendor_path="s3://vendor-bucket/folder10/", source_path=None, s3_landing_path="s3://purgo-bucket/landing10/", s3_archive_path="s3://purgo-bucket/archive10/",
        delta_load_ts="2024-03-30T00:00:00.000+0000", full_or_incremental_load="I", zip_file="Y", vendor="VendorJ", delimiter="|", source_landing="N",
        src_landing_table_name="tableJ", publish_unstitched="Y", publish_unstitched_table_name="unstitchJ", publish_stitched="N", publish_stitched_table_name=None,
        primary_key="pk", header="N", date_pattern="MM/dd/yyyy", actual_file_name="file!@#$.csv", vendor_file_deletion_flag="Y", file_recursive_flag=None,
        total_weeks_req_data="2", total_weeks_file_data="2", active_flag="A"),
    # Edge: NULLs in non-key columns
    Row(config_id="11", source_object_name=None, source_system=None, file_name=None, frequency=None, location=None, domain=None, sub_domain=None,
        s3_vendor_path="s3://vendor-bucket/folder11/", source_path=None, s3_landing_path="s3://purgo-bucket/landing11/", s3_archive_path="s3://purgo-bucket/archive11/",
        delta_load_ts=None, full_or_incremental_load=None, zip_file=None, vendor=None, delimiter=None, source_landing=None,
        src_landing_table_name=None, publish_unstitched=None, publish_unstitched_table_name=None, publish_stitched=None, publish_stitched_table_name=None,
        primary_key=None, header=None, date_pattern=None, actual_file_name=None, vendor_file_deletion_flag=None, file_recursive_flag=None,
        total_weeks_req_data=None, total_weeks_file_data=None, active_flag="A"),
    # Edge: all S3 paths null
    Row(config_id="12", source_object_name="objL", source_system="sysL", file_name="fileL.csv", frequency="daily", location="locL", domain="domL", sub_domain="subL",
        s3_vendor_path=None, source_path=None, s3_landing_path=None, s3_archive_path=None,
        delta_load_ts="2024-03-31T00:00:00.000+0000", full_or_incremental_load="F", zip_file="N", vendor="VendorL", delimiter=",", source_landing="Y",
        src_landing_table_name="tableL", publish_unstitched="N", publish_unstitched_table_name=None, publish_stitched="Y", publish_stitched_table_name="stitchedL",
        primary_key="id", header="Y", date_pattern="yyyy-MM-dd", actual_file_name="fileL.csv", vendor_file_deletion_flag="N", file_recursive_flag="N",
        total_weeks_req_data="4", total_weeks_file_data="4", active_flag="A"),
    # Edge: all S3 paths empty string
    Row(config_id="13", source_object_name="objM", source_system="sysM", file_name="fileM.csv", frequency="weekly", location="locM", domain="domM", sub_domain="subM",
        s3_vendor_path="", source_path=None, s3_landing_path="", s3_archive_path="",
        delta_load_ts="2024-04-01T00:00:00.000+0000", full_or_incremental_load="I", zip_file="Y", vendor="VendorM", delimiter="|", source_landing="N",
        src_landing_table_name="tableM", publish_unstitched="Y", publish_unstitched_table_name="unstitchM", publish_stitched="N", publish_stitched_table_name=None,
        primary_key="pk", header="N", date_pattern="MM/dd/yyyy", actual_file_name="fileM.csv", vendor_file_deletion_flag="Y", file_recursive_flag=None,
        total_weeks_req_data="2", total_weeks_file_data="2", active_flag="A"),
    # Edge: active_flag NULL
    Row(config_id="14", source_object_name="objN", source_system="sysN", file_name="fileN.csv", frequency="monthly", location="locN", domain="domN", sub_domain="subN",
        s3_vendor_path="s3://vendor-bucket/folder14/", source_path=None, s3_landing_path="s3://purgo-bucket/landing14/", s3_archive_path="s3://purgo-bucket/archive14/",
        delta_load_ts="2024-04-02T00:00:00.000+0000", full_or_incremental_load="F", zip_file="N", vendor="VendorN", delimiter=";", source_landing="Y",
        src_landing_table_name="tableN", publish_unstitched="N", publish_unstitched_table_name=None, publish_stitched="Y", publish_stitched_table_name="stitchedN",
        primary_key="id", header="Y", date_pattern="dd-MM-yyyy", actual_file_name="fileN.csv", vendor_file_deletion_flag="N", file_recursive_flag="N",
        total_weeks_req_data="1", total_weeks_file_data="1", active_flag=None),
    # Edge: active_flag empty string
    Row(config_id="15", source_object_name="objO", source_system="sysO", file_name="fileO.csv", frequency="hourly", location="locO", domain="domO", sub_domain="subO",
        s3_vendor_path="s3://vendor-bucket/folder15/", source_path=None, s3_landing_path="s3://purgo-bucket/landing15/", s3_archive_path="s3://purgo-bucket/archive15/",
        delta_load_ts="2024-04-03T00:00:00.000+0000", full_or_incremental_load="I", zip_file="Y", vendor="VendorO", delimiter="\t", source_landing="N",
        src_landing_table_name="tableO", publish_unstitched="Y", publish_unstitched_table_name="unstitchO", publish_stitched="N", publish_stitched_table_name=None,
        primary_key="pk", header="N", date_pattern="yyyy/MM/dd", actual_file_name="fileO.csv", vendor_file_deletion_flag="Y", file_recursive_flag="Y",
        total_weeks_req_data="3", total_weeks_file_data="3", active_flag=""),
    # Happy path: long S3 path, edge file name
    Row(config_id="16", source_object_name="objP", source_system="sysP", file_name="fileP.csv", frequency="daily", location="locP", domain="domP", sub_domain="subP",
        s3_vendor_path="s3://vendor-bucket/very/long/path/with/many/levels/", source_path=None, s3_landing_path="s3://purgo-bucket/landing16/", s3_archive_path="s3://purgo-bucket/archive16/",
        delta_load_ts="2024-04-04T00:00:00.000+0000", full_or_incremental_load="F", zip_file="N", vendor="VendorP", delimiter=",", source_landing="Y",
        src_landing_table_name="tableP", publish_unstitched="N", publish_unstitched_table_name=None, publish_stitched="Y", publish_stitched_table_name="stitchedP",
        primary_key="id", header="Y", date_pattern="yyyy-MM-dd", actual_file_name="fileP.csv", vendor_file_deletion_flag="N", file_recursive_flag="Y",
        total_weeks_req_data="4", total_weeks_file_data="4", active_flag="A"),
    # Happy path: file_recursive_flag = "N"
    Row(config_id="17", source_object_name="objQ", source_system="sysQ", file_name="fileQ.csv", frequency="weekly", location="locQ", domain="domQ", sub_domain="subQ",
        s3_vendor_path="s3://vendor-bucket/folder17/", source_path=None, s3_landing_path="s3://purgo-bucket/landing17/", s3_archive_path="s3://purgo-bucket/archive17/",
        delta_load_ts="2024-04-05T00:00:00.000+0000", full_or_incremental_load="I", zip_file="Y", vendor="VendorQ", delimiter="|", source_landing="N",
        src_landing_table_name="tableQ", publish_unstitched="Y", publish_unstitched_table_name="unstitchQ", publish_stitched="N", publish_stitched_table_name=None,
        primary_key="pk", header="N", date_pattern="MM/dd/yyyy", actual_file_name="fileQ.csv", vendor_file_deletion_flag="Y", file_recursive_flag="N",
        total_weeks_req_data="2", total_weeks_file_data="2", active_flag="A"),
    # Happy path: file_recursive_flag = "Y"
    Row(config_id="18", source_object_name="objR", source_system="sysR", file_name="fileR.csv", frequency="monthly", location="locR", domain="domR", sub_domain="subR",
        s3_vendor_path="s3://vendor-bucket/folder18/", source_path=None, s3_landing_path="s3://purgo-bucket/landing18/", s3_archive_path="s3://purgo-bucket/archive18/",
        delta_load_ts="2024-04-06T00:00:00.000+0000", full_or_incremental_load="F", zip_file="N", vendor="VendorR", delimiter=";", source_landing="Y",
        src_landing_table_name="tableR", publish_unstitched="N", publish_unstitched_table_name=None, publish_stitched="Y", publish_stitched_table_name="stitchedR",
        primary_key="id", header="Y", date_pattern="dd-MM-yyyy", actual_file_name="fileR.csv", vendor_file_deletion_flag="N", file_recursive_flag="Y",
        total_weeks_req_data="1", total_weeks_file_data="1", active_flag="A"),
    # Happy path: file_recursive_flag = None
    Row(config_id="19", source_object_name="objS", source_system="sysS", file_name="fileS.csv", frequency="hourly", location="locS", domain="domS", sub_domain="subS",
        s3_vendor_path="s3://vendor-bucket/folder19/", source_path=None, s3_landing_path="s3://purgo-bucket/landing19/", s3_archive_path="s3://purgo-bucket/archive19/",
        delta_load_ts="2024-04-07T00:00:00.000+0000", full_or_incremental_load="I", zip_file="Y", vendor="VendorS", delimiter="\t", source_landing="N",
        src_landing_table_name="tableS", publish_unstitched="Y", publish_unstitched_table_name="unstitchS", publish_stitched="N", publish_stitched_table_name=None,
        primary_key="pk", header="N", date_pattern="yyyy/MM/dd", actual_file_name="fileS.csv", vendor_file_deletion_flag="Y", file_recursive_flag=None,
        total_weeks_req_data="3", total_weeks_file_data="3", active_flag="A"),
    # Happy path: file_name with whitespace and special chars
    Row(config_id="20", source_object_name="objT", source_system="sysT", file_name="file T 2024-04-08.csv", frequency="daily", location="locT", domain="domT", sub_domain="subT",
        s3_vendor_path="s3://vendor-bucket/folder20/", source_path=None, s3_landing_path="s3://purgo-bucket/landing20/", s3_archive_path="s3://purgo-bucket/archive20/",
        delta_load_ts="2024-04-08T00:00:00.000+0000", full_or_incremental_load="F", zip_file="N", vendor="VendorT", delimiter=",", source_landing="Y",
        src_landing_table_name="tableT", publish_unstitched="N", publish_unstitched_table_name=None, publish_stitched="Y", publish_stitched_table_name="stitchedT",
        primary_key="id", header="Y", date_pattern="yyyy-MM-dd", actual_file_name="file T 2024-04-08.csv", vendor_file_deletion_flag="N", file_recursive_flag="Y",
        total_weeks_req_data="4", total_weeks_file_data="4", active_flag="A"),
    # Edge: file_name is null
    Row(config_id="21", source_object_name="objU", source_system="sysU", file_name=None, frequency="daily", location="locU", domain="domU", sub_domain="subU",
        s3_vendor_path="s3://vendor-bucket/folder21/", source_path=None, s3_landing_path="s3://purgo-bucket/landing21/", s3_archive_path="s3://purgo-bucket/archive21/",
        delta_load_ts="2024-04-09T00:00:00.000+0000", full_or_incremental_load="F", zip_file="N", vendor="VendorU", delimiter=",", source_landing="Y",
        src_landing_table_name="tableU", publish_unstitched="N", publish_unstitched_table_name=None, publish_stitched="Y", publish_stitched_table_name="stitchedU",
        primary_key="id", header="Y", date_pattern="yyyy-MM-dd", actual_file_name=None, vendor_file_deletion_flag="N", file_recursive_flag="Y",
        total_weeks_req_data="4", total_weeks_file_data="4", active_flag="A"),
    # Edge: file_name is empty string
    Row(config_id="22", source_object_name="objV", source_system="sysV", file_name="", frequency="daily", location="locV", domain="domV", sub_domain="subV",
        s3_vendor_path="s3://vendor-bucket/folder22/", source_path=None, s3_landing_path="s3://purgo-bucket/landing22/", s3_archive_path="s3://purgo-bucket/archive22/",
        delta_load_ts="2024-04-10T00:00:00.000+0000", full_or_incremental_load="F", zip_file="N", vendor="VendorV", delimiter=",", source_landing="Y",
        src_landing_table_name="tableV", publish_unstitched="N", publish_unstitched_table_name=None, publish_stitched="Y", publish_stitched_table_name="stitchedV",
        primary_key="id", header="Y", date_pattern="yyyy-MM-dd", actual_file_name="", vendor_file_deletion_flag="N", file_recursive_flag="Y",
        total_weeks_req_data="4", total_weeks_file_data="4", active_flag="A"),
    # Edge: config_id is null
    Row(config_id=None, source_object_name="objW", source_system="sysW", file_name="fileW.csv", frequency="daily", location="locW", domain="domW", sub_domain="subW",
        s3_vendor_path="s3://vendor-bucket/folder23/", source_path=None, s3_landing_path="s3://purgo-bucket/landing23/", s3_archive_path="s3://purgo-bucket/archive23/",
        delta_load_ts="2024-04-11T00:00:00.000+0000", full_or_incremental_load="F", zip_file="N", vendor="VendorW", delimiter=",", source_landing="Y",
        src_landing_table_name="tableW", publish_unstitched="N", publish_unstitched_table_name=None, publish_stitched="Y", publish_stitched_table_name="stitchedW",
        primary_key="id", header="Y", date_pattern="yyyy-MM-dd", actual_file_name="fileW.csv", vendor_file_deletion_flag="N", file_recursive_flag="Y",
        total_weeks_req_data="4", total_weeks_file_data="4", active_flag="A"),
    # Edge: config_id is empty string
    Row(config_id="", source_object_name="objX", source_system="sysX", file_name="fileX.csv", frequency="daily", location="locX", domain="domX", sub_domain="subX",
        s3_vendor_path="s3://vendor-bucket/folder24/", source_path=None, s3_landing_path="s3://purgo-bucket/landing24/", s3_archive_path="s3://purgo-bucket/archive24/",
        delta_load_ts="2024-04-12T00:00:00.000+0000", full_or_incremental_load="F", zip_file="N", vendor="VendorX", delimiter=",", source_landing="Y",
        src_landing_table_name="tableX", publish_unstitched="N", publish_unstitched_table_name=None, publish_stitched="Y", publish_stitched_table_name="stitchedX",
        primary_key="id", header="Y", date_pattern="yyyy-MM-dd", actual_file_name="fileX.csv", vendor_file_deletion_flag="N", file_recursive_flag="Y",
        total_weeks_req_data="4", total_weeks_file_data="4", active_flag="A"),
    # Edge: file_recursive_flag = "Y", but s3_vendor_path is a subfolder with special chars
    Row(config_id="25", source_object_name="objY", source_system="sysY", file_name="fileY.csv", frequency="daily", location="locY", domain="domY", sub_domain="subY",
        s3_vendor_path="s3://vendor-bucket/子フォルダ25/", source_path=None, s3_landing_path="s3://purgo-bucket/landing25/", s3_archive_path="s3://purgo-bucket/archive25/",
        delta_load_ts="2024-04-13T00:00:00.000+0000", full_or_incremental_load="F", zip_file="N", vendor="VendorY", delimiter=",", source_landing="Y",
        src_landing_table_name="tableY", publish_unstitched="N", publish_unstitched_table_name=None, publish_stitched="Y", publish_stitched_table_name="stitchedY",
        primary_key="id", header="Y", date_pattern="yyyy-MM-dd", actual_file_name="fileY.csv", vendor_file_deletion_flag="N", file_recursive_flag="Y",
        total_weeks_req_data="4", total_weeks_file_data="4", active_flag="A"),
    # Edge: file_recursive_flag = "N", s3_vendor_path with trailing slash
    Row(config_id="26", source_object_name="objZ", source_system="sysZ", file_name="fileZ.csv", frequency="daily", location="locZ", domain="domZ", sub_domain="subZ",
        s3_vendor_path="s3://vendor-bucket/folder26/", source_path=None, s3_landing_path="s3://purgo-bucket/landing26/", s3_archive_path="s3://purgo-bucket/archive26/",
        delta_load_ts="2024-04-14T00:00:00.000+0000", full_or_incremental_load="F", zip_file="N", vendor="VendorZ", delimiter=",", source_landing="Y",
        src_landing_table_name="tableZ", publish_unstitched="N", publish_unstitched_table_name=None, publish_stitched="Y", publish_stitched_table_name="stitchedZ",
        primary_key="id", header="Y", date_pattern="yyyy-MM-dd", actual_file_name="fileZ.csv", vendor_file_deletion_flag="N", file_recursive_flag="N",
        total_weeks_req_data="4", total_weeks_file_data="4", active_flag="A"),
]

ingest_config_master_df = spark.createDataFrame(ingest_config_master_data, schema=ingest_config_master_schema)

# Write to Unity Catalog table for test (commented out, as this is test data generation only)
# ingest_config_master_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("purgo_playground.ingest_config_master")

# Show the generated test data
ingest_config_master_df.show(truncate=False)
