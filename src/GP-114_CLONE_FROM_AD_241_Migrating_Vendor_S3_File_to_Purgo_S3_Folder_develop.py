spark.catalog.setCurrentCatalog("purgo_databricks")

# ==========================================================================================
# Databricks PySpark Script: Transfer eligible files from Vendor S3 to Purgo S3
# ==========================================================================================
# This script:
#   - Reads S3 paths and file eligibility from purgo_databricks.purgo_playground.ingest_config_master
#   - Uses AWS credentials from Databricks secret scope "aws_keys"
#   - Lists files in Vendor, Purgo, and Archive S3 folders (non-recursive, root only)
#   - Transfers only files that are:
#       * In Vendor S3 folder
#       * Not present in Purgo or Archive S3 folders
#       * Marked as active (active_flag = 'A')
#   - File name matching is case-sensitive and includes extension
#   - Files are copied (not moved) from Vendor to Purgo S3
#   - All outcomes (SUCCESS, SKIPPED, FAILED) are logged in purgo_databricks.purgo_playground.s3_file_process_log
#   - Handles missing/invalid AWS credentials, missing S3 paths, and permission errors
#   - Uses only Databricks native APIs and best practices
#   - All code is Databricks-compatible and production-ready
# ==========================================================================================

# =========================
# Imports and Setup
# =========================

from pyspark.sql import SparkSession  # SparkSession is already available in Databricks
from pyspark.sql import functions as F  
from pyspark.sql.types import StringType, TimestampType  
from datetime import datetime  
import sys  

# =========================
# Section: Helper Functions
# =========================

def get_aws_secret(scope, key):
    """
    Retrieve AWS credentials from Databricks secret scope.
    Raises ValueError if missing or invalid.
    """
    try:
        value = dbutils.secrets.get(scope=scope, key=key)
        if not value:
            raise ValueError(f"Missing or invalid AWS credentials: {key}")
        return value
    except Exception:
        raise ValueError(f"Missing or invalid AWS credentials: {key}")

def validate_s3_path(row, col_name):
    """
    Validate that S3 path is not null or empty.
    Raises ValueError if invalid.
    """
    val = row[col_name]
    if val is None or str(val).strip() == "":
        raise ValueError(f"Missing S3 path in ingest_config_master: {col_name}")

def list_s3_files(s3_path, aws_access_key, aws_secret_key):
    """
    List files in the root of the given S3 path (non-recursive).
    Returns a set of file names (not full paths).
    Raises PermissionError if access is denied.
    """
    try:
        # Remove trailing slash for consistency
        s3_path = s3_path.rstrip("/")
        # Use dbutils.fs.ls for S3 listing (Databricks native)
        # Set AWS credentials for S3 access
        spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", aws_access_key)
        spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", aws_secret_key)
        # Only list root files (non-recursive)
        files = dbutils.fs.ls(s3_path)
        file_names = set()
        for f in files:
            # Only include files (not directories)
            if not f.isDir():
                # Only include files in the root (no '/')
                name = f.name
                if "/" not in name:
                    file_names.add(name)
        return file_names
    except Exception as e:
        if "AccessDenied" in str(e) or "Permission denied" in str(e):
            raise PermissionError(f"Permission denied for S3 path: {s3_path}")
        # If path does not exist, treat as empty
        if "NoSuchBucket" in str(e) or "does not exist" in str(e):
            return set()
        raise

def copy_s3_file(src_path, dest_path, file_name, aws_access_key, aws_secret_key):
    """
    Copy a file from src_path/file_name to dest_path/file_name in S3.
    Raises PermissionError if access is denied.
    """
    try:
        # Remove trailing slash for consistency
        src_path = src_path.rstrip("/")
        dest_path = dest_path.rstrip("/")
        src_file = f"{src_path}/{file_name}"
        dest_file = f"{dest_path}/{file_name}"
        # Set AWS credentials for S3 access
        spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", aws_access_key)
        spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", aws_secret_key)
        dbutils.fs.cp(src_file, dest_file, recurse=False)
        return True
    except Exception as e:
        if "AccessDenied" in str(e) or "Permission denied" in str(e):
            raise PermissionError(f"Permission denied for S3 path: {src_path if 'read' in str(e) else dest_path}")
        raise

def log_file_process(file_name, s3_vendor_path, s3_landing_path, s3_archive_path, file_status):
    """
    Log the file processing outcome to s3_file_process_log table.
    """
    # Ensure all values are strings or None
    row = (
        str(file_name) if file_name is not None else None,
        str(s3_vendor_path) if s3_vendor_path is not None else None,
        str(s3_landing_path) if s3_landing_path is not None else None,
        str(s3_archive_path) if s3_archive_path is not None else None,
        str(file_status),
        datetime.now()
    )
    log_schema = ["file_name", "s3_vendor_path", "s3_landing_path", "s3_archive_path", "file_status", "file_processed_date"]
    log_df = spark.createDataFrame([row], log_schema)
    # Ensure schema matches target table
    log_df = log_df.withColumn("file_processed_date", F.col("file_processed_date").cast(TimestampType()))
    log_df.write.mode("append").format("delta").saveAsTable("purgo_databricks.purgo_playground.s3_file_process_log")

# =========================
# Section: Main Processing Logic
# =========================

def main():
    # Block: Retrieve AWS credentials from Databricks secret scope
    try:
        aws_access_key = get_aws_secret("aws_keys", "access_key")
        aws_secret_key = get_aws_secret("aws_keys", "secret_key")
    except ValueError as ve:
        # Log and exit on missing/invalid credentials
        print(str(ve))
        sys.exit(1)

    # Block: Read eligible configs from ingest_config_master (active_flag = 'A')
    config_df = (
        spark.table("purgo_databricks.purgo_playground.ingest_config_master")
        .filter(F.col("active_flag") == "A")
        .select(
            "file_name",
            "s3_vendor_path",
            "s3_landing_path",
            "s3_archive_path"
        )
    )

    # Block: Iterate over eligible configs
    for row in config_df.collect():
        file_name = row["file_name"]
        s3_vendor_path = row["s3_vendor_path"]
        s3_landing_path = row["s3_landing_path"]
        s3_archive_path = row["s3_archive_path"]

        # Validate S3 paths and file_name
        try:
            validate_s3_path(row, "s3_vendor_path")
            validate_s3_path(row, "s3_landing_path")
            validate_s3_path(row, "s3_archive_path")
            if file_name is None or str(file_name).strip() == "":
                raise ValueError("Missing or invalid file_name in ingest_config_master")
        except ValueError as ve:
            log_file_process(file_name, s3_vendor_path, s3_landing_path, s3_archive_path, "FAILED")
            print(str(ve))
            continue

        # Block: List files in Vendor, Purgo, and Archive S3 folders (non-recursive)
        try:
            vendor_files = list_s3_files(s3_vendor_path, aws_access_key, aws_secret_key)
            purgo_files = list_s3_files(s3_landing_path, aws_access_key, aws_secret_key)
            archive_files = list_s3_files(s3_archive_path, aws_access_key, aws_secret_key)
        except PermissionError as pe:
            log_file_process(file_name, s3_vendor_path, s3_landing_path, s3_archive_path, "FAILED")
            print(str(pe))
            continue
        except Exception as e:
            log_file_process(file_name, s3_vendor_path, s3_landing_path, s3_archive_path, "FAILED")
            print(f"Unexpected error listing S3 files: {str(e)}")
            continue

        # Block: Check eligibility for transfer
        if file_name not in vendor_files:
            # File not present in Vendor S3, skip
            log_file_process(file_name, s3_vendor_path, s3_landing_path, s3_archive_path, "SKIPPED")
            continue
        if file_name in purgo_files or file_name in archive_files:
            # File already present in Purgo or Archive, skip
            log_file_process(file_name, s3_vendor_path, s3_landing_path, s3_archive_path, "SKIPPED")
            continue

        # Block: Copy file from Vendor to Purgo S3
        try:
            copy_s3_file(s3_vendor_path, s3_landing_path, file_name, aws_access_key, aws_secret_key)
            log_file_process(file_name, s3_vendor_path, s3_landing_path, s3_archive_path, "SUCCESS")
        except PermissionError as pe:
            log_file_process(file_name, s3_vendor_path, s3_landing_path, s3_archive_path, "FAILED")
            print(str(pe))
            continue
        except Exception as e:
            log_file_process(file_name, s3_vendor_path, s3_landing_path, s3_archive_path, "FAILED")
            print(f"File transfer failed: {str(e)}")
            continue

# =========================
# Section: Script Entry Point
# =========================

if __name__ == "__main__":
    main()
# ==========================================================================================
# End of script
# ==========================================================================================
