%pip install boto3
%pip install botocore

spark.catalog.setCurrentCatalog("purgo_databricks")

# ---------------------------------------------------------------------------
# Databricks PySpark Script: Transfer Eligible Files from Vendor S3 to Purgo S3
# ---------------------------------------------------------------------------
# Catalog: purgo_databricks
# Schema: purgo_playground
# Configuration Table: purgo_playground.ingest_config_master
# Log Table: purgo_playground.s3_file_process_log
# AWS Credentials: Databricks secret scope "aws_keys" (keys: "access_key", "secret_key")
# File eligibility: active_flag = "A", file_name case-sensitive, top-level only, skip if exists in Purgo/Archive
# File transfer: COPY (not move), skip if exists in Purgo, log all attempts/results
# No recursion into subfolders
# ---------------------------------------------------------------------------

# from pyspark.sql import SparkSession  # SparkSession is already available in Databricks
from pyspark.sql import functions as F  
from pyspark.sql.types import StringType, TimestampType  
from datetime import datetime  
import boto3  
from botocore.exceptions import ClientError  

# ---------------------------------------------------------------------------
# SECTION: Helper Functions
# ---------------------------------------------------------------------------

def get_aws_credentials():
    """
    Retrieve AWS credentials from Databricks secret scope.
    Raises Exception if not found.
    """
    try:
        access_key = dbutils.secrets.get(scope="aws_keys", key="access_key")
        secret_key = dbutils.secrets.get(scope="aws_keys", key="secret_key")
        if not access_key or not secret_key:
            raise Exception("AWS credentials not found in Databricks secret scope 'aws_keys'")
        return access_key, secret_key
    except Exception as e:
        raise Exception("AWS credentials not found in Databricks secret scope 'aws_keys'") from e

def parse_s3_path(s3_path):
    """
    Parse S3 URI into bucket and prefix.
    """
    if not s3_path or not s3_path.startswith("s3://"):
        return None, None
    path = s3_path.replace("s3://", "")
    parts = path.split("/", 1)
    bucket = parts[0]
    prefix = parts[1] if len(parts) > 1 else ""
    # Ensure prefix ends with "/" if not empty
    if prefix and not prefix.endswith("/"):
        prefix += "/"
    return bucket, prefix

def list_s3_files(s3_client, bucket, prefix):
    """
    List top-level files (no recursion) in the given S3 bucket/prefix.
    Returns a set of file names (not including subfolder files).
    """
    files = set()
    try:
        paginator = s3_client.get_paginator('list_objects_v2')
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix, Delimiter="/"):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                # Only include files directly under the prefix (no subfolders)
                rel_path = key[len(prefix):] if key.startswith(prefix) else key
                if rel_path and "/" not in rel_path:
                    files.add(rel_path)
    except ClientError as e:
        if e.response['Error']['Code'] == 'AccessDenied':
            raise Exception(f"S3 access denied for s3://{bucket}/{prefix}")
        elif e.response['Error']['Code'] == 'NoSuchBucket':
            # Treat as empty folder
            return set()
        else:
            raise
    return files

def copy_s3_file(s3_client, src_bucket, src_key, dest_bucket, dest_key):
    """
    Copy file from src_bucket/src_key to dest_bucket/dest_key.
    Returns True if successful, False otherwise.
    """
    try:
        s3_client.copy(
            {"Bucket": src_bucket, "Key": src_key},
            dest_bucket,
            dest_key
        )
        return True
    except ClientError as e:
        if e.response['Error']['Code'] == 'AccessDenied':
            raise Exception(f"S3 access denied for s3://{src_bucket}/{src_key}")
        else:
            raise

def log_file_process(spark, log_rows):
    """
    Insert log rows into purgo_playground.s3_file_process_log.
    log_rows: list of dicts with keys: file_name, s3_vendor_path, s3_landing_path, s3_archive_path, file_status, file_processed_date
    """
    if not log_rows:
        return
    log_schema = ["file_name", "s3_vendor_path", "s3_landing_path", "s3_archive_path", "file_status", "file_processed_date"]
    log_df = spark.createDataFrame([tuple(row.get(col) for col in log_schema) for row in log_rows], log_schema)
    # Ensure types
    log_df = (
        log_df
        .withColumn("file_name", F.col("file_name").cast(StringType()))
        .withColumn("s3_vendor_path", F.col("s3_vendor_path").cast(StringType()))
        .withColumn("s3_landing_path", F.col("s3_landing_path").cast(StringType()))
        .withColumn("s3_archive_path", F.col("s3_archive_path").cast(StringType()))
        .withColumn("file_status", F.col("file_status").cast(StringType()))
        .withColumn("file_processed_date", F.col("file_processed_date").cast(TimestampType()))
    )
    log_df.write.mode("append").format("delta").saveAsTable("purgo_playground.s3_file_process_log")

# ---------------------------------------------------------------------------
# SECTION: Main Logic
# ---------------------------------------------------------------------------

def main():
    # Set current catalog/schema
    spark.sql('USE CATALOG purgo_databricks')
    spark.sql('USE purgo_playground')

    # Get AWS credentials
    try:
        access_key, secret_key = get_aws_credentials()
    except Exception as e:
        # Log error for all files (no files processed)
        # No files to log since config not loaded, so just raise
        raise

    # Create S3 client
    s3_client = boto3.client(
        "s3",
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key
    )

    # Read config table
    config_df = (
        spark.table("purgo_playground.ingest_config_master")
        .select(
            "config_id", "file_name", "s3_vendor_path", "s3_landing_path", "s3_archive_path", "active_flag"
        )
    )

    # Build config dict: (s3_vendor_path, file_name) -> config_row
    config_rows = config_df.collect()
    config_map = {}
    for row in config_rows:
        key = (row.s3_vendor_path, row.file_name)
        config_map[key] = {
            "config_id": row.config_id,
            "file_name": row.file_name,
            "s3_vendor_path": row.s3_vendor_path,
            "s3_landing_path": row.s3_landing_path,
            "s3_archive_path": row.s3_archive_path,
            "active_flag": row.active_flag
        }

    # Group configs by s3_vendor_path for efficient S3 listing
    vendor_path_to_files = {}
    for row in config_rows:
        if row.s3_vendor_path and row.file_name:
            vendor_path_to_files.setdefault(row.s3_vendor_path, set()).add(row.file_name)

    # For each unique vendor S3 path, process files
    log_rows = []
    for vendor_path, config_file_names in vendor_path_to_files.items():
        # Parse S3 vendor path
        src_bucket, src_prefix = parse_s3_path(vendor_path)
        if not src_bucket or src_prefix is None:
            # Log error for all files under this config
            for file_name in config_file_names:
                config_row = config_map.get((vendor_path, file_name), {})
                log_rows.append({
                    "file_name": file_name,
                    "s3_vendor_path": vendor_path,
                    "s3_landing_path": config_row.get("s3_landing_path"),
                    "s3_archive_path": config_row.get("s3_archive_path"),
                    "file_status": "ERROR_CONFIG",
                    "file_processed_date": datetime.utcnow()
                })
            continue

        # List top-level files in vendor S3 folder
        try:
            vendor_files = list_s3_files(s3_client, src_bucket, src_prefix)
        except Exception as e:
            # S3 access error: log for all files in this vendor path
            for file_name in config_file_names:
                config_row = config_map.get((vendor_path, file_name), {})
                log_rows.append({
                    "file_name": file_name,
                    "s3_vendor_path": vendor_path,
                    "s3_landing_path": config_row.get("s3_landing_path"),
                    "s3_archive_path": config_row.get("s3_archive_path"),
                    "file_status": "ERROR_S3_ACCESS",
                    "file_processed_date": datetime.utcnow()
                })
            continue

        # For each file in vendor folder, determine action
        for file_name in vendor_files:
            config_row = config_map.get((vendor_path, file_name))
            if not config_row:
                # File not in config: log as SKIPPED_NOT_CONFIGURED
                log_rows.append({
                    "file_name": file_name,
                    "s3_vendor_path": vendor_path,
                    "s3_landing_path": None,
                    "s3_archive_path": None,
                    "file_status": "SKIPPED_NOT_CONFIGURED",
                    "file_processed_date": datetime.utcnow()
                })
                continue

            # Check active_flag
            if config_row.get("active_flag") != "A":
                log_rows.append({
                    "file_name": file_name,
                    "s3_vendor_path": vendor_path,
                    "s3_landing_path": config_row.get("s3_landing_path"),
                    "s3_archive_path": config_row.get("s3_archive_path"),
                    "file_status": "SKIPPED_INACTIVE",
                    "file_processed_date": datetime.utcnow()
                })
                continue

            # Validate required S3 paths
            s3_landing_path = config_row.get("s3_landing_path")
            s3_archive_path = config_row.get("s3_archive_path")
            if not s3_landing_path or not s3_archive_path:
                log_rows.append({
                    "file_name": file_name,
                    "s3_vendor_path": vendor_path,
                    "s3_landing_path": s3_landing_path,
                    "s3_archive_path": s3_archive_path,
                    "file_status": "ERROR_CONFIG",
                    "file_processed_date": datetime.utcnow()
                })
                continue

            # Parse landing and archive S3 paths
            dest_bucket, dest_prefix = parse_s3_path(s3_landing_path)
            archive_bucket, archive_prefix = parse_s3_path(s3_archive_path)
            if not dest_bucket or dest_prefix is None or not archive_bucket or archive_prefix is None:
                log_rows.append({
                    "file_name": file_name,
                    "s3_vendor_path": vendor_path,
                    "s3_landing_path": s3_landing_path,
                    "s3_archive_path": s3_archive_path,
                    "file_status": "ERROR_CONFIG",
                    "file_processed_date": datetime.utcnow()
                })
                continue

            # Check if file exists in Purgo S3 (landing)
            try:
                purgo_files = list_s3_files(s3_client, dest_bucket, dest_prefix)
            except Exception as e:
                log_rows.append({
                    "file_name": file_name,
                    "s3_vendor_path": vendor_path,
                    "s3_landing_path": s3_landing_path,
                    "s3_archive_path": s3_archive_path,
                    "file_status": "ERROR_S3_ACCESS",
                    "file_processed_date": datetime.utcnow()
                })
                continue
            if file_name in purgo_files:
                log_rows.append({
                    "file_name": file_name,
                    "s3_vendor_path": vendor_path,
                    "s3_landing_path": s3_landing_path,
                    "s3_archive_path": s3_archive_path,
                    "file_status": "SKIPPED_EXISTS",
                    "file_processed_date": datetime.utcnow()
                })
                continue

            # Check if file exists in Archive S3
            try:
                archive_files = list_s3_files(s3_client, archive_bucket, archive_prefix)
            except Exception as e:
                log_rows.append({
                    "file_name": file_name,
                    "s3_vendor_path": vendor_path,
                    "s3_landing_path": s3_landing_path,
                    "s3_archive_path": s3_archive_path,
                    "file_status": "ERROR_S3_ACCESS",
                    "file_processed_date": datetime.utcnow()
                })
                continue
            if file_name in archive_files:
                log_rows.append({
                    "file_name": file_name,
                    "s3_vendor_path": vendor_path,
                    "s3_landing_path": s3_landing_path,
                    "s3_archive_path": s3_archive_path,
                    "file_status": "SKIPPED_ARCHIVED",
                    "file_processed_date": datetime.utcnow()
                })
                continue

            # Copy file from Vendor S3 to Purgo S3
            src_key = src_prefix + file_name
            dest_key = dest_prefix + file_name
            try:
                copy_s3_file(s3_client, src_bucket, src_key, dest_bucket, dest_key)
                log_rows.append({
                    "file_name": file_name,
                    "s3_vendor_path": vendor_path,
                    "s3_landing_path": s3_landing_path,
                    "s3_archive_path": s3_archive_path,
                    "file_status": "SUCCESS",
                    "file_processed_date": datetime.utcnow()
                })
            except Exception as e:
                msg = str(e)
                if "S3 access denied" in msg:
                    status = "ERROR_S3_ACCESS"
                else:
                    status = "ERROR_COPY"
                log_rows.append({
                    "file_name": file_name,
                    "s3_vendor_path": vendor_path,
                    "s3_landing_path": s3_landing_path,
                    "s3_archive_path": s3_archive_path,
                    "file_status": status,
                    "file_processed_date": datetime.utcnow()
                })

    # Log all file process results
    log_file_process(spark, log_rows)

# ---------------------------------------------------------------------------
# Run main logic
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        # Log error if AWS credentials missing or other fatal error
        # No files to log if config not loaded, so just raise
        raise

# ---------------------------------------------------------------------------
# END OF SCRIPT
# ---------------------------------------------------------------------------
