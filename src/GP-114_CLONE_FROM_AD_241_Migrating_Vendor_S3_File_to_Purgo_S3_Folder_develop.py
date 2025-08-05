spark.catalog.setCurrentCatalog("purgo_databricks")

# ------------------------------------------------------------------------------------
# Databricks PySpark Script: Transfer Active Files from Vendor S3 to Purgo S3
# ------------------------------------------------------------------------------------
# This script:
#   - Reads all configs from purgo_playground.ingest_config_master where active_flag = 'A'
#   - For each config, retrieves s3_vendor_path, s3_landing_path, s3_archive_path
#   - Lists files at the root of the Vendor S3 path (no recursion)
#   - Skips files already present (by base name, case-sensitive) in Purgo or Archive S3 folders
#   - Copies eligible files from Vendor S3 to Purgo S3 (files remain in Vendor S3)
#   - Logs all transfer attempts and errors to purgo_playground.s3_file_process_log
#   - Handles missing configs, missing S3 paths, AWS credential errors, S3 access errors, and unexpected exceptions
#   - Uses Databricks secrets for AWS credentials (scope: aws_keys, keys: access_key, secret_key)
#   - All file operations use dbutils.fs (Databricks native)
#   - All code is production-ready, robust, and follows Databricks best practices
# ------------------------------------------------------------------------------------

# ---------------------------------
# Imports and Setup
# ---------------------------------
from pyspark.sql import functions as F  
from pyspark.sql.types import StringType, TimestampType, StructType, StructField  
from datetime import datetime  
import traceback  

# ---------------------------------
# Helper Functions
# ---------------------------------

def get_aws_credentials():
    """
    Retrieve AWS credentials from Databricks secret scope.
    Returns (access_key, secret_key) or (None, None) if missing/invalid.
    """
    try:
        access_key = dbutils.secrets.get(scope="aws_keys", key="access_key")
        secret_key = dbutils.secrets.get(scope="aws_keys", key="secret_key")
        if not access_key or not secret_key:
            return None, None
        return access_key, secret_key
    except Exception:
        return None, None

def s3_path_to_dbfs_mount(s3_path, access_key, secret_key):
    """
    Convert an S3 path to a dbfs mount path using s3a:// and AWS credentials.
    This does NOT create a persistent mount, but allows dbutils.fs operations.
    """
    # s3_path: e.g., s3://bucket/folder
    if not s3_path or not s3_path.startswith("s3://"):
        return None
    s3a_path = s3_path.replace("s3://", "s3a://", 1)
    return s3a_path

def list_s3_files_root(s3_path, access_key, secret_key):
    """
    List all files at the root of the given S3 path (no recursion).
    Returns a list of file base names (not full paths).
    """
    s3a_path = s3_path_to_dbfs_mount(s3_path, access_key, secret_key)
    if not s3a_path:
        raise Exception(f"Invalid S3 path: {s3_path}")
    try:
        # Set Hadoop configs for S3 access (session-scoped)
        spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", access_key)
        spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", secret_key)
        # List files at root (no recursion)
        files = []
        for f in dbutils.fs.ls(s3a_path):
            if f.isFile():
                # Only include files at the root (no subfolders)
                base_name = f.name
                if "/" not in base_name:
                    files.append(base_name)
        return files
    except Exception as e:
        raise Exception(f"S3 access denied or path not found: {s3_path} ({str(e)})")

def file_exists_in_s3(s3_path, file_name, access_key, secret_key):
    """
    Check if a file with the given base name exists at the root of the S3 path.
    Returns True if exists, False otherwise.
    """
    try:
        files = list_s3_files_root(s3_path, access_key, secret_key)
        return file_name in files
    except Exception:
        return False

def copy_s3_file(src_s3_path, dest_s3_path, file_name, access_key, secret_key):
    """
    Copy a file from src_s3_path/file_name to dest_s3_path/file_name using dbutils.fs.cp.
    Returns True if successful, False otherwise.
    """
    src_s3a = s3_path_to_dbfs_mount(src_s3_path, access_key, secret_key)
    dest_s3a = s3_path_to_dbfs_mount(dest_s3_path, access_key, secret_key)
    if not src_s3a or not dest_s3a:
        return False
    src_file = src_s3a.rstrip("/") + "/" + file_name
    dest_file = dest_s3a.rstrip("/") + "/" + file_name
    try:
        dbutils.fs.cp(src_file, dest_file, recurse=False)
        return True
    except Exception:
        return False

def log_file_process(file_name, s3_vendor_path, s3_landing_path, s3_archive_path, file_status, log_table, error_message=None):
    """
    Log a file processing attempt to the log table.
    """
    now = datetime.utcnow()
    row = [(file_name, s3_vendor_path, s3_landing_path, s3_archive_path, file_status, now)]
    schema = StructType([
        StructField("file_name", StringType(), True),
        StructField("s3_vendor_path", StringType(), True),
        StructField("s3_landing_path", StringType(), True),
        StructField("s3_archive_path", StringType(), True),
        StructField("file_status", StringType(), True),
        StructField("file_processed_date", TimestampType(), True)
    ])
    df = spark.createDataFrame(row, schema=schema)
    # Ensure column count matches
    if len(df.columns) == len(spark.table(log_table).columns):
        df.write.mode("append").format("delta").saveAsTable(log_table)
    # Optionally, print error for debugging
    if error_message:
        print(f"[{file_status}] {file_name}: {error_message}")

# ---------------------------------
# Main Processing Logic
# ---------------------------------

def process_all_configs():
    """
    Main function to process all active configs and transfer eligible files.
    """
    config_table = "purgo_playground.ingest_config_master"
    log_table = "purgo_playground.s3_file_process_log"

    # Get AWS credentials
    access_key, secret_key = get_aws_credentials()
    if not access_key or not secret_key:
        # Log error for all attempted configs
        configs = spark.table(config_table).filter(F.col("active_flag") == "A").collect()
        if not configs:
            log_file_process(None, None, None, None, "ERROR", log_table, "AWS credentials missing or invalid")
        else:
            for cfg in configs:
                log_file_process(None, cfg.s3_vendor_path, cfg.s3_landing_path, cfg.s3_archive_path, "ERROR", log_table, "AWS credentials missing or invalid")
        return

    # Read all active configs
    configs = spark.table(config_table).filter(F.col("active_flag") == "A").collect()
    if not configs:
        # No active configs
        log_file_process(None, None, None, None, "ERROR", log_table, "No active configs found")
        return

    for cfg in configs:
        try:
            s3_vendor_path = cfg.s3_vendor_path
            s3_landing_path = cfg.s3_landing_path
            s3_archive_path = cfg.s3_archive_path
            config_id = cfg.config_id

            # Validate S3 paths
            if not s3_vendor_path or not s3_landing_path or not s3_archive_path:
                log_file_process(None, s3_vendor_path, s3_landing_path, s3_archive_path, "ERROR", log_table, "Missing S3 path in config")
                continue

            # List files at root of Vendor S3 path
            try:
                vendor_files = list_s3_files_root(s3_vendor_path, access_key, secret_key)
            except Exception as e:
                log_file_process(None, s3_vendor_path, s3_landing_path, s3_archive_path, "ERROR", log_table, str(e))
                continue

            # List files at root of Purgo and Archive S3 paths
            try:
                purgo_files = list_s3_files_root(s3_landing_path, access_key, secret_key)
            except Exception as e:
                log_file_process(None, s3_vendor_path, s3_landing_path, s3_archive_path, "ERROR", log_table, str(e))
                continue
            try:
                archive_files = list_s3_files_root(s3_archive_path, access_key, secret_key)
            except Exception as e:
                log_file_process(None, s3_vendor_path, s3_landing_path, s3_archive_path, "ERROR", log_table, str(e))
                continue

            # Determine eligible files (not in Purgo or Archive, by base name, case-sensitive)
            eligible_files = []
            skipped_exists = []
            skipped_archive = []
            for f in vendor_files:
                if f in purgo_files:
                    skipped_exists.append(f)
                elif f in archive_files:
                    skipped_archive.append(f)
                else:
                    eligible_files.append(f)

            # Log skipped files (already in Purgo)
            for f in skipped_exists:
                log_file_process(f, s3_vendor_path, s3_landing_path, s3_archive_path, "SKIPPED_EXISTS", log_table, "File already exists in Purgo S3 folder")

            # Log skipped files (already in Archive)
            for f in skipped_archive:
                log_file_process(f, s3_vendor_path, s3_landing_path, s3_archive_path, "SKIPPED_ARCHIVE", log_table, "File already exists in Archive S3 folder")

            # If no eligible files, log SKIPPED_NONE
            if not eligible_files:
                log_file_process(None, s3_vendor_path, s3_landing_path, s3_archive_path, "SKIPPED_NONE", log_table, "No eligible files to transfer")
                continue

            # Copy eligible files and log results
            for f in eligible_files:
                try:
                    success = copy_s3_file(s3_vendor_path, s3_landing_path, f, access_key, secret_key)
                    if success:
                        log_file_process(f, s3_vendor_path, s3_landing_path, s3_archive_path, "SUCCESS", log_table)
                    else:
                        log_file_process(f, s3_vendor_path, s3_landing_path, s3_archive_path, "ERROR", log_table, "File copy failed")
                except Exception as e:
                    log_file_process(f, s3_vendor_path, s3_landing_path, s3_archive_path, "ERROR", log_table, f"Unexpected error: {str(e)}")
        except Exception as e:
            # Catch-all for unexpected config-level errors
            log_file_process(None, getattr(cfg, "s3_vendor_path", None), getattr(cfg, "s3_landing_path", None), getattr(cfg, "s3_archive_path", None), "ERROR", log_table, f"Unexpected error: {traceback.format_exc()}")

# ---------------------------------
# Execute Main Processing
# ---------------------------------
process_all_configs()
# ------------------------------------------------------------------------------------
# End of Script
# ------------------------------------------------------------------------------------
