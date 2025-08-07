%pip install IPython
%pip install boto3
%pip install botocore

spark.catalog.setCurrentCatalog("purgo_databricks")

# -----------------------------------------------------------
# /*
#   Databricks PySpark Script: Transfer eligible files from Vendor S3 to Purgo S3
#   - Catalog: purgo_databricks
#   - Schema: purgo_playground
#   - Config Table: purgo_playground.ingest_config_master
#   - Only process configs with active_flag = "A"
#   - Only transfer files not present in Purgo or Archive S3
#   - S3 paths are dynamically retrieved from config table
#   - AWS credentials are securely accessed from Databricks secret scope "aws_keys"
#   - File transfer is COPY (not MOVE)
#   - File name matching is case-sensitive, full match, including extension
#   - Handles recursive/non-recursive listing based on file_recursive_flag
#   - All error handling and logging as per requirements
#   - No plain text output outside of code/comments
#   - All code is Databricks/PySpark native and production-ready
# */

# -----------------------------------------------------------
# /* 
#   IMPORTS
#   - Only necessary imports included
#   - Each import is annotated with required pip package
# */
# from pyspark.sql import SparkSession  # SparkSession is already available in Databricks
from pyspark.sql.functions import col, lit, when, array, struct, expr  
from pyspark.sql.types import StringType, StructType, StructField  
from pyspark.sql.utils import AnalysisException  
import re  
import sys  
import traceback  
import boto3  
from botocore.exceptions import ClientError, NoCredentialsError, EndpointConnectionError  

# -----------------------------------------------------------
# /* 
#   SETUP & CONFIGURATION
#   - Set current catalog and schema
#   - All tables referenced with full path
# */
spark.sql('USE CATALOG purgo_databricks')
spark.sql('USE purgo_playground')

# -----------------------------------------------------------
# /* 
#   LOGGING SETUP
#   - Use Python logging for error and summary logs
# */
import logging  
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("s3_file_transfer")

# -----------------------------------------------------------
# /* 
#   HELPER FUNCTIONS
#   - S3 URI validation
#   - S3 file listing (recursive/non-recursive)
#   - S3 file copy (using boto3)
#   - AWS credentials retrieval from Databricks secrets
#   - Error handling and logging
#   - dbutils is available in Databricks notebooks, but not as a global in jobs/scripts.
#     To ensure compatibility, get dbutils via IPython if not present.
# */
try:
    dbutils
except NameError:
    import IPython
    dbutils = IPython.get_ipython().user_ns["dbutils"]

def is_valid_s3_uri(s3_uri):
    # Validate S3 URI format: s3://bucket/prefix/
    if not isinstance(s3_uri, str) or not s3_uri.startswith("s3://"):
        return False
    m = re.match(r"^s3://([^/]+)/?(.*)", s3_uri)
    return m is not None

def parse_s3_uri(s3_uri):
    # Parse S3 URI into bucket and prefix
    m = re.match(r"^s3://([^/]+)/?(.*)", s3_uri)
    if not m:
        raise Exception(f"Invalid S3 URI in configuration: {s3_uri}")
    bucket = m.group(1)
    prefix = m.group(2)
    if prefix and not prefix.endswith("/"):
        prefix += "/"
    return bucket, prefix or ""

def list_s3_files(s3_client, bucket, prefix, recursive):
    # List files in S3 bucket/prefix, optionally recursively
    files = []
    paginator = s3_client.get_paginator('list_objects_v2')
    operation_parameters = {'Bucket': bucket, 'Prefix': prefix}
    try:
        for page in paginator.paginate(**operation_parameters):
            if 'Contents' in page:
                for obj in page['Contents']:
                    key = obj['Key']
                    if not key.endswith('/'):
                        if recursive:
                            rel_key = key[len(prefix):] if key.startswith(prefix) else key
                            files.append(rel_key)
                        else:
                            # Only files directly under prefix (no '/')
                            rel_key = key[len(prefix):] if key.startswith(prefix) else key
                            if '/' not in rel_key:
                                files.append(rel_key)
        return files
    except ClientError as e:
        raise Exception(f"S3 list error for s3://{bucket}/{prefix}: {e.response.get('Error', {}).get('Message', str(e))}")
    except Exception as e:
        raise Exception(f"S3 list error for s3://{bucket}/{prefix}: {str(e)}")

def copy_s3_file(s3_client, src_bucket, src_prefix, file_name, dst_bucket, dst_prefix):
    # Copy file from src_bucket/src_prefix/file_name to dst_bucket/dst_prefix/file_name
    src_key = src_prefix + file_name
    dst_key = dst_prefix + file_name
    copy_source = {'Bucket': src_bucket, 'Key': src_key}
    try:
        s3_client.copy(copy_source, dst_bucket, dst_key)
        return True, None
    except ClientError as e:
        error_msg = e.response.get('Error', {}).get('Message', str(e))
        return False, error_msg
    except Exception as e:
        return False, str(e)

def get_aws_credentials():
    # Retrieve AWS credentials from Databricks secret scope "aws_keys"
    try:
        access_key = dbutils.secrets.get(scope="aws_keys", key="access_key")
        secret_key = dbutils.secrets.get(scope="aws_keys", key="secret_key")
        if not access_key or not secret_key:
            raise Exception("AWS credentials not found in Databricks secret scope 'aws_keys'")
        return access_key, secret_key
    except Exception as e:
        raise Exception("AWS credentials not found in Databricks secret scope 'aws_keys'")

def log_error(msg):
    logger.error(msg)

def log_info(msg):
    logger.info(msg)

# -----------------------------------------------------------
# /* 
#   MAIN LOGIC
#   - Retrieve active configs from ingest_config_master
#   - For each config, validate S3 paths
#   - For each config, list files in Vendor, Purgo, Archive S3
#   - Determine eligible files (not in Purgo or Archive)
#   - Copy eligible files from Vendor to Purgo S3
#   - Log summary and errors
# */

def main():
    # Summary and error logs
    transfer_summary = {}
    error_log = []

    # Step 1: Retrieve AWS credentials
    try:
        access_key, secret_key = get_aws_credentials()
    except Exception as e:
        log_error(str(e))
        sys.exit(1)

    # Step 2: Create boto3 S3 client
    try:
        s3_client = boto3.client(
            's3',
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key
        )
    except Exception as e:
        log_error(f"Failed to create S3 client: {str(e)}")
        sys.exit(1)

    # Step 3: Retrieve active configs from ingest_config_master
    try:
        config_df = spark.table("purgo_playground.ingest_config_master")
    except AnalysisException:
        log_error("Configuration table purgo_playground.ingest_config_master not found or inaccessible")
        sys.exit(1)
    except Exception as e:
        log_error(f"Error accessing configuration table: {str(e)}")
        sys.exit(1)

    active_configs = config_df.filter(col("active_flag") == "A").select(
        "config_id", "s3_vendor_path", "s3_landing_path", "s3_archive_path", "file_recursive_flag"
    )

    if active_configs.rdd.isEmpty():
        log_error("No active configurations found in purgo_playground.ingest_config_master")
        sys.exit(1)

    for row in active_configs.collect():
        config_id = row["config_id"]
        s3_vendor_path = row["s3_vendor_path"]
        s3_landing_path = row["s3_landing_path"]
        s3_archive_path = row["s3_archive_path"]
        file_recursive_flag = row["file_recursive_flag"]

        # Step 4: Validate S3 paths (not null, not empty, valid S3 URI)
        if (s3_vendor_path is None or s3_vendor_path == "" or
            s3_landing_path is None or s3_landing_path == "" or
            s3_archive_path is None or s3_archive_path == ""):
            log_error("S3 path columns (s3_vendor_path, s3_landing_path, s3_archive_path) must not be null or empty for active configuration")
            continue
        if not is_valid_s3_uri(s3_vendor_path):
            log_error(f"Invalid S3 URI in configuration: {s3_vendor_path}")
            continue
        if not is_valid_s3_uri(s3_landing_path):
            log_error(f"Invalid S3 URI in configuration: {s3_landing_path}")
            continue
        if not is_valid_s3_uri(s3_archive_path):
            log_error(f"Invalid S3 URI in configuration: {s3_archive_path}")
            continue

        # Step 5: Parse S3 URIs
        try:
            vendor_bucket, vendor_prefix = parse_s3_uri(s3_vendor_path)
            purgo_bucket, purgo_prefix = parse_s3_uri(s3_landing_path)
            archive_bucket, archive_prefix = parse_s3_uri(s3_archive_path)
        except Exception as e:
            log_error(str(e))
            continue

        # Step 6: Determine recursive listing
        recursive = (file_recursive_flag == "Y")

        # Step 7: List files in Vendor, Purgo, Archive S3
        try:
            vendor_files = list_s3_files(s3_client, vendor_bucket, vendor_prefix, recursive)
        except Exception as e:
            log_error(f"Failed to list Vendor S3 files for config_id {config_id}: {str(e)}")
            continue
        try:
            purgo_files = list_s3_files(s3_client, purgo_bucket, purgo_prefix, True)
        except Exception as e:
            log_error(f"Failed to list Purgo S3 files for config_id {config_id}: {str(e)}")
            continue
        try:
            archive_files = list_s3_files(s3_client, archive_bucket, archive_prefix, True)
        except Exception as e:
            log_error(f"Failed to list Archive S3 files for config_id {config_id}: {str(e)}")
            continue

        # Step 8: Clean file lists (skip null/empty, deduplicate)
        vendor_files = [f for f in set(vendor_files) if f is not None and f != ""]
        purgo_files = set([f for f in purgo_files if f is not None and f != ""])
        archive_files = set([f for f in archive_files if f is not None and f != ""])

        # Step 9: Determine eligible files (not in Purgo or Archive)
        eligible_files = [f for f in vendor_files if f not in purgo_files and f not in archive_files]

        if not eligible_files:
            log_info(f"No eligible files to transfer for config_id {config_id}")
            continue

        # Step 10: Copy eligible files from Vendor to Purgo S3
        transferred = []
        for file_name in eligible_files:
            try:
                success, err = copy_s3_file(
                    s3_client,
                    vendor_bucket, vendor_prefix, file_name,
                    purgo_bucket, purgo_prefix
                )
                if success:
                    transferred.append(file_name)
                else:
                    log_error(f"Failed to transfer {file_name}: {err}")
                    error_log.append(f"Failed to transfer {file_name}: {err}")
            except Exception as e:
                log_error(f"Failed to transfer {file_name}: {str(e)}")
                error_log.append(f"Failed to transfer {file_name}: {str(e)}")

        # Step 11: Log summary for this config
        if transferred:
            transfer_summary[config_id] = transferred
            log_info(f"config_id {config_id}: transferred_files: {', '.join(transferred)}")

    # Step 12: Log overall summary
    if transfer_summary:
        for config_id, files in transfer_summary.items():
            log_info(f"Summary - config_id: {config_id}, transferred_files: {', '.join(files)}")
    else:
        log_info("No files were transferred in this run.")

    # Step 13: Log errors
    if error_log:
        for err in error_log:
            log_error(err)

# -----------------------------------------------------------
# /* 
#   EXECUTE MAIN LOGIC
# */
if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        log_error(f"Fatal error in S3 file transfer script: {str(e)}")
        traceback.print_exc()

# /* End of script */
