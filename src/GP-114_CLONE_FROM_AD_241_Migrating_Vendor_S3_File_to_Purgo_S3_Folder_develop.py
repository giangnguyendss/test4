%pip install boto3
%pip install botocore

spark.catalog.setCurrentCatalog("purgo_databricks")

# ---------------------------------------------------------------------------
# Databricks PySpark Script: Transfer Active Vendor S3 Files to Purgo S3 Folder
# ---------------------------------------------------------------------------
# Requirements:
# - Only process configs in purgo_playground.ingest_config_master with active_flag = "A"
# - Only files in the root of vendor S3 folder (no recursion)
# - File name match is case-sensitive, including extension, and only on file name (not path)
# - Do not transfer if file exists in Purgo or Archive S3 folders
# - Only .csv and .txt files allowed
# - No overwrite in Purgo S3; skip if exists
# - No move; copy only (do not delete from vendor S3)
# - Use AWS credentials from Databricks secret scope "aws_keys"
# - Raise/log errors for invalid config, S3 path, file name, credentials, etc.
# - No temp views/tables; no status table/log update unless specified
# - All string fields use STRING type; all date fields use DATE or TIMESTAMP type
# - Use only Databricks native data types and APIs
# - All code is production-ready, robust, and follows Databricks best practices
# ---------------------------------------------------------------------------

# -------------------------------
# Imports and Setup
# -------------------------------

# Commented out SparkSession initialization (already available in Databricks)
# from pyspark.sql import SparkSession  # built-in
# spark = SparkSession.builder.getOrCreate()

import re  
import sys  
from pyspark.sql.types import StringType, StructType, StructField, LongType  
from pyspark.sql.functions import col  
from typing import List, Dict  

# -------------------------------
# Constants and Configurations
# -------------------------------

CATALOG_NAME = "purgo_databricks"
SCHEMA_NAME = "purgo_playground"
CONFIG_TABLE = f"{SCHEMA_NAME}.ingest_config_master"
ALLOWED_EXTENSIONS = [".csv", ".txt"]
INVALID_FILENAME_PATTERN = r"[:\n\t]"
AWS_SECRET_SCOPE = "aws_keys"
AWS_ACCESS_KEY_NAME = "access_key"
AWS_SECRET_KEY_NAME = "secret_key"

# -------------------------------
# Helper Functions
# -------------------------------

def get_aws_credential(secret_scope: str, key: str) -> str:
    """
    Retrieve AWS credential from Databricks secret scope.
    Raises Exception if not found or not accessible.
    """
    try:
        return dbutils.secrets.get(scope=secret_scope, key=key)
    except Exception as e:
        if "not found" in str(e) or "does not exist" in str(e):
            raise Exception(f"Missing AWS credentials in Databricks secret scope '{secret_scope}'")
        if "PERMISSION_DENIED" in str(e) or "permission" in str(e).lower():
            raise Exception(f"Unable to access Databricks secret scope '{secret_scope}' for AWS credentials")
        raise Exception(f"Failed to retrieve AWS credential '{key}' from secret scope '{secret_scope}': {str(e)}")

def is_valid_s3_uri(uri: str) -> bool:
    """
    Validate S3 URI format.
    """
    return isinstance(uri, str) and uri.startswith("s3://") and len(uri) > 5

def is_valid_file_name(file_name: str) -> bool:
    """
    Validate file name: not null/empty, no forbidden chars, no leading/trailing whitespace, no subfolder.
    """
    if file_name is None or file_name == "":
        return False
    if re.search(INVALID_FILENAME_PATTERN, file_name):
        return False
    if file_name.strip() != file_name:
        return False
    if "/" in file_name:
        return False
    return True

def is_allowed_extension(file_name: str) -> bool:
    """
    Check if file extension is allowed.
    """
    return any(file_name.endswith(ext) for ext in ALLOWED_EXTENSIONS)

def has_duplicate_file_names(file_list: List[Dict]) -> bool:
    """
    Check for duplicate file names in a list of file dicts.
    """
    names = [f["name"] for f in file_list if f["name"] is not None]
    return len(names) != len(set(names))

def is_zero_byte(file_dict: Dict) -> bool:
    """
    Check if file is zero bytes.
    """
    return file_dict.get("size", 1) == 0

def list_s3_files(folder_path: str, aws_access_key: str, aws_secret_key: str) -> List[Dict]:
    """
    List files in S3 folder (root only, no recursion).
    Returns list of dicts: {name, size}
    Raises Exception if folder does not exist or S3 access fails.
    """
    import boto3  
    from botocore.exceptions import ClientError  

    # Parse bucket and prefix from s3://bucket/prefix/
    match = re.match(r"s3://([^/]+)/(.+)", folder_path.rstrip("/"))
    if not match:
        raise Exception(f"Invalid S3 URI: {folder_path}")
    bucket, prefix = match.group(1), match.group(2)
    prefix = prefix.rstrip("/") + "/"

    try:
        s3 = boto3.client(
            "s3",
            aws_access_key_id=aws_access_key,
            aws_secret_access_key=aws_secret_key,
        )
        paginator = s3.get_paginator("list_objects_v2")
        page_iterator = paginator.paginate(Bucket=bucket, Prefix=prefix, Delimiter="/")
        files = []
        for page in page_iterator:
            for obj in page.get("Contents", []):
                key = obj["Key"]
                # Only root files (no subfolder)
                rel_key = key[len(prefix):]
                if rel_key and "/" not in rel_key:
                    files.append({"name": rel_key, "size": obj["Size"]})
        return files
    except ClientError as e:
        if e.response["Error"]["Code"] in ["NoSuchBucket", "404"]:
            raise Exception(f"S3 bucket or folder '{folder_path}' does not exist")
        if e.response["Error"]["Code"] in ["InvalidAccessKeyId", "SignatureDoesNotMatch"]:
            raise Exception("Failed to authenticate to S3 with provided credentials")
        raise Exception(f"S3 access error for '{folder_path}': {str(e)}")
    except Exception as e:
        raise Exception(f"S3 access error for '{folder_path}': {str(e)}")

def copy_s3_file(src_folder: str, file_name: str, dest_folder: str, aws_access_key: str, aws_secret_key: str) -> None:
    """
    Copy file from src_folder/file_name to dest_folder/file_name in S3.
    Raises Exception if copy fails.
    """
    import boto3  
    from botocore.exceptions import ClientError  

    src_match = re.match(r"s3://([^/]+)/(.+)", src_folder.rstrip("/"))
    dest_match = re.match(r"s3://([^/]+)/(.+)", dest_folder.rstrip("/"))
    if not src_match or not dest_match:
        raise Exception(f"Invalid S3 URI for copy: {src_folder} or {dest_folder}")
    src_bucket, src_prefix = src_match.group(1), src_match.group(2)
    dest_bucket, dest_prefix = dest_match.group(1), dest_match.group(2)
    src_key = src_prefix.rstrip("/") + "/" + file_name
    dest_key = dest_prefix.rstrip("/") + "/" + file_name

    try:
        s3 = boto3.client(
            "s3",
            aws_access_key_id=aws_access_key,
            aws_secret_access_key=aws_secret_key,
        )
        copy_source = {"Bucket": src_bucket, "Key": src_key}
        s3.copy(copy_source, dest_bucket, dest_key)
    except ClientError as e:
        raise Exception(f"Failed to copy '{file_name}' from '{src_folder}' to '{dest_folder}': {str(e)}")
    except Exception as e:
        raise Exception(f"Failed to copy '{file_name}' from '{src_folder}' to '{dest_folder}': {str(e)}")

def log_warning(msg: str) -> None:
    """
    Log a warning message (Databricks log + stdout).
    """
    print(f"WARNING: {msg}")

def log_error(msg: str) -> None:
    """
    Log an error message (Databricks log + stdout).
    """
    print(f"ERROR: {msg}")

# -------------------------------
# Main Processing Logic
# -------------------------------

def main():
    # Set Unity Catalog and schema
    spark.sql(f"USE CATALOG {CATALOG_NAME}")
    spark.sql(f"USE {SCHEMA_NAME}")

    # Retrieve AWS credentials from Databricks secret
    try:
        aws_access_key = get_aws_credential(AWS_SECRET_SCOPE, AWS_ACCESS_KEY_NAME)
        aws_secret_key = get_aws_credential(AWS_SECRET_SCOPE, AWS_SECRET_KEY_NAME)
    except Exception as e:
        log_error(str(e))
        sys.exit(1)

    # Read active configs from ingest_config_master
    configs_df = (
        spark.table(CONFIG_TABLE)
        .filter(
            (col("active_flag") == "A") &
            col("s3_vendor_path").isNotNull() &
            col("s3_landing_path").isNotNull() &
            col("s3_archive_path").isNotNull()
        )
        .select(
            "config_id", "s3_vendor_path", "s3_landing_path", "s3_archive_path"
        )
    )

    configs = configs_df.collect()
    if not configs:
        # No active configs; nothing to do
        return

    for row in configs:
        config_id = row["config_id"]
        s3_vendor_path = row["s3_vendor_path"]
        s3_landing_path = row["s3_landing_path"]
        s3_archive_path = row["s3_archive_path"]

        # Validate S3 URIs
        if not (is_valid_s3_uri(s3_vendor_path) and is_valid_s3_uri(s3_landing_path) and is_valid_s3_uri(s3_archive_path)):
            log_error(f"Invalid S3 URI in ingest_config_master for config_id {config_id}")
            continue

        # List files in vendor, purgo, and archive S3 folders (root only)
        try:
            vendor_files = list_s3_files(s3_vendor_path, aws_access_key, aws_secret_key)
        except Exception as e:
            log_error(f"{str(e)} for config_id {config_id}")
            continue
        try:
            purgo_files = list_s3_files(s3_landing_path, aws_access_key, aws_secret_key)
        except Exception as e:
            log_error(f"{str(e)} for config_id {config_id}")
            continue
        try:
            archive_files = list_s3_files(s3_archive_path, aws_access_key, aws_secret_key)
        except Exception as e:
            log_error(f"{str(e)} for config_id {config_id}")
            continue

        # Check for duplicate file names in vendor S3 folder
        if has_duplicate_file_names(vendor_files):
            log_error(f"Duplicate file name found in vendor S3 folder for config_id {config_id}")
            continue

        purgo_file_names = set(f["name"] for f in purgo_files)
        archive_file_names = set(f["name"] for f in archive_files)

        for f in vendor_files:
            file_name = f["name"]
            file_size = f["size"]

            # File name validation
            if not is_valid_file_name(file_name):
                if file_name is None or file_name == "":
                    log_error(f"Invalid file name encountered in vendor S3 folder for config_id {config_id}")
                elif re.search(INVALID_FILENAME_PATTERN, file_name):
                    log_error(f"Invalid file name '{file_name}' in vendor S3 folder for config_id {config_id}")
                elif file_name.strip() != file_name:
                    log_error(f"File name '{file_name}' contains leading or trailing whitespace in vendor S3 folder for config_id {config_id}")
                elif "/" in file_name:
                    # Subfolder file; skip silently as per requirements
                    continue
                continue

            # File extension validation
            if not is_allowed_extension(file_name):
                log_warning(f"File extension '{file_name[file_name.rfind('.'):]}' not allowed for {file_name} in config_id {config_id}")
                continue

            # Zero-byte file check
            if is_zero_byte(f):
                log_warning(f"File '{file_name}' in vendor S3 folder for config_id {config_id} is empty and was skipped")
                continue

            # Skip if file exists in Purgo or Archive S3 (case-sensitive)
            if file_name in purgo_file_names:
                # Do not overwrite; skip
                continue
            if file_name in archive_file_names:
                # Do not transfer; skip
                continue

            # Copy file from vendor S3 to Purgo S3
            try:
                copy_s3_file(s3_vendor_path, file_name, s3_landing_path, aws_access_key, aws_secret_key)
                print(f"Copied '{file_name}' from '{s3_vendor_path}' to '{s3_landing_path}' for config_id {config_id}")
            except Exception as e:
                log_error(str(e))
                continue

# -------------------------------
# Script Entry Point
# -------------------------------

if __name__ == "__main__":
    main()
# ---------------------------------------------------------------------------
# End of Script
# ---------------------------------------------------------------------------
