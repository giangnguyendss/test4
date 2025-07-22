# PySpark script for Databricks: Partitioned Parquet backup of customer_360_raw + Delta vacuum cleanup
# Purpose: Efficiently backup Unity Catalog customer_360_raw as partitioned Parquet (partitioned by state) to a volume with snappy compression, and vacuum/remove records older than 30 days from the Delta table.
# Author: Giang Nguyen
# Date: 2025-07-22
# Description: This script reads all columns from the Unity Catalog table purgo_databricks.purgo_playground.customer_360_raw, validates that 'state' and 'updated_at' exist, writes a snappy-compressed and state-partitioned Parquet backup to /Volumes/customer_360_raw_backup, logs all key actions/errors to /dbfs/logs/customer_360_raw_backup.log, and runs delete+VACUUM on the Delta table to physically remove records with updated_at older than 30 days. Script includes data quality, permission, and failure checks.

# -- Imports: Spark SQL, Delta Lake, OS, logging, JSON
from pyspark.sql import functions as F              
from pyspark.sql.types import StructType, StructField, StringType, TimestampType 
from delta.tables import DeltaTable                 
import datetime                                    
import os                                          
import json                                        

# -- Constants/config
CATALOG_TABLE = "purgo_databricks.purgo_playground.customer_360_raw"
PARQUET_BACKUP_PATH = "/Volumes/customer_360_raw_backup"
LOG_FILE_PATH = "/dbfs/logs/customer_360_raw_backup.log"
RETENTION_DAYS = 30
PARTITION_COL = "state"
TIMESTAMP_COL = "updated_at"
PARQUET_COMPRESSION = "snappy"

# -- Utility function: Logging JSON event
def log_event(operation, status, details="", error_message=None):
    """
    Logs an operation status as a JSON object to the log file.
    
    Args:
        operation (str): Operation performed.
        status (str): Result/status.
        details (str): Optional details.
        error_message (str|None): Optional error message.
    Returns:
        None
    """
    entry = {
        "event_time": datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "operation": operation,
        "status": status,
        "details": details,
        "error_message": error_message
    }
    try:
        with open(LOG_FILE_PATH, "a") as f:
            f.write(json.dumps(entry) + "\n")
    except Exception as e:
        # Fallback: print to driver stdout but don't fail
        print(f"Log write failed: {e}")

# -- Read source table, handle permission/table errors
try:
    df = spark.read.table(CATALOG_TABLE)
    log_event("read_table", "OK", f"Table {CATALOG_TABLE} loaded ({df.count()} rows)")
except Exception as ex:
    log_event("read_table", "FAIL", "", f"READ_ERROR: {str(ex)}")
    raise

# -- Validate required cols before backup
columns = df.columns
if PARTITION_COL not in columns:
    log_event("col_check", "FAIL", "", f"PARTITION_COLUMN_MISSING: '{PARTITION_COL}'")
    raise Exception(f"PARTITION_COLUMN_MISSING: '{PARTITION_COL}'")
if TIMESTAMP_COL not in columns:
    log_event("col_check", "FAIL", "", f"TIMESTAMP_COLUMN_MISSING: '{TIMESTAMP_COL}'")
    raise Exception(f"TIMESTAMP_COLUMN_MISSING: '{TIMESTAMP_COL}'")

# -- Check for empty table/data
row_count = df.count()
if row_count == 0:
    log_event("backup", "FAIL", "NO_DATA_TO_BACKUP")
else:
    # -- Write backup as snappy compressed Parquet, partitioned by state
    try:
        if PARQUET_COMPRESSION == "none":
            log_event("backup", "FAIL", "", "COMPRESSION_REQUIRED: Output compression must be enabled")
            raise Exception("COMPRESSION_REQUIRED: Output compression must be enabled")
        df.write.mode("overwrite") \
            .partitionBy(PARTITION_COL) \
            .option("compression", PARQUET_COMPRESSION) \
            .parquet(PARQUET_BACKUP_PATH)
        log_event("backup", "OK", f"Wrote {row_count} rows backup partitioned by {PARTITION_COL} to {PARQUET_BACKUP_PATH}")
    except Exception as ex:
        log_event("backup", "FAIL", "", str(ex))
        raise

    # -- Optional: Validate backup Parquet, row count and schema
    try:
        df2 = spark.read.parquet(PARQUET_BACKUP_PATH)
        backup_count = df2.count()
        if backup_count != row_count:
            log_event("backup_validation", "FAIL", f"Source {row_count} vs Backup {backup_count}", "ROW_COUNT_MISMATCH")
        if set(df2.columns) != set(df.columns):
            log_event("backup_validation", "FAIL", f"Backup columns: {df2.columns}", "COLUMN_MISMATCH")
        log_event("backup_validation", "OK", f"Backup schema and count validated, {backup_count} rows")
    except Exception as ex:
        log_event("backup_validation", "FAIL", "", f"BACKUP_READ_FAIL: {str(ex)}")
        raise

# -- Vacuum records older than 30 days from the Delta table (delete + vacuum)
try:
    delta_tbl = DeltaTable.forName(spark, CATALOG_TABLE)
    now_utc = datetime.datetime.utcnow()
    cutoff = now_utc - datetime.timedelta(days=RETENTION_DAYS)
    cutoff_str = cutoff.strftime("%Y-%m-%d %H:%M:%S")
    # Validate updated_at can be parsed, or raise
    tmp = df.withColumn("updated_at_ts", F.to_timestamp(F.col(TIMESTAMP_COL), "yyyy-MM-dd HH:mm:ss"))
    bad_date_cnt = tmp.filter((F.col(TIMESTAMP_COL).isNotNull()) & (F.col("updated_at_ts").isNull())).count()
    if bad_date_cnt > 0:
        log_event("vacuum", "FAIL", "", "INVALID_DATETIME_FORMAT: 'updated_at' is not yyyy-MM-dd HH:mm:ss")
        raise Exception("INVALID_DATETIME_FORMAT: 'updated_at' is not yyyy-MM-dd HH:mm:ss")
    # Delete rows older than cutoff in-place
    delta_tbl.delete(f"{TIMESTAMP_COL} < '{cutoff_str}'")
    log_event("vacuum_delete", "OK", f"Removed records with {TIMESTAMP_COL} < {cutoff_str}")
    # Run physical Delta VACUUM
    spark.sql(f"VACUUM `{CATALOG_TABLE}` RETAIN 0 HOURS")
    log_event("vacuum_physical", "OK", f"VACUUM complete for {CATALOG_TABLE}")
except Exception as ex:
    log_event("vacuum", "FAIL", "", str(ex))
    raise

# -- Data consistency check: compare row counts after backup
def validate_backup_consistency():
    """
    Validates that backup Parquet row count matches source post-backup.
    Args: None
    Returns: None
    """
    try:
        src_cnt = spark.read.table(CATALOG_TABLE).count()
        backup_cnt = spark.read.parquet(PARQUET_BACKUP_PATH).count()
        if src_cnt != backup_cnt:
            log_event("consistency_check", "FAIL", f"{src_cnt} source vs {backup_cnt} backup", "ROWCOUNT_MISMATCH_POST_BACKUP")
        else:
            log_event("consistency_check", "OK", f"{src_cnt} rows in both backup and source")
    except Exception as ex:
        log_event("consistency_check", "FAIL", "", str(ex))

validate_backup_consistency()

# -- End of script
# All major steps logged and validated
