# PySpark script for Databricks: Column-wise validation of product_plant vs product_plant_v2, with result table recreation and comprehensive error handling
# Purpose: Compare each row and column (by name) from product_plant and product_plant_v2, generate validation columns, output validation table
# Author: Giang Nguyen
# Date: 2025-07-22
# Description:
#   This script performs record-by-record, column-by-column comparison of two tables: product_plant and product_plant_v2.
#   - For columns common to both tables, it outputs product_plant.column, product_plant_v2.column, column_validation triplets.
#   - Joins on a shared key ('id'), only includes rows present in both tables.
#   - Handles nulls, type, and case sensitivity as per functional spec.
#   - Re-creates the output table 'pp_validation_results' on each run.
#   - Ignores columns only present on one side; includes only columns present in both.
#   - Detailed logging and data quality checks provided.
#   - All table and column references are hard-coded for demonstration, but could be parameterized.

# --- REQUIRED IMPORTS ---
from pyspark.sql import DataFrame  
from pyspark.sql.types import StructType, StructField, IntegerType, StringType  
from pyspark.sql.functions import col, when, lit  
from pyspark.sql.utils import AnalysisException  
# SparkSession import/initialization commented out (Databricks provides 'spark')
from pyspark.sql import SparkSession
# spark = SparkSession.builder.getOrCreate()

# -- CONFIG SECTION: Schema and table names --
PRODUCT_PLANT_TABLE = "default.product_plant"
PRODUCT_PLANT_V2_TABLE = "default.product_plant_v2"
VALIDATION_TABLE = "default.pp_validation_results"

JOIN_KEY = ["id"]  # Columns to join on -- must be present in BOTH tables!

# -- UTILITY FUNCTION: LOGGING --
def log_info(msg):
    """Prints informational log messages to stdout for tracking."""
    print("[INFO]", msg)

# -- FUNCTION: Load Table with Safety Checks --
def load_table_or_fail(table: str) -> DataFrame:
    """
    Loads a Spark SQL table into a DataFrame, or raises an error if not found.
    Arguments:
        table: str - The table name with schema (e.g., 'default.product_plant')
    Returns:
        df: DataFrame - The loaded DataFrame
    """
    try:
        return spark.table(table)
    except AnalysisException as e:
        raise RuntimeError(f"Table {table} not found: {str(e)}")

# -- FUNCTION: Validate Presence of Join Key in DataFrame --
def assert_join_key(df: DataFrame, key_cols: list, table_name: str):
    """
    Checks if all join keys exist in the given DataFrame.
    Arguments:
        df: DataFrame - DataFrame to check
        key_cols: list - List of key column names to check
        table_name: str - For error message
    Returns: None, raises RuntimeError if missing
    """
    missing = [k for k in key_cols if k not in df.columns]
    if missing:
        raise RuntimeError(f"Join key(s) {missing} missing in {table_name}")

# -- FUNCTION: Get Common Columns (Excluding Join Keys) --
def get_common_columns(df1: DataFrame, df2: DataFrame, skip_cols: list = None) -> list:
    """
    Identifies columns present in both DataFrames, skipping the given ones.
    Arguments:
        df1: DataFrame
        df2: DataFrame
        skip_cols: list (optional) - Columns to exclude from comparison
    Returns:
        common: list - List of column names present in both (and not in skip_cols)
    """
    if skip_cols is None:
        skip_cols = []
    common = [
        c for c in df1.columns if c in df2.columns and c not in skip_cols
    ]
    return common

# -- FUNCTION: Generate Validation Expressions for a Column Triplet --
def validation_exprs(
    col_name: str,
    pp_alias: str,
    ppv2_alias: str
):
    """
    For given column, returns:
     - product_plant.col alias
     - product_plant_v2.col alias
     - validation column (Match/Mismatch)
    Arguments:
        col_name: str - name of the column (as in both DataFrames)
        pp_alias: str - DataFrame alias for product_plant (e.g., 'pp')
        ppv2_alias: str - DataFrame alias for product_plant_v2 (e.g., 'ppv2')
    Returns:
        (col_expr_pp, col_expr_ppv2, validation_col_expr)
    """
    col_pp = col(f"{pp_alias}.{col_name}").alias(f"product_plant_{col_name}")
    col_ppv2 = col(f"{ppv2_alias}.{col_name}").alias(f"product_plant_v2_{col_name}")
    col_validation = (
        when(
            (col(f"{pp_alias}.{col_name}").isNull()) & (col(f"{ppv2_alias}.{col_name}").isNull()),
            lit("Match")
        ).when(
            col(f"{pp_alias}.{col_name}") == col(f"{ppv2_alias}.{col_name}"),
            lit("Match")
        ).otherwise(lit("Mismatch")).alias(f"{col_name}_validation")
    )
    return col_pp, col_ppv2, col_validation

# -- MAIN FUNCTION: Compare Tables and Write Results --
def compare_and_write_validation():
    """
    Loads product_plant and product_plant_v2, joins on JOIN_KEY,
    performs column-wise value comparison for all common columns except join keys.
    Writes results to VALIDATION_TABLE with columns:
    [product_plant.col, product_plant_v2.col, col_validation, ...] in order per column,
    and re-creates validation table each execution.
    Arguments: None
    Returns: None
    """
    # --- Load tables ---
    log_info("Loading input tables...")
    pp_df = load_table_or_fail(PRODUCT_PLANT_TABLE)
    ppv2_df = load_table_or_fail(PRODUCT_PLANT_V2_TABLE)

    # --- Validate join key(s) present ---
    assert_join_key(pp_df, JOIN_KEY, PRODUCT_PLANT_TABLE)
    assert_join_key(ppv2_df, JOIN_KEY, PRODUCT_PLANT_V2_TABLE)

    # --- Only consider join key columns present in BOTH tables ---
    joined_key_cols = [k for k in JOIN_KEY if (k in pp_df.columns and k in ppv2_df.columns)]

    # -- Get columns for validation (excluding join keys) --
    common_columns = get_common_columns(pp_df, ppv2_df, skip_cols=JOIN_KEY)
    if not common_columns:
        raise RuntimeError(
            f"No common columns to validate between {PRODUCT_PLANT_TABLE} and {PRODUCT_PLANT_V2_TABLE} (other than join keys)"
        )

    log_info(f"Columns to validate: {common_columns}")
    log_info(f"Using join key(s): {joined_key_cols}")

    # -- Alias for clarity & column separation --
    pp = pp_df.alias("pp")
    ppv2 = ppv2_df.alias("ppv2")

    # -- Join on join key(s), inner join per requirements --
    log_info("Performing inner join on join key(s)...")
    join_cond = [col(f"pp.{k}") == col(f"ppv2.{k}") for k in joined_key_cols]
    joined = pp.join(ppv2, on=join_cond, how="inner")

    # -- Build select expression list respecting output column order (triplets for each col) --
    all_select_exprs = []
    # Add each join key as triplet with validation
    for k in joined_key_cols:
        all_select_exprs.extend(validation_exprs(k, "pp", "ppv2"))
    # For each common column (excluding join keys), add triplets
    for colname in common_columns:
        all_select_exprs.extend(validation_exprs(colname, "pp", "ppv2"))

    log_info("Selecting and building validation columns...")
    validation_df = joined.select(*all_select_exprs)

    # --- Recreate/destroy output table before saving ---
    log_info(f"Dropping existing validation table: {VALIDATION_TABLE}")
    spark.sql(f"DROP TABLE IF EXISTS {VALIDATION_TABLE}")

    log_info("Writing validation results to new table with overwrite mode...")
    validation_df.write.mode("overwrite").saveAsTable(VALIDATION_TABLE)

    log_info("Data validation success: table written.")

# -- EXECUTE MAIN LOGIC --
try:
    compare_and_write_validation()
except Exception as e:
    log_info(f"Validation process failed: {str(e)}")
    raise

# -- END OF SCRIPT --
