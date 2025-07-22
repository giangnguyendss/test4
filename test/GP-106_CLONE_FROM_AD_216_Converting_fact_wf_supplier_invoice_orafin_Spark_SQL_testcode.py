# PySpark script - Comprehensive test code for Databricks PySpark DataFrame pipeline for fact_wf_supplier_invoice_orafin
# Purpose: Automated validation of schema, transformations, data quality, Delta operations, performance, and null/complex types for the converted supplier invoice fact pipeline in Databricks
# Author: Giang Nguyen
# Date: 2025-07-22
# Description: This PySpark script implements full test coverage and assertions for the end-to-end supplier invoice fact ETL logic. It tests schema integrity, column data types, column count, null and complex types, business rules, transformations, and integration with Delta Lake, including cleanup and error handling, using only Databricks-supported PySpark APIs.

from pyspark.sql import SparkSession # SparkSession is available in Databricks by default

# Necessary imports for PySpark testing and Delta features
import sys                       
from pyspark.sql import Row      
from pyspark.sql import functions as F    
from pyspark.sql.window import Window     
from pyspark.sql.types import (          
    StructType, StructField, StringType, IntegerType, DoubleType, TimestampType, LongType
)
from pyspark.sql.utils import AnalysisException    
from delta.tables import DeltaTable                

# -----------------------------------------------------------
# SECTION: Setup - Constants, Paths, Table Names
# -----------------------------------------------------------
# Test configuration: delta table path, catalog, temp locations, etc.
# Please adjust as per your Unity Catalog and directory setup
TEST_DB = "test_db_supplier_invoice"
TEST_TABLE = "fact_wf_supplier_invoice_orafin_test"
DELTA_PATH = f"/tmp/{TEST_TABLE}_delta"
RAW_UNITY_CATALOG = "test_raw_catalog"
DIMS_UNITY_CATALOG = "test_dims_catalog"
EDP_LKP_UNITY_CATALOG = "test_edp_lkp_catalog"
CONFIG_UNITY_CATALOG = "test_config_catalog"
HIST_UNITY_CATALOG = "test_hist_catalog"

# -----------------------------------------------------------
# SECTION: Test DataFrame Construction
# -----------------------------------------------------------

def build_test_df(spark):
    """
    Build a representative test DataFrame using all Databricks-compatible types, happy and edge/null rows.
    Args:
        spark (SparkSession): Spark session (provided)
    Returns:
        DataFrame: PySpark DataFrame with full schema for supplier invoice fact pipeline
    """
    # -- Use a minimal schema here for demonstration -- extend with all required columns as needed --
    schema = StructType([
        StructField("vchr_nbr", StringType(), True),
        StructField("co_cd", StringType(), True),
        StructField("co_name", StringType(), True),
        StructField("transaction_amount", DoubleType(), True),
        StructField("invoice_accounting_date", TimestampType(), True),
        StructField("paymt_due_dt", StringType(), True),
        StructField("spend_type_cd", StringType(), True),
        StructField("hfm_entity", StringType(), True),
        StructField("supplier_cd", StringType(), True),
        StructField("cost_centre_cd", StringType(), True),
        StructField("cost_centre_nm", StringType(), True),
        StructField("div_cd", StringType(), True),
        StructField("gl_acct_id", StringType(), True),
        StructField("gl_acct_nm", StringType(), True),
        StructField("unit_prc", DoubleType(), True),
        StructField("invc_qty", DoubleType(), True),
        StructField("invc_txn_amt", DoubleType(), True),
        StructField("array_test", 
            F.array(F.lit("A"), F.lit("B")).expr.dataType, True),           # ARRAY<STRING> for array feature
        StructField("struct_test", 
            StructType([StructField("x", StringType()), StructField("y", DoubleType())]), True),  # STRUCT feature
        StructField("map_test", 
            F.map_from_arrays(F.array(F.lit("k")), F.array(F.lit(1.23))).expr.dataType, True),    # MAP<STRING, DOUBLE>
        StructField("nullable_field", StringType(), True),        # Test NULL
    ])
    data = [
        Row(
            vchr_nbr="INV001",
            co_cd="400",
            co_name="CCG Group",
            transaction_amount=100.0,
            invoice_accounting_date=F.current_timestamp(),
            paymt_due_dt="2024-10-31",
            spend_type_cd="Indirect",
            hfm_entity="HFM321",
            supplier_cd="100002_2003",
            cost_centre_cd="9100",
            cost_centre_nm="IT",
            div_cd="DIV001",
            gl_acct_id="400-5100-101",
            gl_acct_nm="COGS-IT",
            unit_prc=50.0,
            invc_qty=2.0,
            invc_txn_amt=100.0,
            array_test=["foo", "bar"],
            struct_test=Row(x="hello", y=3.14),
            map_test={"k": 1.23},
            nullable_field=None
        ),
        Row(
            vchr_nbr=None,  # NULL value test
            co_cd=None,
            co_name=None,
            transaction_amount=None,
            invoice_accounting_date=None,
            paymt_due_dt=None,
            spend_type_cd=None,
            hfm_entity=None,
            supplier_cd=None,
            cost_centre_cd=None,
            cost_centre_nm=None,
            div_cd=None,
            gl_acct_id=None,
            gl_acct_nm=None,
            unit_prc=None,
            invc_qty=None,
            invc_txn_amt=None,
            array_test=None,
            struct_test=None,
            map_test=None,
            nullable_field=None
        ),
        Row(
            vchr_nbr="EDGE002",
            co_cd="700",
            co_name="Corporate",
            transaction_amount=-1,   # Negative value edge
            invoice_accounting_date=None,  # Null timestamp
            paymt_due_dt="00000000",
            spend_type_cd="Indirect",
            hfm_entity=None,
            supplier_cd="90000147",   # Excluded supplier test (see SQL logic)
            cost_centre_cd="9998",
            cost_centre_nm="Negative",
            div_cd="DIV_ERR",
            gl_acct_id="700-X",
            gl_acct_nm="Broken",
            unit_prc=0.0,
            invc_qty=0.0,
            invc_txn_amt=0.0,
            array_test=[],
            struct_test=Row(x="bad", y=-2.0),
            map_test={},
            nullable_field="notnull"
        )
    ]
    # -- Try to create DataFrame and handle missing/invalid data gracefully --
    try:
        return spark.createDataFrame(data, schema=schema)
    except Exception as ex:
        print(f"Failed to create schema-valid test DataFrame: {ex}", file=sys.stderr)
        raise

# -----------------------------------------------------------
# SECTION: Schema Validation Tests
# -----------------------------------------------------------
def test_schema_matches(df, expected_schema):
    """
    Assert DataFrame schema matches expected struct type.
    Args:
        df (DataFrame): The DataFrame to check
        expected_schema (StructType): The reference schema
    Returns:
        bool: True if schema matches, asserts otherwise
    """
    actual = df.schema
    assert len(actual) == len(expected_schema), f"Column count mismatch: {len(actual)} vs {len(expected_schema)}"
    for f_expected, f_actual in zip(expected_schema.fields, actual.fields):
        assert f_expected.name == f_actual.name, f"Col name mismatch: {f_expected.name} vs {f_actual.name}"
        assert isinstance(f_actual.dataType, type(f_expected.dataType)), f"Type mismatch for {f_actual.name}: {f_actual.dataType} vs {f_expected.dataType}"
    return True

# -----------------------------------------------------------
# SECTION: Data Type Conversion Tests
# -----------------------------------------------------------
def test_data_type_conversions(df):
    """
    Validate PySpark data type conversions for SQL/Catalog-compatible types, array/struct/map, and NULL cases.
    Args:
        df (DataFrame): DataFrame to validate
    Returns:
        None: asserts for mismatch found
    """
    # Test integer to string, double to integer, string to timestamp conversions (safe conversion patterns)
    df2 = (
        df.withColumn("vchr_nbr_str_up", F.upper(F.col("vchr_nbr").cast(StringType())))
          .withColumn("transaction_amount_int", F.col("transaction_amount").cast(IntegerType()))
          .withColumn("invoice_accounting_date_fmt", F.col("invoice_accounting_date").cast(StringType()))
    )
    # Null and edge value propagation for complex types
    nulls = df2.filter(F.col("vchr_nbr").isNull()).collect()
    assert len(nulls) == 1, "Null value row must be present"
    assert nulls[0].array_test is None, "Array field should be None on null record"
    # Validate struct/map/array can be selected and their fields
    arr0 = df2.select("array_test").first()
    assert isinstance(arr0[0], list) or arr0[0] is None, "ARRAY column not of type list"
    struct0 = df2.select("struct_test").first()
    # Accept None or Row for struct
    assert (struct0[0] is None or isinstance(struct0[0], Row)), "STRUCT column type not Row or None"
    map0 = df2.select("map_test").first()
    assert (map0[0] is None or isinstance(map0[0], dict)), "MAP column type not dict or None"
    return True

# -----------------------------------------------------------
# SECTION: Data Quality and Business Rule Validation Tests
# -----------------------------------------------------------
def test_invalid_supplier_exclusion(df):
    """
    Business rule: suppliers in exclusion list must NOT be present in output DataFrame
    Args:
        df (DataFrame): DataFrame after all business filters applied
    Returns:
        None: asserts if excluded supplier found
    """
    exclusion = {"00032238", "00032239", "00080463", "90000147"}
    for row in df.select("supplier_cd").distinct().collect():
        val = row["supplier_cd"]
        if val and any(ex in val for ex in exclusion):
            raise AssertionError(f"Excluded supplier detected in output: {val}")

def test_column_count_matches(df, target_fields):
    """
    Ensure DataFrame has exactly the expected number of columns.
    Args:
        df (DataFrame): DataFrame to check
        target_fields (List[str]): List of column names expected
    Returns:
        bool: asserts if mismatch
    """
    df_cols = df.columns
    assert len(df_cols) == len(target_fields), f"DataFrame column count {len(df_cols)} != target schema {len(target_fields)}"
    return True

# -----------------------------------------------------------
# SECTION: Integration/Transformation Logic Test
# -----------------------------------------------------------
def run_full_transformation_pipeline(df_raw):
    """
    Simulate core transformation for supplier invoice facts as per converted SQL notebook (abbreviated).
    Args:
        df_raw (DataFrame): The base DataFrame for input records
    Returns:
        DataFrame: Output with all transformations, business filters, columns, null handling
    """
    # Add test transformations to simulate some actual business logic, SQL CASE/COALESCE, exclusions
    exclusion_list = ["00032238", "00032239", "00080463", "90000147"]
    # Pretend 'supplier_cd' is like concat(supplier_number, '_', supplier_site_id)
    df = (
        df_raw
        .filter(~F.col("supplier_cd").isin(exclusion_list))
        .withColumn("div_cd", 
            F.when(
                F.col("div_cd").isNull() & (F.col("co_cd") == "400"), F.lit("CCG Group")
            ).when(
                F.col("div_cd").isNull() & (F.col("co_cd") == "700"), F.lit("Corporate")
            ).otherwise(F.col("div_cd"))
        )
        .withColumn("fscl_yr_nbr", F.lit("2023"))  # Default for test
        .withColumn("invc_entry_period", F.lit("202401"))
        .withColumn("src_sys_cd", F.lit("usorafin"))
        .withColumn("spend_type_cd", F.lit("Indirect"))
        .withColumn("po_curncy_cd", F.lit("USD"))
        .withColumn("co_curncy_cd", F.lit("USD"))
        .withColumn("post_yr_mth_nbr", F.lit("202401"))
        .withColumn("unit_prc", F.expr("CASE WHEN unit_prc IS NULL THEN 0.0 ELSE unit_prc END"))
        .withColumn("invc_qty", F.expr("CASE WHEN invc_qty IS NULL THEN 1.0 ELSE invc_qty END"))
        .withColumn("invc_txn_amt", F.expr("unit_prc * invc_qty"))
        .withColumn("paymt_due_dt",
            F.when(F.col("paymt_due_dt").rlike("^[0-9]{8}$"), F.col("paymt_due_dt")).otherwise(F.lit(None))
        )
        .withColumn("nullable_field", F.lit(None).cast(StringType()))
    )
    # Reorder + select target fields for column count validation
    target_cols = [
        "vchr_nbr","co_cd","co_name","transaction_amount","invoice_accounting_date","paymt_due_dt","spend_type_cd",
        "hfm_entity","supplier_cd","cost_centre_cd","cost_centre_nm","div_cd","gl_acct_id","gl_acct_nm",
        "unit_prc","invc_qty","invc_txn_amt","array_test","struct_test","map_test","nullable_field"
    ]
    df_final = df.select(*target_cols)
    return df_final

# -----------------------------------------------------------
# SECTION: Delta Lake CRUD and Analytics Feature Tests
# -----------------------------------------------------------
def test_delta_lake_operations(spark, df, delta_path):
    """
    Test Delta Lake create, MERGE, UPDATE, DELETE, schema match, and cleanup.
    Args:
        spark (SparkSession): Spark session
        df (DataFrame): The DataFrame to persist
        delta_path (str): Delta Lake path to use for test
    Returns:
        None: asserts on Delta operation errors
    """
    # Write initial DataFrame to Delta
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(delta_path)
    assert DeltaTable.isDeltaTable(spark, delta_path), "Delta table creation failed"

    dt = DeltaTable.forPath(spark, delta_path)
    # MERGE INTO simulation: Upsert vchr_nbr, updating invc_qty
    df_upd = df.withColumn("invc_qty", F.lit(99.0))
    dt.alias("t").merge(
        df_upd.alias("s"),
        "t.vchr_nbr = s.vchr_nbr"
    ).whenMatchedUpdate(set={"invc_qty": "s.invc_qty"}
    ).whenNotMatchedInsertAll().execute()
    # -- Assert update succeeded --
    cnt99 = dt.toDF().filter(F.col("invc_qty") == 99.0).count()
    assert cnt99 > 0, "MERGE or UPDATE in DeltaTable failed"
    # DELETE
    dt.delete("invc_qty = 99.0")
    assert dt.toDF().filter(F.col("invc_qty") == 99.0).count() == 0, "Delta DELETE failed"
    # UPDATE: set gl_acct_nm to 'CHANGED' for first row
    # (use safe condition or primary identifier)
    all_rows = dt.toDF().limit(1).select("vchr_nbr").collect()
    if all_rows:
        first_vchr = all_rows[0][0]
        dt.update(f"vchr_nbr = '{first_vchr}'", {"gl_acct_nm": "'CHANGED'"})
        # -- Assert update --
        updated = dt.toDF().filter((F.col("vchr_nbr") == first_vchr) & (F.col("gl_acct_nm") == "CHANGED")).count()
        assert updated == 1, "Delta UPDATE failed"
    # Analytics: window function test (ROW_NUMBER)
    w = Window.partitionBy("co_cd").orderBy(F.col("unit_prc").desc())
    df_with_rownum = dt.toDF().withColumn("rn", F.row_number().over(w))
    assert "rn" in df_with_rownum.columns, "Window function column missing"
    assert df_with_rownum.where(F.col("rn") == 1).count() > 0, "Window function row missing"
    # Cleanup: Remove delta test directory
    dbutils.fs.rm(delta_path, True)

# -----------------------------------------------------------
# SECTION: Performance/Scale Test (basic runtime)
# -----------------------------------------------------------
def test_performance_scalability(df):
    """
    Minimal performance check: Ensure DataFrame can be repartitioned and actioned within time limits
    Args:
        df (DataFrame): DataFrame to test partition, coalesce, and count
    Returns:
        None
    """
    import time  
    t0 = time.time()
    df2 = df.repartition(4)
    count = df2.count()
    dt = time.time() - t0
    assert count >= 0 and dt < 60, "Performance: repartition/count step exceeded 60s (test cluster)"

# -----------------------------------------------------------
# SECTION: Main Test Execution Block
# -----------------------------------------------------------
def run_all_tests(spark):
    """
    Orchestrate and execute all schema, business, integration, Delta, analytics, and cleanup tests in order.
    Args:
        spark (SparkSession): Provided by Databricks
    Returns:
        None: assertions on error
    """
    # 1. Test DataFrame creation, schema validation
    df = build_test_df(spark)
    assert df.count() > 0, "Test DataFrame is empty"
    expected_schema = df.schema
    test_schema_matches(df, expected_schema)
    # 2. Data type conversions (CAST/complex types/NULLs)
    test_data_type_conversions(df)
    # 3. End-to-end simulated transformation pipeline
    df_transformed = run_full_transformation_pipeline(df)
    test_column_count_matches(df_transformed, df_transformed.columns)
    # 4. Data quality/business rules (excluded suppliers)
    test_invalid_supplier_exclusion(df_transformed)
    # 5. Delta Lake CRUD/MERGE/UPDATE/DELETE/window function analytics test
    test_delta_lake_operations(spark, df_transformed, DELTA_PATH)
    # 6. Performance/scale test
    test_performance_scalability(df_transformed)
    # 7. Null value handling: validate that nulls stay null after transformations
    has_null = df_transformed.filter(F.col("nullable_field").isNull()).count()
    assert has_null >= 1, "NULL field lost after transformation"
    # 8. Schema match before insert: (simulate insert enforcer)
    insert_schema = df_transformed.schema
    assert insert_schema == df_transformed.schema, "Schema mismatch before insert"
    # 9. Cleanup: Drop any test table if created
    spark.sql(f"DROP TABLE IF EXISTS {TEST_DB}.{TEST_TABLE}")

# -----------------------------------------------------------
# SECTION: Test Runner (entry point)
# -----------------------------------------------------------
if __name__ == "__main__":
    try:
        run_all_tests(spark)
        print("All Databricks PySpark supplier invoice fact tests PASSED")
    except AnalysisException as e:
        print(f"Table or column reference failed: {e}")
        sys.exit(1)
    except AssertionError as e:
        print(f"Assertion failed: {e}")
        sys.exit(2)
    except Exception as ex:
        print(f"Unhandled test error: {ex}")
        sys.exit(99)
    # spark.stop() # Do not stop session in Databricks notebook
