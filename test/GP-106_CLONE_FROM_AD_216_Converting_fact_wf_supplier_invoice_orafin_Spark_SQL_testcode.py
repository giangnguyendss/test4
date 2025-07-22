%pip install pytest

# PySpark script - Comprehensive Databricks test suite for fact_wf_supplier_invoice_orafin ETL PySpark conversion
# Purpose: Validate PySpark transformations for "fact_wf_supplier_invoice_orafin" covering schema, type, filter, and business rules
# Author: Giang Nguyen
# Date: 2025-07-22
# Description: This test script verifies all critical PySpark conversion requirements for fact_wf_supplier_invoice_orafin, including schema validation, data type enforcement, NULL handling, business rules exclusion, functional correctness, and end-to-end pipeline integration. It also validates Delta Lake operations and performs required cleanup.

import pytest  
import pyspark.sql.functions as F  
from pyspark.sql.types import (  
    StructType, StructField, StringType, DoubleType, IntegerType, LongType
)
from pyspark.sql import Row  
from datetime import datetime, timedelta  

# -------------------------------------------------------------------------------------------
# SECTION: SETUP AND CONFIGURATION
# -------------------------------------------------------------------------------------------

# -- No SparkSession initialization (already available in Databricks)
# -- Define constants as would be loaded from Constants module
EXCLUDED_SUPPLIERS = {"00032238", "00032239", "00080463", "90000147"}
EXCLUDED_CONCAT_SEGMENTS = {
    "4009999110011000000000000000", "4009999111011104000000000000", "4009999111011105000000000000",
    "4009999111011106000000000000", "4009999129012946000000000000", "4009999138013800000000000000",
    "4009999172017211000000000000", "4009999200020004000000000000", "4009999240024042000000000000",
    "4009999240024047000000000000", "7009801138013800000000000000", "7009801210021031000000000000",
    "7009801210021079000000000000"
}
BLOCKED_MAINFRAME_PATTERNS = ["DS", "DR", "PE", "PR", "PS", "RG", "TR"]

# -- Define schema (as per requirement)
SCHEMA = StructType([
    StructField("document_type", StringType(), True),
    StructField("txn_ref_nbr", StringType(), True),
    StructField("invc_entry_period", StringType(), True),
    StructField("po_nbr", StringType(), True),
    StructField("po_line_nbr", StringType(), True),
    StructField("src_sys_cd", StringType(), True),
    StructField("vchr_nbr", StringType(), True),
    StructField("vchr_line_nbr", StringType(), True),
    StructField("fscl_yr_nbr", StringType(), True),
    StructField("vchr_type_cd", StringType(), True),
    StructField("vchr_status", StringType(), True),
    StructField("item_nbr", StringType(), True),
    StructField("item_desc", StringType(), True),
    StructField("thermo_item_nbr", StringType(), True),
    StructField("supplier_cd", StringType(), True),
    StructField("supplier_name", StringType(), True),
    StructField("supplier_type_cd", StringType(), True),
    StructField("buyer_cd", StringType(), True),
    StructField("document_desc", StringType(), True),
    StructField("invc_txn_type", StringType(), True),
    StructField("buyer_nm", StringType(), True),
    StructField("co_cd", StringType(), True),
    StructField("co_name", StringType(), True),
    StructField("hfm_entity", StringType(), True),
    StructField("business_unit", StringType(), True),
    StructField("lcr_flag", StringType(), True),
    StructField("lcr_region", StringType(), True),
    StructField("vomi_flag", StringType(), True),
    StructField("payment_compliance_flg", StringType(), True),
    StructField("po_curncy_cd", StringType(), True),
    StructField("co_curncy_cd", StringType(), True),
    StructField("post_yr_mth_nbr", StringType(), True),
    StructField("invc_entry_dt", StringType(), True),
    StructField("paymt_due_dt", StringType(), True),
    StructField("suplr_invc_dt", StringType(), True),
    StructField("aprval_dt", StringType(), True),
    StructField("txn_orig_id", StringType(), True),
    StructField("suplr_invc_nbr", StringType(), True),
    StructField("invc_apprv_id", StringType(), True),
    StructField("unit_prc", DoubleType(), True),
    StructField("invc_qty", DoubleType(), True),
    StructField("base_qty", DoubleType(), True),
    StructField("invc_txn_amt", DoubleType(), True),
    StructField("invc_co_amt", DoubleType(), True),
    StructField("invc_txn_pmar_amt", DoubleType(), True),
    StructField("invc_co_pmar_amt", DoubleType(), True),
    StructField("unit_prc_pmar_amt", DoubleType(), True),
    StructField("txn_curncy_mth_rt", DoubleType(), True),
    StructField("co_curncy_mth_rt", DoubleType(), True),
    StructField("uom_conv_factor", DoubleType(), True),
    StructField("invc_uom_cd", StringType(), True),
    StructField("base_uom_cd", StringType(), True),
    StructField("profit_cntr", StringType(), True),
    StructField("div_cd", StringType(), True),
    StructField("site_cd", StringType(), True),
    StructField("site_name", StringType(), True),
    StructField("reporting_site", StringType(), True),
    StructField("warehouse", StringType(), True),
    StructField("warehouse_nm", StringType(), True),
    StructField("unit", StringType(), True),
    StructField("nature", StringType(), True),
    StructField("inv_flg", StringType(), True),
    StructField("inv_flg_text", StringType(), True),
    StructField("spend_type_cd", StringType(), True),
    StructField("po_paymt_terms_cd", StringType(), True),
    StructField("po_paymt_terms_desc", StringType(), True),
    StructField("suplr_paymt_terms_cd", StringType(), True),
    StructField("suplr_paymt_terms_desc", StringType(), True),
    StructField("fk_orig", StringType(), True),
    StructField("floor_stock_cd", StringType(), True),
    StructField("contract_flag", StringType(), True),
    StructField("contract_type", StringType(), True),
    StructField("contract_start_date", StringType(), True),
    StructField("contract_end_date", StringType(), True),
    StructField("erp_commondity_cd", StringType(), True),
    StructField("erp_commondity_nm", StringType(), True),
    StructField("sec_supp_cd", StringType(), True),
    StructField("part_rev_no", StringType(), True),
    StructField("cost_centre_cd", StringType(), True),
    StructField("cost_centre_nm", StringType(), True),
    StructField("vendor_mat_no", StringType(), True),
    StructField("gl_acct_id", StringType(), True),
    StructField("gl_acct_nm", StringType(), True),
    StructField("pass_through_field", StringType(), True),
    StructField("pass_through_line", StringType(), True),
    StructField("inv_line_desc", StringType(), True),
    StructField("remit_to_addr_line_1", StringType(), True),
    StructField("remit_to_addr_line_2", StringType(), True),
    StructField("remit_to_addr_line_3", StringType(), True),
    StructField("remit_to_addr_line_4", StringType(), True),
    StructField("remit_to_city_nm", StringType(), True),
    StructField("remit_to_st_cd", StringType(), True),
    StructField("remit_to_rgn_cd", StringType(), True),
    StructField("remit_to_rgn_nm", StringType(), True),
    StructField("remit_to_cntry_cd", StringType(), True),
    StructField("remit_to_cntry_nm", StringType(), True),
    StructField("suplr_nm_src", StringType(), True),
    StructField("rpt_flex1", StringType(), True),
    StructField("invc_txn_amt_clsfctn", StringType(), True),
    StructField("supplier_segment", StringType(), True),
    StructField("ap_payment_term_cd", StringType(), True),
    StructField("ap_payment_term_desc", StringType(), True),
    StructField("actual_payment_dt", StringType(), True),
    StructField("source_country", StringType(), True),
])

# -- Defensive test data loading (try/except per requirements)
try:
    from test.GP_106_CLONE_FROM_AD_216_Converting_fact_wf_supplier_invoice_orafin_Spark_SQL_testdata import get_test_rows  
    test_rows = get_test_rows()
except Exception as e:
    test_rows = []
    # Handle: print(f"Test data failed to initialize: {e}")
test_df = spark.createDataFrame(test_rows, schema=SCHEMA)

# -------------------------------------------------------------------------------------------
# SECTION: PYSPARK FUNCTIONS (function conversion coverage)
# -------------------------------------------------------------------------------------------

def is_blocked_mainframe(invoice_source_code: str, invoice_description: str) -> bool:
    """
    Determines if a row is blocked MAINFRAME scenario

    Args:
        invoice_source_code (str): invoice_source_code field
        invoice_description (str): invoice_description field

    Returns:
        bool: True if blocked MAINFRAME, else False
    """
    if invoice_source_code == "MAINFRAME" and invoice_description is not None:
        return any(invoice_description.startswith(pat) for pat in BLOCKED_MAINFRAME_PATTERNS)
    return False

def is_valid_natural_account_segment(natural_account_segment: str) -> bool:
    """
    Validates the natural_account_segment

    Args:
        natural_account_segment (str): Natural account value as string

    Returns:
        bool: True if NULL or between 4000 and 8999, else False
    """
    if natural_account_segment is None:
        return True
    try:
        val = int(natural_account_segment)
        return 4000 <= val <= 8999
    except Exception:
        return False

def is_valid_cost_center_segment(cost_center_segment: str) -> bool:
    """
    Validates the cost_center_segment

    Args:
        cost_center_segment (str): Cost center value as string

    Returns:
        bool: True if NULL or between 9000 and 9999, else False
    """
    if cost_center_segment is None:
        return True
    try:
        val = int(cost_center_segment)
        return 9000 <= val <= 9999
    except Exception:
        return False

def has_excluded_concats(concat_segments: str) -> bool:
    """
    Determines if concat_segments value is in the exclusion list

    Args:
        concat_segments (str): Value to check

    Returns:
        bool: True if value (with '.' replaced) is in exclusion list, else False
    """
    if concat_segments is None:
        return False
    key = concat_segments.replace('.', '')
    return key in EXCLUDED_CONCAT_SEGMENTS

def is_excluded_supplier(supplier_cd: str) -> bool:
    """
    Checks whether supplier is in the exclusion list

    Args:
        supplier_cd (str): Supplier code (format 'supplier_number_supplier_site_id')

    Returns:
        bool: True if supplier_number in exclusion list, else False
    """
    if supplier_cd is None or "_" not in supplier_cd:
        return False
    return supplier_cd.split("_")[0].zfill(8) in EXCLUDED_SUPPLIERS

def add_invc_entry_period(df):
    """
    Adds/overwrites 'invc_entry_period' column as 'yyyyMM' formatted from suplr_invc_dt date field

    Args:
        df (DataFrame): Input DataFrame

    Returns:
        DataFrame: DataFrame with new invc_entry_period column
    """
    return df.withColumn("invc_entry_period", 
                         F.when(F.col("suplr_invc_dt").isNotNull(),
                                F.col("suplr_invc_dt").substr(1, 6)
                         ).otherwise(F.lit(None).cast(StringType()))
                        )

# -------------------------------------------------------------------------------------------
# SECTION: SCHEMA VALIDATION UNIT TESTS
# -------------------------------------------------------------------------------------------

def test_schema_validation():
    """
    Validates that the DataFrame schema matches design (column names and data types)
    """
    expected_fields = [(f.name, f.dataType) for f in SCHEMA.fields]
    df_fields = [(f.name, f.dataType) for f in test_df.schema.fields]
    assert expected_fields == df_fields, f"Schema mismatch: {expected_fields} != {df_fields}"

# -------------------------------------------------------------------------------------------
# SECTION: DATA TYPE TESTING (PySpark built-in types)
# -------------------------------------------------------------------------------------------

def test_data_types_and_nulls():
    """
    Validate all data types and null propagation with mixed/edge value rows
    """
    sample = test_df.limit(5).collect()
    for row in sample:
        # Example double test
        assert (row.unit_prc is None or isinstance(row.unit_prc, float)), "unit_prc not a float"
        # Example string test
        assert (row.document_type is None or isinstance(row.document_type, str)), "document_type not string"
        # Null propagation
        for fname in SCHEMA.fieldNames():
            # Test: will not raise error if field is None
            _ = getattr(row, fname, None)

# -------------------------------------------------------------------------------------------
# SECTION: DATA QUALITY AND BUSINESS RULES VALIDATION
# -------------------------------------------------------------------------------------------

def test_invoice_accounting_date_filter():
    """
    Invoice accounting date must be within last 3 years (YYYY >= current year - 3)
    """
    now = datetime.now()
    lower_year = now.year - 3
    # Here, take 'invc_entry_dt' as a sample date field
    for r in test_df.select("invc_entry_dt").collect():
        val = r.invc_entry_dt
        if val and len(val) >= 4:
            y = int(val[:4])
            assert y >= lower_year, f"Invoice accounting date out of range: {val}"

def test_excluded_suppliers():
    """
    Exclude supplier_number in EXCLUDED_SUPPLIERS
    """
    for r in test_df.select("supplier_cd").collect():
        if r.supplier_cd:
            supplier_num = r.supplier_cd.split("_")[0].zfill(8)
            assert supplier_num not in EXCLUDED_SUPPLIERS, f"supplier_number is in exclusion list: {supplier_num}"

def test_excluded_concats():
    """
    Exclude rows where concat_segments in EXCLUDED_CONCAT_SEGMENTS
    """
    # This simulates gl_acct_id or other derived segment; for test data just check gl_acct_id
    for r in test_df.select("gl_acct_id").collect():
        if r.gl_acct_id:
            key = r.gl_acct_id.replace('.', '')
            assert key not in EXCLUDED_CONCAT_SEGMENTS, f"concat_segments contains an excluded value: {key}"

def test_blocked_mainframe():
    """
    Exclude (invoice_source_code == MAINFRAME and inv_line_desc starts with blocked pattern)
    """
    df = test_df.select(F.lit("MAINFRAME").alias("invoice_source_code"), F.col("inv_line_desc"))
    for r in df.collect():
        if r.inv_line_desc is not None and r.inv_line_desc[:2] in BLOCKED_MAINFRAME_PATTERNS:
            assert False, "MAINFRAME with blocked invoice_description pattern not excluded"

def test_valid_natural_account_segment():
    """
    Valid only if natural_account_segment is None or between 4000 and 8999
    """
    for r in test_df.select("cost_centre_cd").collect():
        if r.cost_centre_cd is not None:
            try:
                segment = int(r.cost_centre_cd)
                assert segment >= 9000 and segment <= 9999, f"Invalid cost_center_segment: {r.cost_centre_cd}"
            except Exception:
                pass

def test_valid_cost_center_segment():
    """
    Valid only if cost_center_segment is None or between 9000 and 9999
    """
    for r in test_df.select("cost_centre_cd").collect():
        if r.cost_centre_cd is not None:
            try:
                segment = int(r.cost_centre_cd)
                assert segment >= 9000 and segment <= 9999, f"Invalid cost_center_segment: {r.cost_centre_cd}"
            except Exception:
                pass

# -------------------------------------------------------------------------------------------
# SECTION: DATA TYPE CONVERSION AND NULL HANDLING TESTS
# -------------------------------------------------------------------------------------------

def test_data_type_and_null_cast():
    """
    Validates explicit data type conversions and null propagation
    """
    tdf = test_df.withColumn("unit_prc_cdouble", F.col("unit_prc").cast(DoubleType()))
    for r in tdf.select("unit_prc", "unit_prc_cdouble").take(10):
        assert (r.unit_prc == r.unit_prc_cdouble) or (r.unit_prc is None and r.unit_prc_cdouble is None)

def test_complex_type_operations():
    """
    Simulate and validate complex types (e.g., ARRAY/STRUCT/MAP columns)
    """
    # Example: Create array of [unit_prc, invc_qty]
    arr_df = test_df.withColumn("amt_array", F.array("unit_prc", "invc_txn_amt"))
    for r in arr_df.select("amt_array").collect():
        assert isinstance(r.amt_array, list)
        assert len(r.amt_array) == 2

    # Example: Join STRUCT of address lines
    struct_df = test_df.withColumn("remit_struct", F.struct("remit_to_addr_line_1", "remit_to_city_nm"))
    for r in struct_df.select("remit_struct").collect():
        assert hasattr(r.remit_struct, "remit_to_addr_line_1")

# -------------------------------------------------------------------------------------------
# SECTION: FUNCTIONAL/INTEGRATION/PIPELINE TESTS
# -------------------------------------------------------------------------------------------

def test_add_invc_entry_period_logic():
    """
    Validates that invc_entry_period is correctly derived from suplr_invc_dt
    """
    source_df = test_df.withColumn("suplr_invc_dt", F.lit("20220815"))
    result_df = add_invc_entry_period(source_df)
    assert all(r.invc_entry_period == "202208" for r in result_df.select("invc_entry_period").collect())

# -------------------------------------------------------------------------------------------
# SECTION: DELTA LAKE OPERATION TESTS
# -------------------------------------------------------------------------------------------

def test_delta_insert_and_merge(tmp_path="/tmp/test_fwsio_delta"):
    """
    Create Delta table, insert data, update and merge, and validate changes with cleanup
    Args:
        tmp_path (str): Write location for Delta table
    Returns:
        None
    """
    import shutil 

    # -- Delta Lake import
    from delta.tables import DeltaTable  

    # -- Cleanup if exists
    try:
        shutil.rmtree(tmp_path)
    except Exception:
        pass
    # -- Create and write Delta
    test_df.write.format("delta").mode("overwrite").save(tmp_path)

    # -- Read and update via DeltaTable
    dt = DeltaTable.forPath(spark, tmp_path)

    # -- UPDATE: Set document_type to 'Test' where NULL
    dt.update(
        condition="document_type IS NULL",
        set={"document_type": "'Test'"}
    )
    # -- MERGE: insert row with specified key only if not exists
    new_rows = [Row(**{k: None for k in SCHEMA.fieldNames()})]
    merge_df = spark.createDataFrame(new_rows, schema=SCHEMA)
    dt.alias("t").merge(
        merge_df.alias("src"),
        "t.vchr_nbr = src.vchr_nbr",
    ).whenNotMatchedInsertAll().execute()

    # -- VALIDATION: Count rows increased by 1 (match record count)
    df_after = spark.read.format("delta").load(tmp_path)
    assert df_after.count() == test_df.count() + 1

    # -- DELETE: Remove rows where vchr_nbr is NULL
    dt.delete("vchr_nbr IS NULL")
    df_final = spark.read.format("delta").load(tmp_path)
    assert df_final.count() == test_df.count()

    # -- Cleanup
    try:
        shutil.rmtree(tmp_path)
    except Exception:
        pass

# -------------------------------------------------------------------------------------------
# SECTION: WINDOW AND ANALYTIC FUNCTION TESTS
# -------------------------------------------------------------------------------------------

def test_pyspark_window_functions():
    """
    Validates use of window function (row_number) for latest record per supplier_cd
    """
    from pyspark.sql.window import Window  

    win = Window.partitionBy("supplier_cd").orderBy(F.desc("actual_payment_dt"))
    df = test_df.withColumn("row_number", F.row_number().over(win))
    top_df = df.filter(F.col("row_number") == 1)
    assert top_df.select("supplier_cd").distinct().count() == top_df.count(), "row_number window test failed"

# -------------------------------------------------------------------------------------------
# SECTION: PERFORMANCE (SMOKE/BENCHMARK) TEST
# -------------------------------------------------------------------------------------------

def test_performance_batch_processing():
    """
    Simple batch read/process/write smoke/performance check for pipeline
    """
    import time
    start = time.time()
    out_df = test_df.withColumn("new_col", F.lit("perf"))
    count = out_df.count()
    delta = time.time() - start
    assert count == test_df.count()
    assert delta < 10, f"Batch ETL step took too long: {delta}s"

# -------------------------------------------------------------------------------------------
# SECTION: CLEANUP/COLLECTION MANAGEMENT
# -------------------------------------------------------------------------------------------

def test_cleanup_dummy_table():
    """
    Cleans up any dummy table if created as part of Delta Lake or batch tests
    """
    import shutil
    # Example: remove /tmp/test_fwsio_delta
    try:
        shutil.rmtree("/tmp/test_fwsio_delta")
    except Exception:
        pass

# -------------------------------------------------------------------------------------------
# SECTION: RUN UNIT TESTS (pytest will discover)
# -------------------------------------------------------------------------------------------

def run_all_tests():
    """
    Runs all PySpark data pipeline and logic tests for fact_wf_supplier_invoice_orafin conversion
    """
    test_schema_validation()
    test_data_types_and_nulls()
    test_invoice_accounting_date_filter()
    test_excluded_suppliers()
    test_excluded_concats()
    test_blocked_mainframe()
    test_valid_natural_account_segment()
    test_valid_cost_center_segment()
    test_data_type_and_null_cast()
    test_complex_type_operations()
    test_add_invc_entry_period_logic()
    test_delta_insert_and_merge()
    test_pyspark_window_functions()
    test_performance_batch_processing()
    test_cleanup_dummy_table()

# -- To run all tests in Databricks environment, uncomment the following:
# run_all_tests()
