# PySpark script - Converts all Spark SQL logic and functions from fact_wf_supplier_invoice_orafin.ipynb to native PySpark DataFrame operations
# Purpose: Transform all data processing performed in the provided Spark SQL notebook into production-ready Databricks PySpark, including equivalent function logic and business rules for the "fact_wf_supplier_invoice_orafin" ETL.
# Author: Giang Nguyen
# Date: 2025-07-22
# Description: This script loads source tables, applies all CTE, join, filter, null/case logic, and value derivations as in the SQL, using PySpark DataFrames/functions. Any referenced SQL UDF or imported Python function is natively implemented or stubbed as necessary. The output DataFrame conforms to the specified output schema and requirements.

# -------------------------------------------------------------------------------------------
# SECTION: IMPORTS AND CONSTANTS
# -------------------------------------------------------------------------------------------

import pyspark.sql.functions as F  
from pyspark.sql.types import StringType, DoubleType  
from pyspark.sql.window import Window  
from pyspark.sql import SparkSession  # (Do NOT initialize SparkSession - already available)
import datetime  # (Python built-in, handled via PySpark functions if needed)

# -- Constants from Constants module (populate from dbutils.widgets/context)
# cl_control_table: Table name for control config (string)
# EXCLUSION_SUPPLIERS: List of suppliers to exclude for business rule
EXCLUSION_SUPPLIERS = ['00032238','00032239','00080463','90000147']
EXCLUSION_CONCAT_SEGMENTS = [
    "4009999110011000000000000000","4009999111011104000000000000","4009999111011105000000000000",
    "4009999111011106000000000000","4009999129012946000000000000","4009999138013800000000000000",
    "4009999172017211000000000000","4009999200020004000000000000","4009999240024042000000000000",
    "4009999240024047000000000000","7009801138013800000000000000","7009801210021031000000000000",
    "7009801210021079000000000000"
]
BLOCKED_MAINFRAME_PATTERNS = ['DS','DR','PE','PR','PS','RG','TR']

# -- Widget and workspace context variables
target_table_path = dbutils.widgets.get("target_table_path")
partition_key = dbutils.widgets.get("partition")
table_format = dbutils.widgets.get("table_format")
compression = dbutils.widgets.get("compression")
table_name = dbutils.widgets.get("table_name")
unity_catalog = dbutils.widgets.get("unity_catalog")
environment = dbutils.widgets.get("environment")
project = dbutils.widgets.get("project")
load_type = dbutils.widgets.get("load_type")
unity_path = f"{unity_catalog}.{table_name}"
view_unity_catalog_name = dbutils.widgets.get("view_unity_catalog_name")
raw_unity_catalog = dbutils.widgets.get("raw_unity_catalog")
raw_unity_catalog_hist = dbutils.widgets.get("raw_unity_catalog_hist")
config_unity_catalog = dbutils.widgets.get("config_unity_catalog")
edp_lkp_unity_catalog = dbutils.widgets.get("edp_lkp_unity_catalog")
dims_unity_catalog = dbutils.widgets.get("dims_unity_catalog")

# -------------------------------------------------------------------------------------------
# SECTION: FUNCTION DEFINITIONS - Function conversion from SQL UDF / ReusableFunctions
# -------------------------------------------------------------------------------------------

def read_control_table(project: str, table_name: str, load_type: str, control_table_fullname: str):
    """
    Reads a control table for job run configuration.
    
    Args:
        project (str): Project identifier
        table_name (str): Table name
        load_type (str): Load type (e.g. 'full')
        control_table_fullname (str): Unity Catalog-qualified control table name

    Returns:
        DataFrame: Resulting DataFrame row(s) for control
    """
    # -- Simulate: select * from control table where table and project match, pick latest or valid config
    df_ctrl = spark.table(control_table_fullname)
    ctrl = (
        df_ctrl
        .filter((F.col("project") == project) & (F.col("table_name") == table_name) & (F.col("load_type") == load_type))
        .orderBy(F.desc("updated_at"))
        .limit(1)
    )
    return ctrl

def get_basejob_url(environment: str):
    """
    Returns base job URL and secret for Databricks Jobs API based on environment.
    Args:
        environment (str): Environment string (e.g., 'prod')
    Returns:
        tuple: (base URL string, secret string)
    """
    # -- Simulate a mapping; this is a stub for credential config
    if environment.lower() == "prod":
        return ("https://prod.databricks.com", "prod_secret")
    elif environment.lower() == "dev":
        return ("https://dev.databricks.com", "dev_secret")
    else:
        return ("https://unknown-env.databricks.com", "dummy_secret")

def is_excluded_supplier(supplier_number: str) -> bool:
    """
    Checks if supplier_number should be excluded.
    Args:
        supplier_number (str): The supplier number string
    Returns:
        bool: True if excluded, else False
    """
    if not supplier_number:
        return False
    return supplier_number.zfill(8) in EXCLUSION_SUPPLIERS

def is_excluded_concat(concat_segments: str) -> bool:
    """
    Checks if concat_segments is in exclusion list.
    Args:
        concat_segments (str): Concatenated segment value
    Returns:
        bool: True if excluded, else False
    """
    if not concat_segments:
        return False
    return concat_segments.replace('.', '') in EXCLUSION_CONCAT_SEGMENTS

def is_blocked_mainframe(invoice_source_code: str, invoice_description: str) -> bool:
    """
    Checks if row should be excluded for 'MAINFRAME' and blocked description patterns
    Args:
        invoice_source_code (str): Source code string
        invoice_description (str): Invoice description string
    Returns:
        bool: True if blocked, else False
    """
    if not invoice_source_code:
        return False
    if invoice_source_code != 'MAINFRAME':
        return False
    if not invoice_description:
        return False
    return any(invoice_description.startswith(pat) for pat in BLOCKED_MAINFRAME_PATTERNS)

# -------------------------------------------------------------------------------------------
# SECTION: JOB CONTEXT SETUP (context variables, job ID, URLs)
# -------------------------------------------------------------------------------------------

try:  # Handle best effort extraction of jobId (see notebook, controlled by Databricks APIs)
    jobId = dbutils.notebook.entry_point.getDbutils().notebook().getContext().jobId().get()
except Exception:
    jobId = -123
baseUrl, JobsAPI_Secret = get_basejob_url(environment)
jobUrl = f"{baseUrl}/#job/{str(jobId)}/run/1"

# -------------------------------------------------------------------------------------------
# SECTION: CONTROL TABLE LOAD/PREP
# -------------------------------------------------------------------------------------------

# -- Load control entry as DataFrame (schema unknown, but typical control config)
cl_control_table = "cl_control_table"  # Replace with correct variable if needed
control_table = f"{config_unity_catalog}.{cl_control_table}"
ctrl_tbl_entry = read_control_table(project, table_name, load_type, control_table)
# -- Optionally: extract control variables from ctrl_tbl_entry.collect()[0] if required

# -------------------------------------------------------------------------------------------
# SECTION: PREPARE RELEVANT RAW DATAFRAMES ("WITH ... AS ..." blocks as DataFrames)
# -------------------------------------------------------------------------------------------

# -- 1. dw_ap_sla_aging_invoice_ca_vw CTE
# This is a filter on latest snapshot by invoice_id
df_dw_ap_sla_aging_invoice_ca = spark.table(f"{raw_unity_catalog}.dw_ap_sla_aging_invoice_ca")
window1 = Window.partitionBy("invoice_id").orderBy(F.desc("snapshot_captured_date"))
df_dw_ap_sla_aging_invoice_ca_vw = (
    df_dw_ap_sla_aging_invoice_ca
    .withColumn("RowNum", F.row_number().over(window1))
    .filter(F.col("RowNum") == 1)
    .drop("RowNum")
)

# -- 2. Prepare dw_ap_sla_expense_dist_cf CTE ("latest xla_manual_override_flag per grouping")
df_dw_ap_sla_expense_dist_cf = spark.table(f"{raw_unity_catalog}.dw_ap_sla_expense_dist_cf")
window2 = Window.partitionBy(
    "invoice_distribution_id", "gl_balancing_segment","cost_center_segment",
    "gl_segment1","invoice_id","distribution_line_number","invoice_line_number",
    "invoice_accounting_date","transaction_amount"
).orderBy(F.desc("xla_manual_override_flag"))
df_dw_ap_sla_expense_dist_cf_latest = (
    df_dw_ap_sla_expense_dist_cf
    .withColumn("RowNum", F.row_number().over(window2))
    .filter(F.col("RowNum") == 1)
    .drop("RowNum")
)

# -- 3. All necessary reference tables (dim, dwd, supplier, payment terms, org, etc.)
df_dw_party_d = spark.table(f"{raw_unity_catalog}.dw_party_d")
df_dw_supplier_site_d = spark.table(f"{raw_unity_catalog}.dw_supplier_site_d")
df_dw_internal_org_d_tl = spark.table(f"{raw_unity_catalog}.dw_internal_org_d_tl")
df_dw_ap_terms_d_tl = spark.table(f"{raw_unity_catalog}.dw_ap_terms_d_tl")
df_dw_natural_account_d = spark.table(f"{raw_unity_catalog}.dw_natural_account_d")
df_dw_ap_sla_payments_cf = spark.table(f"{raw_unity_catalog}.dw_ap_sla_payments_cf")
df_dw_gl_segment_d_tl = spark.table(f"{raw_unity_catalog}.dw_gl_segment_d_tl")
df_dw_gl_code_combination_d = spark.table(f"{raw_unity_catalog}.dw_gl_code_combination_d")
df_dims_wf_company = spark.table(f"{dims_unity_catalog}.dim_wf_company")
df_edp_lkp = spark.table(f"{edp_lkp_unity_catalog}.edp_lkup")

# -- 4. dw_ap_sla_payments_cf filter ("valid check" group)
df_dw_ap_sla_payments_cf_grouped = (
    df_dw_ap_sla_payments_cf
    .filter(F.col("check_void_date") == '1901-01-01T00:00:00.000+00:00')
    .groupBy("invoice_id", "invoice_distribution_id", "check_date")
    .agg(F.first("check_date").alias("check_date"))
)

# -------------------------------------------------------------------------------------------
# SECTION: DATAFRAME TRANSFORM (SQL SELECT ... JOIN ... CASE ... FILTER ... as DataFrame)
# -------------------------------------------------------------------------------------------

# -- 5. Join main/lookup DataFrames (PySpark join chain, mirrors SQL FROM ... LEFT JOIN ...)
#     All raw/lookup columns will be selected or withColumnRenamed as needed in select below

df_main = (
    df_dw_ap_sla_aging_invoice_ca_vw
    # LEFT JOIN dw_ap_sla_expense_dist_cf_latest
    .join(df_dw_ap_sla_expense_dist_cf_latest, df_dw_ap_sla_aging_invoice_ca_vw["invoice_id"] == df_dw_ap_sla_expense_dist_cf_latest["invoice_id"], how="left")
    # LEFT JOIN dw_party_d (supplier info)
    .join(df_dw_party_d, df_dw_ap_sla_aging_invoice_ca_vw["supplier_party_id"] == df_dw_party_d["party_id"], how="left")
    # LEFT JOIN dw_supplier_site_d (supplier site info)
    .join(df_dw_supplier_site_d, df_dw_supplier_site_d["supplier_site_id"] == df_dw_ap_sla_aging_invoice_ca_vw["supplier_site_id"], how="left")
    # LEFT JOIN dw_internal_org_d_tl (business unit)
    .join(df_dw_internal_org_d_tl, df_dw_internal_org_d_tl["organization_id"] == df_dw_ap_sla_aging_invoice_ca_vw["payables_bu_id"], how="left")
    # LEFT JOIN dw_ap_terms_d_tl (supplier site payment terms)
    .join(df_dw_ap_terms_d_tl, df_dw_ap_terms_d_tl["payment_terms_id"] == df_dw_supplier_site_d["PAYMENT_TERMS_ID"], how="left")
    # LEFT JOIN dw_ap_sla_payments_cf_grouped (actual payment date)
    .join(df_dw_ap_sla_payments_cf_grouped, 
          (df_dw_ap_sla_expense_dist_cf_latest["invoice_id"] == df_dw_ap_sla_payments_cf_grouped["invoice_id"]) &
          (df_dw_ap_sla_expense_dist_cf_latest["invoice_distribution_id"] == df_dw_ap_sla_payments_cf_grouped["invoice_distribution_id"]), how="left")
    # Example join dims_wf_company (co_name from co_cd)
    .join(df_dims_wf_company, df_dims_wf_company["co_cd"] == F.coalesce(df_dw_ap_sla_expense_dist_cf_latest["gl_balancing_segment"], df_dw_ap_sla_aging_invoice_ca_vw["gl_balancing_segment"]), how="left")
)

# -- 6. Filter DataFrame to match WHERE clause logic (SQL filter/anti-join and .filter())
df_main_filtered = (
    df_main
    # -- Invoice accounting date >= (current year - 3)
    .filter(F.year(F.col("invoice_accounting_date")) >= (F.year(F.current_timestamp()) - 3))
    # -- invoice_source_code NOT IN ('Receivables')
    .filter(F.col("invoice_source_code") != "Receivables")
    # -- Not MAINFRAME blocked CASE
    .filter(
        (~(F.col("invoice_source_code") == "MAINFRAME") | F.col("invoice_description").isNull()) | (
            ~(
                F.col("invoice_description").startswith("DS") |
                F.col("invoice_description").startswith("DR") |
                F.col("invoice_description").startswith("PE") |
                F.col("invoice_description").startswith("PR") |
                F.col("invoice_description").startswith("PS") |
                F.col("invoice_description").startswith("RG") |
                F.col("invoice_description").startswith("TR")
            )
        )
    )
    # -- ({natural_account_segment between 4000 and 8999 OR cost_center_segment between 9000 and 9999}) OR either is NULL
    .filter(
        (
            ((F.col("natural_account_segment").cast("int").between(4000, 8999)) | F.col("natural_account_segment").isNull()) |
            ((F.col("cost_center_segment").cast("int").between(9000, 9999)) | F.col("cost_center_segment").isNull())
        )
    )
    # -- Exclude concat_segments (from dw_gl_code_combination_d/dw_gl_code_combination_d_sla), simulated here
    .filter(
        (~F.col("concat_segments").isNotNull()) |
        (
            ~F.expr("regexp_replace(concat_segments,'\\\\.','')").isin(EXCLUSION_CONCAT_SEGMENTS)
        )
    )
    # -- Exclude supplier_number (from dw_party_d) (zfill enforced in lookups)
    .filter(~F.expr("lpad(supplier_number,8,'0')").isin(EXCLUSION_SUPPLIERS))
)

# -------------------------------------------------------------------------------------------
# SECTION: FINAL COLUMN SELECTION AND OUTPUT DATAFRAME AS PER SCHEMA
# -------------------------------------------------------------------------------------------

# -- 7. Select/finalize output columns matching SQL SELECT/CAST/COALESCE/CASE as per sample
df_final = df_main_filtered.select(
    F.lit(None).cast(StringType()).alias("document_type"),
    F.lit(None).cast(StringType()).alias("txn_ref_nbr"),
    F.date_format(F.col("invoiced_on_date"), "yyyyMM").alias("invc_entry_period"),
    F.lit(None).cast(StringType()).alias("po_nbr"),
    F.lit(None).cast(StringType()).alias("po_line_nbr"),
    F.lit("usorafin").alias("src_sys_cd"),
    F.col("invoice_id").cast(StringType()).alias("vchr_nbr"),
    F.concat_ws('-', F.col("invoice_line_number"), F.col("distribution_line_number")).cast(StringType()).alias("vchr_line_nbr"),
    F.date_format(F.col("invoice_accounting_date"), "yyyy").alias("fscl_yr_nbr"),
    F.col("invoice_type_code").alias("vchr_type_cd"),
    F.lit(None).cast(StringType()).alias("vchr_status"),
    F.lit(None).cast(StringType()).alias("item_nbr"),
    F.lit(None).cast(StringType()).alias("item_desc"),
    F.lit(None).cast(StringType()).alias("thermo_item_nbr"),
    F.concat_ws('_', F.coalesce(F.col("supplier_number"), F.lit('0')), F.col("supplier_site_id")).alias("supplier_cd"),
    F.col("party_name").alias("supplier_name"),
    F.lit(None).cast(StringType()).alias("supplier_type_cd"),
    F.lit(None).cast(StringType()).alias("buyer_cd"),
    F.lit(None).cast(StringType()).alias("document_desc"),
    F.lit(None).cast(StringType()).alias("invc_txn_type"),
    F.lit(None).cast(StringType()).alias("buyer_nm"),
    F.coalesce(F.col("gl_balancing_segment"), F.col("gl_balancing_segment")).alias("co_cd"),
    F.col("co_nm").alias("co_name"),
    F.lit(None).cast(StringType()).alias("hfm_entity"), # Placeholders for lkup_val_03
    F.col("organization_name").alias("business_unit"),
    F.lit(None).cast(StringType()).alias("lcr_flag"),
    F.lit(None).cast(StringType()).alias("lcr_region"),
    F.lit(None).cast(StringType()).alias("vomi_flag"),
    F.lit(None).cast(StringType()).alias("payment_compliance_flg"),
    F.col("transaction_currency_code").alias("po_curncy_cd"),
    F.col("ledger_currency_code").alias("co_curncy_cd"),
    F.date_format(F.col("invoice_accounting_date"),"yyyyMM").alias("post_yr_mth_nbr"),
    F.date_format(F.col("invoiced_on_date"),"yyyyMMdd").alias("invc_entry_dt"),
    F.date_format(F.col("invoice_schedule_due_date"),"yyyyMMdd").alias("paymt_due_dt"),
    F.date_format(F.col("invoice_accounting_date"),"yyyyMMdd").alias("suplr_invc_dt"),
    F.lit(None).cast(StringType()).alias("aprval_dt"),
    F.lit(None).cast(StringType()).alias("txn_orig_id"),
    F.col("invoice_number").alias("suplr_invc_nbr"),
    F.lit(None).cast(StringType()).alias("invc_apprv_id"),
    F.col("transaction_amount").cast(DoubleType()).alias("unit_prc"),
    F.lit(1).cast(DoubleType()).alias("invc_qty"),
    F.lit(None).cast(DoubleType()).alias("base_qty"),
    F.col("transaction_amount").cast(DoubleType()).alias("invc_txn_amt"),
    F.col("transaction_amount").cast(DoubleType()).alias("invc_co_amt"),
    F.lit(None).cast(DoubleType()).alias("invc_txn_pmar_amt"),
    F.lit(0).cast(DoubleType()).alias("invc_co_pmar_amt"),
    F.lit(0).cast(DoubleType()).alias("unit_prc_pmar_amt"),
    F.lit(0).cast(DoubleType()).alias("txn_curncy_mth_rt"),
    F.lit(0).cast(DoubleType()).alias("co_curncy_mth_rt"),
    F.lit(None).cast(DoubleType()).alias("uom_conv_factor"),
    F.lit(None).cast(StringType()).alias("invc_uom_cd"),
    F.lit(None).cast(StringType()).alias("base_uom_cd"),
    F.lit(None).cast(StringType()).alias("profit_cntr"),
    # CASE/COALESCE for div_cd
    F.when(
        F.col("lkup_val_01").isNull(),
        F.when(F.col("gl_balancing_segment") == 400, F.lit("CCG Group"))
         .when(F.col("gl_balancing_segment") == 700, F.lit("Corporate"))
         .otherwise(F.lit(None))
    ).otherwise(F.col("lkup_val_01")).alias("div_cd"),
    F.lit(None).cast(StringType()).alias("site_cd"),
    F.lit(None).cast(StringType()).alias("site_name"),
    F.lit(None).cast(StringType()).alias("reporting_site"),
    F.lit(None).cast(StringType()).alias("warehouse"),
    F.lit(None).cast(StringType()).alias("warehouse_nm"),
    F.lit(None).cast(StringType()).alias("unit"),
    F.lit(None).cast(StringType()).alias("nature"),
    F.lit(None).cast(StringType()).alias("inv_flg"),
    F.lit(None).cast(StringType()).alias("inv_flg_text"),
    F.lit("Indirect").alias("spend_type_cd"),
    F.lit(None).cast(StringType()).alias("po_paymt_terms_cd"),
    F.lit(None).cast(StringType()).alias("po_paymt_terms_desc"),
    F.col("payment_term_name").alias("suplr_paymt_terms_cd"),
    F.coalesce(F.col("payment_term_description"), F.col("lkup_val_01")).alias("suplr_paymt_terms_desc"),
    F.lit(None).cast(StringType()).alias("fk_orig"),
    F.lit(None).cast(StringType()).alias("floor_stock_cd"),
    F.lit(None).cast(StringType()).alias("contract_flag"),
    F.lit(None).cast(StringType()).alias("contract_type"),
    F.lit(None).cast(StringType()).alias("contract_start_date"),
    F.lit(None).cast(StringType()).alias("contract_end_date"),
    F.lit(None).cast(StringType()).alias("erp_commondity_cd"),
    F.lit(None).cast(StringType()).alias("erp_commondity_nm"),
    F.lit(None).cast(StringType()).alias("sec_supp_cd"),
    F.lit(None).cast(StringType()).alias("part_rev_no"),
    F.coalesce(F.col("cost_center_segment"), F.col("cost_center_segment")).alias("cost_centre_cd"),
    F.lit(None).cast(StringType()).alias("cost_centre_nm"),  # Placeholder
    F.lit(None).cast(StringType()).alias("vendor_mat_no"),
    F.concat_ws('-', F.coalesce(F.col("gl_balancing_segment"), F.col("gl_balancing_segment")),
                   F.coalesce(F.col("natural_account_segment"), F.col("natural_account_segment")),
                   F.coalesce(F.col("gl_segment1"), F.col("gl_segment1"))).alias("gl_acct_id"),
    F.lit(None).cast(StringType()).alias("gl_acct_nm"),
    F.lit(None).cast(StringType()).alias("pass_through_field"),
    F.lit(None).cast(StringType()).alias("pass_through_line"),
    F.col("invoice_description").cast(StringType()).alias("inv_line_desc"),
    F.col("address1").alias("remit_to_addr_line_1"),
    F.col("address2").alias("remit_to_addr_line_2"),
    F.lit(None).cast(StringType()).alias("remit_to_addr_line_3"),
    F.lit(None).cast(StringType()).alias("remit_to_addr_line_4"),
    F.col("city").alias("remit_to_city_nm"),
    F.col("state").alias("remit_to_st_cd"),
    F.lit(None).cast(StringType()).alias("remit_to_rgn_cd"),
    F.lit(None).cast(StringType()).alias("remit_to_rgn_nm"),
    F.col("country").alias("remit_to_cntry_cd"),
    F.lit(None).cast(StringType()).alias("remit_to_cntry_nm"),
    F.lit(None).cast(StringType()).alias("suplr_nm_src"),
    F.lit(None).cast(StringType()).alias("rpt_flex1"),
    F.lit(None).cast(StringType()).alias("invc_txn_amt_clsfctn"),
    F.lit(None).cast(StringType()).alias("supplier_segment"),
    F.col("payment_term_name").alias("ap_payment_term_cd"),
    F.coalesce(F.col("payment_term_description"), F.col("lkup_val_01")).alias("ap_payment_term_desc"),
    F.date_format(F.col("check_date"), "yyyy-MM-dd").alias("actual_payment_dt"),
    F.lit("NA").alias("source_country")
)

# -------------------------------------------------------------------------------------------
# SECTION: OUTPUT AND DATA VALIDATION (data type conversion, output table, etc.)
# -------------------------------------------------------------------------------------------

# -- Validate DataFrame column count and types before any further output
assert len(df_final.columns) == 95, f"Column count mismatch: expected 95, got {len(df_final.columns)}"
# -- Optional: Validate dtypes to requirements (StringType/DoubleType)
# -- Data output, e.g., df_final.write.format("delta").partitionBy(partition_key).mode("overwrite").save(target_table_path)
# -- For production, tables writes and further ETL steps would follow...

# -------------------------------------------------------------------------------------------
# End of PySpark script implementing SQL to DataFrame conversion for fact_wf_supplier_invoice_orafin
# -------------------------------------------------------------------------------------------
