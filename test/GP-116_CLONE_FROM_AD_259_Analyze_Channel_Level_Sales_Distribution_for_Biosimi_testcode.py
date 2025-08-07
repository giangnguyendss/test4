spark.catalog.setCurrentCatalog("purgo_databricks")

# /* 
# Databricks PySpark Test Suite for Channel-wise Sales Performance Analysis for Biosimilar Brands
# 
# - Catalog: purgo_databricks
# - Schema: purgo_playground
# - Table: bai_sales_agg_obu_customer_datapack
# 
# This test suite covers:
#   - Data type validation
#   - Filtering logic (brand, cdl_effective_date, null/invalid handling)
#   - Aggregation and grouping
#   - Output schema and sorting
#   - Data quality and edge cases
#   - Delta Lake operations (if applicable)
#   - Cleanup
# 
# Assumptions:
#   - 'spark' session is available
#   - Test data is loaded into DataFrame 'df' as per the provided test data script
#   - Reference date for 36-month filter is provided as 'reference_cdl_effective_date'
#   - Only brands: "kanjinti", "mvasi", "riabni" are included
#   - cdl_effective_date is a string in "yyyy-MM-dd" format
#   - Null/invalid cdl_effective_date records are excluded
#   - Null sales metrics are treated as zero in aggregation
#   - Output is sorted by all grouping columns in ascending order
#   - All code is Databricks-compatible and uses only supported PySpark features
# */

# ---------------------------
# /* SECTION: Imports & Setup */
# ---------------------------

# from pyspark.sql import SparkSession  # SparkSession is already available in Databricks
from pyspark.sql import functions as F  
from pyspark.sql.types import (         
    StructType, StructField, StringType, BooleanType, TimestampType, LongType
)
from datetime import datetime           

# ---------------------------
# /* SECTION: Test Parameters */
# ---------------------------

# Reference date for 36-month filter (can be parameterized in real test)
reference_cdl_effective_date = "2023-01-01"

# Allowed brands
allowed_brands = ["kanjinti", "mvasi", "riabni"]

# ---------------------------
# /* SECTION: Helper Functions */
# ---------------------------

def is_valid_date(date_str):
    # Helper to check if a string is a valid yyyy-MM-dd date
    try:
        datetime.strptime(date_str, "%Y-%m-%d")
        return True
    except Exception:
        return False

# UDF for date validation (for DataFrame filtering)
from pyspark.sql.functions import udf  
is_valid_date_udf = udf(is_valid_date, BooleanType())

# ---------------------------
# /* SECTION: Data Preparation */
# ---------------------------

# Assume 'df' is already loaded as per the provided test data script

# Add a column for valid cdl_effective_date (yyyy-MM-dd)
df_valid = df.withColumn(
    "cdl_effective_date_valid",
    (F.col("cdl_effective_date").isNotNull()) &
    (F.length(F.col("cdl_effective_date")) == 10) &
    is_valid_date_udf(F.col("cdl_effective_date"))
)

# Filter for valid cdl_effective_date only
df_valid = df_valid.filter(F.col("cdl_effective_date_valid") == True)

# Parse cdl_effective_date as date
df_valid = df_valid.withColumn(
    "cdl_effective_date_parsed",
    F.to_date(F.col("cdl_effective_date"), "yyyy-MM-dd")
)

# Parse reference date
reference_date_lit = F.lit(reference_cdl_effective_date)
reference_date_col = F.to_date(reference_date_lit, "yyyy-MM-dd")

# Calculate months difference between reference and cdl_effective_date
df_valid = df_valid.withColumn(
    "months_diff",
    F.abs(F.months_between(reference_date_col, F.col("cdl_effective_date_parsed")))
)

# Filter for allowed brands and within 36 months
df_filtered = df_valid.filter(
    (F.col("brand_normalized_name").isin(allowed_brands)) &
    (F.col("months_diff") <= 36)
)

# Treat null sales metrics as zero (already handled in test data, but ensure here)
df_filtered = df_filtered.withColumn(
    "integrated_units", F.coalesce(F.col("integrated_units"), F.lit(0).cast(LongType()))
).withColumn(
    "integrated_normalized_units", F.coalesce(F.col("integrated_normalized_units"), F.lit(0).cast(LongType()))
).withColumn(
    "integrated_dollars", F.coalesce(F.col("integrated_dollars"), F.lit(0).cast(LongType()))
)

# ---------------------------
# /* SECTION: Aggregation Logic */
# ---------------------------

group_cols = [
    "brand_normalized_name",
    "normalized_name",
    "market_normalized_name",
    "competitor_flag",
    "channel_name"
]

agg_df = df_filtered.groupBy(*group_cols).agg(
    F.sum("integrated_units").alias("sum_integrated_units"),
    F.sum("integrated_normalized_units").alias("sum_integrated_normalized_units"),
    F.sum("integrated_dollars").alias("sum_integrated_dollars")
)

# Sort output as required
agg_df = agg_df.orderBy(
    "brand_normalized_name",
    "normalized_name",
    "market_normalized_name",
    "competitor_flag",
    "channel_name"
)

# ---------------------------
# /* SECTION: Schema Validation Tests */
# ---------------------------

expected_schema = StructType([
    StructField("brand_normalized_name", StringType(), True),
    StructField("normalized_name", StringType(), True),
    StructField("market_normalized_name", StringType(), True),
    StructField("competitor_flag", BooleanType(), True),
    StructField("channel_name", StringType(), True),
    StructField("sum_integrated_units", LongType(), True),
    StructField("sum_integrated_normalized_units", LongType(), True),
    StructField("sum_integrated_dollars", LongType(), True),
])

# Assert schema matches expected
actual_fields = [(f.name, f.dataType, f.nullable) for f in agg_df.schema.fields]
expected_fields = [(f.name, f.dataType, f.nullable) for f in expected_schema.fields]
assert actual_fields == expected_fields, f"Schema mismatch: {actual_fields} != {expected_fields}"

# Assert number of columns matches
assert len(agg_df.columns) == len(expected_schema.fields), "Column count mismatch"

# ---------------------------
# /* SECTION: Data Type Conversion Tests */
# ---------------------------

# Test that all sum columns are LongType (integer)
for colname in ["sum_integrated_units", "sum_integrated_normalized_units", "sum_integrated_dollars"]:
    assert dict(agg_df.dtypes)[colname] == "bigint", f"{colname} is not bigint"

# Test that all string columns are StringType
for colname in ["brand_normalized_name", "normalized_name", "market_normalized_name", "channel_name"]:
    assert dict(agg_df.dtypes)[colname] == "string", f"{colname} is not string"

# Test that competitor_flag is BooleanType
assert dict(agg_df.dtypes)["competitor_flag"] == "boolean", "competitor_flag is not boolean"

# ---------------------------
# /* SECTION: Data Quality & Filtering Tests */
# ---------------------------

# 1. Exclude records with null or invalid cdl_effective_date
invalid_cdl_dates = df.filter(
    (F.col("cdl_effective_date").isNull()) |
    (F.length(F.col("cdl_effective_date")) != 10) |
    (~is_valid_date_udf(F.col("cdl_effective_date")))
)
assert invalid_cdl_dates.count() > 0, "No invalid cdl_effective_date test records found"
for row in invalid_cdl_dates.collect():
    assert row["brand_normalized_name"] in allowed_brands or row["brand_normalized_name"] is None, "Test data error: invalid brand in invalid_cdl_dates"
assert not agg_df.filter(F.col("cdl_effective_date").isNull()).count(), "Null cdl_effective_date should be excluded"

# 2. Exclude records with brand not in allowed list
excluded_brands = df.filter(~F.col("brand_normalized_name").isin(allowed_brands))
assert excluded_brands.count() > 0, "No excluded brand test records found"
for row in excluded_brands.collect():
    assert row["brand_normalized_name"] not in allowed_brands, "Test data error: allowed brand in excluded_brands"
for brand in excluded_brands.select("brand_normalized_name").distinct().collect():
    assert not agg_df.filter(F.col("brand_normalized_name") == brand["brand_normalized_name"]).count(), f"Brand {brand['brand_normalized_name']} should be excluded"

# 3. Exclude records outside 36 month window
outside_window = df_valid.filter(
    (F.col("brand_normalized_name").isin(allowed_brands)) &
    (F.abs(F.months_between(reference_date_col, F.to_date(F.col("cdl_effective_date"), "yyyy-MM-dd"))) > 36)
)
for row in outside_window.collect():
    assert not agg_df.filter(
        (F.col("brand_normalized_name") == row["brand_normalized_name"]) &
        (F.col("normalized_name") == row["normalized_name"]) &
        (F.col("market_normalized_name") == row["market_normalized_name"]) &
        (F.col("competitor_flag") == row["competitor_flag"]) &
        (F.col("channel_name") == row["channel_name"])
    ).count(), f"Record outside 36 month window should be excluded: {row}"

# 4. Null sales metrics treated as zero
null_sales = df_filtered.filter(
    (F.col("integrated_units") == 0) |
    (F.col("integrated_normalized_units") == 0) |
    (F.col("integrated_dollars") == 0)
)
assert null_sales.count() > 0, "No null/zero sales test records found"

# 5. Duplicate records are summed
# For a known duplicate group, check sum equals sum of all duplicates
dup_group = {
    "brand_normalized_name": "kanjinti",
    "normalized_name": "trastuzumab",
    "market_normalized_name": "oncology",
    "competitor_flag": False,
    "channel_name": "online"
}
dup_rows = df_filtered.filter(
    (F.col("brand_normalized_name") == dup_group["brand_normalized_name"]) &
    (F.col("normalized_name") == dup_group["normalized_name"]) &
    (F.col("market_normalized_name") == dup_group["market_normalized_name"]) &
    (F.col("competitor_flag") == dup_group["competitor_flag"]) &
    (F.col("channel_name") == dup_group["channel_name"])
)
expected_sum_units = dup_rows.agg(F.sum("integrated_units")).collect()[0][0]
expected_sum_norm_units = dup_rows.agg(F.sum("integrated_normalized_units")).collect()[0][0]
expected_sum_dollars = dup_rows.agg(F.sum("integrated_dollars")).collect()[0][0]
agg_row = agg_df.filter(
    (F.col("brand_normalized_name") == dup_group["brand_normalized_name"]) &
    (F.col("normalized_name") == dup_group["normalized_name"]) &
    (F.col("market_normalized_name") == dup_group["market_normalized_name"]) &
    (F.col("competitor_flag") == dup_group["competitor_flag"]) &
    (F.col("channel_name") == dup_group["channel_name"])
).collect()
assert len(agg_row) == 1, "Duplicate group aggregation missing"
assert agg_row[0]["sum_integrated_units"] == expected_sum_units, "Sum of integrated_units for duplicate group incorrect"
assert agg_row[0]["sum_integrated_normalized_units"] == expected_sum_norm_units, "Sum of integrated_normalized_units for duplicate group incorrect"
assert agg_row[0]["sum_integrated_dollars"] == expected_sum_dollars, "Sum of integrated_dollars for duplicate group incorrect"

# 6. Output includes all present channels after filtering
present_channels = [row["channel_name"] for row in df_filtered.select("channel_name").distinct().collect()]
agg_channels = [row["channel_name"] for row in agg_df.select("channel_name").distinct().collect()]
for ch in present_channels:
    assert ch in agg_channels, f"Channel {ch} missing from output"

# 7. Output does not include groups with no records after filtering
# (No explicit test needed; groupBy/agg naturally omits empty groups)

# 8. Output is sorted as required
agg_pd = agg_df.toPandas()
sorted_pd = agg_pd.sort_values(
    by=["brand_normalized_name", "normalized_name", "market_normalized_name", "competitor_flag", "channel_name"],
    ascending=[True, True, True, True, True]
).reset_index(drop=True)
assert agg_pd.equals(sorted_pd), "Output is not sorted as required"

# 9. Output is empty if all records are excluded
df_empty = df.filter(F.lit(False))  # Empty DataFrame
df_empty_valid = df_empty.withColumn("cdl_effective_date_valid", F.lit(False))
df_empty_filtered = df_empty_valid.filter(F.lit(False))
agg_empty = df_empty_filtered.groupBy(*group_cols).agg(
    F.sum("integrated_units").alias("sum_integrated_units"),
    F.sum("integrated_normalized_units").alias("sum_integrated_normalized_units"),
    F.sum("integrated_dollars").alias("sum_integrated_dollars")
)
assert agg_empty.count() == 0, "Output should be empty when all records are excluded"

# 10. Special/multibyte characters are preserved
special_row = agg_df.filter(F.col("normalized_name").like("%™%") | F.col("market_normalized_name").like("%ó%") | F.col("normalized_name").like("%é%") | F.col("market_normalized_name").like("%肿%") | F.col("normalized_name").like("%и%")).collect()
assert len(special_row) > 0, "Special/multibyte character rows missing"

# ---------------------------
# /* SECTION: Delta Lake Operations (if applicable) */
# ---------------------------

# Test Delta Lake write and read (if table is Delta)
delta_test_path = "/tmp/purgo_playground_bai_sales_agg_obu_customer_datapack_test_delta"
try:
    # Write to Delta
    agg_df.write.format("delta").mode("overwrite").save(delta_test_path)
    # Read back
    delta_df = spark.read.format("delta").load(delta_test_path)
    # Validate schema and row count
    assert delta_df.count() == agg_df.count(), "Delta Lake row count mismatch"
    assert delta_df.schema == agg_df.schema, "Delta Lake schema mismatch"
finally:
    # Cleanup Delta test data
    import shutil  
    try:
        shutil.rmtree("/dbfs" + delta_test_path)
    except Exception:
        pass

# ---------------------------
# /* SECTION: Performance Test (Basic) */
# ---------------------------

import time  
start_time = time.time()
_ = agg_df.collect()
elapsed = time.time() - start_time
assert elapsed < 10, f"Aggregation took too long: {elapsed} seconds"

# ---------------------------
# /* SECTION: Output Display (for manual validation) */
# ---------------------------

agg_df.show(truncate=False)

# ---------------------------
# /* SECTION: Cleanup */
# ---------------------------

# No temp views or temp tables used, so no cleanup required

# /* END OF TEST SUITE */
