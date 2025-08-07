# /*
# Channel-wise Sales Performance Analysis for Biosimilar Brands (kanjinti, mvasi, riabni)
# 
# - Source Table: purgo_playground.bai_sales_agg_obu_customer_datapack
# - Catalog: purgo_databricks
# - Schema: purgo_playground
# 
# Requirements:
#   - Filter for brands: kanjinti, mvasi, riabni
#   - Filter for records with valid cdl_effective_date (yyyy-MM-dd), non-null, and within 36 months of a reference date
#   - Group by: brand_normalized_name, normalized_name, market_normalized_name, competitor_flag, channel_name
#   - Aggregate: sum of integrated_units, integrated_normalized_units, integrated_dollars (treat nulls as zero)
#   - Output: Sorted by all grouping columns ascending
#   - Data type validation and conversion
#   - Exclude records with invalid or null cdl_effective_date
#   - Exclude records with brand not in allowed list
#   - All code is Databricks PySpark native and production-ready
#   - No temp views or temp tables used
#   - No plain text output, all documentation in comments
# */

# ---------------------------
# /* SECTION: Imports & Setup */
# ---------------------------

# from pyspark.sql import SparkSession  # SparkSession is already available in Databricks
from pyspark.sql import functions as F  
from pyspark.sql.types import StringType, BooleanType, LongType, StructType, StructField  
from datetime import datetime  

# ---------------------------
# /* SECTION: Parameters */
# ---------------------------

# Reference date for 36-month filter (should be parameterized in production)
reference_cdl_effective_date = "2023-01-01"  # yyyy-MM-dd

# Allowed brands for analysis
allowed_brands = ["kanjinti", "mvasi", "riabni"]

# ---------------------------
# /* SECTION: Data Read */
# ---------------------------

# Set catalog and schema context
spark.catalog.setCurrentCatalog("purgo_databricks")
spark.catalog.setCurrentDatabase("purgo_playground")

# Read source table
source_table = "purgo_playground.bai_sales_agg_obu_customer_datapack"
df = spark.table(source_table)

# ---------------------------
# /* SECTION: Data Validation & Filtering */
# ---------------------------

# Helper UDF to check if a string is a valid yyyy-MM-dd date
def is_valid_date(date_str):
    try:
        datetime.strptime(date_str, "%Y-%m-%d")
        return True
    except Exception:
        return False

is_valid_date_udf = F.udf(is_valid_date, BooleanType())

# Add a column for valid cdl_effective_date (non-null, correct length, valid format)
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

# Parse reference date as column
reference_date_lit = F.lit(reference_cdl_effective_date)
reference_date_col = F.to_date(reference_date_lit, "yyyy-MM-dd")

# Calculate absolute months difference between reference and cdl_effective_date
df_valid = df_valid.withColumn(
    "months_diff",
    F.abs(F.months_between(reference_date_col, F.col("cdl_effective_date_parsed")))
)

# Filter for allowed brands and within 36 months window
df_filtered = df_valid.filter(
    (F.col("brand_normalized_name").isin(allowed_brands)) &
    (F.col("months_diff") <= 36)
)

# Treat null sales metrics as zero (for aggregation)
df_filtered = df_filtered.withColumn(
    "integrated_units", F.coalesce(F.col("integrated_units"), F.lit(0).cast(LongType()))
).withColumn(
    "integrated_normalized_units", F.coalesce(F.col("integrated_normalized_units"), F.lit(0).cast(LongType()))
).withColumn(
    "integrated_dollars", F.coalesce(F.col("integrated_dollars"), F.lit(0).cast(LongType()))
)

# ---------------------------
# /* SECTION: Aggregation & Output */
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
# /* SECTION: Output Display */
# ---------------------------

# Show the result of channel-wise aggregation
agg_df.show(truncate=False)

# /* END OF SCRIPT */
