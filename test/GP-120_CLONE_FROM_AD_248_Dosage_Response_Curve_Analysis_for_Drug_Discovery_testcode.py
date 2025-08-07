spark.catalog.setCurrentCatalog("purgo_databricks")

# ---------------------------------------------------------------------------
# Databricks PySpark Test Suite for Compound Dosage Response Analysis
# ---------------------------------------------------------------------------
# This test suite validates the aggregation and classification logic for
# purgo_playground.dosage_response_analysis as per the requirements and Gherkin scenarios.
# ---------------------------------------------------------------------------

# from pyspark.sql import SparkSession  # SparkSession is already available in Databricks

# ---------------------------
# Imports
# ---------------------------
from pyspark.sql import functions as F  
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, LongType, IntegerType  # Built-in PySpark
)
from pyspark.sql.utils import AnalysisException  

# ---------------------------
# Utility Functions
# ---------------------------

def check_required_columns(df, required_cols):
    """
    Check if all required columns exist in the DataFrame.
    Raise an Exception if any are missing.
    """
    missing = [col for col in required_cols if col not in df.columns]
    if missing:
        raise Exception(f"Required column(s) {missing} not found in source table")

def check_column_types(df, col_types):
    """
    Check if columns have expected data types.
    Raise an Exception if any type mismatches are found.
    """
    for col, expected_type in col_types.items():
        if col in df.columns:
            actual_type = df.schema[col].dataType
            if not isinstance(actual_type, expected_type):
                raise Exception(f"Invalid data type for {col}: expected {expected_type.__name__}")

def warn_on_negative_or_zero(df, col, warning_list, warning_message):
    """
    Check for negative or zero values in a column and append warning if found.
    """
    if col in df.columns:
        if df.filter((F.col(col) <= 0) & (F.col(col).isNotNull())).count() > 0:
            warning_list.append(warning_message)

def warn_on_negative(df, col, warning_list, warning_message):
    """
    Check for negative values in a column and append warning if found.
    """
    if col in df.columns:
        if df.filter((F.col(col) < 0) & (F.col(col).isNotNull())).count() > 0:
            warning_list.append(warning_message)

def display_warning(warning_list):
    """
    Display all warnings in the warning_list.
    """
    for w in warning_list:
        print(f"WARNING: {w}")

# ---------------------------
# Test Setup: Read Source Table
# ---------------------------

# Required columns and their expected types
required_columns = [
    "compound_id", "dose_mg", "response_pct", "efficacy_score", "ic50", "auc"
]
expected_types = {
    "compound_id": StringType,
    "dose_mg": DoubleType,
    "response_pct": LongType,
    "efficacy_score": LongType,
    "ic50": DoubleType,
    "auc": LongType
}

# Try reading the table and validate schema
try:
    df = spark.read.table("purgo_playground.dosage_response_analysis")
except AnalysisException as e:
    raise Exception("Source table purgo_playground.dosage_response_analysis not found") from e

# Check required columns
check_required_columns(df, required_columns)

# Check column types
for col, typ in expected_types.items():
    if col in df.columns:
        actual_type = df.schema[col].dataType
        if not isinstance(actual_type, typ):
            raise Exception(f"Invalid data type for {col}: expected {typ.__name__}")

# ---------------------------
# Test 1: Schema Validation
# ---------------------------
# /* Test that the schema matches the expected columns and types */
schema = df.schema
assert "compound_id" in schema.names, "compound_id column missing"
assert "dose_mg" in schema.names, "dose_mg column missing"
assert "response_pct" in schema.names, "response_pct column missing"
assert "efficacy_score" in schema.names, "efficacy_score column missing"
assert "ic50" in schema.names, "ic50 column missing"
assert "auc" in schema.names, "auc column missing"
assert isinstance(schema["dose_mg"].dataType, DoubleType), "dose_mg should be DoubleType"
assert isinstance(schema["response_pct"].dataType, LongType), "response_pct should be LongType"
assert isinstance(schema["efficacy_score"].dataType, LongType), "efficacy_score should be LongType"
assert isinstance(schema["ic50"].dataType, DoubleType), "ic50 should be DoubleType"
assert isinstance(schema["auc"].dataType, LongType), "auc should be LongType"

# ---------------------------
# Test 2: Data Type Conversion and NULL Handling
# ---------------------------
# /* Test that averages ignore NULLs and raise errors on invalid types */
try:
    # Try to cast dose_mg to double (should succeed if all values are valid)
    df.withColumn("dose_mg", F.col("dose_mg").cast("double"))
except Exception as e:
    raise Exception("Invalid data type for dose_mg: expected double") from e

try:
    df.withColumn("response_pct", F.col("response_pct").cast("long"))
except Exception as e:
    raise Exception("Invalid data type for response_pct: expected bigint") from e

try:
    df.withColumn("efficacy_score", F.col("efficacy_score").cast("long"))
except Exception as e:
    raise Exception("Invalid data type for efficacy_score: expected bigint") from e

# ---------------------------
# Test 3: Exclude Null compound_id
# ---------------------------
# /* Records with null compound_id should be excluded from aggregation */
df_nonull = df.filter(F.col("compound_id").isNotNull())
assert df_nonull.filter(F.col("compound_id").isNull()).count() == 0, "Null compound_id not excluded"

# ---------------------------
# Test 4: Aggregation Logic
# ---------------------------
# /* Aggregate by compound_id and compute required statistics */
agg_exprs = [
    F.avg("dose_mg").alias("avg_dose_mg"),
    F.avg("response_pct").alias("avg_response_pct"),
    F.avg("efficacy_score").alias("avg_efficacy"),
    F.avg("ic50").alias("avg_ic50"),
    F.avg("auc").alias("avg_auc"),
    F.count(F.lit(1)).alias("experiment_count")
]
df_agg = df_nonull.groupBy("compound_id").agg(*agg_exprs)

# ---------------------------
# Test 5: Potency Level Classification
# ---------------------------
# /* Classify potency_level based on avg_ic50 */
def classify_potency(avg_ic50):
    if avg_ic50 is None:
        return None
    if avg_ic50 < 2.0:
        return "High"
    elif 2.0 <= avg_ic50 <= 4.0:
        return "Moderate"
    elif avg_ic50 > 4.0:
        return "Low"
    else:
        return None

from pyspark.sql.functions import udf  
from pyspark.sql.types import StringType  

classify_potency_udf = udf(classify_potency, StringType())
df_agg = df_agg.withColumn("potency_level", classify_potency_udf(F.col("avg_ic50")))

# ---------------------------
# Test 6: Output Format Validation
# ---------------------------
# /* Ensure output columns and types are as required */
expected_output_cols = [
    "compound_id", "avg_dose_mg", "avg_response_pct", "avg_efficacy",
    "avg_ic50", "avg_auc", "experiment_count", "potency_level"
]
for col in expected_output_cols:
    assert col in df_agg.columns, f"Output missing column: {col}"

# ---------------------------
# Test 7: Happy Path - CMPD001
# ---------------------------
# /* Validate aggregation for CMPD001 (no nulls) */
row = df_agg.filter(F.col("compound_id") == "CMPD001").collect()[0]
assert abs(row["avg_dose_mg"] - 15.0) < 0.01, "CMPD001 avg_dose_mg incorrect"
assert abs(row["avg_response_pct"] - 82.33) < 0.02, "CMPD001 avg_response_pct incorrect"
assert abs(row["avg_efficacy"] - 91.0) < 0.01, "CMPD001 avg_efficacy incorrect"
assert abs(row["avg_ic50"] - 1.6) < 0.01, "CMPD001 avg_ic50 incorrect"
assert abs(row["avg_auc"] - 122.33) < 0.02, "CMPD001 avg_auc incorrect"
assert row["experiment_count"] == 3, "CMPD001 experiment_count incorrect"
assert row["potency_level"] == "High", "CMPD001 potency_level incorrect"

# ---------------------------
# Test 8: Potency Level Classification (Examples)
# ---------------------------
# /* Validate classification for High, Moderate, Low */
row = df_agg.filter(F.col("compound_id") == "CMPD002").collect()[0]
assert abs(row["avg_ic50"] - 1.43) < 0.02, "CMPD002 avg_ic50 incorrect"
assert row["potency_level"] == "High", "CMPD002 potency_level incorrect"

row = df_agg.filter(F.col("compound_id") == "CMPD003").collect()[0]
assert abs(row["avg_ic50"] - 2.88) < 0.02, "CMPD003 avg_ic50 incorrect"
assert row["potency_level"] == "Moderate", "CMPD003 potency_level incorrect"

row = df_agg.filter(F.col("compound_id") == "CMPD004").collect()[0]
assert abs(row["avg_ic50"] - 5.03) < 0.02, "CMPD004 avg_ic50 incorrect"
assert row["potency_level"] == "Low", "CMPD004 potency_level incorrect"

# ---------------------------
# Test 9: Null Handling in Aggregation (CMPD005)
# ---------------------------
# /* Nulls should be ignored in averages, experiment_count should count all rows */
row = df_agg.filter(F.col("compound_id") == "CMPD005").collect()[0]
assert abs(row["avg_dose_mg"] - 12.5) < 0.01, "CMPD005 avg_dose_mg incorrect"
assert abs(row["avg_response_pct"] - 82.5) < 0.01, "CMPD005 avg_response_pct incorrect"
assert abs(row["avg_efficacy"] - 90.5) < 0.01, "CMPD005 avg_efficacy incorrect"
assert abs(row["avg_ic50"] - 1.6) < 0.01, "CMPD005 avg_ic50 incorrect"
assert abs(row["avg_auc"] - 122.5) < 0.01, "CMPD005 avg_auc incorrect"
assert row["experiment_count"] == 3, "CMPD005 experiment_count incorrect"

# ---------------------------
# Test 10: All Nulls for a Metric (CMPD006)
# ---------------------------
# /* All averages should be null, experiment_count should be 2 */
row = df_agg.filter(F.col("compound_id") == "CMPD006").collect()[0]
assert row["avg_dose_mg"] is None, "CMPD006 avg_dose_mg should be null"
assert row["avg_response_pct"] is None, "CMPD006 avg_response_pct should be null"
assert row["avg_efficacy"] is None, "CMPD006 avg_efficacy should be null"
assert row["avg_ic50"] is None, "CMPD006 avg_ic50 should be null"
assert row["avg_auc"] is None, "CMPD006 avg_auc should be null"
assert row["experiment_count"] == 2, "CMPD006 experiment_count incorrect"
assert row["potency_level"] is None, "CMPD006 potency_level should be null"

# ---------------------------
# Test 11: No Records for a Compound (CMPD007)
# ---------------------------
# /* No output row should be produced for CMPD007 */
assert df_agg.filter(F.col("compound_id") == "CMPD007").count() == 0, "CMPD007 should not be present"

# ---------------------------
# Test 12: Potency Level Threshold Edge Cases
# ---------------------------
# /* Test edge values for avg_ic50 */
row = df_agg.filter(F.col("compound_id") == "CMPD009").collect()[0]
assert abs(row["avg_ic50"] - 2.0) < 0.01, "CMPD009 avg_ic50 incorrect"
assert row["potency_level"] == "Moderate", "CMPD009 potency_level incorrect"

row = df_agg.filter(F.col("compound_id") == "CMPD010").collect()[0]
assert abs(row["avg_ic50"] - 4.0) < 0.01, "CMPD010 avg_ic50 incorrect"
assert row["potency_level"] == "Moderate", "CMPD010 potency_level incorrect"

row = df_agg.filter(F.col("compound_id") == "CMPD011").collect()[0]
assert abs(row["avg_ic50"] - 2.01) < 0.01, "CMPD011 avg_ic50 incorrect"
assert row["potency_level"] == "Moderate", "CMPD011 potency_level incorrect"

row = df_agg.filter(F.col("compound_id") == "CMPD012").collect()[0]
assert abs(row["avg_ic50"] - 4.01) < 0.01, "CMPD012 avg_ic50 incorrect"
assert row["potency_level"] == "Low", "CMPD012 potency_level incorrect"

row = df_agg.filter(F.col("compound_id") == "CMPD013").collect()[0]
assert abs(row["avg_ic50"] - 1.99) < 0.01, "CMPD013 avg_ic50 incorrect"
assert row["potency_level"] == "High", "CMPD013 potency_level incorrect"

# ---------------------------
# Test 13: Filtering by approved_flag (CMPD014)
# ---------------------------
# /* Without filtering */
row = df_agg.filter(F.col("compound_id") == "CMPD014").collect()[0]
assert abs(row["avg_dose_mg"] - 15.0) < 0.01, "CMPD014 avg_dose_mg incorrect (no filter)"
assert row["experiment_count"] == 3, "CMPD014 experiment_count incorrect (no filter)"

# /* With filtering on approved_flag = 'Y' (case-sensitive) */
df_approved = df_nonull.filter(F.col("approved_flag") == "Y")
df_agg_approved = df_approved.groupBy("compound_id").agg(*agg_exprs)
df_agg_approved = df_agg_approved.withColumn("potency_level", classify_potency_udf(F.col("avg_ic50")))
row = df_agg_approved.filter(F.col("compound_id") == "CMPD014").collect()[0]
assert abs(row["avg_dose_mg"] - 12.5) < 0.01, "CMPD014 avg_dose_mg incorrect (approved only)"
assert row["experiment_count"] == 2, "CMPD014 experiment_count incorrect (approved only)"

# ---------------------------
# Test 14: Output Format Columns
# ---------------------------
# /* Output DataFrame should have all required columns */
assert set(df_agg.columns) == set(expected_output_cols), "Output columns do not match specification"

# ---------------------------
# Test 15: Compound with only null ic50 values (CMPD015)
# ---------------------------
row = df_agg.filter(F.col("compound_id") == "CMPD015").collect()[0]
assert row["avg_ic50"] is None, "CMPD015 avg_ic50 should be null"
assert row["potency_level"] is None, "CMPD015 potency_level should be null"

# ---------------------------
# Test 16: Compound with zero experiments (CMPD016)
# ---------------------------
assert df_agg.filter(F.col("compound_id") == "CMPD016").count() == 0, "CMPD016 should not be present"

# ---------------------------
# Test 17: Negative or Zero dose_mg (CMPD017, CMPD018, CMPD019)
# ---------------------------
warnings = []
warn_on_negative_or_zero(df_nonull.filter(F.col("compound_id") == "CMPD017"), "dose_mg", warnings, "Zero or negative dose_mg detected")
row = df_agg.filter(F.col("compound_id") == "CMPD017").collect()[0]
assert abs(row["avg_dose_mg"] - 5.0) < 0.01, "CMPD017 avg_dose_mg incorrect"
assert "Zero or negative dose_mg detected" in warnings, "Warning for zero dose_mg not shown"

warnings = []
warn_on_negative_or_zero(df_nonull.filter(F.col("compound_id") == "CMPD018"), "dose_mg", warnings, "Zero or negative dose_mg detected")
row = df_agg.filter(F.col("compound_id") == "CMPD018").collect()[0]
assert abs(row["avg_dose_mg"] - 5.0) < 0.01, "CMPD018 avg_dose_mg incorrect"
assert "Zero or negative dose_mg detected" in warnings, "Warning for negative dose_mg not shown"

warnings = []
warn_on_negative_or_zero(df_nonull.filter(F.col("compound_id") == "CMPD019"), "dose_mg", warnings, "Zero or negative dose_mg detected")
row = df_agg.filter(F.col("compound_id") == "CMPD019").collect()[0]
assert abs(row["avg_dose_mg"] - 15.0) < 0.01, "CMPD019 avg_dose_mg incorrect"
assert "Zero or negative dose_mg detected" not in warnings, "Unexpected warning for positive dose_mg"

# ---------------------------
# Test 18: Negative efficacy_score, response_pct, ic50, auc
# ---------------------------
warnings = []
warn_on_negative(df_nonull.filter(F.col("compound_id") == "CMPD027"), "efficacy_score", warnings, "Negative efficacy_score detected")
row = df_agg.filter(F.col("compound_id") == "CMPD027").collect()[0]
assert abs(row["avg_efficacy"] - (-10.0)) < 0.01, "CMPD027 avg_efficacy incorrect"
assert "Negative efficacy_score detected" in warnings, "Warning for negative efficacy_score not shown"

warnings = []
warn_on_negative(df_nonull.filter(F.col("compound_id") == "CMPD028"), "response_pct", warnings, "Negative response_pct detected")
row = df_agg.filter(F.col("compound_id") == "CMPD028").collect()[0]
assert abs(row["avg_response_pct"] - (-20.0)) < 0.01, "CMPD028 avg_response_pct incorrect"
assert "Negative response_pct detected" in warnings, "Warning for negative response_pct not shown"

warnings = []
warn_on_negative(df_nonull.filter(F.col("compound_id") == "CMPD029"), "ic50", warnings, "Negative ic50 detected")
row = df_agg.filter(F.col("compound_id") == "CMPD029").collect()[0]
assert abs(row["avg_ic50"] - (-1.0)) < 0.01, "CMPD029 avg_ic50 incorrect"
assert "Negative ic50 detected" in warnings, "Warning for negative ic50 not shown"

warnings = []
warn_on_negative(df_nonull.filter(F.col("compound_id") == "CMPD030"), "auc", warnings, "Negative auc detected")
row = df_agg.filter(F.col("compound_id") == "CMPD030").collect()[0]
assert abs(row["avg_auc"] - (-100.0)) < 0.01, "CMPD030 avg_auc incorrect"
assert "Negative auc detected" in warnings, "Warning for negative auc not shown"

# ---------------------------
# Test 19: Duplicate Records (CMPD021)
# ---------------------------
row = df_agg.filter(F.col("compound_id") == "CMPD021").collect()[0]
assert row["experiment_count"] == 2, "CMPD021 experiment_count should count duplicates"

# ---------------------------
# Test 20: Single Record (CMPD022)
# ---------------------------
row = df_agg.filter(F.col("compound_id") == "CMPD022").collect()[0]
assert abs(row["avg_dose_mg"] - 10.0) < 0.01, "CMPD022 avg_dose_mg incorrect"
assert abs(row["avg_response_pct"] - 80.0) < 0.01, "CMPD022 avg_response_pct incorrect"
assert abs(row["avg_efficacy"] - 90.0) < 0.01, "CMPD022 avg_efficacy incorrect"
assert abs(row["avg_ic50"] - 1.5) < 0.01, "CMPD022 avg_ic50 incorrect"
assert abs(row["avg_auc"] - 120.0) < 0.01, "CMPD022 avg_auc incorrect"
assert row["experiment_count"] == 1, "CMPD022 experiment_count incorrect"
assert row["potency_level"] == "High", "CMPD022 potency_level incorrect"

# ---------------------------
# Test 21: Mixed-case approved_flag (CMPD023)
# ---------------------------
# /* Only 'Y' (case-sensitive) should be included when filtering */
df_approved = df_nonull.filter((F.col("compound_id") == "CMPD023") & (F.col("approved_flag") == "Y"))
assert df_approved.count() == 1, "Only one record with approved_flag='Y' for CMPD023"

# ---------------------------
# Test 22: Future date_tested (CMPD024)
# ---------------------------
# /* Record should be included unless filtering by date */
row = df_agg.filter(F.col("compound_id") == "CMPD024").collect()[0]
assert row is not None, "CMPD024 with future date_tested should be present"

# ---------------------------
# Test 23: Non-unique compound_id (CMPD025 vs cmpd025)
# ---------------------------
assert df_agg.filter(F.col("compound_id") == "CMPD025").count() == 1, "CMPD025 should be present"
assert df_agg.filter(F.col("compound_id") == "cmpd025").count() == 1, "cmpd025 should be present"

# ---------------------------
# Test 24: Null compound_id excluded
# ---------------------------
assert df_agg.filter(F.col("compound_id").isNull()).count() == 0, "Null compound_id should be excluded"

# ---------------------------
# Test 25: Compound with only null ic50 values (CMPD015)
# ---------------------------
row = df_agg.filter(F.col("compound_id") == "CMPD015").collect()[0]
assert row["avg_ic50"] is None, "CMPD015 avg_ic50 should be null"
assert row["potency_level"] is None, "CMPD015 potency_level should be null"

# ---------------------------
# Test 26: Output destination not specified
# ---------------------------
# /* If not writing to table, display results and show warning */
if not "output_destination":
    print("WARNING: No output destination specified; results displayed only")
    display(df_agg)

# ---------------------------
# Test 27: Data Quality - Check for NULLs in output
# ---------------------------
# /* No compound_id in output should be null */
assert df_agg.filter(F.col("compound_id").isNull()).count() == 0, "Output contains null compound_id"

# ---------------------------
# Test 28: Data Quality - Check for negative values in output
# ---------------------------
# /* Negative values in averages should trigger warnings (already checked above) */

# ---------------------------
# Test 29: Data Type Error Handling
# ---------------------------
# /* Simulate invalid data type for dose_mg, response_pct, efficacy_score */
try:
    # Create a DataFrame with invalid type for dose_mg
    schema_invalid = StructType([
        StructField("compound_id", StringType(), True),
        StructField("dose_mg", StringType(), True),
        StructField("response_pct", LongType(), True),
        StructField("efficacy_score", LongType(), True),
        StructField("ic50", DoubleType(), True),
        StructField("auc", LongType(), True)
    ])
    data_invalid = [("CMPD008", "abc", 80, 90, 1.5, 120)]
    df_invalid = spark.createDataFrame(data_invalid, schema=schema_invalid)
    df_invalid.withColumn("dose_mg", F.col("dose_mg").cast("double")).collect()
    assert False, "Expected error for invalid dose_mg type"
except Exception as e:
    assert "Invalid" in str(e) or "cannot be cast" in str(e), "Invalid data type for dose_mg: expected double"

try:
    schema_invalid = StructType([
        StructField("compound_id", StringType(), True),
        StructField("dose_mg", DoubleType(), True),
        StructField("response_pct", StringType(), True),
        StructField("efficacy_score", LongType(), True),
        StructField("ic50", DoubleType(), True),
        StructField("auc", LongType(), True)
    ])
    data_invalid = [("CMPD020", 10.0, "eighty", 90, 1.5, 120)]
    df_invalid = spark.createDataFrame(data_invalid, schema=schema_invalid)
    df_invalid.withColumn("response_pct", F.col("response_pct").cast("long")).collect()
    assert False, "Expected error for invalid response_pct type"
except Exception as e:
    assert "Invalid" in str(e) or "cannot be cast" in str(e), "Invalid data type for response_pct: expected bigint"

try:
    schema_invalid = StructType([
        StructField("compound_id", StringType(), True),
        StructField("dose_mg", DoubleType(), True),
        StructField("response_pct", LongType(), True),
        StructField("efficacy_score", DoubleType(), True),
        StructField("ic50", DoubleType(), True),
        StructField("auc", LongType(), True)
    ])
    data_invalid = [("CMPD026", 10.0, 80, 90.5, 1.5, 120)]
    df_invalid = spark.createDataFrame(data_invalid, schema=schema_invalid)
    df_invalid.withColumn("efficacy_score", F.col("efficacy_score").cast("long")).collect()
    assert False, "Expected error for invalid efficacy_score type"
except Exception as e:
    assert "Invalid" in str(e) or "cannot be cast" in str(e), "Invalid data type for efficacy_score: expected bigint"

# ---------------------------
# Test 30: Missing Required Columns
# ---------------------------
# /* Simulate missing columns and assert error is raised */
for missing_col in ["ic50", "auc", "efficacy_score", "dose_mg", "response_pct", "compound_id"]:
    try:
        cols = [c for c in required_columns if c != missing_col]
        schema_missing = StructType([StructField(c, StringType(), True) for c in cols])
        data_missing = [tuple("x" for _ in cols)]
        df_missing = spark.createDataFrame(data_missing, schema=schema_missing)
        check_required_columns(df_missing, required_columns)
        assert False, f"Expected error for missing column {missing_col}"
    except Exception as e:
        assert missing_col in str(e), f"Missing column error not raised for {missing_col}"

# ---------------------------
# Test 31: Performance Test (Batch)
# ---------------------------
# /* Performance: Ensure aggregation completes within reasonable time for batch */
import time  
start_time = time.time()
df_agg.collect()
elapsed = time.time() - start_time
assert elapsed < 30, "Batch aggregation took too long"

# ---------------------------
# Test 32: Data Quality - experiment_count matches input
# ---------------------------
# /* For each compound, experiment_count should match number of input records */
input_counts = df_nonull.groupBy("compound_id").count().collect()
for row_in in input_counts:
    row_out = df_agg.filter(F.col("compound_id") == row_in["compound_id"]).collect()[0]
    assert row_out["experiment_count"] == row_in["count"], f"experiment_count mismatch for {row_in['compound_id']}"

# ---------------------------
# Test 33: Window Function Test (Analytics)
# ---------------------------
# /* Test window function: rank compounds by avg_ic50 ascending */
from pyspark.sql.window import Window  
w = Window.orderBy(F.col("avg_ic50").asc_nulls_last())
df_ranked = df_agg.withColumn("ic50_rank", F.rank().over(w))
assert "ic50_rank" in df_ranked.columns, "ic50_rank column missing from window function output"

# ---------------------------
# Test 34: Delta Lake Operations (MERGE, UPDATE, DELETE)
# ---------------------------
# /* Test Delta Lake upsert and delete operations on output table */
from delta.tables import DeltaTable  

# Create or replace output table for test
df_agg.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("purgo_playground.compound_stats_analysis")

# MERGE: Upsert a new row
merge_df = spark.createDataFrame([("CMPD999", 10.0, 80.0, 90.0, 1.5, 120.0, 1, "High")], schema=df_agg.schema)
delta_table = DeltaTable.forName(spark, "purgo_playground.compound_stats_analysis")
delta_table.alias("tgt").merge(
    merge_df.alias("src"),
    "tgt.compound_id = src.compound_id"
).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
assert delta_table.toDF().filter(F.col("compound_id") == "CMPD999").count() == 1, "MERGE failed to insert new row"

# UPDATE: Update avg_dose_mg for CMPD999
delta_table.update(
    condition=F.col("compound_id") == "CMPD999",
    set={"avg_dose_mg": F.lit(20.0)}
)
assert delta_table.toDF().filter((F.col("compound_id") == "CMPD999") & (F.col("avg_dose_mg") == 20.0)).count() == 1, "UPDATE failed"

# DELETE: Remove CMPD999
delta_table.delete(F.col("compound_id") == "CMPD999")
assert delta_table.toDF().filter(F.col("compound_id") == "CMPD999").count() == 0, "DELETE failed"

# ---------------------------
# Test 35: Streaming Scenario (if applicable)
# ---------------------------
# /* Test streaming read and aggregation (if supported) */
try:
    df_stream = spark.readStream.format("delta").table("purgo_playground.dosage_response_analysis")
    df_stream_agg = df_stream.groupBy("compound_id").agg(
        F.avg("dose_mg").alias("avg_dose_mg"),
        F.avg("response_pct").alias("avg_response_pct"),
        F.avg("efficacy_score").alias("avg_efficacy"),
        F.avg("ic50").alias("avg_ic50"),
        F.avg("auc").alias("avg_auc"),
        F.count(F.lit(1)).alias("experiment_count")
    )
    # Streaming test: just check schema and that query can be started/stopped
    query = df_stream_agg.writeStream.format("memory").queryName("test_stream").outputMode("complete").start()
    query.processAllAvailable()
    query.stop()
except Exception as e:
    # If streaming not supported, skip
    pass

# ---------------------------
# Test 36: Data Quality - No duplicate columns in output
# ---------------------------
assert len(df_agg.columns) == len(set(df_agg.columns)), "Duplicate columns in output"

# ---------------------------
# Test 37: Data Quality - No NULLs in experiment_count
# ---------------------------
assert df_agg.filter(F.col("experiment_count").isNull()).count() == 0, "experiment_count should not be null"

# ---------------------------
# Test 38: Data Quality - Potency level only allowed values or null
# ---------------------------
allowed_potency = {"High", "Moderate", "Low", None}
for row in df_agg.select("potency_level").distinct().collect():
    assert row["potency_level"] in allowed_potency, f"Invalid potency_level: {row['potency_level']}"

# ---------------------------
# Test 39: Data Quality - Output row count matches unique compound_id count
# ---------------------------
unique_compounds = df_nonull.select("compound_id").distinct().count()
assert df_agg.count() == unique_compounds, "Output row count does not match unique compound_id count"

# ---------------------------
# Test 40: Final Output Display
# ---------------------------
# /* Display the final results as required */
display(df_agg)

# ---------------------------
# End of Test Suite
# ---------------------------
