spark.catalog.setCurrentCatalog("purgo_databricks")

# -----------------------------------------------------------------------------
# Compound Dosage Response Analysis: Aggregate Compound Statistics and Potency
# -----------------------------------------------------------------------------
# This script reads the Unity Catalog table purgo_playground.dosage_response_analysis,
# validates schema and data types, aggregates compound-level statistics, classifies
# potency levels, handles nulls and edge cases, and displays the final results.
# All warnings and errors are handled as per requirements.
# -----------------------------------------------------------------------------
# Output columns:
#   compound_id, avg_dose_mg, avg_response_pct, avg_efficacy, avg_ic50, avg_auc,
#   experiment_count, potency_level
# Potency Level Classification:
#   - High:     avg_ic50 < 2.0
#   - Moderate: 2.0 <= avg_ic50 <= 4.0
#   - Low:      avg_ic50 > 4.0
#   - Null if avg_ic50 is null
# -----------------------------------------------------------------------------
# All code is Databricks PySpark, production-ready, and follows Databricks best practices.
# -----------------------------------------------------------------------------

# from pyspark.sql import SparkSession  # SparkSession is already available in Databricks

# ---------------------------
# Imports
# ---------------------------
from pyspark.sql import functions as F  
from pyspark.sql.types import (
    StringType, DoubleType, LongType  # Built-in PySpark
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
        raise Exception(f"Required column '{missing[0]}' not found in source table")

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
        if df.filter((F.col(col) <= 0) & (F.col(col).isNotNull())).limit(1).count() > 0:
            warning_list.append(warning_message)

def warn_on_negative(df, col, warning_list, warning_message):
    """
    Check for negative values in a column and append warning if found.
    """
    if col in df.columns:
        if df.filter((F.col(col) < 0) & (F.col(col).isNotNull())).limit(1).count() > 0:
            warning_list.append(warning_message)

def display_warning(warning_list):
    """
    Display all warnings in the warning_list.
    """
    for w in warning_list:
        print(f"WARNING: {w}")

# ---------------------------
# Read Source Table and Validate Schema
# ---------------------------

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
# Data Quality: Exclude Null compound_id
# ---------------------------
df = df.filter(F.col("compound_id").isNotNull())

# ---------------------------
# Data Type Conversion (defensive, in case of schema drift)
# ---------------------------
# If any cast fails, an error will be raised
try:
    df = df.withColumn("dose_mg", F.col("dose_mg").cast("double"))
    df = df.withColumn("response_pct", F.col("response_pct").cast("long"))
    df = df.withColumn("efficacy_score", F.col("efficacy_score").cast("long"))
    df = df.withColumn("ic50", F.col("ic50").cast("double"))
    df = df.withColumn("auc", F.col("auc").cast("long"))
except Exception as e:
    raise Exception("Invalid data type in one or more columns") from e

# ---------------------------
# Data Quality Warnings (Negative/Zero Values)
# ---------------------------
warnings = []
warn_on_negative_or_zero(df, "dose_mg", warnings, "Zero or negative dose_mg detected")
warn_on_negative(df, "efficacy_score", warnings, "Negative efficacy_score detected")
warn_on_negative(df, "response_pct", warnings, "Negative response_pct detected")
warn_on_negative(df, "ic50", warnings, "Negative ic50 detected")
warn_on_negative(df, "auc", warnings, "Negative auc detected")

# ---------------------------
# Aggregation: Compound-level Statistics
# ---------------------------
agg_exprs = [
    F.avg("dose_mg").alias("avg_dose_mg"),
    F.avg("response_pct").alias("avg_response_pct"),
    F.avg("efficacy_score").alias("avg_efficacy"),
    F.avg("ic50").alias("avg_ic50"),
    F.avg("auc").alias("avg_auc"),
    F.count(F.lit(1)).alias("experiment_count")
]
df_agg = df.groupBy("compound_id").agg(*agg_exprs)

# ---------------------------
# Potency Level Classification
# ---------------------------
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
classify_potency_udf = udf(classify_potency, StringType())
df_agg = df_agg.withColumn("potency_level", classify_potency_udf(F.col("avg_ic50")))

# ---------------------------
# Output: Display Results
# ---------------------------
expected_output_cols = [
    "compound_id", "avg_dose_mg", "avg_response_pct", "avg_efficacy",
    "avg_ic50", "avg_auc", "experiment_count", "potency_level"
]
df_agg = df_agg.select(*expected_output_cols)

# ---------------------------
# Output Destination: Display in Notebook/Session
# ---------------------------
if not globals().get("output_destination"):
    print("WARNING: No output destination specified; results displayed only")
    display(df_agg)

# ---------------------------
# Display Warnings (if any)
# ---------------------------
display_warning(warnings)

# ---------------------------
# End of Script
# ---------------------------
