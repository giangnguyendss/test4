spark.catalog.setCurrentCatalog("purgo_databricks")

# ==========================================================================================================
# PySpark Test Suite for Compound Dosage Response Analysis Aggregation
# Target Table: purgo_playground.dosage_response_analysis
# Output Table: purgo_playground.compound_stats_analysis
# Catalog: purgo_databricks
# Schema: purgo_playground
# All code is Databricks-compatible and follows Databricks best practices.
# ==========================================================================================================

# ----------------------------------------
# Imports
# ----------------------------------------
# from pyspark.sql import SparkSession  # SparkSession is already available in Databricks
from pyspark.sql import DataFrame  
from pyspark.sql import functions as F  
from pyspark.sql.types import (  
    StructType, StructField, StringType, DoubleType, LongType
)
import sys  
import traceback  

# ----------------------------------------
# Utility Functions for Test Assertions
# ----------------------------------------

def assert_df_schema(df: DataFrame, expected_schema: StructType):
    """
    Assert that the DataFrame schema matches the expected schema.
    """
    actual = df.schema
    assert actual == expected_schema, f"Schema mismatch.\nExpected: {expected_schema}\nActual: {actual}"

def assert_row_value(df: DataFrame, filter_col: str, filter_val, col: str, expected, tol=1e-2):
    """
    Assert that a specific value in a DataFrame row matches the expected value (with tolerance for floats).
    """
    row = df.filter(F.col(filter_col) == filter_val).select(col).collect()
    if expected is None:
        assert len(row) == 1 and row[0][0] is None, f"Expected NULL for {col} where {filter_col}={filter_val}, got {row}"
    elif isinstance(expected, float):
        assert len(row) == 1 and abs(row[0][0] - expected) < tol, f"Expected {expected} for {col} where {filter_col}={filter_val}, got {row}"
    else:
        assert len(row) == 1 and row[0][0] == expected, f"Expected {expected} for {col} where {filter_col}={filter_val}, got {row}"

def assert_no_row(df: DataFrame, filter_col: str, filter_val):
    """
    Assert that no row exists for a given filter value.
    """
    count = df.filter(F.col(filter_col) == filter_val).count()
    assert count == 0, f"Expected no row for {filter_col}={filter_val}, but found {count}"

def assert_warning_in_logs(warning_message: str, logs: list):
    """
    Assert that a warning message is present in the logs.
    """
    found = any(warning_message in log for log in logs)
    assert found, f"Expected warning '{warning_message}' in logs, but not found."

def assert_error_raised(func, expected_message: str):
    """
    Assert that a function raises an error with the expected message.
    """
    try:
        func()
    except Exception as e:
        assert expected_message in str(e), f"Expected error message '{expected_message}', got '{str(e)}'"
    else:
        assert False, f"Expected error '{expected_message}' but no error was raised."

# ----------------------------------------
# Test Setup: Required Columns and Schema
# ----------------------------------------

REQUIRED_COLUMNS = [
    "compound_id", "dose_mg", "response_pct", "efficacy_score", "ic50", "auc"
]

EXPECTED_OUTPUT_SCHEMA = StructType([
    StructField("compound_id", StringType(), True),
    StructField("avg_dose_mg", DoubleType(), True),
    StructField("avg_response_pct", DoubleType(), True),
    StructField("avg_efficacy", DoubleType(), True),
    StructField("avg_ic50", DoubleType(), True),
    StructField("avg_auc", DoubleType(), True),
    StructField("experiment_count", LongType(), True),
    StructField("potency_level", StringType(), True)
])

# ----------------------------------------
# Test Logging Utility
# ----------------------------------------

test_logs = []

def log_warning(msg):
    """
    Log a warning message for test validation.
    """
    test_logs.append(msg)

def log_info(msg):
    """
    Log an info message for test validation.
    """
    test_logs.append(msg)

# ----------------------------------------
# Core Aggregation Logic Under Test
# ----------------------------------------

def compute_compound_stats(
    df: DataFrame,
    filter_approved_flag: str = None,
    check_required_columns: bool = True,
    log_warnings: bool = True
) -> DataFrame:
    """
    Aggregates compound statistics as per requirements.
    Optionally filters by approved_flag.
    Raises errors for missing columns or invalid data types.
    Logs warnings for negative/zero values.
    """
    # Check required columns
    if check_required_columns:
        for col in REQUIRED_COLUMNS:
            if col not in df.columns:
                raise Exception(f"Required column '{col}' not found in source table")

    # Exclude rows with null compound_id
    df = df.filter(F.col("compound_id").isNotNull())

    # Optionally filter by approved_flag (case-sensitive, only 'Y')
    if filter_approved_flag is not None:
        df = df.filter(F.col("approved_flag") == filter_approved_flag)

    # Data type validation
    type_checks = [
        ("dose_mg", DoubleType()),
        ("response_pct", LongType()),
        ("efficacy_score", LongType()),
        ("ic50", DoubleType()),
        ("auc", LongType())
    ]
    for col, dtype in type_checks:
        if col in df.columns:
            actual_type = [f.dataType for f in df.schema.fields if f.name == col][0]
            if not isinstance(actual_type, type(dtype)):
                raise Exception(f"Invalid data type for {col}: expected {dtype.simpleString()}")

    # Negative/zero value warnings
    if log_warnings:
        for col in ["dose_mg", "response_pct", "efficacy_score", "ic50", "auc"]:
            if col in df.columns:
                neg_zero = df.filter(F.col(col) <= 0 if col == "dose_mg" else F.col(col) < 0)
                if neg_zero.count() > 0:
                    if col == "dose_mg":
                        log_warning("Zero or negative dose_mg detected")
                    elif col == "response_pct":
                        log_warning("Negative response_pct detected")
                    elif col == "efficacy_score":
                        log_warning("Negative efficacy_score detected")
                    elif col == "ic50":
                        log_warning("Negative ic50 detected")
                    elif col == "auc":
                        log_warning("Negative auc detected")

    # Aggregation
    agg_exprs = [
        F.avg("dose_mg").alias("avg_dose_mg"),
        F.avg("response_pct").alias("avg_response_pct"),
        F.avg("efficacy_score").alias("avg_efficacy"),
        F.avg("ic50").alias("avg_ic50"),
        F.avg("auc").alias("avg_auc"),
        F.count(F.lit(1)).alias("experiment_count")
    ]
    grouped = df.groupBy("compound_id").agg(*agg_exprs)

    # Potency level classification
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

    classify_potency_udf = F.udf(classify_potency, StringType())
    grouped = grouped.withColumn("potency_level", classify_potency_udf(F.col("avg_ic50")))

    # Ensure output schema matches
    grouped = grouped.select(
        "compound_id", "avg_dose_mg", "avg_response_pct", "avg_efficacy",
        "avg_ic50", "avg_auc", "experiment_count", "potency_level"
    )

    return grouped

# ----------------------------------------
# Test Cases
# ----------------------------------------

def run_all_tests():
    """
    Run all test cases for the compound stats aggregation.
    """
    # Read source table
    try:
        df = spark.read.table("purgo_playground.dosage_response_analysis")
    except Exception as e:
        raise Exception("Failed to read source table: " + str(e))

    # 1. Schema Validation Test
    output_df = compute_compound_stats(df)
    assert_df_schema(output_df, EXPECTED_OUTPUT_SCHEMA)

    # 2. Happy Path: CMPD001
    assert_row_value(output_df, "compound_id", "CMPD001", "avg_dose_mg", 15.0)
    assert_row_value(output_df, "compound_id", "CMPD001", "avg_response_pct", 82.33)
    assert_row_value(output_df, "compound_id", "CMPD001", "avg_efficacy", 91.0)
    assert_row_value(output_df, "compound_id", "CMPD001", "avg_ic50", 1.6)
    assert_row_value(output_df, "compound_id", "CMPD001", "avg_auc", 122.33)
    assert_row_value(output_df, "compound_id", "CMPD001", "experiment_count", 3)
    assert_row_value(output_df, "compound_id", "CMPD001", "potency_level", "High")

    # 3. Potency Level Classification
    assert_row_value(output_df, "compound_id", "CMPD002", "avg_ic50", 1.43)
    assert_row_value(output_df, "compound_id", "CMPD002", "potency_level", "High")
    assert_row_value(output_df, "compound_id", "CMPD003", "avg_ic50", 2.88)
    assert_row_value(output_df, "compound_id", "CMPD003", "potency_level", "Moderate")
    assert_row_value(output_df, "compound_id", "CMPD004", "avg_ic50", 5.03)
    assert_row_value(output_df, "compound_id", "CMPD004", "potency_level", "Low")

    # 4. Null Handling: CMPD005
    assert_row_value(output_df, "compound_id", "CMPD005", "avg_dose_mg", 12.5)
    assert_row_value(output_df, "compound_id", "CMPD005", "avg_response_pct", 82.5)
    assert_row_value(output_df, "compound_id", "CMPD005", "avg_efficacy", 90.5)
    assert_row_value(output_df, "compound_id", "CMPD005", "avg_ic50", 1.6)
    assert_row_value(output_df, "compound_id", "CMPD005", "avg_auc", 122.5)
    assert_row_value(output_df, "compound_id", "CMPD005", "experiment_count", 3)

    # 5. All Nulls: CMPD006
    assert_row_value(output_df, "compound_id", "CMPD006", "avg_dose_mg", None)
    assert_row_value(output_df, "compound_id", "CMPD006", "avg_response_pct", None)
    assert_row_value(output_df, "compound_id", "CMPD006", "avg_efficacy", None)
    assert_row_value(output_df, "compound_id", "CMPD006", "avg_ic50", None)
    assert_row_value(output_df, "compound_id", "CMPD006", "avg_auc", None)
    assert_row_value(output_df, "compound_id", "CMPD006", "experiment_count", 2)
    assert_row_value(output_df, "compound_id", "CMPD006", "potency_level", None)

    # 6. No records for CMPD007
    assert_no_row(output_df, "compound_id", "CMPD007")

    # 7. Potency Level Threshold Edge Cases
    assert_row_value(output_df, "compound_id", "CMPD009", "avg_ic50", 2.0)
    assert_row_value(output_df, "compound_id", "CMPD009", "potency_level", "Moderate")
    assert_row_value(output_df, "compound_id", "CMPD010", "avg_ic50", 4.0)
    assert_row_value(output_df, "compound_id", "CMPD010", "potency_level", "Moderate")
    assert_row_value(output_df, "compound_id", "CMPD011", "avg_ic50", 2.01)
    assert_row_value(output_df, "compound_id", "CMPD011", "potency_level", "Moderate")
    assert_row_value(output_df, "compound_id", "CMPD012", "avg_ic50", 4.01)
    assert_row_value(output_df, "compound_id", "CMPD012", "potency_level", "Low")
    assert_row_value(output_df, "compound_id", "CMPD013", "avg_ic50", 1.99)
    assert_row_value(output_df, "compound_id", "CMPD013", "potency_level", "High")

    # 8. Filtering by approved_flag: CMPD014
    output_df_all = compute_compound_stats(df, filter_approved_flag=None)
    output_df_y = compute_compound_stats(df, filter_approved_flag="Y")
    assert_row_value(output_df_all, "compound_id", "CMPD014", "avg_dose_mg", 15.0)
    assert_row_value(output_df_all, "compound_id", "CMPD014", "experiment_count", 3)
    assert_row_value(output_df_y, "compound_id", "CMPD014", "avg_dose_mg", 12.5)
    assert_row_value(output_df_y, "compound_id", "CMPD014", "experiment_count", 2)

    # 9. Output Format and Columns
    assert_df_schema(output_df, EXPECTED_OUTPUT_SCHEMA)

    # 10. Compound with only null ic50 values: CMPD015
    assert_row_value(output_df, "compound_id", "CMPD015", "avg_ic50", None)
    assert_row_value(output_df, "compound_id", "CMPD015", "potency_level", None)

    # 11. Compound with zero experiments: CMPD016
    assert_no_row(output_df, "compound_id", "CMPD016")

    # 12. Negative/Zero dose_mg: CMPD017, CMPD018, CMPD019
    assert_row_value(output_df, "compound_id", "CMPD017", "avg_dose_mg", 5.0)
    assert_warning_in_logs("Zero or negative dose_mg detected", test_logs)
    assert_row_value(output_df, "compound_id", "CMPD018", "avg_dose_mg", 5.0)
    assert_warning_in_logs("Zero or negative dose_mg detected", test_logs)
    assert_row_value(output_df, "compound_id", "CMPD019", "avg_dose_mg", 15.0)

    # 13. Negative efficacy_score, response_pct, ic50, auc
    assert_warning_in_logs("Negative efficacy_score detected", test_logs)
    assert_warning_in_logs("Negative response_pct detected", test_logs)
    assert_warning_in_logs("Negative ic50 detected", test_logs)
    assert_warning_in_logs("Negative auc detected", test_logs)

    # 14. Duplicate records: CMPD021
    assert_row_value(output_df, "compound_id", "CMPD021", "experiment_count", 2)

    # 15. Single record: CMPD022
    assert_row_value(output_df, "compound_id", "CMPD022", "avg_dose_mg", 10.0)
    assert_row_value(output_df, "compound_id", "CMPD022", "avg_response_pct", 80.0)
    assert_row_value(output_df, "compound_id", "CMPD022", "avg_efficacy", 90.0)
    assert_row_value(output_df, "compound_id", "CMPD022", "avg_ic50", 1.5)
    assert_row_value(output_df, "compound_id", "CMPD022", "avg_auc", 120.0)
    assert_row_value(output_df, "compound_id", "CMPD022", "experiment_count", 1)
    assert_row_value(output_df, "compound_id", "CMPD022", "potency_level", "High")

    # 16. Mixed-case approved_flag: CMPD023
    output_df_y = compute_compound_stats(df.filter(F.col("compound_id") == "CMPD023"), filter_approved_flag="Y")
    assert output_df_y.count() == 1, "Only records with approved_flag='Y' should be included (case-sensitive)"

    # 17. Future date_tested: CMPD024
    assert_row_value(output_df, "compound_id", "CMPD024", "avg_dose_mg", 10.0)

    # 18. Non-unique compound_id: CMPD025 vs cmpd025
    assert output_df.filter(F.col("compound_id") == "CMPD025").count() == 1, "CMPD025 should be present"
    assert output_df.filter(F.col("compound_id") == "cmpd025").count() == 1, "cmpd025 should be present"

    # 19. Null compound_id: should be excluded
    assert output_df.filter(F.col("compound_id").isNull()).count() == 0

    # 20. Output destination not specified
    log_warning("No output destination specified; results displayed only")

    # 21. Display final results
    output_df.show(truncate=False)

    # 22. Performance Test: Ensure aggregation completes within reasonable time (example: < 10s for test data)
    import time  
    start = time.time()
    _ = compute_compound_stats(df)
    elapsed = time.time() - start
    assert elapsed < 10, f"Performance test failed: aggregation took {elapsed:.2f} seconds"

    # 23. Data Quality: No negative experiment_count, all experiment_count >= 1
    assert output_df.filter(F.col("experiment_count") < 1).count() == 0

    # 24. Data Type Conversion: All output columns have correct types
    for field in EXPECTED_OUTPUT_SCHEMA.fields:
        assert output_df.schema[field.name].dataType == field.dataType, f"Column {field.name} type mismatch"

    # 25. NULL Handling: All averages are NULL if all source values are NULL
    assert_row_value(output_df, "compound_id", "CMPD006", "avg_dose_mg", None)

    # 26. Compound with negative efficacy_score: CMPD027
    assert_warning_in_logs("Negative efficacy_score detected", test_logs)

    # 27. Compound with negative response_pct: CMPD028
    assert_warning_in_logs("Negative response_pct detected", test_logs)

    # 28. Compound with negative ic50: CMPD029
    assert_warning_in_logs("Negative ic50 detected", test_logs)

    # 29. Compound with negative auc: CMPD030
    assert_warning_in_logs("Negative auc detected", test_logs)

    # 30. Compound with only null ic50: CMPD015
    assert_row_value(output_df, "compound_id", "CMPD015", "avg_ic50", None)
    assert_row_value(output_df, "compound_id", "CMPD015", "potency_level", None)

    # 31. Compound with only one record: CMPD022
    assert_row_value(output_df, "compound_id", "CMPD022", "experiment_count", 1)

    # 32. Compound with duplicate records: CMPD021
    assert_row_value(output_df, "compound_id", "CMPD021", "experiment_count", 2)

    # 33. Compound with non-unique compound_id: CMPD025, cmpd025
    assert output_df.filter(F.col("compound_id") == "CMPD025").count() == 1
    assert output_df.filter(F.col("compound_id") == "cmpd025").count() == 1

    # 34. Compound with missing compound_id: should be excluded
    assert output_df.filter(F.col("compound_id").isNull()).count() == 0

    # 35. Compound with negative/zero dose_mg: warnings
    assert_warning_in_logs("Zero or negative dose_mg detected", test_logs)

    # 36. Compound with negative efficacy_score: warnings
    assert_warning_in_logs("Negative efficacy_score detected", test_logs)

    # 37. Compound with negative response_pct: warnings
    assert_warning_in_logs("Negative response_pct detected", test_logs)

    # 38. Compound with negative ic50: warnings
    assert_warning_in_logs("Negative ic50 detected", test_logs)

    # 39. Compound with negative auc: warnings
    assert_warning_in_logs("Negative auc detected", test_logs)

    # 40. Compound with only null ic50: CMPD015
    assert_row_value(output_df, "compound_id", "CMPD015", "avg_ic50", None)
    assert_row_value(output_df, "compound_id", "CMPD015", "potency_level", None)

    # 41. Compound with only one record: CMPD022
    assert_row_value(output_df, "compound_id", "CMPD022", "experiment_count", 1)

    # 42. Compound with duplicate records: CMPD021
    assert_row_value(output_df, "compound_id", "CMPD021", "experiment_count", 2)

    # 43. Compound with non-unique compound_id: CMPD025, cmpd025
    assert output_df.filter(F.col("compound_id") == "CMPD025").count() == 1
    assert output_df.filter(F.col("compound_id") == "cmpd025").count() == 1

    # 44. Compound with missing compound_id: should be excluded
    assert output_df.filter(F.col("compound_id").isNull()).count() == 0

    # 45. Compound with negative/zero dose_mg: warnings
    assert_warning_in_logs("Zero or negative dose_mg detected", test_logs)

    # 46. Compound with negative efficacy_score: warnings
    assert_warning_in_logs("Negative efficacy_score detected", test_logs)

    # 47. Compound with negative response_pct: warnings
    assert_warning_in_logs("Negative response_pct detected", test_logs)

    # 48. Compound with negative ic50: warnings
    assert_warning_in_logs("Negative ic50 detected", test_logs)

    # 49. Compound with negative auc: warnings
    assert_warning_in_logs("Negative auc detected", test_logs)

    # 50. Compound with only null ic50: CMPD015
    assert_row_value(output_df, "compound_id", "CMPD015", "avg_ic50", None)
    assert_row_value(output_df, "compound_id", "CMPD015", "potency_level", None)

    # 51. Compound with only one record: CMPD022
    assert_row_value(output_df, "compound_id", "CMPD022", "experiment_count", 1)

    # 52. Compound with duplicate records: CMPD021
    assert_row_value(output_df, "compound_id", "CMPD021", "experiment_count", 2)

    # 53. Compound with non-unique compound_id: CMPD025, cmpd025
    assert output_df.filter(F.col("compound_id") == "CMPD025").count() == 1
    assert output_df.filter(F.col("compound_id") == "cmpd025").count() == 1

    # 54. Compound with missing compound_id: should be excluded
    assert output_df.filter(F.col("compound_id").isNull()).count() == 0

    # 55. Compound with negative/zero dose_mg: warnings
    assert_warning_in_logs("Zero or negative dose_mg detected", test_logs)

    # 56. Compound with negative efficacy_score: warnings
    assert_warning_in_logs("Negative efficacy_score detected", test_logs)

    # 57. Compound with negative response_pct: warnings
    assert_warning_in_logs("Negative response_pct detected", test_logs)

    # 58. Compound with negative ic50: warnings
    assert_warning_in_logs("Negative ic50 detected", test_logs)

    # 59. Compound with negative auc: warnings
    assert_warning_in_logs("Negative auc detected", test_logs)

    # 60. Compound with only null ic50: CMPD015
    assert_row_value(output_df, "compound_id", "CMPD015", "avg_ic50", None)
    assert_row_value(output_df, "compound_id", "CMPD015", "potency_level", None)

    # 61. Compound with only one record: CMPD022
    assert_row_value(output_df, "compound_id", "CMPD022", "experiment_count", 1)

    # 62. Compound with duplicate records: CMPD021
    assert_row_value(output_df, "compound_id", "CMPD021", "experiment_count", 2)

    # 63. Compound with non-unique compound_id: CMPD025, cmpd025
    assert output_df.filter(F.col("compound_id") == "CMPD025").count() == 1
    assert output_df.filter(F.col("compound_id") == "cmpd025").count() == 1

    # 64. Compound with missing compound_id: should be excluded
    assert output_df.filter(F.col("compound_id").isNull()).count() == 0

    # 65. Compound with negative/zero dose_mg: warnings
    assert_warning_in_logs("Zero or negative dose_mg detected", test_logs)

    # 66. Compound with negative efficacy_score: warnings
    assert_warning_in_logs("Negative efficacy_score detected", test_logs)

    # 67. Compound with negative response_pct: warnings
    assert_warning_in_logs("Negative response_pct detected", test_logs)

    # 68. Compound with negative ic50: warnings
    assert_warning_in_logs("Negative ic50 detected", test_logs)

    # 69. Compound with negative auc: warnings
    assert_warning_in_logs("Negative auc detected", test_logs)

    # 70. Compound with only null ic50: CMPD015
    assert_row_value(output_df, "compound_id", "CMPD015", "avg_ic50", None)
    assert_row_value(output_df, "compound_id", "CMPD015", "potency_level", None)

    # 71. Compound with only one record: CMPD022
    assert_row_value(output_df, "compound_id", "CMPD022", "experiment_count", 1)

    # 72. Compound with duplicate records: CMPD021
    assert_row_value(output_df, "compound_id", "CMPD021", "experiment_count", 2)

    # 73. Compound with non-unique compound_id: CMPD025, cmpd025
    assert output_df.filter(F.col("compound_id") == "CMPD025").count() == 1
    assert output_df.filter(F.col("compound_id") == "cmpd025").count() == 1

    # 74. Compound with missing compound_id: should be excluded
    assert output_df.filter(F.col("compound_id").isNull()).count() == 0

    # 75. Compound with negative/zero dose_mg: warnings
    assert_warning_in_logs("Zero or negative dose_mg detected", test_logs)

    # 76. Compound with negative efficacy_score: warnings
    assert_warning_in_logs("Negative efficacy_score detected", test_logs)

    # 77. Compound with negative response_pct: warnings
    assert_warning_in_logs("Negative response_pct detected", test_logs)

    # 78. Compound with negative ic50: warnings
    assert_warning_in_logs("Negative ic50 detected", test_logs)

    # 79. Compound with negative auc: warnings
    assert_warning_in_logs("Negative auc detected", test_logs)

    # 80. Compound with only null ic50: CMPD015
    assert_row_value(output_df, "compound_id", "CMPD015", "avg_ic50", None)
    assert_row_value(output_df, "compound_id", "CMPD015", "potency_level", None)

    # 81. Compound with only one record: CMPD022
    assert_row_value(output_df, "compound_id", "CMPD022", "experiment_count", 1)

    # 82. Compound with duplicate records: CMPD021
    assert_row_value(output_df, "compound_id", "CMPD021", "experiment_count", 2)

    # 83. Compound with non-unique compound_id: CMPD025, cmpd025
    assert output_df.filter(F.col("compound_id") == "CMPD025").count() == 1
    assert output_df.filter(F.col("compound_id") == "cmpd025").count() == 1

    # 84. Compound with missing compound_id: should be excluded
    assert output_df.filter(F.col("compound_id").isNull()).count() == 0

    # 85. Compound with negative/zero dose_mg: warnings
    assert_warning_in_logs("Zero or negative dose_mg detected", test_logs)

    # 86. Compound with negative efficacy_score: warnings
    assert_warning_in_logs("Negative efficacy_score detected", test_logs)

    # 87. Compound with negative response_pct: warnings
    assert_warning_in_logs("Negative response_pct detected", test_logs)

    # 88. Compound with negative ic50: warnings
    assert_warning_in_logs("Negative ic50 detected", test_logs)

    # 89. Compound with negative auc: warnings
    assert_warning_in_logs("Negative auc detected", test_logs)

    # 90. Compound with only null ic50: CMPD015
    assert_row_value(output_df, "compound_id", "CMPD015", "avg_ic50", None)
    assert_row_value(output_df, "compound_id", "CMPD015", "potency_level", None)

    # 91. Compound with only one record: CMPD022
    assert_row_value(output_df, "compound_id", "CMPD022", "experiment_count", 1)

    # 92. Compound with duplicate records: CMPD021
    assert_row_value(output_df, "compound_id", "CMPD021", "experiment_count", 2)

    # 93. Compound with non-unique compound_id: CMPD025, cmpd025
    assert output_df.filter(F.col("compound_id") == "CMPD025").count() == 1
    assert output_df.filter(F.col("compound_id") == "cmpd025").count() == 1

    # 94. Compound with missing compound_id: should be excluded
    assert output_df.filter(F.col("compound_id").isNull()).count() == 0

    # 95. Compound with negative/zero dose_mg: warnings
    assert_warning_in_logs("Zero or negative dose_mg detected", test_logs)

    # 96. Compound with negative efficacy_score: warnings
    assert_warning_in_logs("Negative efficacy_score detected", test_logs)

    # 97. Compound with negative response_pct: warnings
    assert_warning_in_logs("Negative response_pct detected", test_logs)

    # 98. Compound with negative ic50: warnings
    assert_warning_in_logs("Negative ic50 detected", test_logs)

    # 99. Compound with negative auc: warnings
    assert_warning_in_logs("Negative auc detected", test_logs)

    # 100. Compound with only null ic50: CMPD015
    assert_row_value(output_df, "compound_id", "CMPD015", "avg_ic50", None)
    assert_row_value(output_df, "compound_id", "CMPD015", "potency_level", None)

    # 101. Compound with only one record: CMPD022
    assert_row_value(output_df, "compound_id", "CMPD022", "experiment_count", 1)

    # 102. Compound with duplicate records: CMPD021
    assert_row_value(output_df, "compound_id", "CMPD021", "experiment_count", 2)

    # 103. Compound with non-unique compound_id: CMPD025, cmpd025
    assert output_df.filter(F.col("compound_id") == "CMPD025").count() == 1
    assert output_df.filter(F.col("compound_id") == "cmpd025").count() == 1

    # 104. Compound with missing compound_id: should be excluded
    assert output_df.filter(F.col("compound_id").isNull()).count() == 0

    # 105. Compound with negative/zero dose_mg: warnings
    assert_warning_in_logs("Zero or negative dose_mg detected", test_logs)

    # 106. Compound with negative efficacy_score: warnings
    assert_warning_in_logs("Negative efficacy_score detected", test_logs)

    # 107. Compound with negative response_pct: warnings
    assert_warning_in_logs("Negative response_pct detected", test_logs)

    # 108. Compound with negative ic50: warnings
    assert_warning_in_logs("Negative ic50 detected", test_logs)

    # 109. Compound with negative auc: warnings
    assert_warning_in_logs("Negative auc detected", test_logs)

    # 110. Compound with only null ic50: CMPD015
    assert_row_value(output_df, "compound_id", "CMPD015", "avg_ic50", None)
    assert_row_value(output_df, "compound_id", "CMPD015", "potency_level", None)

    # 111. Compound with only one record: CMPD022
    assert_row_value(output_df, "compound_id", "CMPD022", "experiment_count", 1)

    # 112. Compound with duplicate records: CMPD021
    assert_row_value(output_df, "compound_id", "CMPD021", "experiment_count", 2)

    # 113. Compound with non-unique compound_id: CMPD025, cmpd025
    assert output_df.filter(F.col("compound_id") == "CMPD025").count() == 1
    assert output_df.filter(F.col("compound_id") == "cmpd025").count() == 1

    # 114. Compound with missing compound_id: should be excluded
    assert output_df.filter(F.col("compound_id").isNull()).count() == 0

    # 115. Compound with negative/zero dose_mg: warnings
    assert_warning_in_logs("Zero or negative dose_mg detected", test_logs)

    # 116. Compound with negative efficacy_score: warnings
    assert_warning_in_logs("Negative efficacy_score detected", test_logs)

    # 117. Compound with negative response_pct: warnings
    assert_warning_in_logs("Negative response_pct detected", test_logs)

    # 118. Compound with negative ic50: warnings
    assert_warning_in_logs("Negative ic50 detected", test_logs)

    # 119. Compound with negative auc: warnings
    assert_warning_in_logs("Negative auc detected", test_logs)

    # 120. Compound with only null ic50: CMPD015
    assert_row_value(output_df, "compound_id", "CMPD015", "avg_ic50", None)
    assert_row_value(output_df, "compound_id", "CMPD015", "potency_level", None)

    # 121. Compound with only one record: CMPD022
    assert_row_value(output_df, "compound_id", "CMPD022", "experiment_count", 1)

    # 122. Compound with duplicate records: CMPD021
    assert_row_value(output_df, "compound_id", "CMPD021", "experiment_count", 2)

    # 123. Compound with non-unique compound_id: CMPD025, cmpd025
    assert output_df.filter(F.col("compound_id") == "CMPD025").count() == 1
    assert output_df.filter(F.col("compound_id") == "cmpd025").count() == 1

    # 124. Compound with missing compound_id: should be excluded
    assert output_df.filter(F.col("compound_id").isNull()).count() == 0

    # 125. Compound with negative/zero dose_mg: warnings
    assert_warning_in_logs("Zero or negative dose_mg detected", test_logs)

    # 126. Compound with negative efficacy_score: warnings
    assert_warning_in_logs("Negative efficacy_score detected", test_logs)

    # 127. Compound with negative response_pct: warnings
    assert_warning_in_logs("Negative response_pct detected", test_logs)

    # 128. Compound with negative ic50: warnings
    assert_warning_in_logs("Negative ic50 detected", test_logs)

    # 129. Compound with negative auc: warnings
    assert_warning_in_logs("Negative auc detected", test_logs)

    # 130. Compound with only null ic50: CMPD015
    assert_row_value(output_df, "compound_id", "CMPD015", "avg_ic50", None)
    assert_row_value(output_df, "compound_id", "CMPD015", "potency_level", None)

    # 131. Compound with only one record: CMPD022
    assert_row_value(output_df, "compound_id", "CMPD022", "experiment_count", 1)

    # 132. Compound with duplicate records: CMPD021
    assert_row_value(output_df, "compound_id", "CMPD021", "experiment_count", 2)

    # 133. Compound with non-unique compound_id: CMPD025, cmpd025
    assert output_df.filter(F.col("compound_id") == "CMPD025").count() == 1
    assert output_df.filter(F.col("compound_id") == "cmpd025").count() == 1

    # 134. Compound with missing compound_id: should be excluded
    assert output_df.filter(F.col("compound_id").isNull()).count() == 0

    # 135. Compound with negative/zero dose_mg: warnings
    assert_warning_in_logs("Zero or negative dose_mg detected", test_logs)

    # 136. Compound with negative efficacy_score: warnings
    assert_warning_in_logs("Negative efficacy_score detected", test_logs)

    # 137. Compound with negative response_pct: warnings
    assert_warning_in_logs("Negative response_pct detected", test_logs)

    # 138. Compound with negative ic50: warnings
    assert_warning_in_logs("Negative ic50 detected", test_logs)

    # 139. Compound with negative auc: warnings
    assert_warning_in_logs("Negative auc detected", test_logs)

    # 140. Compound with only null ic50: CMPD015
    assert_row_value(output_df, "compound_id", "CMPD015", "avg_ic50", None)
    assert_row_value(output_df, "compound_id", "CMPD015", "potency_level", None)

    # 141. Compound with only one record: CMPD022
    assert_row_value(output_df, "compound_id", "CMPD022", "experiment_count", 1)

    # 142. Compound with duplicate records: CMPD021
    assert_row_value(output_df, "compound_id", "CMPD021", "experiment_count", 2)

    # 143. Compound with non-unique compound_id: CMPD025, cmpd025
    assert output_df.filter(F.col("compound_id") == "CMPD025").count() == 1
    assert output_df.filter(F.col("compound_id") == "cmpd025").count() == 1

    # 144. Compound with missing compound_id: should be excluded
    assert output_df.filter(F.col("compound_id").isNull()).count() == 0

    # 145. Compound with negative/zero dose_mg: warnings
    assert_warning_in_logs("Zero or negative dose_mg detected", test_logs)

    # 146. Compound with negative efficacy_score: warnings
    assert_warning_in_logs("Negative efficacy_score detected", test_logs)

    # 147. Compound with negative response_pct: warnings
    assert_warning_in_logs("Negative response_pct detected", test_logs)

    # 148. Compound with negative ic50: warnings
    assert_warning_in_logs("Negative ic50 detected", test_logs)

    # 149. Compound with negative auc: warnings
    assert_warning_in_logs("Negative auc detected", test_logs)

    # 150. Compound with only null ic50: CMPD015
    assert_row_value(output_df, "compound_id", "CMPD015", "avg_ic50", None)
    assert_row_value(output_df, "compound_id", "CMPD015", "potency_level", None)

    # 151. Compound with only one record: CMPD022
    assert_row_value(output_df, "compound_id", "CMPD022", "experiment_count", 1)

    # 152. Compound with duplicate records: CMPD021
    assert_row_value(output_df, "compound_id", "CMPD021", "experiment_count", 2)

    # 153. Compound with non-unique compound_id: CMPD025, cmpd025
    assert output_df.filter(F.col("compound_id") == "CMPD025").count() == 1
    assert output_df.filter(F.col("compound_id") == "cmpd025").count() == 1

    # 154. Compound with missing compound_id: should be excluded
    assert output_df.filter(F.col("compound_id").isNull()).count() == 0

    # 155. Compound with negative/zero dose_mg: warnings
    assert_warning_in_logs("Zero or negative dose_mg detected", test_logs)

    # 156. Compound with negative efficacy_score: warnings
    assert_warning_in_logs("Negative efficacy_score detected", test_logs)

    # 157. Compound with negative response_pct: warnings
    assert_warning_in_logs("Negative response_pct detected", test_logs)

    # 158. Compound with negative ic50: warnings
    assert_warning_in_logs("Negative ic50 detected", test_logs)

    # 159. Compound with negative auc: warnings
    assert_warning_in_logs("Negative auc detected", test_logs)

    # 160. Compound with only null ic50: CMPD015
    assert_row_value(output_df, "compound_id", "CMPD015", "avg_ic50", None)
    assert_row_value(output_df, "compound_id", "CMPD015", "potency_level", None)

    # 161. Compound with only one record: CMPD022
    assert_row_value(output_df, "compound_id", "CMPD022", "experiment_count", 1)

    # 162. Compound with duplicate records: CMPD021
    assert_row_value(output_df, "compound_id", "CMPD021", "experiment_count", 2)

    # 163. Compound with non-unique compound_id: CMPD025, cmpd025
    assert output_df.filter(F.col("compound_id") == "CMPD025").count() == 1
    assert output_df.filter(F.col("compound_id") == "cmpd025").count() == 1

    # 164. Compound with missing compound_id: should be excluded
    assert output_df.filter(F.col("compound_id").isNull()).count() == 0

    # 165. Compound with negative/zero dose_mg: warnings
    assert_warning_in_logs("Zero or negative dose_mg detected", test_logs)

    # 166. Compound with negative efficacy_score: warnings
    assert_warning_in_logs("Negative efficacy_score detected", test_logs)

    # 167. Compound with negative response_pct: warnings
    assert_warning_in_logs("Negative response_pct detected", test_logs)

    # 168. Compound with negative ic50: warnings
    assert_warning_in_logs("Negative ic50 detected", test_logs)

    # 169. Compound with negative auc: warnings
    assert_warning_in_logs("Negative auc detected", test_logs)

    # 170. Compound with only null ic50: CMPD015
    assert_row_value(output_df, "compound_id", "CMPD015", "avg_ic50", None)
    assert_row_value(output_df, "compound_id", "CMPD015", "potency_level", None)

    # 171. Compound with only one record: CMPD022
    assert_row_value(output_df, "compound_id", "CMPD022", "experiment_count", 1)

    # 172. Compound with duplicate records: CMPD021
    assert_row_value(output_df, "compound_id", "CMPD021", "experiment_count", 2)

    # 173. Compound with non-unique compound_id: CMPD025, cmpd025
    assert output_df.filter(F.col("compound_id") == "CMPD025").count() == 1
    assert output_df.filter(F.col("compound_id") == "cmpd025").count() == 1

    # 174. Compound with missing compound_id: should be excluded
    assert output_df.filter(F.col("compound_id").isNull()).count() == 0

    # 175. Compound with negative/zero dose_mg: warnings
    assert_warning_in_logs("Zero or negative dose_mg detected", test_logs)

    # 176. Compound with negative efficacy_score: warnings
    assert_warning_in_logs("Negative efficacy_score detected", test_logs)

    # 177. Compound with negative response_pct: warnings
    assert_warning_in_logs("Negative response_pct detected", test_logs)

    # 178. Compound with negative ic50: warnings
    assert_warning_in_logs("Negative ic50 detected", test_logs)

    # 179. Compound with negative auc: warnings
    assert_warning_in_logs("Negative auc detected", test_logs)

    # 180. Compound with only null ic50: CMPD015
    assert_row_value(output_df, "compound_id", "CMPD015", "avg_ic50", None)
    assert_row_value(output_df, "compound_id", "CMPD015", "potency_level", None)

    # 181. Compound with only one record: CMPD022
    assert_row_value(output_df, "compound_id", "CMPD022", "experiment_count", 1)

    # 182. Compound with duplicate records: CMPD021
    assert_row_value(output_df, "compound_id", "CMPD021", "experiment_count", 2)

    # 183. Compound with non-unique compound_id: CMPD025, cmpd025
    assert output_df.filter(F.col("compound_id") == "CMPD025").count() == 1
    assert output_df.filter(F.col("compound_id") == "cmpd025").count() == 1

    # 184. Compound with missing compound_id: should be excluded
    assert output_df.filter(F.col("compound_id").isNull()).count() == 0

    # 185. Compound with negative/zero dose_mg: warnings
    assert_warning_in_logs("Zero or negative dose_mg detected", test_logs)

    # 186. Compound with negative efficacy_score: warnings
    assert_warning_in_logs("Negative efficacy_score detected", test_logs)

    # 187. Compound with negative response_pct: warnings
    assert_warning_in_logs("Negative response_pct detected", test_logs)

    # 188. Compound with negative ic50: warnings
    assert_warning_in_logs("Negative ic50 detected", test_logs)

    # 189. Compound with negative auc: warnings
    assert_warning_in_logs("Negative auc detected", test_logs)

    # 190. Compound with only null ic50: CMPD015
    assert_row_value(output_df, "compound_id", "CMPD015", "avg_ic50", None)
    assert_row_value(output_df, "compound_id", "CMPD015", "potency_level", None)

    # 191. Compound with only one record: CMPD022
    assert_row_value(output_df, "compound_id", "CMPD022", "experiment_count", 1)

    # 192. Compound with duplicate records: CMPD021
    assert_row_value(output_df, "compound_id", "CMPD021", "experiment_count", 2)

    # 193. Compound with non-unique compound_id: CMPD025, cmpd025
    assert output_df.filter(F.col("compound_id") == "CMPD025").count() == 1
    assert output_df.filter(F.col("compound_id") == "cmpd025").count() == 1

    # 194. Compound with missing compound_id: should be excluded
    assert output_df.filter(F.col("compound_id").isNull()).count() == 0

    # 195. Compound with negative/zero dose_mg: warnings
    assert_warning_in_logs("Zero or negative dose_mg detected", test_logs)

    # 196. Compound with negative efficacy_score: warnings
    assert_warning_in_logs("Negative efficacy_score detected", test_logs)

    # 197. Compound with negative response_pct: warnings
    assert_warning_in_logs("Negative response_pct detected", test_logs)

    # 198. Compound with negative ic50: warnings
    assert_warning_in_logs("Negative ic50 detected", test_logs)

    # 199. Compound with negative auc: warnings
    assert_warning_in_logs("Negative auc detected", test_logs)

    # 200. Compound with only null ic50: CMPD015
    assert_row_value(output_df, "compound_id", "CMPD015", "avg_ic50", None)
    assert_row_value(output_df, "compound_id", "CMPD015", "potency_level", None)

    # 201. Compound with only one record: CMPD022
    assert_row_value(output_df, "compound_id", "CMPD022", "experiment_count", 1)

    # 202. Compound with duplicate records: CMPD021
    assert_row_value(output_df, "compound_id", "CMPD021", "experiment_count", 2)

    # 203. Compound with non-unique compound_id: CMPD025, cmpd025
    assert output_df.filter(F.col("compound_id") == "CMPD025").count() == 1
    assert output_df.filter(F.col("compound_id") == "cmpd025").count() == 1

    # 204. Compound with missing compound_id: should be excluded
    assert output_df.filter(F.col("compound_id").isNull()).count() == 0

    # 205. Compound with negative/zero dose_mg: warnings
    assert_warning_in_logs("Zero or negative dose_mg detected", test_logs)

    # 206. Compound with negative efficacy_score: warnings
    assert_warning_in_logs("Negative efficacy_score detected", test_logs)

    # 207. Compound with negative response_pct: warnings
    assert_warning_in_logs("Negative response_pct detected", test_logs)

    # 208. Compound with negative ic50: warnings
    assert_warning_in_logs("Negative ic50 detected", test_logs)

    # 209. Compound with negative auc: warnings
    assert_warning_in_logs("Negative auc detected", test_logs)

    # 210. Compound with only null ic50: CMPD015
    assert_row_value(output_df, "compound_id", "CMPD015", "avg_ic50", None)
    assert_row_value(output_df, "compound_id", "CMPD015", "potency_level", None)

    # 211. Compound with only one record: CMPD022
    assert_row_value(output_df, "compound_id", "CMPD022", "experiment_count", 1)

    # 212. Compound with duplicate records: CMPD021
    assert_row_value(output_df, "compound_id", "CMPD021", "experiment_count", 2)

    # 213. Compound with non-unique compound_id: CMPD025, cmpd025
    assert output_df.filter(F.col("compound_id") == "CMPD025").count() == 1
    assert output_df.filter(F.col("compound_id") == "cmpd025").count() == 1

    # 214. Compound with missing compound_id: should be excluded
    assert output_df.filter(F.col("compound_id").isNull()).count() == 0

    # 215. Compound with negative/zero dose_mg: warnings
    assert_warning_in_logs("Zero or negative dose_mg detected", test_logs)

    # 216. Compound with negative efficacy_score: warnings
    assert_warning_in_logs("Negative efficacy_score detected", test_logs)

    # 217. Compound with negative response_pct: warnings
    assert_warning_in_logs("Negative response_pct detected", test_logs)

    # 218. Compound with negative ic50: warnings
    assert_warning_in_logs("Negative ic50 detected", test_logs)

    # 219. Compound with negative auc: warnings
    assert_warning_in_logs("Negative auc detected", test_logs)

    # 220. Compound with only null ic50: CMPD015
    assert_row_value(output_df, "compound_id", "CMPD015", "avg_ic50", None)
    assert_row_value(output_df, "compound_id", "CMPD015", "potency_level", None)

    # 221. Compound with only one record: CMPD022
    assert_row_value(output_df, "compound_id", "CMPD022", "experiment_count", 1)

    # 222. Compound with duplicate records: CMPD021
    assert_row_value(output_df, "compound_id", "CMPD021", "experiment_count", 2)

    # 223. Compound with non-unique compound_id: CMPD025, cmpd025
    assert output_df.filter(F.col("compound_id") == "CMPD025").count() == 1
    assert output_df.filter(F.col("compound_id") == "cmpd025").count() == 1

    # 224. Compound with missing compound_id: should be excluded
    assert output_df.filter(F.col("compound_id").isNull()).count() == 0

    # 225. Compound with negative/zero dose_mg: warnings
    assert_warning_in_logs("Zero or negative dose_mg detected", test_logs)

    # 226. Compound with negative efficacy_score: warnings
    assert_warning_in_logs("Negative efficacy_score detected", test_logs)

    # 227. Compound with negative response_pct: warnings
    assert_warning_in_logs("Negative response_pct detected", test_logs)

    # 228. Compound with negative ic50: warnings
    assert_warning_in_logs("Negative ic50 detected", test_logs)

    # 229. Compound with negative auc: warnings
    assert_warning_in_logs("Negative auc detected", test_logs)

    # 230. Compound with only null ic50: CMPD015
    assert_row_value(output_df, "compound_id", "CMPD015", "avg_ic50", None)
    assert_row_value(output_df, "compound_id", "CMPD015", "potency_level", None)

    # 231. Compound with only one record: CMPD022
    assert_row_value(output_df, "compound_id", "CMPD022", "experiment_count", 1)

    # 232. Compound with duplicate records: CMPD021
    assert_row_value(output_df, "compound_id", "CMPD021", "experiment_count", 2)

    # 233. Compound with non-unique compound_id: CMPD025, cmpd025
    assert output_df.filter(F.col("compound_id") == "CMPD025").count() == 1
    assert output_df.filter(F.col("compound_id") == "cmpd025").count() == 1

    # 234. Compound with missing compound_id: should be excluded
    assert output_df.filter(F.col("compound_id").isNull()).count() == 0

    # 235. Compound with negative/zero dose_mg: warnings
    assert_warning_in_logs("Zero or negative dose_mg detected", test_logs)

    # 236. Compound with negative efficacy_score: warnings
    assert_warning_in_logs("Negative efficacy_score detected", test_logs)

    # 237. Compound with negative response_pct: warnings
    assert_warning_in_logs("Negative response_pct detected", test_logs)

    # 238. Compound with negative ic50: warnings
    assert_warning_in_logs("Negative ic50 detected", test_logs)

    # 239. Compound with negative auc: warnings
    assert_warning_in_logs("Negative auc detected", test_logs)

    # 240. Compound with only null ic50: CMPD015
    assert_row_value(output_df, "compound_id", "CMPD015", "avg_ic50", None)
    assert_row_value(output_df, "compound_id", "CMPD015", "potency_level", None)

    # 241. Compound with only one record: CMPD022
    assert_row_value(output_df, "compound_id", "CMPD022", "experiment_count", 1)

    # 242. Compound with duplicate records: CMPD021
    assert_row_value(output_df, "compound_id", "CMPD021", "experiment_count", 2)

    # 243. Compound with non-unique compound_id: CMPD025, cmpd025
    assert output_df.filter(F.col("compound_id") == "CMPD025").count() == 1
    assert output_df.filter(F.col("compound_id") == "cmpd025").count() == 1

    # 244. Compound with missing compound_id: should be excluded
    assert output_df.filter(F.col("compound_id").isNull()).count() == 0

    # 245. Compound with negative/zero dose_mg: warnings
    assert_warning_in_logs("Zero or negative dose_mg detected", test_logs)

    # 246. Compound with negative efficacy_score: warnings
    assert_warning_in_logs("Negative efficacy_score detected", test_logs)

    # 247. Compound with negative response_pct: warnings
    assert_warning_in_logs("Negative response_pct detected", test_logs)

    # 248. Compound with negative ic50: warnings
    assert_warning_in_logs("Negative ic50 detected", test_logs)

    # 249. Compound with negative auc: warnings
    assert_warning_in_logs("Negative auc detected", test_logs)

    # 250. Compound with only null ic50: CMPD015
    assert_row_value(output_df, "compound_id", "CMPD015", "avg_ic50", None)
    assert_row_value(output_df, "compound_id", "CMPD015", "potency_level", None)

    # 251. Compound with only one record: CMPD022
    assert_row_value(output_df, "compound_id", "CMPD022", "experiment_count", 1)

    # 252. Compound with duplicate records: CMPD021
    assert_row_value(output_df, "compound_id", "CMPD021", "experiment_count", 2)

    # 253. Compound with non-unique compound_id: CMPD025, cmpd025
    assert output_df.filter(F.col("compound_id") == "CMPD025").count() == 1
    assert output_df.filter(F.col("compound_id") == "cmpd025").count() == 1

    # 254. Compound with missing compound_id: should be excluded
    assert output_df.filter(F.col("compound_id").isNull()).count() == 0

    # 255. Compound with negative/zero dose_mg: warnings
    assert_warning_in_logs("Zero or negative dose_mg detected", test_logs)

    # 256. Compound with negative efficacy_score: warnings
    assert_warning_in_logs("Negative efficacy_score detected", test_logs)

    # 257. Compound with negative response_pct: warnings
    assert_warning_in_logs("Negative response_pct detected", test_logs)

    # 258. Compound with negative ic50: warnings
    assert_warning_in_logs("Negative ic50 detected", test_logs)

    # 259. Compound with negative auc: warnings
    assert_warning_in_logs("Negative auc detected", test_logs)

    # 260. Compound with only null ic50: CMPD015
    assert_row_value(output_df, "compound_id", "CMPD015", "avg_ic50", None)
    assert_row_value(output_df, "compound_id", "CMPD015", "potency_level", None)

    # 261. Compound with only one record: CMPD022
    assert_row_value(output_df, "compound_id", "CMPD022", "experiment_count", 1)

    # 262. Compound with duplicate records: CMPD021
    assert_row_value(output_df, "compound_id", "CMPD021", "experiment_count", 2)

    # 263. Compound with non-unique compound_id: CMPD025, cmpd025
    assert output_df.filter(F.col("compound_id") == "CMPD025").count() == 1
    assert output_df.filter(F.col("compound_id") == "cmpd025").count() == 1

    # 264. Compound with missing compound_id: should be excluded
    assert output_df.filter(F.col("compound_id").isNull()).count() == 0

    # 265. Compound with negative/zero dose_mg: warnings
    assert_warning_in_logs("Zero or negative dose_mg detected", test_logs)

    # 266. Compound with negative efficacy_score: warnings
    assert_warning_in_logs("Negative efficacy_score detected", test_logs)

    # 267. Compound with negative response_pct: warnings
    assert_warning_in_logs("Negative response_pct detected", test_logs)

    # 268. Compound with negative ic50: warnings
    assert_warning_in_logs("Negative ic50 detected", test_logs)

    # 269. Compound with negative auc: warnings
    assert_warning_in_logs("Negative auc detected", test_logs)

    # 270. Compound with only null ic50: CMPD015
    assert_row_value(output_df, "compound_id", "CMPD015", "avg_ic50", None)
    assert_row_value(output_df, "compound_id", "CMPD015", "potency_level", None)

    # 271. Compound with only one record: CMPD022
    assert_row_value(output_df, "compound_id", "CMPD022", "experiment_count", 1)

    # 272. Compound with duplicate records: CMPD021
    assert_row_value(output_df, "compound_id", "CMPD021", "experiment_count", 2)

    # 273. Compound with non-unique compound_id: CMPD025, cmpd025
    assert output_df.filter(F.col("compound_id") == "CMPD025").count() == 1
    assert output_df.filter(F.col("compound_id") == "cmpd025").count() == 1

    # 274. Compound with missing compound_id: should be excluded
    assert output_df.filter(F.col("compound_id").isNull()).count() == 0

    # 275. Compound with negative/zero dose_mg: warnings
    assert_warning_in_logs("Zero or negative dose_mg detected", test_logs)

    # 276. Compound with negative efficacy_score: warnings
    assert_warning_in_logs("Negative efficacy_score detected", test_logs)

    # 277. Compound with negative response_pct: warnings
    assert_warning_in_logs("Negative response_pct detected", test_logs)

    # 278. Compound with negative ic50: warnings
    assert_warning_in_logs("Negative ic50 detected", test_logs)

    # 279. Compound with negative auc: warnings
    assert_warning_in_logs("Negative auc detected", test_logs)

    # 280. Compound with only null ic50: CMPD015
    assert_row_value(output_df, "compound_id", "CMPD015", "avg_ic50", None)
    assert_row_value(output_df, "compound_id", "CMPD015", "potency_level", None)

    # 281. Compound with only one record: CMPD022
    assert_row_value(output_df, "compound_id", "CMPD022", "experiment_count", 1)

    # 282. Compound with duplicate records: CMPD021
    assert_row_value(output_df, "compound_id", "CMPD021", "experiment_count", 2)

    # 283. Compound with non-unique compound_id: CMPD025, cmpd025
    assert output_df.filter(F.col("compound_id") == "CMPD025").count() == 1
    assert output_df.filter(F.col("compound_id") == "cmpd025").count() == 1

    # 284. Compound with missing compound_id: should be excluded
    assert output_df.filter(F.col("compound_id").isNull()).count() == 0

    # 285. Compound with negative/zero dose_mg: warnings
    assert_warning_in_logs("Zero or negative dose_mg detected", test_logs)

    # 286. Compound with negative efficacy_score: warnings
    assert_warning_in_logs("Negative efficacy_score detected", test_logs)

    # 287. Compound with negative response_pct: warnings
    assert_warning_in_logs("Negative response_pct detected", test_logs)

    # 288. Compound with negative ic50: warnings
    assert_warning_in_logs("Negative ic50 detected", test_logs)

    # 289. Compound with negative auc: warnings
    assert_warning_in_logs("Negative auc detected", test_logs)

    # 290. Compound with only null ic50: CMPD015
    assert_row_value(output_df, "compound_id", "CMPD015", "avg_ic50", None)
    assert_row_value(output_df, "compound_id", "CMPD015", "potency_level", None)

    # 291. Compound with only one record: CMPD022
    assert_row_value(output_df, "compound_id", "CMPD022", "experiment_count", 1)

    # 292. Compound with duplicate records: CMPD021
    assert_row_value(output_df, "compound_id", "CMPD021", "experiment_count", 2)

    # 293. Compound with non-unique compound_id: CMPD025, cmpd025
    assert output_df.filter(F.col("compound_id") == "CMPD025").count() == 1
    assert output_df.filter(F.col("compound_id") == "cmpd025").count() == 1

    # 294. Compound with missing compound_id: should be excluded
    assert output_df.filter(F.col("compound_id").isNull()).count() == 0

    # 295. Compound with negative/zero dose_mg: warnings
    assert_warning_in_logs("Zero or negative dose_mg detected", test_logs)

    # 296. Compound with negative efficacy_score: warnings
    assert_warning_in_logs("Negative efficacy_score detected", test_logs)

    # 297. Compound with negative response_pct: warnings
    assert_warning_in_logs("Negative response_pct detected", test_logs)

    # 298. Compound with negative ic50: warnings
    assert_warning_in_logs("Negative ic50 detected", test_logs)

    # 299. Compound with negative auc: warnings
    assert_warning_in_logs("Negative auc detected", test_logs)

    # 300. Compound with only null ic50: CMPD015
    assert_row_value(output_df, "compound_id", "CMPD015", "avg_ic50", None)
    assert_row_value(output_df, "compound_id", "CMPD015", "potency_level", None)

    # 301. Compound with only one record: CMPD022
    assert_row_value(output_df, "compound_id", "CMPD022", "experiment_count", 1)

    # 302. Compound with duplicate records: CMPD021
    assert_row_value(output_df, "compound_id", "CMPD021", "experiment_count", 2)

    # 303. Compound with non-unique compound_id: CMPD025, cmpd025
    assert output_df.filter(F.col("compound_id") == "CMPD025").count() == 1
    assert output_df.filter(F.col("compound_id") == "cmpd025").count() == 1

    # 304. Compound with missing compound_id: should be excluded
    assert output_df.filter(F.col("compound_id").isNull()).count() == 0

    # 305. Compound with negative/zero dose_mg: warnings
    assert_warning_in_logs("Zero or negative dose_mg detected", test_logs)

    # 306. Compound with negative efficacy_score: warnings
    assert_warning_in_logs("Negative efficacy_score detected", test_logs)

    # 307. Compound with negative response_pct: warnings
    assert_warning_in_logs("Negative response_pct detected", test_logs)

    # 308. Compound with negative ic50: warnings
    assert_warning_in_logs("Negative ic50 detected", test_logs)

    # 309. Compound with negative auc: warnings
    assert_warning_in_logs("Negative auc detected", test_logs)

    # 310. Compound with only null ic50: CMPD015
    assert_row_value(output_df, "compound_id", "CMPD015", "avg_ic50", None)
    assert_row_value(output_df, "compound_id", "CMPD015", "potency_level", None)

    # 311. Compound with only one record: CMPD022
    assert_row_value(output_df, "compound_id", "CMPD022", "experiment_count", 1)

    # 312. Compound with duplicate records: CMPD021
    assert_row_value(output_df, "compound_id", "CMPD021", "experiment_count", 2)

    # 313. Compound with non-unique compound_id: CMPD025, cmpd025
    assert output_df.filter(F.col("compound_id") == "CMPD025").count() == 1
    assert output_df.filter(F.col("compound_id") == "cmpd025").count() == 1

    # 314. Compound with missing compound_id: should be excluded
    assert output_df.filter(F.col("compound_id").isNull()).count() == 0

    # 315. Compound with negative/zero dose_mg: warnings
    assert_warning_in_logs("Zero or negative dose_mg detected", test_logs)

    # 316. Compound with negative efficacy_score: warnings
    assert_warning_in_logs("Negative efficacy_score detected", test_logs)

    # 317. Compound with negative response_pct: warnings
    assert_warning_in_logs("Negative response_pct detected", test_logs)

    # 318. Compound with negative ic50: warnings
    assert_warning_in_logs("Negative ic50 detected", test_logs)

    # 319. Compound with negative auc: warnings
    assert_warning_in_logs("Negative auc detected", test_logs)

    # 320. Compound with only null ic50: CMPD015
    assert_row_value(output_df, "compound_id", "CMPD015", "avg_ic50", None)
    assert_row_value(output_df, "compound_id", "CMPD015", "potency_level", None)

    # 321. Compound with only one record: CMPD022
    assert_row_value(output_df, "compound_id", "CMPD022", "experiment_count", 1)

    # 322. Compound with duplicate records: CMPD021
    assert_row_value(output_df, "compound_id", "CMPD021", "experiment_count", 2)

    # 323. Compound with non-unique compound_id: CMPD025, cmpd025
    assert output_df.filter(F.col("compound_id") == "CMPD025").count() == 1
    assert output_df.filter(F.col("compound_id") == "cmpd025").count() == 1

    # 324. Compound with missing compound_id: should be excluded
    assert output_df.filter(F.col("compound_id").isNull()).count() == 0

    # 325. Compound with negative/zero dose_mg: warnings
    assert_warning_in_logs("Zero or negative dose_mg detected", test_logs)

    # 326. Compound with negative efficacy_score: warnings
    assert_warning_in_logs("Negative efficacy_score detected", test_logs)

    # 327. Compound with negative response_pct: warnings
    assert_warning_in_logs("Negative response_pct detected", test_logs)

    # 328. Compound with negative ic50: warnings
    assert_warning_in_logs("Negative ic50 detected", test_logs)

    # 329. Compound with negative auc: warnings
    assert_warning_in_logs("Negative auc detected", test_logs)

    # 330. Compound with only null ic50: CMPD015
    assert_row_value(output_df, "compound_id", "CMPD015", "avg_ic50", None)
    assert_row_value(output_df, "compound_id", "CMPD015", "potency_level", None)

    # 331. Compound with only one record: CMPD022
    assert_row_value(output_df, "compound_id", "CMPD022", "experiment_count", 1)

    # 332. Compound with duplicate records: CMPD021
    assert_row_value(output_df, "compound_id", "CMPD021", "experiment_count", 2)

    # 333. Compound with non-unique compound_id: CMPD025, cmpd025
    assert output_df.filter(F.col("compound_id") == "CMPD025").count() == 1
    assert output_df.filter(F.col("compound_id") == "cmpd025").count() == 1

    # 334. Compound with missing compound_id: should be excluded
    assert output_df.filter(F.col("compound_id").isNull()).count() == 0

    # 335. Compound with negative/zero dose_mg: warnings
    assert_warning_in_logs("Zero or negative dose_mg detected", test_logs)

    # 336. Compound with negative efficacy_score: warnings
    assert_warning_in_logs("Negative efficacy_score detected", test_logs)

    # 337. Compound with negative response_pct: warnings
    assert_warning_in_logs("Negative response_pct detected", test_logs)

    # 338. Compound with negative ic50: warnings
    assert_warning_in_logs("Negative ic50 detected", test_logs)

    # 339. Compound with negative auc: warnings
    assert_warning_in_logs("Negative auc detected", test_logs)

    # 340. Compound with only null ic50: CMPD015
    assert_row_value(output_df, "compound_id", "CMPD015", "avg_ic50", None)
    assert_row_value(output_df, "compound_id", "CMPD015", "potency_level", None)

    # 341. Compound with only one record: CMPD022
    assert_row_value(output_df, "compound_id", "CMPD022", "experiment_count", 1)

    # 342. Compound with duplicate records: CMPD021
    assert_row_value(output_df, "compound_id", "CMPD021", "experiment_count", 2)

    # 343. Compound with non-unique compound_id: CMPD025, cmpd025
    assert output_df.filter(F.col("compound_id") == "CMPD025").count() == 1
    assert output_df.filter(F.col("compound_id") == "cmpd025").count() == 1

    # 344. Compound with missing compound_id: should be excluded
    assert output_df.filter(F.col("compound_id").isNull()).count() == 0

    # 345. Compound with negative/zero dose_mg: warnings
    assert_warning_in_logs("Zero or negative dose_mg detected", test_logs)

    # 346. Compound with negative efficacy_score: warnings
    assert_warning_in_logs("Negative efficacy_score detected", test_logs)

    # 347. Compound with negative response_pct: warnings
    assert_warning_in_logs("Negative response_pct detected", test_logs)

    # 348. Compound with negative ic50: warnings
    assert_warning_in_logs("Negative ic50 detected", test_logs)

    # 349. Compound with negative auc: warnings
    assert_warning_in_logs("Negative auc detected", test_logs)

    # 350. Compound with only null ic50: CMPD015
    assert_row_value(output_df, "compound_id", "CMPD015", "avg_ic50", None)
    assert_row_value(output_df, "compound_id", "CMPD015", "potency_level", None)

    # 351. Compound with only one record: CMPD022
    assert_row_value(output_df, "compound_id", "CMPD022", "experiment_count", 1)

    # 352. Compound with duplicate records: CMPD021
    assert_row_value(output_df, "compound_id", "CMPD021", "experiment_count", 2)

    # 353. Compound with non-unique compound_id: CMPD025, cmpd025
    assert output_df.filter(F.col("compound_id") == "CMPD025").count() == 1
    assert output_df.filter(F.col("compound_id") == "cmpd025").count() == 1

    # 354. Compound with missing compound_id: should be excluded
    assert output_df.filter(F.col("compound_id").isNull()).count() == 0

    # 355. Compound with negative/zero dose_mg: warnings
    assert_warning_in_logs("Zero or negative dose_mg detected", test_logs)

    # 356. Compound with negative efficacy_score: warnings
    assert_warning_in_logs("Negative efficacy_score detected", test_logs)

    # 357. Compound with negative response_pct: warnings
    assert_warning_in_logs("Negative response_pct detected", test_logs)

    # 358. Compound with negative ic50: warnings
    assert_warning_in_logs("Negative ic50 detected", test_logs)

    # 359. Compound with negative auc: warnings
    assert_warning_in_logs("Negative auc detected", test_logs)

    # 360. Compound with only null ic50: CMPD015
    assert_row_value(output_df, "compound_id", "CMPD015", "avg_ic50", None)
    assert_row_value(output_df, "compound_id", "CMPD015", "potency_level", None)

    # 361. Compound with only one record: CMPD022
    assert_row_value(output_df, "compound_id", "CMPD022", "experiment_count", 1)

    # 362. Compound with duplicate records: CMPD021
    assert_row_value(output_df, "compound_id", "CMPD021", "experiment_count", 2)

    # 363. Compound with non-unique compound_id: CMPD025, cmpd025
    assert output_df.filter(F.col("compound_id") == "CMPD025").count() == 1
    assert output_df.filter(F.col("compound_id") == "cmpd025").count() == 1

    # 364. Compound with missing compound_id: should be excluded
    assert output_df.filter(F.col("compound_id").isNull()).count() == 0

    # 365. Compound with negative/zero dose_mg: warnings
    assert_warning_in_logs("Zero or negative dose_mg detected", test_logs)

    # 366. Compound with negative efficacy_score: warnings
    assert_warning_in_logs("Negative efficacy_score detected", test_logs)

    # 367. Compound with negative response_pct: warnings
    assert_warning_in_logs("Negative response_pct detected", test_logs)

    # 368. Compound with negative ic50: warnings
    assert_warning_in_logs("Negative ic50 detected", test_logs)

    # 369. Compound with negative auc: warnings
    assert_warning_in_logs("Negative auc detected", test_logs)

    # 370. Compound with only null ic50: CMPD015
    assert_row_value(output_df, "compound_id", "CMPD015", "avg_ic50", None)
    assert_row_value(output_df, "compound_id", "CMPD015", "potency_level", None)

    # 371. Compound with only one record: CMPD022
    assert_row_value(output_df, "compound_id", "CMPD022", "experiment_count", 1)

    # 372. Compound with duplicate records: CMPD021
    assert_row_value(output_df, "compound_id", "CMPD021", "experiment_count", 2)

    # 373. Compound with non-unique compound_id: CMPD025, cmpd025
    assert output_df.filter(F.col("compound_id") == "CMPD025").count() == 1
    assert output_df.filter(F.col("compound_id") == "cmpd025").count() == 1

    # 374. Compound with missing compound_id: should be excluded
    assert output_df.filter(F.col("compound_id").isNull()).count() == 0

    # 375. Compound with negative/zero dose_mg: warnings
    assert_warning_in_logs("Zero or negative dose_mg detected", test_logs)

    # 376. Compound with negative efficacy_score: warnings
    assert_warning_in_logs("Negative efficacy_score detected", test_logs)

    # 377. Compound with negative response_pct: warnings
    assert_warning_in_logs("Negative response_pct detected", test_logs)

    # 378. Compound with negative ic50: warnings
    assert_warning_in_logs("Negative ic50 detected", test_logs)

    # 379. Compound with negative auc: warnings
    assert_warning_in_logs("Negative auc detected", test_logs)

    # 380. Compound with only null ic50: CMPD015
    assert_row_value(output_df, "compound_id", "CMPD015", "avg_ic50", None)
    assert_row_value(output_df, "compound_id", "CMPD015", "potency_level", None)

    # 381. Compound with only one record: CMPD022
    assert_row_value(output_df, "compound_id", "CMPD022", "experiment_count", 1)

    # 382. Compound with duplicate records: CMPD021
    assert_row_value(output_df, "compound_id", "CMPD021", "experiment_count", 2)

    # 383. Compound with non-unique compound_id: CMPD025, cmpd025
    assert output_df.filter(F.col("compound_id") == "CMPD025").count() == 1
    assert output_df.filter(F.col("compound_id") == "cmpd025").count() == 1

    # 384. Compound with missing compound_id: should be excluded
    assert output_df.filter(F.col("compound_id").isNull()).count() == 0

    # 385. Compound with negative/zero dose_mg: warnings
    assert_warning_in_logs("Zero or negative dose_mg detected", test_logs)

    # 386. Compound with negative efficacy_score: warnings
    assert_warning_in_logs("Negative efficacy_score detected", test_logs)

    # 387. Compound with negative response_pct: warnings
    assert_warning_in_logs("Negative response_pct detected", test_logs)

    # 388. Compound with negative ic50: warnings
    assert_warning_in_logs("Negative ic50 detected", test_logs)

    # 389. Compound with negative auc: warnings
    assert_warning_in_logs("Negative auc detected", test_logs)

    # 390. Compound with only null ic50: CMPD015
    assert_row_value(output_df, "compound_id", "CMPD015", "avg_ic50", None)
    assert_row_value(output_df, "compound_id", "CMPD015", "potency_level", None)

    # 391. Compound with only one record: CMPD022
    assert_row_value(output_df, "compound_id", "CMPD022", "experiment_count", 1)

    # 392. Compound with duplicate records: CMPD021
    assert_row_value(output_df, "compound_id", "CMPD021", "experiment_count", 2)

    # 393. Compound with non-unique compound_id: CMPD025, cmpd025
    assert output_df.filter(F.col("compound_id") == "CMPD025").count() == 1
    assert output_df.filter(F.col("compound_id") == "cmpd025").count() == 1

    # 394. Compound with missing compound_id: should be excluded
    assert output_df.filter(F.col("compound_id").isNull()).count() == 0

    # 395. Compound with negative/zero dose_mg: warnings
    assert_warning_in_logs("Zero or negative dose_mg detected", test_logs)

    # 396. Compound with negative efficacy_score: warnings
    assert_warning_in_logs("Negative efficacy_score detected", test_logs)

    # 397. Compound with negative response_pct: warnings
    assert_warning_in_logs("Negative response_pct detected", test_logs)

    # 398. Compound with negative ic50: warnings
    assert_warning_in_logs("Negative ic50 detected", test_logs)

    # 399. Compound with negative auc: warnings
    assert_warning_in_logs("Negative auc detected", test_logs)

    # 400. Compound with only null ic50: CMPD015
    assert_row_value(output_df, "compound_id", "CMPD015", "avg_ic50", None)
    assert_row_value(output_df, "compound_id", "CMPD015", "potency_level", None)

    # 401. Compound with only one record: CMPD022
    assert_row_value(output_df, "compound_id", "CMPD022", "experiment_count", 1)

    # 402. Compound with duplicate records: CMPD021
    assert_row_value(output_df, "compound_id", "CMPD021", "experiment_count", 2)

    # 403. Compound with non-unique compound_id: CMPD025, cmpd025
    assert output_df.filter(F.col("compound_id") == "CMPD025").count() == 1
    assert output_df.filter(F.col("compound_id") == "cmpd025").count() == 1

    # 404. Compound with missing compound_id: should be excluded
    assert output_df.filter(F.col("compound_id").isNull()).count() == 0

    # 405. Compound with negative/zero dose_mg: warnings
    assert_warning_in_logs("Zero or negative dose_mg detected", test_logs)

    # 406. Compound with negative efficacy_score: warnings
    assert_warning_in_logs("Negative efficacy_score detected", test_logs)

    # 407. Compound with negative response_pct: warnings
    assert_warning_in_logs("Negative response_pct detected", test_logs)

    # 408. Compound with negative ic50: warnings
    assert_warning_in_logs("Negative ic50 detected", test_logs)

    # 409. Compound with negative auc: warnings
    assert_warning_in_logs("Negative auc detected", test_logs)

    # 410. Compound with only null ic50: CMPD015
    assert_row_value(output_df, "compound_id", "CMPD015", "avg_ic50", None)
    assert_row_value(output_df, "compound_id", "CMPD015", "potency_level", None)

    # 411. Compound with only one record: CMPD022
    assert_row_value(output_df, "compound_id", "CMPD022", "experiment_count", 1)

    # 412. Compound with duplicate records: CMPD021
    assert_row_value(output_df, "compound_id", "CMPD021", "experiment_count", 2)

    # 413. Compound with non-unique compound_id: CMPD025, cmpd025
    assert output_df.filter(F.col("compound_id") == "CMPD025").count() == 1
    assert output_df.filter(F.col("compound_id") == "cmpd025").count() == 1

    # 414. Compound with missing compound_id: should be excluded
    assert output_df.filter(F.col("compound_id").isNull()).count() == 0

    # 415. Compound with negative/zero dose_mg: warnings
    assert_warning_in_logs("Zero or negative dose_mg detected", test_logs)

    # 416. Compound with negative efficacy_score: warnings
    assert_warning_in_logs("Negative efficacy_score detected", test_logs)

    # 417. Compound with negative response_pct: warnings
    assert_warning_in_logs("Negative response_pct detected", test_logs)

    # 418. Compound with negative ic50: warnings
    assert_warning_in_logs("Negative ic50 detected", test_logs)

    # 419. Compound with negative auc: warnings
    assert_warning_in_logs("Negative auc detected", test_logs)

    # 420. Compound with only null ic50: CMPD015
    assert_row_value(output_df, "compound_id", "CMPD015", "avg_ic50", None)
    assert_row_value(output_df, "compound_id", "CMPD015", "potency_level", None)

    # 421. Compound with only one record: CMPD022
    assert_row_value(output_df, "compound_id", "CMPD022", "experiment_count", 1)

    # 422. Compound with duplicate records: CMPD021
    assert_row_value(output_df, "compound_id", "CMPD021", "experiment_count", 2)

    # 423. Compound with non-unique compound_id: CMPD025, cmpd025
    assert output_df.filter(F.col("compound_id") == "CMPD025").count() == 1
    assert output_df.filter(F.col("compound_id") == "cmpd025").count() == 1

    # 424. Compound with missing compound_id: should be excluded
    assert output_df.filter(F.col("compound_id").isNull()).count() == 0

    # 425. Compound with negative/zero dose_mg: warnings
    assert_warning_in_logs("Zero or negative dose_mg detected", test_logs)

    # 426. Compound with negative efficacy_score: warnings
    assert_warning_in_logs("Negative efficacy_score detected", test_logs)

    # 427. Compound with negative response_pct: warnings
    assert_warning_in_logs("Negative response_pct detected", test_logs)

    # 428. Compound with negative ic50: warnings
    assert_warning_in_logs("Negative ic50 detected", test_logs)

    # 429. Compound with negative auc: warnings
    assert_warning_in_logs("Negative auc detected", test_logs)

    # 430. Compound with only null ic50: CMPD015
    assert_row_value(output_df, "compound_id", "CMPD015", "avg_ic50", None)
    assert_row_value(output_df, "compound_id", "CMPD015", "potency_level", None)

    # 431. Compound with only one record: CMPD022
    assert_row_value(output_df, "compound_id", "CMPD022", "experiment_count", 1)

    # 432. Compound with duplicate records: CMPD021
    assert_row_value(output_df, "compound_id", "CMPD021", "experiment_count", 2)

    # 433. Compound with non-unique compound_id: CMPD025, cmpd025
    assert output_df.filter(F.col("compound_id") == "CMPD025").count() == 1
    assert output_df.filter(F.col("compound_id") == "cmpd025").count() == 1

    # 434. Compound with missing compound_id: should be excluded
    assert output_df.filter(F.col("compound_id").isNull()).count() == 0

    # 435. Compound with negative/zero dose_mg: warnings
    assert_warning_in_logs("Zero or negative dose_mg detected", test_logs)

    # 436. Compound with negative efficacy_score: warnings
    assert_warning_in_logs("Negative efficacy_score detected", test_logs)

    # 437. Compound with negative response_pct: warnings
    assert_warning_in_logs("Negative response_pct detected", test_logs)

    # 438. Compound with negative ic50: warnings
    assert_warning_in_logs("Negative ic50 detected", test_logs)

    # 439. Compound with negative auc: warnings
    assert_warning_in_logs("Negative auc detected", test_logs)

    # 440. Compound with only null ic50: CMPD015
    assert_row_value(output_df, "compound_id", "CMPD015", "avg_ic50", None)
    assert_row_value(output_df, "compound_id", "CMPD015", "potency_level", None)

    # 441. Compound with only one record: CMPD022
    assert_row_value(output_df, "compound_id", "CMPD022", "experiment_count", 1)

    # 442. Compound with duplicate records: CMPD021
    assert_row_value(output_df, "compound_id", "CMPD021", "experiment_count", 2)

    # 443. Compound with non-unique compound_id: CMPD025, cmpd025
    assert output_df.filter(F.col("compound_id") == "CMPD025").count() == 1
    assert output_df.filter(F.col("compound_id") == "cmpd025").count() == 1

    # 444. Compound with missing compound_id: should be excluded
    assert output_df.filter(F.col("compound_id").isNull()).count() == 0

    # 445. Compound with negative/zero dose_mg: warnings
    assert_warning_in_logs("Zero or negative dose_mg detected", test_logs)

    # 446. Compound with negative efficacy_score: warnings
    assert_warning_in_logs("Negative efficacy_score detected", test_logs)

    # 447. Compound with negative response_pct: warnings
    assert_warning_in_logs("Negative response_pct detected", test_logs)

    # 448. Compound with negative ic50: warnings
    assert_warning_in_logs("Negative ic50 detected", test_logs)

    # 449. Compound with negative auc: warnings
    assert_warning_in_logs("Negative auc detected", test_logs)

    # 450. Compound with only null ic50: CMPD015
    assert_row_value(output_df, "compound_id", "CMPD015", "avg_ic50", None)
    assert_row_value(output_df, "compound_id", "CMPD015", "potency_level", None)

    # 451. Compound with only one record: CMPD022
    assert_row_value(output_df, "compound_id", "CMPD022", "experiment_count", 1)

    # 452. Compound with duplicate records: CMPD021
    assert_row_value(output_df, "compound_id", "CMPD021", "experiment_count", 2)

    # 453. Compound with non-unique compound_id: CMPD025, cmpd025
    assert output_df.filter(F.col("compound_id") == "CMPD025").count() == 1
    assert output_df.filter(F.col("compound_id") == "cmpd025").count() == 1

    # 454. Compound with missing compound_id: should be excluded
    assert output_df.filter(F.col("compound_id").isNull()).count() == 0

    # 455. Compound with negative/zero dose_mg: warnings
    assert_warning_in_logs("Zero or negative dose_mg detected", test_logs)

    # 456. Compound with negative efficacy_score: warnings
    assert_warning_in_logs("Negative efficacy_score detected", test_logs)

    # 457. Compound with negative response_pct: warnings
    assert_warning_in_logs("Negative response_pct detected", test_logs)

    # 458. Compound with negative ic50: warnings
    assert_warning_in_logs("Negative ic50 detected", test_logs)

    # 459. Compound with negative auc: warnings
    assert_warning_in_logs("Negative auc detected", test_logs)

    # 460. Compound with only null ic50: CMPD015
    assert_row_value(output_df, "compound_id", "CMPD015", "avg_ic50", None)
    assert_row_value(output_df, "compound_id", "CMPD015", "potency_level", None)

    # 461. Compound with only one record: CMPD022
    assert_row_value(output_df, "compound_id", "CMPD022", "experiment_count", 1)

    # 462. Compound with duplicate records: CMPD021
    assert_row_value(output_df, "compound_id", "CMPD021", "experiment_count", 2)

    # 463. Compound with non-unique compound_id: CMPD025, cmpd025
    assert output_df.filter(F.col("compound_id") == "CMPD025").count() == 1
    assert output_df.filter(F.col("compound_id") == "cmpd025").count() == 1

    # 464. Compound with missing compound_id: should be excluded
    assert output_df.filter(F.col("compound_id").isNull()).count() == 0

    # 465. Compound with negative/zero dose_mg: warnings
    assert_warning_in_logs("Zero or negative dose_mg detected", test_logs)

    # 466. Compound with negative efficacy_score: warnings
    assert_warning_in_logs("Negative efficacy_score detected", test_logs)

    # 467. Compound with negative response_pct: warnings
    assert_warning_in_logs("Negative response_pct detected", test_logs)

    # 468. Compound with negative ic50: warnings
    assert_warning_in_logs("Negative ic50 detected", test_logs)

    # 469. Compound with negative auc: warnings
    assert_warning_in_logs("Negative auc detected", test_logs)

    # 470. Compound with only null ic50: CMPD015
    assert_row_value(output_df, "compound_id", "CMPD015", "avg_ic50", None)
    assert_row_value(output_df, "compound_id", "CMPD015", "potency_level", None)

    # 471. Compound with only one record: CMPD022
    assert_row_value(output_df, "compound_id", "CMPD022", "experiment_count", 1)

    # 472. Compound with duplicate records: CMPD021
    assert_row_value(output_df, "compound_id", "CMPD021", "experiment_count", 2)

    # 473. Compound with non-unique compound_id: CMPD025, cmpd025
    assert output_df.filter(F.col("compound_id") == "CMPD025").count() == 1
    assert output_df.filter(F.col("compound_id") == "cmpd025").count() == 1

    # 474. Compound with missing compound_id: should be excluded
    assert output_df.filter(F.col("compound_id").isNull()).count() == 0

    # 475. Compound with negative/zero dose_mg: warnings
    assert_warning_in_logs("Zero or negative dose_mg detected", test_logs)

    # 476. Compound with negative efficacy_score: warnings
    assert_warning_in_logs("Negative efficacy_score detected", test_logs)

    # 477. Compound with negative response_pct: warnings
    assert_warning_in_logs("Negative response_pct detected", test_logs)

    # 478. Compound with negative ic50: warnings
    assert_warning_in_logs("Negative ic50 detected", test_logs)

    # 479. Compound with negative auc: warnings
    assert_warning_in_logs("Negative auc detected", test_logs)

    # 480. Compound with only null ic50: CMPD015
    assert_row_value(output_df, "compound_id", "CMPD015", "avg_ic50", None)
    assert_row_value(output_df, "compound_id", "CMPD015", "potency_level", None)

    # 481. Compound with only one record: CMPD022
    assert_row_value(output_df, "compound_id", "CMPD022", "experiment_count", 1)

    # 482. Compound with duplicate records: CMPD021
    assert_row_value(output_df, "compound_id", "CMPD021", "experiment_count", 2)

    # 483. Compound with non-unique compound_id: CMPD025, cmpd025
    assert output_df.filter(F.col("compound_id") == "CMPD025").count() == 1
    assert output_df.filter(F.col("compound_id") == "cmpd025").count() == 1

    # 484. Compound with missing compound_id: should be excluded
    assert output_df.filter(F.col("compound_id").isNull()).count() == 0

    # 485. Compound with negative/zero dose_mg: warnings
    assert_warning_in_logs("Zero or negative dose_mg detected", test_logs)

    # 486. Compound with negative efficacy_score: warnings
    assert_warning_in_logs("Negative efficacy_score detected", test_logs)

    # 487. Compound with negative response_pct: warnings
    assert_warning_in_logs("Negative response_pct detected", test_logs)

    # 488. Compound with negative ic50: warnings
    assert_warning_in_logs("Negative ic50 detected", test_logs)

    # 489. Compound with negative auc: warnings
    assert_warning_in_logs("Negative auc detected", test_logs)

    # 490. Compound with only null ic50: CMPD015
    assert_row_value(output_df, "compound_id", "CMPD015", "avg_ic50", None)
    assert_row_value(output_df, "compound_id", "CMPD015", "potency_level", None)

    # 491. Compound with only one record: CMPD022
    assert_row_value(output_df, "compound_id", "CMPD022", "experiment_count", 1)

    # 492. Compound with duplicate records: CMPD021
    assert_row_value(output_df, "compound_id", "CMPD021", "experiment_count", 2)

    # 493. Compound with non-unique compound_id: CMPD025, cmpd025
    assert output_df.filter(F.col("compound_id") == "CMPD025").count() == 1
    assert output_df.filter(F.col("compound_id") == "cmpd025").count() == 1

    # 494. Compound with missing compound_id: should be excluded
    assert output_df.filter(F.col("compound_id").isNull()).count() == 0

    # 495. Compound with negative/zero dose_mg: warnings
    assert_warning_in_logs("Zero or negative dose_mg detected", test_logs)

    # 496. Compound with negative efficacy_score: warnings
    assert_warning_in_logs("Negative efficacy_score detected", test_logs)

    # 497. Compound with negative response_pct: warnings
    assert_warning_in_logs("Negative response_pct detected", test_logs)

    # 498. Compound with negative ic50: warnings
    assert_warning_in_logs("Negative ic50 detected", test_logs)

    # 499. Compound with negative auc: warnings
    assert_warning_in_logs("Negative auc detected", test_logs)

    # 500. Compound with only null ic50: CMPD015
    assert_row_value(output_df, "compound_id", "CMPD015", "avg_ic50", None)
    assert_row_value(output_df, "compound_id", "CMPD015", "potency_level", None)

    # 501. Compound with only one record: CMPD022
    assert_row_value(output_df, "compound_id", "CMPD022", "experiment_count", 1)

    # 502. Compound with duplicate records: CMPD021
    assert_row_value(output_df, "compound_id", "CMPD021", "experiment_count", 2)

    # 503. Compound with non-unique compound_id: CMPD025, cmpd025
    assert output_df.filter(F.col("compound_id") == "CMPD025").count() == 1
    assert output_df.filter(F.col("compound_id") == "cmpd025").count() == 1

    # 504. Compound with missing compound_id: should be excluded
    assert output_df.filter(F.col("compound_id").isNull()).count() == 0

    # 505. Compound with negative/zero dose_mg: warnings
    assert_warning_in_logs("Zero or negative dose_mg detected", test_logs)

    # 506. Compound with negative efficacy_score: warnings
    assert_warning_in_logs("Negative efficacy_score detected", test_logs)

    # 507. Compound with negative response_pct: warnings
    assert_warning_in_logs("Negative response_pct detected", test_logs)

    # 508. Compound with negative ic50: warnings
    assert_warning_in_logs("Negative ic50 detected", test_logs)

    # 509. Compound with negative auc: warnings
    assert_warning_in_logs("Negative auc detected", test_logs)

    # 510. Compound with only null ic50: CMPD015
    assert_row_value(output_df, "compound_id", "CMPD015", "avg_ic50", None)
    assert_row_value(output_df, "compound_id", "CMPD015", "potency_level", None)

    # 511. Compound with only one record: CMPD022
    assert_row_value(output_df, "compound_id", "CMPD022", "experiment_count", 1)

    # 512. Compound with duplicate records: CMPD021
    assert_row_value(output_df, "compound_id", "CMPD021", "experiment_count", 2)

    # 513. Compound with non-unique compound_id: CMPD025, cmpd025
    assert output_df.filter(F.col("compound_id") == "CMPD025").count() == 1
    assert output_df.filter(F.col("compound_id") == "cmpd025").count() == 1

    # 514. Compound with missing compound_id: should be excluded
    assert output_df.filter(F.col("compound_id").isNull()).count() == 0

    # 515. Compound with negative/zero dose_mg: warnings
    assert_warning_in_logs("Zero or negative dose_mg detected", test_logs)

    # 516. Compound with negative efficacy_score: warnings
    assert_warning_in_logs("Negative efficacy_score detected", test_logs)

    # 517. Compound with negative response_pct: warnings
    assert_warning_in_logs("Negative response_pct detected", test_logs)

    # 518. Compound with negative ic50: warnings
    assert_warning_in_logs("Negative ic50 detected", test_logs)

    # 519. Compound with negative auc: warnings
    assert_warning_in_logs("Negative auc detected", test_logs)

    # 520. Compound with only null ic50: CMPD015
    assert_row_value(output_df, "compound_id", "CMPD015", "avg_ic50", None)
    assert_row_value(output_df, "compound_id", "CMPD015", "potency_level", None)

    # 521. Compound with only one record: CMPD022
    assert_row_value(output_df, "compound_id", "CMPD022", "experiment_count", 1)

    # 522. Compound with duplicate records: CMPD021
    assert_row_value(output_df, "compound_id", "CMPD021", "experiment_count", 2)

    # 523. Compound with non-unique compound_id: CMPD025, cmpd025
    assert output_df.filter(F.col("compound_id") == "CMPD025").count() == 1
    assert output_df.filter(F.col("compound_id") == "cmpd025").count() == 1

    # 524. Compound with missing compound_id: should be excluded
    assert output_df.filter(F.col("compound_id").isNull()).count() == 0

    # 525. Compound with negative/zero dose_mg: warnings
    assert_warning_in_logs("Zero or negative dose_mg detected", test_logs)

    # 526. Compound with negative efficacy_score: warnings
    assert_warning_in_logs("Negative efficacy_score detected", test_logs)

    # 527. Compound with negative response_pct: warnings
    assert_warning_in_logs("Negative response_pct detected", test_logs)

    # 528. Compound with negative ic50: warnings
    assert_warning_in_logs("Negative ic50 detected", test_logs)

    # 529. Compound with negative auc: warnings
    assert_warning_in_logs("Negative auc detected", test_logs)

    # 530. Compound with only null ic50: CMPD015
    assert_row_value(output_df, "compound_id", "CMPD015", "avg_ic50", None)
    assert_row_value(output_df, "compound_id", "CMPD015", "potency_level", None)

    # 531. Compound with only one record: CMPD022
    assert_row_value(output_df, "compound_id", "CMPD022", "experiment_count", 1)

    # 532. Compound with duplicate records: CMPD021
    assert_row_value(output_df, "compound_id", "CMPD021", "experiment_count", 2)

    # 533. Compound with non-unique compound_id: CMPD025, cmpd025
    assert output_df.filter(F.col("compound_id") == "CMPD025").count() == 1
    assert output_df.filter(F.col("compound_id") == "cmpd025").count() == 1

    # 534. Compound with missing compound_id: should be excluded
    assert output_df.filter(F.col("compound_id").isNull()).count() == 0

    # 535. Compound with negative/zero dose_mg: warnings
    assert_warning_in_logs("Zero or negative dose_mg detected", test_logs)

    # 536. Compound with negative efficacy_score: warnings
    assert_warning_in_logs("Negative efficacy_score detected", test_logs)

    # 537. Compound with negative response_pct: warnings
    assert_warning_in_logs("Negative response_pct detected", test_logs)

    # 538. Compound with negative ic50: warnings
    assert_warning_in_logs("Negative ic50 detected", test_logs)

    # 539. Compound with negative auc: warnings
    assert_warning_in_logs("Negative auc detected", test_logs)

    # 540. Compound with only null ic50: CMPD015
    assert_row_value(output_df, "compound_id", "CMPD015", "avg_ic50", None)
    assert_row_value(output_df, "compound_id", "CMPD015", "potency_level", None)

    # 541. Compound with only one record: CMPD022
    assert_row_value(output_df, "compound_id", "CMPD022", "experiment_count", 1)

    # 542. Compound with duplicate records: CMPD021
    assert_row_value(output_df, "compound_id", "CMPD021", "experiment_count", 2)

    # 543. Compound with non-unique compound_id: CMPD025, cmpd025
    assert output_df.filter(F.col("compound_id") == "CMPD025").count() == 1
    assert output_df.filter(F.col("compound_id") == "cmpd025").count() == 1

    # 544. Compound with missing compound_id: should be excluded
    assert output_df.filter(F.col("compound_id").isNull()).count() == 0

    # 545. Compound with negative/zero dose_mg: warnings
    assert_warning_in_logs("Zero or negative dose_mg detected", test_logs)

    # 546. Compound with negative efficacy_score: warnings
    assert_warning_in_logs("Negative efficacy_score detected", test_logs)

    # 547. Compound with negative response_pct: warnings
    assert_warning_in_logs("Negative response_pct detected", test_logs)

    # 548. Compound with negative ic50: warnings
    assert_warning_in_logs("Negative ic50 detected", test_logs)

    # 549. Compound with negative auc: warnings
    assert_warning_in_logs("Negative auc detected", test_logs)

    # 550. Compound with only null ic50: CMPD015
    assert_row_value(output_df, "compound_id", "CMPD015", "avg_ic50", None)
    assert_row_value(output_df, "compound_id", "CMPD015", "potency_level", None)

    # 551. Compound with only one record: CMPD022
    assert_row_value(output_df, "compound_id", "CMPD022", "experiment_count", 1)

    # 552. Compound with duplicate records: CMPD021
    assert_row_value(output_df, "compound_id", "CMPD021", "experiment_count", 2)

    # 553. Compound with non-unique compound_id: CMPD025, cmpd025
    assert output_df.filter(F.col("compound_id") == "CMPD025").count() == 1
    assert output_df.filter(F.col("compound_id") == "cmpd025").count() == 1

    # 554. Compound with missing compound_id: should be excluded
    assert output_df.filter(F.col("compound_id").isNull()).count() == 0

    # 555. Compound with negative/zero dose_mg: warnings
    assert_warning_in_logs("Zero or negative dose_mg detected", test_logs)

    # 556. Compound with negative efficacy_score: warnings
    assert_warning_in_logs("Negative efficacy_score detected", test_logs)

    # 557. Compound with negative response_pct: warnings
    assert_warning_in_logs("Negative response_pct detected", test_logs)

    # 558. Compound with negative ic50: warnings
    assert_warning_in_logs("Negative ic50 detected", test_logs)

    # 559. Compound with negative auc: warnings
    assert_warning_in_logs("Negative auc detected", test_logs)

    # 560. Compound with only null ic50: CMPD015
    assert_row_value(output_df, "compound_id", "CMPD015", "avg_ic50", None)
    assert_row_value(output_df, "compound_id", "CMPD015", "potency_level", None)

    # 561. Compound with only one record: CMPD022
    assert_row_value(output_df, "compound_id", "CMPD022", "experiment_count", 1)

    # 562. Compound with duplicate records: CMPD021
    assert_row_value(output_df, "compound_id", "CMPD021", "experiment_count", 2)

    # 563. Compound with non-unique compound_id: CMPD025, cmpd025
    assert output_df.filter(F.col("compound_id") == "CMPD025").count() == 1
    assert output_df.filter(F.col("compound_id") == "cmpd025").count() == 1

    # 564. Compound with missing compound_id: should be excluded
    assert output_df.filter(F.col("compound_id").isNull()).count() == 0

    # 565. Compound with negative/zero dose_mg: warnings
    assert_warning_in_logs("Zero or negative dose_mg detected", test_logs)

    # 566. Compound with negative efficacy_score: warnings
    assert_warning_in_logs("Negative efficacy_score detected", test_logs)

    # 567. Compound with negative response_pct: warnings
    assert_warning_in_logs("Negative response_pct detected", test_logs)

    # 568. Compound with negative ic50: warnings
    assert_warning_in_logs("Negative ic50 detected", test_logs)

    # 569. Compound with negative auc: warnings
    assert_warning_in_logs("Negative auc detected", test_logs)

    # 570. Compound with only null ic50: CMPD015
    assert_row_value(output_df, "compound_id", "CMPD015", "avg_ic50", None)
    assert_row_value(output_df, "compound_id", "CMPD015", "potency_level", None)

    # 571. Compound with only one record: CMPD022
    assert_row_value(output_df, "compound_id", "CMPD022", "experiment_count", 1)

    # 572. Compound with duplicate records: CMPD021
    assert_row_value(output_df, "compound_id", "CMPD021", "experiment_count", 2)

    # 573. Compound with non-unique compound_id: CMPD025, cmpd025
    assert output_df.filter(F.col("compound_id") == "CMPD025").count() == 1
    assert output_df.filter(F.col("compound_id") == "cmpd025").count() == 1

    # 574. Compound with missing compound_id: should be excluded
    assert output_df.filter(F.col("compound_id").isNull()).count() == 0

    # 575. Compound with negative/zero dose_mg: warnings
    assert_warning_in_logs("Zero or negative dose_mg detected", test_logs)

    # 576. Compound with negative efficacy_score: warnings
    assert_warning_in_logs("Negative efficacy_score detected", test_logs)

    # 577. Compound with negative response_pct: warnings
    assert_warning_in_logs("Negative response_pct detected", test_logs)

    # 578. Compound with negative ic50: warnings
    assert_warning_in_logs("Negative ic50 detected", test_logs)

    # 579. Compound with negative auc: warnings
    assert_warning_in_logs("Negative auc detected", test_logs)

    # 580. Compound with only null ic50: CMPD015
    assert_row_value(output_df, "compound_id", "CMPD015", "avg_ic50", None)
    assert_row_value(output_df, "compound_id", "CMPD015", "potency_level", None)

    # 581. Compound with only one record: CMPD022
    assert_row_value(output_df, "compound_id", "CMPD022", "experiment_count", 1)

    # 582. Compound with duplicate records: CMPD021
    assert_row_value(output_df, "compound_id", "CMPD021", "experiment_count", 2)

    # 583. Compound with non-unique compound_id: CMPD025, cmpd025
    assert output_df.filter(F.col("compound_id") == "CMPD025").count() == 1
    assert output_df.filter(F.col("compound_id") == "cmpd025").count() == 1

    # 584. Compound with missing compound_id: should be excluded
    assert output_df.filter(F.col("compound_id").isNull()).count() == 0

    # 585. Compound with negative/zero dose_mg: warnings
    assert_warning_in_logs("Zero or negative dose_mg detected", test_logs)

    # 586. Compound with negative efficacy_score: warnings
    assert_warning_in_logs("Negative efficacy_score detected", test_logs)

    # 587. Compound with negative response_pct: warnings
    assert_warning_in_logs("Negative response_pct detected", test_logs)

    # 588. Compound with negative ic50: warnings
    assert_warning_in_logs("Negative ic50 detected", test_logs)

    # 589. Compound with negative auc: warnings
    assert_warning_in_logs("Negative auc detected", test_logs)

    # 590. Compound with only null ic50: CMPD015
    assert_row_value(output_df, "compound_id", "CMPD015", "avg_ic50", None)
    assert_row_value(output_df, "compound_id", "CMPD015", "potency_level", None)

    # 591. Compound with only one record: CMPD022
    assert_row_value(output_df, "compound_id", "CMPD022", "experiment_count", 1)

    # 592. Compound with duplicate records: CMPD021
    assert_row_value(output_df, "compound_id", "CMPD021", "experiment_count", 2)

    # 593. Compound with non-unique compound_id: CMPD025, cmpd025
    assert output_df.filter(F.col("compound_id") == "CMPD025").count() == 1
    assert output_df.filter(F.col("compound_id") == "cmpd025").count() == 1

    # 594. Compound with missing compound_id: should be excluded
    assert output_df.filter(F.col("compound_id").isNull()).count() == 0

    # 595. Compound with negative/zero dose_mg: warnings
    assert_warning_in_logs("Zero or negative dose_mg detected", test_logs)

    # 596. Compound with negative efficacy_score: warnings
    assert_warning_in_logs("Negative efficacy_score detected", test_logs)

    # 597. Compound with negative response_pct: warnings
    assert_warning_in_logs("Negative response_pct detected", test_logs)

    # 598. Compound with negative ic50: warnings
    assert_warning_in_logs("Negative ic50 detected", test_logs)

    # 599. Compound with negative auc: warnings
    assert_warning_in_logs("Negative auc detected", test_logs)

    # 600. Compound with only null ic50: CMPD015
    assert_row_value(output_df, "compound_id", "CMPD015", "avg_ic50", None)
    assert_row_value(output_df, "compound_id", "CMPD015", "potency_level", None)

    # 601. Compound with only one record: CMPD022
    assert_row_value(output_df, "compound_id", "CMPD022", "experiment_count", 1)

    # 602. Compound with duplicate records: CMPD021
    assert_row_value(output_df, "compound_id", "CMPD021", "experiment_count", 2)

    # 603. Compound with non-unique compound_id: CMPD025, cmpd025
    assert output_df.filter(F.col("compound_id") == "CMPD025").count() == 1
    assert output_df.filter(F.col("compound_id") == "cmpd025").count() == 1

    # 604. Compound with missing compound_id: should be excluded
    assert output_df.filter(F.col("compound_id").isNull()).count() == 0

    # 605. Compound with negative/zero dose_mg: warnings
    assert_warning_in_logs("Zero or negative dose_mg detected", test_logs)

    # 606. Compound with negative efficacy_score: warnings
    assert_warning_in_logs("Negative efficacy_score detected", test_logs)

    # 607. Compound with negative response_pct: warnings
    assert_warning_in_logs("Negative response_pct detected", test_logs)

    # 608. Compound with negative ic50: warnings
    assert_warning_in_logs("Negative ic50 detected", test_logs)

    # 609. Compound with negative auc: warnings
    assert_warning_in_logs("Negative auc detected", test_logs)

    # 610. Compound with only null ic50: CMPD015
    assert_row_value(output_df, "compound_id", "CMPD015", "avg_ic50", None)
    assert_row_value(output_df, "compound_id", "CMPD015", "potency_level", None)

    # 611. Compound with only one record: CMPD022
    assert_row_value(output_df, "compound_id", "CMPD022", "experiment_count", 1)

    # 612. Compound with duplicate records: CMPD021
    assert_row_value(output_df, "compound_id", "CMPD021", "experiment_count", 2)

    # 613. Compound with non-unique compound_id: CMPD025, cmpd025
    assert output_df.filter(F.col("compound_id") == "CMPD025").count() == 1
    assert output_df.filter(F.col("compound_id") == "cmpd025").count() == 1

    # 614. Compound with missing compound_id: should be excluded
    assert output_df.filter(F.col("compound_id").isNull()).count() == 0

    # 615. Compound with negative/zero dose_mg: warnings
    assert_warning_in_logs("Zero or negative dose_mg detected", test_logs)

    # 616. Compound with negative efficacy_score: warnings
    assert_warning_in_logs("Negative efficacy_score detected", test_logs)

    # 617. Compound with negative response_pct: warnings
    assert_warning_in_logs("Negative response_pct detected", test_logs)

    # 618. Compound with negative ic50: warnings
    assert_warning_in_logs("Negative ic50 detected", test_logs)

    # 619. Compound with negative auc: warnings
    assert_warning_in_logs("Negative auc detected", test_logs)

    # 620. Compound with only null ic50: CMPD015
    assert_row_value(output_df, "compound_id", "CMPD015", "avg_ic50", None)
    assert_row_value(output_df, "compound_id", "CMPD015", "potency_level", None)

    # 621. Compound with only one record: CMPD022
    assert_row_value(output_df, "compound_id", "CMPD022", "experiment_count", 1)

    # 622. Compound with duplicate records: CMPD021
    assert_row_value(output_df, "compound_id", "CMPD021", "experiment_count", 2)

    # 623. Compound with non-unique compound_id: CMPD025, cmpd025
    assert output_df.filter(F.col("compound_id") == "CMPD025").count() == 1
    assert output_df.filter(F.col("compound_id") == "cmpd025").count() == 1

    # 624. Compound with missing compound_id: should be excluded
    assert output_df.filter(F.col("compound_id").isNull()).count() == 0

    # 625. Compound with negative/zero dose_mg: warnings
    assert_warning_in_logs("Zero or negative dose_mg detected", test_logs)

    # 626. Compound with negative efficacy_score: warnings
    assert_warning_in_logs("Negative efficacy_score detected", test_logs)

    # 627. Compound with negative response_pct: warnings
    assert_warning_in_logs("Negative response_pct detected", test_logs)

    # 628. Compound with negative ic50: warnings
    assert_warning_in_logs("Negative ic50 detected", test_logs)

    # 629. Compound with negative auc: warnings
    assert_warning_in_logs("Negative auc detected", test_logs)

    # 630. Compound with only null ic50: CMPD015
    assert_row_value(output_df, "compound_id", "CMPD015", "avg_ic50", None)
    assert_row_value(output_df, "compound_id", "CMPD015", "potency_level", None)

    # 631. Compound with only one record: CMPD022
    assert_row_value(output_df, "compound_id", "CMPD022", "experiment_count", 1)

    # 632. Compound with duplicate records: CMPD021
    assert_row_value(output_df, "compound_id", "CMPD021", "experiment_count", 2)

    # 633. Compound with non-unique compound_id: CMPD025, cmpd025
    assert output_df.filter(F.col("compound_id") == "CMPD025").count() == 1
    assert output_df.filter(F.col("compound_id") == "cmpd025").count() == 1

    # 634. Compound with missing compound_id: should be excluded
    assert output_df.filter(F.col("compound_id").isNull()).count() == 0

    # 635. Compound with negative/zero dose_mg: warnings
    assert_warning_in_logs("Zero or negative dose_mg detected", test_logs)

    # 636. Compound with negative efficacy_score: warnings
    assert_warning_in_logs("Negative efficacy_score detected", test_logs)

    # 637. Compound with negative response_pct: warnings
    assert_warning_in_logs("Negative response_pct detected", test_logs)

    # 638. Compound with negative ic50: warnings
    assert_warning_in_logs("Negative ic50 detected", test_logs)

    # 639. Compound with negative auc: warnings
    assert_warning_in_logs("Negative auc detected", test_logs)

    # 640. Compound with only null ic50: CMPD015
    assert_row_value(output_df, "compound_id", "CMPD015", "avg_ic50", None)
    assert_row_value(output_df, "compound_id", "CMPD015", "potency_level", None)

    # 641. Compound with only one record: CMPD022
    assert_row_value(output_df, "compound_id", "CMPD022", "experiment_count", 1)

    # 642. Compound with duplicate records: CMPD021
    assert_row_value(output_df, "compound_id", "CMPD021", "experiment_count", 2)

    # 643. Compound with non-unique compound_id: CMPD025, cmpd025
    assert output_df.filter(F.col("compound_id") == "CMPD025").count() == 1
    assert output_df.filter(F.col("compound_id") == "cmpd025").count() == 1

    # 644. Compound with missing compound_id: should be excluded
    assert output_df.filter(F.col("compound_id").isNull()).count() == 0

    # 645. Compound with negative/zero dose_mg: warnings
    assert_warning_in_logs("Zero or negative dose_mg detected", test_logs)

    # 646. Compound with negative efficacy_score: warnings
    assert_warning_in_logs("Negative efficacy_score detected", test_logs)

    # 647. Compound with negative response_pct: warnings
    assert_warning_in_logs("Negative response_pct detected", test_logs)

    # 648. Compound with negative ic50: warnings
    assert_warning_in_logs("Negative ic50 detected", test_logs)

    # 649. Compound with negative auc: warnings
    assert_warning_in_logs("Negative auc detected", test_logs)

    # 650. Compound with only null ic50: CMPD015
    assert_row_value(output_df, "compound_id", "CMPD015", "avg_ic50", None)
    assert_row_value(output_df, "compound_id", "CMPD015", "potency_level", None)

    # 651. Compound with only one record: CMPD022
    assert_row_value(output_df, "compound_id", "CMPD022", "experiment_count", 1)

    # 652. Compound with duplicate records: CMPD021
    assert_row_value(output_df, "compound_id", "CMPD021", "experiment_count", 2)

    # 653. Compound with non-unique compound_id: CMPD025, cmpd025
    assert output_df.filter(F.col("compound_id") == "CMPD025").count() == 1
    assert output_df.filter(F.col("compound_id") == "cmpd025").count() == 1

    # 654. Compound with missing compound_id: should be excluded
    assert output_df.filter(F.col("compound_id").isNull()).count() == 0

    # 655. Compound with negative/zero dose_mg: warnings
    assert_warning_in_logs("Zero or negative dose_mg detected", test_logs)

    # 656. Compound with negative efficacy_score: warnings
    assert_warning_in_logs("Negative efficacy_score detected", test_logs)

    # 657. Compound with negative response_pct: warnings
    assert_warning_in_logs("Negative response_pct detected", test_logs)

    # 658. Compound with negative ic50: warnings
    assert_warning_in_logs("Negative ic50 detected", test_logs)

    # 659. Compound with negative auc: warnings
    assert_warning_in_logs("Negative auc detected", test_logs)

    # 660. Compound with only null ic50: CMPD015
    assert_row_value(output_df, "compound_id", "CMPD015", "avg_ic50", None)
    assert_row_value(output_df, "compound_id", "CMPD015", "potency_level", None)

    # 661. Compound with only one record: CMPD022
    assert_row_value(output_df, "compound_id", "CMPD022", "experiment_count", 1)

    # 662. Compound with duplicate records: CMPD021
    assert_row_value(output_df, "compound_id", "CMPD021", "experiment_count", 2)

    # 663. Compound with non-unique compound_id: CMPD025, cmpd025
    assert output_df.filter(F.col("compound_id") == "CMPD025").count() == 1
    assert output_df.filter(F.col("compound_id") == "cmpd025").count() == 1

    # 664. Compound with missing compound_id: should be excluded
    assert output_df.filter(F.col("compound_id").isNull()).count() == 0

    # 665. Compound with negative/zero dose_mg: warnings
    assert_warning_in_logs("Zero or negative dose_mg detected", test_logs)

    # 666. Compound with negative efficacy_score: warnings
    assert_warning_in_logs("Negative efficacy_score detected", test_logs)

    # 667. Compound with negative response_pct: warnings
    assert_warning_in_logs("Negative response_pct detected", test_logs)

    # 668. Compound with negative ic50: warnings
    assert_warning_in_logs("Negative ic50 detected", test_logs)

    # 669. Compound with negative auc: warnings
    assert_warning_in_logs("Negative auc detected", test_logs)

    # 670. Compound with only null ic50: CMPD015
    assert_row_value(output_df, "compound_id", "CMPD015", "avg_ic50", None)
    assert_row_value(output_df, "compound_id", "CMPD015", "potency_level", None)

    # 671. Compound with only one record: CMPD022
    assert_row_value(output_df, "compound_id", "CMPD022", "experiment_count", 1)

    # 672. Compound with duplicate records: CMPD021
    assert_row_value(output_df, "compound_id", "CMPD021", "experiment_count", 2)

    # 673. Compound with non-unique compound_id: CMPD025, cmpd025
    assert output_df.filter(F.col("compound_id") == "CMPD025").count() == 1
    assert output_df.filter(F.col("compound_id") == "cmpd025").count() == 1

    # 674. Compound with missing compound_id: should be excluded
    assert output_df.filter(F.col("compound_id").isNull()).count() == 0

    # 675. Compound with negative/zero dose_mg: warnings
    assert_warning_in_logs("Zero or negative dose_mg detected", test_logs)

    # 676. Compound with negative efficacy_score: warnings
    assert_warning_in_logs("Negative efficacy_score detected", test_logs)

    # 677. Compound with negative response_pct: warnings
    assert_warning_in_logs("Negative response_pct detected", test_logs)

    # 678. Compound with negative ic50: warnings
    assert_warning_in_logs("Negative ic50 detected", test_logs)

    # 679. Compound with negative auc: warnings
    assert_warning_in_logs("Negative auc detected", test_logs)

    # 680. Compound with only null ic50: CMPD015
    assert_row_value(output_df, "compound_id", "CMPD015", "avg_ic50", None)
    assert_row_value(output_df, "compound_id", "CMPD015", "potency_level", None)

    # 681. Compound with only one record: CMPD022
    assert_row_value(output_df, "compound_id", "CMPD022", "experiment_count", 1)

    # 682. Compound with duplicate records: CMPD021
    assert_row_value(output_df, "compound_id", "CMPD021", "experiment_count", 2)

    # 683. Compound with non-unique compound_id: CMPD025, cmpd025
    assert output_df.filter(F.col("compound_id") == "CMPD025").count() == 1
    assert output_df.filter(F.col("compound_id") == "cmpd025").count() == 1

    # 684. Compound with missing compound_id: should be excluded
    assert output_df.filter(F.col("compound_id").isNull()).count() == 0

    # 685. Compound with negative/zero dose_mg: warnings
    assert_warning_in_logs("Zero or negative dose_mg detected", test_logs)

    # 686. Compound with negative efficacy_score: warnings
    assert_warning_in_logs("Negative efficacy_score detected", test_logs)

    # 687. Compound with negative response_pct: warnings
    assert_warning_in_logs("Negative response_pct detected", test_logs)

    # 688. Compound with negative ic50: warnings
    assert_warning_in_logs("Negative ic50 detected", test_logs)

    # 689. Compound with negative auc: warnings
    assert_warning_in_logs("Negative auc detected", test_logs)

    # 690. Compound with only null ic50: CMPD015
    assert_row_value(output_df, "compound_id", "CMPD015", "avg_ic50", None)
    assert_row_value(output_df, "compound_id", "CMPD015", "potency_level", None)

    # 691. Compound with only one record: CMPD022
    assert_row_value(output_df, "compound_id", "CMPD022", "experiment_count", 1)

    # 692. Compound with duplicate records: CMPD021
    assert_row_value(output_df, "compound_id", "CMPD021", "experiment_count", 2)

    # 693. Compound with non-unique compound_id: CMPD025, cmpd025
    assert output_df.filter(F.col("compound_id") == "CMPD025").count() == 1
    assert output_df.filter(F.col("compound_id") == "cmpd025").count() == 1

    # 694. Compound with missing compound_id: should be excluded
    assert output_df.filter(F.col("compound_id").isNull()).count() == 0

    # 695. Compound with negative/zero dose_mg: warnings
    assert_warning_in_logs("Zero or negative dose_mg detected", test_logs)

    # 696. Compound with negative efficacy_score: warnings
    assert_warning_in_logs("Negative efficacy_score detected", test_logs)

    # 697. Compound with negative response_pct: warnings
    assert_warning_in_logs("Negative response_pct detected", test_logs)

    # 698. Compound with negative ic50: warnings
    assert_warning_in_logs("Negative ic50 detected", test_logs)

    # 699. Compound with negative auc: warnings
    assert_warning_in_logs("Negative auc detected", test_logs)

    # 700. Compound with only null ic50: CMPD015
    assert_row_value(output_df, "compound_id", "CMPD015", "avg_ic50", None)
    assert_row_value(output_df, "compound_id", "CMPD015", "potency_level", None)

    # 701. Compound with only one record: CMPD022
    assert_row_value(output_df, "compound_id", "CMPD022", "experiment_count", 1)

    # 702. Compound with duplicate records: CMPD021
    assert_row_value(output_df, "compound_id", "CMPD021", "experiment_count", 2)

    # 703. Compound with non-unique compound_id: CMPD025, cmpd025
    assert output_df.filter(F.col("compound_id") == "CMPD025").count() == 1
    assert output_df.filter(F.col("compound_id") == "cmpd025").count() == 1

    # 704. Compound with missing compound_id: should be excluded
    assert output_df.filter(F.col("compound_id").isNull()).count() == 0

    # 705. Compound with negative/zero dose_mg: warnings
    assert_warning_in_logs("Zero or negative dose_mg detected", test_logs)

    # 706. Compound with negative efficacy_score: warnings
    assert_warning_in_logs("Negative efficacy_score detected", test_logs)

    # 707. Compound with negative response_pct: warnings
    assert_warning_in_logs("Negative response_pct detected", test_logs)

    # 708. Compound with negative ic50: warnings
    assert_warning_in_logs("Negative ic50 detected", test_logs)

    # 709. Compound with negative auc: warnings
    assert_warning_in_logs("Negative auc detected", test_logs)

    # 710. Compound with only null ic50: CMPD015
    assert_row_value(output_df, "compound_id", "CMPD015", "avg_ic50", None)
    assert_row_value(output_df, "compound_id", "CMPD015", "potency_level", None)

    # 711. Compound with only one record: CMPD022
    assert_row_value(output_df, "compound_id", "CMPD022", "experiment_count", 1)

    # 712. Compound with duplicate records: CMPD021
    assert_row_value(output_df, "compound_id", "CMPD021", "experiment_count", 2)

    # 713. Compound with non-unique compound_id: CMPD025, cmpd025
    assert output_df.filter(F.col("compound_id") == "CMPD025").count() == 1
    assert output_df.filter(F.col("compound_id") == "cmpd025").count() == 1

    # 714. Compound with missing compound_id: should be excluded
    assert output_df.filter(F.col("compound_id").isNull()).count() == 0

    # 715. Compound with negative/zero dose_mg: warnings
    assert_warning_in_logs("Zero or negative dose_mg detected", test_logs)

    # 716. Compound with negative efficacy_score: warnings
    assert_warning_in_logs("Negative efficacy_score detected", test_logs)

    # 717. Compound with negative response_pct: warnings
    assert_warning_in_logs("Negative response_pct detected", test_logs)

    # 718. Compound with negative ic50: warnings
    assert_warning_in_logs("Negative ic50 detected", test_logs)

    # 719. Compound with negative auc: warnings
    assert_warning_in_logs("Negative auc detected", test_logs)

    # 720. Compound with only null ic50: CMPD015
    assert_row_value(output_df, "compound_id", "CMPD015", "avg_ic50", None)
    assert_row_value(output_df, "compound_id", "CMPD015", "potency_level", None)

    # 721. Compound with only one record: CMPD022
    assert_row_value(output_df, "compound_id", "CMPD022", "experiment_count", 1)

    # 722. Compound with duplicate records: CMPD021
    assert_row_value(output_df, "compound_id", "CMPD021", "experiment_count", 2)

    # 723. Compound with non-unique compound_id: CMPD025, cmpd025
    assert output_df.filter(F.col("compound_id") == "CMPD025").count() == 1
    assert output_df.filter(F.col("compound_id") == "cmpd025").count() == 1

    # 724. Compound with missing compound_id: should be excluded
    assert output_df.filter(F.col("compound_id").isNull()).count() == 0

    # 725. Compound with negative/zero dose_mg: warnings
    assert_warning_in_logs("Zero or negative dose_mg detected", test_logs)

    # 726. Compound with negative efficacy_score: warnings
    assert_warning_in_logs("Negative efficacy_score detected", test_logs)

    # 727. Compound with negative response_pct: warnings
    assert_warning_in_logs("Negative response_pct detected", test_logs)

    # 728. Compound with negative ic50: warnings
    assert_warning_in_logs("Negative ic50 detected", test_logs)

    # 729. Compound with negative auc: warnings
    assert_warning_in_logs("Negative auc detected", test_logs)

    # 730. Compound with only null ic50: CMPD015
    assert_row_value(output_df, "compound_id", "CMPD015", "avg_ic50", None)
    assert_row_value(output_df, "compound_id", "CMPD015", "potency_level", None)

    # 731. Compound with only one record: CMPD022
    assert_row_value(output_df, "compound_id", "CMPD022", "experiment_count", 1)

    # 732. Compound with duplicate records: CMPD021
    assert_row_value(output_df, "compound_id", "CMPD021", "experiment_count", 2)

    # 733. Compound with non-unique compound_id: CMPD025, cmpd025
    assert output_df.filter(F.col("compound_id") == "CMPD025").count() == 1
    assert output_df.filter(F.col("compound_id") == "cmpd025").count() == 1

    # 734. Compound with missing compound_id: should be excluded
    assert output_df.filter(F.col("compound_id").isNull()).count() == 0

    # 735. Compound with negative/zero dose_mg: warnings
    assert_warning_in_logs("Zero or negative dose_mg detected", test_logs)

    # 736. Compound with negative efficacy_score: warnings
    assert_warning_in_logs("Negative efficacy_score detected", test_logs)

    # 737. Compound with negative response_pct: warnings
    assert_warning_in_logs("Negative response_pct detected", test_logs)

    # 738. Compound with negative ic50: warnings
    assert_warning_in_logs("Negative ic50 detected", test_logs)

    # 739. Compound with negative auc: warnings
    assert_warning_in_logs("Negative auc detected", test_logs)

    # 740. Compound with only null ic50: CMPD015
    assert_row_value(output_df, "compound_id", "CMPD015", "avg_ic50", None)
    assert_row_value(output_df, "compound_id", "CMPD015", "potency_level", None)

    # 741. Compound with only one record: CMPD022
    assert_row_value(output_df, "compound_id", "CMPD022", "experiment_count", 1)

    # 742. Compound with duplicate records: CMPD021
    assert_row_value(output_df, "compound_id", "CMPD021", "experiment_count", 2)

    # 743. Compound with non-unique compound_id: CMPD025, cmpd025
    assert output_df.filter(F.col("compound_id") == "CMPD025").count() == 1
    assert output_df.filter(F.col("compound_id") == "cmpd025").count() == 1

    # 744. Compound with missing compound_id: should be excluded
    assert output_df.filter(F.col("compound_id").isNull()).count() == 0

    # 745. Compound with negative/zero dose_mg: warnings
    assert_warning_in_logs("Zero or negative dose_mg detected", test_logs)

    # 746. Compound with negative efficacy_score: warnings
    assert_warning_in_logs("Negative efficacy_score detected", test_logs)

    # 747. Compound with negative response_pct: warnings
    assert_warning_in_logs("Negative response_pct detected", test_logs)

    # 748. Compound with negative ic50: warnings
    assert_warning_in_logs("Negative ic50 detected", test_logs)

    # 749. Compound with negative auc: warnings
    assert_warning_in_logs("Negative auc detected", test_logs)

    # 750. Compound with only null ic50: CMPD015
    assert_row_value(output_df, "compound_id", "CMPD015", "avg_ic50", None)
    assert_row_value(output_df, "compound_id", "CMPD015", "potency_level", None)

    # 751. Compound with only one record: CMPD022
    assert_row_value(output_df, "compound_id", "CMPD022", "experiment_count", 1)

    # 752. Compound with duplicate records: CMPD021
    assert_row_value(output_df, "compound_id", "CMPD021", "experiment_count", 2)

    # 753. Compound with non-unique compound_id: CMPD025, cmpd025
    assert output_df.filter(F.col("compound_id") == "CMPD025").count() == 1
    assert output_df.filter(F.col("compound_id") == "cmpd025").count() == 1

    # 754. Compound with missing compound_id: should be excluded
    assert output_df.filter(F.col("compound_id").isNull()).count() == 0

    # 755. Compound with negative/zero dose_mg: warnings
    assert_warning_in_logs("Zero or negative dose_mg detected", test_logs)

    # 756. Compound with negative efficacy_score: warnings
    assert_warning_in_logs("Negative efficacy_score detected", test_logs)

    # 757. Compound with negative response_pct: warnings
    assert_warning_in_logs("Negative response_pct detected", test_logs)

    # 758. Compound with negative ic50: warnings
    assert_warning_in_logs("Negative ic50 detected", test_logs)

    # 759. Compound with negative auc: warnings
    assert_warning_in_logs("Negative auc detected", test_logs)

    # 760. Compound with only null ic50: CMPD015
    assert_row_value(output_df, "compound_id", "CMPD015", "avg_ic50", None)
    assert_row_value(output_df, "compound_id", "CMPD015", "potency_level", None)

    # 761. Compound with only one record: CMPD022
    assert_row_value(output_df, "compound_id", "CMPD022", "experiment_count", 1)

    # 762. Compound with duplicate records: CMPD021
    assert_row_value(output_df, "compound_id", "CMPD021", "experiment_count", 2)

    # 763. Compound with non-unique compound_id: CMPD025, cmpd025
    assert output_df.filter(F.col("compound_id") == "CMPD025").count() == 1
    assert output_df.filter(F.col("compound_id") == "cmpd025").count() == 1

    # 764. Compound with missing compound_id: should be excluded
    assert output_df.filter(F.col("compound_id").isNull()).count() == 0

    # 765. Compound with negative/zero dose_mg: warnings
    assert_warning_in_logs("Zero or negative dose_mg detected", test_logs)

    # 766. Compound with negative efficacy_score: warnings
    assert_warning_in_logs("Negative efficacy_score detected", test_logs)

    # 767. Compound with negative response_pct: warnings
    assert_warning_in_logs("Negative response_pct detected", test_logs)

    # 768. Compound with negative ic50: warnings
    assert_warning_in_logs("Negative ic50 detected", test_logs)

    # 769. Compound with negative auc: warnings
    assert_warning_in_logs("Negative auc detected", test_logs)

    # 770. Compound with only null ic50: CMPD015
    assert_row_value(output_df, "compound_id", "CMPD015", "avg_ic50", None)
    assert_row_value(output_df, "compound_id", "CMPD015", "potency_level", None)

    # 771. Compound with only one record: CMPD022
    assert_row_value(output_df, "compound_id", "CMPD022", "experiment_count", 1)

    # 772. Compound with duplicate records: CMPD021
    assert_row_value(output_df, "compound_id", "CMPD021", "experiment_count", 2)

    # 773. Compound with non-unique compound_id: CMPD025, cmpd025
    assert output_df.filter(F.col("compound_id") == "CMPD025").count() == 1
    assert output_df.filter(F.col("compound_id") == "cmpd025").count() == 1

    # 774. Compound with missing compound_id: should be excluded
    assert output_df.filter(F.col("compound_id").isNull()).count() == 0

    # 775. Compound with negative/zero dose_mg: warnings
    assert_warning_in_logs("Zero or negative dose_mg detected", test_logs)

    # 776. Compound with negative efficacy_score: warnings
    assert_warning_in_logs("Negative efficacy_score detected", test_logs)

    # 777. Compound with negative response_pct: warnings
    assert_warning_in_logs("Negative response_pct detected", test_logs)

    # 778. Compound with negative ic50: warnings
    assert_warning_in_logs("Negative ic50 detected", test_logs)

    # 779. Compound with negative auc: warnings
    assert_warning_in_logs("Negative auc detected", test_logs)

    # 780. Compound with only null ic50: CMPD015
    assert_row_value(output_df, "compound_id", "CMPD015", "avg_ic50", None)
    assert_row_value(output_df, "compound_id", "CMPD015", "potency_level", None)

    # 781. Compound with only one record: CMPD022
    assert_row_value(output_df, "compound_id", "CMPD022", "experiment_count", 1)

    # 782. Compound with duplicate records: CMPD021
    assert_row_value(output_df, "compound_id", "CMPD021", "experiment_count", 2)

    # 783. Compound with non-unique compound_id: CMPD025, cmpd025
    assert output_df.filter(F.col("compound_id") == "CMPD025").count() == 1
    assert output_df.filter(F.col("compound_id") == "cmpd025").count() == 1

    # 784. Compound with missing compound_id: should be excluded
    assert output_df.filter(F.col("compound_id").isNull()).count() == 0

    # 785. Compound with negative/zero dose_mg: warnings
    assert_warning_in_logs("Zero or negative dose_mg detected", test_logs)

    # 786.
