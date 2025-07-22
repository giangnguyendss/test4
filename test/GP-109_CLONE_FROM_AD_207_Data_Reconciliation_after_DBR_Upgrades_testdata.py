# PySpark script for Databricks: Generates test data for product_plant and product_plant_v2 tables and performs column-wise data validation
# Purpose: Generate and validate diverse test records for data reconciliation post-DBR upgrades
# Author: Giang Nguyen
# Date: 2025-07-22
# Description: 
#   This script generates comprehensive test data (with a focus on data types, edge cases, nulls, special characters)
#   for two tables: product_plant and product_plant_v2. It joins them on 'id', compares columns by name and creates
#   a validation result column for each, indicating "Match" or "Mismatch". The results are saved in a new table
#   pp_validation_results with the prescribed output column order.

# Imports for test data generation, schema, and operations
from pyspark.sql import Row 
from pyspark.sql.types import (StructType, StructField, IntegerType, StringType, TimestampType) 
from pyspark.sql.functions import col, when, lit, expr, concat, array, monotonically_increasing_id 

# -- Step 1: Generate test data for product_plant and product_plant_v2 as DataFrames --

def get_product_plant_schema():
    """
    Purpose: Returns predefined schema for product_plant and product_plant_v2 (identical for the test scenario).
    Arguments: None
    Returns: StructType - the schema object.
    """
    return StructType([
        StructField("id", IntegerType(), False),
        StructField("name", StringType(), True),
        StructField("qty", IntegerType(), True),
    ])

def get_product_plant_test_data():
    """
    Purpose: Produces diverse test rows for product_plant
    Arguments: None
    Returns: list[Row] - List of Row objects representing test data (happy path, edge, nulls, special, error cases)
    """
    return [
        # id,      name                  qty
        Row(1,     "Bolt",               10),           # Happy path, exact match
        Row(2,     "Nut",                0),            # Edge: zero quantity
        Row(3,     "Screw",              999999999),    # Edge: large number
        Row(4,     "Washer",             None),         # NULL qty
        Row(5,     None,                 5),            # NULL name
        Row(6,     "Spring",             12),           # Happy path
        Row(7,     "bolt",               7),            # Case difference
        Row(8,     "",                   2),            # Edge: blank string name
        Row(9,     "Γραναζι",            1),            # Multi-byte/Unicode (Greek)
        Row(10,    "Schraube\nTest",     3),            # Special char: newline
        Row(11,    "Bolt\tTab",          -1),           # Edge: negative qty
        Row(12,    "Nut \"Big\"",        18),           # Special char: quotes
        Row(13,    "星-Star",             None),         # Multibyte & NULL
        Row(14,    "Washer",             0),            # Duplicate name, zero qty
        Row(15,    "Spring",             12000),        # Large qty
        Row(16,    "Nut",                None),         # NULL qty, normal name
        Row(17,    "O-Ring",             8),            # Happy path, special char
        Row(18,    None,                 None),         # All NULL
        Row(19,    "Böt",                10),           # Special: umlaut
        Row(20,    "Space ",             15),           # Trailing space
        Row(21,    "Space",              15),           # Compare with id=20, no space
        Row(22,    "NullString",         0),            # Edge: zero qty
        Row(23,    "数字",                 1),            # Multibyte: Chinese
        Row(24,    "Ampers&nd",          6),            # Special char: &
        Row(25,    "Pipe|Bar",           13),           # Special char: |
        Row(26,    "Single'Quote",       14),           # Special char: single quote
        Row(27,    "Special!@#$%^&*()",  7),            # Special char: mix
        Row(28,    "Emoji🙂",             9),            # Emoji/multibyte
        Row(29,    "Latin-Ü",            16),           # Latin special char
        Row(30,    "newline\n",          11),           # Edge: name with newline
    ]

def get_product_plant_v2_test_data():
    """
    Purpose: Produces corresponding test rows for product_plant_v2 with deliberate mismatches, nulls, errors, and special cases.
    Arguments: None
    Returns: list[Row] - List of Row objects representing test data (mirroring some, diverging for other, test all scenarios)
    """
    return [
        # id,      name                      qty
        Row(1,     "Bolt",                   10),      # Happy path, match
        Row(2,     "nut",                    0),       # Case mismatch
        Row(3,     "Screw",                  999999999),# Happy path, big num match
        Row(4,     "Washer",                 0),       # qty NULL vs 0
        Row(5,     None,                     5),       # NULL name match
        Row(6,     "Spring",                 999),     # qty mismatch
        Row(7,     "BOLT",                   7),       # Case/upper mismatch
        Row(8,     "",                       2),       # Blank string match
        Row(9,     "Γραναζι",                2),       # Multibyte, qty mismatch
        Row(10,    "Schraube Test",          3),       # Special char: different
        Row(11,    "Bolt Tab",               -1),      # Special char missing
        Row(12,    "Nut \"Big\"",            0),       # qty mismatch (18 vs 0)
        Row(13,    None,                     None),    # NULL name & qty
        Row(14,    "Washer",                 None),    # qty zero vs NULL
        Row(15,    "Spring",                 12000),   # Happy path, big num match
        Row(16,    "Nut",                    None),    # Happy path, NULL qty
        Row(17,    "O-Ring",                 100),     # qty mismatch
        Row(18,    None,                     None),    # Both NULL
        Row(19,    "Bot",                    10),      # Typo mismatch
        Row(20,    "Space",                  15),      # Trailing space diff
        Row(21,    "Space",                  None),    # Missing qty
        Row(22,    "NullString",             0),       # Match zero qty & name
        Row(23,    "数字",                     41),      # Multibyte, qty mismatch
        Row(24,    "Ampers&nd",              5),       # qty mismatch
        Row(25,    "Pipe Bar",               13),      # Special char difference
        Row(26,    "Single'Quote",           13),      # qty mismatch
        Row(27,    "Special!@#$%^&*()",      8),       # qty mismatch
        Row(28,    "Emoji🙂",                 None),    # qty missing in v2
        Row(29,    "Latin-U",                16),      # Special char diff
        Row(30,    "newline",                11),      # new-line removed
    ]

# -- Step 2: Create DataFrames --

try:
    pp_df = spark.createDataFrame(get_product_plant_test_data(), schema=get_product_plant_schema())
except Exception as e:
    print("Failed to create product_plant DataFrame:", str(e))
    pp_df = None

try:
    ppv2_df = spark.createDataFrame(get_product_plant_v2_test_data(), schema=get_product_plant_schema())
except Exception as e:
    print("Failed to create product_plant_v2 DataFrame:", str(e))
    ppv2_df = None

# -- Step 3: Alias columns for both tables for easy reference in join/validation --

pp_df = pp_df.select(
    col("id").alias("product_plant_id"),
    col("name").alias("product_plant_name"),
    col("qty").alias("product_plant_qty"),
)

ppv2_df = ppv2_df.select(
    col("id").alias("product_plant_v2_id"),
    col("name").alias("product_plant_v2_name"),
    col("qty").alias("product_plant_v2_qty"),
)

# -- Step 4: Join on the primary key (id) - only include records present in BOTH tables ("inner" join) --

joined_df = pp_df.join(
    ppv2_df,
    pp_df.product_plant_id == ppv2_df.product_plant_v2_id,
    how="inner"
)

# -- Step 5: Per-column validation, following requirements for NULL (Match if both NULL), type/strict comparison, case-sensitivity --

def validate_column(col1, col2, col_suffix):
    """
    Purpose: Compares two columns and returns a validation column: "Match"/"Mismatch" following rules:
      - Match if both NULL
      - Match if values == (type & case sensitive)
      - Mismatch otherwise
    Arguments:
        col1 (Column): First column to compare
        col2 (Column): Second column to compare
        col_suffix (str): Suffix for the new column name (e.g., 'id', 'name', etc.)
    Returns:
        Column: New column representing the validation result
    """
    return when(
        col1.isNull() & col2.isNull(), lit("Match")
    ).when(
        col1 == col2, lit("Match")
    ).otherwise(lit("Mismatch")).alias(f"{col_suffix}_validation")

# -- Step 6: Assemble final output DataFrame in required order: pp.colX, ppv2.colX, colX_validation...

result_df = joined_df.select(
    col("product_plant_id"),
    col("product_plant_v2_id"),
    validate_column(col("product_plant_id"), col("product_plant_v2_id"), "id"),
    col("product_plant_name"),
    col("product_plant_v2_name"),
    validate_column(col("product_plant_name"), col("product_plant_v2_name"), "name"),
    col("product_plant_qty"),
    col("product_plant_v2_qty"),
    validate_column(col("product_plant_qty"), col("product_plant_v2_qty"), "qty"),
)

# -- Step 7: Drop and recreate the pp_validation_results output table per requirements--

# Drop target table if exists
spark.sql("DROP TABLE IF EXISTS pp_validation_results")

# Write result_df to pp_validation_results (overwrite)
result_df.write.saveAsTable("pp_validation_results", mode="overwrite")
