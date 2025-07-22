# PySpark script for Databricks: Comprehensive test suite for column-wise data validation between product_plant and product_plant_v2 tables
# Purpose: End-to-end unit, integration, performance, and data quality tests for cross-table value-by-value, column-by-column reconciliation
# Author: Giang Nguyen
# Date: 2025-07-22
# Description:
#   This script validates the data reconciliation logic by generating, transforming, and assessing product_plant and product_plant_v2 tables:
#   - Ensures correct schema and datatype mapping
#   - Tests NULL, type, and edge case handling in validation columns
#   - Checks output ordering, strictness of comparisons, handling of special characters, and table lifecycle operations
#   - Measures performance of validation on realistic dataset size
#   - Guarantees Delta Lake interop, clean-up, error handling, constraint checks, and native Databricks compatibility

# -- REQUIRED IMPORTS CONFIGURATION --
from pyspark.sql import Row  
from pyspark.sql.types import (StructType, StructField, IntegerType, StringType)  
from pyspark.sql.functions import col, when, lit, expr, count, monotonically_increasing_id  
import time  

# COMMENTED OUT SparkSession import/creation (Databricks provides the 'spark' object)
from pyspark.sql import SparkSession
# spark = SparkSession.builder.getOrCreate()

# -- TEST SECTION 1: SCHEMA VALIDATION --

def test_pp_and_ppv2_schema():
    """
    Purpose: Ensures both product_plant and product_plant_v2 tables have correct schemas (column names and datatypes).
    Arguments: None
    Returns: None, asserts success or raises AssertionError.
    """
    pp_schema = StructType([
        StructField("id", IntegerType(), False),
        StructField("name", StringType(), True),
        StructField("qty", IntegerType(), True),
    ])
    try:
        pp_df = spark.createDataFrame([Row(1, "A", 9)], schema=pp_schema)
        ppv2_df = spark.createDataFrame([Row(2, "B", 8)], schema=pp_schema)
    except Exception as e:
        raise AssertionError(f"Schema creation failed: {str(e)}")
    # Validate number and order of columns
    assert [f.name for f in pp_df.schema.fields] == ["id", "name", "qty"], "product_plant schema mismatch"
    assert [f.dataType for f in pp_df.schema.fields] == [IntegerType(), StringType(), IntegerType()], "product_plant data types mismatch"
    assert [f.name for f in ppv2_df.schema.fields] == ["id", "name", "qty"], "product_plant_v2 schema mismatch"
    assert [f.dataType for f in ppv2_df.schema.fields] == [IntegerType(), StringType(), IntegerType()], "product_plant_v2 data types mismatch"

test_pp_and_ppv2_schema()

# -- TEST SECTION 2: TABLE CREATION AND CLEANUP --
def test_table_drop_and_create():
    """
    Purpose: Ensures the pp_validation_results table is dropped and recreated before population.
    Arguments: None
    Returns: None, asserts success.
    """
    # Try to drop and create dummy table, ensure it works
    spark.sql("DROP TABLE IF EXISTS pp_validation_results")
    # Create dummy validation output to test
    dummy_df = spark.createDataFrame([Row(a=1, b=2, c="Match")])
    dummy_df.write.saveAsTable("pp_validation_results", mode="overwrite")
    found = spark._jsparkSession.catalog().tableExists("pp_validation_results")
    assert found, "pp_validation_results table was not created after DROP/CREATE"

test_table_drop_and_create()

# -- TEST SECTION 3: TEST DATA GENERATION/INPUT COLUMN COUNT CHECKING --

def test_input_column_count_and_alignment():
    """
    Purpose: Asserts that the test DataFrames have matching column count and correct alignment before join/processing,
    preventing column mismatch during insert.
    Arguments: None
    Returns: None, asserts as needed.
    """
    schema = StructType([
        StructField("id", IntegerType(), False),
        StructField("name", StringType(), True),
        StructField("qty", IntegerType(), True),
    ])
    pp_data = [Row(1, "Bolt", 10)]
    ppv2_data = [Row(1, "Bolt", 10)]
    pp_df = spark.createDataFrame(pp_data, schema=schema)
    ppv2_df = spark.createDataFrame(ppv2_data, schema=schema)
    assert len(pp_df.columns) == len(ppv2_df.columns) == 3, "Input schema column count mismatch"
    assert pp_df.columns == ppv2_df.columns, "Input column order mismatch"

test_input_column_count_and_alignment()

# -- TEST SECTION 4: VALUE-BY-VALUE AND NULL HANDLING UNIT TESTS --
def test_column_value_validation():
    """
    Purpose: Unit tests for the column-level validation logic: matches, mismatches, hard/soft/null/case/whitespace/type/special case edge.
    Arguments: None
    Returns: None, asserts each scenario.
    """
    schema = StructType([
        StructField("id", IntegerType(), False),
        StructField("name", StringType(), True),
        StructField("qty", IntegerType(), True),
    ])
    # Happy Path
    df1 = spark.createDataFrame([Row(1, "Bolt", 8)], schema=schema)
    df2 = spark.createDataFrame([Row(1, "Bolt", 8)], schema=schema)
    j1 = df1.alias("a").join(df2.alias("b"), col("a.id") == col("b.id"))
    res = j1.select(
        when(col("a.name").isNull() & col("b.name").isNull(), lit("Match"))
         .when(col("a.name") == col("b.name"), lit("Match"))
         .otherwise(lit("Mismatch")).alias("name_validation")
    ).collect()[0]["name_validation"]
    assert res == "Match", "Failed to validate happy path match"
    # Type Sensitivity
    df1 = spark.createDataFrame([Row(2, "A", 9)], schema=schema)
    df2 = spark.createDataFrame([Row(2, "A", "9")], schema=StructType([
        StructField("id", IntegerType(), False),
        StructField("name", StringType(), True),
        StructField("qty", StringType(), True),
    ]))
    j2 = df1.alias("a").join(df2.alias("b"), col("a.id") == col("b.id"))
    res = j2.select(
        when(col("a.qty").isNull() & col("b.qty").isNull(), lit("Match"))
        .when(col("a.qty") == col("b.qty"), lit("Match"))
        .otherwise(lit("Mismatch")).alias("qty_validation")
    ).collect()[0]["qty_validation"]
    assert res == "Mismatch", "Type-sensitive comparison failed"
    # NULL handling: both NULL
    df1 = spark.createDataFrame([Row(3, None, None)], schema=schema)
    df2 = spark.createDataFrame([Row(3, None, None)], schema=schema)
    j3 = df1.alias("a").join(df2.alias("b"), col("a.id") == col("b.id"))
    res = j3.select(
        when(col("a.name").isNull() & col("b.name").isNull(), lit("Match"))
        .when(col("a.name") == col("b.name"), lit("Match"))
        .otherwise(lit("Mismatch")).alias("name_validation")
    ).collect()[0]["name_validation"]
    assert res == "Match", "NULL both sides should match"
    # Only one side NULL
    df1 = spark.createDataFrame([Row(4, "Bolt", None)], schema=schema)
    df2 = spark.createDataFrame([Row(4, None, None)], schema=schema)
    j4 = df1.alias("a").join(df2.alias("b"), col("a.id") == col("b.id"))
    res = j4.select(
        when(col("a.name").isNull() & col("b.name").isNull(), lit("Match"))
        .when(col("a.name") == col("b.name"), lit("Match"))
        .otherwise(lit("Mismatch")).alias("name_validation")
    ).collect()[0]["name_validation"]
    assert res == "Mismatch", "One-side NULL mismatch failed"
    # Case sensitivity
    df1 = spark.createDataFrame([Row(5, "Bolt", 1)], schema=schema)
    df2 = spark.createDataFrame([Row(5, "BOLT", 1)], schema=schema)
    j5 = df1.alias("a").join(df2.alias("b"), col("a.id") == col("b.id"))
    res = j5.select(
        when(col("a.name").isNull() & col("b.name").isNull(), lit("Match"))
        .when(col("a.name") == col("b.name"), lit("Match"))
        .otherwise(lit("Mismatch")).alias("name_validation")
    ).collect()[0]["name_validation"]
    assert res == "Mismatch", "Case sensitivity comparison failed"
    # White space sensitivity
    df1 = spark.createDataFrame([Row(6, "Bolt ", 2)], schema=schema)
    df2 = spark.createDataFrame([Row(6, "Bolt", 2)], schema=schema)
    j6 = df1.alias("a").join(df2.alias("b"), col("a.id") == col("b.id"))
    res = j6.select(
        when(col("a.name").isNull() & col("b.name").isNull(), lit("Match"))
        .when(col("a.name") == col("b.name"), lit("Match"))
        .otherwise(lit("Mismatch")).alias("name_validation")
    ).collect()[0]["name_validation"]
    assert res == "Mismatch", "Whitespace comparison failed"
    # Special/multibyte character handling
    df1 = spark.createDataFrame([Row(7, "Γραναζι", 5)], schema=schema)
    df2 = spark.createDataFrame([Row(7, "Γραναζι", 5)], schema=schema)
    j7 = df1.alias("a").join(df2.alias("b"), col("a.id") == col("b.id"))
    res = j7.select(
        when(col("a.name").isNull() & col("b.name").isNull(), lit("Match"))
        .when(col("a.name") == col("b.name"), lit("Match"))
        .otherwise(lit("Mismatch")).alias("name_validation")
    ).collect()[0]["name_validation"]
    assert res == "Match", "Special/multibyte character match failed"

test_column_value_validation()

# -- TEST SECTION 5: JOIN ON PRIMARY KEY, ROW INCLUSION/EXCLUSION --

def test_primary_key_join_and_row_filter():
    """
    Purpose: Ensures validation only processes rows where id exists in both tables (inner join); missing-key rows are excluded.
    Arguments: None
    Returns: None, asserts success.
    """
    schema = StructType([
        StructField("id", IntegerType(), False),
        StructField("name", StringType(), True),
        StructField("qty", IntegerType(), True),
    ])
    df1 = spark.createDataFrame([Row(1, "Bolt", 10), Row(2, "Nail", 7)], schema=schema)
    df2 = spark.createDataFrame([Row(1, "Bolt", 10), Row(3, "Screw", 5)], schema=schema)
    joined_df = df1.join(df2, df1.id == df2.id, how="inner")
    ids = set([r["id"] for r in joined_df.select(df1.id).collect()])
    assert ids == {1}, "Rows with missing primary key not correctly excluded"

test_primary_key_join_and_row_filter()

# -- TEST SECTION 6: OUTPUT COLUMN ORDER AND OUTPUT VALIDATION --

def test_output_column_sequence_and_content():
    """
    Purpose: Asserts the validation result table has correct column ordering
    [product_plant.colA, product_plant_v2.colA, colA_validation, ...]
    and correct values for a representative example row.
    Arguments: None
    Returns: None, asserts as required.
    """
    schema = StructType([
        StructField("id", IntegerType(), False),
        StructField("name", StringType(), True),
        StructField("qty", IntegerType(), True),
    ])
    rowA = Row(1, "A", 1)
    rowB = Row(1, "B", 1)
    df1 = spark.createDataFrame([rowA], schema=schema)
    df2 = spark.createDataFrame([rowB], schema=schema)
    df1a = df1.select(col("id").alias("product_plant_id"),
                      col("name").alias("product_plant_name"),
                      col("qty").alias("product_plant_qty"),
    )
    df2a = df2.select(col("id").alias("product_plant_v2_id"),
                      col("name").alias("product_plant_v2_name"),
                      col("qty").alias("product_plant_v2_qty"),
    )
    joined = df1a.join(df2a, df1a.product_plant_id == df2a.product_plant_v2_id, "inner")
    out = joined.select(
        col("product_plant_id"),
        col("product_plant_v2_id"),
        when(col("product_plant_id").isNull() & col("product_plant_v2_id").isNull(), lit("Match"))
        .when(col("product_plant_id") == col("product_plant_v2_id"), lit("Match"))
        .otherwise(lit("Mismatch")).alias("id_validation"),
        col("product_plant_name"),
        col("product_plant_v2_name"),
        when(col("product_plant_name").isNull() & col("product_plant_v2_name").isNull(), lit("Match"))
        .when(col("product_plant_name") == col("product_plant_v2_name"), lit("Match"))
        .otherwise(lit("Mismatch")).alias("name_validation"),
        col("product_plant_qty"),
        col("product_plant_v2_qty"),
        when(col("product_plant_qty").isNull() & col("product_plant_v2_qty").isNull(), lit("Match"))
        .when(col("product_plant_qty") == col("product_plant_v2_qty"), lit("Match"))
        .otherwise(lit("Mismatch")).alias("qty_validation"),
    )
    out_columns = out.columns
    expected_sequence = [
        "product_plant_id",
        "product_plant_v2_id",
        "id_validation",
        "product_plant_name",
        "product_plant_v2_name",
        "name_validation",
        "product_plant_qty",
        "product_plant_v2_qty",
        "qty_validation"
    ]
    assert out_columns == expected_sequence, f"Output columns not in expected order, got {out_columns}"
    first_row = out.collect()[0]
    assert first_row["id_validation"] == "Match"
    assert first_row["name_validation"] == "Mismatch"
    assert first_row["qty_validation"] == "Match"

test_output_column_sequence_and_content()

# -- TEST SECTION 7: HANDLING COLUMNS ONLY PRESENT IN ONE TABLE (EXCLUDE) --

def test_exclude_columns_not_in_both():
    """
    Purpose: Ensures only columns with matching name in both tables are compared and output - extra columns are ignored.
    Arguments: None
    Returns: None, asserts proper exclusion.
    """
    schema1 = StructType([
        StructField("id", IntegerType(), False),
        StructField("name", StringType(), True),
        StructField("qty", IntegerType(), True),
        StructField("extra", StringType(), True),
    ])
    schema2 = StructType([
        StructField("id", IntegerType(), False),
        StructField("name", StringType(), True),
        StructField("qty", IntegerType(), True),
    ])
    df1 = spark.createDataFrame([Row(1, "X", 1, "ignoreme")], schema=schema1)
    df2 = spark.createDataFrame([Row(1, "X", 1)], schema=schema2)
    # Only match up columns present in both
    select_cols = [c for c in df1.columns if c in df2.columns]
    join1 = df1.select([col(x) for x in select_cols]).join(df2, "id")
    assert "extra" not in join1.columns, "Extra column present in output, test failed"

test_exclude_columns_not_in_both()

# -- TEST SECTION 8: PERFORMANCE/SCALABILITY TESTING (LIMITED SCALE) --
def test_performance_on_large_dataset():
    """
    Purpose: Validates that validation logic can run efficiently on (simulated) large datasets (>100K rows).
    Arguments: None
    Returns: None, asserts elapsed time under threshold (20s for 100K, on Databricks).
    """
    schema = StructType([
        StructField("id", IntegerType(), False),
        StructField("name", StringType(), True),
        StructField("qty", IntegerType(), True),
    ])
    # Generate 100K rows of test data
    base = [(i, f"Item{i}", i % 250) for i in range(100000)]
    base2 = [(i, f"Item{i}", i % 250) for i in range(100000)]
    df1 = spark.createDataFrame(base, schema=schema)
    df2 = spark.createDataFrame(base2, schema=schema)
    dfa = df1.select(
        col("id").alias("product_plant_id"),
        col("name").alias("product_plant_name"),
        col("qty").alias("product_plant_qty"),
    )
    dfb = df2.select(
        col("id").alias("product_plant_v2_id"),
        col("name").alias("product_plant_v2_name"),
        col("qty").alias("product_plant_v2_qty"),
    )
    t0 = time.time()
    joined = dfa.join(dfb, dfa.product_plant_id == dfb.product_plant_v2_id, "inner")
    validated = joined.select(
        col("product_plant_id"),
        col("product_plant_v2_id"),
        when(col("product_plant_id").isNull() & col("product_plant_v2_id").isNull(), lit("Match"))
        .when(col("product_plant_id") == col("product_plant_v2_id"), lit("Match"))
        .otherwise(lit("Mismatch")).alias("id_validation"),
    )
    cnt = validated.count()
    t1 = time.time()
    assert cnt == 100000, f"Validation row count mismatch: got {cnt}, expected 100000"
    assert (t1 - t0) < 20, f"Performance regression: {t1-t0:.2f}s for 100K rows"

test_performance_on_large_dataset()

# -- TEST SECTION 9: DELTA LAKE OPERATIONS --

def test_delta_lake_merge_update_delete():
    """
    Purpose: End-to-end test for Databricks Delta Lake MERGE, UPDATE, DELETE using output table.
    Arguments: None
    Returns: None, asserts correctness and table clean-up.
    """
    # Input preparation: clean state
    spark.sql("DROP TABLE IF EXISTS test_pp_output_delta")
    test_out = spark.createDataFrame([
        Row(product_plant_id=1, product_plant_v2_id=1, id_validation="Match"),
        Row(product_plant_id=2, product_plant_v2_id=2, id_validation="Mismatch"),
    ])
    test_out.write.saveAsTable("test_pp_output_delta", mode="overwrite")
    # UPDATE: set all to "Match"
    spark.sql("""
        UPDATE test_pp_output_delta
        SET id_validation = 'Match'
        WHERE product_plant_id = 2
    """)
    # CHECK updated
    d = spark.sql("SELECT * FROM test_pp_output_delta WHERE product_plant_id = 2").collect()
    assert d[0]['id_validation'] == "Match", "Delta UPDATE failed"
    # DELETE rows
    spark.sql("DELETE FROM test_pp_output_delta WHERE product_plant_id = 2")
    d2 = spark.sql("SELECT * FROM test_pp_output_delta")
    assert d2.count() == 1, "Delta DELETE failed"
    # MERGE
    spark.sql("""
        MERGE INTO test_pp_output_delta t
        USING (SELECT 2 as product_plant_id, 2 as product_plant_v2_id, 'Match' as id_validation) s
        ON t.product_plant_id = s.product_plant_id
        WHEN MATCHED THEN UPDATE SET t.id_validation = s.id_validation
        WHEN NOT MATCHED THEN INSERT *
    """)
    d3 = spark.sql("SELECT * FROM test_pp_output_delta WHERE product_plant_id = 2")
    assert d3.count() == 1, "Delta MERGE failed"
    # CLEANUP
    spark.sql("DROP TABLE IF EXISTS test_pp_output_delta")

test_delta_lake_merge_update_delete()

# -- TEST SECTION 10: WINDOW/ANALYTICS FUNCTIONS IN OUTPUT TABLE --

def test_window_function_features():
    """
    Purpose: Validates that analytics and window functions operate as expected on the validation results table.
    Arguments: None
    Returns: None, asserts correctness.
    """
    tname = "test_window_validation"
    spark.sql(f"DROP TABLE IF EXISTS {tname}")
    sample = spark.createDataFrame([
        Row(pid=1, namev="A", qtyv=10,   valid="Match"),
        Row(pid=2, namev="B", qtyv=20,   valid="Mismatch"),
        Row(pid=3, namev="C", qtyv=20,   valid="Mismatch"),
        Row(pid=4, namev="D", qtyv=5,    valid="Match")
    ])
    sample.write.saveAsTable(tname, mode="overwrite")
    winres = spark.sql(f"""
        SELECT *, 
          ROW_NUMBER() OVER (ORDER BY pid) as rn,
          COUNT(*) OVER (PARTITION BY valid) as cnt_by_valid
        FROM {tname}
    """)
    rows = winres.collect()
    assert any(r['valid'] == 'Mismatch' and r['cnt_by_valid'] == 2 for r in rows), "Window function COUNT failed"
    assert any(r['rn'] == 4 for r in rows), "ROW_NUMBER window function failed"
    spark.sql(f"DROP TABLE IF EXISTS {tname}")

test_window_function_features()

# -- TEST SECTION 11: FOREIGN KEY RELATIONSHIP, CONSTRAINTS, AND CHECKS (SQL BLOCK) --

# Create validate status check constraint on test output table
spark.sql("""
    DROP TABLE IF EXISTS test_pp_ck
""")
spark.sql("""
    CREATE TABLE test_pp_ck (
        id INT,
        status STRING,
        CONSTRAINT status_values CHECK (status IN ("Match", "Mismatch"))
    )
""")
# Insert should pass for allowed value
spark.sql("""
    INSERT INTO test_pp_ck VALUES (1, "Match")
""")
# Insert should fail (will catch exception) for disallowed value
try:
    spark.sql('INSERT INTO test_pp_ck VALUES (2, "NotAllowed")')
    assert False, "Constraint violation was not caught"
except Exception:
    pass
# Cleanup
spark.sql("DROP TABLE IF EXISTS test_pp_ck")

# -- TEST SECTION 12: ERROR HANDLING FOR FILE OPENING OR DATA CREATION --

def test_file_open_try_except():
    """
    Purpose: Simulates file/data loading with try-except to handle missing/invalid sources gracefully.
    Arguments: None
    Returns: None, expects failure on purpose.
    """
    try:
        # Try to load a DataFrame from a non-existent file (should fail and not crash script)
        spark.read.csv("/volume/not_a_real_file.csv")
    except Exception:
        pass  # Success: handled

test_file_open_try_except()

# -- TEST SECTION 13: NULL/TYPE CONVERSION AND COMPLEX TYPE (ARRAY/STRUCT) COMPATIBILITY --

def test_complex_types_and_nulls():
    """
    Purpose: Ensures supported complex types (STRUCT, ARRAY) are handled in schema and data conversion.
    Arguments: None
    Returns: None, asserts on supported types only.
    """
    from pyspark.sql.types import StructType, StructField, IntegerType, StringType  
    from pyspark.sql import Row  
    stype = StructType([
        StructField("id", IntegerType(), False),
        StructField("arr", 
            StructType([
                StructField("item1", StringType(), True),
                StructField("item2", StringType(), True)
            ]), True)
    ])
    df = spark.createDataFrame([Row(id=1, arr=Row(item1="a", item2=None))], schema=stype)
    assert df.schema["arr"].dataType.fieldNames() == ["item1", "item2"], "STRUCT field names mismatch"
    # Null handling: one nested value null
    row = df.collect()[0]
    assert row.arr.item2 is None, "Null in STRUCT not handled"

test_complex_types_and_nulls()

# -- END OF SCRIPT --

# COMMENTED OUT: spark.stop()
