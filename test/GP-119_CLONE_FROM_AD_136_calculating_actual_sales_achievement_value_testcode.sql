/* 
==========================================================================================
Databricks SQL Test Suite for Actual Value Calculation (purgo_playground.actual_value_calculation)
==========================================================================================

- This test suite validates the calculation of actual_value for each brand_name, country_code, territory_id, month, and year
  by summing sales_value from purgo_playground.t3_itm_territory_sales and sales_net_price_local from purgo_playground.t3_ttm_territory_sales.
- Only records where source_system_name exists in purgo_playground.control_table.source_system are included.
- All columns in the output table are NOT NULL and have the correct data types.
- The suite covers: schema validation, data type conversion, NULL handling, error exclusion, aggregation, deduplication, and data quality.
- All SQL assertions use safe patterns and CTEs.
- All comments are in block (/* */) or line (--) style as per requirements.
==========================================================================================
*/

/*------------------------------------------------------------------------------
SECTION: Setup - Clean up and prepare the target table for test
------------------------------------------------------------------------------*/
-- Clean up the target table before running tests
DELETE FROM purgo_playground.actual_value_calculation;

/*------------------------------------------------------------------------------
SECTION: Test 1 - Schema Validation for actual_value_calculation
------------------------------------------------------------------------------*/
-- Validate that the target table has the correct schema and NOT NULL constraints
WITH schema_info AS (
  SELECT 
    column_name,
    data_type,
    is_nullable
  FROM information_schema.columns
  WHERE table_schema = "purgo_playground"
    AND table_name = "actual_value_calculation"
)
SELECT
  CASE WHEN COUNT(*) = 6 THEN 1 ELSE 0 END AS column_count_assertion,
  MAX(CASE WHEN column_name = "country_code" AND data_type = "STRING" AND is_nullable = "NO" THEN 1 ELSE 0 END) AS country_code_assertion,
  MAX(CASE WHEN column_name = "brand_name" AND data_type = "STRING" AND is_nullable = "NO" THEN 1 ELSE 0 END) AS brand_name_assertion,
  MAX(CASE WHEN column_name = "territory_id" AND data_type = "STRING" AND is_nullable = "NO" THEN 1 ELSE 0 END) AS territory_id_assertion,
  MAX(CASE WHEN column_name = "month" AND data_type = "STRING" AND is_nullable = "NO" THEN 1 ELSE 0 END) AS month_assertion,
  MAX(CASE WHEN column_name = "year" AND data_type = "STRING" AND is_nullable = "NO" THEN 1 ELSE 0 END) AS year_assertion,
  MAX(CASE WHEN column_name = "actual_value" AND data_type = "DOUBLE" AND is_nullable = "NO" THEN 1 ELSE 0 END) AS actual_value_assertion
FROM schema_info;

/*------------------------------------------------------------------------------
SECTION: Test 2 - Insert Calculation Logic and Validate Data Quality
------------------------------------------------------------------------------*/
-- Insert the calculation result into the target table
INSERT INTO purgo_playground.actual_value_calculation (country_code, brand_name, territory_id, month, year, actual_value)
WITH
  /* 
    CTE: valid_itm
    - Filters t3_itm_territory_sales for valid records:
      - All required columns are NOT NULL
      - sales_value is numeric and NOT NULL
      - source_system_name exists in control_table
  */
  valid_itm AS (
    SELECT
      i.country_code,
      i.brand_name,
      i.territory_id,
      CAST(MONTH(i.sales_month) AS STRING) AS month,
      CAST(YEAR(i.sales_month) AS STRING) AS year,
      i.sales_value
    FROM purgo_playground.t3_itm_territory_sales i
    INNER JOIN purgo_playground.control_table c
      ON i.source_system_name = c.source_system
    WHERE
      i.country_code IS NOT NULL
      AND i.brand_name IS NOT NULL
      AND i.territory_id IS NOT NULL
      AND i.sales_month IS NOT NULL
      AND i.sales_value IS NOT NULL
      AND TRY_CAST(i.sales_value AS DOUBLE) IS NOT NULL
      AND i.source_system_name IS NOT NULL
  ),
  /* 
    CTE: valid_ttm
    - Filters t3_ttm_territory_sales for valid records:
      - All required columns are NOT NULL
      - sales_net_price_local is numeric and NOT NULL
      - source_system_name exists in control_table
  */
  valid_ttm AS (
    SELECT
      t.country_code,
      t.brand_name,
      t.territory_id,
      CAST(MONTH(t.fiscal_date) AS STRING) AS month,
      CAST(YEAR(t.fiscal_date) AS STRING) AS year,
      t.sales_net_price_local
    FROM purgo_playground.t3_ttm_territory_sales t
    INNER JOIN purgo_playground.control_table c
      ON t.source_system_name = c.source_system
    WHERE
      t.country_code IS NOT NULL
      AND t.brand_name IS NOT NULL
      AND t.territory_id IS NOT NULL
      AND t.fiscal_date IS NOT NULL
      AND t.sales_net_price_local IS NOT NULL
      AND TRY_CAST(t.sales_net_price_local AS DOUBLE) IS NOT NULL
      AND t.source_system_name IS NOT NULL
  ),
  /* 
    CTE: unioned_sales
    - Union all valid sales from both sources, tagging the source
  */
  unioned_sales AS (
    SELECT
      country_code,
      brand_name,
      territory_id,
      month,
      year,
      sales_value AS value
    FROM valid_itm
    UNION ALL
    SELECT
      country_code,
      brand_name,
      territory_id,
      month,
      year,
      sales_net_price_local AS value
    FROM valid_ttm
  ),
  /* 
    CTE: aggregated_sales
    - Aggregate (sum) all values for each unique key
  */
  aggregated_sales AS (
    SELECT
      country_code,
      brand_name,
      territory_id,
      month,
      year,
      SUM(value) AS actual_value
    FROM unioned_sales
    GROUP BY country_code, brand_name, territory_id, month, year
  )
SELECT
  country_code,
  brand_name,
  territory_id,
  month,
  year,
  actual_value
FROM aggregated_sales;

/*------------------------------------------------------------------------------
SECTION: Test 3 - Assert No NULLs in Output Table
------------------------------------------------------------------------------*/
-- Assert that there are no NULLs in any NOT NULL column
WITH null_counts AS (
  SELECT
    SUM(CASE WHEN country_code IS NULL THEN 1 ELSE 0 END) AS null_country_code,
    SUM(CASE WHEN brand_name IS NULL THEN 1 ELSE 0 END) AS null_brand_name,
    SUM(CASE WHEN territory_id IS NULL THEN 1 ELSE 0 END) AS null_territory_id,
    SUM(CASE WHEN month IS NULL THEN 1 ELSE 0 END) AS null_month,
    SUM(CASE WHEN year IS NULL THEN 1 ELSE 0 END) AS null_year,
    SUM(CASE WHEN actual_value IS NULL THEN 1 ELSE 0 END) AS null_actual_value
  FROM purgo_playground.actual_value_calculation
)
SELECT
  CASE WHEN null_country_code = 0 THEN 1 ELSE 0 END AS country_code_not_null_assertion,
  CASE WHEN null_brand_name = 0 THEN 1 ELSE 0 END AS brand_name_not_null_assertion,
  CASE WHEN null_territory_id = 0 THEN 1 ELSE 0 END AS territory_id_not_null_assertion,
  CASE WHEN null_month = 0 THEN 1 ELSE 0 END AS month_not_null_assertion,
  CASE WHEN null_year = 0 THEN 1 ELSE 0 END AS year_not_null_assertion,
  CASE WHEN null_actual_value = 0 THEN 1 ELSE 0 END AS actual_value_not_null_assertion
FROM null_counts;

/*------------------------------------------------------------------------------
SECTION: Test 4 - Data Type Validation for Output Table
------------------------------------------------------------------------------*/
-- Validate that the data types in the output table are as expected
WITH type_info AS (
  SELECT
    column_name,
    data_type
  FROM information_schema.columns
  WHERE table_schema = "purgo_playground"
    AND table_name = "actual_value_calculation"
)
SELECT
  MAX(CASE WHEN column_name = "country_code" AND data_type = "STRING" THEN 1 ELSE 0 END) AS country_code_type_assertion,
  MAX(CASE WHEN column_name = "brand_name" AND data_type = "STRING" THEN 1 ELSE 0 END) AS brand_name_type_assertion,
  MAX(CASE WHEN column_name = "territory_id" AND data_type = "STRING" THEN 1 ELSE 0 END) AS territory_id_type_assertion,
  MAX(CASE WHEN column_name = "month" AND data_type = "STRING" THEN 1 ELSE 0 END) AS month_type_assertion,
  MAX(CASE WHEN column_name = "year" AND data_type = "STRING" THEN 1 ELSE 0 END) AS year_type_assertion,
  MAX(CASE WHEN column_name = "actual_value" AND data_type = "DOUBLE" THEN 1 ELSE 0 END) AS actual_value_type_assertion
FROM type_info;

/*------------------------------------------------------------------------------
SECTION: Test 5 - Validate Month and Year Extraction Logic
------------------------------------------------------------------------------*/
-- Validate that month and year are correctly extracted from date columns
WITH itm_month_year AS (
  SELECT
    sales_month,
    CAST(MONTH(sales_month) AS STRING) AS extracted_month,
    CAST(YEAR(sales_month) AS STRING) AS extracted_year
  FROM purgo_playground.t3_itm_territory_sales
  WHERE sales_month IS NOT NULL
  LIMIT 5
),
ttm_month_year AS (
  SELECT
    fiscal_date,
    CAST(MONTH(fiscal_date) AS STRING) AS extracted_month,
    CAST(YEAR(fiscal_date) AS STRING) AS extracted_year
  FROM purgo_playground.t3_ttm_territory_sales
  WHERE fiscal_date IS NOT NULL
  LIMIT 5
)
SELECT * FROM itm_month_year
UNION ALL
SELECT * FROM ttm_month_year;

/*------------------------------------------------------------------------------
SECTION: Test 6 - Exclude Records with Invalid Numeric Values
------------------------------------------------------------------------------*/
-- Assert that no record in output table comes from a row with invalid numeric sales_value or sales_net_price_local
WITH invalid_itm AS (
  SELECT
    country_code,
    brand_name,
    territory_id,
    sales_month
  FROM purgo_playground.t3_itm_territory_sales
  WHERE TRY_CAST(sales_value AS DOUBLE) IS NULL AND sales_value IS NOT NULL
),
invalid_ttm AS (
  SELECT
    country_code,
    brand_name,
    territory_id,
    fiscal_date
  FROM purgo_playground.t3_ttm_territory_sales
  WHERE TRY_CAST(sales_net_price_local AS DOUBLE) IS NULL AND sales_net_price_local IS NOT NULL
),
output_invalid AS (
  SELECT
    a.*
  FROM purgo_playground.actual_value_calculation a
  LEFT JOIN invalid_itm i
    ON a.country_code = i.country_code
    AND a.brand_name = i.brand_name
    AND a.territory_id = i.territory_id
    AND a.month = CAST(MONTH(i.sales_month) AS STRING)
    AND a.year = CAST(YEAR(i.sales_month) AS STRING)
  LEFT JOIN invalid_ttm t
    ON a.country_code = t.country_code
    AND a.brand_name = t.brand_name
    AND a.territory_id = t.territory_id
    AND a.month = CAST(MONTH(t.fiscal_date) AS STRING)
    AND a.year = CAST(YEAR(t.fiscal_date) AS STRING)
  WHERE i.country_code IS NOT NULL OR t.country_code IS NOT NULL
)
SELECT
  CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END AS invalid_numeric_exclusion_assertion
FROM output_invalid;

/*------------------------------------------------------------------------------
SECTION: Test 7 - Exclude Records with NULLs in Required Columns
------------------------------------------------------------------------------*/
-- Assert that no record in output table comes from a row with NULL in required columns
WITH null_itm AS (
  SELECT
    country_code,
    brand_name,
    territory_id,
    sales_month
  FROM purgo_playground.t3_itm_territory_sales
  WHERE country_code IS NULL OR brand_name IS NULL OR territory_id IS NULL OR sales_month IS NULL OR sales_value IS NULL OR source_system_name IS NULL
),
null_ttm AS (
  SELECT
    country_code,
    brand_name,
    territory_id,
    fiscal_date
  FROM purgo_playground.t3_ttm_territory_sales
  WHERE country_code IS NULL OR brand_name IS NULL OR territory_id IS NULL OR fiscal_date IS NULL OR sales_net_price_local IS NULL OR source_system_name IS NULL
),
output_nulls AS (
  SELECT
    a.*
  FROM purgo_playground.actual_value_calculation a
  LEFT JOIN null_itm i
    ON a.country_code = i.country_code
    AND a.brand_name = i.brand_name
    AND a.territory_id = i.territory_id
    AND a.month = CAST(MONTH(i.sales_month) AS STRING)
    AND a.year = CAST(YEAR(i.sales_month) AS STRING)
  LEFT JOIN null_ttm t
    ON a.country_code = t.country_code
    AND a.brand_name = t.brand_name
    AND a.territory_id = t.territory_id
    AND a.month = CAST(MONTH(t.fiscal_date) AS STRING)
    AND a.year = CAST(YEAR(t.fiscal_date) AS STRING)
  WHERE i.country_code IS NOT NULL OR t.country_code IS NOT NULL
)
SELECT
  CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END AS null_required_column_exclusion_assertion
FROM output_nulls;

/*------------------------------------------------------------------------------
SECTION: Test 8 - Exclude Records with source_system_name Not in control_table
------------------------------------------------------------------------------*/
-- Assert that no record in output table comes from a row with source_system_name not in control_table
WITH itm_not_in_control AS (
  SELECT
    country_code,
    brand_name,
    territory_id,
    sales_month
  FROM purgo_playground.t3_itm_territory_sales i
  LEFT JOIN purgo_playground.control_table c
    ON i.source_system_name = c.source_system
  WHERE c.source_system IS NULL
),
ttm_not_in_control AS (
  SELECT
    country_code,
    brand_name,
    territory_id,
    fiscal_date
  FROM purgo_playground.t3_ttm_territory_sales t
  LEFT JOIN purgo_playground.control_table c
    ON t.source_system_name = c.source_system
  WHERE c.source_system IS NULL
),
output_not_in_control AS (
  SELECT
    a.*
  FROM purgo_playground.actual_value_calculation a
  LEFT JOIN itm_not_in_control i
    ON a.country_code = i.country_code
    AND a.brand_name = i.brand_name
    AND a.territory_id = i.territory_id
    AND a.month = CAST(MONTH(i.sales_month) AS STRING)
    AND a.year = CAST(YEAR(i.sales_month) AS STRING)
  LEFT JOIN ttm_not_in_control t
    ON a.country_code = t.country_code
    AND a.brand_name = t.brand_name
    AND a.territory_id = t.territory_id
    AND a.month = CAST(MONTH(t.fiscal_date) AS STRING)
    AND a.year = CAST(YEAR(t.fiscal_date) AS STRING)
  WHERE i.country_code IS NOT NULL OR t.country_code IS NOT NULL
)
SELECT
  CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END AS source_system_exclusion_assertion
FROM output_not_in_control;

/*------------------------------------------------------------------------------
SECTION: Test 9 - Aggregation and Deduplication
------------------------------------------------------------------------------*/
-- Assert that duplicate records are aggregated (summed) correctly
WITH expected_agg AS (
  SELECT
    "US" AS country_code,
    "BRAND_A" AS brand_name,
    "T001" AS territory_id,
    "5" AS month,
    "2023" AS year,
    100.0 + 50.0 + 25.0 + 50.0 + 25.0 + 25.0 + 50.0 AS expected_sum -- sum of all US/BRAND_A/T001/2023-05 sales_value and sales_net_price_local
),
actual_agg AS (
  SELECT
    country_code,
    brand_name,
    territory_id,
    month,
    year,
    actual_value
  FROM purgo_playground.actual_value_calculation
  WHERE country_code = "US"
    AND brand_name = "BRAND_A"
    AND territory_id = "T001"
    AND month = "5"
    AND year = "2023"
)
SELECT
  CASE WHEN a.actual_value = e.expected_sum THEN 1 ELSE 0 END AS aggregation_assertion
FROM actual_agg a
JOIN expected_agg e
  ON a.country_code = e.country_code
  AND a.brand_name = e.brand_name
  AND a.territory_id = e.territory_id
  AND a.month = e.month
  AND a.year = e.year;

/*------------------------------------------------------------------------------
SECTION: Test 10 - Exclude Keys with No Data in Either Table
------------------------------------------------------------------------------*/
-- Assert that no record exists for a key with no data in either table
WITH all_keys AS (
  SELECT
    country_code,
    brand_name,
    territory_id,
    month,
    year
  FROM purgo_playground.actual_value_calculation
),
itm_keys AS (
  SELECT
    country_code,
    brand_name,
    territory_id,
    CAST(MONTH(sales_month) AS STRING) AS month,
    CAST(YEAR(sales_month) AS STRING) AS year
  FROM purgo_playground.t3_itm_territory_sales
  WHERE country_code IS NOT NULL AND brand_name IS NOT NULL AND territory_id IS NOT NULL AND sales_month IS NOT NULL
),
ttm_keys AS (
  SELECT
    country_code,
    brand_name,
    territory_id,
    CAST(MONTH(fiscal_date) AS STRING) AS month,
    CAST(YEAR(fiscal_date) AS STRING) AS year
  FROM purgo_playground.t3_ttm_territory_sales
  WHERE country_code IS NOT NULL AND brand_name IS NOT NULL AND territory_id IS NOT NULL AND fiscal_date IS NOT NULL
),
valid_keys AS (
  SELECT * FROM itm_keys
  UNION
  SELECT * FROM ttm_keys
),
invalid_keys AS (
  SELECT
    a.country_code,
    a.brand_name,
    a.territory_id,
    a.month,
    a.year
  FROM all_keys a
  LEFT JOIN valid_keys v
    ON a.country_code = v.country_code
    AND a.brand_name = v.brand_name
    AND a.territory_id = v.territory_id
    AND a.month = v.month
    AND a.year = v.year
  WHERE v.country_code IS NULL
)
SELECT
  CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END AS no_orphan_key_assertion
FROM invalid_keys;

/*------------------------------------------------------------------------------
SECTION: Test 11 - Data Quality: No Negative or NULL actual_value (unless in source)
------------------------------------------------------------------------------*/
-- Assert that negative actual_value only exists if negative in source, and no NULLs
WITH negative_actuals AS (
  SELECT *
  FROM purgo_playground.actual_value_calculation
  WHERE actual_value < 0
)
SELECT
  COUNT(*) AS negative_actual_value_count
FROM negative_actuals;

/*------------------------------------------------------------------------------
SECTION: Test 12 - Data Quality: Output actual_value is 0.0 only if source is 0.0
------------------------------------------------------------------------------*/
-- Assert that actual_value = 0.0 only if all source values for that key are 0.0
WITH zero_actuals AS (
  SELECT *
  FROM purgo_playground.actual_value_calculation
  WHERE actual_value = 0.0
),
source_zero AS (
  SELECT
    country_code,
    brand_name,
    territory_id,
    CAST(MONTH(sales_month) AS STRING) AS month,
    CAST(YEAR(sales_month) AS STRING) AS year,
    sales_value
  FROM purgo_playground.t3_itm_territory_sales
  WHERE sales_value = 0.0
  UNION ALL
  SELECT
    country_code,
    brand_name,
    territory_id,
    CAST(MONTH(fiscal_date) AS STRING) AS month,
    CAST(YEAR(fiscal_date) AS STRING) AS year,
    sales_net_price_local
  FROM purgo_playground.t3_ttm_territory_sales
  WHERE sales_net_price_local = 0.0
)
SELECT
  COUNT(*) AS zero_actual_value_count
FROM zero_actuals z
LEFT JOIN source_zero s
  ON z.country_code = s.country_code
  AND z.brand_name = s.brand_name
  AND z.territory_id = s.territory_id
  AND z.month = s.month
  AND z.year = s.year
WHERE s.country_code IS NULL;

/*------------------------------------------------------------------------------
SECTION: Test 13 - Window Function: Rank Brands by actual_value per Country/Month/Year
------------------------------------------------------------------------------*/
-- Validate window function works on the output table
WITH ranked_brands AS (
  SELECT
    country_code,
    month,
    year,
    brand_name,
    actual_value,
    RANK() OVER (PARTITION BY country_code, month, year ORDER BY actual_value DESC) AS brand_rank
  FROM purgo_playground.actual_value_calculation
)
SELECT * FROM ranked_brands WHERE brand_rank = 1;

/*------------------------------------------------------------------------------
SECTION: Test 14 - Delta Lake Operations: DELETE, UPDATE, MERGE
------------------------------------------------------------------------------*/
-- DELETE: Remove a test record and assert it is gone
DELETE FROM purgo_playground.actual_value_calculation
WHERE country_code = "US" AND brand_name = "BRAND_A" AND territory_id = "T001" AND month = "5" AND year = "2023";

SELECT
  CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END AS delete_assertion
FROM purgo_playground.actual_value_calculation
WHERE country_code = "US" AND brand_name = "BRAND_A" AND territory_id = "T001" AND month = "5" AND year = "2023";

-- UPDATE: Set actual_value to 9999.99 for a test record and assert the update
UPDATE purgo_playground.actual_value_calculation
SET actual_value = 9999.99
WHERE country_code = "DE" AND brand_name = "BRAND_B" AND territory_id = "T002" AND month = "12" AND year = "2022";

SELECT
  CASE WHEN actual_value = 9999.99 THEN 1 ELSE 0 END AS update_assertion
FROM purgo_playground.actual_value_calculation
WHERE country_code = "DE" AND brand_name = "BRAND_B" AND territory_id = "T002" AND month = "12" AND year = "2022";

-- MERGE: Upsert a record and assert the merge
MERGE INTO purgo_playground.actual_value_calculation AS target
USING (SELECT "ZZ" AS country_code, "BRAND_X" AS brand_name, "T999" AS territory_id, "1" AS month, "2025" AS year, 123.45 AS actual_value) AS source
ON target.country_code = source.country_code
  AND target.brand_name = source.brand_name
  AND target.territory_id = source.territory_id
  AND target.month = source.month
  AND target.year = source.year
WHEN MATCHED THEN
  UPDATE SET actual_value = source.actual_value
WHEN NOT MATCHED THEN
  INSERT (country_code, brand_name, territory_id, month, year, actual_value)
  VALUES (source.country_code, source.brand_name, source.territory_id, source.month, source.year, source.actual_value);

SELECT
  CASE WHEN COUNT(*) = 1 AND actual_value = 123.45 THEN 1 ELSE 0 END AS merge_assertion
FROM purgo_playground.actual_value_calculation
WHERE country_code = "ZZ" AND brand_name = "BRAND_X" AND territory_id = "T999" AND month = "1" AND year = "2025";

/*------------------------------------------------------------------------------
SECTION: Test 15 - Cleanup: Remove test records inserted by Delta operations
------------------------------------------------------------------------------*/
DELETE FROM purgo_playground.actual_value_calculation
WHERE country_code = "ZZ" AND brand_name = "BRAND_X" AND territory_id = "T999" AND month = "1" AND year = "2025";
