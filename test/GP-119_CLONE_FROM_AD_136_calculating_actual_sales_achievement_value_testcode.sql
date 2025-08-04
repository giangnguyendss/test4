/*
==========================================================================================
Databricks SQL Comprehensive Test Suite for Actual Sales Value Calculation
==========================================================================================
- This test suite validates the calculation of actual_value for each brand_name, country_code,
  territory_id, year, and month by summing sales_value from t3_itm_territory_sales and 
  sales_net_price_local from t3_ttm_territory_sales, with all required join, filter, 
  aggregation, data type, and data quality rules as per requirements.
- All test queries use only Databricks SQL and native data types.
- All assertions are implemented as SQL queries with explicit checks and error raising.
- All code is commented for clarity and maintainability.
==========================================================================================
*/

/*------------------------------------------------------------------------------
SECTION: SETUP & CLEANUP
------------------------------------------------------------------------------*/

/* -- Clean up output table before test run to ensure idempotency */
DELETE FROM purgo_playground.actual_sales_summary;

/*------------------------------------------------------------------------------
SECTION: SCHEMA VALIDATION TESTS
------------------------------------------------------------------------------*/

/* -- Test: Validate t3_itm_territory_sales schema (column existence and types) */
WITH itm_schema AS (
  SELECT 
    COUNT(*) AS missing_columns
  FROM (
    SELECT 
      CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END AS missing_sales_value
    FROM information_schema.columns
    WHERE table_schema = "purgo_playground"
      AND table_name = "t3_itm_territory_sales"
      AND column_name = "sales_value"
      AND data_type IN ("double", "float", "decimal")
  )
)
SELECT 
  CASE 
    WHEN missing_columns > 0 THEN 
      RAISE_ERROR("Column \"sales_value\" does not exist in table \"t3_itm_territory_sales\"")
    ELSE "OK"
  END AS itm_sales_value_column_check
FROM itm_schema;

/* -- Test: Validate t3_itm_territory_sales.sales_month is DATE type */
WITH itm_sales_month_type AS (
  SELECT data_type
  FROM information_schema.columns
  WHERE table_schema = "purgo_playground"
    AND table_name = "t3_itm_territory_sales"
    AND column_name = "sales_month"
)
SELECT 
  CASE 
    WHEN COUNT(*) = 0 THEN RAISE_ERROR("Column \"sales_month\" does not exist in table \"t3_itm_territory_sales\"")
    WHEN MAX(data_type) <> "date" THEN RAISE_ERROR("Column \"sales_month\" must be of type DATE")
    ELSE "OK"
  END AS itm_sales_month_type_check
FROM itm_sales_month_type;

/* -- Test: Validate t3_ttm_territory_sales schema (column existence and types) */
WITH ttm_schema AS (
  SELECT 
    COUNT(*) AS missing_columns
  FROM (
    SELECT 
      CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END AS missing_sales_net_price_local
    FROM information_schema.columns
    WHERE table_schema = "purgo_playground"
      AND table_name = "t3_ttm_territory_sales"
      AND column_name = "sales_net_price_local"
      AND data_type IN ("double", "float", "decimal")
  )
)
SELECT 
  CASE 
    WHEN missing_columns > 0 THEN 
      RAISE_ERROR("Column \"sales_net_price_local\" does not exist in table \"t3_ttm_territory_sales\"")
    ELSE "OK"
  END AS ttm_sales_net_price_local_column_check
FROM ttm_schema;

/* -- Test: Validate t3_ttm_territory_sales.fiscal_date is DATE type */
WITH ttm_fiscal_date_type AS (
  SELECT data_type
  FROM information_schema.columns
  WHERE table_schema = "purgo_playground"
    AND table_name = "t3_ttm_territory_sales"
    AND column_name = "fiscal_date"
)
SELECT 
  CASE 
    WHEN COUNT(*) = 0 THEN RAISE_ERROR("Column \"fiscal_date\" does not exist in table \"t3_ttm_territory_sales\"")
    WHEN MAX(data_type) <> "date" THEN RAISE_ERROR("Column \"fiscal_date\" must be of type DATE")
    ELSE "OK"
  END AS ttm_fiscal_date_type_check
FROM ttm_fiscal_date_type;

/*------------------------------------------------------------------------------
SECTION: FUNCTIONALITY TESTS - ACTUAL VALUE CALCULATION
------------------------------------------------------------------------------*/

/* -- Main Calculation CTE: actual_value for each group, with all filters and rules */
WITH
  /* -- Filtered ITM sales: only valid, non-null keys, and source_system_name in control_table */
  itm_valid AS (
    SELECT
      i.country_code,
      i.brand_name,
      i.territory_id,
      i.source_system_name,
      i.sales_value,
      i.sales_month
    FROM purgo_playground.t3_itm_territory_sales i
    INNER JOIN purgo_playground.control_table c
      ON i.country_code = c.country_code
      AND i.brand_name = c.brand_name
      AND i.source_system_name = c.source_system
    WHERE i.country_code IS NOT NULL
      AND i.brand_name IS NOT NULL
      AND i.territory_id IS NOT NULL
      AND i.sales_month IS NOT NULL
  ),
  /* -- Filtered TTM sales: only valid, non-null keys, and source_system_name in control_table */
  ttm_valid AS (
    SELECT
      t.country_code,
      t.brand_name,
      t.territory_id,
      t.source_system_name,
      t.sales_net_price_local,
      t.fiscal_date
    FROM purgo_playground.t3_ttm_territory_sales t
    INNER JOIN purgo_playground.control_table c
      ON t.country_code = c.country_code
      AND t.brand_name = c.brand_name
      AND t.source_system_name = c.source_system
    WHERE t.country_code IS NOT NULL
      AND t.brand_name IS NOT NULL
      AND t.territory_id IS NOT NULL
      AND t.fiscal_date IS NOT NULL
  ),
  /* -- Join on all required keys and extracted year/month */
  joined_sales AS (
    SELECT
      itm.country_code,
      itm.brand_name,
      itm.territory_id,
      EXTRACT(YEAR FROM itm.sales_month) AS year,
      EXTRACT(MONTH FROM itm.sales_month) AS month,
      itm.sales_value,
      ttm.sales_net_price_local
    FROM itm_valid itm
    INNER JOIN ttm_valid ttm
      ON itm.country_code = ttm.country_code
      AND itm.brand_name = ttm.brand_name
      AND itm.territory_id = ttm.territory_id
      AND EXTRACT(YEAR FROM itm.sales_month) = EXTRACT(YEAR FROM ttm.fiscal_date)
      AND EXTRACT(MONTH FROM itm.sales_month) = EXTRACT(MONTH FROM ttm.fiscal_date)
  ),
  /* -- Aggregate and handle nulls as zero */
  actual_value_agg AS (
    SELECT
      country_code,
      brand_name,
      territory_id,
      year,
      month,
      SUM(COALESCE(sales_value, 0.0)) + SUM(COALESCE(sales_net_price_local, 0.0)) AS actual_value
    FROM joined_sales
    GROUP BY country_code, brand_name, territory_id, year, month
  )
/* -- Output: Insert into actual_sales_summary for test validation */
INSERT INTO purgo_playground.actual_sales_summary (country_code, brand_name, territory_id, year, month, actual_value)
SELECT country_code, brand_name, territory_id, year, month, actual_value
FROM actual_value_agg;

/*------------------------------------------------------------------------------
SECTION: DATA QUALITY & OUTPUT VALIDATION TESTS
------------------------------------------------------------------------------*/

/* -- Test: Validate that all output columns exist and have correct data types */
WITH output_schema AS (
  SELECT column_name, data_type
  FROM information_schema.columns
  WHERE table_schema = "purgo_playground"
    AND table_name = "actual_sales_summary"
    AND column_name IN ("country_code", "brand_name", "territory_id", "year", "month", "actual_value")
)
SELECT
  CASE
    WHEN COUNT(*) <> 6 THEN RAISE_ERROR("Output table schema does not have all required columns")
    WHEN MAX(CASE WHEN column_name = "country_code" AND data_type = "string" THEN 1 ELSE 0 END) = 0 THEN RAISE_ERROR("country_code must be string")
    WHEN MAX(CASE WHEN column_name = "brand_name" AND data_type = "string" THEN 1 ELSE 0 END) = 0 THEN RAISE_ERROR("brand_name must be string")
    WHEN MAX(CASE WHEN column_name = "territory_id" AND data_type = "string" THEN 1 ELSE 0 END) = 0 THEN RAISE_ERROR("territory_id must be string")
    WHEN MAX(CASE WHEN column_name = "year" AND data_type = "int" THEN 1 ELSE 0 END) = 0 THEN RAISE_ERROR("year must be int")
    WHEN MAX(CASE WHEN column_name = "month" AND data_type = "int" THEN 1 ELSE 0 END) = 0 THEN RAISE_ERROR("month must be int")
    WHEN MAX(CASE WHEN column_name = "actual_value" AND data_type = "double" THEN 1 ELSE 0 END) = 0 THEN RAISE_ERROR("actual_value must be double")
    ELSE "OK"
  END AS output_schema_check
FROM output_schema;

/* -- Test: Validate that no output row contains null in country_code, brand_name, or territory_id */
SELECT
  CASE
    WHEN COUNT(*) > 0 THEN RAISE_ERROR("Output contains null in country_code, brand_name, or territory_id")
    ELSE "OK"
  END AS null_key_check
FROM purgo_playground.actual_sales_summary
WHERE country_code IS NULL OR brand_name IS NULL OR territory_id IS NULL;

/* -- Test: Validate that only records with matching year and month in both tables are included */
WITH itm_months AS (
  SELECT DISTINCT country_code, brand_name, territory_id, EXTRACT(YEAR FROM sales_month) AS year, EXTRACT(MONTH FROM sales_month) AS month
  FROM purgo_playground.t3_itm_territory_sales
  WHERE sales_month IS NOT NULL
),
ttm_months AS (
  SELECT DISTINCT country_code, brand_name, territory_id, EXTRACT(YEAR FROM fiscal_date) AS year, EXTRACT(MONTH FROM fiscal_date) AS month
  FROM purgo_playground.t3_ttm_territory_sales
  WHERE fiscal_date IS NOT NULL
),
valid_pairs AS (
  SELECT i.country_code, i.brand_name, i.territory_id, i.year, i.month
  FROM itm_months i
  INNER JOIN ttm_months t
    ON i.country_code = t.country_code
    AND i.brand_name = t.brand_name
    AND i.territory_id = t.territory_id
    AND i.year = t.year
    AND i.month = t.month
)
SELECT
  CASE
    WHEN EXISTS (
      SELECT 1
      FROM purgo_playground.actual_sales_summary s
      LEFT ANTI JOIN valid_pairs v
        ON s.country_code = v.country_code
        AND s.brand_name = v.brand_name
        AND s.territory_id = v.territory_id
        AND s.year = v.year
        AND s.month = v.month
    ) THEN RAISE_ERROR("Output contains records with non-matching year/month in both tables")
    ELSE "OK"
  END AS join_year_month_check;

/* -- Test: Validate that records with source_system_name not in control_table are excluded */
WITH invalid_source AS (
  SELECT i.country_code, i.brand_name, i.territory_id, EXTRACT(YEAR FROM i.sales_month) AS year, EXTRACT(MONTH FROM i.sales_month) AS month
  FROM purgo_playground.t3_itm_territory_sales i
  LEFT ANTI JOIN purgo_playground.control_table c
    ON i.country_code = c.country_code
    AND i.brand_name = c.brand_name
    AND i.source_system_name = c.source_system
  WHERE i.sales_month IS NOT NULL
  UNION ALL
  SELECT t.country_code, t.brand_name, t.territory_id, EXTRACT(YEAR FROM t.fiscal_date) AS year, EXTRACT(MONTH FROM t.fiscal_date) AS month
  FROM purgo_playground.t3_ttm_territory_sales t
  LEFT ANTI JOIN purgo_playground.control_table c
    ON t.country_code = c.country_code
    AND t.brand_name = c.brand_name
    AND t.source_system_name = c.source_system
  WHERE t.fiscal_date IS NOT NULL
)
SELECT
  CASE
    WHEN EXISTS (
      SELECT 1
      FROM purgo_playground.actual_sales_summary s
      INNER JOIN invalid_source inv
        ON s.country_code = inv.country_code
        AND s.brand_name = inv.brand_name
        AND s.territory_id = inv.territory_id
        AND s.year = inv.year
        AND s.month = inv.month
    ) THEN RAISE_ERROR("Output contains records with source_system_name not in control_table")
    ELSE "OK"
  END AS invalid_source_check;

/* -- Test: Validate null handling: actual_value is correct when sales_value or sales_net_price_local is null */
WITH null_handling_test AS (
  SELECT
    s.country_code, s.brand_name, s.territory_id, s.year, s.month, s.actual_value,
    COALESCE(i.sales_value, 0.0) + COALESCE(t.sales_net_price_local, 0.0) AS expected_value
  FROM purgo_playground.actual_sales_summary s
  LEFT JOIN purgo_playground.t3_itm_territory_sales i
    ON s.country_code = i.country_code
    AND s.brand_name = i.brand_name
    AND s.territory_id = i.territory_id
    AND s.year = EXTRACT(YEAR FROM i.sales_month)
    AND s.month = EXTRACT(MONTH FROM i.sales_month)
  LEFT JOIN purgo_playground.t3_ttm_territory_sales t
    ON s.country_code = t.country_code
    AND s.brand_name = t.brand_name
    AND s.territory_id = t.territory_id
    AND s.year = EXTRACT(YEAR FROM t.fiscal_date)
    AND s.month = EXTRACT(MONTH FROM t.fiscal_date)
  WHERE (i.sales_value IS NULL OR t.sales_net_price_local IS NULL)
)
SELECT
  CASE
    WHEN COUNT(*) > 0 AND MAX(ABS(actual_value - expected_value)) > 0.0001 THEN RAISE_ERROR("Null handling failed: actual_value does not match expected sum with nulls as zero")
    ELSE "OK"
  END AS null_handling_check
FROM null_handling_test;

/* -- Test: Validate aggregation for multiple records per group */
WITH agg_test AS (
  SELECT
    s.country_code, s.brand_name, s.territory_id, s.year, s.month, s.actual_value,
    (
      SELECT 
        SUM(COALESCE(i.sales_value, 0.0))
      FROM purgo_playground.t3_itm_territory_sales i
      WHERE i.country_code = s.country_code
        AND i.brand_name = s.brand_name
        AND i.territory_id = s.territory_id
        AND EXTRACT(YEAR FROM i.sales_month) = s.year
        AND EXTRACT(MONTH FROM i.sales_month) = s.month
    ) +
    (
      SELECT 
        SUM(COALESCE(t.sales_net_price_local, 0.0))
      FROM purgo_playground.t3_ttm_territory_sales t
      WHERE t.country_code = s.country_code
        AND t.brand_name = s.brand_name
        AND t.territory_id = s.territory_id
        AND EXTRACT(YEAR FROM t.fiscal_date) = s.year
        AND EXTRACT(MONTH FROM t.fiscal_date) = s.month
    ) AS expected_value
  FROM purgo_playground.actual_sales_summary s
)
SELECT
  CASE
    WHEN COUNT(*) > 0 AND MAX(ABS(actual_value - expected_value)) > 0.0001 THEN RAISE_ERROR("Aggregation failed: actual_value does not match expected sum")
    ELSE "OK"
  END AS aggregation_check
FROM agg_test;

/* -- Test: Validate year and month extraction from date columns */
WITH year_month_test AS (
  SELECT
    s.country_code, s.brand_name, s.territory_id, s.year, s.month,
    EXTRACT(YEAR FROM i.sales_month) AS itm_year, EXTRACT(MONTH FROM i.sales_month) AS itm_month,
    EXTRACT(YEAR FROM t.fiscal_date) AS ttm_year, EXTRACT(MONTH FROM t.fiscal_date) AS ttm_month
  FROM purgo_playground.actual_sales_summary s
  LEFT JOIN purgo_playground.t3_itm_territory_sales i
    ON s.country_code = i.country_code
    AND s.brand_name = i.brand_name
    AND s.territory_id = i.territory_id
    AND s.year = EXTRACT(YEAR FROM i.sales_month)
    AND s.month = EXTRACT(MONTH FROM i.sales_month)
  LEFT JOIN purgo_playground.t3_ttm_territory_sales t
    ON s.country_code = t.country_code
    AND s.brand_name = t.brand_name
    AND s.territory_id = t.territory_id
    AND s.year = EXTRACT(YEAR FROM t.fiscal_date)
    AND s.month = EXTRACT(MONTH FROM t.fiscal_date)
)
SELECT
  CASE
    WHEN COUNT(*) > 0 AND (MIN(s.year) <> MIN(itm_year) OR MIN(s.month) <> MIN(itm_month)) THEN RAISE_ERROR("Year/month extraction from sales_month failed")
    WHEN COUNT(*) > 0 AND (MIN(s.year) <> MIN(ttm_year) OR MIN(s.month) <> MIN(ttm_month)) THEN RAISE_ERROR("Year/month extraction from fiscal_date failed")
    ELSE "OK"
  END AS year_month_extraction_check
FROM year_month_test;

/* -- Test: Validate exclusion of records with null brand_name, country_code, or territory_id in input */
WITH null_input AS (
  SELECT country_code, brand_name, territory_id
  FROM purgo_playground.t3_itm_territory_sales
  WHERE country_code IS NULL OR brand_name IS NULL OR territory_id IS NULL
  UNION ALL
  SELECT country_code, brand_name, territory_id
  FROM purgo_playground.t3_ttm_territory_sales
  WHERE country_code IS NULL OR brand_name IS NULL OR territory_id IS NULL
)
SELECT
  CASE
    WHEN EXISTS (
      SELECT 1
      FROM purgo_playground.actual_sales_summary s
      INNER JOIN null_input n
        ON s.country_code = n.country_code
        AND s.brand_name = n.brand_name
        AND s.territory_id = n.territory_id
    ) THEN RAISE_ERROR("Output contains records with null brand_name, country_code, or territory_id from input")
    ELSE "OK"
  END AS null_input_exclusion_check;

/*------------------------------------------------------------------------------
SECTION: PERFORMANCE TEST (ROW COUNT VALIDATION)
------------------------------------------------------------------------------*/

/* -- Test: Validate that row count in output matches expected number of valid groups */
WITH valid_groups AS (
  SELECT
    itm.country_code,
    itm.brand_name,
    itm.territory_id,
    EXTRACT(YEAR FROM itm.sales_month) AS year,
    EXTRACT(MONTH FROM itm.sales_month) AS month
  FROM purgo_playground.t3_itm_territory_sales itm
  INNER JOIN purgo_playground.t3_ttm_territory_sales ttm
    ON itm.country_code = ttm.country_code
    AND itm.brand_name = ttm.brand_name
    AND itm.territory_id = ttm.territory_id
    AND EXTRACT(YEAR FROM itm.sales_month) = EXTRACT(YEAR FROM ttm.fiscal_date)
    AND EXTRACT(MONTH FROM itm.sales_month) = EXTRACT(MONTH FROM ttm.fiscal_date)
  INNER JOIN purgo_playground.control_table c1
    ON itm.country_code = c1.country_code
    AND itm.brand_name = c1.brand_name
    AND itm.source_system_name = c1.source_system
  INNER JOIN purgo_playground.control_table c2
    ON ttm.country_code = c2.country_code
    AND ttm.brand_name = c2.brand_name
    AND ttm.source_system_name = c2.source_system
  WHERE itm.country_code IS NOT NULL
    AND itm.brand_name IS NOT NULL
    AND itm.territory_id IS NOT NULL
    AND itm.sales_month IS NOT NULL
    AND ttm.fiscal_date IS NOT NULL
)
SELECT
  CASE
    WHEN (SELECT COUNT(*) FROM purgo_playground.actual_sales_summary) <> (SELECT COUNT(DISTINCT country_code, brand_name, territory_id, year, month) FROM valid_groups)
    THEN RAISE_ERROR("Row count in output does not match expected number of valid groups")
    ELSE "OK"
  END AS row_count_performance_check;

/*------------------------------------------------------------------------------
SECTION: DELTA LAKE & DML OPERATIONS TESTS
------------------------------------------------------------------------------*/

/* -- Test: Delta Lake MERGE, UPDATE, DELETE operations on output table */

/* -- MERGE: Upsert a test row and validate */
MERGE INTO purgo_playground.actual_sales_summary AS target
USING (SELECT "ZZ" AS country_code, "BRAND_TEST" AS brand_name, "T999" AS territory_id, 2099 AS year, 12 AS month, 9999.99 AS actual_value) AS source
ON target.country_code = source.country_code
  AND target.brand_name = source.brand_name
  AND target.territory_id = source.territory_id
  AND target.year = source.year
  AND target.month = source.month
WHEN MATCHED THEN UPDATE SET actual_value = source.actual_value
WHEN NOT MATCHED THEN INSERT (country_code, brand_name, territory_id, year, month, actual_value) VALUES (source.country_code, source.brand_name, source.territory_id, source.year, source.month, source.actual_value);

/* -- Validate MERGE */
SELECT
  CASE
    WHEN COUNT(*) = 1 AND MAX(actual_value) = 9999.99 THEN "OK"
    ELSE RAISE_ERROR("Delta Lake MERGE failed")
  END AS delta_merge_check
FROM purgo_playground.actual_sales_summary
WHERE country_code = "ZZ" AND brand_name = "BRAND_TEST" AND territory_id = "T999" AND year = 2099 AND month = 12;

/* -- UPDATE: Change actual_value and validate */
UPDATE purgo_playground.actual_sales_summary
SET actual_value = 8888.88
WHERE country_code = "ZZ" AND brand_name = "BRAND_TEST" AND territory_id = "T999" AND year = 2099 AND month = 12;

SELECT
  CASE
    WHEN COUNT(*) = 1 AND MAX(actual_value) = 8888.88 THEN "OK"
    ELSE RAISE_ERROR("Delta Lake UPDATE failed")
  END AS delta_update_check
FROM purgo_playground.actual_sales_summary
WHERE country_code = "ZZ" AND brand_name = "BRAND_TEST" AND territory_id = "T999" AND year = 2099 AND month = 12;

/* -- DELETE: Remove test row and validate */
DELETE FROM purgo_playground.actual_sales_summary
WHERE country_code = "ZZ" AND brand_name = "BRAND_TEST" AND territory_id = "T999" AND year = 2099 AND month = 12;

SELECT
  CASE
    WHEN COUNT(*) = 0 THEN "OK"
    ELSE RAISE_ERROR("Delta Lake DELETE failed")
  END AS delta_delete_check
FROM purgo_playground.actual_sales_summary
WHERE country_code = "ZZ" AND brand_name = "BRAND_TEST" AND territory_id = "T999" AND year = 2099 AND month = 12;

/*------------------------------------------------------------------------------
SECTION: WINDOW FUNCTION & ANALYTICS TESTS
------------------------------------------------------------------------------*/

/* -- Test: Window function - rank brands by actual_value per country/month/year */
WITH ranked AS (
  SELECT
    country_code,
    year,
    month,
    brand_name,
    actual_value,
    RANK() OVER (PARTITION BY country_code, year, month ORDER BY actual_value DESC) AS brand_rank
  FROM purgo_playground.actual_sales_summary
)
SELECT
  CASE
    WHEN COUNT(*) = 0 THEN RAISE_ERROR("Window function RANK failed: no rows returned")
    ELSE "OK"
  END AS window_function_check
FROM ranked
WHERE brand_rank = 1;

/*------------------------------------------------------------------------------
SECTION: CLEANUP
------------------------------------------------------------------------------*/

/* -- Clean up test row from output table (if not already deleted) */
DELETE FROM purgo_playground.actual_sales_summary
WHERE country_code = "ZZ" AND brand_name = "BRAND_TEST" AND territory_id = "T999" AND year = 2099 AND month = 12;
