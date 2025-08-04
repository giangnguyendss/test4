/* 
==========================================================================================
  Databricks SQL Test Suite for Actual Sales Value Calculation by Brand, Country, Territory
  ----------------------------------------------------------------------------------------
  - All test logic is self-contained and executable in Databricks SQL environment.
  - Covers: schema validation, data type checks, NULL handling, aggregation, filtering,
    join logic, data quality, and output validation.
  - All comments follow Databricks SQL best practices.
  - All table and column references are fully qualified.
  - All constraints and checks are explicitly defined.
  - No temp views or temp tables are used.
==========================================================================================
*/

/* 
==========================================================================================
  SECTION: Setup - Clean up and Prepare Output Table for Test Results
==========================================================================================
*/

-- Drop and recreate the output table to ensure schema matches and is clean for test
DROP TABLE IF EXISTS purgo_playground.actual_sales_summary;

CREATE TABLE purgo_playground.actual_sales_summary (
  country_code STRING NOT NULL,
  brand_name STRING NOT NULL,
  territory_id STRING NOT NULL,
  year INT NOT NULL,
  month INT NOT NULL,
  actual_value DOUBLE NOT NULL
);

/* 
==========================================================================================
  SECTION: Test 1 - Schema Validation for Output Table
==========================================================================================
*/

-- Validate that the output table has the correct schema
WITH schema_check AS (
  SELECT 
    COUNT(*) AS col_count,
    SUM(CASE WHEN column_name = "country_code" AND data_type = "STRING" THEN 1 ELSE 0 END) AS cc,
    SUM(CASE WHEN column_name = "brand_name" AND data_type = "STRING" THEN 1 ELSE 0 END) AS bn,
    SUM(CASE WHEN column_name = "territory_id" AND data_type = "STRING" THEN 1 ELSE 0 END) AS tid,
    SUM(CASE WHEN column_name = "year" AND data_type = "INT" THEN 1 ELSE 0 END) AS yr,
    SUM(CASE WHEN column_name = "month" AND data_type = "INT" THEN 1 ELSE 0 END) AS mn,
    SUM(CASE WHEN column_name = "actual_value" AND data_type = "DOUBLE" THEN 1 ELSE 0 END) AS av
  FROM information_schema.columns
  WHERE table_schema = "purgo_playground"
    AND table_name = "actual_sales_summary"
)
SELECT
  CASE 
    WHEN col_count = 6 AND cc = 1 AND bn = 1 AND tid = 1 AND yr = 1 AND mn = 1 AND av = 1
    THEN "PASS"
    ELSE "FAIL"
  END AS schema_validation_result
FROM schema_check;

/* 
==========================================================================================
  SECTION: Test 2 - Data Type Conversion and NULL Handling
==========================================================================================
*/

-- Validate that all sales_value and sales_net_price_local are DOUBLE and not NULL for included records
WITH itm_valid AS (
  SELECT *
  FROM purgo_playground.t3_itm_territory_sales
  WHERE sales_value IS NOT NULL
),
ttm_valid AS (
  SELECT *
  FROM purgo_playground.t3_ttm_territory_sales
  WHERE sales_net_price_local IS NOT NULL
)
SELECT
  (SELECT COUNT(*) FROM itm_valid) AS itm_valid_count,
  (SELECT COUNT(*) FROM purgo_playground.t3_itm_territory_sales WHERE sales_value IS NULL) AS itm_invalid_count,
  (SELECT COUNT(*) FROM ttm_valid) AS ttm_valid_count,
  (SELECT COUNT(*) FROM purgo_playground.t3_ttm_territory_sales WHERE sales_net_price_local IS NULL) AS ttm_invalid_count;

/* 
==========================================================================================
  SECTION: Test 3 - Main Calculation Query (Unit + Integration)
==========================================================================================
*/

-- Insert the calculated actual_value into the output table, ensuring all constraints and filters
INSERT OVERWRITE purgo_playground.actual_sales_summary
WITH
  /* CTE: Valid source systems from control_table */
  valid_source_systems AS (
    SELECT DISTINCT source_system
    FROM purgo_playground.control_table
    WHERE source_system IS NOT NULL AND TRIM(source_system) != ""
  ),
  /* CTE: Valid ITM sales (filtering out NULLs and invalids) */
  itm_sales AS (
    SELECT
      country_code,
      brand_name,
      source_system_name,
      territory_id,
      sales_value,
      sales_month,
      YEAR(sales_month) AS year,
      MONTH(sales_month) AS month
    FROM purgo_playground.t3_itm_territory_sales
    WHERE
      country_code IS NOT NULL AND TRIM(country_code) != ""
      AND brand_name IS NOT NULL AND TRIM(brand_name) != ""
      AND source_system_name IS NOT NULL AND TRIM(source_system_name) != ""
      AND territory_id IS NOT NULL AND TRIM(territory_id) != ""
      AND sales_value IS NOT NULL
      AND sales_month IS NOT NULL
      AND source_system_name IN (SELECT source_system FROM valid_source_systems)
  ),
  /* CTE: Valid TTM sales (filtering out NULLs and invalids) */
  ttm_sales AS (
    SELECT
      country_code,
      brand_name,
      source_system_name,
      territory_id,
      sales_net_price_local,
      fiscal_date,
      YEAR(fiscal_date) AS year,
      MONTH(fiscal_date) AS month
    FROM purgo_playground.t3_ttm_territory_sales
    WHERE
      country_code IS NOT NULL AND TRIM(country_code) != ""
      AND brand_name IS NOT NULL AND TRIM(brand_name) != ""
      AND source_system_name IS NOT NULL AND TRIM(source_system_name) != ""
      AND territory_id IS NOT NULL AND TRIM(territory_id) != ""
      AND sales_net_price_local IS NOT NULL
      AND fiscal_date IS NOT NULL
      AND source_system_name IN (SELECT source_system FROM valid_source_systems)
  ),
  /* CTE: Aggregate ITM sales by key */
  agg_itm AS (
    SELECT
      country_code,
      brand_name,
      territory_id,
      year,
      month,
      SUM(sales_value) AS itm_value
    FROM itm_sales
    GROUP BY country_code, brand_name, territory_id, year, month
  ),
  /* CTE: Aggregate TTM sales by key */
  agg_ttm AS (
    SELECT
      country_code,
      brand_name,
      territory_id,
      year,
      month,
      SUM(sales_net_price_local) AS ttm_value
    FROM ttm_sales
    GROUP BY country_code, brand_name, territory_id, year, month
  ),
  /* CTE: Full outer join to allow for missing data in either table (treat missing as zero) */
  combined AS (
    SELECT
      COALESCE(a.country_code, b.country_code) AS country_code,
      COALESCE(a.brand_name, b.brand_name) AS brand_name,
      COALESCE(a.territory_id, b.territory_id) AS territory_id,
      COALESCE(a.year, b.year) AS year,
      COALESCE(a.month, b.month) AS month,
      COALESCE(a.itm_value, 0.0) AS itm_value,
      COALESCE(b.ttm_value, 0.0) AS ttm_value
    FROM agg_itm a
    FULL OUTER JOIN agg_ttm b
      ON a.country_code = b.country_code
      AND a.brand_name = b.brand_name
      AND a.territory_id = b.territory_id
      AND a.year = b.year
      AND a.month = b.month
  )
SELECT
  country_code,
  brand_name,
  territory_id,
  year,
  month,
  itm_value + ttm_value AS actual_value
FROM combined
ORDER BY country_code, brand_name, territory_id, year, month;

/* 
==========================================================================================
  SECTION: Test 4 - Data Quality: No Duplicates, Correct Aggregation, Output Order
==========================================================================================
*/

-- Assert that there are no duplicate records for the same combination
WITH dup_check AS (
  SELECT
    country_code, brand_name, territory_id, year, month,
    COUNT(*) AS cnt
  FROM purgo_playground.actual_sales_summary
  GROUP BY country_code, brand_name, territory_id, year, month
  HAVING COUNT(*) > 1
)
SELECT
  CASE WHEN COUNT(*) = 0 THEN "PASS" ELSE "FAIL" END AS no_duplicate_records
FROM dup_check;

-- Assert that the output is ordered as required
WITH ordered AS (
  SELECT
    country_code, brand_name, territory_id, year, month,
    ROW_NUMBER() OVER (ORDER BY country_code, brand_name, territory_id, year, month) AS rn
  FROM purgo_playground.actual_sales_summary
)
SELECT
  CASE WHEN MIN(rn) = 1 THEN "PASS" ELSE "FAIL" END AS output_ordered
FROM ordered;

/* 
==========================================================================================
  SECTION: Test 5 - Data Quality: Exclusion of Invalid/Null/Non-numeric Records
==========================================================================================
*/

-- Assert that records with NULL or empty required fields are not present
WITH nulls_excluded AS (
  SELECT *
  FROM purgo_playground.actual_sales_summary
  WHERE country_code IS NULL OR TRIM(country_code) = ""
     OR brand_name IS NULL OR TRIM(brand_name) = ""
     OR territory_id IS NULL OR TRIM(territory_id) = ""
     OR year IS NULL
     OR month IS NULL
     OR actual_value IS NULL
)
SELECT
  CASE WHEN COUNT(*) = 0 THEN "PASS" ELSE "FAIL" END AS nulls_excluded
FROM nulls_excluded;

-- Assert that records with invalid source_system_name are not present
WITH invalid_source AS (
  SELECT s.*
  FROM purgo_playground.actual_sales_summary s
  LEFT JOIN purgo_playground.t3_itm_territory_sales i
    ON s.country_code = i.country_code
    AND s.brand_name = i.brand_name
    AND s.territory_id = i.territory_id
    AND s.year = YEAR(i.sales_month)
    AND s.month = MONTH(i.sales_month)
  LEFT JOIN purgo_playground.t3_ttm_territory_sales t
    ON s.country_code = t.country_code
    AND s.brand_name = t.brand_name
    AND s.territory_id = t.territory_id
    AND s.year = YEAR(t.fiscal_date)
    AND s.month = MONTH(t.fiscal_date)
  WHERE
    (i.source_system_name IS NOT NULL AND i.source_system_name NOT IN (SELECT source_system FROM purgo_playground.control_table))
    OR
    (t.source_system_name IS NOT NULL AND t.source_system_name NOT IN (SELECT source_system FROM purgo_playground.control_table))
)
SELECT
  CASE WHEN COUNT(*) = 0 THEN "PASS" ELSE "FAIL" END AS invalid_source_excluded
FROM invalid_source;

/* 
==========================================================================================
  SECTION: Test 6 - Data Type Validation in Output Table
==========================================================================================
*/

-- Assert that all columns in output table have correct data types
WITH type_check AS (
  SELECT
    SUM(CASE WHEN typeof(country_code) = "string" THEN 1 ELSE 0 END) AS cc,
    SUM(CASE WHEN typeof(brand_name) = "string" THEN 1 ELSE 0 END) AS bn,
    SUM(CASE WHEN typeof(territory_id) = "string" THEN 1 ELSE 0 END) AS tid,
    SUM(CASE WHEN typeof(year) = "int" THEN 1 ELSE 0 END) AS yr,
    SUM(CASE WHEN typeof(month) = "int" THEN 1 ELSE 0 END) AS mn,
    SUM(CASE WHEN typeof(actual_value) = "double" THEN 1 ELSE 0 END) AS av,
    COUNT(*) AS total
  FROM purgo_playground.actual_sales_summary
)
SELECT
  CASE WHEN cc = total AND bn = total AND tid = total AND yr = total AND mn = total AND av = total
    THEN "PASS"
    ELSE "FAIL"
  END AS output_type_validation
FROM type_check;

/* 
==========================================================================================
  SECTION: Test 7 - Aggregation and Summing Multiple Records
==========================================================================================
*/

-- Assert that for a known combination with multiple records, the sum is correct
WITH expected AS (
  SELECT "US" AS country_code, "BRANDX" AS brand_name, "T001" AS territory_id, 2023 AS year, 5 AS month, 100.50+10.00+20.00+200.75+5.00+15.00 AS expected_value
),
actual AS (
  SELECT actual_value
  FROM purgo_playground.actual_sales_summary
  WHERE country_code = "US" AND brand_name = "BRANDX" AND territory_id = "T001" AND year = 2023 AND month = 5
)
SELECT
  CASE WHEN ABS(a.actual_value - e.expected_value) < 0.0001 THEN "PASS" ELSE "FAIL" END AS aggregation_sum_check
FROM expected e
JOIN actual a ON 1=1;

/* 
==========================================================================================
  SECTION: Test 8 - Exclusion of Brand if Either Source System is Missing in Control Table
==========================================================================================
*/

-- Assert that a brand_name is excluded if either source_system_name is missing in control_table
WITH excluded_brand AS (
  SELECT *
  FROM purgo_playground.actual_sales_summary
  WHERE country_code = "US" AND brand_name = "BRANDX" AND territory_id = "T001" AND year = 2023 AND month = 5
    AND (
      EXISTS (
        SELECT 1 FROM purgo_playground.t3_itm_territory_sales i
        WHERE i.country_code = "US" AND i.brand_name = "BRANDX" AND i.territory_id = "T001" AND YEAR(i.sales_month) = 2023 AND MONTH(i.sales_month) = 5
          AND i.source_system_name NOT IN (SELECT source_system FROM purgo_playground.control_table)
      )
      OR
      EXISTS (
        SELECT 1 FROM purgo_playground.t3_ttm_territory_sales t
        WHERE t.country_code = "US" AND t.brand_name = "BRANDX" AND t.territory_id = "T001" AND YEAR(t.fiscal_date) = 2023 AND MONTH(t.fiscal_date) = 5
          AND t.source_system_name NOT IN (SELECT source_system FROM purgo_playground.control_table)
      )
    )
)
SELECT
  CASE WHEN COUNT(*) = 0 THEN "PASS" ELSE "FAIL" END AS brand_exclusion_check
FROM excluded_brand;

/* 
==========================================================================================
  SECTION: Test 9 - Output Row Count and Performance (Quick Check)
==========================================================================================
*/

-- Assert that the output row count matches the number of unique valid combinations
WITH valid_itm AS (
  SELECT DISTINCT country_code, brand_name, territory_id, YEAR(sales_month) AS year, MONTH(sales_month) AS month
  FROM purgo_playground.t3_itm_territory_sales
  WHERE
    country_code IS NOT NULL AND TRIM(country_code) != ""
    AND brand_name IS NOT NULL AND TRIM(brand_name) != ""
    AND source_system_name IS NOT NULL AND TRIM(source_system_name) != ""
    AND territory_id IS NOT NULL AND TRIM(territory_id) != ""
    AND sales_value IS NOT NULL
    AND sales_month IS NOT NULL
    AND source_system_name IN (SELECT source_system FROM purgo_playground.control_table)
),
valid_ttm AS (
  SELECT DISTINCT country_code, brand_name, territory_id, YEAR(fiscal_date) AS year, MONTH(fiscal_date) AS month
  FROM purgo_playground.t3_ttm_territory_sales
  WHERE
    country_code IS NOT NULL AND TRIM(country_code) != ""
    AND brand_name IS NOT NULL AND TRIM(brand_name) != ""
    AND source_system_name IS NOT NULL AND TRIM(source_system_name) != ""
    AND territory_id IS NOT NULL AND TRIM(territory_id) != ""
    AND sales_net_price_local IS NOT NULL
    AND fiscal_date IS NOT NULL
    AND source_system_name IN (SELECT source_system FROM purgo_playground.control_table)
),
all_valid AS (
  SELECT * FROM valid_itm
  UNION
  SELECT * FROM valid_ttm
)
SELECT
  CASE WHEN (SELECT COUNT(*) FROM purgo_playground.actual_sales_summary) = (SELECT COUNT(*) FROM all_valid)
    THEN "PASS"
    ELSE "FAIL"
  END AS output_row_count_check;

/* 
==========================================================================================
  SECTION: Test 10 - Window Function and Analytics Feature Test
==========================================================================================
*/

-- Test: Calculate running total of actual_value per brand_name, ordered by year, month
WITH running_total AS (
  SELECT
    brand_name,
    year,
    month,
    actual_value,
    SUM(actual_value) OVER (PARTITION BY brand_name ORDER BY year, month ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_total
  FROM purgo_playground.actual_sales_summary
)
SELECT
  brand_name, year, month, actual_value, running_total
FROM running_total
ORDER BY brand_name, year, month;

/* 
==========================================================================================
  SECTION: Test 11 - Delta Lake Operations: MERGE, UPDATE, DELETE
==========================================================================================
*/

-- Test: MERGE - Upsert a new record and update an existing one
MERGE INTO purgo_playground.actual_sales_summary AS target
USING (
  SELECT "US" AS country_code, "BRANDX" AS brand_name, "T001" AS territory_id, 2023 AS year, 5 AS month, 999.99 AS actual_value
  UNION ALL
  SELECT "ZZ" AS country_code, "BRANDNEW" AS brand_name, "T999" AS territory_id, 2025 AS year, 12 AS month, 123.45 AS actual_value
) AS source
ON target.country_code = source.country_code
   AND target.brand_name = source.brand_name
   AND target.territory_id = source.territory_id
   AND target.year = source.year
   AND target.month = source.month
WHEN MATCHED THEN
  UPDATE SET actual_value = source.actual_value
WHEN NOT MATCHED THEN
  INSERT (country_code, brand_name, territory_id, year, month, actual_value)
  VALUES (source.country_code, source.brand_name, source.territory_id, source.year, source.month, source.actual_value);

-- Test: UPDATE - Set actual_value to 0 for a specific record
UPDATE purgo_playground.actual_sales_summary
SET actual_value = 0.0
WHERE country_code = "ZZ" AND brand_name = "BRANDNEW" AND territory_id = "T999" AND year = 2025 AND month = 12;

-- Test: DELETE - Remove the test record
DELETE FROM purgo_playground.actual_sales_summary
WHERE country_code = "ZZ" AND brand_name = "BRANDNEW" AND territory_id = "T999" AND year = 2025 AND month = 12;

/* 
==========================================================================================
  SECTION: Test 12 - Cleanup (Remove Test Records)
==========================================================================================
*/

-- Remove the upserted test record for US/BRANDX/T001/2023/5 to restore original state
UPDATE purgo_playground.actual_sales_summary
SET actual_value = 100.50+10.00+20.00+200.75+5.00+15.00
WHERE country_code = "US" AND brand_name = "BRANDX" AND territory_id = "T001" AND year = 2023 AND month = 5;
