/* =============================================================================
Databricks SQL Comprehensive Test Suite for Actual Value Calculation
===============================================================================
- This test code validates the calculation of actual_value for each brand_name, 
  country_code, territory_id, month, and year by summing sales_value from 
  purgo_playground.t3_itm_territory_sales and sales_net_price_local from 
  purgo_playground.t3_ttm_territory_sales.
- It covers: 
    * Data type and schema validation
    * NULL and invalid value handling
    * Data quality and aggregation
    * Delta Lake operations (MERGE/UPDATE/DELETE)
    * Window and analytics functions
    * Cleanup operations
    * Constraint and foreign key validation
    * End-to-end integration and performance checks
- All comments are block (/* */) for sections and line (--) for inline explanations.
- All SQL uses double quotes for string literals.
============================================================================= */

/*-----------------------------------------------------------------------------
SECTION: Setup - Clean up and recreate target table with constraints
-----------------------------------------------------------------------------*/
-- Drop and recreate the target table with NOT NULL and CHECK constraints
DROP TABLE IF EXISTS purgo_playground.actual_value_calculation;

CREATE TABLE purgo_playground.actual_value_calculation (
    country_code STRING NOT NULL,
    brand_name STRING NOT NULL,
    territory_id STRING NOT NULL,
    month STRING NOT NULL,
    year STRING NOT NULL,
    actual_value DOUBLE NOT NULL,
    CONSTRAINT month_range CHECK (CAST(month AS INT) BETWEEN 1 AND 12),
    CONSTRAINT year_format CHECK (LENGTH(year) = 4 AND year RLIKE "^[0-9]{4}$")
)
USING DELTA
;

/*-----------------------------------------------------------------------------
SECTION: Data Type and Schema Validation
-----------------------------------------------------------------------------*/
-- Validate that the schema matches the expected structure and constraints
DESCRIBE TABLE purgo_playground.actual_value_calculation;

-- Assert that all columns are NOT NULL and have correct types
SELECT 
    COUNT(*) AS invalid_schema_count
FROM (
    SELECT 
        column_name, 
        data_type, 
        is_nullable
    FROM information_schema.columns
    WHERE table_schema = "purgo_playground"
      AND table_name = "actual_value_calculation"
      AND (
            (column_name = "country_code" AND data_type != "STRING")
         OR (column_name = "brand_name" AND data_type != "STRING")
         OR (column_name = "territory_id" AND data_type != "STRING")
         OR (column_name = "month" AND data_type != "STRING")
         OR (column_name = "year" AND data_type != "STRING")
         OR (column_name = "actual_value" AND data_type != "DOUBLE")
         OR (is_nullable = "YES")
      )
) AS schema_violations
;
-- Assert: Should return 0 rows (no schema violations)

/*-----------------------------------------------------------------------------
SECTION: Main Calculation Query (Unit + Integration Test)
-----------------------------------------------------------------------------*/
-- Insert actual_value calculation into the target table
MERGE INTO purgo_playground.actual_value_calculation AS tgt
USING (
    WITH
    -- CTE: Valid ITM records
    itm_valid AS (
        SELECT
            country_code,
            brand_name,
            territory_id,
            CAST(MONTH(sales_month) AS STRING) AS month,
            CAST(YEAR(sales_month) AS STRING) AS year,
            sales_value
        FROM purgo_playground.t3_itm_territory_sales
        WHERE 
            country_code IS NOT NULL
            AND brand_name IS NOT NULL
            AND territory_id IS NOT NULL
            AND sales_month IS NOT NULL
            AND sales_value IS NOT NULL
            AND TRY_CAST(sales_value AS DOUBLE) IS NOT NULL
            AND source_system_name IS NOT NULL
            AND source_system_name IN (
                SELECT source_system FROM purgo_playground.control_table
            )
    ),
    -- CTE: Valid TTM records
    ttm_valid AS (
        SELECT
            country_code,
            brand_name,
            territory_id,
            CAST(MONTH(fiscal_date) AS STRING) AS month,
            CAST(YEAR(fiscal_date) AS STRING) AS year,
            sales_net_price_local
        FROM purgo_playground.t3_ttm_territory_sales
        WHERE 
            country_code IS NOT NULL
            AND brand_name IS NOT NULL
            AND territory_id IS NOT NULL
            AND fiscal_date IS NOT NULL
            AND sales_net_price_local IS NOT NULL
            AND TRY_CAST(sales_net_price_local AS DOUBLE) IS NOT NULL
            AND source_system_name IS NOT NULL
            AND source_system_name IN (
                SELECT source_system FROM purgo_playground.control_table
            )
    ),
    -- CTE: Union all valid keys from both tables
    all_keys AS (
        SELECT country_code, brand_name, territory_id, month, year FROM itm_valid
        UNION
        SELECT country_code, brand_name, territory_id, month, year FROM ttm_valid
    ),
    -- CTE: Aggregate sales_value from ITM
    agg_itm AS (
        SELECT
            country_code,
            brand_name,
            territory_id,
            month,
            year,
            SUM(sales_value) AS total_sales_value
        FROM itm_valid
        GROUP BY country_code, brand_name, territory_id, month, year
    ),
    -- CTE: Aggregate sales_net_price_local from TTM
    agg_ttm AS (
        SELECT
            country_code,
            brand_name,
            territory_id,
            month,
            year,
            SUM(sales_net_price_local) AS total_sales_net_price_local
        FROM ttm_valid
        GROUP BY country_code, brand_name, territory_id, month, year
    ),
    -- CTE: Final aggregation and join
    final_agg AS (
        SELECT
            k.country_code,
            k.brand_name,
            k.territory_id,
            k.month,
            k.year,
            COALESCE(i.total_sales_value, 0.0) + COALESCE(t.total_sales_net_price_local, 0.0) AS actual_value
        FROM all_keys k
        LEFT JOIN agg_itm i
            ON k.country_code = i.country_code
            AND k.brand_name = i.brand_name
            AND k.territory_id = i.territory_id
            AND k.month = i.month
            AND k.year = i.year
        LEFT JOIN agg_ttm t
            ON k.country_code = t.country_code
            AND k.brand_name = t.brand_name
            AND k.territory_id = t.territory_id
            AND k.month = t.month
            AND k.year = t.year
    )
    SELECT
        country_code,
        brand_name,
        territory_id,
        month,
        year,
        actual_value
    FROM final_agg
) AS src
ON tgt.country_code = src.country_code
   AND tgt.brand_name = src.brand_name
   AND tgt.territory_id = src.territory_id
   AND tgt.month = src.month
   AND tgt.year = src.year
WHEN MATCHED THEN UPDATE SET
    actual_value = src.actual_value
WHEN NOT MATCHED THEN INSERT (
    country_code, brand_name, territory_id, month, year, actual_value
) VALUES (
    src.country_code, src.brand_name, src.territory_id, src.month, src.year, src.actual_value
)
;

/*-----------------------------------------------------------------------------
SECTION: Data Quality Validation - Exclude Invalid/Null/Non-numeric Records
-----------------------------------------------------------------------------*/
-- Assert: No records with NULL in any NOT NULL column
SELECT COUNT(*) AS null_violation_count
FROM purgo_playground.actual_value_calculation
WHERE country_code IS NULL
   OR brand_name IS NULL
   OR territory_id IS NULL
   OR month IS NULL
   OR year IS NULL
   OR actual_value IS NULL
;

-- Assert: No records with month outside 1-12 or year not 4 digits
SELECT COUNT(*) AS constraint_violation_count
FROM purgo_playground.actual_value_calculation
WHERE CAST(month AS INT) NOT BETWEEN 1 AND 12
   OR LENGTH(year) != 4
   OR year NOT RLIKE "^[0-9]{4}$"
;

-- Assert: No records for source_system_name not in control_table
WITH
itm_invalid AS (
    SELECT country_code, brand_name, territory_id, CAST(MONTH(sales_month) AS STRING) AS month, CAST(YEAR(sales_month) AS STRING) AS year
    FROM purgo_playground.t3_itm_territory_sales
    WHERE source_system_name IS NOT NULL
      AND source_system_name NOT IN (SELECT source_system FROM purgo_playground.control_table)
),
ttm_invalid AS (
    SELECT country_code, brand_name, territory_id, CAST(MONTH(fiscal_date) AS STRING) AS month, CAST(YEAR(fiscal_date) AS STRING) AS year
    FROM purgo_playground.t3_ttm_territory_sales
    WHERE source_system_name IS NOT NULL
      AND source_system_name NOT IN (SELECT source_system FROM purgo_playground.control_table)
),
invalid_keys AS (
    SELECT * FROM itm_invalid
    UNION
    SELECT * FROM ttm_invalid
)
SELECT COUNT(*) AS invalid_source_system_count
FROM purgo_playground.actual_value_calculation a
JOIN invalid_keys k
  ON a.country_code = k.country_code
 AND a.brand_name = k.brand_name
 AND a.territory_id = k.territory_id
 AND a.month = k.month
 AND a.year = k.year
;

-- Assert: No records for keys where both sales_value and sales_net_price_local are NULL
WITH
itm_null AS (
    SELECT country_code, brand_name, territory_id, CAST(MONTH(sales_month) AS STRING) AS month, CAST(YEAR(sales_month) AS STRING) AS year
    FROM purgo_playground.t3_itm_territory_sales
    WHERE sales_value IS NULL
),
ttm_null AS (
    SELECT country_code, brand_name, territory_id, CAST(MONTH(fiscal_date) AS STRING) AS month, CAST(YEAR(fiscal_date) AS STRING) AS year
    FROM purgo_playground.t3_ttm_territory_sales
    WHERE sales_net_price_local IS NULL
),
null_keys AS (
    SELECT * FROM itm_null
    INTERSECT
    SELECT * FROM ttm_null
)
SELECT COUNT(*) AS null_key_count
FROM purgo_playground.actual_value_calculation a
JOIN null_keys k
  ON a.country_code = k.country_code
 AND a.brand_name = k.brand_name
 AND a.territory_id = k.territory_id
 AND a.month = k.month
 AND a.year = k.year
;

-- Assert: No records for keys with non-numeric sales_value or sales_net_price_local
WITH
itm_non_numeric AS (
    SELECT country_code, brand_name, territory_id, CAST(MONTH(sales_month) AS STRING) AS month, CAST(YEAR(sales_month) AS STRING) AS year
    FROM purgo_playground.t3_itm_territory_sales
    WHERE TRY_CAST(sales_value AS DOUBLE) IS NULL AND sales_value IS NOT NULL
),
ttm_non_numeric AS (
    SELECT country_code, brand_name, territory_id, CAST(MONTH(fiscal_date) AS STRING) AS month, CAST(YEAR(fiscal_date) AS STRING) AS year
    FROM purgo_playground.t3_ttm_territory_sales
    WHERE TRY_CAST(sales_net_price_local AS DOUBLE) IS NULL AND sales_net_price_local IS NOT NULL
),
non_numeric_keys AS (
    SELECT * FROM itm_non_numeric
    UNION
    SELECT * FROM ttm_non_numeric
)
SELECT COUNT(*) AS non_numeric_key_count
FROM purgo_playground.actual_value_calculation a
JOIN non_numeric_keys k
  ON a.country_code = k.country_code
 AND a.brand_name = k.brand_name
 AND a.territory_id = k.territory_id
 AND a.month = k.month
 AND a.year = k.year
;

/*-----------------------------------------------------------------------------
SECTION: Aggregation and Duplicate Handling Validation
-----------------------------------------------------------------------------*/
-- Assert: For a known duplicate key, actual_value is the sum of all matching sales_value and sales_net_price_local
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
;
-- Expected actual_value: sum of all sales_value and sales_net_price_local for this key

/*-----------------------------------------------------------------------------
SECTION: Window Function and Analytics Feature Test
-----------------------------------------------------------------------------*/
-- Use window function to rank brands by actual_value per country/month/year
SELECT
    country_code,
    month,
    year,
    brand_name,
    actual_value,
    RANK() OVER (PARTITION BY country_code, month, year ORDER BY actual_value DESC) AS brand_rank
FROM purgo_playground.actual_value_calculation
ORDER BY country_code, year, month, brand_rank
;

/*-----------------------------------------------------------------------------
SECTION: Delta Lake Operations - UPDATE, DELETE, MERGE
-----------------------------------------------------------------------------*/
-- UPDATE: Set actual_value to 0 for a specific test key and validate
UPDATE purgo_playground.actual_value_calculation
SET actual_value = 0.0
WHERE country_code = "DE"
  AND brand_name = "BRAND_B"
  AND territory_id = "T002"
  AND month = "12"
  AND year = "2022"
;

SELECT actual_value
FROM purgo_playground.actual_value_calculation
WHERE country_code = "DE"
  AND brand_name = "BRAND_B"
  AND territory_id = "T002"
  AND month = "12"
  AND year = "2022"
;

-- DELETE: Remove a specific test key and validate
DELETE FROM purgo_playground.actual_value_calculation
WHERE country_code = "FR"
  AND brand_name = "BRAND_C"
  AND territory_id = "T003"
  AND month = "1"
  AND year = "2024"
;

SELECT COUNT(*) AS deleted_count
FROM purgo_playground.actual_value_calculation
WHERE country_code = "FR"
  AND brand_name = "BRAND_C"
  AND territory_id = "T003"
  AND month = "1"
  AND year = "2024"
;

-- MERGE: Re-insert the deleted record for further tests
MERGE INTO purgo_playground.actual_value_calculation AS tgt
USING (
    SELECT "FR" AS country_code, "BRAND_C" AS brand_name, "T003" AS territory_id, "1" AS month, "2024" AS year, 300.0 AS actual_value
) AS src
ON tgt.country_code = src.country_code
   AND tgt.brand_name = src.brand_name
   AND tgt.territory_id = src.territory_id
   AND tgt.month = src.month
   AND tgt.year = src.year
WHEN NOT MATCHED THEN INSERT (
    country_code, brand_name, territory_id, month, year, actual_value
) VALUES (
    src.country_code, src.brand_name, src.territory_id, src.month, src.year, src.actual_value
)
;

/*-----------------------------------------------------------------------------
SECTION: Performance Test - Count and Timing
-----------------------------------------------------------------------------*/
-- Count total records for performance baseline
SELECT COUNT(*) AS total_actual_value_records
FROM purgo_playground.actual_value_calculation
;

-- Timing: Use Databricks SQL EXPLAIN to check query plan for main calculation
EXPLAIN
WITH
    itm_valid AS (
        SELECT
            country_code,
            brand_name,
            territory_id,
            CAST(MONTH(sales_month) AS STRING) AS month,
            CAST(YEAR(sales_month) AS STRING) AS year,
            sales_value
        FROM purgo_playground.t3_itm_territory_sales
        WHERE 
            country_code IS NOT NULL
            AND brand_name IS NOT NULL
            AND territory_id IS NOT NULL
            AND sales_month IS NOT NULL
            AND sales_value IS NOT NULL
            AND TRY_CAST(sales_value AS DOUBLE) IS NOT NULL
            AND source_system_name IS NOT NULL
            AND source_system_name IN (
                SELECT source_system FROM purgo_playground.control_table
            )
    ),
    ttm_valid AS (
        SELECT
            country_code,
            brand_name,
            territory_id,
            CAST(MONTH(fiscal_date) AS STRING) AS month,
            CAST(YEAR(fiscal_date) AS STRING) AS year,
            sales_net_price_local
        FROM purgo_playground.t3_ttm_territory_sales
        WHERE 
            country_code IS NOT NULL
            AND brand_name IS NOT NULL
            AND territory_id IS NOT NULL
            AND fiscal_date IS NOT NULL
            AND sales_net_price_local IS NOT NULL
            AND TRY_CAST(sales_net_price_local AS DOUBLE) IS NOT NULL
            AND source_system_name IS NOT NULL
            AND source_system_name IN (
                SELECT source_system FROM purgo_playground.control_table
            )
    ),
    all_keys AS (
        SELECT country_code, brand_name, territory_id, month, year FROM itm_valid
        UNION
        SELECT country_code, brand_name, territory_id, month, year FROM ttm_valid
    ),
    agg_itm AS (
        SELECT
            country_code,
            brand_name,
            territory_id,
            month,
            year,
            SUM(sales_value) AS total_sales_value
        FROM itm_valid
        GROUP BY country_code, brand_name, territory_id, month, year
    ),
    agg_ttm AS (
        SELECT
            country_code,
            brand_name,
            territory_id,
            month,
            year,
            SUM(sales_net_price_local) AS total_sales_net_price_local
        FROM ttm_valid
        GROUP BY country_code, brand_name, territory_id, month, year
    ),
    final_agg AS (
        SELECT
            k.country_code,
            k.brand_name,
            k.territory_id,
            k.month,
            k.year,
            COALESCE(i.total_sales_value, 0.0) + COALESCE(t.total_sales_net_price_local, 0.0) AS actual_value
        FROM all_keys k
        LEFT JOIN agg_itm i
            ON k.country_code = i.country_code
            AND k.brand_name = i.brand_name
            AND k.territory_id = i.territory_id
            AND k.month = i.month
            AND k.year = i.year
        LEFT JOIN agg_ttm t
            ON k.country_code = t.country_code
            AND k.brand_name = t.brand_name
            AND k.territory_id = t.territory_id
            AND k.month = t.month
            AND k.year = t.year
    )
SELECT
    country_code,
    brand_name,
    territory_id,
    month,
    year,
    actual_value
FROM final_agg
;

/*-----------------------------------------------------------------------------
SECTION: Foreign Key Validation
-----------------------------------------------------------------------------*/
-- Assert: All source_system_name in calculation exist in control_table
SELECT COUNT(*) AS fk_violation_count
FROM (
    SELECT DISTINCT source_system_name
    FROM purgo_playground.t3_itm_territory_sales
    WHERE source_system_name IS NOT NULL
      AND source_system_name NOT IN (SELECT source_system FROM purgo_playground.control_table)
    UNION
    SELECT DISTINCT source_system_name
    FROM purgo_playground.t3_ttm_territory_sales
    WHERE source_system_name IS NOT NULL
      AND source_system_name NOT IN (SELECT source_system FROM purgo_playground.control_table)
) AS fk_violations
;

/*-----------------------------------------------------------------------------
SECTION: Cleanup - Remove test records (if needed)
-----------------------------------------------------------------------------*/
-- Optionally, clean up test records from actual_value_calculation
-- DELETE FROM purgo_playground.actual_value_calculation WHERE year = "2023" AND month = "5";
