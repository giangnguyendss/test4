/* 
==========================================================================================
Databricks SQL Test Suite for purgo_playground.actual_value_calculation
==========================================================================================
- This test suite validates the calculation of actual_value for each brand, country, territory, month, and year
  by summing sales_value from t3_itm_territory_sales and sales_net_price_local from t3_ttm_territory_sales.
- It covers:
    * Data type handling (including NULLs and non-numeric)
    * Schema validation
    * Data quality and aggregation
    * Filtering by control_table
    * Output formatting and constraints
    * Edge and error scenarios
- All assertions are implemented as SQL queries with comments.
- All CTEs are used directly in validation queries.
- All table and column references are fully qualified.
- All constraints and checks are explicitly validated.
==========================================================================================
*/

/*------------------------------------------------------------------------------
SECTION: 1. SCHEMA VALIDATION FOR OUTPUT TABLE
------------------------------------------------------------------------------*/
-- Validate that the output table has the correct schema and constraints
SELECT
  COUNT(*) AS invalid_schema_count
FROM (
  SELECT
    -- Check NOT NULL constraints
    CASE WHEN country_code IS NULL THEN 1 ELSE 0 END AS country_code_null,
    CASE WHEN brand_name IS NULL THEN 1 ELSE 0 END AS brand_name_null,
    CASE WHEN territory_id IS NULL THEN 1 ELSE 0 END AS territory_id_null,
    CASE WHEN month IS NULL THEN 1 ELSE 0 END AS month_null,
    CASE WHEN year IS NULL THEN 1 ELSE 0 END AS year_null,
    CASE WHEN actual_value IS NULL THEN 1 ELSE 0 END AS actual_value_null,
    -- Check data types (STRING for all except actual_value)
    CASE WHEN typeof(country_code) != "STRING" THEN 1 ELSE 0 END AS country_code_type,
    CASE WHEN typeof(brand_name) != "STRING" THEN 1 ELSE 0 END AS brand_name_type,
    CASE WHEN typeof(territory_id) != "STRING" THEN 1 ELSE 0 END AS territory_id_type,
    CASE WHEN typeof(month) != "STRING" THEN 1 ELSE 0 END AS month_type,
    CASE WHEN typeof(year) != "STRING" THEN 1 ELSE 0 END AS year_type,
    CASE WHEN typeof(actual_value) != "DOUBLE" THEN 1 ELSE 0 END AS actual_value_type
  FROM purgo_playground.actual_value_calculation
) t
WHERE
  country_code_null = 1 OR brand_name_null = 1 OR territory_id_null = 1
  OR month_null = 1 OR year_null = 1 OR actual_value_null = 1
  OR country_code_type = 1 OR brand_name_type = 1 OR territory_id_type = 1
  OR month_type = 1 OR year_type = 1 OR actual_value_type = 1;

-- Assertion: invalid_schema_count must be 0

/*------------------------------------------------------------------------------
SECTION: 2. HAPPY PATH - BOTH TABLES HAVE DATA
------------------------------------------------------------------------------*/
-- Validate correct calculation for US/BRANDX/TID001/2023-05
WITH expected AS (
  SELECT "US" AS country_code, "BRANDX" AS brand_name, "TID001" AS territory_id, "05" AS month, "2023" AS year, 300.0 AS actual_value
)
SELECT
  COUNT(*) AS match_count
FROM purgo_playground.actual_value_calculation a
JOIN expected e
  ON a.country_code = e.country_code
  AND a.brand_name = e.brand_name
  AND a.territory_id = e.territory_id
  AND a.month = e.month
  AND a.year = e.year
  AND ABS(a.actual_value - e.actual_value) < 0.0001;

-- Assertion: match_count = 1

-- Validate correct calculation for DE/BRANDY/TID002/2022-12
WITH expected AS (
  SELECT "DE" AS country_code, "BRANDY" AS brand_name, "TID002" AS territory_id, "12" AS month, "2022" AS year, 125.0 AS actual_value
)
SELECT
  COUNT(*) AS match_count
FROM purgo_playground.actual_value_calculation a
JOIN expected e
  ON a.country_code = e.country_code
  AND a.brand_name = e.brand_name
  AND a.territory_id = e.territory_id
  AND a.month = e.month
  AND a.year = e.year
  AND ABS(a.actual_value - e.actual_value) < 0.0001;

-- Assertion: match_count = 1

/*------------------------------------------------------------------------------
SECTION: 3. ONLY ONE SOURCE TABLE HAS DATA
------------------------------------------------------------------------------*/
-- ITM only: US/BRANDZ/TID003/2023-06
WITH expected AS (
  SELECT "US" AS country_code, "BRANDZ" AS brand_name, "TID003" AS territory_id, "06" AS month, "2023" AS year, 150.0 AS actual_value
)
SELECT
  COUNT(*) AS match_count
FROM purgo_playground.actual_value_calculation a
JOIN expected e
  ON a.country_code = e.country_code
  AND a.brand_name = e.brand_name
  AND a.territory_id = e.territory_id
  AND a.month = e.month
  AND a.year = e.year
  AND ABS(a.actual_value - e.actual_value) < 0.0001;

-- Assertion: match_count = 1

-- TTM only: FR/BRANDW/TID004/2023-07
WITH expected AS (
  SELECT "FR" AS country_code, "BRANDW" AS brand_name, "TID004" AS territory_id, "07" AS month, "2023" AS year, 80.0 AS actual_value
)
SELECT
  COUNT(*) AS match_count
FROM purgo_playground.actual_value_calculation a
JOIN expected e
  ON a.country_code = e.country_code
  AND a.brand_name = e.brand_name
  AND a.territory_id = e.territory_id
  AND a.month = e.month
  AND a.year = e.year
  AND ABS(a.actual_value - e.actual_value) < 0.0001;

-- Assertion: match_count = 1

/*------------------------------------------------------------------------------
SECTION: 4. EXCLUDE RECORDS WHERE SOURCE_SYSTEM_NAME NOT IN CONTROL_TABLE
------------------------------------------------------------------------------*/
-- IT/BRANDQ/TID005/2023-08 should NOT exist
SELECT
  COUNT(*) AS excluded_count
FROM purgo_playground.actual_value_calculation
WHERE country_code = "IT" AND brand_name = "BRANDQ" AND territory_id = "TID005" AND month = "08" AND year = "2023";

-- Assertion: excluded_count = 0

/*------------------------------------------------------------------------------
SECTION: 5. NULL SALES VALUES TREATED AS ZERO
------------------------------------------------------------------------------*/
-- ES/BRANDN/TID006/2023-09: ITM NULL, TTM 90.0 => 90.0
WITH expected AS (
  SELECT "ES" AS country_code, "BRANDN" AS brand_name, "TID006" AS territory_id, "09" AS month, "2023" AS year, 90.0 AS actual_value
)
SELECT
  COUNT(*) AS match_count
FROM purgo_playground.actual_value_calculation a
JOIN expected e
  ON a.country_code = e.country_code
  AND a.brand_name = e.brand_name
  AND a.territory_id = e.territory_id
  AND a.month = e.month
  AND a.year = e.year
  AND ABS(a.actual_value - e.actual_value) < 0.0001;

-- UK/BRANDO/TID007/2023-10: ITM 110.0, TTM NULL => 110.0
WITH expected AS (
  SELECT "UK" AS country_code, "BRANDO" AS brand_name, "TID007" AS territory_id, "10" AS month, "2023" AS year, 110.0 AS actual_value
)
SELECT
  COUNT(*) AS match_count
FROM purgo_playground.actual_value_calculation a
JOIN expected e
  ON a.country_code = e.country_code
  AND a.brand_name = e.brand_name
  AND a.territory_id = e.territory_id
  AND a.month = e.month
  AND a.year = e.year
  AND ABS(a.actual_value - e.actual_value) < 0.0001;

-- NL/BRANDP/TID008/2023-11: both NULL => 0.0 (should NOT exist)
SELECT
  COUNT(*) AS excluded_count
FROM purgo_playground.actual_value_calculation
WHERE country_code = "NL" AND brand_name = "BRANDP" AND territory_id = "TID008" AND month = "11" AND year = "2023";

-- Assertion: excluded_count = 0

/*------------------------------------------------------------------------------
SECTION: 6. OUTPUT MONTH AND YEAR FORMATTING
------------------------------------------------------------------------------*/
-- US/BRANDM/TID009/2023-01: month = "01", year = "2023"
WITH expected AS (
  SELECT "US" AS country_code, "BRANDM" AS brand_name, "TID009" AS territory_id, "01" AS month, "2023" AS year, 500.0 AS actual_value
)
SELECT
  COUNT(*) AS match_count
FROM purgo_playground.actual_value_calculation a
JOIN expected e
  ON a.country_code = e.country_code
  AND a.brand_name = e.brand_name
  AND a.territory_id = e.territory_id
  AND a.month = e.month
  AND a.year = e.year
  AND ABS(a.actual_value - e.actual_value) < 0.0001;

-- Assertion: match_count = 1

/*------------------------------------------------------------------------------
SECTION: 7. DUPLICATE RECORDS AGGREGATED BY SUM
------------------------------------------------------------------------------*/
-- US/BRANDD/TID010/2023-02: ITM 50+70, TTM 30+20 => 170.0
WITH expected AS (
  SELECT "US" AS country_code, "BRANDD" AS brand_name, "TID010" AS territory_id, "02" AS month, "2023" AS year, 170.0 AS actual_value
)
SELECT
  COUNT(*) AS match_count
FROM purgo_playground.actual_value_calculation a
JOIN expected e
  ON a.country_code = e.country_code
  AND a.brand_name = e.brand_name
  AND a.territory_id = e.territory_id
  AND a.month = e.month
  AND a.year = e.year
  AND ABS(a.actual_value - e.actual_value) < 0.0001;

-- Assertion: match_count = 1

/*------------------------------------------------------------------------------
SECTION: 8. NON-NUMERIC SALES VALUES TREATED AS ZERO
------------------------------------------------------------------------------*/
-- US/BRANDE/TID011/2023-03: ITM 'abc' (0), TTM 100.0 => 100.0
WITH expected AS (
  SELECT "US" AS country_code, "BRANDE" AS brand_name, "TID011" AS territory_id, "03" AS month, "2023" AS year, 100.0 AS actual_value
)
SELECT
  COUNT(*) AS match_count
FROM purgo_playground.actual_value_calculation a
JOIN expected e
  ON a.country_code = e.country_code
  AND a.brand_name = e.brand_name
  AND a.territory_id = e.territory_id
  AND a.month = e.month
  AND a.year = e.year
  AND ABS(a.actual_value - e.actual_value) < 0.0001;

-- US/BRANDF/TID012/2023-04: ITM 200.0, TTM 'xyz' (0) => 200.0
WITH expected AS (
  SELECT "US" AS country_code, "BRANDF" AS brand_name, "TID012" AS territory_id, "04" AS month, "2023" AS year, 200.0 AS actual_value
)
SELECT
  COUNT(*) AS match_count
FROM purgo_playground.actual_value_calculation a
JOIN expected e
  ON a.country_code = e.country_code
  AND a.brand_name = e.brand_name
  AND a.territory_id = e.territory_id
  AND a.month = e.month
  AND a.year = e.year
  AND ABS(a.actual_value - e.actual_value) < 0.0001;

-- US/BRANDG/TID013/2023-05: both non-numeric => 0.0 (should NOT exist)
SELECT
  COUNT(*) AS excluded_count
FROM purgo_playground.actual_value_calculation
WHERE country_code = "US" AND brand_name = "BRANDG" AND territory_id = "TID013" AND month = "05" AND year = "2023";

-- Assertion: excluded_count = 0

/*------------------------------------------------------------------------------
SECTION: 9. OUTPUT FIELDS MUST NOT BE NULL
------------------------------------------------------------------------------*/
-- US/BRANDH/TID014/2023-06: all fields not null
SELECT
  COUNT(*) AS null_field_count
FROM purgo_playground.actual_value_calculation
WHERE (country_code = "US" AND brand_name = "BRANDH" AND territory_id = "TID014" AND month = "06" AND year = "2023")
  AND (country_code IS NULL OR brand_name IS NULL OR territory_id IS NULL OR month IS NULL OR year IS NULL OR actual_value IS NULL);

-- Assertion: null_field_count = 0

/*------------------------------------------------------------------------------
SECTION: 10. NO OUTPUT IF BOTH TABLES HAVE NO DATA FOR A COMBINATION
------------------------------------------------------------------------------*/
-- US/BRANDI/TID015/2023-07: should NOT exist
SELECT
  COUNT(*) AS excluded_count
FROM purgo_playground.actual_value_calculation
WHERE country_code = "US" AND brand_name = "BRANDI" AND territory_id = "TID015" AND month = "07" AND year = "2023";

-- Assertion: excluded_count = 0

/*------------------------------------------------------------------------------
SECTION: 11. DATA QUALITY - SPECIAL CHARACTERS AND MULTI-BYTE
------------------------------------------------------------------------------*/
-- JP/ＢＲＡＮＤ特/ＴＩＤ016/2023-12: 123.45 + 321.54 = 444.99
WITH expected AS (
  SELECT "JP" AS country_code, "ＢＲＡＮＤ特" AS brand_name, "TID016" AS territory_id, "12" AS month, "2023" AS year, 444.99 AS actual_value
)
SELECT
  COUNT(*) AS match_count
FROM purgo_playground.actual_value_calculation a
JOIN expected e
  ON a.country_code = e.country_code
  AND a.brand_name = e.brand_name
  AND a.territory_id = e.territory_id
  AND a.month = e.month
  AND a.year = e.year
  AND ABS(a.actual_value - e.actual_value) < 0.0001;

-- CN/品牌名/TID017/2023-12: 234.56 + 432.65 = 667.21
WITH expected AS (
  SELECT "CN" AS country_code, "品牌名" AS brand_name, "TID017" AS territory_id, "12" AS month, "2023" AS year, 667.21 AS actual_value
)
SELECT
  COUNT(*) AS match_count
FROM purgo_playground.actual_value_calculation a
JOIN expected e
  ON a.country_code = e.country_code
  AND a.brand_name = e.brand_name
  AND a.territory_id = e.territory_id
  AND a.month = e.month
  AND a.year = e.year
  AND ABS(a.actual_value - e.actual_value) < 0.0001;

-- KR/브랜드/TID018/2023-12: 345.67 + 543.76 = 889.43
WITH expected AS (
  SELECT "KR" AS country_code, "브랜드" AS brand_name, "TID018" AS territory_id, "12" AS month, "2023" AS year, 889.43 AS actual_value
)
SELECT
  COUNT(*) AS match_count
FROM purgo_playground.actual_value_calculation a
JOIN expected e
  ON a.country_code = e.country_code
  AND a.brand_name = e.brand_name
  AND a.territory_id = e.territory_id
  AND a.month = e.month
  AND a.year = e.year
  AND ABS(a.actual_value - e.actual_value) < 0.0001;

-- US/BRAND!@#/TID019/2023-12: 456.78 + 654.87 = 1111.65
WITH expected AS (
  SELECT "US" AS country_code, "BRAND!@#" AS brand_name, "TID019" AS territory_id, "12" AS month, "2023" AS year, 1111.65 AS actual_value
)
SELECT
  COUNT(*) AS match_count
FROM purgo_playground.actual_value_calculation a
JOIN expected e
  ON a.country_code = e.country_code
  AND a.brand_name = e.brand_name
  AND a.territory_id = e.territory_id
  AND a.month = e.month
  AND a.year = e.year
  AND ABS(a.actual_value - e.actual_value) < 0.0001;

-- US/BRAND SPACE/TID020/2023-12: 567.89 + 765.98 = 1333.87
WITH expected AS (
  SELECT "US" AS country_code, "BRAND SPACE" AS brand_name, "TID020" AS territory_id, "12" AS month, "2023" AS year, 1333.87 AS actual_value
)
SELECT
  COUNT(*) AS match_count
FROM purgo_playground.actual_value_calculation a
JOIN expected e
  ON a.country_code = e.country_code
  AND a.brand_name = e.brand_name
  AND a.territory_id = e.territory_id
  AND a.month = e.month
  AND a.year = e.year
  AND ABS(a.actual_value - e.actual_value) < 0.0001;

-- US/BRAND-UNDER/TID021/2023-12: 678.90 + 876.09 = 1554.99
WITH expected AS (
  SELECT "US" AS country_code, "BRAND-UNDER" AS brand_name, "TID021" AS territory_id, "12" AS month, "2023" AS year, 1554.99 AS actual_value
)
SELECT
  COUNT(*) AS match_count
FROM purgo_playground.actual_value_calculation a
JOIN expected e
  ON a.country_code = e.country_code
  AND a.brand_name = e.brand_name
  AND a.territory_id = e.territory_id
  AND a.month = e.month
  AND a.year = e.year
  AND ABS(a.actual_value - e.actual_value) < 0.0001;

-- US/BRAND_UNDER/TID022/2023-12: 789.01 + 987.10 = 1776.11
WITH expected AS (
  SELECT "US" AS country_code, "BRAND_UNDER" AS brand_name, "TID022" AS territory_id, "12" AS month, "2023" AS year, 1776.11 AS actual_value
)
SELECT
  COUNT(*) AS match_count
FROM purgo_playground.actual_value_calculation a
JOIN expected e
  ON a.country_code = e.country_code
  AND a.brand_name = e.brand_name
  AND a.territory_id = e.territory_id
  AND a.month = e.month
  AND a.year = e.year
  AND ABS(a.actual_value - e.actual_value) < 0.0001;

-- Assertion: match_count = 1 for each

/*------------------------------------------------------------------------------
SECTION: 12. PERFORMANCE TEST - COUNT OF OUTPUT RECORDS
------------------------------------------------------------------------------*/
-- Validate that the number of output records matches the number of expected unique (country_code, brand_name, territory_id, month, year) combinations
WITH expected_keys AS (
  SELECT "US" AS country_code, "BRANDX" AS brand_name, "TID001" AS territory_id, "05" AS month, "2023" AS year UNION ALL
  SELECT "DE", "BRANDY", "TID002", "12", "2022" UNION ALL
  SELECT "US", "BRANDZ", "TID003", "06", "2023" UNION ALL
  SELECT "FR", "BRANDW", "TID004", "07", "2023" UNION ALL
  SELECT "ES", "BRANDN", "TID006", "09", "2023" UNION ALL
  SELECT "UK", "BRANDO", "TID007", "10", "2023" UNION ALL
  SELECT "US", "BRANDM", "TID009", "01", "2023" UNION ALL
  SELECT "US", "BRANDD", "TID010", "02", "2023" UNION ALL
  SELECT "US", "BRANDE", "TID011", "03", "2023" UNION ALL
  SELECT "US", "BRANDF", "TID012", "04", "2023" UNION ALL
  SELECT "US", "BRANDH", "TID014", "06", "2023" UNION ALL
  SELECT "JP", "ＢＲＡＮＤ特", "TID016", "12", "2023" UNION ALL
  SELECT "CN", "品牌名", "TID017", "12", "2023" UNION ALL
  SELECT "KR", "브랜드", "TID018", "12", "2023" UNION ALL
  SELECT "US", "BRAND!@#", "TID019", "12", "2023" UNION ALL
  SELECT "US", "BRAND SPACE", "TID020", "12", "2023" UNION ALL
  SELECT "US", "BRAND-UNDER", "TID021", "12", "2023" UNION ALL
  SELECT "US", "BRAND_UNDER", "TID022", "12", "2023"
)
SELECT
  (SELECT COUNT(*) FROM purgo_playground.actual_value_calculation) AS actual_count,
  (SELECT COUNT(*) FROM expected_keys) AS expected_count;

-- Assertion: actual_count = expected_count

/*------------------------------------------------------------------------------
SECTION: 13. DELTA LAKE OPERATIONS (IF TABLE IS DELTA)
------------------------------------------------------------------------------*/
-- Validate that the table is a Delta table and supports MERGE, UPDATE, DELETE
DESCRIBE DETAIL purgo_playground.actual_value_calculation;

-- Assertion: format = "delta"

-- Test DELETE operation (should succeed and affect rows)
DELETE FROM purgo_playground.actual_value_calculation WHERE country_code = "US" AND brand_name = "BRANDX" AND territory_id = "TID001" AND month = "05" AND year = "2023";

-- Assertion: 1 row deleted

-- Test UPDATE operation (should succeed)
UPDATE purgo_playground.actual_value_calculation
SET actual_value = 999.99
WHERE country_code = "DE" AND brand_name = "BRANDY" AND territory_id = "TID002" AND month = "12" AND year = "2022";

-- Assertion: 1 row updated

-- Test MERGE operation (upsert)
MERGE INTO purgo_playground.actual_value_calculation AS target
USING (SELECT "US" AS country_code, "BRANDX" AS brand_name, "TID001" AS territory_id, "05" AS month, "2023" AS year, 300.0 AS actual_value) AS source
ON target.country_code = source.country_code AND target.brand_name = source.brand_name AND target.territory_id = source.territory_id AND target.month = source.month AND target.year = source.year
WHEN MATCHED THEN UPDATE SET actual_value = source.actual_value
WHEN NOT MATCHED THEN INSERT (country_code, brand_name, territory_id, month, year, actual_value) VALUES (source.country_code, source.brand_name, source.territory_id, source.month, source.year, source.actual_value);

-- Assertion: row inserted or updated

/*------------------------------------------------------------------------------
SECTION: 14. CLEANUP OPERATIONS
------------------------------------------------------------------------------*/
-- Clean up test changes (restore deleted/updated records)
DELETE FROM purgo_playground.actual_value_calculation WHERE (country_code = "DE" AND brand_name = "BRANDY" AND territory_id = "TID002" AND month = "12" AND year = "2022") OR (country_code = "US" AND brand_name = "BRANDX" AND territory_id = "TID001" AND month = "05" AND year = "2023");

INSERT INTO purgo_playground.actual_value_calculation (country_code, brand_name, territory_id, month, year, actual_value) VALUES
  ("DE", "BRANDY", "TID002", "12", "2022", 125.0),
  ("US", "BRANDX", "TID001", "05", "2023", 300.0);

-- End of test suite
