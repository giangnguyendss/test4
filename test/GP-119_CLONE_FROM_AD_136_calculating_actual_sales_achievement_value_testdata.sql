-- Test Data Generation for purgo_playground.actual_value_calculation scenario
-- This script generates comprehensive test data for t3_itm_territory_sales, t3_ttm_territory_sales, and control_table
-- covering happy path, edge, error, null, special character, and data type scenarios.

-- 1. CONTROL TABLE TEST DATA
-- Covers valid, missing, and special character source_systems
CREATE OR REPLACE TABLE purgo_playground.control_table (
  country_code STRING,
  source_system STRING,
  brand_name STRING
);

INSERT INTO purgo_playground.control_table (country_code, source_system, brand_name) VALUES
  -- Happy path: valid source systems
  ('US', 'SYS1', 'BRANDX'),
  ('US', 'SYS2', 'BRANDX'),
  ('DE', 'SYS3', 'BRANDY'),
  ('FR', 'SYS5', 'BRANDW'),
  ('US', 'SYS4', 'BRANDZ'),
  ('ES', 'SYS8', 'BRANDN'),
  ('ES', 'SYS9', 'BRANDN'),
  ('UK', 'SYS10', 'BRANDO'),
  ('UK', 'SYS11', 'BRANDO'),
  ('NL', 'SYS12', 'BRANDP'),
  ('NL', 'SYS13', 'BRANDP'),
  ('US', 'SYS14', 'BRANDM'),
  ('US', 'SYS15', 'BRANDM'),
  ('US', 'SYS16', 'BRANDD'),
  ('US', 'SYS17', 'BRANDD'),
  ('US', 'SYS18', 'BRANDE'),
  ('US', 'SYS19', 'BRANDE'),
  ('US', 'SYS20', 'BRANDF'),
  ('US', 'SYS21', 'BRANDF'),
  ('US', 'SYS22', 'BRANDG'),
  ('US', 'SYS23', 'BRANDG'),
  ('US', 'SYS24', 'BRANDH'),
  ('US', 'SYS25', 'BRANDH'),
  -- Special characters and multi-byte
  ('JP', 'ＳＹＳ26', 'ＢＲＡＮＤ特'), -- full-width chars
  ('CN', 'SYS27', '品牌名'),           -- Chinese chars
  ('KR', 'SYS28', '브랜드'),           -- Korean chars
  ('US', 'SYS29', 'BRAND!@#'),        -- special chars
  ('US', 'SYS30', 'BRAND SPACE'),     -- space in brand
  ('US', 'SYS31', 'BRAND-UNDER'),     -- hyphen
  ('US', 'SYS32', 'BRAND_UNDER');     -- underscore

-- 2. t3_itm_territory_sales TEST DATA
CREATE OR REPLACE TABLE purgo_playground.t3_itm_territory_sales (
  country_code STRING,
  brand_name STRING,
  source_system_name STRING,
  territory_id STRING,
  sales_value DOUBLE,
  sales_month DATE
);

INSERT INTO purgo_playground.t3_itm_territory_sales (country_code, brand_name, source_system_name, territory_id, sales_value, sales_month) VALUES
  -- Happy path: both tables have data
  ('US', 'BRANDX', 'SYS1', 'TID001', 100.0, DATE'2023-05-01'),
  ('DE', 'BRANDY', 'SYS3', 'TID002', 50.0, DATE'2022-12-01'),
  -- Only ITM has data
  ('US', 'BRANDZ', 'SYS4', 'TID003', 150.0, DATE'2023-06-01'),
  -- Only TTM has data (no ITM record for FR/BRANDW/TID004/2023-07)
  -- Exclude: source_system not in control_table
  ('IT', 'BRANDQ', 'SYS6', 'TID005', 120.0, DATE'2023-08-01'),
  -- Null sales_value
  ('ES', 'BRANDN', 'SYS8', 'TID006', NULL, DATE'2023-09-01'),
  -- Null sales_net_price_local in TTM, so ITM only
  ('UK', 'BRANDO', 'SYS10', 'TID007', 110.0, DATE'2023-10-01'),
  -- Both null
  ('NL', 'BRANDP', 'SYS12', 'TID008', NULL, DATE'2023-11-01'),
  -- Month/year formatting
  ('US', 'BRANDM', 'SYS14', 'TID009', 200.0, DATE'2023-01-05'),
  -- Duplicate records (to test aggregation)
  ('US', 'BRANDD', 'SYS16', 'TID010', 50.0, DATE'2023-02-01'),
  ('US', 'BRANDD', 'SYS16', 'TID010', 70.0, DATE'2023-02-01'),
  -- Error: non-numeric sales_value (should be treated as 0)
  ('US', 'BRANDE', 'SYS18', 'TID011', CAST('abc' AS DOUBLE), DATE'2023-03-01'),
  ('US', 'BRANDF', 'SYS20', 'TID012', 200.0, DATE'2023-04-01'),
  ('US', 'BRANDG', 'SYS22', 'TID013', CAST('abc' AS DOUBLE), DATE'2023-05-01'),
  -- Not null output fields
  ('US', 'BRANDH', 'SYS24', 'TID014', 100.0, DATE'2023-06-01'),
  -- Special characters and multi-byte
  ('JP', 'ＢＲＡＮＤ特', 'ＳＹＳ26', 'TID016', 123.45, DATE'2023-12-01'),
  ('CN', '品牌名', 'SYS27', 'TID017', 234.56, DATE'2023-12-01'),
  ('KR', '브랜드', 'SYS28', 'TID018', 345.67, DATE'2023-12-01'),
  ('US', 'BRAND!@#', 'SYS29', 'TID019', 456.78, DATE'2023-12-01'),
  ('US', 'BRAND SPACE', 'SYS30', 'TID020', 567.89, DATE'2023-12-01'),
  ('US', 'BRAND-UNDER', 'SYS31', 'TID021', 678.90, DATE'2023-12-01'),
  ('US', 'BRAND_UNDER', 'SYS32', 'TID022', 789.01, DATE'2023-12-01'),
  -- Edge: NULLs in all keys (should not appear in output)
  (NULL, NULL, NULL, NULL, NULL, NULL);

-- 3. t3_ttm_territory_sales TEST DATA
CREATE OR REPLACE TABLE purgo_playground.t3_ttm_territory_sales (
  country_code STRING,
  brand_name STRING,
  source_system_name STRING,
  territory_id STRING,
  sales_net_price_local DOUBLE,
  fiscal_date DATE
);

INSERT INTO purgo_playground.t3_ttm_territory_sales (country_code, brand_name, source_system_name, territory_id, sales_net_price_local, fiscal_date) VALUES
  -- Happy path: both tables have data
  ('US', 'BRANDX', 'SYS2', 'TID001', 200.0, DATE'2023-05-15'),
  ('DE', 'BRANDY', 'SYS3', 'TID002', 75.0, DATE'2022-12-20'),
  -- Only TTM has data
  ('FR', 'BRANDW', 'SYS5', 'TID004', 80.0, DATE'2023-07-10'),
  -- Exclude: source_system not in control_table
  ('IT', 'BRANDQ', 'SYS7', 'TID005', 130.0, DATE'2023-08-15'),
  -- Null sales_net_price_local
  ('UK', 'BRANDO', 'SYS11', 'TID007', NULL, DATE'2023-10-20'),
  -- Null sales_value in ITM, so TTM only
  ('ES', 'BRANDN', 'SYS9', 'TID006', 90.0, DATE'2023-09-10'),
  -- Both null
  ('NL', 'BRANDP', 'SYS13', 'TID008', NULL, DATE'2023-11-15'),
  -- Month/year formatting
  ('US', 'BRANDM', 'SYS15', 'TID009', 300.0, DATE'2023-01-25'),
  -- Duplicate records (to test aggregation)
  ('US', 'BRANDD', 'SYS17', 'TID010', 30.0, DATE'2023-02-15'),
  ('US', 'BRANDD', 'SYS17', 'TID010', 20.0, DATE'2023-02-15'),
  -- Error: non-numeric sales_net_price_local (should be treated as 0)
  ('US', 'BRANDE', 'SYS19', 'TID011', 100.0, DATE'2023-03-10'),
  ('US', 'BRANDF', 'SYS21', 'TID012', CAST('xyz' AS DOUBLE), DATE'2023-04-10'),
  ('US', 'BRANDG', 'SYS23', 'TID013', CAST('xyz' AS DOUBLE), DATE'2023-05-10'),
  -- Not null output fields
  ('US', 'BRANDH', 'SYS25', 'TID014', 200.0, DATE'2023-06-15'),
  -- Special characters and multi-byte
  ('JP', 'ＢＲＡＮＤ特', 'ＳＹＳ26', 'TID016', 321.54, DATE'2023-12-15'),
  ('CN', '品牌名', 'SYS27', 'TID017', 432.65, DATE'2023-12-15'),
  ('KR', '브랜드', 'SYS28', 'TID018', 543.76, DATE'2023-12-15'),
  ('US', 'BRAND!@#', 'SYS29', 'TID019', 654.87, DATE'2023-12-15'),
  ('US', 'BRAND SPACE', 'SYS30', 'TID020', 765.98, DATE'2023-12-15'),
  ('US', 'BRAND-UNDER', 'SYS31', 'TID021', 876.09, DATE'2023-12-15'),
  ('US', 'BRAND_UNDER', 'SYS32', 'TID022', 987.10, DATE'2023-12-15'),
  -- Edge: NULLs in all keys (should not appear in output)
  (NULL, NULL, NULL, NULL, NULL, NULL);

-- 4. VALIDATION QUERY: CTEs for test data
WITH
  itm AS (
    SELECT
      country_code,
      brand_name,
      source_system_name,
      territory_id,
      -- Handle non-numeric and null sales_value as 0
      CASE WHEN TRY_CAST(sales_value AS DOUBLE) IS NULL THEN 0.0 ELSE sales_value END AS sales_value,
      sales_month
    FROM purgo_playground.t3_itm_territory_sales
    WHERE country_code IS NOT NULL AND brand_name IS NOT NULL AND territory_id IS NOT NULL AND sales_month IS NOT NULL
  ),
  ttm AS (
    SELECT
      country_code,
      brand_name,
      source_system_name,
      territory_id,
      -- Handle non-numeric and null sales_net_price_local as 0
      CASE WHEN TRY_CAST(sales_net_price_local AS DOUBLE) IS NULL THEN 0.0 ELSE sales_net_price_local END AS sales_net_price_local,
      fiscal_date
    FROM purgo_playground.t3_ttm_territory_sales
    WHERE country_code IS NOT NULL AND brand_name IS NOT NULL AND territory_id IS NOT NULL AND fiscal_date IS NOT NULL
  ),
  -- All valid source_systems for filtering
  valid_sources AS (
    SELECT DISTINCT source_system FROM purgo_playground.control_table
  ),
  -- All valid combinations from ITM
  itm_keys AS (
    SELECT
      country_code,
      brand_name,
      territory_id,
      LPAD(MONTH(sales_month), 2, '0') AS month,
      CAST(YEAR(sales_month) AS STRING) AS year
    FROM itm
  ),
  -- All valid combinations from TTM
  ttm_keys AS (
    SELECT
      country_code,
      brand_name,
      territory_id,
      LPAD(MONTH(fiscal_date), 2, '0') AS month,
      CAST(YEAR(fiscal_date) AS STRING) AS year
    FROM ttm
  ),
  -- Union all keys to get all possible combinations
  all_keys AS (
    SELECT * FROM itm_keys
    UNION
    SELECT * FROM ttm_keys
  ),
  -- Aggregate ITM sales
  agg_itm AS (
    SELECT
      country_code,
      brand_name,
      territory_id,
      LPAD(MONTH(sales_month), 2, '0') AS month,
      CAST(YEAR(sales_month) AS STRING) AS year,
      SUM(CASE WHEN TRY_CAST(sales_value AS DOUBLE) IS NULL THEN 0.0 ELSE sales_value END) AS itm_value,
      MAX(source_system_name) AS itm_source
    FROM itm
    GROUP BY country_code, brand_name, territory_id, LPAD(MONTH(sales_month), 2, '0'), CAST(YEAR(sales_month) AS STRING)
  ),
  -- Aggregate TTM sales
  agg_ttm AS (
    SELECT
      country_code,
      brand_name,
      territory_id,
      LPAD(MONTH(fiscal_date), 2, '0') AS month,
      CAST(YEAR(fiscal_date) AS STRING) AS year,
      SUM(CASE WHEN TRY_CAST(sales_net_price_local AS DOUBLE) IS NULL THEN 0.0 ELSE sales_net_price_local END) AS ttm_value,
      MAX(source_system_name) AS ttm_source
    FROM ttm
    GROUP BY country_code, brand_name, territory_id, LPAD(MONTH(fiscal_date), 2, '0'), CAST(YEAR(fiscal_date) AS STRING)
  ),
  -- Join aggregates on all keys (full outer join logic)
  joined AS (
    SELECT
      k.country_code,
      k.brand_name,
      k.territory_id,
      k.month,
      k.year,
      COALESCE(i.itm_value, 0.0) AS itm_value,
      COALESCE(t.ttm_value, 0.0) AS ttm_value,
      i.itm_source,
      t.ttm_source
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
-- Final output: filter by valid source_systems, ensure no nulls, and sum values
SELECT
  j.country_code,
  j.brand_name,
  j.territory_id,
  j.month,
  j.year,
  (j.itm_value + j.ttm_value) AS actual_value
FROM joined j
WHERE
  -- At least one source_system must be valid for the record to be included
  (
    (j.itm_source IS NOT NULL AND j.itm_source IN (SELECT source_system FROM valid_sources))
    OR
    (j.ttm_source IS NOT NULL AND j.ttm_source IN (SELECT source_system FROM valid_sources))
  )
  -- Exclude records where both values are zero (i.e., both tables have no data)
  AND ((j.itm_value + j.ttm_value) IS NOT NULL AND (j.itm_value + j.ttm_value) != 0)
  -- Ensure all required fields are not null
  AND j.country_code IS NOT NULL
  AND j.brand_name IS NOT NULL
  AND j.territory_id IS NOT NULL
  AND j.month IS NOT NULL
  AND j.year IS NOT NULL
ORDER BY
  j.country_code, j.brand_name, j.territory_id, j.year, j.month;
