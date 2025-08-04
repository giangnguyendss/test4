USE CATALOG purgo_databricks;

/* 
==========================================================================================
Databricks SQL Implementation: Calculate actual_value per brand, country, territory, month, and year
==========================================================================================

- Sums sales_value from purgo_playground.t3_itm_territory_sales and sales_net_price_local from purgo_playground.t3_ttm_territory_sales
- Joins on country_code, brand_name, territory_id, and extracted month/year from date columns
- Only includes records where source_system_name exists in purgo_playground.control_table.source_system
- Excludes records with NULLs in required columns or invalid numeric values
- Aggregates (sums) all values for each unique key
- Output columns: country_code, brand_name, territory_id, month, year, actual_value (all NOT NULL)
==========================================================================================
*/

/*------------------------------------------------------------------------------
SECTION: Insert Calculation Result into Target Table
------------------------------------------------------------------------------*/
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
      AND CAST(i.sales_value AS DOUBLE) IS NOT NULL
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
      AND CAST(t.sales_net_price_local AS DOUBLE) IS NOT NULL
      AND t.source_system_name IS NOT NULL
  ),
  /* 
    CTE: unioned_sales
    - Union all valid sales from both sources
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
FROM aggregated_sales
;

/*------------------------------------------------------------------------------
SECTION: Column Comments for actual_value_calculation Table
------------------------------------------------------------------------------*/
-- Country code (ISO-2 or ISO-3). Not null. Example: "US", "DE"
ALTER TABLE purgo_playground.actual_value_calculation ALTER COLUMN country_code COMMENT 'Country code (ISO-2 or ISO-3). Not null. Example: "US", "DE"';
-- Brand name. Not null. Example: "BRAND_A"
ALTER TABLE purgo_playground.actual_value_calculation ALTER COLUMN brand_name COMMENT 'Brand name. Not null. Example: "BRAND_A"';
-- Territory identifier. Not null. Example: "T001"
ALTER TABLE purgo_playground.actual_value_calculation ALTER COLUMN territory_id COMMENT 'Territory identifier. Not null. Example: "T001"';
-- Month extracted from sales_month or fiscal_date. String (1-12). Not null.
ALTER TABLE purgo_playground.actual_value_calculation ALTER COLUMN month COMMENT 'Month extracted from sales_month or fiscal_date. String (1-12). Not null.';
-- Year extracted from sales_month or fiscal_date. String (e.g., "2023"). Not null.
ALTER TABLE purgo_playground.actual_value_calculation ALTER COLUMN year COMMENT 'Year extracted from sales_month or fiscal_date. String (e.g., "2023"). Not null.';
-- Sum of sales_value (ITM) and sales_net_price_local (TTM) for the key. Not null.
ALTER TABLE purgo_playground.actual_value_calculation ALTER COLUMN actual_value COMMENT 'Sum of sales_value (ITM) and sales_net_price_local (TTM) for the key. Not null.';

/*------------------------------------------------------------------------------
SECTION: Data Quality Logging (for error exclusion)
------------------------------------------------------------------------------*/
-- Log invalid numeric values in sales_value or sales_net_price_local
SELECT
  "Invalid numeric value in sales_value or sales_net_price_local" AS error_message,
  i.country_code,
  i.brand_name,
  i.territory_id,
  i.sales_month,
  i.sales_value
FROM purgo_playground.t3_itm_territory_sales i
WHERE CAST(i.sales_value AS DOUBLE) IS NULL AND i.sales_value IS NOT NULL
UNION ALL
SELECT
  "Invalid numeric value in sales_value or sales_net_price_local" AS error_message,
  t.country_code,
  t.brand_name,
  t.territory_id,
  t.fiscal_date,
  t.sales_net_price_local
FROM purgo_playground.t3_ttm_territory_sales t
WHERE CAST(t.sales_net_price_local AS DOUBLE) IS NULL AND t.sales_net_price_local IS NOT NULL
;

-- Log nulls in required columns
SELECT
  "Null value in required column" AS error_message,
  i.country_code,
  i.brand_name,
  i.territory_id,
  i.sales_month,
  i.sales_value
FROM purgo_playground.t3_itm_territory_sales i
WHERE i.country_code IS NULL OR i.brand_name IS NULL OR i.territory_id IS NULL OR i.sales_month IS NULL OR i.sales_value IS NULL OR i.source_system_name IS NULL
UNION ALL
SELECT
  "Null value in required column" AS error_message,
  t.country_code,
  t.brand_name,
  t.territory_id,
  t.fiscal_date,
  t.sales_net_price_local
FROM purgo_playground.t3_ttm_territory_sales t
WHERE t.country_code IS NULL OR t.brand_name IS NULL OR t.territory_id IS NULL OR t.fiscal_date IS NULL OR t.sales_net_price_local IS NULL OR t.source_system_name IS NULL
;

/*------------------------------------------------------------------------------
SECTION: End of Script
------------------------------------------------------------------------------*/
-- End of production-ready Databricks SQL implementation for actual_value calculation
