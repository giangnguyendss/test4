USE CATALOG purgo_databricks;

/*
==========================================================================================
  Databricks SQL: Calculate actual_value for each brand_name, country_code, and territory_id by month and year
  ----------------------------------------------------------------------------------------
  - Sums sales_value from purgo_playground.t3_itm_territory_sales and sales_net_price_local from purgo_playground.t3_ttm_territory_sales
  - Joins on country_code, brand_name, territory_id, and (year, month) extracted from sales_month/fiscal_date
  - Only includes records where source_system_name from both tables exists in purgo_playground.control_table.source_system
  - Excludes records with NULL/empty required fields or invalid (non-numeric) sales values
  - Treats missing data in either table as zero for aggregation
  - Output columns: country_code (STRING), brand_name (STRING), territory_id (STRING), year (INT), month (INT), actual_value (DOUBLE)
  - Output is ordered by country_code, brand_name, territory_id, year, month
  - All logic is contained in CTEs, no temp views or temp tables are used
==========================================================================================
*/

WITH
/* CTE: Valid source systems from control_table */
valid_source_systems AS (
  SELECT DISTINCT source_system
  FROM purgo_playground.control_table
  WHERE source_system IS NOT NULL AND TRIM(source_system) != ""
),
/* CTE: Valid ITM sales (filtering out NULLs, empties, and invalids) */
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
/* CTE: Valid TTM sales (filtering out NULLs, empties, and invalids) */
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
/*
==========================================================================================
  FINAL SELECT: Output actual_value per combination, ordered as required
==========================================================================================
*/
SELECT
  country_code,
  brand_name,
  territory_id,
  year,
  month,
  itm_value + ttm_value AS actual_value
FROM combined
ORDER BY country_code, brand_name, territory_id, year, month
;
-- End of Databricks SQL implementation for actual_value calculation
