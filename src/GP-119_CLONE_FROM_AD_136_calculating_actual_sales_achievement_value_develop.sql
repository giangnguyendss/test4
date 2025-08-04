USE CATALOG purgo_databricks;

/*
==========================================================================================
Databricks SQL: Calculate Actual Sales Value per Brand, Country, Territory, Year, and Month
==========================================================================================
- Sums sales_value from purgo_playground.t3_itm_territory_sales and sales_net_price_local
  from purgo_playground.t3_ttm_territory_sales for each (brand_name, country_code, territory_id, year, month).
- Only includes records where source_system_name in both tables exists in purgo_playground.control_table.
- Joins on country_code, brand_name, territory_id, and extracted year/month from sales_month/fiscal_date.
- Handles nulls in sales_value and sales_net_price_local as zero.
- Excludes records with null brand_name, country_code, or territory_id.
- Returns columns: country_code (STRING), brand_name (STRING), territory_id (STRING), year (INT), month (INT), actual_value (DOUBLE).
==========================================================================================
*/

WITH
/*------------------------------------------------------------------------------
  CTE: itm_valid
  - Filters t3_itm_territory_sales to only valid records:
    * Non-null country_code, brand_name, territory_id, sales_month
    * source_system_name exists in control_table for same country_code, brand_name
------------------------------------------------------------------------------*/
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
/*------------------------------------------------------------------------------
  CTE: ttm_valid
  - Filters t3_ttm_territory_sales to only valid records:
    * Non-null country_code, brand_name, territory_id, fiscal_date
    * source_system_name exists in control_table for same country_code, brand_name
------------------------------------------------------------------------------*/
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
/*------------------------------------------------------------------------------
  CTE: joined_sales
  - Joins itm_valid and ttm_valid on country_code, brand_name, territory_id, year, and month
  - Extracts year and month from sales_month (itm) and fiscal_date (ttm)
------------------------------------------------------------------------------*/
joined_sales AS (
  SELECT
    itm.country_code,
    itm.brand_name,
    itm.territory_id,
    YEAR(itm.sales_month) AS year,
    MONTH(itm.sales_month) AS month,
    itm.sales_value,
    ttm.sales_net_price_local
  FROM itm_valid itm
  INNER JOIN ttm_valid ttm
    ON itm.country_code = ttm.country_code
    AND itm.brand_name = ttm.brand_name
    AND itm.territory_id = ttm.territory_id
    AND YEAR(itm.sales_month) = YEAR(ttm.fiscal_date)
    AND MONTH(itm.sales_month) = MONTH(ttm.fiscal_date)
),
/*------------------------------------------------------------------------------
  CTE: actual_value_agg
  - Aggregates by country_code, brand_name, territory_id, year, month
  - Sums sales_value and sales_net_price_local, treating nulls as zero
------------------------------------------------------------------------------*/
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
/*------------------------------------------------------------------------------
  FINAL SELECT: Return actual_value per group
------------------------------------------------------------------------------*/
SELECT
  country_code,
  brand_name,
  territory_id,
  year,
  month,
  actual_value
FROM actual_value_agg
ORDER BY country_code, brand_name, territory_id, year, month
;
