USE CATALOG purgo_databricks;

/*
==========================================================================================
Databricks SQL Implementation: Calculate actual_value for each brand, country, territory, month, and year
==========================================================================================
- Sums sales_value from purgo_playground.t3_itm_territory_sales and sales_net_price_local from purgo_playground.t3_ttm_territory_sales.
- Joins on country_code, brand_name, territory_id, and month/year (extracted as 'MM' and 'YYYY' strings).
- Only includes records where source_system_name from both tables exists in purgo_playground.control_table.source_system.
- If a record exists in one table but not the other, the missing value is treated as zero.
- Nulls and non-numeric values in sales_value or sales_net_price_local are treated as zero.
- Duplicate records are aggregated by summing their values.
- Output: country_code, brand_name, territory_id, month, year, actual_value (all NOT NULL).
- Table: purgo_playground.actual_value_calculation
==========================================================================================
*/

/*------------------------------------------------------------------------------
SECTION: 1. CREATE OR REPLACE TABLE DDL FOR OUTPUT TABLE
------------------------------------------------------------------------------*/
CREATE TABLE IF NOT EXISTS purgo_playground.actual_value_calculation (
  country_code STRING NOT NULL,
  brand_name STRING NOT NULL,
  territory_id STRING NOT NULL,
  month STRING NOT NULL,
  year STRING NOT NULL,
  actual_value DOUBLE NOT NULL
)
USING DELTA
COMMENT 'Aggregated actual sales value by brand, country, territory, month, and year'
TBLPROPERTIES (
  delta.columnMapping.mode = 'name',
  delta.autoOptimize.optimizeWrite = 'true',
  delta.autoOptimize.autoCompact = 'true'
);

-- Column comments for documentation (run as separate statements, not in CTEs)
-- COMMENT ON COLUMN purgo_playground.actual_value_calculation.country_code IS 'Country code (ISO, e.g. US, DE, FR)';
-- COMMENT ON COLUMN purgo_playground.actual_value_calculation.brand_name IS 'Brand name';
-- COMMENT ON COLUMN purgo_playground.actual_value_calculation.territory_id IS 'Territory identifier';
-- COMMENT ON COLUMN purgo_playground.actual_value_calculation.month IS 'Month in MM format (01-12)';
-- COMMENT ON COLUMN purgo_playground.actual_value_calculation.year IS 'Year in YYYY format';
-- COMMENT ON COLUMN purgo_playground.actual_value_calculation.actual_value IS 'Sum of sales_value (ITM) and sales_net_price_local (TTM); nulls/non-numeric treated as 0';

/*------------------------------------------------------------------------------
SECTION: 2. MAIN INSERT/OVERWRITE LOGIC USING CTEs
------------------------------------------------------------------------------*/
INSERT OVERWRITE TABLE purgo_playground.actual_value_calculation
WITH
  /* ITM: Cleaned and aggregated sales_value */
  itm_agg AS (
    SELECT
      country_code,
      brand_name,
      territory_id,
      LPAD(CAST(MONTH(sales_month) AS STRING), 2, '0') AS month,
      CAST(YEAR(sales_month) AS STRING) AS year,
      SUM(
        CASE
          WHEN TRY_CAST(sales_value AS DOUBLE) IS NULL THEN 0.0
          ELSE CAST(sales_value AS DOUBLE)
        END
      ) AS itm_value,
      MAX(source_system_name) AS itm_source
    FROM purgo_playground.t3_itm_territory_sales
    WHERE country_code IS NOT NULL
      AND brand_name IS NOT NULL
      AND territory_id IS NOT NULL
      AND sales_month IS NOT NULL
    GROUP BY country_code, brand_name, territory_id, LPAD(CAST(MONTH(sales_month) AS STRING), 2, '0'), CAST(YEAR(sales_month) AS STRING)
  ),
  /* TTM: Cleaned and aggregated sales_net_price_local */
  ttm_agg AS (
    SELECT
      country_code,
      brand_name,
      territory_id,
      LPAD(CAST(MONTH(fiscal_date) AS STRING), 2, '0') AS month,
      CAST(YEAR(fiscal_date) AS STRING) AS year,
      SUM(
        CASE
          WHEN TRY_CAST(sales_net_price_local AS DOUBLE) IS NULL THEN 0.0
          ELSE CAST(sales_net_price_local AS DOUBLE)
        END
      ) AS ttm_value,
      MAX(source_system_name) AS ttm_source
    FROM purgo_playground.t3_ttm_territory_sales
    WHERE country_code IS NOT NULL
      AND brand_name IS NOT NULL
      AND territory_id IS NOT NULL
      AND fiscal_date IS NOT NULL
    GROUP BY country_code, brand_name, territory_id, LPAD(CAST(MONTH(fiscal_date) AS STRING), 2, '0'), CAST(YEAR(fiscal_date) AS STRING)
  ),
  /* All valid source_systems for filtering */
  valid_sources AS (
    SELECT DISTINCT source_system FROM purgo_playground.control_table WHERE source_system IS NOT NULL
  ),
  /* All unique keys from both ITM and TTM */
  all_keys AS (
    SELECT country_code, brand_name, territory_id, month, year FROM itm_agg
    UNION
    SELECT country_code, brand_name, territory_id, month, year FROM ttm_agg
  ),
  /* Join aggregates on all keys (full outer join logic) */
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
    LEFT JOIN itm_agg i
      ON k.country_code = i.country_code
      AND k.brand_name = i.brand_name
      AND k.territory_id = i.territory_id
      AND k.month = i.month
      AND k.year = i.year
    LEFT JOIN ttm_agg t
      ON k.country_code = t.country_code
      AND k.brand_name = t.brand_name
      AND k.territory_id = t.territory_id
      AND k.month = t.month
      AND k.year = t.year
  )
/*------------------------------------------------------------------------------
SECTION: 3. FINAL SELECT AND FILTERING
------------------------------------------------------------------------------*/
SELECT
  j.country_code,
  j.brand_name,
  j.territory_id,
  j.month,
  j.year,
  (j.itm_value + j.ttm_value) AS actual_value
FROM joined j
WHERE
  /* At least one source_system must be valid for the record to be included */
  (
    (j.itm_source IS NOT NULL AND j.itm_source IN (SELECT source_system FROM valid_sources))
    OR
    (j.ttm_source IS NOT NULL AND j.ttm_source IN (SELECT source_system FROM valid_sources))
  )
  /* Exclude records where both values are zero (i.e., both tables have no data) */
  AND ((j.itm_value + j.ttm_value) IS NOT NULL AND (j.itm_value + j.ttm_value) != 0)
  /* Ensure all required fields are not null */
  AND j.country_code IS NOT NULL
  AND j.brand_name IS NOT NULL
  AND j.territory_id IS NOT NULL
  AND j.month IS NOT NULL
  AND j.year IS NOT NULL
;

/* End of implementation */
