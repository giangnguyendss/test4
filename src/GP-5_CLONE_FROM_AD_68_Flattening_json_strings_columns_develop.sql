-- Databricks SQL script: Extract each JSON field from product_details in d_product_revenue
-- Purpose: Select all expected fields from product_details complex JSON string as individual columns.
-- Author: Giang Nguyen
-- Date: 2025-07-22
-- Description: This script extracts batch_number, expiration_date, manufacturing_site, regulatory_approval, and price from the
--              product_details column (JSON string format) in purgo_playground.d_product_revenue. It enforces strict casting and
--              formatting, returns NULL for missing or invalid values, and excludes any extra/unexpected JSON fields. Malformed
--              JSON strings will result in SQL query errors.

--------------------------------------------------------------------------------
/* SECTION: Extract fields from JSON string in product_details with strict type and format enforcement */
--------------------------------------------------------------------------------

-- Query: Flatten all required fields from the product_details JSON string
WITH extracted_product_details AS (
  SELECT
    -- batch_number: Product batch identifier. Type: STRING.
    CAST(get_json_object(product_details, "$.batch_number") AS STRING) AS batch_number,
    -- expiration_date: Product expiration in 'yyyy-MM-dd' format. Type: STRING. NULL if not present or improperly formatted.
    CASE
      WHEN regexp_replace(get_json_object(product_details, "$.expiration_date"), '[^\d-]', '') RLIKE '^\d{4}-\d{2}-\d{2}$'
        THEN get_json_object(product_details, "$.expiration_date")
      ELSE NULL
    END AS expiration_date,
    -- manufacturing_site: Name of the manufacturing location. Type: STRING.
    CAST(get_json_object(product_details, "$.manufacturing_site") AS STRING) AS manufacturing_site,
    -- regulatory_approval: Regulatory approval status. Type: STRING.
    CAST(get_json_object(product_details, "$.regulatory_approval") AS STRING) AS regulatory_approval,
    -- price: Numeric product price. Type: DECIMAL(10,2). Casts string or number, NULL for non-numeric or missing/invalid values.
    CASE
      WHEN get_json_object(product_details, "$.price") RLIKE '^[+-]?\d+(\.\d+)?$'
        THEN CAST(get_json_object(product_details, "$.price") AS DECIMAL(10,2))
      ELSE NULL
    END AS price
  FROM purgo_playground.d_product_revenue
)
-- Output: Each required JSON field as separate column, strictly typed, NULLs for any missing/invalid data.
SELECT
  batch_number,
  expiration_date,
  manufacturing_site,
  regulatory_approval,
  price
FROM extracted_product_details;
-- End of script
