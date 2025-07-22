-- Databricks SQL script: Extract JSON fields from product_details as per design
-- Purpose: Select and cast specified fields from the JSON string column product_details
-- Author: Giang Nguyen
-- Date: 2025-07-22
-- Description: This script queries purgo_playground.d_product_revenue, extracting the keys batch_number, expiration_date,
--              manufacturing_site, regulatory_approval, and price from the product_details column (JSON string).
--              Strict Databricks SQL JSON extraction and type-casting are applied, enforcing type rules as follows:
--              STRING for the first four fields, DECIMAL(10,2) for price.
--              Missing or malformed fields result in NULL; malformed price or date format also yields NULL.
--              No rows or columns are filtered/excluded except as per extraction and casting logic.

WITH extracted_product_details AS (
  -- This CTE extracts each JSON key from the product_details field and applies proper type-casting.
  SELECT
    -- Extracts the batch_number field as STRING, or NULL if missing or not a string.
    CAST(
      get_json_object(product_details, '$.batch_number') AS STRING
    ) AS batch_number,
    -- Extracts expiration_date; returns value if matches 'yyyy-MM-dd', else NULL (invalid formats).
    CASE
      WHEN regexp_replace(get_json_object(product_details, '$.expiration_date'), '[^\d-]', '') 
         RLIKE '^\d{4}-\d{2}-\d{2}$'
      THEN get_json_object(product_details, '$.expiration_date')
      ELSE NULL
    END AS expiration_date,
    -- Extracts manufacturing_site as STRING, or NULL if missing.
    CAST(
      get_json_object(product_details, '$.manufacturing_site') AS STRING
    ) AS manufacturing_site,
    -- Extracts regulatory_approval as STRING, or NULL if missing.
    CAST(
      get_json_object(product_details, '$.regulatory_approval') AS STRING
    ) AS regulatory_approval,
    -- Extracts price, handles number or string, outputs as DECIMAL(10,2), NULL if non-numeric or missing.
    CASE
      WHEN CAST(get_json_object(product_details, '$.price') AS DECIMAL(10,2)) IS NOT NULL
        THEN CAST(get_json_object(product_details, '$.price') AS DECIMAL(10,2))
      -- If price as string (e.g., "249.50"), try to cast after removing quotes (Databricks will handle NULL if not numeric)
      WHEN TRY_CAST(get_json_object(product_details, '$.price') AS STRING) IS NOT NULL AND
         TRY_CAST(get_json_object(product_details, '$.price') AS DECIMAL(10,2)) IS NOT NULL
        THEN CAST(get_json_object(product_details, '$.price') AS DECIMAL(10,2))
      ELSE NULL
    END AS price
  FROM purgo_playground.d_product_revenue
)
-- Final SELECT of all extracted and cast fields as per requirements.
SELECT
  batch_number,
  expiration_date,
  manufacturing_site,
  regulatory_approval,
  price
FROM extracted_product_details
;
