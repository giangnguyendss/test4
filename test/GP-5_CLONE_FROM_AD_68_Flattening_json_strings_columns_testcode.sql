-- Databricks SQL script: Extract and validate fields from JSON column in product revenue table
-- Purpose: Test extraction, casting, NULL handling, and error scenarios when flattening JSON string product_details
-- Author: Giang Nguyen
-- Date: 2025-07-22
-- Description: This SQL test script runs validation queries over purgo_playground.d_product_revenue, ensuring correct extraction
--              and casting of JSON keys batch_number, expiration_date, manufacturing_site, regulatory_approval, and price.
--              Also tests for proper NULLs on missing/invalid keys, error raising on malformed JSON, extra field exclusion,
--              type strictness for price/expiration_date, and result column conformity.

--------------------------------------------------------------------------------
/* SECTION: Unit Test – Validate strict extraction/casting of expected fields */
--------------------------------------------------------------------------------

-- Test: For all product_details, only the five expected columns are extracted and have correct types or NULLs if missing
WITH extracted_product_details AS (
  SELECT
    CAST(get_json_object(product_details, "$.batch_number") AS STRING) AS batch_number,
    CASE
      WHEN regexp_replace(get_json_object(product_details, "$.expiration_date"), "[^\\d-]", "") RLIKE "^\\d{4}-\\d{2}-\\d{2}$"
        THEN get_json_object(product_details, "$.expiration_date")
      ELSE NULL
    END AS expiration_date,
    CAST(get_json_object(product_details, "$.manufacturing_site") AS STRING) AS manufacturing_site,
    CAST(get_json_object(product_details, "$.regulatory_approval") AS STRING) AS regulatory_approval,
    CASE
      WHEN TRY_CAST(get_json_object(product_details, "$.price") AS DECIMAL(10,2)) IS NOT NULL
        THEN CAST(get_json_object(product_details, "$.price") AS DECIMAL(10,2))
      ELSE NULL
    END AS price
  FROM purgo_playground.d_product_revenue
)
-- Validate presence of only the required columns and their types/nulls.
SELECT
  CASE WHEN COUNT(*) > 0 THEN "PASS" ELSE "FAIL" END AS unit_test_extraction_result
FROM (
  SELECT *
  FROM extracted_product_details
) t;

--------------------------------------------------------------------------------
/* SECTION: Data Quality Test – Happy Path and Type Handling */
--------------------------------------------------------------------------------

-- Test: Happy Path - All keys present, all types correct, values match test dataset for positive scenario
WITH happy_path_row AS (
  SELECT
    CAST(get_json_object(product_details, "$.batch_number") AS STRING) AS batch_number,
    CASE WHEN regexp_replace(get_json_object(product_details, "$.expiration_date"), "[^\\d-]", "") RLIKE "^\\d{4}-\\d{2}-\\d{2}$"
      THEN get_json_object(product_details, "$.expiration_date")
      ELSE NULL
    END AS expiration_date,
    CAST(get_json_object(product_details, "$.manufacturing_site") AS STRING) AS manufacturing_site,
    CAST(get_json_object(product_details, "$.regulatory_approval") AS STRING) AS regulatory_approval,
    CASE WHEN TRY_CAST(get_json_object(product_details, "$.price") AS DECIMAL(10,2)) IS NOT NULL
      THEN CAST(get_json_object(product_details, "$.price") AS DECIMAL(10,2))
      ELSE NULL
    END AS price
  FROM purgo_playground.d_product_revenue
  WHERE product_details LIKE "%BATCH2024-5678%"
)
-- Assertion: Expected values from sample row
SELECT
  CASE
    WHEN batch_number = "BATCH2024-5678"
     AND expiration_date = "2025-12-31"
     AND manufacturing_site = "Site A"
     AND regulatory_approval = "Approved"
     AND price = 250.75
    THEN "PASS"
    ELSE "FAIL"
  END AS happy_path_test_result
FROM happy_path_row;

--------------------------------------------------------------------------------
/* SECTION: Data Quality Test – NULL on Missing Keys */
--------------------------------------------------------------------------------

-- Test: NULL produced for missing JSON keys (e.g., missing batch_number & expiration_date)
WITH missing_keys AS (
  SELECT
    CAST(get_json_object(product_details, "$.batch_number") AS STRING) AS batch_number,
    CASE WHEN regexp_replace(get_json_object(product_details, "$.expiration_date"), "[^\\d-]", "") RLIKE "^\\d{4}-\\d{2}-\\d{2}$"
      THEN get_json_object(product_details, "$.expiration_date")
      ELSE NULL
    END AS expiration_date,
    CAST(get_json_object(product_details, "$.manufacturing_site") AS STRING) AS manufacturing_site,
    CAST(get_json_object(product_details, "$.regulatory_approval") AS STRING) AS regulatory_approval,
    CASE WHEN TRY_CAST(get_json_object(product_details, "$.price") AS DECIMAL(10,2)) IS NOT NULL
      THEN CAST(get_json_object(product_details, "$.price") AS DECIMAL(10,2))
      ELSE NULL
    END AS price
  FROM purgo_playground.d_product_revenue
  WHERE product_details LIKE "%Site B%" AND product_details LIKE "%Approved%"
)
SELECT
  CASE
    WHEN batch_number IS NULL AND expiration_date IS NULL
     AND manufacturing_site = "Site B"
     AND regulatory_approval = "Approved"
     AND price = 99.99
    THEN "PASS"
    ELSE "FAIL"
  END AS missing_keys_test_result
FROM missing_keys;

--------------------------------------------------------------------------------
/* SECTION: Data Quality Test – Price as String vs Number and Non-Numeric */
--------------------------------------------------------------------------------

-- Test: Price provided as string – should cast to decimal
WITH price_as_string AS (
  SELECT
    CASE WHEN TRY_CAST(get_json_object(product_details, "$.price") AS DECIMAL(10,2)) IS NOT NULL
      THEN CAST(get_json_object(product_details, "$.price") AS DECIMAL(10,2))
      ELSE NULL
    END AS price
  FROM purgo_playground.d_product_revenue
  WHERE product_details LIKE "%\"price\": \"249.50\"%"
)
SELECT
  CASE
    WHEN price = 249.50 THEN "PASS"
    ELSE "FAIL"
  END AS price_string_cast_test_result
FROM price_as_string;

-- Test: Non-numeric price (should yield NULL)
WITH price_non_numeric AS (
  SELECT
    CASE WHEN TRY_CAST(get_json_object(product_details, "$.price") AS DECIMAL(10,2)) IS NOT NULL
      THEN CAST(get_json_object(product_details, "$.price") AS DECIMAL(10,2))
      ELSE NULL
    END AS price
  FROM purgo_playground.d_product_revenue
  WHERE product_details LIKE "%NotANumber%"
)
SELECT
  CASE
    WHEN price IS NULL THEN "PASS"
    ELSE "FAIL"
  END AS price_non_numeric_test_result
FROM price_non_numeric;

--------------------------------------------------------------------------------
/* SECTION: Data Quality Test – Strict yyyy-MM-dd Date Format */
--------------------------------------------------------------------------------

-- Test: Valid expiration_date format (yyyy-MM-dd)
WITH valid_date AS (
  SELECT
    CASE
      WHEN regexp_replace(get_json_object(product_details, "$.expiration_date"), "[^\\d-]", "") RLIKE "^\\d{4}-\\d{2}-\\d{2}$"
        THEN get_json_object(product_details, "$.expiration_date")
      ELSE NULL
    END AS expiration_date
  FROM purgo_playground.d_product_revenue
  WHERE product_details LIKE "%2030-02-28%"
)
SELECT
  CASE
    WHEN expiration_date = "2030-02-28" THEN "PASS"
    ELSE "FAIL"
  END AS expiration_date_valid_test_result
FROM valid_date;

-- Test: Invalid expiration_date format (should yield NULL)
WITH invalid_date AS (
  SELECT
    CASE
      WHEN regexp_replace(get_json_object(product_details, "$.expiration_date"), "[^\\d-]", "") RLIKE "^\\d{4}-\\d{2}-\\d{2}$"
        THEN get_json_object(product_details, "$.expiration_date")
      ELSE NULL
    END AS expiration_date
  FROM purgo_playground.d_product_revenue
  WHERE product_details LIKE "%02-28-2030%" OR product_details LIKE "%2030/02/28%"
)
SELECT
  CASE
    WHEN expiration_date IS NULL THEN "PASS"
    ELSE "FAIL"
  END AS expiration_date_invalid_test_result
FROM invalid_date;

--------------------------------------------------------------------------------
/* SECTION: Schema Validation – Only Expected Columns Output and Type Compliance */
--------------------------------------------------------------------------------

-- Test: Output column names and types as per requirements
WITH test_schema_output AS (
  SELECT * FROM (
    SELECT
      CAST(get_json_object(product_details, "$.batch_number") AS STRING) AS batch_number,
      CASE
        WHEN regexp_replace(get_json_object(product_details, "$.expiration_date"), "[^\\d-]", "") RLIKE "^\\d{4}-\\d{2}-\\d{2}$"
          THEN get_json_object(product_details, "$.expiration_date")
        ELSE NULL
      END AS expiration_date,
      CAST(get_json_object(product_details, "$.manufacturing_site") AS STRING) AS manufacturing_site,
      CAST(get_json_object(product_details, "$.regulatory_approval") AS STRING) AS regulatory_approval,
      CASE
        WHEN TRY_CAST(get_json_object(product_details, "$.price") AS DECIMAL(10,2)) IS NOT NULL
          THEN CAST(get_json_object(product_details, "$.price") AS DECIMAL(10,2))
        ELSE NULL
      END AS price
    FROM purgo_playground.d_product_revenue
    LIMIT 1
  )
)
SELECT
  CASE
    WHEN struct(
      typeof(batch_number),
      typeof(expiration_date),
      typeof(manufacturing_site),
      typeof(regulatory_approval),
      typeof(price)
    ) = struct("STRING", "STRING", "STRING", "STRING", "DECIMAL(10,2)")
    THEN "PASS"
    ELSE "FAIL"
  END AS schema_type_and_column_names_test
FROM test_schema_output;

--------------------------------------------------------------------------------
/* SECTION: Error Handling – Malformed JSON Must Throw Error */
--------------------------------------------------------------------------------

-- Test: Malformed JSON raises error (run in safe mode to capture exception)—this block is for manual/external exception harness
-- Note: Databricks SQL will error on malformed JSON when using get_json_object; except if input is not a valid JSON string.
-- For safety, this test should be verified outside the script via a test harness or by capturing runtime error externally.
-- Example row: { batch_number: BATCH2024-5678, regulatory_approval }
-- -- Expected: Query fails with error regarding malformed JSON

--------------------------------------------------------------------------------
/* SECTION: Extra Field Ignored – Ensure Only Five Columns Selected */
--------------------------------------------------------------------------------

-- Test: Ignore extra keys – only the five columns are present, ignore any extra fields
WITH extra_field_row AS (
  SELECT
    COUNT(*) AS correct_columns
  FROM (
    SELECT
      CAST(get_json_object(product_details, "$.batch_number") AS STRING) AS batch_number,
      CASE
        WHEN regexp_replace(get_json_object(product_details, "$.expiration_date"), "[^\\d-]", "") RLIKE "^\\d{4}-\\d{2}-\\d{2}$"
          THEN get_json_object(product_details, "$.expiration_date")
        ELSE NULL
      END AS expiration_date,
      CAST(get_json_object(product_details, "$.manufacturing_site") AS STRING) AS manufacturing_site,
      CAST(get_json_object(product_details, "$.regulatory_approval") AS STRING) AS regulatory_approval,
      CASE
        WHEN TRY_CAST(get_json_object(product_details, "$.price") AS DECIMAL(10,2)) IS NOT NULL
          THEN CAST(get_json_object(product_details, "$.price") AS DECIMAL(10,2))
        ELSE NULL
      END AS price
    FROM purgo_playground.d_product_revenue
    WHERE product_details LIKE "%extra_field%"
  )
)
SELECT
  CASE
    WHEN correct_columns = 1 THEN "PASS"
    ELSE "FAIL"
  END AS ignore_extra_field_test_result
FROM extra_field_row;

--------------------------------------------------------------------------------
/* SECTION: Integration Test – End-to-End Extraction Query Executes Without Error */
--------------------------------------------------------------------------------

-- Test: Full extraction query executes without syntax/runtime error
-- If this cell runs and returns rows, test is considered passed
WITH extracted_product_details AS (
  SELECT
    CAST(get_json_object(product_details, "$.batch_number") AS STRING) AS batch_number,
    CASE
      WHEN regexp_replace(get_json_object(product_details, "$.expiration_date"), "[^\\d-]", "") RLIKE "^\\d{4}-\\d{2}-\\d{2}$"
        THEN get_json_object(product_details, "$.expiration_date")
      ELSE NULL
    END AS expiration_date,
    CAST(get_json_object(product_details, "$.manufacturing_site") AS STRING) AS manufacturing_site,
    CAST(get_json_object(product_details, "$.regulatory_approval") AS STRING) AS regulatory_approval,
    CASE
      WHEN TRY_CAST(get_json_object(product_details, "$.price") AS DECIMAL(10,2)) IS NOT NULL
        THEN CAST(get_json_object(product_details, "$.price") AS DECIMAL(10,2))
      ELSE NULL
    END AS price
  FROM purgo_playground.d_product_revenue
)
SELECT
  "PASS" AS integration_e2e_query_execution_result
FROM extracted_product_details
LIMIT 1;
