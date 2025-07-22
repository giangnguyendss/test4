-- Databricks SQL script: Atomic upsert of Excel-mapped data with strict validation to ingest_config_master
-- Purpose: Performs all-or-nothing atomic insert/update into default.ingest_config_master by fully validating and mapping Excel data
-- Author: Giang Nguyen
-- Date: 2025-07-22
-- Description: Loads, trims, validates, and atomic-upserts all records from Excel into the ingest_config_master table.
--              Aborts the entire operation if validation errors exist or duplicate config_id in the Excel.

--------------------------------------------------------------------------------
-- SECTION 1: ASSUMPTION - input table tmp_excel_ingest_config_master exists
-- (You must load the Excel into a temp table named tmp_excel_ingest_config_master 
--  prior to running this SQL, e.g. via PySpark)
--------------------------------------------------------------------------------

--------------------------------------------------------------------------------
-- SECTION 2: PREPARE TRIMMED/TYPECASTED DATA CTE
--------------------------------------------------------------------------------

WITH raw_excel_data AS (
  SELECT
    TRIM(config_id) AS config_id,
    TRIM(source_object_name) AS source_object_name,
    TRIM(source_system) AS source_system,
    TRIM(file_name) AS file_name,
    TRIM(frequency) AS frequency,
    TRIM(location) AS location,
    TRIM(domain) AS domain,
    TRIM(sub_domain) AS sub_domain,
    TRIM(active_flag) AS active_flag,
    TRIM(s3_vendor_path) AS s3_vendor_path,
    TRIM(source_path) AS source_path,
    TRIM(s3_landing_path) AS s3_landing_path,
    TRIM(s3_archive_path) AS s3_archive_path,
    TRIM(delta_load_ts) AS delta_load_ts,
    TRIM(full_or_incremental_load) AS full_or_incremental_load,
    TRIM(zip_file) AS zip_file,
    TRIM(vendor) AS vendor,
    TRIM(delimiter) AS delimiter,
    TRIM(source_landing) AS source_landing,
    TRIM(src_landing_table_name) AS src_landing_table_name,
    TRIM(publish_unstitched) AS publish_unstitched,
    TRIM(publish_unstitched_table_name) AS publish_unstitched_table_name,
    TRIM(publish_stitched) AS publish_stitched,
    TRIM(publish_stitched_table_name) AS publish_stitched_table_name,
    TRIM(primary_key) AS primary_key,
    TRIM(header) AS header,
    TRIM(date_pattern) AS date_pattern,
    TRIM(actual_file_name) AS actual_file_name,
    TRIM(vendor_file_deletion_flag) AS vendor_file_deletion_flag,
    TRIM(file_recursive_flag) AS file_recursive_flag,
    TRIM(total_weeks_req_data) AS total_weeks_req_data,
    TRIM(total_weeks_file_data) AS total_weeks_file_data
  FROM tmp_excel_ingest_config_master
),

--------------------------------------------------------------------------------
-- SECTION 3: PARSE & NORMALIZE (TYPES, NULLS FOR BLANK, DATETIME FORMAT)
--------------------------------------------------------------------------------

parsed_excel_data AS (
  SELECT
    CAST(config_id AS INT) AS config_id,
    NULLIF(source_object_name, '') AS source_object_name,
    NULLIF(source_system, '') AS source_system,
    NULLIF(file_name, '') AS file_name,
    NULLIF(frequency, '') AS frequency,
    NULLIF(location, '') AS location,
    NULLIF(domain, '') AS domain,
    NULLIF(sub_domain, '') AS sub_domain,
    CAST(NULLIF(active_flag, '') AS INT) AS active_flag,
    NULLIF(s3_vendor_path, '') AS s3_vendor_path,
    NULLIF(source_path, '') AS source_path,
    NULLIF(s3_landing_path, '') AS s3_landing_path,
    NULLIF(s3_archive_path, '') AS s3_archive_path,
    CASE 
      WHEN delta_load_ts = '' THEN NULL
      WHEN TRY_CAST(TO_TIMESTAMP(delta_load_ts, 'yyyy-MM-dd HH:mm:ss') AS TIMESTAMP) IS NOT NULL THEN TO_TIMESTAMP(delta_load_ts, 'yyyy-MM-dd HH:mm:ss')
      ELSE '__INVALID_TS__'
    END AS delta_load_ts,
    NULLIF(full_or_incremental_load, '') AS full_or_incremental_load,
    NULLIF(zip_file, '') AS zip_file,
    NULLIF(vendor, '') AS vendor,
    CASE WHEN delimiter = '' THEN NULL ELSE delimiter END AS delimiter,
    NULLIF(source_landing, '') AS source_landing,
    NULLIF(src_landing_table_name, '') AS src_landing_table_name,
    NULLIF(publish_unstitched, '') AS publish_unstitched,
    CASE WHEN publish_unstitched_table_name = '' THEN NULL ELSE publish_unstitched_table_name END AS publish_unstitched_table_name,
    NULLIF(publish_stitched, '') AS publish_stitched,
    CASE WHEN publish_stitched_table_name = '' THEN NULL ELSE publish_stitched_table_name END AS publish_stitched_table_name,
    NULLIF(primary_key, '') AS primary_key,
    NULLIF(header, '') AS header,
    CASE WHEN date_pattern = '' THEN NULL ELSE date_pattern END AS date_pattern,
    NULLIF(actual_file_name, '') AS actual_file_name,
    NULLIF(vendor_file_deletion_flag, '') AS vendor_file_deletion_flag,
    NULLIF(file_recursive_flag, '') AS file_recursive_flag,
    CAST(NULLIF(total_weeks_req_data, '') AS INT) AS total_weeks_req_data,
    CAST(NULLIF(total_weeks_file_data, '') AS INT) AS total_weeks_file_data
  FROM raw_excel_data
),

--------------------------------------------------------------------------------
-- SECTION 4: VALIDATION RULES (ONE-ROW-ONE-ERROR, ENUMS, REQ., TYPES)
--------------------------------------------------------------------------------

validated_excel_data AS (
  SELECT
    *,
    CASE 
      WHEN config_id IS NULL OR config_id <= 0 THEN CONCAT('Validation error: config_id is required and > 0 (got ', COALESCE(CAST(config_id AS STRING), 'NULL'), ')')
      WHEN source_object_name IS NULL THEN 'Validation error: source_object_name is required and cannot be blank'
      WHEN source_system IS NULL THEN 'Validation error: source_system is required and cannot be blank'
      WHEN file_name IS NULL THEN 'Validation error: file_name is required and cannot be blank'
      WHEN frequency IS NULL THEN 'Validation error: frequency is required and cannot be blank'
      WHEN location IS NULL THEN 'Validation error: location is required and cannot be blank'
      WHEN domain IS NULL THEN 'Validation error: domain is required and cannot be blank'
      WHEN sub_domain IS NULL THEN 'Validation error: sub_domain is required and cannot be blank'
      WHEN active_flag IS NULL OR active_flag NOT IN (0, 1) THEN 'Validation error: active_flag must be 0 or 1'
      WHEN s3_vendor_path IS NULL THEN 'Validation error: s3_vendor_path is required and cannot be blank'
      WHEN source_path IS NULL THEN 'Validation error: source_path is required and cannot be blank'
      WHEN s3_landing_path IS NULL THEN 'Validation error: s3_landing_path is required and cannot be blank'
      WHEN s3_archive_path IS NULL THEN 'Validation error: s3_archive_path is required and cannot be blank'
      WHEN delta_load_ts = '__INVALID_TS__' THEN 'Validation error: delta_load_ts must be in format yyyy-MM-dd HH:mm:ss'
      WHEN full_or_incremental_load IS NULL OR full_or_incremental_load NOT IN ('Full', 'Incremental') 
        THEN 'Validation error: full_or_incremental_load must be Full or Incremental'
      WHEN zip_file IS NULL OR zip_file NOT IN ('Yes', 'No') 
        THEN 'Validation error: zip_file must be Yes or No'
      WHEN vendor IS NULL THEN 'Validation error: vendor is required and cannot be blank'
      WHEN source_landing IS NULL THEN 'Validation error: source_landing is required and cannot be blank'
      WHEN src_landing_table_name IS NULL THEN 'Validation error: src_landing_table_name is required and cannot be blank'
      WHEN publish_unstitched IS NULL OR publish_unstitched NOT IN ('Yes', 'No') 
        THEN 'Validation error: publish_unstitched must be Yes or No'
      WHEN publish_stitched IS NULL OR publish_stitched NOT IN ('Yes', 'No')
        THEN 'Validation error: publish_stitched must be Yes or No'
      WHEN primary_key IS NULL THEN 'Validation error: primary_key is required and cannot be blank'
      WHEN header IS NULL OR header NOT IN ('Yes', 'No') 
        THEN 'Validation error: header must be Yes or No'
      WHEN actual_file_name IS NULL THEN 'Validation error: actual_file_name is required and cannot be blank'
      WHEN vendor_file_deletion_flag IS NULL OR vendor_file_deletion_flag NOT IN ('Yes', 'No')
        THEN 'Validation error: vendor_file_deletion_flag must be Yes or No'
      WHEN file_recursive_flag IS NULL OR file_recursive_flag NOT IN ('Yes', 'No') 
        THEN 'Validation error: file_recursive_flag must be Yes or No'
      WHEN total_weeks_req_data IS NULL OR total_weeks_req_data < 0
        THEN 'Validation error: total_weeks_req_data must be >= 0'
      WHEN total_weeks_file_data IS NULL OR total_weeks_file_data < 0
        THEN 'Validation error: total_weeks_file_data must be >= 0'
      ELSE NULL
    END AS validation_error
  FROM parsed_excel_data
),

--------------------------------------------------------------------------------
-- SECTION 5: DUPLICATE config_id DETECTION
--------------------------------------------------------------------------------

duplicate_config_id_check AS (
  SELECT config_id, COUNT(*) AS cnt
  FROM validated_excel_data
  GROUP BY config_id
  HAVING COUNT(*) > 1
),

--------------------------------------------------------------------------------
-- SECTION 6: AGGREGATE ALL ERRORS FOR ATOMIC ABORT
--------------------------------------------------------------------------------

error_collection AS (
  SELECT 
    config_id, validation_error
  FROM validated_excel_data
  WHERE validation_error IS NOT NULL
  UNION ALL
  SELECT 
    config_id, 
    CONCAT('Duplicate config_id value (', CAST(config_id AS STRING), ') found in input Excel.')
  FROM duplicate_config_id_check
),

--------------------------------------------------------------------------------
-- SECTION 7: FINAL CTE - ONYL VALIDATED ROWS, FOR MERGE
--------------------------------------------------------------------------------

valid_excel_rows AS (
  SELECT
    config_id,
    source_object_name,
    source_system,
    file_name,
    frequency,
    location,
    domain,
    sub_domain,
    active_flag,
    s3_vendor_path,
    source_path,
    s3_landing_path,
    s3_archive_path,
    CAST(delta_load_ts AS TIMESTAMP) AS delta_load_ts,
    full_or_incremental_load,
    zip_file,
    vendor,
    delimiter,
    source_landing,
    src_landing_table_name,
    publish_unstitched,
    publish_unstitched_table_name,
    publish_stitched,
    publish_stitched_table_name,
    primary_key,
    header,
    date_pattern,
    actual_file_name,
    vendor_file_deletion_flag,
    file_recursive_flag,
    total_weeks_req_data,
    total_weeks_file_data
  FROM validated_excel_data
  WHERE validation_error IS NULL
)

--------------------------------------------------------------------------------
-- SECTION 8: RAISE ERROR IF VALIDATION OR DUP DETECTED (ATOMIC ALL OR NOTHING)
--------------------------------------------------------------------------------

SELECT 
  RAISE_ERROR(
    CONCAT('Atomic upsert failed due to validation errors. Errors: ',
      array_join(COLLECT_LIST(CONCAT('config_id=', CAST(config_id AS STRING), ': ', validation_error)), '; ')
    )
  ) AS validation_step
FROM error_collection
WHERE EXISTS (SELECT 1 FROM error_collection)

UNION ALL

SELECT 'NO_VALIDATION_ERRORS' 
  WHERE NOT EXISTS (SELECT 1 FROM error_collection)
;

--------------------------------------------------------------------------------
-- SECTION 9: RUN ATOMIC UPSERT (MERGE) ONLY IF PREVIOUS STEP RETURNS 'VALIDATION_PASSED'
--------------------------------------------------------------------------------

MERGE INTO default.ingest_config_master AS tgt
USING valid_excel_rows AS src
ON tgt.config_id = src.config_id
WHEN MATCHED THEN
  UPDATE SET
    tgt.source_object_name = src.source_object_name,
    tgt.source_system = src.source_system,
    tgt.file_name = src.file_name,
    tgt.frequency = src.frequency,
    tgt.location = src.location,
    tgt.domain = src.domain,
    tgt.sub_domain = src.sub_domain,
    tgt.active_flag = src.active_flag,
    tgt.s3_vendor_path = src.s3_vendor_path,
    tgt.source_path = src.source_path,
    tgt.s3_landing_path = src.s3_landing_path,
    tgt.s3_archive_path = src.s3_archive_path,
    tgt.delta_load_ts = src.delta_load_ts,
    tgt.full_or_incremental_load = src.full_or_incremental_load,
    tgt.zip_file = src.zip_file,
    tgt.vendor = src.vendor,
    tgt.delimiter = src.delimiter,
    tgt.source_landing = src.source_landing,
    tgt.src_landing_table_name = src.src_landing_table_name,
    tgt.publish_unstitched = src.publish_unstitched,
    tgt.publish_unstitched_table_name = src.publish_unstitched_table_name,
    tgt.publish_stitched = src.publish_stitched,
    tgt.publish_stitched_table_name = src.publish_stitched_table_name,
    tgt.primary_key = src.primary_key,
    tgt.header = src.header,
    tgt.date_pattern = src.date_pattern,
    tgt.actual_file_name = src.actual_file_name,
    tgt.vendor_file_deletion_flag = src.vendor_file_deletion_flag,
    tgt.file_recursive_flag = src.file_recursive_flag,
    tgt.total_weeks_req_data = src.total_weeks_req_data,
    tgt.total_weeks_file_data = src.total_weeks_file_data
WHEN NOT MATCHED THEN
  INSERT (
    config_id, source_object_name, source_system, file_name, frequency, location, domain, sub_domain, active_flag,
    s3_vendor_path, source_path, s3_landing_path, s3_archive_path, delta_load_ts, full_or_incremental_load, zip_file, vendor,
    delimiter, source_landing, src_landing_table_name, publish_unstitched, publish_unstitched_table_name, publish_stitched,
    publish_stitched_table_name, primary_key, header, date_pattern, actual_file_name,
    vendor_file_deletion_flag, file_recursive_flag, total_weeks_req_data, total_weeks_file_data
  )
  VALUES (
    src.config_id, src.source_object_name, src.source_system, src.file_name, src.frequency, src.location, src.domain, src.sub_domain, src.active_flag,
    src.s3_vendor_path, src.source_path, src.s3_landing_path, src.s3_archive_path, src.delta_load_ts, src.full_or_incremental_load, src.zip_file, src.vendor,
    src.delimiter, src.source_landing, src.src_landing_table_name, src.publish_unstitched, src.publish_unstitched_table_name, src.publish_stitched,
    src.publish_stitched_table_name, src.primary_key, src.header, src.date_pattern, src.actual_file_name,
    src.vendor_file_deletion_flag, src.file_recursive_flag, src.total_weeks_req_data, src.total_weeks_file_data
  )
;

-- End of atomic ingest_config_master upsert script
