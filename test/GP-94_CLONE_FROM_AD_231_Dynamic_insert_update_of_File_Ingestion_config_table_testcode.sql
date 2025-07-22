-- Databricks SQL script: Comprehensive end-to-end test for ingest_config_master upsert from Excel
-- Purpose: Validates schema, constraints, upsert logic, and DML flows for ingest_config_master table
-- Author: Giang Nguyen
-- Date: 2025-07-22
-- Description: Covers table creation, data quality, upsert scenarios, type/enum checks, trimming, atomicity, analytics & DML validation, and clean-up. 

/* ------------------------- *
 * SECTION 1: SETUP & DDL
 * ------------------------- */

-- Drop table if exists for repeatable, isolated test runs
DROP TABLE IF EXISTS default.ingest_config_master;

-- Create ingest_config_master table: All columns, NOT NULL and ENUM constraints (CHECK not supported in Databricks, enforced in code)
CREATE TABLE default.ingest_config_master (
  config_id INT NOT NULL,
  source_object_name STRING NOT NULL,
  source_system STRING NOT NULL,
  file_name STRING NOT NULL,
  frequency STRING NOT NULL,
  location STRING NOT NULL,
  domain STRING NOT NULL,
  sub_domain STRING NOT NULL,
  active_flag INT NOT NULL,
  s3_vendor_path STRING NOT NULL,
  source_path STRING NOT NULL,
  s3_landing_path STRING NOT NULL,
  s3_archive_path STRING NOT NULL,
  delta_load_ts TIMESTAMP,
  full_or_incremental_load STRING NOT NULL,
  zip_file STRING NOT NULL,
  vendor STRING NOT NULL,
  delimiter STRING,
  source_landing STRING NOT NULL,
  src_landing_table_name STRING NOT NULL,
  publish_unstitched STRING NOT NULL,
  publish_unstitched_table_name STRING,
  publish_stitched STRING NOT NULL,
  publish_stitched_table_name STRING,
  primary_key STRING NOT NULL,
  header STRING NOT NULL,
  date_pattern STRING,
  actual_file_name STRING NOT NULL,
  vendor_file_deletion_flag STRING NOT NULL,
  file_recursive_flag STRING NOT NULL,
  total_weeks_req_data INT NOT NULL,
  total_weeks_file_data INT NOT NULL
);

/* ------------------------- *
 * SECTION 2: TABLE SCHEMA VALIDATION
 * ------------------------- */

-- Validate the column count is 31 (no more, no less)
WITH cols AS (
  SELECT COUNT(*) AS num_cols
  FROM information_schema.columns
  WHERE table_schema = 'default' AND table_name = 'ingest_config_master'
)
SELECT
  CASE WHEN num_cols = 31 THEN 'PASS'
       ELSE RAISE_ERROR("FAIL: ingest_config_master must have exactly 31 columns, found " || CAST(num_cols AS STRING))
  END AS schema_column_count_assertion
FROM cols;

-- Validate expected column names and order
WITH tbl_cols AS (
  SELECT column_name, ordinal_position
  FROM information_schema.columns
  WHERE table_schema = 'default' AND table_name = 'ingest_config_master'
),
exp_cols AS (
  SELECT stack(31,
    'config_id', 1, 'source_object_name', 2, 'source_system', 3, 'file_name', 4, 'frequency', 5, 'location', 6, 'domain',7,
    'sub_domain', 8, 'active_flag', 9, 's3_vendor_path', 10, 'source_path', 11, 's3_landing_path', 12, 's3_archive_path', 13,
    'delta_load_ts', 14, 'full_or_incremental_load', 15, 'zip_file', 16, 'vendor', 17, 'delimiter', 18, 'source_landing', 19,
    'src_landing_table_name', 20, 'publish_unstitched', 21, 'publish_unstitched_table_name', 22, 'publish_stitched', 23,
    'publish_stitched_table_name', 24, 'primary_key', 25, 'header', 26, 'date_pattern', 27, 'actual_file_name', 28,
    'vendor_file_deletion_flag', 29, 'file_recursive_flag', 30, 'total_weeks_req_data', 31, 'total_weeks_file_data', 32
  ) AS (column_name, ordinal_position)
)
SELECT
  CASE WHEN (
      SELECT count(*)
      FROM tbl_cols t
      JOIN exp_cols e ON t.column_name = e.column_name AND t.ordinal_position = e.ordinal_position
    ) = 31
    THEN 'PASS'
    ELSE RAISE_ERROR("FAIL: ingest_config_master columns do not match expected order or names")
  END AS schema_column_order_assertion;

/* ------------------------- *
 * SECTION 3: ENUM/CHECK SIMULATION & NEGATIVE TESTS
 * ------------------------- */

-- ENUM and CHECK constraint simulation (since native table-level check constraint is not supported in Databricks SQL DDL)
-- These inserts should fail on later application-level validation (not in table DDL itself)

-- Invalid ENUM: should cause app-level validation error if enforced
INSERT INTO default.ingest_config_master
(config_id, source_object_name, source_system, file_name, frequency, location, domain, sub_domain, active_flag, s3_vendor_path, source_path, s3_landing_path, s3_archive_path, delta_load_ts, full_or_incremental_load, zip_file, vendor, delimiter, source_landing, src_landing_table_name, publish_unstitched, publish_unstitched_table_name, publish_stitched, publish_stitched_table_name, primary_key, header, date_pattern, actual_file_name, vendor_file_deletion_flag, file_recursive_flag, total_weeks_req_data, total_weeks_file_data)
VALUES
(9000, 'bad_enum', 'BadS', 'bad_conf.csv', 'Yearly', 'NOLOC', 'ERR', 'S', 1, 's3://bad', '/bad', 's3://bad', 's3://bad', NULL, 'Full', 'Yes', 'BADV', NULL, 'BAD_L', 'BAD_T', 'WrongEnum', NULL, 'Yes', NULL, 'BADID', 'Yes', NULL, 'bad_enum.csv', 'No', 'No', 1, 1);

-- Invalid negative numeric: total_weeks_req_data < 0
INSERT INTO default.ingest_config_master
(config_id, source_object_name, source_system, file_name, frequency, location, domain, sub_domain, active_flag, s3_vendor_path, source_path, s3_landing_path, s3_archive_path, delta_load_ts, full_or_incremental_load, zip_file, vendor, delimiter, source_landing, src_landing_table_name, publish_unstitched, publish_unstitched_table_name, publish_stitched, publish_stitched_table_name, primary_key, header, date_pattern, actual_file_name, vendor_file_deletion_flag, file_recursive_flag, total_weeks_req_data, total_weeks_file_data)
VALUES
(9001, 'neg_int', 'BadS', 'bad_neg.csv', 'Monthly', 'NOLOC', 'ERR', 'S', 1, 's3://bad', '/bad', 's3://bad', 's3://bad', NULL, 'Full', 'No', 'BADV', NULL, 'BAD_L', 'BAD_T', 'Yes', NULL, 'Yes', NULL, 'BADID', 'Yes', NULL, 'neg_int.csv', 'No', 'No', -2, 3);

/* ------------------------- *
 * SECTION 4: TABLE CLEANUP BEFORE MAJOR UPSERT
 * ------------------------- */

-- Clean prior test data before atomic insert/update block
DELETE FROM default.ingest_config_master;

-- Verify table is empty (should PASS)
SELECT
  CASE WHEN count(*) = 0 THEN 'PASS'
  ELSE RAISE_ERROR("FAIL: ingest_config_master is not empty before upsert")
  END AS empty_pre_upsert_assertion
FROM default.ingest_config_master;

/* ------------------------- *
 * SECTION 5: UPSERT LOGIC & TRIMMING, NULL, ENUM, TYPE HANDLING
 * ------------------------- */

-- Emulate ingest_config_data.xlsx records with inline CTE values, include blanks and whitespace
WITH excel_data AS (
  SELECT
    CAST(config_id AS INT) AS config_id,
    TRIM(source_object_name) AS source_object_name,
    TRIM(source_system) AS source_system,
    TRIM(file_name) AS file_name,
    TRIM(frequency) AS frequency,
    TRIM(location) AS location,
    TRIM(domain) AS domain,
    TRIM(sub_domain) AS sub_domain,
    CAST(TRIM(active_flag) AS INT) AS active_flag,
    TRIM(s3_vendor_path) AS s3_vendor_path,
    TRIM(source_path) AS source_path,
    TRIM(s3_landing_path) AS s3_landing_path,
    TRIM(s3_archive_path) AS s3_archive_path,
    CASE WHEN TRIM(delta_load_ts) = "" THEN NULL ELSE CAST(TRIM(delta_load_ts) AS TIMESTAMP) END AS delta_load_ts,
    TRIM(full_or_incremental_load) AS full_or_incremental_load,
    TRIM(zip_file) AS zip_file,
    TRIM(vendor) AS vendor,
    CASE WHEN TRIM(delimiter) = "" THEN NULL ELSE TRIM(delimiter) END AS delimiter,
    TRIM(source_landing) AS source_landing,
    TRIM(src_landing_table_name) AS src_landing_table_name,
    TRIM(publish_unstitched) AS publish_unstitched,
    CASE WHEN TRIM(publish_unstitched_table_name) = "" THEN NULL ELSE TRIM(publish_unstitched_table_name) END AS publish_unstitched_table_name,
    TRIM(publish_stitched) AS publish_stitched,
    CASE WHEN TRIM(publish_stitched_table_name) = "" THEN NULL ELSE TRIM(publish_stitched_table_name) END AS publish_stitched_table_name,
    TRIM(primary_key) AS primary_key,
    TRIM(header) AS header,
    CASE WHEN TRIM(date_pattern) = "" THEN NULL ELSE TRIM(date_pattern) END AS date_pattern,
    TRIM(actual_file_name) AS actual_file_name,
    TRIM(vendor_file_deletion_flag) AS vendor_file_deletion_flag,
    TRIM(file_recursive_flag) AS file_recursive_flag,
    CAST(TRIM(total_weeks_req_data) AS INT) AS total_weeks_req_data,
    CAST(TRIM(total_weeks_file_data) AS INT) AS total_weeks_file_data
  FROM (
    VALUES
      (1, 'study_metadata', 'SystemA', 'study_metadata_202403.csv', 'Daily', 'US', 'Clinical', 'Trials', '1', 's3://vendor/systema', '/data/incoming/systema', 's3://landing/systema', 's3://archive/systema', '2024-03-15 10:00:00', 'Incremental', 'No', 'VendorA', ',', 'SystemA_Landing', 'study_metadata_landing', 'Yes', 'study_metadata_unstitched', 'Yes', 'study_metadata_stitched', 'study_id', 'Yes', 'yyyy-MM-dd', 'study_metadata_20240315.csv', 'No', 'Yes', '12', '10'),
      (2, 'patient_data', 'SystemB', 'patient_data_202403.xlsx', 'Weekly', 'EU', 'Patients', 'Demographics', '1', 's3://vendor/systemb', '/data/incoming/systemb', 's3://landing/systemb', 's3://archive/systemb', '2024-03-14 09:45:00', 'Full', 'Yes', 'VendorB', ';', 'SystemB_Landing', 'patient_data_landing', 'No', '', 'Yes', 'patient_data_stitched', 'patient_id', 'Yes', 'dd-MM-yyyy', 'patient_data_14032024.xlsx', 'No', 'No', '24', '22'),
      (3, 'drug_inventory', 'SystemC', 'drug_inventory_202403.txt', 'Monthly', 'APAC', 'Inventory', 'Pharma', '1', 's3://vendor/systemc', '/data/incoming/systemc', 's3://landing/systemc', 's3://archive/systemc', '2024-03-10 08:30:00', 'Incremental', 'No', 'VendorC', '|', 'SystemC_Landing', 'drug_inventory_landing', 'Yes', '', 'Yes', 'drug_inventory_stitched', 'drug_id', 'Yes', 'yyyyMMdd', 'drug_inventory_20240310.txt', 'Yes', 'No', '6', '5'),
      (4, '  dataset_4  ', ' System4 ', ' dataset_4_202403.csv', 'Daily', 'Global', 'Category', 'SubCategory', '1', 's3://vendor/system4', '/data/incoming/system4', 's3://landing/system4', 's3://archive/system4', '2024-03-15 12:00:00', 'Incremental', 'No', 'Vendor4', ',', 'System4_Landing', 'dataset_4_landing', 'Yes', 'dataset_4_unstitched', 'Yes', 'dataset_4_stitched', 'primary_key_4', 'Yes', 'yyyy-MM-dd', 'dataset_4_20240315.csv', 'No', 'Yes', '10', '8')
    ) AS raw(
      config_id, source_object_name, source_system, file_name, frequency, location, domain, sub_domain,
      active_flag, s3_vendor_path, source_path, s3_landing_path, s3_archive_path, delta_load_ts,
      full_or_incremental_load, zip_file, vendor, delimiter, source_landing, src_landing_table_name,
      publish_unstitched, publish_unstitched_table_name, publish_stitched, publish_stitched_table_name,
      primary_key, header, date_pattern, actual_file_name, vendor_file_deletion_flag, file_recursive_flag,
      total_weeks_req_data, total_weeks_file_data
    )
)
-- Filter to only valid rows (emulate business/enum/non-null/numeric validation atomically)
, valid_excel AS (
  SELECT *
  FROM excel_data
  WHERE
    full_or_incremental_load IN ("Full", "Incremental")
    AND zip_file IN ("Yes", "No")
    AND publish_unstitched IN ("Yes", "No")
    AND publish_stitched IN ("Yes", "No")
    AND vendor_file_deletion_flag IN ("Yes", "No")
    AND file_recursive_flag IN ("Yes", "No")
    AND header IN ("Yes", "No")
    AND source_object_name <> "" AND source_system <> "" AND file_name <> "" AND frequency <> ""
    AND location <> "" AND domain <> "" AND sub_domain <> "" AND s3_vendor_path <> ""
    AND source_path <> "" AND s3_landing_path <> "" AND s3_archive_path <> ""
    AND source_landing <> "" AND src_landing_table_name <> "" AND publish_unstitched <> ""
    AND publish_stitched <> "" AND primary_key <> "" AND header <> "" AND actual_file_name <> ""
    AND (active_flag IN (0,1))
    AND (total_weeks_req_data >= 0 AND total_weeks_file_data >= 0)
    AND (delta_load_ts IS NULL OR TRY_CAST(delta_load_ts AS TIMESTAMP) IS NOT NULL)
)

-- Upsert valid data using atomic MERGE
MERGE INTO default.ingest_config_master AS tgt
USING valid_excel AS src
ON tgt.config_id = src.config_id
WHEN MATCHED THEN
  UPDATE SET
    source_object_name = src.source_object_name,
    source_system = src.source_system,
    file_name = src.file_name,
    frequency = src.frequency,
    location = src.location,
    domain = src.domain,
    sub_domain = src.sub_domain,
    active_flag = src.active_flag,
    s3_vendor_path = src.s3_vendor_path,
    source_path = src.source_path,
    s3_landing_path = src.s3_landing_path,
    s3_archive_path = src.s3_archive_path,
    delta_load_ts = src.delta_load_ts,
    full_or_incremental_load = src.full_or_incremental_load,
    zip_file = src.zip_file,
    vendor = src.vendor,
    delimiter = src.delimiter,
    source_landing = src.source_landing,
    src_landing_table_name = src.src_landing_table_name,
    publish_unstitched = src.publish_unstitched,
    publish_unstitched_table_name = src.publish_unstitched_table_name,
    publish_stitched = src.publish_stitched,
    publish_stitched_table_name = src.publish_stitched_table_name,
    primary_key = src.primary_key,
    header = src.header,
    date_pattern = src.date_pattern,
    actual_file_name = src.actual_file_name,
    vendor_file_deletion_flag = src.vendor_file_deletion_flag,
    file_recursive_flag = src.file_recursive_flag,
    total_weeks_req_data = src.total_weeks_req_data,
    total_weeks_file_data = src.total_weeks_file_data
WHEN NOT MATCHED THEN
  INSERT (
    config_id, source_object_name, source_system, file_name, frequency, location, domain, sub_domain,
    active_flag, s3_vendor_path, source_path, s3_landing_path, s3_archive_path, delta_load_ts,
    full_or_incremental_load, zip_file, vendor, delimiter, source_landing, src_landing_table_name,
    publish_unstitched, publish_unstitched_table_name, publish_stitched, publish_stitched_table_name,
    primary_key, header, date_pattern, actual_file_name, vendor_file_deletion_flag, file_recursive_flag,
    total_weeks_req_data, total_weeks_file_data
  )
  VALUES (
    src.config_id, src.source_object_name, src.source_system, src.file_name, src.frequency, src.location, src.domain, src.sub_domain,
    src.active_flag, src.s3_vendor_path, src.source_path, src.s3_landing_path, src.s3_archive_path, src.delta_load_ts,
    src.full_or_incremental_load, src.zip_file, src.vendor, src.delimiter, src.source_landing, src.src_landing_table_name,
    src.publish_unstitched, src.publish_unstitched_table_name, src.publish_stitched, src.publish_stitched_table_name,
    src.primary_key, src.header, src.date_pattern, src.actual_file_name, src.vendor_file_deletion_flag, src.file_recursive_flag,
    src.total_weeks_req_data, src.total_weeks_file_data
  );

/* ------------------------- *
 * SECTION 6: RESULT & DATA QUALITY ASSERTIONS
 * ------------------------- */

-- Assert record count (should be 4 from above inline input)
SELECT
  CASE WHEN count(*) = 4 THEN 'PASS'
  ELSE RAISE_ERROR("FAIL: Expected 4 post-upsert records")
  END AS upsert_row_count_check
FROM default.ingest_config_master;

-- Assert trimming occurred: source_object_name for config_id = 4
WITH t AS (SELECT source_object_name FROM default.ingest_config_master WHERE config_id = 4)
SELECT
  CASE WHEN source_object_name = 'dataset_4' THEN 'PASS'
       ELSE RAISE_ERROR("FAIL: source_object_name not trimmed for config_id=4")
  END AS trim_assertion
FROM t;

-- Assert blank is written as NULL: publish_unstitched_table_name for config_id = 3
WITH t AS (SELECT publish_unstitched_table_name FROM default.ingest_config_master WHERE config_id = 3)
SELECT
  CASE WHEN publish_unstitched_table_name IS NULL THEN 'PASS'
       ELSE RAISE_ERROR("FAIL: blank field not loaded as NULL")
  END AS blank_to_null_assertion
FROM t;

-- Assert specific string update: publish_stitched_table_name for config_id = 2
WITH t AS (SELECT publish_stitched_table_name FROM default.ingest_config_master WHERE config_id = 2)
SELECT
  CASE WHEN publish_stitched_table_name = 'patient_data_stitched' THEN 'PASS'
       ELSE RAISE_ERROR("FAIL: publish_stitched_table_name incorrect value for config_id=2")
  END AS update_value_assertion
FROM t;

-- Validate key data types: INT, TIMESTAMP, etc.
WITH t AS (
  SELECT
    typeof(config_id) AS tid,
    typeof(active_flag) AS taf,
    typeof(total_weeks_req_data) AS twrd,
    typeof(total_weeks_file_data) AS twfd,
    typeof(delta_load_ts) AS tts
  FROM default.ingest_config_master
  LIMIT 1
)
SELECT
  CASE
    WHEN tid = 'int' AND taf = 'int' AND twrd = 'int' AND twfd = 'int' AND tts = 'timestamp'
    THEN 'PASS'
    ELSE RAISE_ERROR("FAIL: One or more core types incorrect")
  END AS datatype_assertion
FROM t;

/* ------------------------- *
 * SECTION 7: WINDOW/ANALYTICS FUNCTIONS
 * ------------------------- */

-- Window analytics check: latest per frequency
WITH win AS (
  SELECT config_id, frequency, delta_load_ts,
    ROW_NUMBER() OVER (PARTITION BY frequency ORDER BY delta_load_ts DESC) AS rn
  FROM default.ingest_config_master
)
SELECT
  CASE WHEN count(*) >= 1 THEN 'PASS'
  ELSE RAISE_ERROR("FAIL: Window function assertion failed")
  END AS window_analytics_assertion
FROM win WHERE rn = 1;

/* ------------------------- *
 * SECTION 8: DELTA/UPDATE/DELETE VALIDATION
 * ------------------------- */

-- Run UPDATE, then check it
UPDATE default.ingest_config_master SET active_flag = 0 WHERE config_id = 2;
WITH t AS (SELECT active_flag FROM default.ingest_config_master WHERE config_id = 2)
SELECT
  CASE WHEN active_flag = 0 THEN 'PASS'
       ELSE RAISE_ERROR("FAIL: UPDATE did not set active_flag")
  END AS update_assertion
FROM t;

-- Run DELETE, assert gone
DELETE FROM default.ingest_config_master WHERE config_id = 2;
WITH t AS (SELECT count(*) AS c FROM default.ingest_config_master WHERE config_id = 2)
SELECT
  CASE WHEN c=0 THEN 'PASS'
       ELSE RAISE_ERROR("FAIL: DELETE did not remove row")
  END AS delete_assertion
FROM t;

/* ------------------------- *
 * SECTION 9: CLEAN-UP
 * ------------------------- */

DROP TABLE IF EXISTS default.ingest_config_master;
-- End of Databricks SQL test script
