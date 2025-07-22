-- Databricks SQL script: UPSERT test data generation for ingest_config_master
-- Purpose: Generate comprehensive test data and upsert into ingest_config_master for validation scenarios
-- Author: Giang Nguyen
-- Date: 2025-07-22
-- Description: This script creates a CTE with diverse, schema-compliant and edge-case records (30 total), 
-- including happy path, invalid, null, special, and boundary scenarios as per requirements.
-- All scenarios reflect the schema and rules (see comments next to values). 
-- Data is merged into ingest_config_master on config_id, inserting new or updating existing as needed.

-- CTE to define diverse test data for ingest_config_master including happy-path, edge, error, and null test cases
WITH test_data as (
  SELECT 
    -- Happy path record, base case
    1 as config_id,
    'study_metadata' as source_object_name,
    'SystemA' as source_system,
    'study_metadata_202403.csv' as file_name,
    'Daily' as frequency,
    'US' as location,
    'Clinical' as domain,
    'Trials' as sub_domain,
    1 as active_flag,
    's3://vendor/systema' as s3_vendor_path,
    '/data/incoming/systema' as source_path,
    's3://landing/systema' as s3_landing_path,
    's3://archive/systema' as s3_archive_path,
    TIMESTAMP('2024-03-15 10:00:00') as delta_load_ts,
    'Incremental' as full_or_incremental_load,
    'No' as zip_file,
    'VendorA' as vendor,
    ',' as delimiter,
    'SystemA_Landing' as source_landing,
    'study_metadata_landing' as src_landing_table_name,
    'Yes' as publish_unstitched,
    'study_metadata_unstitched' as publish_unstitched_table_name,
    'Yes' as publish_stitched,
    'study_metadata_stitched' as publish_stitched_table_name,
    'study_id' as primary_key,
    'Yes' as header,
    'yyyy-MM-dd' as date_pattern,
    'study_metadata_20240315.csv' as actual_file_name,
    'No' as vendor_file_deletion_flag,
    'Yes' as file_recursive_flag,
    12 as total_weeks_req_data,
    10 as total_weeks_file_data
  UNION ALL
    -- Valid Weekly case, edge: publish_unstitched_table_name NULL, delimiter different, date_pattern variant
    SELECT
    2, 'patient_data', 'SystemB', 'patient_data_202403.xlsx', 'Weekly', 'EU', 'Patients', 'Demographics', 1,
    's3://vendor/systemb', '/data/incoming/systemb', 's3://landing/systemb', 's3://archive/systemb',
    TIMESTAMP('2024-03-14 09:45:00'), 'Full', 'Yes', 'VendorB', ';', 'SystemB_Landing', 'patient_data_landing',
    'No', NULL, 'Yes', 'patient_data_stitched', 'patient_id', 'Yes', 'dd-MM-yyyy', 'patient_data_14032024.xlsx',
    'No', 'No', 24, 22
  UNION ALL
    -- Monthly file, edge: delimiter=|, zip_file=No, vendor_file_deletion_flag Yes
    SELECT
    3, 'drug_inventory', 'SystemC', 'drug_inventory_202403.txt', 'Monthly', 'APAC', 'Inventory', 'Pharma', 1,
    's3://vendor/systemc', '/data/incoming/systemc', 's3://landing/systemc', 's3://archive/systemc',
    TIMESTAMP('2024-03-10 08:30:00'), 'Incremental', 'No', 'VendorC', '|', 'SystemC_Landing', 'drug_inventory_landing',
    'Yes', 'drug_inventory_unstitched', 'Yes', 'drug_inventory_stitched', 'drug_id', 'Yes', 'yyyyMMdd', 'drug_inventory_20240310.txt',
    'Yes', 'No', 6, 5
  UNION ALL
    -- Ingestion with non-ASCII chars in object name, special char in location
    SELECT
    4, '研究_Metadata', 'SysΔ', 'ファイル_202403.csv', 'Monthly', '上海', '研究', '试验', 0, 
    's3://vendor/qíxìng', '/data/页/incoming/qíxìng', 's3://landing/qíxìng', 's3://archive/qíxìng',
    TIMESTAMP('2024-03-10 23:59:59'), 'Full', 'Yes', '供應商Z', '^', 'Shanghai_Landing', 'research_landing',
    'Yes', 'metadata_unstitched', 'Yes', 'metadata_stitched', '研究_id', 'No', 'yyyy/MM/dd', 'metadata_20240310.csv',
    'No', 'Yes', 0, 0
  UNION ALL
    -- Edge: publish_stitched_table_name is NULL (optional), delta_load_ts NULL
    SELECT
    5, 'partial_table', 'SysE', 'partial.csv', 'Monthly', 'Asia', 'Finance', 'Banking', 1,
    's3://vendor/finance', '/data/finance', 's3://landing/finance', 's3://archive/finance',
    NULL, 'Full', 'No', 'VendorE', ':', 'Finance_Landing', 'partial_landing',
    'No', 'partial_unstitched', 'Yes', NULL, 'acct_id', 'Yes', NULL, 'partial_202403.csv',
    'No', 'No', 5, 5
  UNION ALL
    -- File name with special, embedded chars
    SELECT
    6, 'edge_file', 'EdgeSys', 'edge,comma.csv', 'Weekly', 'FR', 'Test', 'Sub', 1,
    's3://vendor/edge', '/data/edge', 's3://landing/edge', 's3://archive/edge',
    TIMESTAMP('2024-03-22 00:00:00'), 'Full', 'No', 'EdgeVendor', NULL, 'Edge_Landing', 'edge_landing',
    'Yes', NULL, 'No', NULL, 'eid', 'No', NULL, 'edge_2024_03.csv',
    'No', 'Yes', 1, 1
  UNION ALL
    -- Edge: All optional fields set to NULL, empty strings for nullable columns
    SELECT
    7, 'null_edge', 'NullSys', 'null_edge.csv', 'Monthly', 'Global', 'Null', 'Nullity', 1,
    's3://vendor/null', '/data/incoming/null', 's3://landing/null', 's3://archive/null',
    NULL, 'Full', 'Yes', 'NullVendor', NULL, 'Null_Landing', 'null_landing',
    'No', '', 'No', '', 'nid', 'Yes', NULL, 'null_edge.csv',
    'Yes', 'No', 3, 3
  UNION ALL
    -- Boundary: config_id = max int32 + 1, total_weeks_req_data = 0, publish flags 'No'
    SELECT
    2147483648, 'limittest', 'LimSys', 'limittest.csv', 'Monthly', 'Antarctica', 'Limits', 'Test', 0,
    's3://vendor/lim', '/data/incoming/lim', 's3://landing/lim', 's3://archive/lim',
    NULL, 'Incremental', 'No', 'LimVendor', NULL, 'Lim_Landing', 'lim_landing',
    'No', NULL, 'No', NULL, 'lid', 'No', NULL, 'limit_file.csv',
    'No', 'No', 0, 0
  UNION ALL
    -- Error case: config_id duplicate in same dataset
    SELECT
    8, 'dup_id', 'DupSys', 'dup_file1.csv', 'Daily', 'UK', 'Dup', 'Test', 1,
    's3://vendor/dup', '/data/incoming/dup', 's3://landing/dup', 's3://archive/dup',
    TIMESTAMP('2024-03-20 14:00:00'), 'Incremental', 'No', 'DupVendor', '!', 'Dup_Landing', 'dup_landing',
    'Yes', 'dup_unstitched', 'Yes', 'dup_stitched', 'dup_id', 'Yes', 'yyyy-MM-dd', 'dup_file1.csv',
    'No', 'Yes', 9, 8
  UNION ALL
    -- Error case: duplicate config_id again, should trigger validation
    SELECT
    8, 'dup_id2', 'DupSys', 'dup_file2.csv', 'Daily', 'UK', 'Dup', 'Test', 1,
    's3://vendor/dup', '/data/incoming/dup', 's3://landing/dup', 's3://archive/dup',
    TIMESTAMP('2024-03-20 15:00:00'), 'Full', 'Yes', 'DupVendor', NULL, 'Dup_Landing', 'dup_landing2',
    'No', NULL, 'No', NULL, 'dup_id2', 'No', NULL, 'dup_file2.csv',
    'No', 'No', 9, 7
  UNION ALL
    -- Error record: invalid active_flag (not 0/1)
    SELECT
    9, 'invalid_flag', 'BadSys', 'badflag.csv', 'Daily', 'CA', 'Flag', 'Invalid', 2,
    's3://vendor/bad', '/data/incoming/bad', 's3://landing/bad', 's3://archive/bad',
    TIMESTAMP('2024-03-12 09:00:00'), 'Full', 'No', 'BadVendor', '*', 'Bad_Landing', 'bad_landing',
    'Yes', 'bad_unstitched', 'Yes', 'bad_stitched', 'bad_id', 'Yes', 'yyyy-MM-dd', 'bad.csv',
    'No', 'Yes', 2, 0
  UNION ALL
    -- Error: invalid ENUM value for publish_unstitched (not Yes/No)
    SELECT
    10, 'enum_invalid', 'ESys', 'enum_invalid.csv', 'Daily', 'DE', 'Domain', 'Edge', 1,
    's3://vendor/enum', '/data/incoming/enum', 's3://landing/enum', 's3://archive/enum',
    TIMESTAMP('2024-03-16 14:45:00'), 'Full', 'No', 'EnumVendor', NULL, 'Enum_Landing', 'enum_landing',
    'MAYBE', 'enum_unstitched', 'Yes', 'enum_stitched', 'enum_id', 'No', NULL, 'enum_invalid.csv',
    'No', 'No', 4, 2
  UNION ALL
    -- Error: invalid full_or_incremental_load (not Full/Incremental)
    SELECT
    11, 'enum_invalid2', 'ESys', 'enum_invalid2.csv', 'Weekly', 'JP', 'Domain', 'Edge', 1,
    's3://vendor/enum2', '/data/incoming/enum2', 's3://landing/enum2', 's3://archive/enum2',
    TIMESTAMP('2024-03-17 10:00:00'), 'Partial', 'No', 'EnumVendor2', NULL, 'Enum2_Landing', 'enum2_landing',
    'Yes', 'enum2_unstitched', 'Yes', 'enum2_stitched', 'enum2_id', 'No', NULL, 'enum_invalid2.csv',
    'No', 'No', 5, 2
  UNION ALL
    -- Error: invalid vendor_file_deletion_flag (not Yes/No)
    SELECT
    12, 'invalid_del', 'VSys', 'invalid_del.csv', 'Monthly', 'BR', 'Domain', 'Delete', 1,
    's3://vendor/invdel', '/data/incoming/invdel', 's3://landing/invdel', 's3://archive/invdel',
    TIMESTAMP('2024-03-18 12:34:00'), 'Full', 'No', 'DelVendor', NULL, 'Del_Landing', 'del_landing',
    'Yes', 'del_unstitched', 'Yes', 'del_stitched', 'del_id', 'No', NULL, 'invalid_del.csv',
    'TRUE', 'Yes', 7, 4
  UNION ALL
    -- Error: negative total_weeks_req_data
    SELECT
    13, 'bad_week', 'NegSys', 'neg_week.csv', 'Monthly', 'AU', 'Domain', 'Bad', 1,
    's3://vendor/badweek', '/data/incoming/badweek', 's3://landing/badweek', 's3://archive/badweek',
    TIMESTAMP('2024-03-19 22:22:00'), 'Incremental', 'No', 'NegVendor', NULL, 'Neg_Landing', 'neg_landing',
    'Yes', 'neg_unstitched', 'Yes', 'neg_stitched', 'neg_id', 'Yes', 'yyyy-MM-dd', 'neg_week.csv',
    'No', 'Yes', -1, 1
  UNION ALL
    -- Error: invalid timestamp format (should cause failure)
    SELECT
    14, 'bad_timestamp', 'TSys', 'bad_ts.csv', 'Weekly', 'IN', 'Domain', 'TS', 1,
    's3://vendor/ts', '/data/incoming/ts', 's3://landing/ts', 's3://archive/ts',
    '15-04-2024 10:00' as delta_load_ts, 'Incremental', 'No', 'TSVendor', NULL, 'TS_Landing', 'ts_landing',
    'Yes', 'ts_unstitched', 'Yes', 'ts_stitched', 'ts_id', 'No', NULL, 'ts.csv',
    'No', 'Yes', 3, 1
  UNION ALL
    -- Error: missing required field (primary_key=NULL)
    SELECT
    15, 'missingkey', 'KeySys', 'missing_key.csv', 'Daily', 'US', 'Domain', 'Key', 1,
    's3://vendor/key', '/data/incoming/key', 's3://landing/key', 's3://archive/key',
    TIMESTAMP('2024-03-21 20:21:00'), 'Full', 'No', 'KeyVendor', NULL, 'Key_Landing', 'key_landing',
    'Yes', 'key_unstitched', 'Yes', 'key_stitched', NULL, 'Yes', 'yyyy-MM-dd', 'missing_key.csv',
    'No', 'Yes', 1, 1
  UNION ALL
    -- Whitespace to be trimmed: leading/trailing spaces on object system/fields
    SELECT
    16, '  trim_spaces  ', '  SysTrim   ', '  trimfile.csv ', 'Daily', ' BE ', 'Data  ', '  Sub  ', 1,
    ' s3://vendor/trim ', ' /data/incoming/trim ', ' s3://landing/trim ', ' s3://archive/trim ',
    TIMESTAMP('2024-03-22 09:30:00'), 'Full', 'No', ' TrimVendor ', NULL, ' Trim_Landing ', ' trim_landing',
    'Yes', ' trim_unstitched ', 'Yes', ' trim_stitched ', ' tid ', 'Yes', ' yyyy-MM-dd ', ' trimfile.csv ',
    'No', 'Yes', 2, 2
  UNION ALL
    -- Edge: minimal numeric field values >0
    SELECT
    17, 'min_numeric', 'NumSys', 'num.csv', 'Weekly', 'ZA', 'Domain', 'Num', 1,
    's3://vendor/num', '/data/incoming/num', 's3://landing/num', 's3://archive/num',
    TIMESTAMP('2024-03-01 01:01:01'), 'Incremental', 'Yes', 'NumVendor', '|', 'Num_Landing', 'num_landing',
    'No', NULL, 'No', NULL, 'nid', 'Yes', NULL, 'num.csv',
    'No', 'No', 1, 0
  UNION ALL
    -- Edge: Unicode/emoji in fields
    SELECT
    18, 'emojî_😀', 'Sys🎉', 'emoji.csv', 'Daily', 'US', '🎯', '🌍', 1,
    's3://vendor/emoji', '/data/incoming/emoji', 's3://landing/emoji', 's3://archive/emoji',
    TIMESTAMP('2024-03-05 05:05:05'), 'Full', 'No', 'Vendor😀', '😊', 'Emoji_Landing', 'emoji_landing',
    'Yes', 'emoji_unstitched', 'Yes', 'emoji_stitched', 'emoji_id', 'No', NULL, 'emoji.csv',
    'No', 'Yes', 10, 5
  UNION ALL
    -- All happy path, top rows from ingest_config_data.xlsx (testing bulk insert)
    SELECT
    19, 'dataset_19', 'System19', 'dataset_19_202403.csv', 'Daily', 'Global', 'Category', 'SubCategory', 1,
    's3://vendor/system19', '/data/incoming/system19', 's3://landing/system19', 's3://archive/system19',
    TIMESTAMP('2024-03-15 12:00:00'), 'Incremental', 'No', 'Vendor19', ',', 'System19_Landing', 'dataset_19_landing',
    'Yes', 'dataset_19_unstitched', 'Yes', 'dataset_19_stitched', 'primary_key_19', 'Yes', 'yyyy-MM-dd', 'dataset_19_20240315.csv',
    'No', 'Yes', 10, 8
  UNION ALL
    SELECT
    20, 'dataset_20', 'System20', 'dataset_20_202403.csv', 'Daily', 'Global', 'Category', 'SubCategory', 1,
    's3://vendor/system20', '/data/incoming/system20', 's3://landing/system20', 's3://archive/system20',
    TIMESTAMP('2024-03-15 12:00:00'), 'Incremental', 'No', 'Vendor20', ',', 'System20_Landing', 'dataset_20_landing',
    'Yes', 'dataset_20_unstitched', 'Yes', 'dataset_20_stitched', 'primary_key_20', 'Yes', 'yyyy-MM-dd', 'dataset_20_20240315.csv',
    'No', 'Yes', 10, 8
  UNION ALL
    SELECT
    21, 'dataset_21', 'System21', 'dataset_21_202403.csv', 'Daily', 'Global', 'Category', 'SubCategory', 1,
    's3://vendor/system21', '/data/incoming/system21', 's3://landing/system21', 's3://archive/system21',
    TIMESTAMP('2024-03-15 12:00:00'), 'Incremental', 'No', 'Vendor21', ',', 'System21_Landing', 'dataset_21_landing',
    'Yes', 'dataset_21_unstitched', 'Yes', 'dataset_21_stitched', 'primary_key_21', 'Yes', 'yyyy-MM-dd', 'dataset_21_20240315.csv',
    'No', 'Yes', 10, 8
  UNION ALL
    SELECT
    22, 'dataset_22', 'System22', 'dataset_22_202403.csv', 'Daily', 'Global', 'Category', 'SubCategory', 1,
    's3://vendor/system22', '/data/incoming/system22', 's3://landing/system22', 's3://archive/system22',
    TIMESTAMP('2024-03-15 12:00:00'), 'Incremental', 'No', 'Vendor22', ',', 'System22_Landing', 'dataset_22_landing',
    'Yes', 'dataset_22_unstitched', 'Yes', 'dataset_22_stitched', 'primary_key_22', 'Yes', 'yyyy-MM-dd', 'dataset_22_20240315.csv',
    'No', 'Yes', 10, 8
  UNION ALL
    SELECT
    23, 'dataset_23', 'System23', 'dataset_23_202403.csv', 'Daily', 'Global', 'Category', 'SubCategory', 1,
    's3://vendor/system23', '/data/incoming/system23', 's3://landing/system23', 's3://archive/system23',
    TIMESTAMP('2024-03-15 12:00:00'), 'Incremental', 'No', 'Vendor23', ',', 'System23_Landing', 'dataset_23_landing',
    'Yes', 'dataset_23_unstitched', 'Yes', 'dataset_23_stitched', 'primary_key_23', 'Yes', 'yyyy-MM-dd', 'dataset_23_20240315.csv',
    'No', 'Yes', 10, 8
  UNION ALL
    SELECT
    24, 'dataset_24', 'System24', 'dataset_24_202403.csv', 'Daily', 'Global', 'Category', 'SubCategory', 1,
    's3://vendor/system24', '/data/incoming/system24', 's3://landing/system24', 's3://archive/system24',
    TIMESTAMP('2024-03-15 12:00:00'), 'Incremental', 'No', 'Vendor24', ',', 'System24_Landing', 'dataset_24_landing',
    'Yes', 'dataset_24_unstitched', 'Yes', 'dataset_24_stitched', 'primary_key_24', 'Yes', 'yyyy-MM-dd', 'dataset_24_20240315.csv',
    'No', 'Yes', 10, 8
  UNION ALL
    SELECT
    25, 'dataset_25', 'System25', 'dataset_25_202403.csv', 'Daily', 'Global', 'Category', 'SubCategory', 1,
    's3://vendor/system25', '/data/incoming/system25', 's3://landing/system25', 's3://archive/system25',
    TIMESTAMP('2024-03-15 12:00:00'), 'Incremental', 'No', 'Vendor25', ',', 'System25_Landing', 'dataset_25_landing',
    'Yes', 'dataset_25_unstitched', 'Yes', 'dataset_25_stitched', 'primary_key_25', 'Yes', 'yyyy-MM-dd', 'dataset_25_20240315.csv',
    'No', 'Yes', 10, 8
  UNION ALL
    SELECT
    26, 'dataset_26', 'System26', 'dataset_26_202403.csv', 'Daily', 'Global', 'Category', 'SubCategory', 1,
    's3://vendor/system26', '/data/incoming/system26', 's3://landing/system26', 's3://archive/system26',
    TIMESTAMP('2024-03-15 12:00:00'), 'Incremental', 'No', 'Vendor26', ',', 'System26_Landing', 'dataset_26_landing',
    'Yes', 'dataset_26_unstitched', 'Yes', 'dataset_26_stitched', 'primary_key_26', 'Yes', 'yyyy-MM-dd', 'dataset_26_20240315.csv',
    'No', 'Yes', 10, 8
  UNION ALL
    SELECT
    27, 'dataset_27', 'System27', 'dataset_27_202403.csv', 'Daily', 'Global', 'Category', 'SubCategory', 1,
    's3://vendor/system27', '/data/incoming/system27', 's3://landing/system27', 's3://archive/system27',
    TIMESTAMP('2024-03-15 12:00:00'), 'Incremental', 'No', 'Vendor27', ',', 'System27_Landing', 'dataset_27_landing',
    'Yes', 'dataset_27_unstitched', 'Yes', 'dataset_27_stitched', 'primary_key_27', 'Yes', 'yyyy-MM-dd', 'dataset_27_20240315.csv',
    'No', 'Yes', 10, 8
  UNION ALL
    SELECT
    28, 'dataset_28', 'System28', 'dataset_28_202403.csv', 'Daily', 'Global', 'Category', 'SubCategory', 1,
    's3://vendor/system28', '/data/incoming/system28', 's3://landing/system28', 's3://archive/system28',
    TIMESTAMP('2024-03-15 12:00:00'), 'Incremental', 'No', 'Vendor28', ',', 'System28_Landing', 'dataset_28_landing',
    'Yes', 'dataset_28_unstitched', 'Yes', 'dataset_28_stitched', 'primary_key_28', 'Yes', 'yyyy-MM-dd', 'dataset_28_20240315.csv',
    'No', 'Yes', 10, 8
  UNION ALL
    SELECT
    29, 'dataset_29', 'System29', 'dataset_29_202403.csv', 'Daily', 'Global', 'Category', 'SubCategory', 1,
    's3://vendor/system29', '/data/incoming/system29', 's3://landing/system29', 's3://archive/system29',
    TIMESTAMP('2024-03-15 12:00:00'), 'Incremental', 'No', 'Vendor29', ',', 'System29_Landing', 'dataset_29_landing',
    'Yes', 'dataset_29_unstitched', 'Yes', 'dataset_29_stitched', 'primary_key_29', 'Yes', 'yyyy-MM-dd', 'dataset_29_20240315.csv',
    'No', 'Yes', 10, 8
  UNION ALL
    SELECT
    30, 'dataset_30', 'System30', 'dataset_30_202403.csv', 'Daily', 'Global', 'Category', 'SubCategory', 1,
    's3://vendor/system30', '/data/incoming/system30', 's3://landing/system30', 's3://archive/system30',
    TIMESTAMP('2024-03-15 12:00:00'), 'Incremental', 'No', 'Vendor30', ',', 'System30_Landing', 'dataset_30_landing',
    'Yes', 'dataset_30_unstitched', 'Yes', 'dataset_30_stitched', 'primary_key_30', 'Yes', 'yyyy-MM-dd', 'dataset_30_20240315.csv',
    'No', 'Yes', 10, 8
)
-- Merge statement for upsert operation (insert if new config_id, update if exists)
-- This ensures atomicity for the batch as per Databricks SQL semantics
MERGE INTO ingest_config_master as tgt
USING test_data as src
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
        tgt.delta_load_ts = CAST(src.delta_load_ts AS TIMESTAMP),
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
        delta_load_ts,
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
    )
    VALUES (
        src.config_id,
        src.source_object_name,
        src.source_system,
        src.file_name,
        src.frequency,
        src.location,
        src.domain,
        src.sub_domain,
        src.active_flag,
        src.s3_vendor_path,
        src.source_path,
        src.s3_landing_path,
        src.s3_archive_path,
        CAST(src.delta_load_ts AS TIMESTAMP),
        src.full_or_incremental_load,
        src.zip_file,
        src.vendor,
        src.delimiter,
        src.source_landing,
        src.src_landing_table_name,
        src.publish_unstitched,
        src.publish_unstitched_table_name,
        src.publish_stitched,
        src.publish_stitched_table_name,
        src.primary_key,
        src.header,
        src.date_pattern,
        src.actual_file_name,
        src.vendor_file_deletion_flag,
        src.file_recursive_flag,
        src.total_weeks_req_data,
        src.total_weeks_file_data
    );
-- End of Databricks SQL script for ingest_config_master test UPSERTs
