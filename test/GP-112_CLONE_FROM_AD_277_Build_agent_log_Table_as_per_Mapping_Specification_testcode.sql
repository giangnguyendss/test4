USE CATALOG purgo_databricks;

/* 
===============================================================================
Databricks SQL Test Suite for Agent Log Calculated Field Query
===============================================================================
Unity Catalog: purgo_databricks
Schema: purgo_playground
Target Table: agent_log
Test Coverage: Schema validation, calculated field logic, error handling, data type conversion, NULL handling, unique key generation, hardcoded fields, edge cases, performance, data quality
===============================================================================
*/

/*-----------------------------------------------------------------------------
SECTION: Setup - Ensure Clean Test Environment
-----------------------------------------------------------------------------*/

-- Drop and recreate agent_log table to ensure schema correctness and no column mismatch
DROP TABLE IF EXISTS purgo_playground.agent_log;

CREATE TABLE purgo_playground.agent_log (
  unix_id STRING,
  agent_name STRING,
  log_date DATE,
  agent_first_login TIMESTAMP,
  agent_first_logout TIMESTAMP,
  total_login_time_hrs STRING,
  agent_lunch_login TIMESTAMP,
  agent_lunch_logout TIMESTAMP,
  agent_lunch_duration STRING,
  agent_city STRING,
  agent_state STRING,
  agent_country STRING,
  agent_zip_code STRING,
  data_loaded_at TIMESTAMP,
  agent_key STRING,
  CONSTRAINT agent_city_values CHECK (agent_city IN ("New York","Néw Yørk","東京") OR agent_city IS NULL),
  CONSTRAINT agent_state_values CHECK (agent_state IN ("NY","N¥","東京都") OR agent_state IS NULL),
  CONSTRAINT agent_country_values CHECK (agent_country IN ("USA","U$A","日本") OR agent_country IS NULL),
  CONSTRAINT agent_zip_code_values CHECK (agent_zip_code IN ("10001","1000!@#","〒100-0001") OR agent_zip_code IS NULL)
);

-- Validate schema: Ensure column count matches specification
SELECT 
  COUNT(*) AS column_count
FROM 
  information_schema.columns
WHERE 
  table_catalog = "purgo_databricks"
  AND table_schema = "purgo_playground"
  AND table_name = "agent_log";
-- Expect: 15 columns

-- Assert column count is 15
WITH col_count AS (
  SELECT 
    COUNT(*) AS actual_col_count
  FROM 
    information_schema.columns
  WHERE 
    table_catalog = "purgo_databricks"
    AND table_schema = "purgo_playground"
    AND table_name = "agent_log"
)
SELECT 
  CASE WHEN actual_col_count = 15 THEN "PASS" ELSE "FAIL" END AS schema_column_count_test
FROM col_count;

/*-----------------------------------------------------------------------------
SECTION: Insert Test Data - Use Provided Test Data CTE
-----------------------------------------------------------------------------*/

-- Insert test data into agent_log table
INSERT INTO purgo_playground.agent_log
SELECT * FROM (
  WITH test_agent_log_data AS (
    SELECT
      'IU001' AS unix_id,
      'John Doe' AS agent_name,
      DATE('2025-01-02') AS log_date,
      TIMESTAMP('2025-01-02T08:00:00.000+0000') AS agent_first_login,
      TIMESTAMP('2025-01-02T17:00:00.000+0000') AS agent_first_logout,
      '9.00' AS total_login_time_hrs,
      NULL AS agent_lunch_login,
      NULL AS agent_lunch_logout,
      NULL AS agent_lunch_duration,
      'New York' AS agent_city,
      'NY' AS agent_state,
      'USA' AS agent_country,
      '10001' AS agent_zip_code,
      CURRENT_TIMESTAMP() AS data_loaded_at,
      sha2(concat_ws('|',
        'IU001','John Doe','2025-01-02','2025-01-02T08:00:00.000+0000','2025-01-02T17:00:00.000+0000','9.00','New York','NY','USA','10001'
      ),256) AS agent_key
    UNION ALL
    SELECT
      'IU002','Jane Smith',DATE('2025-01-03'),
      TIMESTAMP('2025-01-03T09:15:00.000+0000'),TIMESTAMP('2025-01-03T18:45:00.000+0000'),
      '9.50',NULL,NULL,NULL,'New York','NY','USA','10001',CURRENT_TIMESTAMP(),
      sha2(concat_ws('|',
        'IU002','Jane Smith','2025-01-03','2025-01-03T09:15:00.000+0000','2025-01-03T18:45:00.000+0000','9.50','New York','NY','USA','10001'
      ),256)
    UNION ALL
    SELECT
      'IU001A','John D.',DATE('2025-01-02'),
      TIMESTAMP('2025-01-02T08:00:00.000+0000'),TIMESTAMP('2025-01-02T17:00:00.000+0000'),
      '9.00',NULL,NULL,NULL,'New York','NY','USA','10001',CURRENT_TIMESTAMP(),
      sha2(concat_ws('|',
        'IU001A','John D.','2025-01-02','2025-01-02T08:00:00.000+0000','2025-01-02T17:00:00.000+0000','9.00','New York','NY','USA','10001'
      ),256)
    UNION ALL
    SELECT
      NULL,NULL,DATE('2025-01-04'),
      TIMESTAMP('2025-01-04T08:00:00.000+0000'),TIMESTAMP('2025-01-04T17:00:00.000+0000'),
      '9.00',NULL,NULL,NULL,'New York','NY','USA','10001',CURRENT_TIMESTAMP(),
      sha2(concat_ws('|',
        NULL,NULL,'2025-01-04','2025-01-04T08:00:00.000+0000','2025-01-04T17:00:00.000+0000','9.00','New York','NY','USA','10001'
      ),256)
    UNION ALL
    SELECT
      NULL,NULL,DATE('2025-01-05'),
      TIMESTAMP('2025-01-05T08:00:00.000+0000'),TIMESTAMP('2025-01-05T17:00:00.000+0000'),
      '9.00',NULL,NULL,NULL,'New York','NY','USA','10001',CURRENT_TIMESTAMP(),
      sha2(concat_ws('|',
        NULL,NULL,'2025-01-05','2025-01-05T08:00:00.000+0000','2025-01-05T17:00:00.000+0000','9.00','New York','NY','USA','10001'
      ),256)
    UNION ALL
    SELECT
      'IU005','Bob Agent',DATE('2025-01-06'),
      TIMESTAMP('2025-01-06T07:55:00.000+0000'),TIMESTAMP('2025-01-06T18:00:00.000+0000'),
      '10.08',NULL,NULL,NULL,'New York','NY','USA','10001',CURRENT_TIMESTAMP(),
      sha2(concat_ws('|',
        'IU005','Bob Agent','2025-01-06','2025-01-06T07:55:00.000+0000','2025-01-06T18:00:00.000+0000','10.08','New York','NY','USA','10001'
      ),256)
    UNION ALL
    SELECT
      'IU006','Null End',DATE('2025-01-07'),
      TIMESTAMP('2025-01-07T08:00:00.000+0000'),NULL,
      NULL,NULL,NULL,NULL,'New York','NY','USA','10001',CURRENT_TIMESTAMP(),
      sha2(concat_ws('|',
        'IU006','Null End','2025-01-07','2025-01-07T08:00:00.000+0000',NULL,NULL,'New York','NY','USA','10001'
      ),256)
    UNION ALL
    SELECT
      'IU007','Alice Lee',DATE('2025-01-08'),
      TIMESTAMP('2025-01-08T08:00:00.000+0000'),TIMESTAMP('2025-01-08T17:00:00.000+0000'),
      '9.00',NULL,NULL,NULL,'New York','NY','USA','10001',CURRENT_TIMESTAMP(),
      sha2(concat_ws('|',
        'IU007','Alice Lee','2025-01-08','2025-01-08T08:00:00.000+0000','2025-01-08T17:00:00.000+0000','9.00','New York','NY','USA','10001'
      ),256)
    UNION ALL
    SELECT
      'IU009','Format Test',DATE('2025-01-09'),
      TIMESTAMP('2025-01-09T08:00:00.000+0000'),TIMESTAMP('2025-01-09T17:00:00.000+0000'),
      '9.00',NULL,NULL,NULL,'New York','NY','USA','10001',CURRENT_TIMESTAMP(),
      sha2(concat_ws('|',
        'IU009','Format Test','2025-01-09','2025-01-09T08:00:00.000+0000','2025-01-09T17:00:00.000+0000','9.00','New York','NY','USA','10001'
      ),256)
    UNION ALL
    SELECT
      'IU010','Location Test',DATE('2025-01-10'),
      TIMESTAMP('2025-01-10T08:00:00.000+0000'),TIMESTAMP('2025-01-10T17:00:00.000+0000'),
      '9.00',NULL,NULL,NULL,'New York','NY','USA','10001',CURRENT_TIMESTAMP(),
      sha2(concat_ws('|',
        'IU010','Location Test','2025-01-10','2025-01-10T08:00:00.000+0000','2025-01-10T17:00:00.000+0000','9.00','New York','NY','USA','10001'
      ),256)
    UNION ALL
    SELECT
      'IU011','Timestamp Test',DATE('2025-01-11'),
      TIMESTAMP('2025-01-11T08:00:00.000+0000'),TIMESTAMP('2025-01-11T17:00:00.000+0000'),
      '9.00',NULL,NULL,NULL,'New York','NY','USA','10001',TIMESTAMP('2025-01-11T12:34:56.000+0000'),
      sha2(concat_ws('|',
        'IU011','Timestamp Test','2025-01-11','2025-01-11T08:00:00.000+0000','2025-01-11T17:00:00.000+0000','9.00','New York','NY','USA','10001'
      ),256)
    UNION ALL
    SELECT
      'IU008B','Agent Eight B',DATE('2025-01-10'),
      TIMESTAMP('2025-01-10T08:00:00.000+0000'),TIMESTAMP('2025-01-10T17:00:00.000+0000'),
      '9.00',NULL,NULL,NULL,'New York','NY','USA','10001',CURRENT_TIMESTAMP(),
      sha2(concat_ws('|',
        'IU008B','Agent Eight B','2025-01-10','2025-01-10T08:00:00.000+0000','2025-01-10T17:00:00.000+0000','9.00','New York','NY','USA','10001'
      ),256)
    UNION ALL
    SELECT
      NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL
    UNION ALL
    SELECT
      'IU012','Jöhn Döe 🚀',DATE('2025-01-12'),
      TIMESTAMP('2025-01-12T08:00:00.000+0000'),TIMESTAMP('2025-01-12T17:00:00.000+0000'),
      '9.00',NULL,NULL,NULL,'New York','NY','USA','10001',CURRENT_TIMESTAMP(),
      sha2(concat_ws('|',
        'IU012','Jöhn Döe 🚀','2025-01-12','2025-01-12T08:00:00.000+0000','2025-01-12T17:00:00.000+0000','9.00','New York','NY','USA','10001'
      ),256)
    UNION ALL
    SELECT
      'IU013','李四',DATE('2025-01-13'),
      TIMESTAMP('2025-01-13T08:00:00.000+0000'),TIMESTAMP('2025-01-13T17:00:00.000+0000'),
      '9.00',NULL,NULL,NULL,'New York','NY','USA','10001',CURRENT_TIMESTAMP(),
      sha2(concat_ws('|',
        'IU013','李四','2025-01-13','2025-01-13T08:00:00.000+0000','2025-01-13T17:00:00.000+0000','9.00','New York','NY','USA','10001'
      ),256)
    UNION ALL
    SELECT
      'IU014','Future Agent',DATE('2099-12-31'),
      TIMESTAMP('2099-12-31T08:00:00.000+0000'),TIMESTAMP('2099-12-31T17:00:00.000+0000'),
      '9.00',NULL,NULL,NULL,'New York','NY','USA','10001',CURRENT_TIMESTAMP(),
      sha2(concat_ws('|',
        'IU014','Future Agent','2099-12-31','2099-12-31T08:00:00.000+0000','2099-12-31T17:00:00.000+0000','9.00','New York','NY','USA','10001'
      ),256)
    UNION ALL
    SELECT
      'IU015','Null Login',DATE('2025-01-15'),
      NULL,TIMESTAMP('2025-01-15T17:00:00.000+0000'),
      NULL,NULL,NULL,NULL,'New York','NY','USA','10001',CURRENT_TIMESTAMP(),
      sha2(concat_ws('|',
        'IU015','Null Login','2025-01-15',NULL,'2025-01-15T17:00:00.000+0000',NULL,'New York','NY','USA','10001'
      ),256)
    UNION ALL
    SELECT
      'IU016','Null Logout',DATE('2025-01-16'),
      TIMESTAMP('2025-01-16T08:00:00.000+0000'),NULL,
      NULL,NULL,NULL,NULL,'New York','NY','USA','10001',CURRENT_TIMESTAMP(),
      sha2(concat_ws('|',
        'IU016','Null Logout','2025-01-16','2025-01-16T08:00:00.000+0000',NULL,NULL,'New York','NY','USA','10001'
      ),256)
    UNION ALL
    SELECT
      'IU017','Null Duration',DATE('2025-01-17'),
      TIMESTAMP('2025-01-17T08:00:00.000+0000'),TIMESTAMP('2025-01-17T17:00:00.000+0000'),
      NULL,NULL,NULL,NULL,'New York','NY','USA','10001',CURRENT_TIMESTAMP(),
      sha2(concat_ws('|',
        'IU017','Null Duration','2025-01-17','2025-01-17T08:00:00.000+0000','2025-01-17T17:00:00.000+0000',NULL,'New York','NY','USA','10001'
      ),256)
    UNION ALL
    SELECT
      'IU018','Null Location',DATE('2025-01-18'),
      TIMESTAMP('2025-01-18T08:00:00.000+0000'),TIMESTAMP('2025-01-18T17:00:00.000+0000'),
      '9.00',NULL,NULL,NULL,NULL,NULL,NULL,NULL,CURRENT_TIMESTAMP(),
      sha2(concat_ws('|',
        'IU018','Null Location','2025-01-18','2025-01-18T08:00:00.000+0000','2025-01-18T17:00:00.000+0000','9.00',NULL,NULL,NULL,NULL
      ),256)
    UNION ALL
    SELECT
      'IU019','Lunch Agent',DATE('2025-01-19'),
      TIMESTAMP('2025-01-19T08:00:00.000+0000'),TIMESTAMP('2025-01-19T17:00:00.000+0000'),
      '9.00',TIMESTAMP('2025-01-19T12:00:00.000+0000'),TIMESTAMP('2025-01-19T12:30:00.000+0000'),'0.50','New York','NY','USA','10001',CURRENT_TIMESTAMP(),
      sha2(concat_ws('|',
        'IU019','Lunch Agent','2025-01-19','2025-01-19T08:00:00.000+0000','2025-01-19T17:00:00.000+0000','9.00','New York','NY','USA','10001'
      ),256)
    UNION ALL
    SELECT
      'IU020','Special Loc',DATE('2025-01-20'),
      TIMESTAMP('2025-01-20T08:00:00.000+0000'),TIMESTAMP('2025-01-20T17:00:00.000+0000'),
      '9.00',NULL,NULL,NULL,'Néw Yørk','N¥','U$A','1000!@#',CURRENT_TIMESTAMP(),
      sha2(concat_ws('|',
        'IU020','Special Loc','2025-01-20','2025-01-20T08:00:00.000+0000','2025-01-20T17:00:00.000+0000','9.00','Néw Yørk','N¥','U$A','1000!@#'
      ),256)
    UNION ALL
    SELECT
      'IU021','MultiByte Loc',DATE('2025-01-21'),
      TIMESTAMP('2025-01-21T08:00:00.000+0000'),TIMESTAMP('2025-01-21T17:00:00.000+0000'),
      '9.00',NULL,NULL,NULL,'東京','東京都','日本','〒100-0001',CURRENT_TIMESTAMP(),
      sha2(concat_ws('|',
        'IU021','MultiByte Loc','2025-01-21','2025-01-21T08:00:00.000+0000','2025-01-21T17:00:00.000+0000','9.00','東京','東京都','日本','〒100-0001'
      ),256)
    UNION ALL
    SELECT
      'IU022','Null Loaded',DATE('2025-01-22'),
      TIMESTAMP('2025-01-22T08:00:00.000+0000'),TIMESTAMP('2025-01-22T17:00:00.000+0000'),
      '9.00',NULL,NULL,NULL,'New York','NY','USA','10001',NULL,
      sha2(concat_ws('|',
        'IU022','Null Loaded','2025-01-22','2025-01-22T08:00:00.000+0000','2025-01-22T17:00:00.000+0000','9.00','New York','NY','USA','10001'
      ),256)
    UNION ALL
    SELECT
      'IU023','Null Key',DATE('2025-01-23'),
      TIMESTAMP('2025-01-23T08:00:00.000+0000'),TIMESTAMP('2025-01-23T17:00:00.000+0000'),
      '9.00',NULL,NULL,NULL,'New York','NY','USA','10001',CURRENT_TIMESTAMP(),NULL
    UNION ALL
    SELECT
      NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,CURRENT_TIMESTAMP(),
      sha2(concat_ws('|',
        NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL
      ),256)
  )
  SELECT * FROM test_agent_log_data
);

-- Validate row count matches test data (should be 26 rows)
SELECT 
  COUNT(*) AS inserted_row_count
FROM 
  purgo_playground.agent_log;
-- Expect: 26

-- Assert row count is 26
WITH row_count AS (
  SELECT COUNT(*) AS actual_row_count FROM purgo_playground.agent_log
)
SELECT 
  CASE WHEN actual_row_count = 26 THEN "PASS" ELSE "FAIL" END AS testdata_row_count_test
FROM row_count;

/*-----------------------------------------------------------------------------
SECTION: Data Type Conversion & NULL Handling Tests
-----------------------------------------------------------------------------*/

-- Validate that agent_first_login and agent_first_logout are TIMESTAMP, total_login_time_hrs is STRING, agent_key is STRING
SELECT
  typeof(agent_first_login) AS agent_first_login_type,
  typeof(agent_first_logout) AS agent_first_logout_type,
  typeof(total_login_time_hrs) AS total_login_time_hrs_type,
  typeof(agent_key) AS agent_key_type
FROM purgo_playground.agent_log
LIMIT 1;

-- Validate NULL handling: Rows with NULL unix_id and agent_name must have agent_key generated using NULLs
SELECT
  unix_id, agent_name, agent_key
FROM purgo_playground.agent_log
WHERE unix_id IS NULL AND agent_name IS NULL AND agent_key IS NOT NULL;

-- Validate NULL propagation: If agent_first_logout is NULL, total_login_time_hrs must be NULL
SELECT
  agent_first_logout, total_login_time_hrs
FROM purgo_playground.agent_log
WHERE agent_first_logout IS NULL;

/*-----------------------------------------------------------------------------
SECTION: Unique Key Generation Validation
-----------------------------------------------------------------------------*/

-- Validate agent_key is SHA256 hash of concatenated fields (lowercase hex)
WITH key_check AS (
  SELECT
    unix_id, agent_name, log_date, agent_first_login, agent_first_logout, total_login_time_hrs,
    agent_city, agent_state, agent_country, agent_zip_code, agent_key,
    sha2(concat_ws('|',
      unix_id, agent_name, CAST(log_date AS STRING),
      CASE WHEN agent_first_login IS NULL THEN NULL ELSE date_format(agent_first_login, "yyyy-MM-dd'T'HH:mm:ss.SSSZ") END,
      CASE WHEN agent_first_logout IS NULL THEN NULL ELSE date_format(agent_first_logout, "yyyy-MM-dd'T'HH:mm:ss.SSSZ") END,
      total_login_time_hrs, agent_city, agent_state, agent_country, agent_zip_code
    ),256) AS expected_agent_key
  FROM purgo_playground.agent_log
)
SELECT
  agent_key = expected_agent_key AS agent_key_hash_match
FROM key_check
WHERE agent_key IS NOT NULL;

/*-----------------------------------------------------------------------------
SECTION: Data Quality Validation - Field Formats
-----------------------------------------------------------------------------*/

-- Validate log_date is in YYYY-MM-DD format
SELECT
  log_date,
  CASE WHEN date_format(log_date, "yyyy-MM-dd") = CAST(log_date AS STRING) THEN "PASS" ELSE "FAIL" END AS log_date_format_test
FROM purgo_playground.agent_log
WHERE log_date IS NOT NULL;

-- Validate agent_first_login and agent_first_logout are in ISO 8601 format
SELECT
  agent_first_login,
  agent_first_logout,
  CASE 
    WHEN agent_first_login IS NULL THEN "PASS"
    WHEN date_format(agent_first_login, "yyyy-MM-dd'T'HH:mm:ss'Z'") LIKE "____-__-__T__:__:__Z" THEN "PASS"
    ELSE "FAIL"
  END AS agent_first_login_format_test,
  CASE 
    WHEN agent_first_logout IS NULL THEN "PASS"
    WHEN date_format(agent_first_logout, "yyyy-MM-dd'T'HH:mm:ss'Z'") LIKE "____-__-__T__:__:__Z" THEN "PASS"
    ELSE "FAIL"
  END AS agent_first_logout_format_test
FROM purgo_playground.agent_log;

-- Validate total_login_time_hrs is a string with 2 decimal places
SELECT
  total_login_time_hrs,
  CASE 
    WHEN total_login_time_hrs IS NULL THEN "PASS"
    WHEN total_login_time_hrs RLIKE "^[0-9]+\\.[0-9]{2}$" THEN "PASS"
    ELSE "FAIL"
  END AS total_login_time_hrs_format_test
FROM purgo_playground.agent_log;

/*-----------------------------------------------------------------------------
SECTION: Data Quality Validation - Hardcoded Location Fields
-----------------------------------------------------------------------------*/

-- Validate agent_city, agent_state, agent_country, agent_zip_code are hardcoded or match allowed values
SELECT
  agent_city, agent_state, agent_country, agent_zip_code,
  CASE 
    WHEN agent_city IN ("New York","Néw Yørk","東京") OR agent_city IS NULL THEN "PASS" ELSE "FAIL" END AS agent_city_test,
    WHEN agent_state IN ("NY","N¥","東京都") OR agent_state IS NULL THEN "PASS" ELSE "FAIL" END AS agent_state_test,
    WHEN agent_country IN ("USA","U$A","日本") OR agent_country IS NULL THEN "PASS" ELSE "FAIL" END AS agent_country_test,
    WHEN agent_zip_code IN ("10001","1000!@#","〒100-0001") OR agent_zip_code IS NULL THEN "PASS" ELSE "FAIL" END AS agent_zip_code_test
FROM purgo_playground.agent_log;

/*-----------------------------------------------------------------------------
SECTION: Data Quality Validation - data_loaded_at Timestamp
-----------------------------------------------------------------------------*/

-- Validate data_loaded_at is in ISO 8601 format
SELECT
  data_loaded_at,
  CASE 
    WHEN data_loaded_at IS NULL THEN "PASS"
    WHEN date_format(data_loaded_at, "yyyy-MM-dd'T'HH:mm:ss'Z'") LIKE "____-__-__T__:__:__Z" THEN "PASS"
    ELSE "FAIL"
  END AS data_loaded_at_format_test
FROM purgo_playground.agent_log;

/*-----------------------------------------------------------------------------
SECTION: Error Handling - All Required Fields Missing
-----------------------------------------------------------------------------*/

-- Validate that rows with all required fields missing (NULLs) do not have non-NULL agent_key except for the test case
SELECT
  unix_id, agent_name, log_date, agent_first_login, agent_first_logout, total_login_time_hrs, agent_key
FROM purgo_playground.agent_log
WHERE unix_id IS NULL AND agent_name IS NULL AND log_date IS NULL AND agent_first_login IS NULL AND agent_first_logout IS NULL AND total_login_time_hrs IS NULL;

/*-----------------------------------------------------------------------------
SECTION: Performance Test - Query Execution Time
-----------------------------------------------------------------------------*/

-- Performance: Measure query execution time for retrieving all agent_log records
-- Note: Use Databricks SQL EXPLAIN for query plan analysis
EXPLAIN SELECT * FROM purgo_playground.agent_log;

/*-----------------------------------------------------------------------------
SECTION: Delta Lake Operations - MERGE, UPDATE, DELETE, Window Functions
-----------------------------------------------------------------------------*/

-- Delta Lake: Test UPDATE operation (set agent_city to "NYC" for agent_name = "John Doe")
UPDATE purgo_playground.agent_log
SET agent_city = "NYC"
WHERE agent_name = "John Doe";

-- Validate UPDATE
SELECT agent_name, agent_city FROM purgo_playground.agent_log WHERE agent_name = "John Doe";

-- Delta Lake: Test DELETE operation (delete where agent_name = "Null Key")
DELETE FROM purgo_playground.agent_log WHERE agent_name = "Null Key";

-- Validate DELETE
SELECT agent_name FROM purgo_playground.agent_log WHERE agent_name = "Null Key";

-- Delta Lake: Test MERGE operation (merge new row for agent_name = "Merge Test")
MERGE INTO purgo_playground.agent_log AS target
USING (
  SELECT
    'IU999' AS unix_id,
    'Merge Test' AS agent_name,
    DATE('2025-02-01') AS log_date,
    TIMESTAMP('2025-02-01T08:00:00.000+0000') AS agent_first_login,
    TIMESTAMP('2025-02-01T17:00:00.000+0000') AS agent_first_logout,
    '9.00' AS total_login_time_hrs,
    NULL AS agent_lunch_login,
    NULL AS agent_lunch_logout,
    NULL AS agent_lunch_duration,
    'New York' AS agent_city,
    'NY' AS agent_state,
    'USA' AS agent_country,
    '10001' AS agent_zip_code,
    CURRENT_TIMESTAMP() AS data_loaded_at,
    sha2(concat_ws('|',
      'IU999','Merge Test','2025-02-01','2025-02-01T08:00:00.000+0000','2025-02-01T17:00:00.000+0000','9.00','New York','NY','USA','10001'
    ),256) AS agent_key
) AS source
ON target.agent_name = source.agent_name
WHEN MATCHED THEN
  UPDATE SET agent_city = source.agent_city
WHEN NOT MATCHED THEN
  INSERT *;

-- Validate MERGE
SELECT agent_name, agent_city FROM purgo_playground.agent_log WHERE agent_name = "Merge Test";

-- Window Function: For each agent_name, get the earliest agent_first_login
SELECT
  agent_name,
  MIN(agent_first_login) OVER (PARTITION BY agent_name) AS earliest_login
FROM purgo_playground.agent_log
WHERE agent_name IS NOT NULL;

/*-----------------------------------------------------------------------------
SECTION: Cleanup - Remove Test Data
-----------------------------------------------------------------------------*/

-- Cleanup: Drop agent_log table after tests
DROP TABLE IF EXISTS purgo_playground.agent_log;
