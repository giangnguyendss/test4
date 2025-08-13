USE CATALOG purgo_databricks;

-- Test Data Generation for purgo_playground.agent_log
-- Covers: Happy path, edge cases, error cases, NULLs, special/multibyte chars

WITH test_agent_log_data AS (
  SELECT
    -- Happy Path: Valid agent, all fields populated
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
    -- Happy Path: Valid agent, different times
    SELECT
    'IU002','Jane Smith',DATE('2025-01-03'),
    TIMESTAMP('2025-01-03T09:15:00.000+0000'),TIMESTAMP('2025-01-03T18:45:00.000+0000'),
    '9.50',NULL,NULL,NULL,'New York','NY','USA','10001',CURRENT_TIMESTAMP(),
    sha2(concat_ws('|',
      'IU002','Jane Smith','2025-01-03','2025-01-03T09:15:00.000+0000','2025-01-03T18:45:00.000+0000','9.50','New York','NY','USA','10001'
    ),256)

  UNION ALL
    -- Edge Case: Multiple agent_profile_data records, pick most recent
    SELECT
    'IU001A','John D.',DATE('2025-01-02'),
    TIMESTAMP('2025-01-02T08:00:00.000+0000'),TIMESTAMP('2025-01-02T17:00:00.000+0000'),
    '9.00',NULL,NULL,NULL,'New York','NY','USA','10001',CURRENT_TIMESTAMP(),
    sha2(concat_ws('|',
      'IU001A','John D.','2025-01-02','2025-01-02T08:00:00.000+0000','2025-01-02T17:00:00.000+0000','9.00','New York','NY','USA','10001'
    ),256)

  UNION ALL
    -- Error Case: Missing user_presence_tracker (NULL unix_id, agent_name)
    SELECT
    NULL,NULL,DATE('2025-01-04'),
    TIMESTAMP('2025-01-04T08:00:00.000+0000'),TIMESTAMP('2025-01-04T17:00:00.000+0000'),
    '9.00',NULL,NULL,NULL,'New York','NY','USA','10001',CURRENT_TIMESTAMP(),
    sha2(concat_ws('|',
      NULL,NULL,'2025-01-04','2025-01-04T08:00:00.000+0000','2025-01-04T17:00:00.000+0000','9.00','New York','NY','USA','10001'
    ),256)

  UNION ALL
    -- Error Case: Missing agent_profile_data (NULL unix_id, agent_name)
    SELECT
    NULL,NULL,DATE('2025-01-05'),
    TIMESTAMP('2025-01-05T08:00:00.000+0000'),TIMESTAMP('2025-01-05T17:00:00.000+0000'),
    '9.00',NULL,NULL,NULL,'New York','NY','USA','10001',CURRENT_TIMESTAMP(),
    sha2(concat_ws('|',
      NULL,NULL,'2025-01-05','2025-01-05T08:00:00.000+0000','2025-01-05T17:00:00.000+0000','9.00','New York','NY','USA','10001'
    ),256)

  UNION ALL
    -- Edge Case: Multiple sessions per day, min/max times
    SELECT
    'IU005','Bob Agent',DATE('2025-01-06'),
    TIMESTAMP('2025-01-06T07:55:00.000+0000'),TIMESTAMP('2025-01-06T18:00:00.000+0000'),
    '10.08',NULL,NULL,NULL,'New York','NY','USA','10001',CURRENT_TIMESTAMP(),
    sha2(concat_ws('|',
      'IU005','Bob Agent','2025-01-06','2025-01-06T07:55:00.000+0000','2025-01-06T18:00:00.000+0000','10.08','New York','NY','USA','10001'
    ),256)

  UNION ALL
    -- Error Case: session_end_time NULL
    SELECT
    'IU006','Null End',DATE('2025-01-07'),
    TIMESTAMP('2025-01-07T08:00:00.000+0000'),NULL,
    NULL,NULL,NULL,NULL,'New York','NY','USA','10001',CURRENT_TIMESTAMP(),
    sha2(concat_ws('|',
      'IU006','Null End','2025-01-07','2025-01-07T08:00:00.000+0000',NULL,NULL,'New York','NY','USA','10001'
    ),256)

  UNION ALL
    -- Unique Key Generation Validation
    SELECT
    'IU007','Alice Lee',DATE('2025-01-08'),
    TIMESTAMP('2025-01-08T08:00:00.000+0000'),TIMESTAMP('2025-01-08T17:00:00.000+0000'),
    '9.00',NULL,NULL,NULL,'New York','NY','USA','10001',CURRENT_TIMESTAMP(),
    sha2(concat_ws('|',
      'IU007','Alice Lee','2025-01-08','2025-01-08T08:00:00.000+0000','2025-01-08T17:00:00.000+0000','9.00','New York','NY','USA','10001'
    ),256)

  UNION ALL
    -- Data Validation: Field Formats
    SELECT
    'IU009','Format Test',DATE('2025-01-09'),
    TIMESTAMP('2025-01-09T08:00:00.000+0000'),TIMESTAMP('2025-01-09T17:00:00.000+0000'),
    '9.00',NULL,NULL,NULL,'New York','NY','USA','10001',CURRENT_TIMESTAMP(),
    sha2(concat_ws('|',
      'IU009','Format Test','2025-01-09','2025-01-09T08:00:00.000+0000','2025-01-09T17:00:00.000+0000','9.00','New York','NY','USA','10001'
    ),256)

  UNION ALL
    -- Data Validation: Hardcoded Location Fields
    SELECT
    'IU010','Location Test',DATE('2025-01-10'),
    TIMESTAMP('2025-01-10T08:00:00.000+0000'),TIMESTAMP('2025-01-10T17:00:00.000+0000'),
    '9.00',NULL,NULL,NULL,'New York','NY','USA','10001',CURRENT_TIMESTAMP(),
    sha2(concat_ws('|',
      'IU010','Location Test','2025-01-10','2025-01-10T08:00:00.000+0000','2025-01-10T17:00:00.000+0000','9.00','New York','NY','USA','10001'
    ),256)

  UNION ALL
    -- Data Validation: data_loaded_at Timestamp
    SELECT
    'IU011','Timestamp Test',DATE('2025-01-11'),
    TIMESTAMP('2025-01-11T08:00:00.000+0000'),TIMESTAMP('2025-01-11T17:00:00.000+0000'),
    '9.00',NULL,NULL,NULL,'New York','NY','USA','10001',TIMESTAMP('2025-01-11T12:34:56.000+0000'),
    sha2(concat_ws('|',
      'IU011','Timestamp Test','2025-01-11','2025-01-11T08:00:00.000+0000','2025-01-11T17:00:00.000+0000','9.00','New York','NY','USA','10001'
    ),256)

  UNION ALL
    -- Error Handling: Ambiguous person_identifier, multiple emails, pick most recent profile
    SELECT
    'IU008B','Agent Eight B',DATE('2025-01-10'),
    TIMESTAMP('2025-01-10T08:00:00.000+0000'),TIMESTAMP('2025-01-10T17:00:00.000+0000'),
    '9.00',NULL,NULL,NULL,'New York','NY','USA','10001',CURRENT_TIMESTAMP(),
    sha2(concat_ws('|',
      'IU008B','Agent Eight B','2025-01-10','2025-01-10T08:00:00.000+0000','2025-01-10T17:00:00.000+0000','9.00','New York','NY','USA','10001'
    ),256)

  UNION ALL
    -- Error Handling: All Required Fields Missing (no row generated, shown as NULLs for test)
    SELECT
    NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL

  UNION ALL
    -- Edge Case: Special characters in agent_name
    SELECT
    'IU012','Jöhn Döe 🚀',DATE('2025-01-12'),
    TIMESTAMP('2025-01-12T08:00:00.000+0000'),TIMESTAMP('2025-01-12T17:00:00.000+0000'),
    '9.00',NULL,NULL,NULL,'New York','NY','USA','10001',CURRENT_TIMESTAMP(),
    sha2(concat_ws('|',
      'IU012','Jöhn Döe 🚀','2025-01-12','2025-01-12T08:00:00.000+0000','2025-01-12T17:00:00.000+0000','9.00','New York','NY','USA','10001'
    ),256)

  UNION ALL
    -- Edge Case: Multi-byte characters in agent_name
    SELECT
    'IU013','李四',DATE('2025-01-13'),
    TIMESTAMP('2025-01-13T08:00:00.000+0000'),TIMESTAMP('2025-01-13T17:00:00.000+0000'),
    '9.00',NULL,NULL,NULL,'New York','NY','USA','10001',CURRENT_TIMESTAMP(),
    sha2(concat_ws('|',
      'IU013','李四','2025-01-13','2025-01-13T08:00:00.000+0000','2025-01-13T17:00:00.000+0000','9.00','New York','NY','USA','10001'
    ),256)

  UNION ALL
    -- Edge Case: Out-of-range values (future date)
    SELECT
    'IU014','Future Agent',DATE('2099-12-31'),
    TIMESTAMP('2099-12-31T08:00:00.000+0000'),TIMESTAMP('2099-12-31T17:00:00.000+0000'),
    '9.00',NULL,NULL,NULL,'New York','NY','USA','10001',CURRENT_TIMESTAMP(),
    sha2(concat_ws('|',
      'IU014','Future Agent','2099-12-31','2099-12-31T08:00:00.000+0000','2099-12-31T17:00:00.000+0000','9.00','New York','NY','USA','10001'
    ),256)

  UNION ALL
    -- Edge Case: NULL agent_first_login
    SELECT
    'IU015','Null Login',DATE('2025-01-15'),
    NULL,TIMESTAMP('2025-01-15T17:00:00.000+0000'),
    NULL,NULL,NULL,NULL,'New York','NY','USA','10001',CURRENT_TIMESTAMP(),
    sha2(concat_ws('|',
      'IU015','Null Login','2025-01-15',NULL,'2025-01-15T17:00:00.000+0000',NULL,'New York','NY','USA','10001'
    ),256)

  UNION ALL
    -- Edge Case: NULL agent_first_logout
    SELECT
    'IU016','Null Logout',DATE('2025-01-16'),
    TIMESTAMP('2025-01-16T08:00:00.000+0000'),NULL,
    NULL,NULL,NULL,NULL,'New York','NY','USA','10001',CURRENT_TIMESTAMP(),
    sha2(concat_ws('|',
      'IU016','Null Logout','2025-01-16','2025-01-16T08:00:00.000+0000',NULL,NULL,'New York','NY','USA','10001'
    ),256)

  UNION ALL
    -- Edge Case: NULL total_login_time_hrs
    SELECT
    'IU017','Null Duration',DATE('2025-01-17'),
    TIMESTAMP('2025-01-17T08:00:00.000+0000'),TIMESTAMP('2025-01-17T17:00:00.000+0000'),
    NULL,NULL,NULL,NULL,'New York','NY','USA','10001',CURRENT_TIMESTAMP(),
    sha2(concat_ws('|',
      'IU017','Null Duration','2025-01-17','2025-01-17T08:00:00.000+0000','2025-01-17T17:00:00.000+0000',NULL,'New York','NY','USA','10001'
    ),256)

  UNION ALL
    -- Edge Case: All location fields NULL
    SELECT
    'IU018','Null Location',DATE('2025-01-18'),
    TIMESTAMP('2025-01-18T08:00:00.000+0000'),TIMESTAMP('2025-01-18T17:00:00.000+0000'),
    '9.00',NULL,NULL,NULL,NULL,NULL,NULL,NULL,CURRENT_TIMESTAMP(),
    sha2(concat_ws('|',
      'IU018','Null Location','2025-01-18','2025-01-18T08:00:00.000+0000','2025-01-18T17:00:00.000+0000','9.00',NULL,NULL,NULL,NULL
    ),256)

  UNION ALL
    -- Edge Case: agent_lunch fields populated
    SELECT
    'IU019','Lunch Agent',DATE('2025-01-19'),
    TIMESTAMP('2025-01-19T08:00:00.000+0000'),TIMESTAMP('2025-01-19T17:00:00.000+0000'),
    '9.00',TIMESTAMP('2025-01-19T12:00:00.000+0000'),TIMESTAMP('2025-01-19T12:30:00.000+0000'),'0.50','New York','NY','USA','10001',CURRENT_TIMESTAMP(),
    sha2(concat_ws('|',
      'IU019','Lunch Agent','2025-01-19','2025-01-19T08:00:00.000+0000','2025-01-19T17:00:00.000+0000','9.00','New York','NY','USA','10001'
    ),256)

  UNION ALL
    -- Edge Case: Special characters in agent_city/state/country/zip
    SELECT
    'IU020','Special Loc',DATE('2025-01-20'),
    TIMESTAMP('2025-01-20T08:00:00.000+0000'),TIMESTAMP('2025-01-20T17:00:00.000+0000'),
    '9.00',NULL,NULL,NULL,'Néw Yørk','N¥','U$A','1000!@#',CURRENT_TIMESTAMP(),
    sha2(concat_ws('|',
      'IU020','Special Loc','2025-01-20','2025-01-20T08:00:00.000+0000','2025-01-20T17:00:00.000+0000','9.00','Néw Yørk','N¥','U$A','1000!@#'
    ),256)

  UNION ALL
    -- Edge Case: Multi-byte characters in agent_city/state/country/zip
    SELECT
    'IU021','MultiByte Loc',DATE('2025-01-21'),
    TIMESTAMP('2025-01-21T08:00:00.000+0000'),TIMESTAMP('2025-01-21T17:00:00.000+0000'),
    '9.00',NULL,NULL,NULL,'東京','東京都','日本','〒100-0001',CURRENT_TIMESTAMP(),
    sha2(concat_ws('|',
      'IU021','MultiByte Loc','2025-01-21','2025-01-21T08:00:00.000+0000','2025-01-21T17:00:00.000+0000','9.00','東京','東京都','日本','〒100-0001'
    ),256)

  UNION ALL
    -- Edge Case: NULL data_loaded_at
    SELECT
    'IU022','Null Loaded',DATE('2025-01-22'),
    TIMESTAMP('2025-01-22T08:00:00.000+0000'),TIMESTAMP('2025-01-22T17:00:00.000+0000'),
    '9.00',NULL,NULL,NULL,'New York','NY','USA','10001',NULL,
    sha2(concat_ws('|',
      'IU022','Null Loaded','2025-01-22','2025-01-22T08:00:00.000+0000','2025-01-22T17:00:00.000+0000','9.00','New York','NY','USA','10001'
    ),256)

  UNION ALL
    -- Edge Case: NULL agent_key
    SELECT
    'IU023','Null Key',DATE('2025-01-23'),
    TIMESTAMP('2025-01-23T08:00:00.000+0000'),TIMESTAMP('2025-01-23T17:00:00.000+0000'),
    '9.00',NULL,NULL,NULL,'New York','NY','USA','10001',CURRENT_TIMESTAMP(),NULL

  UNION ALL
    -- Edge Case: All fields NULL except agent_key
    SELECT
    NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,CURRENT_TIMESTAMP(),
    sha2(concat_ws('|',
      NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL
    ),256)
)

SELECT * FROM test_agent_log_data;
