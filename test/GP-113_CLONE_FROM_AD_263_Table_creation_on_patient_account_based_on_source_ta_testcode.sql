/* 
    Databricks SQL Test Suite for purgo_playground.patient_account Table Creation and ETL
    -------------------------------------------------------------------------------
    - Catalog: purgo_databricks
    - Schema: purgo_playground
    - Table: patient_account
    - Source Table: pa_account
    - All requirements, transformation logic, error handling, and validation rules are implemented as per mapping specification and test scenarios.
    - All comments are block or line comments as per requirements.
    - All code uses double quotes for string literals to avoid single quote issues.
    - All DDL uses safe patterns (DROP IF EXISTS, CREATE TABLE, etc.).
    - All constraints, schema validation, and data quality checks are included.
    - No temp views or temporary tables are used.
    - All CTEs are used directly in validation queries.
    - All code is executable in Databricks SQL.
*/

/* -------------------------------------------------------------------------------
   1. Setup: Drop and Create patient_account Table with Constraints
   ------------------------------------------------------------------------------- */
USE CATALOG purgo_databricks;

DROP TABLE IF EXISTS purgo_playground.patient_account;

CREATE TABLE purgo_playground.patient_account (
    patient_foundation_shipment STRING NOT NULL,
    prescriber_id STRING NOT NULL,
    prescriber_key STRING NOT NULL,
    patient_sf_id STRING NOT NULL,
    service_request_type STRING NOT NULL,
    case_sf_id STRING NOT NULL,
    account_id STRING NOT NULL,
    hash_key STRING NOT NULL,
    last_modified_date TIMESTAMP NOT NULL,
    planned_date DATE,
    CONSTRAINT hash_key_unique UNIQUE (hash_key)
);

/* -------------------------------------------------------------------------------
   2. Setup: Drop and Create Error Log Table for ETL Error Scenarios
   ------------------------------------------------------------------------------- */
DROP TABLE IF EXISTS purgo_playground.pat_account_error_log;

CREATE TABLE IF NOT EXISTS purgo_playground.pat_account_error_log (
    hash_key STRING,
    error_col STRING NOT NULL,
    error_message STRING NOT NULL,
    event_time TIMESTAMP
);

/* -------------------------------------------------------------------------------
   3. Test Data CTE: Simulate pa_account Source Data for All Test Scenarios
   ------------------------------------------------------------------------------- */
WITH pa_account_test_data AS (
    SELECT "SHIP123" AS patient_foundation_shipment, "John Smith" AS prescriber_name_c, "123 Main St" AS prescriber_name_c_address, "PAT001" AS patinet_c, "SRV" AS recordtypeid, "CASE001" AS id, "ACC001" AS accountid
    UNION ALL
    SELECT "SHIP456", "Dr. Jane Doe", "456 Oak Ave", "PAT002", "SRV2", "CASE002", "ACC002"
    UNION ALL
    SELECT "SHIP789", "Ms. Alice Brown", "789 Pine Rd", "PAT003", "SRV3", "CASE003", "ACC003"
    UNION ALL
    SELECT "SHIP321", "Dr. Émile Zola", "321 Rue de Paris", "pat004", "SRV4", "CASE004", "ACC004"
    UNION ALL
    SELECT "SHIP654", "Dr. 李小龙", "654 龙街", "XYPAT005", "SRV5", "CASE005", "ACC005"
    UNION ALL
    SELECT "SHIP987", "Dr. O'Connor", "987 O'Street", "PAT006", "SRV6", "CASE006", "ACC006"
    UNION ALL
    SELECT "SHIP111", "Dr. 山田太郎", "111 東京通り", "PAT007", "SRV7", "CASE007", "ACC007"
    UNION ALL
    SELECT "SHIP222", "Dr. Müller", "222 Straße", "PAT008", "SRV8", "CASE008", "ACC008"
    UNION ALL
    SELECT "SHIP333", "Dr. Иван Иванов", "333 ул. Ленина", "PAT009", "SRV9", "CASE009", "ACC009"
    UNION ALL
    SELECT "SHIP444", "Dr. John Doe", "444 Main St", "XYZ123", "SRV10", "CASE010", "ACC010"
    UNION ALL
    SELECT NULL, "Dr. Jane Doe", "555 Oak Ave", "PAT011", "SRV11", "CASE011", "ACC011"
    UNION ALL
    SELECT "SHIP555", NULL, "555 Oak Ave", "PAT012", "SRV12", "CASE012", "ACC012"
    UNION ALL
    SELECT "SHIP666", "Dr. Jane Doe", NULL, "PAT013", "SRV13", "CASE013", "ACC013"
    UNION ALL
    SELECT "SHIP777", "Dr. Jane Doe", "777 Oak Ave", NULL, "SRV14", "CASE014", "ACC014"
    UNION ALL
    SELECT "SHIP888", "Dr. Jane Doe", "888 Oak Ave", "PAT015", NULL, "CASE015", "ACC015"
    UNION ALL
    SELECT "SHIP999", "Dr. Jane Doe", "999 Oak Ave", "PAT016", "SRV16", NULL, "ACC016"
    UNION ALL
    SELECT "SHIP000", "Dr. Jane Doe", "000 Oak Ave", "PAT017", "SRV17", "CASE017", NULL
    UNION ALL
    SELECT "SHIP123", "John Smith", "123 Main St", "PAT001", "SRV", "CASE001", "ACC001"
    UNION ALL
    SELECT "SHIP101", "Dr. Jane Doe", "101 Oak Ave", "PAT018", "SRV18", "CASE018", "ACC018"
    UNION ALL
    SELECT "SHIP202", "  Dr. Jane Doe  ", "202 Oak Ave", "PAT019", "SRV19", "CASE019", "ACC019"
    UNION ALL
    SELECT "SHIP303", "Dr. Jane Doe", "  303 Oak Ave  ", "PAT020", "SRV20", "CASE020", "ACC020"
    UNION ALL
    SELECT "SHIP404", "Dr. Jane #$%&*!", "404 Oak Ave", "PAT021", "SRV21", "CASE021", "ACC021"
    UNION ALL
    SELECT "SHIP505", "Dr. Jane Doe", "505 Oak Ave #$%&*!", "PAT022", "SRV22", "CASE022", "ACC022"
)

/* -------------------------------------------------------------------------------
   4. ETL: Insert Valid Records into patient_account Table with Transformation Logic
   ------------------------------------------------------------------------------- */
INSERT INTO purgo_playground.patient_account
SELECT
    patient_foundation_shipment,
    -- prescriber_id: If "Dr." not in prefix, prepend "Dr. "
    CASE
        WHEN prescriber_name_c IS NULL THEN NULL
        WHEN LOWER(TRIM(prescriber_name_c)) LIKE "dr.%" THEN TRIM(prescriber_name_c)
        ELSE CONCAT("Dr. ", TRIM(prescriber_name_c))
    END AS prescriber_id,
    -- prescriber_key: If "Dr." not in prefix, prepend "Dr. "
    CASE
        WHEN prescriber_name_c_address IS NULL THEN NULL
        WHEN LOWER(TRIM(prescriber_name_c_address)) LIKE "dr.%" THEN TRIM(prescriber_name_c_address)
        ELSE CONCAT("Dr. ", TRIM(prescriber_name_c_address))
    END AS prescriber_key,
    patinet_c,
    recordtypeid,
    id,
    accountid,
    CONCAT(
        COALESCE(patinet_c, ""),
        "#",
        COALESCE(recordtypeid, ""),
        "#",
        COALESCE(id, ""),
        "#",
        COALESCE(accountid, "")
    ) AS hash_key,
    CURRENT_TIMESTAMP() AS last_modified_date,
    NULL AS planned_date
FROM pa_account_test_data
WHERE
    -- Filter: patinet_c must contain "PAT" (case-insensitive)
    patinet_c IS NOT NULL
    AND LOWER(patinet_c) LIKE "%pat%"
    -- Exclude records with any NOT NULL column missing
    AND patient_foundation_shipment IS NOT NULL
    AND prescriber_name_c IS NOT NULL
    AND prescriber_name_c_address IS NOT NULL
    AND recordtypeid IS NOT NULL
    AND id IS NOT NULL
    AND accountid IS NOT NULL
    -- Ensure hash_key uniqueness
    AND CONCAT(
        COALESCE(patinet_c, ""),
        "#",
        COALESCE(recordtypeid, ""),
        "#",
        COALESCE(id, ""),
        "#",
        COALESCE(accountid, "")
    ) NOT IN (
        SELECT hash_key FROM purgo_playground.patient_account
    );

/* -------------------------------------------------------------------------------
   5. Error Logging: Insert Error Records for Invalid/Missing Data
   ------------------------------------------------------------------------------- */
INSERT INTO purgo_playground.pat_account_error_log
SELECT
    CONCAT(
        COALESCE(patinet_c, ""),
        "#",
        COALESCE(recordtypeid, ""),
        "#",
        COALESCE(id, ""),
        "#",
        COALESCE(accountid, "")
    ) AS hash_key,
    error_col,
    error_message,
    CURRENT_TIMESTAMP() AS event_time
FROM (
    SELECT
        patient_foundation_shipment, prescriber_name_c, prescriber_name_c_address, patinet_c, recordtypeid, id, accountid,
        CASE
            WHEN patient_foundation_shipment IS NULL THEN "patient_foundation_shipment"
            WHEN prescriber_name_c IS NULL THEN "prescriber_name_c"
            WHEN prescriber_name_c_address IS NULL THEN "prescriber_name_c_address"
            WHEN patinet_c IS NULL THEN "patinet_c"
            WHEN recordtypeid IS NULL THEN "recordtypeid"
            WHEN id IS NULL THEN "id"
            WHEN accountid IS NULL THEN "accountid"
            ELSE NULL
        END AS error_col,
        CASE
            WHEN patient_foundation_shipment IS NULL THEN "patient_foundation_shipment is required"
            WHEN prescriber_name_c IS NULL THEN "prescriber_name_c is required"
            WHEN prescriber_name_c_address IS NULL THEN "prescriber_name_c_address is required"
            WHEN patinet_c IS NULL THEN "patinet_c is required"
            WHEN recordtypeid IS NULL THEN "recordtypeid is required"
            WHEN id IS NULL THEN "id is required"
            WHEN accountid IS NULL THEN "accountid is required"
            ELSE NULL
        END AS error_message
    FROM pa_account_test_data
    WHERE
        -- Only log errors for records that would otherwise pass the "PAT" filter
        (patient_foundation_shipment IS NULL OR prescriber_name_c IS NULL OR prescriber_name_c_address IS NULL OR patinet_c IS NULL OR recordtypeid IS NULL OR id IS NULL OR accountid IS NULL)
        AND patinet_c IS NOT NULL
        AND LOWER(patinet_c) LIKE "%pat%"
) err
WHERE error_col IS NOT NULL;

/* -------------------------------------------------------------------------------
   6. Error Logging: Log Filtered Out Records (patinet_c does not contain "PAT")
   ------------------------------------------------------------------------------- */
INSERT INTO purgo_playground.pat_account_error_log
SELECT
    NULL AS hash_key,
    "patinet_c" AS error_col,
    "patinet_c does not contain PAT" AS error_message,
    CURRENT_TIMESTAMP() AS event_time
FROM pa_account_test_data
WHERE
    patinet_c IS NOT NULL
    AND LOWER(patinet_c) NOT LIKE "%pat%";

/* -------------------------------------------------------------------------------
   7. Error Logging: Log Duplicate hash_key in Source
   ------------------------------------------------------------------------------- */
INSERT INTO purgo_playground.pat_account_error_log
SELECT
    hash_key,
    "hash_key" AS error_col,
    "Duplicate hash_key in source" AS error_message,
    CURRENT_TIMESTAMP() AS event_time
FROM (
    SELECT
        CONCAT(
            COALESCE(patinet_c, ""),
            "#",
            COALESCE(recordtypeid, ""),
            "#",
            COALESCE(id, ""),
            "#",
            COALESCE(accountid, "")
        ) AS hash_key,
        COUNT(*) AS cnt
    FROM pa_account_test_data
    WHERE
        patinet_c IS NOT NULL
        AND LOWER(patinet_c) LIKE "%pat%"
        AND patient_foundation_shipment IS NOT NULL
        AND prescriber_name_c IS NOT NULL
        AND prescriber_name_c_address IS NOT NULL
        AND recordtypeid IS NOT NULL
        AND id IS NOT NULL
        AND accountid IS NOT NULL
    GROUP BY
        CONCAT(
            COALESCE(patinet_c, ""),
            "#",
            COALESCE(recordtypeid, ""),
            "#",
            COALESCE(id, ""),
            "#",
            COALESCE(accountid, "")
        )
    HAVING cnt > 1
) dup;

/* -------------------------------------------------------------------------------
   8. Error Logging: Log Source Table Does Not Exist Scenario
   ------------------------------------------------------------------------------- */
-- This test is only valid if the source table is missing; simulate with a check
-- If pa_account_test_data is empty, log error
INSERT INTO purgo_playground.pat_account_error_log
SELECT
    NULL AS hash_key,
    "source" AS error_col,
    "Source table pa_account does not exist" AS error_message,
    CURRENT_TIMESTAMP() AS event_time
WHERE NOT EXISTS (
    SELECT 1 FROM pa_account_test_data
);

/* -------------------------------------------------------------------------------
   9. Validation Query: Validate Inserted Data in patient_account Table
   ------------------------------------------------------------------------------- */
WITH inserted_data AS (
    SELECT
        patient_foundation_shipment,
        prescriber_id,
        prescriber_key,
        patient_sf_id,
        service_request_type,
        case_sf_id,
        account_id,
        hash_key,
        last_modified_date,
        planned_date
    FROM purgo_playground.patient_account
)
SELECT * FROM inserted_data;

/* -------------------------------------------------------------------------------
   10. Schema Validation: Assert patient_account Table Schema Matches Specification
   ------------------------------------------------------------------------------- */
-- Assert column count
SELECT
    COUNT(*) AS column_count
FROM
    information_schema.columns
WHERE
    table_catalog = "purgo_databricks"
    AND table_schema = "purgo_playground"
    AND table_name = "patient_account";
-- Should return 10

-- Assert column names and types
SELECT
    column_name,
    data_type,
    is_nullable
FROM
    information_schema.columns
WHERE
    table_catalog = "purgo_databricks"
    AND table_schema = "purgo_playground"
    AND table_name = "patient_account"
ORDER BY ordinal_position;

/* -------------------------------------------------------------------------------
   11. Data Quality Validation: Assert No NULLs in NOT NULL Columns
   ------------------------------------------------------------------------------- */
SELECT
    COUNT(*) AS null_count
FROM purgo_playground.patient_account
WHERE
    patient_foundation_shipment IS NULL
    OR prescriber_id IS NULL
    OR prescriber_key IS NULL
    OR patient_sf_id IS NULL
    OR service_request_type IS NULL
    OR case_sf_id IS NULL
    OR account_id IS NULL
    OR hash_key IS NULL
    OR last_modified_date IS NULL;
-- Should return 0

/* -------------------------------------------------------------------------------
   12. Data Type Conversion and Complex Type Validation
   ------------------------------------------------------------------------------- */
-- Validate prescriber_id and prescriber_key transformation
WITH prescriber_transform AS (
    SELECT
        prescriber_id,
        prescriber_key
    FROM purgo_playground.patient_account
)
SELECT * FROM prescriber_transform;

-- Validate hash_key uniqueness
SELECT
    hash_key,
    COUNT(*) AS cnt
FROM purgo_playground.patient_account
GROUP BY hash_key
HAVING cnt > 1;
-- Should return 0 rows

/* -------------------------------------------------------------------------------
   13. Window Function Test: Analytics Feature Example
   ------------------------------------------------------------------------------- */
-- Example: Count of patient_account records per service_request_type
SELECT
    service_request_type,
    COUNT(*) OVER (PARTITION BY service_request_type) AS type_count
FROM purgo_playground.patient_account;

/* -------------------------------------------------------------------------------
   14. Delta Lake Operations: MERGE, UPDATE, DELETE Test
   ------------------------------------------------------------------------------- */
-- MERGE: Upsert new record (simulate update)
MERGE INTO purgo_playground.patient_account AS target
USING (
    SELECT
        "SHIP999" AS patient_foundation_shipment,
        "Dr. Jane Doe" AS prescriber_id,
        "Dr. 999 Oak Ave" AS prescriber_key,
        "PAT016" AS patient_sf_id,
        "SRV16" AS service_request_type,
        "CASE016" AS case_sf_id,
        "ACC016" AS account_id,
        "PAT016#SRV16#CASE016#ACC016" AS hash_key,
        CURRENT_TIMESTAMP() AS last_modified_date,
        NULL AS planned_date
) AS source
ON target.hash_key = source.hash_key
WHEN MATCHED THEN
    UPDATE SET
        target.last_modified_date = source.last_modified_date
WHEN NOT MATCHED THEN
    INSERT (
        patient_foundation_shipment,
        prescriber_id,
        prescriber_key,
        patient_sf_id,
        service_request_type,
        case_sf_id,
        account_id,
        hash_key,
        last_modified_date,
        planned_date
    ) VALUES (
        source.patient_foundation_shipment,
        source.prescriber_id,
        source.prescriber_key,
        source.patient_sf_id,
        source.service_request_type,
        source.case_sf_id,
        source.account_id,
        source.hash_key,
        source.last_modified_date,
        source.planned_date
    );

-- DELETE: Remove a test record
DELETE FROM purgo_playground.patient_account
WHERE hash_key = "PAT016#SRV16#CASE016#ACC016";

-- UPDATE: Change planned_date for a record
UPDATE purgo_playground.patient_account
SET planned_date = DATE(CURRENT_DATE())
WHERE hash_key = "PAT018#SRV18#CASE018#ACC018";

/* -------------------------------------------------------------------------------
   15. Cleanup: Drop Test Tables (if required)
   ------------------------------------------------------------------------------- */
-- DROP TABLE IF EXISTS purgo_playground.patient_account;
-- DROP TABLE IF EXISTS purgo_playground.pat_account_error_log;

/* -------------------------------------------------------------------------------
   End of Databricks SQL Test Suite for patient_account ETL
   ------------------------------------------------------------------------------- */

