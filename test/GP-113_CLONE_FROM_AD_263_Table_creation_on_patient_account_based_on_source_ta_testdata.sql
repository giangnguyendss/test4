USE CATALOG purgo_databricks;

-- Drop the patient_account table if it already exists
DROP TABLE IF EXISTS purgo_databricks.purgo_playground.patient_account;

-- Create the patient_account table with the required schema
CREATE TABLE purgo_databricks.purgo_playground.patient_account (
    patient_foundation_shipment STRING NOT NULL,
    prescriber_id STRING NOT NULL,
    prescriber_key STRING NOT NULL,
    patient_sf_id STRING NOT NULL,
    service_request_type STRING NOT NULL,
    case_sf_id STRING NOT NULL,
    account_id STRING NOT NULL,
    hash_key STRING NOT NULL,
    last_modified_date TIMESTAMP NOT NULL,
    planned_date DATE
);

-- Test Data Generation CTE
WITH test_data AS (
    SELECT
        -- Happy path: valid record, prescriber_name_c missing "Dr." prefix
        'SHIP123' AS patient_foundation_shipment,
        'John Smith' AS prescriber_name_c,
        '123 Main St' AS prescriber_name_c_address,
        'PAT001' AS patinet_c,
        'SRV' AS recordtypeid,
        'CASE001' AS id,
        'ACC001' AS accountid
    UNION ALL
    SELECT
        -- Happy path: valid record, prescriber_name_c already has "Dr." prefix
        'SHIP456',
        'Dr. Jane Doe',
        '456 Oak Ave',
        'PAT002',
        'SRV2',
        'CASE002',
        'ACC002'
    UNION ALL
    SELECT
        -- Happy path: valid record, prescriber_name_c with other prefix
        'SHIP789',
        'Ms. Alice Brown',
        '789 Pine Rd',
        'PAT003',
        'SRV3',
        'CASE003',
        'ACC003'
    UNION ALL
    SELECT
        -- Edge case: patinet_c lower case "pat"
        'SHIP321',
        'Dr. Émile Zola',
        '321 Rue de Paris',
        'pat004',
        'SRV4',
        'CASE004',
        'ACC004'
    UNION ALL
    SELECT
        -- Edge case: patinet_c contains "PAT" in the middle
        'SHIP654',
        'Dr. 李小龙',
        '654 龙街',
        'XYPAT005',
        'SRV5',
        'CASE005',
        'ACC005'
    UNION ALL
    SELECT
        -- Edge case: prescriber_name_c with special characters
        'SHIP987',
        'Dr. O\'Connor',
        '987 O\'Street',
        'PAT006',
        'SRV6',
        'CASE006',
        'ACC006'
    UNION ALL
    SELECT
        -- Edge case: prescriber_name_c with multi-byte characters
        'SHIP111',
        'Dr. 山田太郎',
        '111 東京通り',
        'PAT007',
        'SRV7',
        'CASE007',
        'ACC007'
    UNION ALL
    SELECT
        -- Edge case: prescriber_name_c_address with special characters
        'SHIP222',
        'Dr. Müller',
        '222 Straße',
        'PAT008',
        'SRV8',
        'CASE008',
        'ACC008'
    UNION ALL
    SELECT
        -- Edge case: prescriber_name_c_address with multi-byte characters
        'SHIP333',
        'Dr. Иван Иванов',
        '333 ул. Ленина',
        'PAT009',
        'SRV9',
        'CASE009',
        'ACC009'
    UNION ALL
    SELECT
        -- Error case: patinet_c does not contain "PAT" (should be filtered out)
        'SHIP444',
        'Dr. John Doe',
        '444 Main St',
        'XYZ123',
        'SRV10',
        'CASE010',
        'ACC010'
    UNION ALL
    SELECT
        -- Error case: NULL patient_foundation_shipment (should be excluded)
        NULL,
        'Dr. Jane Doe',
        '555 Oak Ave',
        'PAT011',
        'SRV11',
        'CASE011',
        'ACC011'
    UNION ALL
    SELECT
        -- Error case: NULL prescriber_name_c (should be excluded)
        'SHIP555',
        NULL,
        '555 Oak Ave',
        'PAT012',
        'SRV12',
        'CASE012',
        'ACC012'
    UNION ALL
    SELECT
        -- Error case: NULL prescriber_name_c_address (should be excluded)
        'SHIP666',
        'Dr. Jane Doe',
        NULL,
        'PAT013',
        'SRV13',
        'CASE013',
        'ACC013'
    UNION ALL
    SELECT
        -- Error case: NULL patinet_c (should be excluded)
        'SHIP777',
        'Dr. Jane Doe',
        '777 Oak Ave',
        NULL,
        'SRV14',
        'CASE014',
        'ACC014'
    UNION ALL
    SELECT
        -- Error case: NULL recordtypeid (should be excluded)
        'SHIP888',
        'Dr. Jane Doe',
        '888 Oak Ave',
        'PAT015',
        NULL,
        'CASE015',
        'ACC015'
    UNION ALL
    SELECT
        -- Error case: NULL id (should be excluded)
        'SHIP999',
        'Dr. Jane Doe',
        '999 Oak Ave',
        'PAT016',
        'SRV16',
        NULL,
        'ACC016'
    UNION ALL
    SELECT
        -- Error case: NULL accountid (should be excluded)
        'SHIP000',
        'Dr. Jane Doe',
        '000 Oak Ave',
        'PAT017',
        'SRV17',
        'CASE017',
        NULL
    UNION ALL
    SELECT
        -- Edge case: duplicate hash_key (should only insert one)
        'SHIP123',
        'John Smith',
        '123 Main St',
        'PAT001',
        'SRV',
        'CASE001',
        'ACC001'
    UNION ALL
    SELECT
        -- Edge case: planned_date is always null
        'SHIP101',
        'Dr. Jane Doe',
        '101 Oak Ave',
        'PAT018',
        'SRV18',
        'CASE018',
        'ACC018'
    UNION ALL
    SELECT
        -- Edge case: prescriber_name_c with leading/trailing spaces
        'SHIP202',
        '  Dr. Jane Doe  ',
        '202 Oak Ave',
        'PAT019',
        'SRV19',
        'CASE019',
        'ACC019'
    UNION ALL
    SELECT
        -- Edge case: prescriber_name_c_address with leading/trailing spaces
        'SHIP303',
        'Dr. Jane Doe',
        '  303 Oak Ave  ',
        'PAT020',
        'SRV20',
        'CASE020',
        'ACC020'
    UNION ALL
    SELECT
        -- Edge case: prescriber_name_c with special symbols
        'SHIP404',
        'Dr. Jane #$%&*!',
        '404 Oak Ave',
        'PAT021',
        'SRV21',
        'CASE021',
        'ACC021'
    UNION ALL
    SELECT
        -- Edge case: prescriber_name_c_address with special symbols
        'SHIP505',
        'Dr. Jane Doe',
        '505 Oak Ave #$%&*!',
        'PAT022',
        'SRV22',
        'CASE022',
        'ACC022'
)

-- Insert transformed test data into patient_account
INSERT INTO purgo_databricks.purgo_playground.patient_account
SELECT
    -- patient_foundation_shipment: must not be null
    patient_foundation_shipment,
    -- prescriber_id: If "Dr." not in prefix, prepend "Dr. "
    CASE
        WHEN prescriber_name_c IS NULL THEN NULL
        WHEN LOWER(TRIM(prescriber_name_c)) LIKE 'dr.%' THEN TRIM(prescriber_name_c)
        ELSE CONCAT('Dr. ', TRIM(prescriber_name_c))
    END AS prescriber_id,
    -- prescriber_key: If "Dr." not in prefix, prepend "Dr. "
    CASE
        WHEN prescriber_name_c_address IS NULL THEN NULL
        WHEN LOWER(TRIM(prescriber_name_c_address)) LIKE 'dr.%' THEN TRIM(prescriber_name_c_address)
        ELSE CONCAT('Dr. ', TRIM(prescriber_name_c_address))
    END AS prescriber_key,
    -- patient_sf_id: straight move from patinet_c
    patinet_c,
    -- service_request_type: straight move from recordtypeid
    recordtypeid,
    -- case_sf_id: straight move from id
    id,
    -- account_id: straight move from accountid
    accountid,
    -- hash_key: concatenation of patinet_c#recordtypeid#id#accountid
    CONCAT(
        COALESCE(patinet_c, ''),
        '#',
        COALESCE(recordtypeid, ''),
        '#',
        COALESCE(id, ''),
        '#',
        COALESCE(accountid, '')
    ) AS hash_key,
    -- last_modified_date: current timestamp in Databricks format
    CURRENT_TIMESTAMP() AS last_modified_date,
    -- planned_date: always null for test data
    NULL AS planned_date
FROM test_data
WHERE
    -- Filter: patinet_c must contain "PAT" (case-insensitive)
    patinet_c IS NOT NULL
    AND LOWER(patinet_c) LIKE '%pat%'
    -- Exclude records with any NOT NULL column missing
    AND patient_foundation_shipment IS NOT NULL
    AND prescriber_name_c IS NOT NULL
    AND prescriber_name_c_address IS NOT NULL
    AND recordtypeid IS NOT NULL
    AND id IS NOT NULL
    AND accountid IS NOT NULL
    -- Ensure hash_key uniqueness
    AND CONCAT(
        COALESCE(patinet_c, ''),
        '#',
        COALESCE(recordtypeid, ''),
        '#',
        COALESCE(id, ''),
        '#',
        COALESCE(accountid, '')
    ) NOT IN (
        SELECT
            hash_key
        FROM purgo_databricks.purgo_playground.patient_account
    )
GROUP BY
    patient_foundation_shipment,
    prescriber_name_c,
    prescriber_name_c_address,
    patinet_c,
    recordtypeid,
    id,
    accountid;

-- Validation Query: CTE for inserted test data
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
    FROM purgo_databricks.purgo_playground.patient_account
)
SELECT * FROM inserted_data;
