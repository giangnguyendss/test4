/*
==========================================================================================
  Databricks SQL Test Suite for purgo_playground.f_inv_movmnt ETL Logic and Data Validation
  - All test code is executable SQL with assertions and validation queries
  - All comments are in block or line comment style as per requirements
  - All table and function references are fully qualified
  - All constraints, schema, and data type checks are included
  - All error and warning scenarios are validated via assertions or error logs
==========================================================================================
*/

/*
==========================================================================================
  SECTION: SETUP - Clean up and Prepare Test Environment
==========================================================================================
*/

-- Drop and recreate the target table for test isolation
DROP TABLE IF EXISTS purgo_playground.f_inv_movmnt;

CREATE TABLE purgo_playground.f_inv_movmnt (
  txn_id STRING NOT NULL,
  inv_loc STRING,
  financial_qty DOUBLE,
  net_qty DOUBLE,
  expired_qt DECIMAL(38,0),
  item_nbr STRING NOT NULL,
  unit_cost DOUBLE,
  uom_rate DOUBLE,
  plant_loc_cd STRING,
  inv_stock_reference STRING,
  stock_type STRING,
  qty_on_hand DOUBLE,
  qty_shipped DOUBLE,
  cancel_dt DECIMAL(38,0),
  flag_active STRING,
  crt_dt TIMESTAMP,
  updt_dt TIMESTAMP
);

-- Drop and recreate *_apl_qty* tables for schema validation
DROP TABLE IF EXISTS purgo_playground.f_inv_movmnt_apl_qty;
CREATE TABLE purgo_playground.f_inv_movmnt_apl_qty (
  txn_id STRING,
  ref_txn_qty DECIMAL(3,1),
  cumulative_txn_qty DECIMAL(4,1),
  cumulative_ref_ord_sched_qty DECIMAL(4,1),
  ref_ord_sched_qty DECIMAL(3,1),
  prior_cumulative_txn_qty DECIMAL(3,1),
  prior_cumulative_ref_ord_sched_qty DECIMAL(3,1),
  apl_qty DECIMAL(5,1)
);

DROP TABLE IF EXISTS purgo_playground.f_inv_movmnt_apl_qty_result;
CREATE TABLE purgo_playground.f_inv_movmnt_apl_qty_result (
  txn_id STRING,
  ref_txn_qty DECIMAL(3,1),
  cumulative_txn_qty DECIMAL(4,1),
  cumulative_ref_ord_sched_qty DECIMAL(4,1),
  ref_ord_sched_qty DECIMAL(3,1),
  prior_cumulative_txn_qty DECIMAL(3,1),
  prior_cumulative_ref_ord_sched_qty DECIMAL(3,1),
  apl_qty DECIMAL(5,1)
);

DROP TABLE IF EXISTS purgo_playground.f_inv_movmnt_apl_qty_test;
CREATE TABLE purgo_playground.f_inv_movmnt_apl_qty_test (
  txn_id STRING NOT NULL,
  ref_txn_qty DECIMAL(3,1),
  cumulative_txn_qty DECIMAL(4,1),
  cumulative_ref_ord_sched_qty DECIMAL(4,1),
  ref_ord_sched_qty DECIMAL(3,1),
  prior_cumulative_txn_qty DECIMAL(3,1),
  prior_cumulative_ref_ord_sched_qty DECIMAL(3,1),
  apl_qty DECIMAL(5,1)
);

-- Clean up any previous error log table
DROP TABLE IF EXISTS purgo_playground.f_inv_movmnt_test_log;
CREATE TABLE purgo_playground.f_inv_movmnt_test_log (
  log_level STRING,
  log_msg STRING,
  log_dt TIMESTAMP
);

/*
==========================================================================================
  SECTION: SCHEMA VALIDATION TESTS
==========================================================================================
*/

-- Validate that the number of columns in the test data matches the target table schema
WITH schema_info AS (
  SELECT COUNT(*) AS col_count
  FROM information_schema.columns
  WHERE table_schema = "purgo_playground" AND table_name = "f_inv_movmnt"
),
test_data_info AS (
  SELECT COUNT(*) AS col_count
  FROM (SELECT * FROM (
    WITH test_data AS (
      SELECT
        '10001|01|0001' AS txn_id,
        'L001' AS inv_loc,
        20.0 AS financial_qty,
        20.0 AS net_qty,
        CAST(20240101 AS DECIMAL(38,0)) AS expired_qt,
        'ABC123' AS item_nbr,
        10.00 AS unit_cost,
        0.76 AS uom_rate,
        '001' AS plant_loc_cd,
        'SUPP001' AS inv_stock_reference,
        'FG' AS stock_type,
        20.0 AS qty_on_hand,
        0.0 AS qty_shipped,
        NULL AS cancel_dt,
        'no' AS flag_active,
        TIMESTAMP("2024-03-21T00:00:00.000+0000") AS crt_dt,
        TIMESTAMP("2024-03-21T00:00:00.000+0000") AS updt_dt
    )
    SELECT * FROM test_data
  ))
)
SELECT
  CASE WHEN schema_info.col_count = test_data_info.col_count THEN "PASS" ELSE "FAIL" END AS schema_column_count_match
FROM schema_info, test_data_info;

-- Validate data types for all columns in f_inv_movmnt
WITH expected_types AS (
  SELECT "txn_id" AS col, "STRING" AS typ UNION ALL
  SELECT "inv_loc", "STRING" UNION ALL
  SELECT "financial_qty", "DOUBLE" UNION ALL
  SELECT "net_qty", "DOUBLE" UNION ALL
  SELECT "expired_qt", "DECIMAL(38,0)" UNION ALL
  SELECT "item_nbr", "STRING" UNION ALL
  SELECT "unit_cost", "DOUBLE" UNION ALL
  SELECT "uom_rate", "DOUBLE" UNION ALL
  SELECT "plant_loc_cd", "STRING" UNION ALL
  SELECT "inv_stock_reference", "STRING" UNION ALL
  SELECT "stock_type", "STRING" UNION ALL
  SELECT "qty_on_hand", "DOUBLE" UNION ALL
  SELECT "qty_shipped", "DOUBLE" UNION ALL
  SELECT "cancel_dt", "DECIMAL(38,0)" UNION ALL
  SELECT "flag_active", "STRING" UNION ALL
  SELECT "crt_dt", "TIMESTAMP" UNION ALL
  SELECT "updt_dt", "TIMESTAMP"
),
actual_types AS (
  SELECT column_name AS col, data_type AS typ
  FROM information_schema.columns
  WHERE table_schema = "purgo_playground" AND table_name = "f_inv_movmnt"
)
SELECT
  e.col,
  e.typ AS expected_type,
  a.typ AS actual_type,
  CASE WHEN e.typ = a.typ THEN "PASS" ELSE "FAIL" END AS type_match
FROM expected_types e
LEFT JOIN actual_types a ON e.col = a.col;

/*
==========================================================================================
  SECTION: LOAD TEST DATA
==========================================================================================
*/

-- Insert all test data into the target table
INSERT INTO purgo_playground.f_inv_movmnt
WITH test_data AS (
  SELECT
    '10001|01|0001' AS txn_id,
    'L001' AS inv_loc,
    20.0 AS financial_qty,
    20.0 AS net_qty,
    CAST(20240101 AS DECIMAL(38,0)) AS expired_qt,
    'ABC123' AS item_nbr,
    10.00 AS unit_cost,
    0.76 AS uom_rate,
    '001' AS plant_loc_cd,
    'SUPP001' AS inv_stock_reference,
    'FG' AS stock_type,
    20.0 AS qty_on_hand,
    0.0 AS qty_shipped,
    NULL AS cancel_dt,
    'no' AS flag_active,
    TIMESTAMP("2024-03-21T00:00:00.000+0000") AS crt_dt,
    TIMESTAMP("2024-03-21T00:00:00.000+0000") AS updt_dt
  UNION ALL
  SELECT
    '10002|02|0002', 'L002', 14.0, 28.0, CAST(20240102 AS DECIMAL(38,0)), 'DEF456', 30.00, 0.76, '002', 'DUNN002', 'RAW', 28.0, 0.0, NULL, 'no',
    TIMESTAMP("2024-03-21T01:00:00.000+0000"), TIMESTAMP("2024-03-21T01:00:00.000+0000")
  UNION ALL
  SELECT
    '10003|03|0003', 'L003', 0.0, 0.0, CAST(99991231 AS DECIMAL(38,0)), 'GHI789', 0.00, 0.76, '003', 'DUNN456', 'WIP', 0.0, 0.0, NULL, 'no',
    TIMESTAMP("2024-03-21T02:00:00.000+0000"), TIMESTAMP("2024-03-21T02:00:00.000+0000")
  UNION ALL
  SELECT
    '20001|01|0001', 'none', 0.0, 0.0, CAST(99991231 AS DECIMAL(38,0)), 'JKL012', 0.00, 0.76, 'none', 'None', 'OT', 0.0, 0.0, NULL, 'no',
    CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
  UNION ALL
  SELECT
    '30001|01|0001', 'L005', 12.0, 10.0, CAST(20240105 AS DECIMAL(38,0)), 'MNO345', 15.00, 0.76, '005', 'SUPP2', 'FG', 10.0, 2.0, NULL, 'yes',
    TIMESTAMP("2024-03-21T03:00:00.000+0000"), TIMESTAMP("2024-03-21T03:00:00.000+0000")
  UNION ALL
  SELECT
    '40001|01|0001', 'L006', 5.0, 5.0, CAST(99991231 AS DECIMAL(38,0)), 'PQR678', 5.00, 0.76, '006', 'None', 'OT', 5.0, 0.0, NULL, 'no',
    TIMESTAMP("2024-03-21T04:00:00.000+0000"), TIMESTAMP("2024-03-21T04:00:00.000+0000")
  UNION ALL
  SELECT
    '50001|01|0001', 'L007', 8.0, 8.0, CAST(20240107 AS DECIMAL(38,0)), 'STU901', 8.00, 0.76, '007', 'SUPP007', 'FG', 8.0, 0.0, CAST(20240320 AS DECIMAL(38,0)), 'no',
    TIMESTAMP("2024-03-21T05:00:00.000+0000"), TIMESTAMP("2024-03-21T05:00:00.000+0000")
  UNION ALL
  SELECT
    '60001|01|0001', 'L008', 9.0, 9.0, CAST(20240108 AS DECIMAL(38,0)), '特殊字符-ßΩ', 9.99, 0.76, '008', 'SUPP008', 'FG', 9.0, 0.0, NULL, 'yes',
    TIMESTAMP("2024-03-21T06:00:00.000+0000"), TIMESTAMP("2024-03-21T06:00:00.000+0000")
  UNION ALL
  SELECT
    '70001|01|0001', 'L009', 0.0, 0.0, CAST(20240109 AS DECIMAL(38,0)), 'DIV0', 0.00, 0.76, '009', 'None', 'OT', 0.0, 0.0, NULL, 'no',
    TIMESTAMP("2024-03-21T07:00:00.000+0000"), TIMESTAMP("2024-03-21T07:00:00.000+0000")
  UNION ALL
  SELECT
    '80001|01|0001', 'L010', 11.0, 11.0, CAST(20240110 AS DECIMAL(38,0)), 'NOP123', 11.00, 0.76, '010', 'None', 'RAW', 11.0, 0.0, NULL, 'no',
    TIMESTAMP("2024-03-21T08:00:00.000+0000"), TIMESTAMP("2024-03-21T08:00:00.000+0000")
  UNION ALL
  SELECT
    '90001|01|0001', 'L011', 13.0, 13.0, CAST(20240111 AS DECIMAL(38,0)), 'XYZ789', 13.00, 0.76, '011', 'None', 'OT', 13.0, 0.0, NULL, 'no',
    TIMESTAMP("2024-03-21T09:00:00.000+0000"), TIMESTAMP("2024-03-21T09:00:00.000+0000")
  UNION ALL
  SELECT
    '10011|01|0001', 'L012', 7.0, 7.0, CAST(20240112 AS DECIMAL(38,0)), 'UNK001', 7.00, 0.76, '012', 'SUPP012', 'OT', 7.0, 0.0, NULL, 'no',
    TIMESTAMP("2024-03-21T10:00:00.000+0000"), TIMESTAMP("2024-03-21T10:00:00.000+0000")
  UNION ALL
  SELECT
    '10012|01|0001', 'L013', 6.0, 6.0, CAST(20240113 AS DECIMAL(38,0)), 'YES001', 6.00, 0.76, '013', 'SUPP013', 'FG', 6.0, 0.0, NULL, 'yes',
    TIMESTAMP("2024-03-21T11:00:00.000+0000"), TIMESTAMP("2024-03-21T11:00:00.000+0000")
  UNION ALL
  SELECT
    '10013|01|0001', 'L014', 5.0, 5.0, CAST(20240114 AS DECIMAL(38,0)), 'NO001', 5.00, 0.76, '014', 'SUPP014', 'FG', 5.0, 0.0, NULL, 'no',
    TIMESTAMP("2024-03-21T12:00:00.000+0000"), TIMESTAMP("2024-03-21T12:00:00.000+0000")
  UNION ALL
  SELECT
    '10014|01|0001', 'L015', 20.0, 15.0, CAST(20240115 AS DECIMAL(38,0)), 'SHIP001', 12.00, 0.76, '015', 'SUPP015', 'FG', 15.0, 5.0, NULL, 'yes',
    TIMESTAMP("2024-03-21T13:00:00.000+0000"), TIMESTAMP("2024-03-21T13:00:00.000+0000")
  UNION ALL
  SELECT
    '10015|01|0001', 'L016', 10.0, 0.0, CAST(20240116 AS DECIMAL(38,0)), 'SHIP002', 8.00, 0.76, '016', 'SUPP016', 'FG', 0.0, 10.0, NULL, 'no',
    TIMESTAMP("2024-03-21T14:00:00.000+0000"), TIMESTAMP("2024-03-21T14:00:00.000+0000")
  UNION ALL
  SELECT
    '10016|01|0001', 'L017', 5.0, 5.0, CAST(20240117 AS DECIMAL(38,0)), 'SHIP003', 6.00, 0.76, '017', 'SUPP017', 'FG', 5.0, 0.0, NULL, 'yes',
    TIMESTAMP("2024-03-21T15:00:00.000+0000"), TIMESTAMP("2024-03-21T15:00:00.000+0000")
  UNION ALL
  SELECT
    '10017|01|0001', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
    CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
  UNION ALL
  SELECT
    '10018|01|0001', '特殊L018', 18.0, 18.0, CAST(20240118 AS DECIMAL(38,0)), 'SPC001', 18.00, 0.76, '特殊P018', 'SUPP018', 'FG', 18.0, 0.0, NULL, 'yes',
    TIMESTAMP("2024-03-21T16:00:00.000+0000"), TIMESTAMP("2024-03-21T16:00:00.000+0000")
  UNION ALL
  SELECT
    '10019|01|0001', 'L019', 19.0, 19.0, CAST(99991231 AS DECIMAL(38,0)), 'MAX001', 19.00, 0.76, '019', 'SUPP019', 'FG', 19.0, 0.0, NULL, 'yes',
    TIMESTAMP("2024-03-21T17:00:00.000+0000"), TIMESTAMP("2024-03-21T17:00:00.000+0000")
  UNION ALL
  SELECT
    '10020|01|0001', 'L020', 20.0, 20.0, CAST(19000101 AS DECIMAL(38,0)), 'MIN001', 20.00, 0.76, '020', 'SUPP020', 'FG', 20.0, 0.0, NULL, 'yes',
    TIMESTAMP("2024-03-21T18:00:00.000+0000"), TIMESTAMP("2024-03-21T18:00:00.000+0000")
  UNION ALL
  SELECT
    '10021|01|0001', NULL, 21.0, 21.0, CAST(20240121 AS DECIMAL(38,0)), 'NULLS001', 21.00, 0.76, NULL, NULL, NULL, 21.0, 0.0, NULL, 'no',
    TIMESTAMP("2024-03-21T19:00:00.000+0000"), TIMESTAMP("2024-03-21T19:00:00.000+0000")
  UNION ALL
  SELECT
    '10022|01|0001', 'L022', 22.0, NULL, CAST(20240122 AS DECIMAL(38,0)), 'QNULL001', 22.00, 0.76, '022', 'SUPP022', 'FG', NULL, NULL, NULL, 'yes',
    TIMESTAMP("2024-03-21T20:00:00.000+0000"), TIMESTAMP("2024-03-21T20:00:00.000+0000")
  UNION ALL
  SELECT
    '10023|01|0001', 'L023', 23.0, 23.0, CAST(20240123 AS DECIMAL(38,0)), 'NOFLAG001', 23.00, 0.76, '023', 'SUPP023', 'FG', 23.0, 0.0, NULL, 'no',
    TIMESTAMP("2024-03-21T21:00:00.000+0000"), TIMESTAMP("2024-03-21T21:00:00.000+0000")
  UNION ALL
  SELECT
    '10024|01|0001', 'L024', 24.0, 24.0, CAST(20240124 AS DECIMAL(38,0)), 'ROUND001', 33.3333, 0.76, '024', 'SUPP024', 'FG', 24.0, 0.0, NULL, 'yes',
    TIMESTAMP("2024-03-21T22:00:00.000+0000"), TIMESTAMP("2024-03-21T22:00:00.000+0000")
  UNION ALL
  SELECT
    '10025|01|0001', 'L025', 25.0, 25.0, CAST(20240125 AS DECIMAL(38,0)), 'NONE001', 25.00, 0.76, '025', 'None', 'FG', 25.0, 0.0, NULL, 'yes',
    TIMESTAMP("2024-03-21T23:00:00.000+0000"), TIMESTAMP("2024-03-21T23:00:00.000+0000")
  UNION ALL
  SELECT
    '10026|01|0001', 'L026', 26.0, 26.0, CAST(20240126 AS DECIMAL(38,0)), 'FLAGSPC', 26.00, 0.76, '026', 'SUPP026', 'FG', 26.0, 0.0, NULL, 'yës',
    TIMESTAMP("2024-03-21T23:30:00.000+0000"), TIMESTAMP("2024-03-21T23:30:00.000+0000")
  UNION ALL
  SELECT
    RPAD('99999', 50, 'X') || '|' || RPAD('99', 10, 'Y') || '|' || RPAD('9999', 10, 'Z'),
    RPAD('L999', 30, 'Ω'),
    99.99, 99.99, CAST(20241231 AS DECIMAL(38,0)),
    RPAD('ITEM999', 40, 'ß'),
    99.99, 0.76,
    RPAD('PLANT999', 30, '€'),
    RPAD('SUPP999', 30, '¥'),
    'FG',
    99.99, 0.0, NULL, 'yes',
    TIMESTAMP("2024-03-21T23:59:59.000+0000"), TIMESTAMP("2024-03-21T23:59:59.000+0000")
  UNION ALL
  SELECT
    '10027|01|0001', 'L027', 0.0, 0.0, CAST(19000101 AS DECIMAL(38,0)), 'MINNUM', 0.00, 0.76, '027', 'SUPP027', 'FG', 0.0, 0.0, NULL, 'no',
    TIMESTAMP("2024-03-21T00:00:01.000+0000"), TIMESTAMP("2024-03-21T00:00:01.000+0000")
  UNION ALL
  SELECT
    '10028|01|0001', 'L028', 9999999999.99, 9999999999.99, CAST(99991231 AS DECIMAL(38,0)), 'MAXNUM', 9999999999.99, 0.76, '028', 'SUPP028', 'FG', 9999999999.99, 0.0, NULL, 'yes',
    TIMESTAMP("2024-03-21T00:00:02.000+0000"), TIMESTAMP("2024-03-21T00:00:02.000+0000")
)
SELECT * FROM test_data
;

/*
==========================================================================================
  SECTION: DATA QUALITY VALIDATION TESTS
==========================================================================================
*/

-- Validate NOT NULL constraints for txn_id and item_nbr
SELECT COUNT(*) AS null_txn_id_count
FROM purgo_playground.f_inv_movmnt
WHERE txn_id IS NULL;

SELECT COUNT(*) AS null_item_nbr_count
FROM purgo_playground.f_inv_movmnt
WHERE item_nbr IS NULL;

-- Validate flag_active constraint (only "yes" or "no" allowed)
SELECT COUNT(*) AS invalid_flag_active_count
FROM purgo_playground.f_inv_movmnt
WHERE flag_active IS NOT NULL AND flag_active NOT IN ("yes", "no");

-- Validate stock_type constraint (only allowed values)
SELECT COUNT(*) AS invalid_stock_type_count
FROM purgo_playground.f_inv_movmnt
WHERE stock_type IS NOT NULL AND stock_type NOT IN ("RAW", "WIP", "FG", "OT");

-- Validate uniqueness of txn_id
WITH txn_id_dupes AS (
  SELECT txn_id, COUNT(*) AS cnt
  FROM purgo_playground.f_inv_movmnt
  GROUP BY txn_id
  HAVING COUNT(*) > 1
)
SELECT COUNT(*) AS duplicate_txn_id_count FROM txn_id_dupes;

-- Validate default values for inv_loc, plant_loc_cd, stock_type, unit_cost, uom_rate, qty_shipped, flag_active
SELECT COUNT(*) AS default_value_failures
FROM purgo_playground.f_inv_movmnt
WHERE (inv_loc IS NULL OR inv_loc = "none")
  AND (plant_loc_cd IS NULL OR plant_loc_cd = "none")
  AND (stock_type IS NULL OR stock_type = "OT")
  AND (unit_cost IS NULL OR unit_cost = 0.00)
  AND (uom_rate IS NULL OR uom_rate = 0.76)
  AND (qty_shipped IS NULL OR qty_shipped = 0)
  AND (flag_active IS NULL OR flag_active = "no");

-- Validate expired_qt default (should be 99991231 if null)
SELECT COUNT(*) AS expired_qt_default_failures
FROM purgo_playground.f_inv_movmnt
WHERE expired_qt IS NULL OR expired_qt = 99991231;

-- Validate cancel_dt logic (should be set only for missing in today's data)
SELECT txn_id, cancel_dt
FROM purgo_playground.f_inv_movmnt
WHERE cancel_dt IS NOT NULL;

-- Validate unit_cost rounding to 2 decimals
SELECT txn_id, unit_cost
FROM purgo_playground.f_inv_movmnt
WHERE unit_cost IS NOT NULL AND unit_cost != ROUND(unit_cost, 2);

-- Validate qty_on_hand and qty_shipped calculation
SELECT txn_id, financial_qty, qty_on_hand, qty_shipped,
  (CASE WHEN qty_shipped = financial_qty - qty_on_hand THEN "PASS" ELSE "FAIL" END) AS qty_shipped_check
FROM purgo_playground.f_inv_movmnt
WHERE financial_qty IS NOT NULL AND qty_on_hand IS NOT NULL AND qty_shipped IS NOT NULL;

-- Validate inv_stock_reference logic for LZBEP = B, L, and others
SELECT txn_id, inv_stock_reference
FROM purgo_playground.f_inv_movmnt
WHERE inv_stock_reference IS NULL OR inv_stock_reference = "None";

-- Validate flag_active logic for dwart
SELECT txn_id, flag_active
FROM purgo_playground.f_inv_movmnt
WHERE flag_active IS NOT NULL AND flag_active NOT IN ("yes", "no");

-- Validate timestamp fields are not null
SELECT COUNT(*) AS null_crt_dt_count
FROM purgo_playground.f_inv_movmnt
WHERE crt_dt IS NULL;

SELECT COUNT(*) AS null_updt_dt_count
FROM purgo_playground.f_inv_movmnt
WHERE updt_dt IS NULL;

/*
==========================================================================================
  SECTION: DATA TYPE CONVERSION AND NULL HANDLING TESTS
==========================================================================================
*/

-- Test explicit casting and null handling for numeric and string fields
WITH conversion_test AS (
  SELECT
    CAST("123.45" AS DOUBLE) AS double_val,
    CAST(NULL AS DOUBLE) AS null_double,
    CAST("20240101" AS DECIMAL(38,0)) AS dec_val,
    CAST(NULL AS DECIMAL(38,0)) AS null_dec,
    CAST(NULL AS STRING) AS null_str
)
SELECT
  CASE WHEN double_val = 123.45 THEN "PASS" ELSE "FAIL" END AS double_cast,
  CASE WHEN null_double IS NULL THEN "PASS" ELSE "FAIL" END AS null_double_cast,
  CASE WHEN dec_val = 20240101 THEN "PASS" ELSE "FAIL" END AS dec_cast,
  CASE WHEN null_dec IS NULL THEN "PASS" ELSE "FAIL" END AS null_dec_cast,
  CASE WHEN null_str IS NULL THEN "PASS" ELSE "FAIL" END AS null_str_cast
FROM conversion_test;

-- Test array, struct, and map types (complex types) for compatibility
WITH complex_type_test AS (
  SELECT
    ARRAY("A", "B", "C") AS arr_val,
    NAMED_STRUCT("a", 1, "b", "X") AS struct_val,
    MAP("k1", "v1", "k2", "v2") AS map_val
)
SELECT
  CASE WHEN size(arr_val) = 3 THEN "PASS" ELSE "FAIL" END AS array_test,
  CASE WHEN struct_val.a = 1 AND struct_val.b = "X" THEN "PASS" ELSE "FAIL" END AS struct_test,
  CASE WHEN map_val["k1"] = "v1" AND map_val["k2"] = "v2" THEN "PASS" ELSE "FAIL" END AS map_test
FROM complex_type_test;

/*
==========================================================================================
  SECTION: DELTA LAKE OPERATIONS TESTS
==========================================================================================
*/

-- Test Delta Lake MERGE operation (simulate upsert)
MERGE INTO purgo_playground.f_inv_movmnt AS target
USING (
  SELECT
    '10001|01|0001' AS txn_id,
    'L001-upd' AS inv_loc,
    21.0 AS financial_qty,
    21.0 AS net_qty,
    CAST(20240101 AS DECIMAL(38,0)) AS expired_qt,
    'ABC123' AS item_nbr,
    11.00 AS unit_cost,
    0.76 AS uom_rate,
    '001' AS plant_loc_cd,
    'SUPP001' AS inv_stock_reference,
    'FG' AS stock_type,
    21.0 AS qty_on_hand,
    0.0 AS qty_shipped,
    NULL AS cancel_dt,
    'yes' AS flag_active,
    CURRENT_TIMESTAMP() AS crt_dt,
    CURRENT_TIMESTAMP() AS updt_dt
) AS source
ON target.txn_id = source.txn_id
WHEN MATCHED THEN
  UPDATE SET
    inv_loc = source.inv_loc,
    financial_qty = source.financial_qty,
    net_qty = source.net_qty,
    expired_qt = source.expired_qt,
    item_nbr = source.item_nbr,
    unit_cost = source.unit_cost,
    uom_rate = source.uom_rate,
    plant_loc_cd = source.plant_loc_cd,
    inv_stock_reference = source.inv_stock_reference,
    stock_type = source.stock_type,
    qty_on_hand = source.qty_on_hand,
    qty_shipped = source.qty_shipped,
    cancel_dt = source.cancel_dt,
    flag_active = source.flag_active,
    crt_dt = source.crt_dt,
    updt_dt = source.updt_dt
WHEN NOT MATCHED THEN
  INSERT *;

-- Validate the update
SELECT inv_loc, financial_qty, flag_active
FROM purgo_playground.f_inv_movmnt
WHERE txn_id = '10001|01|0001';

-- Test Delta Lake DELETE operation
DELETE FROM purgo_playground.f_inv_movmnt WHERE txn_id = '10028|01|0001';

-- Validate the delete
SELECT COUNT(*) AS deleted_count
FROM purgo_playground.f_inv_movmnt
WHERE txn_id = '10028|01|0001';

/*
==========================================================================================
  SECTION: WINDOW FUNCTION AND ANALYTICS TESTS
==========================================================================================
*/

-- Test window function: row_number over partition by stock_type
WITH win_test AS (
  SELECT
    txn_id,
    stock_type,
    ROW_NUMBER() OVER (PARTITION BY stock_type ORDER BY crt_dt) AS rn
  FROM purgo_playground.f_inv_movmnt
)
SELECT stock_type, COUNT(*) AS cnt, MAX(rn) AS max_rn
FROM win_test
GROUP BY stock_type;

/*
==========================================================================================
  SECTION: ERROR AND WARNING LOGGING TESTS
==========================================================================================
*/

-- Log error for invalid flag_active value
INSERT INTO purgo_playground.f_inv_movmnt_test_log
SELECT
  "ERROR" AS log_level,
  "Invalid flag_active value detected: " || flag_active AS log_msg,
  CURRENT_TIMESTAMP() AS log_dt
FROM purgo_playground.f_inv_movmnt
WHERE flag_active IS NOT NULL AND flag_active NOT IN ("yes", "no");

-- Log error for duplicate txn_id
INSERT INTO purgo_playground.f_inv_movmnt_test_log
SELECT
  "ERROR" AS log_level,
  "Duplicate txn_id detected: " || txn_id AS log_msg,
  CURRENT_TIMESTAMP() AS log_dt
FROM (
  SELECT txn_id
  FROM purgo_playground.f_inv_movmnt
  GROUP BY txn_id
  HAVING COUNT(*) > 1
);

-- Log error for expired_dt/expired_qt mapping ambiguity
INSERT INTO purgo_playground.f_inv_movmnt_test_log
SELECT
  "ERROR" AS log_level,
  "Field mapping ambiguity: expired_dt in mapping, expired_qt in schema" AS log_msg,
  CURRENT_TIMESTAMP() AS log_dt
WHERE EXISTS (
  SELECT 1
  FROM information_schema.columns
  WHERE table_schema = "purgo_playground" AND table_name = "f_inv_movmnt"
    AND column_name = "expired_qt"
);

-- Log error for missing mapping for *_apl_qty* tables
INSERT INTO purgo_playground.f_inv_movmnt_test_log
SELECT
  "ERROR" AS log_level,
  "Missing mapping and transformation logic for *_apl_qty* tables" AS log_msg,
  CURRENT_TIMESTAMP() AS log_dt;

-- Log warning for missing data volume/partitioning requirements
INSERT INTO purgo_playground.f_inv_movmnt_test_log
SELECT
  "WARNING" AS log_level,
  "Data volume and partitioning requirements not specified" AS log_msg,
  CURRENT_TIMESTAMP() AS log_dt;

-- Log warning for missing source table refresh cadence
INSERT INTO purgo_playground.f_inv_movmnt_test_log
SELECT
  "WARNING" AS log_level,
  "Source table refresh cadence not specified" AS log_msg,
  CURRENT_TIMESTAMP() AS log_dt;

-- Show all error/warning logs
SELECT * FROM purgo_playground.f_inv_movmnt_test_log;

/*
==========================================================================================
  SECTION: CLEANUP
==========================================================================================
*/

-- Clean up test log table
DROP TABLE IF EXISTS purgo_playground.f_inv_movmnt_test_log;
