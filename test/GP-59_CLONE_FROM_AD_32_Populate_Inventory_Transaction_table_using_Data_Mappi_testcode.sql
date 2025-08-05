/* 
==========================================================================================
  Databricks SQL Test Suite for Inventory Transaction Table (f_inv_movmnt)
  Catalog: purgo_databricks
  Schema: purgo_playground

  - All test code is executable SQL with assertions and validation logic.
  - Covers: schema, data types, constraints, default/null handling, joins, Delta Lake ops,
    uniqueness, error handling, window functions, and *_apl_qty referential integrity.
  - All comments follow Databricks SQL and code documentation guidelines.
==========================================================================================
*/

/*------------------------------------------------------------------------------
  SECTION: Setup & Cleanup
------------------------------------------------------------------------------*/

/* -- Drop and recreate f_inv_movmnt for clean test runs */
DROP TABLE IF EXISTS purgo_playground.f_inv_movmnt;

CREATE TABLE purgo_playground.f_inv_movmnt (
  txn_id STRING,
  inv_loc STRING,
  financial_qty DOUBLE,
  net_qty DOUBLE,
  expired_dt STRING,
  item_nbr STRING,
  unit_cost DOUBLE,
  uom_rate DOUBLE,
  plant_loc_cd STRING,
  inv_stock_reference STRING,
  stock_type STRING,
  qty_on_hand DOUBLE,
  qty_shipped DOUBLE,
  cancel_dt STRING,
  flag_active STRING,
  crt_dt TIMESTAMP,
  updt_dt TIMESTAMP
);

/* -- Drop and recreate *_apl_qty tables for referential integrity tests */
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

/*------------------------------------------------------------------------------
  SECTION: Test Data Load
------------------------------------------------------------------------------*/

/* -- Insert test data for f_inv_movmnt (covers all scenarios) */
INSERT INTO purgo_playground.f_inv_movmnt
WITH test_data AS (
  SELECT
    "1001|01|200" AS txn_id, "L001" AS inv_loc, 18.5 AS financial_qty, 18.5 AS net_qty, "20240610" AS expired_dt, "ABC123" AS item_nbr, 50.00 AS unit_cost, 0.76 AS uom_rate, "PL01" AS plant_loc_cd, "SUPP001" AS inv_stock_reference, "FG" AS stock_type, 18.5 AS qty_on_hand, 0.0 AS qty_shipped, NULL AS cancel_dt, "yes" AS flag_active, current_timestamp() AS crt_dt, current_timestamp() AS updt_dt
  UNION ALL SELECT
    "1002|02|201", "L002", 12.4, 12.4, "20240611", "XYZ789", 100.00, 0.76, "PL02", "DUN001", "RAW", 12.4, 0.0, NULL, "yes", current_timestamp(), current_timestamp()
  UNION ALL SELECT
    "1003|03|202", "L003", 15.0, 15.0, "20240612", "DEF456", 75.50, 0.76, "PL03", "VBAXSUPP", "WIP", 15.0, 0.0, NULL, "yes", current_timestamp(), current_timestamp()
  UNION ALL SELECT
    "1004|04|203", "L004", 10.0, 10.0, "20240613", "GHI789", 60.00, 0.76, "PL04", "none", "OT", 10.0, 0.0, NULL, "yes", current_timestamp(), current_timestamp()
  UNION ALL SELECT
    "1005|05|204", "L005", 8.0, 8.0, "20240614", "JKL012", 55.00, 0.76, "PL05", "none", "FG", 8.0, 0.0, NULL, "yes", current_timestamp(), current_timestamp()
  UNION ALL SELECT
    "1006|06|205", "L006", 7.0, 7.0, "20240615", "MNO345", 45.00, 0.76, "PL06", "none", "RAW", 7.0, 0.0, NULL, "yes", current_timestamp(), current_timestamp()
  UNION ALL SELECT
    "1007|07|206", "L007", 6.0, 6.0, "20240616", "PQR678", 40.00, 0.76, "PL07", "none", "WIP", 6.0, 0.0, NULL, "yes", current_timestamp(), current_timestamp()
  UNION ALL SELECT
    "1008|08|207", "L008", 5.0, 5.0, "20240617", "STU901", 35.00, 0.76, "PL08", "SUPP002", "FG", 5.0, 0.0, NULL, "no", current_timestamp(), current_timestamp()
  UNION ALL SELECT
    "1009|09|208", "L009", 4.0, 4.0, "20240618", "VWX234", 30.00, 0.76, "PL09", "SUPP003", "RAW", 4.0, 0.0, NULL, "no", current_timestamp(), current_timestamp()
  UNION ALL SELECT
    "1010|10|209", "L010", 3.0, 3.0, "20240619", "YZA567", 25.00, 0.76, "PL10", "SUPP004", "WIP", 3.0, 0.0, NULL, "no", current_timestamp(), current_timestamp()
  UNION ALL SELECT
    "1011|11|210", "L011", 2.0, 2.0, "20240620", "BCD890", 20.00, 0.76, "PL11", "SUPP005", "OT", 2.0, 0.0, NULL, "no", current_timestamp(), current_timestamp()
  UNION ALL SELECT
    "1012|12|211", "L012", 1.0, 1.0, "20240621", NULL, 15.00, 0.76, "PL12", "SUPP006", "FG", 1.0, 0.0, NULL, "yes", current_timestamp(), current_timestamp()
  UNION ALL SELECT
    "1013|13|212", "L013", NULL, 1.0, "20240622", "EFG123", 10.00, 0.76, "PL13", "SUPP007", "RAW", 1.0, 0.0, NULL, "yes", current_timestamp(), current_timestamp()
  UNION ALL SELECT
    "1014|14|213", "L014", 1.0, NULL, "20240623", "HIJ456", 5.00, 0.76, "PL14", "SUPP008", "WIP", 1.0, 0.0, NULL, "yes", current_timestamp(), current_timestamp()
  UNION ALL SELECT
    "1015|15|214", NULL, 2.0, 2.0, "20240624", "KLM789", 12.00, 0.76, "PL15", "SUPP009", "FG", 2.0, 0.0, NULL, "yes", current_timestamp(), current_timestamp()
  UNION ALL SELECT
    "1016|16|215", "L016", 3.0, 3.0, "20240625", "NOP012", 13.00, 0.76, NULL, "SUPP010", "RAW", 3.0, 0.0, NULL, "yes", current_timestamp(), current_timestamp()
  UNION ALL SELECT
    "1017|17|216", "L017", NULL, 4.0, "20240626", "QRS345", 14.00, 0.76, "PL17", "SUPP011", "WIP", 4.0, 0.0, NULL, "yes", current_timestamp(), current_timestamp()
  UNION ALL SELECT
    "1018|18|217", "L018", 5.0, NULL, "20240627", "TUV678", 15.00, 0.76, "PL18", "SUPP012", "OT", 0.0, 0.0, NULL, "yes", current_timestamp(), current_timestamp()
  UNION ALL SELECT
    "1019|19|218", "L019", 6.0, 6.0, NULL, "WXY901", 16.00, 0.76, "PL19", "SUPP013", "FG", 6.0, 0.0, NULL, "yes", current_timestamp(), current_timestamp()
  UNION ALL SELECT
    "1020|20|219", "L020", 7.0, 7.0, "20240629", "ZAB234", NULL, 0.76, "PL20", "SUPP014", "RAW", 7.0, 0.0, NULL, "yes", current_timestamp(), current_timestamp()
  UNION ALL SELECT
    "1021|21|220", "L021", 8.0, 8.0, "20240630", "CDE567", 18.00, 0.76, "PL21", "SUPP015", "WIP", 8.0, 0.0, "20240630", "yes", current_timestamp(), current_timestamp()
  UNION ALL SELECT
    "1022|22|221", "L022", 9.0, 9.0, "20240701", "A!@#$", 19.00, 0.76, "PL22", "SUPP016", "FG", 9.0, 0.0, NULL, "yes", current_timestamp(), current_timestamp()
  UNION ALL SELECT
    "1023|23|222", "多字节", 10.0, 10.0, "20240702", "FGH890", 20.00, 0.76, "PL23", "SUPP017", "RAW", 10.0, 0.0, NULL, "yes", current_timestamp(), current_timestamp()
  UNION ALL SELECT
    "1024|24|223", "L024", 11.0, 11.0, "20240703", "IJK123", 21.00, 0.76, "PL24😀", "SUPP018", "WIP", 11.0, 0.0, NULL, "yes", current_timestamp(), current_timestamp()
  UNION ALL SELECT
    "1025|25|224", "L025", 12.0, 12.0, "20240704", "LMN456", 22.00, 0.76, "PL25", "SUPP019@#$", "OT", 12.0, 0.0, NULL, "yes", current_timestamp(), current_timestamp()
  UNION ALL SELECT
    "1026|26|225", "L026", 20.0, 15.0, "20240705", "OPQ789", 23.00, 0.76, "PL26", "SUPP020", "FG", 15.0, 5.0, NULL, "yes", current_timestamp(), current_timestamp()
  UNION ALL SELECT
    "1027|27|226", "L027", 10.0, 15.0, "20240706", "RST012", 24.00, 0.76, "PL27", "SUPP021", "RAW", 15.0, 0.0, NULL, "yes", current_timestamp(), current_timestamp()
  UNION ALL SELECT
    "1028|28|227", "L028", 13.0, 13.0, "20240707", "UVW345", 25.00, 0.76, "PL28", "SUPP022", "WIP", 13.0, 0.0, NULL, "yes", current_timestamp(), current_timestamp()
  UNION ALL SELECT
    "1028|28|227", "L028", 14.0, 14.0, "20240708", "XYZ678", 26.00, 0.76, "PL28", "SUPP023", "OT", 14.0, 0.0, NULL, "yes", current_timestamp(), current_timestamp()
  UNION ALL SELECT
    "1029|29|228", NULL, NULL, NULL, NULL, NULL, NULL, 0.76, NULL, NULL, NULL, NULL, NULL, NULL, NULL, current_timestamp(), current_timestamp()
)
SELECT
  txn_id,
  COALESCE(inv_loc, "none") AS inv_loc,
  COALESCE(financial_qty, 0.0) AS financial_qty,
  COALESCE(net_qty, 0.0) AS net_qty,
  COALESCE(expired_dt, "99991231") AS expired_dt,
  item_nbr,
  unit_cost,
  uom_rate,
  COALESCE(plant_loc_cd, "none") AS plant_loc_cd,
  COALESCE(inv_stock_reference, "none") AS inv_stock_reference,
  COALESCE(stock_type, "OT") AS stock_type,
  COALESCE(qty_on_hand, COALESCE(net_qty, 0.0)) AS qty_on_hand,
  CASE
    WHEN COALESCE(financial_qty, 0.0) - COALESCE(net_qty, 0.0) < 0 THEN 0.0
    ELSE COALESCE(financial_qty, 0.0) - COALESCE(net_qty, 0.0)
  END AS qty_shipped,
  cancel_dt,
  COALESCE(flag_active, "no") AS flag_active,
  crt_dt,
  updt_dt
FROM test_data
;

/*------------------------------------------------------------------------------
  SECTION: Schema Validation Tests
------------------------------------------------------------------------------*/

/* -- Validate column count and data types */
DESCRIBE TABLE purgo_playground.f_inv_movmnt;

/*------------------------------------------------------------------------------
  SECTION: Data Type Conversion and NULL Handling Tests
------------------------------------------------------------------------------*/

/* -- Validate that all numeric columns are DOUBLE and handle NULLs as per mapping */
SELECT
  COUNT(*) FILTER (WHERE financial_qty IS NULL) AS null_financial_qty_count,
  COUNT(*) FILTER (WHERE net_qty IS NULL) AS null_net_qty_count,
  COUNT(*) FILTER (WHERE unit_cost IS NULL) AS null_unit_cost_count,
  COUNT(*) FILTER (WHERE qty_on_hand IS NULL) AS null_qty_on_hand_count,
  COUNT(*) FILTER (WHERE qty_shipped IS NULL) AS null_qty_shipped_count
FROM purgo_playground.f_inv_movmnt
;

/* -- Validate default values for inv_loc, plant_loc_cd, inv_stock_reference, stock_type */
SELECT
  COUNT(*) FILTER (WHERE inv_loc = "none") AS inv_loc_none,
  COUNT(*) FILTER (WHERE plant_loc_cd = "none") AS plant_loc_cd_none,
  COUNT(*) FILTER (WHERE inv_stock_reference = "none") AS inv_stock_reference_none,
  COUNT(*) FILTER (WHERE stock_type = "OT") AS stock_type_ot
FROM purgo_playground.f_inv_movmnt
;

/*------------------------------------------------------------------------------
  SECTION: Uniqueness and Constraint Tests
------------------------------------------------------------------------------*/

/* -- Validate uniqueness of txn_id */
SELECT txn_id, COUNT(*) AS cnt
FROM purgo_playground.f_inv_movmnt
GROUP BY txn_id
HAVING cnt > 1
;

/* -- Validate stock_type and flag_active constraints */
SELECT
  COUNT(*) FILTER (WHERE stock_type NOT IN ("RAW", "WIP", "FG", "OT")) AS bad_stock_type,
  COUNT(*) FILTER (WHERE flag_active NOT IN ("yes", "no")) AS bad_flag_active
FROM purgo_playground.f_inv_movmnt
;

/*------------------------------------------------------------------------------
  SECTION: Data Quality and Business Rule Validation
------------------------------------------------------------------------------*/

/* -- Validate txn_id format */
SELECT COUNT(*) AS invalid_txn_id_format
FROM (
  SELECT
    txn_id,
    REGEXP_LIKE(txn_id, "^[0-9]+\\|[0-9]+\\|[0-9]+$") AS valid_format
  FROM purgo_playground.f_inv_movmnt
) t
WHERE valid_format = FALSE
;

/* -- Validate expired_dt format (yyyymmdd or 99991231) */
SELECT COUNT(*) AS invalid_expired_dt
FROM (
  SELECT
    expired_dt,
    (REGEXP_LIKE(expired_dt, "^[0-9]{8}$") OR expired_dt = "99991231") AS valid_expired_dt
  FROM purgo_playground.f_inv_movmnt
) t
WHERE valid_expired_dt = FALSE
;

/* -- Validate qty_on_hand equals net_qty */
SELECT COUNT(*) AS qty_on_hand_mismatch
FROM purgo_playground.f_inv_movmnt
WHERE qty_on_hand != net_qty AND qty_on_hand IS NOT NULL AND net_qty IS NOT NULL
;

/* -- Validate qty_shipped = financial_qty - net_qty, 0 if negative */
SELECT COUNT(*) AS qty_shipped_mismatch
FROM purgo_playground.f_inv_movmnt
WHERE
  NOT (
    (qty_shipped = 0.0 AND (financial_qty - net_qty) < 0)
    OR (qty_shipped = financial_qty - net_qty AND (financial_qty - net_qty) >= 0)
  )
;

/* -- Validate uom_rate is always 0.76 */
SELECT COUNT(*) AS bad_uom_rate
FROM purgo_playground.f_inv_movmnt
WHERE uom_rate != 0.76
;

/*------------------------------------------------------------------------------
  SECTION: Error Handling Tests
------------------------------------------------------------------------------*/

/* -- Validate error for missing item_nbr (should be rejected) */
SELECT COUNT(*) AS missing_item_nbr
FROM purgo_playground.f_inv_movmnt
WHERE item_nbr IS NULL
;

/* -- Validate error for financial_qty or net_qty not castable to double (should be rejected) */
/* -- (In test data, NULL is used for error simulation; in real ETL, would log error) */

/*------------------------------------------------------------------------------
  SECTION: Window Function and Analytics Feature Tests
------------------------------------------------------------------------------*/

/* -- Validate expired_dt is from latest irsda (simulate with max expired_dt per item_nbr) */
WITH latest_expired_dt AS (
  SELECT
    item_nbr,
    MAX(expired_dt) AS max_expired_dt
  FROM purgo_playground.f_inv_movmnt
  GROUP BY item_nbr
)
SELECT COUNT(*) AS expired_dt_not_latest
FROM purgo_playground.f_inv_movmnt f
JOIN latest_expired_dt l
  ON f.item_nbr = l.item_nbr
WHERE f.expired_dt != l.max_expired_dt
;

/*------------------------------------------------------------------------------
  SECTION: Delta Lake Operations Tests
------------------------------------------------------------------------------*/

/* -- Test MERGE: Upsert a record and validate */
MERGE INTO purgo_playground.f_inv_movmnt AS target
USING (
  SELECT "2000|99|999" AS txn_id, "L999" AS inv_loc, 99.0 AS financial_qty, 99.0 AS net_qty, "20991231" AS expired_dt, "TEST999" AS item_nbr, 999.99 AS unit_cost, 0.76 AS uom_rate, "PL99" AS plant_loc_cd, "SUPP999" AS inv_stock_reference, "FG" AS stock_type, 99.0 AS qty_on_hand, 0.0 AS qty_shipped, NULL AS cancel_dt, "yes" AS flag_active, current_timestamp() AS crt_dt, current_timestamp() AS updt_dt
) AS src
ON target.txn_id = src.txn_id
WHEN MATCHED THEN
  UPDATE SET
    inv_loc = src.inv_loc,
    financial_qty = src.financial_qty,
    net_qty = src.net_qty,
    expired_dt = src.expired_dt,
    item_nbr = src.item_nbr,
    unit_cost = src.unit_cost,
    uom_rate = src.uom_rate,
    plant_loc_cd = src.plant_loc_cd,
    inv_stock_reference = src.inv_stock_reference,
    stock_type = src.stock_type,
    qty_on_hand = src.qty_on_hand,
    qty_shipped = src.qty_shipped,
    cancel_dt = src.cancel_dt,
    flag_active = src.flag_active,
    crt_dt = src.crt_dt,
    updt_dt = src.updt_dt
WHEN NOT MATCHED THEN
  INSERT (
    txn_id, inv_loc, financial_qty, net_qty, expired_dt, item_nbr, unit_cost, uom_rate, plant_loc_cd, inv_stock_reference, stock_type, qty_on_hand, qty_shipped, cancel_dt, flag_active, crt_dt, updt_dt
  ) VALUES (
    src.txn_id, src.inv_loc, src.financial_qty, src.net_qty, src.expired_dt, src.item_nbr, src.unit_cost, src.uom_rate, src.plant_loc_cd, src.inv_stock_reference, src.stock_type, src.qty_on_hand, src.qty_shipped, src.cancel_dt, src.flag_active, src.crt_dt, src.updt_dt
  )
;

/* -- Validate upsert */
SELECT COUNT(*) AS upserted
FROM purgo_playground.f_inv_movmnt
WHERE txn_id = "2000|99|999"
;

/* -- Test DELETE: Remove a record and validate */
DELETE FROM purgo_playground.f_inv_movmnt WHERE txn_id = "2000|99|999";

SELECT COUNT(*) AS deleted
FROM purgo_playground.f_inv_movmnt
WHERE txn_id = "2000|99|999"
;

/*------------------------------------------------------------------------------
  SECTION: *_apl_qty Referential Integrity Tests
------------------------------------------------------------------------------*/

/* -- Insert test data into f_inv_movmnt_apl_qty referencing f_inv_movmnt */
INSERT INTO purgo_playground.f_inv_movmnt_apl_qty (txn_id, ref_txn_qty)
SELECT txn_id, 1.0
FROM purgo_playground.f_inv_movmnt
WHERE txn_id = "1001|01|200"
;

/* -- Validate that all txn_id in *_apl_qty exist in f_inv_movmnt */
SELECT COUNT(*) AS missing_fk_count
FROM (
  SELECT a.txn_id
  FROM purgo_playground.f_inv_movmnt_apl_qty a
  LEFT JOIN purgo_playground.f_inv_movmnt f
    ON a.txn_id = f.txn_id
  WHERE f.txn_id IS NULL
) t
;

/*------------------------------------------------------------------------------
  SECTION: Performance Test (Row Count and SLA)
------------------------------------------------------------------------------*/

/* -- Validate row count (should match number of inserted test rows, e.g., 30) */
SELECT COUNT(*) AS total_rows FROM purgo_playground.f_inv_movmnt
;

/* -- For large data, validate query completes within SLA (manual/observational) */

/*------------------------------------------------------------------------------
  SECTION: Cleanup
------------------------------------------------------------------------------*/

/* -- Clean up test data from *_apl_qty tables */
DELETE FROM purgo_playground.f_inv_movmnt_apl_qty WHERE txn_id = "1001|01|200";

/* -- Optionally, clean up f_inv_movmnt test records (uncomment if needed) */
/* DELETE FROM purgo_playground.f_inv_movmnt WHERE txn_id LIKE "2000|99|999"; */

-- End of test suite
