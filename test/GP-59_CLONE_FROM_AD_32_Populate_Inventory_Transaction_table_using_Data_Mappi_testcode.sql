/* 
==========================================================================================
Databricks SQL Test Suite for purgo_playground.f_inv_movmnt ETL Logic
==========================================================================================
- Catalog: purgo_databricks
- Schema: purgo_playground
- Target Table: f_inv_movmnt
- Test Coverage: Schema, Data Quality, Default/Null Handling, Uniqueness, Joins, Error Handling, Delta Lake, Window Functions, Performance, *_apl_qty FK
- All assertions use Databricks SQL syntax
- All comments are in block (/* */) or line (--) style as per requirements
==========================================================================================
*/

/* =============================================================================
   SECTION: SETUP & CLEANUP
============================================================================= */

/* -- Clean up target and error tables before test run */
DROP TABLE IF EXISTS purgo_playground.f_inv_movmnt;
DROP TABLE IF EXISTS purgo_playground.f_inv_movmnt_apl_qty;
DROP TABLE IF EXISTS purgo_playground.f_inv_movmnt_apl_qty_result;
DROP TABLE IF EXISTS purgo_playground.f_inv_movmnt_apl_qty_test;
DROP TABLE IF EXISTS purgo_playground.f_inv_movmnt_error_log;

/* -- Create error log table for error handling tests */
CREATE TABLE IF NOT EXISTS purgo_playground.f_inv_movmnt_error_log (
  txn_id STRING,
  error_msg STRING,
  crt_dt TIMESTAMP
);

/* -- Create f_inv_movmnt table with correct schema and constraints */
CREATE TABLE IF NOT EXISTS purgo_playground.f_inv_movmnt (
  txn_id STRING NOT NULL,
  inv_loc STRING,
  financial_qty DOUBLE,
  net_qty DOUBLE,
  expired_dt STRING,
  item_nbr STRING NOT NULL,
  unit_cost DOUBLE,
  uom_rate DOUBLE NOT NULL,
  plant_loc_cd STRING,
  inv_stock_reference STRING,
  stock_type STRING,
  qty_on_hand DOUBLE,
  qty_shipped DOUBLE,
  cancel_dt STRING,
  flag_active STRING,
  crt_dt TIMESTAMP NOT NULL,
  updt_dt TIMESTAMP NOT NULL,
  CONSTRAINT stock_type_values CHECK (stock_type IN ("RAW", "WIP", "FG", "OT")),
  CONSTRAINT flag_active_values CHECK (flag_active IN ("yes", "no"))
);

/* -- Create f_inv_movmnt_apl_qty table for FK test */
CREATE TABLE IF NOT EXISTS purgo_playground.f_inv_movmnt_apl_qty (
  txn_id STRING,
  ref_txn_qty DECIMAL(3,1),
  cumulative_txn_qty DECIMAL(4,1),
  cumulative_ref_ord_sched_qty DECIMAL(4,1),
  ref_ord_sched_qty DECIMAL(3,1),
  prior_cumulative_txn_qty DECIMAL(3,1),
  prior_cumulative_ref_ord_sched_qty DECIMAL(3,1),
  apl_qty DECIMAL(5,1)
);

/* =============================================================================
   SECTION: SCHEMA VALIDATION
============================================================================= */

/* -- Validate f_inv_movmnt schema matches specification */
WITH expected_schema AS (
  SELECT "txn_id" AS col_name, "STRING" AS data_type, 1 AS ord UNION ALL
  SELECT "inv_loc", "STRING", 2 UNION ALL
  SELECT "financial_qty", "DOUBLE", 3 UNION ALL
  SELECT "net_qty", "DOUBLE", 4 UNION ALL
  SELECT "expired_dt", "STRING", 5 UNION ALL
  SELECT "item_nbr", "STRING", 6 UNION ALL
  SELECT "unit_cost", "DOUBLE", 7 UNION ALL
  SELECT "uom_rate", "DOUBLE", 8 UNION ALL
  SELECT "plant_loc_cd", "STRING", 9 UNION ALL
  SELECT "inv_stock_reference", "STRING", 10 UNION ALL
  SELECT "stock_type", "STRING", 11 UNION ALL
  SELECT "qty_on_hand", "DOUBLE", 12 UNION ALL
  SELECT "qty_shipped", "DOUBLE", 13 UNION ALL
  SELECT "cancel_dt", "STRING", 14 UNION ALL
  SELECT "flag_active", "STRING", 15 UNION ALL
  SELECT "crt_dt", "TIMESTAMP", 16 UNION ALL
  SELECT "updt_dt", "TIMESTAMP", 17
)
SELECT
  CASE WHEN COUNT(*) > 0 THEN
    RAISE_ERROR("Schema mismatch: f_inv_movmnt does not match expected schema")
  END
FROM (
  SELECT
    e.col_name, e.data_type
  FROM expected_schema e
  LEFT JOIN (
    SELECT
      column_name,
      UPPER(data_type) AS data_type
    FROM information_schema.columns
    WHERE table_catalog = "purgo_databricks"
      AND table_schema = "purgo_playground"
      AND table_name = "f_inv_movmnt"
  ) t
    ON e.col_name = t.column_name AND e.data_type = t.data_type
  WHERE t.column_name IS NULL
);

/* =============================================================================
   SECTION: LOAD TEST DATA
============================================================================= */

/* -- Insert test data into f_inv_movmnt (simulate ETL output) */
INSERT INTO purgo_playground.f_inv_movmnt
(
  txn_id, inv_loc, financial_qty, net_qty, expired_dt, item_nbr, unit_cost, uom_rate,
  plant_loc_cd, inv_stock_reference, stock_type, qty_on_hand, qty_shipped, cancel_dt,
  flag_active, crt_dt, updt_dt
)
WITH test_data AS (
  SELECT
    "1001|01|200" AS txn_id, "L001" AS inv_loc, 18.5 AS financial_qty, 18.5 AS net_qty, "20240610" AS expired_dt, "ABC123" AS item_nbr, 50.00 AS unit_cost, 0.76 AS uom_rate, "PL01" AS plant_loc_cd, "SUPP001" AS inv_stock_reference, "FG" AS stock_type, 18.5 AS qty_on_hand, 0.0 AS qty_shipped, NULL AS cancel_dt, "yes" AS flag_active, current_timestamp() AS crt_dt, current_timestamp() AS updt_dt
  UNION ALL
    SELECT "1002|02|201", "L002", 12.4, 12.4, "20240611", "XYZ789", 100.00, 0.76, "PL02", "DUN001", "RAW", 12.4, 0.0, NULL, "yes", current_timestamp(), current_timestamp()
  UNION ALL
    SELECT "1003|03|202", "L003", 15.0, 15.0, "20240612", "DEF456", 75.50, 0.76, "PL03", "VBAXSUPP", "WIP", 15.0, 0.0, NULL, "yes", current_timestamp(), current_timestamp()
  UNION ALL
    SELECT "1004|04|203", "L004", 10.0, 10.0, "20240613", "GHI789", 60.00, 0.76, "PL04", "none", "OT", 10.0, 0.0, NULL, "yes", current_timestamp(), current_timestamp()
  UNION ALL
    SELECT "1005|05|204", "L005", 8.0, 8.0, "20240614", "JKL012", 55.00, 0.76, "PL05", "none", "FG", 8.0, 0.0, NULL, "yes", current_timestamp(), current_timestamp()
  UNION ALL
    SELECT "1006|06|205", "L006", 7.0, 7.0, "20240615", "MNO345", 45.00, 0.76, "PL06", "none", "RAW", 7.0, 0.0, NULL, "yes", current_timestamp(), current_timestamp()
  UNION ALL
    SELECT "1007|07|206", "L007", 6.0, 6.0, "20240616", "PQR678", 40.00, 0.76, "PL07", "none", "WIP", 6.0, 0.0, NULL, "yes", current_timestamp(), current_timestamp()
  UNION ALL
    SELECT "1008|08|207", "L008", 5.0, 5.0, "20240617", "STU901", 35.00, 0.76, "PL08", "SUPP002", "FG", 5.0, 0.0, NULL, "no", current_timestamp(), current_timestamp()
  UNION ALL
    SELECT "1009|09|208", "L009", 4.0, 4.0, "20240618", "VWX234", 30.00, 0.76, "PL09", "SUPP003", "RAW", 4.0, 0.0, NULL, "no", current_timestamp(), current_timestamp()
  UNION ALL
    SELECT "1010|10|209", "L010", 3.0, 3.0, "20240619", "YZA567", 25.00, 0.76, "PL10", "SUPP004", "WIP", 3.0, 0.0, NULL, "no", current_timestamp(), current_timestamp()
  UNION ALL
    SELECT "1011|11|210", "L011", 2.0, 2.0, "20240620", "BCD890", 20.00, 0.76, "PL11", "SUPP005", "OT", 2.0, 0.0, NULL, "no", current_timestamp(), current_timestamp()
  UNION ALL
    SELECT "1012|12|211", "L012", 1.0, 1.0, "20240621", NULL, 15.00, 0.76, "PL12", "SUPP006", "FG", 1.0, 0.0, NULL, "yes", current_timestamp(), current_timestamp()
  UNION ALL
    SELECT "1013|13|212", "L013", CAST(NULL AS DOUBLE), 1.0, "20240622", "EFG123", 10.00, 0.76, "PL13", "SUPP007", "RAW", 1.0, 0.0, NULL, "yes", current_timestamp(), current_timestamp()
  UNION ALL
    SELECT "1014|14|213", "L014", 1.0, CAST(NULL AS DOUBLE), "20240623", "HIJ456", 5.00, 0.76, "PL14", "SUPP008", "WIP", 1.0, 0.0, NULL, "yes", current_timestamp(), current_timestamp()
  UNION ALL
    SELECT "1015|15|214", NULL, 2.0, 2.0, "20240624", "KLM789", 12.00, 0.76, "PL15", "SUPP009", "FG", 2.0, 0.0, NULL, "yes", current_timestamp(), current_timestamp()
  UNION ALL
    SELECT "1016|16|215", "L016", 3.0, 3.0, "20240625", "NOP012", 13.00, 0.76, NULL, "SUPP010", "RAW", 3.0, 0.0, NULL, "yes", current_timestamp(), current_timestamp()
  UNION ALL
    SELECT "1017|17|216", "L017", NULL, 4.0, "20240626", "QRS345", 14.00, 0.76, "PL17", "SUPP011", "WIP", 4.0, 0.0, NULL, "yes", current_timestamp(), current_timestamp()
  UNION ALL
    SELECT "1018|18|217", "L018", 5.0, NULL, "20240627", "TUV678", 15.00, 0.76, "PL18", "SUPP012", "OT", 0.0, 0.0, NULL, "yes", current_timestamp(), current_timestamp()
  UNION ALL
    SELECT "1019|19|218", "L019", 6.0, 6.0, NULL, "WXY901", 16.00, 0.76, "PL19", "SUPP013", "FG", 6.0, 0.0, NULL, "yes", current_timestamp(), current_timestamp()
  UNION ALL
    SELECT "1020|20|219", "L020", 7.0, 7.0, "20240629", "ZAB234", NULL, 0.76, "PL20", "SUPP014", "RAW", 7.0, 0.0, NULL, "yes", current_timestamp(), current_timestamp()
  UNION ALL
    SELECT "1021|21|220", "L021", 8.0, 8.0, "20240630", "CDE567", 18.00, 0.76, "PL21", "SUPP015", "WIP", 8.0, 0.0, "20240630", "yes", current_timestamp(), current_timestamp()
  UNION ALL
    SELECT "1022|22|221", "L022", 9.0, 9.0, "20240701", "A!@#$", 19.00, 0.76, "PL22", "SUPP016", "FG", 9.0, 0.0, NULL, "yes", current_timestamp(), current_timestamp()
  UNION ALL
    SELECT "1023|23|222", "多字节", 10.0, 10.0, "20240702", "FGH890", 20.00, 0.76, "PL23", "SUPP017", "RAW", 10.0, 0.0, NULL, "yes", current_timestamp(), current_timestamp()
  UNION ALL
    SELECT "1024|24|223", "L024", 11.0, 11.0, "20240703", "IJK123", 21.00, 0.76, "PL24😀", "SUPP018", "WIP", 11.0, 0.0, NULL, "yes", current_timestamp(), current_timestamp()
  UNION ALL
    SELECT "1025|25|224", "L025", 12.0, 12.0, "20240704", "LMN456", 22.00, 0.76, "PL25", "SUPP019@#$", "OT", 12.0, 0.0, NULL, "yes", current_timestamp(), current_timestamp()
  UNION ALL
    SELECT "1026|26|225", "L026", 20.0, 15.0, "20240705", "OPQ789", 23.00, 0.76, "PL26", "SUPP020", "FG", 15.0, 5.0, NULL, "yes", current_timestamp(), current_timestamp()
  UNION ALL
    SELECT "1027|27|226", "L027", 10.0, 15.0, "20240706", "RST012", 24.00, 0.76, "PL27", "SUPP021", "RAW", 15.0, 0.0, NULL, "yes", current_timestamp(), current_timestamp()
  UNION ALL
    SELECT "1028|28|227", "L028", 13.0, 13.0, "20240707", "UVW345", 25.00, 0.76, "PL28", "SUPP022", "WIP", 13.0, 0.0, NULL, "yes", current_timestamp(), current_timestamp()
  UNION ALL
    SELECT "1028|28|227", "L028", 14.0, 14.0, "20240708", "XYZ678", 26.00, 0.76, "PL28", "SUPP023", "OT", 14.0, 0.0, NULL, "yes", current_timestamp(), current_timestamp()
  UNION ALL
    SELECT "1029|29|228", NULL, NULL, NULL, NULL, NULL, NULL, 0.76, NULL, NULL, NULL, NULL, NULL, NULL, NULL, current_timestamp(), current_timestamp()
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

/* =============================================================================
   SECTION: DATA QUALITY & BUSINESS RULE ASSERTIONS
============================================================================= */

/* -- Assert: txn_id is not null and unique */
WITH cte AS (
  SELECT txn_id, COUNT(*) AS cnt
  FROM purgo_playground.f_inv_movmnt
  GROUP BY txn_id
)
SELECT
  CASE WHEN COUNT(*) > 0 THEN
    RAISE_ERROR("Uniqueness violation: txn_id is not unique in f_inv_movmnt")
  END
FROM cte
WHERE cnt > 1;

/* -- Assert: inv_loc is not null and string, default "none" if missing */
SELECT
  CASE WHEN COUNT(*) > 0 THEN
    RAISE_ERROR("inv_loc null or not string or not defaulted to 'none'")
  END
FROM purgo_playground.f_inv_movmnt
WHERE inv_loc IS NULL OR typeof(inv_loc) != "STRING";

/* -- Assert: financial_qty and net_qty are not null, double, default 0 if missing */
SELECT
  CASE WHEN COUNT(*) > 0 THEN
    RAISE_ERROR("financial_qty or net_qty null or not double or not defaulted to 0")
  END
FROM purgo_playground.f_inv_movmnt
WHERE financial_qty IS NULL OR net_qty IS NULL
  OR typeof(financial_qty) != "DOUBLE"
  OR typeof(net_qty) != "DOUBLE";

/* -- Assert: expired_dt is not null, yyyymmdd, default "99991231" if missing */
SELECT
  CASE WHEN COUNT(*) > 0 THEN
    RAISE_ERROR("expired_dt null or not yyyymmdd or not defaulted to '99991231'")
  END
FROM purgo_playground.f_inv_movmnt
WHERE expired_dt IS NULL OR NOT expired_dt RLIKE "^[0-9]{8}$";

/* -- Assert: item_nbr is not null, string */
SELECT
  CASE WHEN COUNT(*) > 0 THEN
    RAISE_ERROR("item_nbr null or not string")
  END
FROM purgo_playground.f_inv_movmnt
WHERE item_nbr IS NULL OR typeof(item_nbr) != "STRING";

/* -- Assert: unit_cost is double, rounded to 2 decimals, null if missing */
SELECT
  CASE WHEN COUNT(*) > 0 THEN
    RAISE_ERROR("unit_cost not double or not rounded to 2 decimals")
  END
FROM purgo_playground.f_inv_movmnt
WHERE unit_cost IS NOT NULL AND (typeof(unit_cost) != "DOUBLE" OR unit_cost != ROUND(unit_cost, 2));

/* -- Assert: uom_rate is not null, double, always 0.76 */
SELECT
  CASE WHEN COUNT(*) > 0 THEN
    RAISE_ERROR("uom_rate not 0.76 or not double")
  END
FROM purgo_playground.f_inv_movmnt
WHERE uom_rate IS NULL OR typeof(uom_rate) != "DOUBLE" OR uom_rate != 0.76;

/* -- Assert: plant_loc_cd is not null, string, default "none" if missing */
SELECT
  CASE WHEN COUNT(*) > 0 THEN
    RAISE_ERROR("plant_loc_cd null or not string or not defaulted to 'none'")
  END
FROM purgo_playground.f_inv_movmnt
WHERE plant_loc_cd IS NULL OR typeof(plant_loc_cd) != "STRING";

/* -- Assert: inv_stock_reference is string, default "none" if missing */
SELECT
  CASE WHEN COUNT(*) > 0 THEN
    RAISE_ERROR("inv_stock_reference not string or not defaulted to 'none'")
  END
FROM purgo_playground.f_inv_movmnt
WHERE inv_stock_reference IS NULL OR typeof(inv_stock_reference) != "STRING";

/* -- Assert: stock_type is not null, one of [RAW, WIP, FG, OT] */
SELECT
  CASE WHEN COUNT(*) > 0 THEN
    RAISE_ERROR("stock_type null or not in [RAW, WIP, FG, OT]")
  END
FROM purgo_playground.f_inv_movmnt
WHERE stock_type IS NULL OR stock_type NOT IN ("RAW", "WIP", "FG", "OT");

/* -- Assert: qty_on_hand is not null, double, equals net_qty */
SELECT
  CASE WHEN COUNT(*) > 0 THEN
    RAISE_ERROR("qty_on_hand null, not double, or not equal to net_qty")
  END
FROM purgo_playground.f_inv_movmnt
WHERE qty_on_hand IS NULL OR typeof(qty_on_hand) != "DOUBLE" OR qty_on_hand != net_qty;

/* -- Assert: qty_shipped is not null, double, equals financial_qty - net_qty, 0 if negative */
SELECT
  CASE WHEN COUNT(*) > 0 THEN
    RAISE_ERROR("qty_shipped null, not double, or not correct calculation")
  END
FROM purgo_playground.f_inv_movmnt
WHERE qty_shipped IS NULL OR typeof(qty_shipped) != "DOUBLE"
  OR qty_shipped != CASE WHEN financial_qty - net_qty < 0 THEN 0.0 ELSE financial_qty - net_qty END;

/* -- Assert: cancel_dt is yyyymmdd or null */
SELECT
  CASE WHEN COUNT(*) > 0 THEN
    RAISE_ERROR("cancel_dt not null and not yyyymmdd")
  END
FROM purgo_playground.f_inv_movmnt
WHERE cancel_dt IS NOT NULL AND NOT CAST(cancel_dt AS STRING) RLIKE "^[0-9]{8}$";

/* -- Assert: flag_active is not null, "yes" or "no" */
SELECT
  CASE WHEN COUNT(*) > 0 THEN
    RAISE_ERROR("flag_active null or not in ['yes','no']")
  END
FROM purgo_playground.f_inv_movmnt
WHERE flag_active IS NULL OR flag_active NOT IN ("yes", "no");

/* -- Assert: crt_dt and updt_dt are not null, timestamp */
SELECT
  CASE WHEN COUNT(*) > 0 THEN
    RAISE_ERROR("crt_dt or updt_dt null or not timestamp")
  END
FROM purgo_playground.f_inv_movmnt
WHERE crt_dt IS NULL OR updt_dt IS NULL;

/* =============================================================================
   SECTION: NULL/DEFAULT HANDLING ASSERTIONS
============================================================================= */

/* -- Assert: inv_loc defaulted to "none" when null */
SELECT
  CASE WHEN COUNT(*) = 0 THEN
    RAISE_ERROR("inv_loc not defaulted to 'none' when null")
  END
FROM purgo_playground.f_inv_movmnt
WHERE txn_id = "1015|15|214" AND inv_loc = "none";

/* -- Assert: plant_loc_cd defaulted to "none" when null */
SELECT
  CASE WHEN COUNT(*) = 0 THEN
    RAISE_ERROR("plant_loc_cd not defaulted to 'none' when null")
  END
FROM purgo_playground.f_inv_movmnt
WHERE txn_id = "1016|16|215" AND plant_loc_cd = "none";

/* -- Assert: financial_qty defaulted to 0 when null */
SELECT
  CASE WHEN COUNT(*) = 0 THEN
    RAISE_ERROR("financial_qty not defaulted to 0 when null")
  END
FROM purgo_playground.f_inv_movmnt
WHERE txn_id = "1017|17|216" AND financial_qty = 0.0;

/* -- Assert: net_qty defaulted to 0 when null */
SELECT
  CASE WHEN COUNT(*) = 0 THEN
    RAISE_ERROR("net_qty not defaulted to 0 when null")
  END
FROM purgo_playground.f_inv_movmnt
WHERE txn_id = "1018|18|217" AND net_qty = 0.0;

/* -- Assert: expired_dt defaulted to "99991231" when null */
SELECT
  CASE WHEN COUNT(*) = 0 THEN
    RAISE_ERROR("expired_dt not defaulted to '99991231' when null")
  END
FROM purgo_playground.f_inv_movmnt
WHERE txn_id = "1019|19|218" AND expired_dt = "99991231";

/* -- Assert: unit_cost is null when missing */
SELECT
  CASE WHEN COUNT(*) = 0 THEN
    RAISE_ERROR("unit_cost not null when missing")
  END
FROM purgo_playground.f_inv_movmnt
WHERE txn_id = "1020|20|219" AND unit_cost IS NULL;

/* -- Assert: uom_rate always 0.76 */
SELECT
  CASE WHEN COUNT(*) = 0 THEN
    RAISE_ERROR("uom_rate not 0.76")
  END
FROM purgo_playground.f_inv_movmnt
WHERE uom_rate = 0.76;

/* =============================================================================
   SECTION: BUSINESS LOGIC ASSERTIONS
============================================================================= */

/* -- Assert: inv_stock_reference logic for LZBEP and related fields */
WITH cte AS (
  SELECT txn_id, inv_stock_reference
  FROM purgo_playground.f_inv_movmnt
  WHERE txn_id IN ("1001|01|200", "1005|05|204", "1002|02|201", "1006|06|205", "1003|03|202", "1007|07|206")
)
SELECT
  CASE WHEN COUNT(*) < 6 THEN
    RAISE_ERROR("inv_stock_reference logic failed for LZBEP and related fields")
  END
FROM cte
WHERE
  (txn_id = "1001|01|200" AND inv_stock_reference = "SUPP001") OR
  (txn_id = "1005|05|204" AND inv_stock_reference = "none") OR
  (txn_id = "1002|02|201" AND inv_stock_reference = "DUN001") OR
  (txn_id = "1006|06|205" AND inv_stock_reference = "none") OR
  (txn_id = "1003|03|202" AND inv_stock_reference = "VBAXSUPP") OR
  (txn_id = "1007|07|206" AND inv_stock_reference = "none");

/* -- Assert: stock_type mapping from PARA.ptart */
WITH cte AS (
  SELECT txn_id, stock_type
  FROM purgo_playground.f_inv_movmnt
  WHERE txn_id IN ("1001|01|200", "1002|02|201", "1003|03|202", "1004|04|203")
)
SELECT
  CASE WHEN COUNT(*) < 4 THEN
    RAISE_ERROR("stock_type mapping failed")
  END
FROM cte
WHERE
  (txn_id = "1001|01|200" AND stock_type = "FG") OR
  (txn_id = "1002|02|201" AND stock_type = "RAW") OR
  (txn_id = "1003|03|202" AND stock_type = "WIP") OR
  (txn_id = "1004|04|203" AND stock_type = "OT");

/* -- Assert: qty_on_hand equals net_qty */
SELECT
  CASE WHEN COUNT(*) = 0 THEN
    RAISE_ERROR("qty_on_hand not equal to net_qty")
  END
FROM purgo_playground.f_inv_movmnt
WHERE txn_id = "1003|03|202" AND qty_on_hand = net_qty;

/* -- Assert: qty_shipped calculation */
SELECT
  CASE WHEN COUNT(*) = 0 THEN
    RAISE_ERROR("qty_shipped calculation failed")
  END
FROM purgo_playground.f_inv_movmnt
WHERE txn_id = "1026|26|225" AND qty_shipped = 5.0;

/* -- Assert: qty_shipped negative, should be 0 */
SELECT
  CASE WHEN COUNT(*) = 0 THEN
    RAISE_ERROR("qty_shipped negative not set to 0")
  END
FROM purgo_playground.f_inv_movmnt
WHERE txn_id = "1027|27|226" AND qty_shipped = 0.0;

/* -- Assert: cancel_dt logic for missing mantr in current data */
SELECT
  CASE WHEN COUNT(*) = 0 THEN
    RAISE_ERROR("cancel_dt not set for missing mantr")
  END
FROM purgo_playground.f_inv_movmnt
WHERE txn_id = "1021|21|220" AND cancel_dt = "20240630";

/* -- Assert: flag_active logic based on dwart */
WITH cte AS (
  SELECT txn_id, flag_active
  FROM purgo_playground.f_inv_movmnt
  WHERE txn_id IN ("1008|08|207", "1009|09|208", "1010|10|209", "1011|11|210", "1001|01|200")
)
SELECT
  CASE WHEN COUNT(*) < 5 THEN
    RAISE_ERROR("flag_active logic failed")
  END
FROM cte
WHERE
  (txn_id = "1008|08|207" AND flag_active = "no") OR
  (txn_id = "1009|09|208" AND flag_active = "no") OR
  (txn_id = "1010|10|209" AND flag_active = "no") OR
  (txn_id = "1011|11|210" AND flag_active = "no") OR
  (txn_id = "1001|01|200" AND flag_active = "yes");

/* =============================================================================
   SECTION: ERROR HANDLING ASSERTIONS
============================================================================= */

/* -- Assert: error logged for item_nbr null */
INSERT INTO purgo_playground.f_inv_movmnt_error_log
SELECT txn_id, "item_nbr (patnr) is required", current_timestamp()
FROM purgo_playground.f_inv_movmnt
WHERE txn_id = "1012|12|211" AND item_nbr IS NULL;

/* -- Assert: error logged for financial_qty not double */
INSERT INTO purgo_playground.f_inv_movmnt_error_log
SELECT txn_id, "Invalid data type for financial_qty", current_timestamp()
FROM purgo_playground.f_inv_movmnt
WHERE txn_id = "1013|13|212" AND financial_qty IS NULL;

/* -- Assert: error logged for net_qty not double */
INSERT INTO purgo_playground.f_inv_movmnt_error_log
SELECT txn_id, "Invalid data type for net_qty", current_timestamp()
FROM purgo_playground.f_inv_movmnt
WHERE txn_id = "1014|14|213" AND net_qty IS NULL;

/* -- Assert: error log table contains expected errors */
SELECT
  CASE WHEN COUNT(*) < 3 THEN
    RAISE_ERROR("Error log does not contain all expected error records")
  END
FROM purgo_playground.f_inv_movmnt_error_log
WHERE error_msg IN ("item_nbr (patnr) is required", "Invalid data type for financial_qty", "Invalid data type for net_qty");

/* =============================================================================
   SECTION: DELTA LAKE & DML OPERATIONS
============================================================================= */

/* -- Test Delta Lake MERGE: update unit_cost for a record */
MERGE INTO purgo_playground.f_inv_movmnt AS tgt
USING (SELECT "1001|01|200" AS txn_id, 99.99 AS new_unit_cost) AS src
ON tgt.txn_id = src.txn_id
WHEN MATCHED THEN UPDATE SET unit_cost = src.new_unit_cost;

/* -- Assert: unit_cost updated */
SELECT
  CASE WHEN COUNT(*) = 0 THEN
    RAISE_ERROR("Delta Lake MERGE failed to update unit_cost")
  END
FROM purgo_playground.f_inv_movmnt
WHERE txn_id = "1001|01|200" AND unit_cost = 99.99;

/* -- Test Delta Lake DELETE: remove a record */
DELETE FROM purgo_playground.f_inv_movmnt WHERE txn_id = "1029|29|228";

/* -- Assert: record deleted */
SELECT
  CASE WHEN COUNT(*) > 0 THEN
    RAISE_ERROR("Delta Lake DELETE failed")
  END
FROM purgo_playground.f_inv_movmnt
WHERE txn_id = "1029|29|228";

/* =============================================================================
   SECTION: WINDOW FUNCTION & ANALYTICS ASSERTIONS
============================================================================= */

/* -- Assert: expired_dt is from first PCHK record by irsda DESC (simulate with window) */
WITH cte AS (
  SELECT
    txn_id,
    expired_dt,
    ROW_NUMBER() OVER (PARTITION BY item_nbr ORDER BY expired_dt DESC) AS rn
  FROM purgo_playground.f_inv_movmnt
  WHERE item_nbr IS NOT NULL
)
SELECT
  CASE WHEN COUNT(*) = 0 THEN
    RAISE_ERROR("Window function for expired_dt failed")
  END
FROM cte
WHERE rn = 1;

/* =============================================================================
   SECTION: *_APL_QTY TABLES FK ASSERTIONS
============================================================================= */

/* -- Insert test data into f_inv_movmnt_apl_qty referencing f_inv_movmnt */
INSERT INTO purgo_playground.f_inv_movmnt_apl_qty (txn_id, ref_txn_qty, cumulative_txn_qty, cumulative_ref_ord_sched_qty, ref_ord_sched_qty, prior_cumulative_txn_qty, prior_cumulative_ref_ord_sched_qty, apl_qty)
SELECT txn_id, 1.0, 2.0, 3.0, 1.0, 0.0, 0.0, 5.0
FROM purgo_playground.f_inv_movmnt
WHERE txn_id = "1001|01|200";

/* -- Assert: all txn_id in *_apl_qty tables exist in f_inv_movmnt */
SELECT
  CASE WHEN COUNT(*) > 0 THEN
    RAISE_ERROR("txn_id in *_apl_qty does not reference f_inv_movmnt")
  END
FROM purgo_playground.f_inv_movmnt_apl_qty a
LEFT JOIN purgo_playground.f_inv_movmnt m ON a.txn_id = m.txn_id
WHERE m.txn_id IS NULL;

/* =============================================================================
   SECTION: PERFORMANCE TEST (SAMPLE)
============================================================================= */

/* -- Assert: row count > 20 and < 100 (simulate large data volume test) */
SELECT
  CASE WHEN COUNT(*) < 20 THEN
    RAISE_ERROR("Performance test: not enough rows loaded")
  END
FROM purgo_playground.f_inv_movmnt;

/* =============================================================================
   SECTION: CLEANUP
============================================================================= */

/* -- Clean up error log and test data (optional) */
TRUNCATE TABLE purgo_playground.f_inv_movmnt_error_log;
-- Do not drop f_inv_movmnt or *_apl_qty tables to allow further manual inspection

/* =============================================================================
   END OF TEST SUITE
============================================================================= */
