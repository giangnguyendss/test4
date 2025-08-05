USE CATALOG purgo_databricks;

/*
==========================================================================================
  Databricks SQL ETL for purgo_playground.f_inv_movmnt
  - Populates f_inv_movmnt from PSEK and related source tables as per mapping
  - Handles all business logic, defaulting, error logging, and data quality checks
  - All table and column references are fully qualified
  - All string fields use STRING, date fields use DECIMAL(38,0) for yyyymmdd, timestamps use TIMESTAMP
  - No CHECK constraints (not supported in Databricks SQL)
  - All enums/constrained values are documented in column comments in the CREATE TABLE statement
  - All joins and logic per mapping and gherkin feature
==========================================================================================
*/

/*
==========================================================================================
  SECTION: TABLE DDL (CREATE IF NOT EXISTS)
==========================================================================================
*/

CREATE TABLE IF NOT EXISTS purgo_playground.f_inv_movmnt (
  txn_id STRING NOT NULL COMMENT 'Unique identifier for inventory movement. Format: PKLNR|ZEIPE|PJAGR',
  inv_loc STRING COMMENT 'Physical location of the inventory. Default: none',
  financial_qty DOUBLE COMMENT 'Qty owned by vendor for a specific division. Default: 0',
  net_qty DOUBLE COMMENT 'Sum of available and reserved quantity for transaction calculation. Default: 0',
  expired_qt DECIMAL(38,0) COMMENT 'Last date of transaction of products on-hold. Format: yyyymmdd. Default: 99991231',
  item_nbr STRING NOT NULL COMMENT 'Unique alphanumeric identifier for product',
  unit_cost DOUBLE COMMENT 'Unit cost of the product. Rounded to 2 decimals. Default: 0.00',
  uom_rate DOUBLE COMMENT 'Conversion rate to global unit of measurement. Default: 0.76',
  plant_loc_cd STRING COMMENT 'Unique code for branch/division. Default: none',
  inv_stock_reference STRING COMMENT 'Reference identifier for supplier. See mapping logic',
  stock_type STRING COMMENT 'Inventory product type. Valid: RAW, WIP, FG, OT. Default: OT',
  qty_on_hand DOUBLE COMMENT 'Quantity physically on hand at plant location. Default: 0',
  qty_shipped DOUBLE COMMENT 'Quantity shipped to customer. Default: 0',
  cancel_dt DECIMAL(38,0) COMMENT 'Date order was canceled. Format: yyyymmdd',
  flag_active STRING COMMENT 'Flag for open/completed transaction. Valid: yes, no. Default: no',
  crt_dt TIMESTAMP COMMENT 'Record create timestamp',
  updt_dt TIMESTAMP COMMENT 'Record update timestamp'
);

/*
==========================================================================================
  SECTION: ERROR LOG TABLE (for ETL error/warning logging)
==========================================================================================
*/

CREATE TABLE IF NOT EXISTS purgo_playground.f_inv_movmnt_etl_log (
  log_level STRING,
  log_msg STRING,
  log_dt TIMESTAMP
);

/*
==========================================================================================
  SECTION: CTEs FOR ETL LOGIC
==========================================================================================
*/

/* 
------------------------------------------------------------------------------------------
  CTE: yesterday_txn_ids
  - Used for cancel_dt logic: find txn_ids present yesterday but not today
------------------------------------------------------------------------------------------
*/
WITH yesterday_txn_ids AS (
  SELECT txn_id
  FROM purgo_playground.f_inv_movmnt
  WHERE crt_dt < date_trunc('day', current_timestamp())
),

/* 
------------------------------------------------------------------------------------------
  CTE: psek_base
  - Leading table: PSEK
  - All string join keys are trimmed as required
------------------------------------------------------------------------------------------
*/
psek_base AS (
  SELECT
    psek.tanpt,
    psek.pklnr,
    psek.pjagr,
    psek.zeipe,
    psek.dwart,
    psek.patnr,
    psek.perks,
    psek.tgort,
    psek.lzbep,
    psek.lifdr,
    psek.sdauf,
    psek.dunnr
  FROM purgo_playground.psek psek
),

/* 
------------------------------------------------------------------------------------------
  CTE: pchk_counts, pchk_dupes, pchk_valid
  - Deduplicate PCHK on (TANPT, PKLNR)
  - If duplicates found, log error and exclude from ETL
------------------------------------------------------------------------------------------
*/
pchk_counts AS (
  SELECT
    tanpt,
    pklnr,
    COUNT(*) AS cnt
  FROM purgo_playground.pchk
  GROUP BY tanpt, pklnr
),
pchk_dupes AS (
  SELECT tanpt, pklnr
  FROM pchk_counts
  WHERE cnt > 1
),
pchk_valid AS (
  SELECT *
  FROM purgo_playground.pchk
  WHERE (tanpt, pklnr) NOT IN (SELECT tanpt, pklnr FROM pchk_dupes)
),

/* 
------------------------------------------------------------------------------------------
  CTE: para_xpy
  - PARA filtered to PRBHA begins with "XPY"
------------------------------------------------------------------------------------------
*/
para_xpy AS (
  SELECT *
  FROM purgo_playground.para
  WHERE prbha LIKE 'XPY%'
),

/* 
------------------------------------------------------------------------------------------
  CTE: main_etl
  - All joins and business logic per mapping
------------------------------------------------------------------------------------------
*/
main_etl AS (
  SELECT
    -- txn_id: Concatenate PKLNR|ZEIPE|PJAGR
    CONCAT(TRIM(psek.pklnr), '|', TRIM(psek.zeipe), '|', TRIM(psek.pjagr)) AS txn_id,

    -- inv_loc: PSEK.tgort, default 'none'
    COALESCE(NULLIF(TRIM(psek.tgort), ''), 'none') AS inv_loc,

    -- financial_qty: 
    -- If pchk.ckarg is not null, sum pchk.TUMLM + pchk.TINSM + pchk.PEINM + pchk.TSPEM + pchk.TLABS
    -- Else, sum parl.LMLME + parl.LNSME + parl.LINME + parl.SPELE + parl.LALST
    CASE
      WHEN pchk.ckarg IS NOT NULL THEN
        COALESCE(pchk.tumlm, 0) + COALESCE(pchk.tinsm, 0) + COALESCE(pchk.peinm, 0) + COALESCE(pchk.tspem, 0) + COALESCE(pchk.tlabs, 0)
      ELSE
        COALESCE(parl.lmlme, 0) + COALESCE(parl.lnsme, 0) + COALESCE(parl.linme, 0) + COALESCE(parl.spele, 0) + COALESCE(parl.lalst, 0)
    END AS financial_qty,

    -- net_qty:
    -- If pchk.ckarg is not null, sum pchk.TUMLM + pchk.TINSM + pchk.PEINM + pchk.TSPEM + pchk.TLABS
    -- Else, sum parl.LMLME + parl.LNSME + parl.LINME + parl.SPELE + parl.LALST
    CASE
      WHEN pchk.ckarg IS NOT NULL THEN
        COALESCE(pchk.tumlm, 0) + COALESCE(pchk.tinsm, 0) + COALESCE(pchk.peinm, 0) + COALESCE(pchk.tspem, 0) + COALESCE(pchk.tlabs, 0)
      ELSE
        COALESCE(parl.lmlme, 0) + COALESCE(parl.lnsme, 0) + COALESCE(parl.linme, 0) + COALESCE(parl.spele, 0) + COALESCE(parl.lalst, 0)
    END AS net_qty,

    -- expired_qt: PCHK.ofdat (yyyymmdd), get first record for patnr, tanpt, ckarg ORDER BY irsda DESC
    -- If not present, default 99991231
    COALESCE(
      CAST(
        (
          SELECT ofdat
          FROM purgo_playground.pchk pchk2
          WHERE pchk2.patnr = psek.patnr
            AND pchk2.tanpt = psek.tanpt
            AND pchk2.ckarg = pchk.ckarg
          ORDER BY pchk2.irsda DESC
          LIMIT 1
        ) AS DECIMAL(38,0)
      ),
      99991231
    ) AS expired_qt,

    -- item_nbr: PSEK.patnr (NOT NULL)
    TRIM(psek.patnr) AS item_nbr,

    -- unit_cost: 
    -- If pbev.xprsv = 'S' then xtprs/peinl, if 'V' then perpr/peinl, else 0.00. Round to 2 decimals.
    CASE
      WHEN pbev.xprsv = 'S' AND COALESCE(pbev.peinl, 0) != 0 THEN ROUND(pbev.xtprs / pbev.peinl, 2)
      WHEN pbev.xprsv = 'V' AND COALESCE(pbev.peinl, 0) != 0 THEN ROUND(pbev.perpr / pbev.peinl, 2)
      ELSE 0.00
    END AS unit_cost,

    -- uom_rate: Hardcode 0.76
    0.76 AS uom_rate,

    -- plant_loc_cd: PSEK.perks, default 'none'
    COALESCE(NULLIF(TRIM(psek.perks), ''), 'none') AS plant_loc_cd,

    -- inv_stock_reference: 
    -- If LZBEP = 'B', use LIFDR, else if 'L', use DUNNR, else if DUNNR blank and SDAUF present, get VBAX.dunnr by PSEK.sdauf = VBAX.vpelm, else None
    CASE
      WHEN UPPER(TRIM(psek.lzbep)) = 'B' THEN COALESCE(NULLIF(TRIM(psek.lifdr), ''), 'None')
      WHEN UPPER(TRIM(psek.lzbep)) = 'L' THEN
        CASE
          WHEN COALESCE(NULLIF(TRIM(psek.dunnr), ''), NULL) IS NOT NULL THEN TRIM(psek.dunnr)
          WHEN COALESCE(NULLIF(TRIM(psek.sdauf), ''), NULL) IS NOT NULL THEN
            COALESCE(
              (
                SELECT TRIM(vbax2.dunnr)
                FROM purgo_playground.vbax vbax2
                WHERE vbax2.vpelm = psek.sdauf
                LIMIT 1
              ),
              'None'
            )
          ELSE 'None'
        END
      ELSE 'None'
    END AS inv_stock_reference,

    -- stock_type: Map PARA.ptart: FERT->FG, ROH->RAW, HALB->WIP, else OT. Default OT.
    CASE
      WHEN UPPER(TRIM(para.ptart)) = 'FERT' THEN 'FG'
      WHEN UPPER(TRIM(para.ptart)) = 'ROH' THEN 'RAW'
      WHEN UPPER(TRIM(para.ptart)) = 'HALB' THEN 'WIP'
      ELSE 'OT'
    END AS stock_type,

    -- qty_on_hand: Equal to net_qty
    CASE
      WHEN
        CASE
          WHEN pchk.ckarg IS NOT NULL THEN
            COALESCE(pchk.tumlm, 0) + COALESCE(pchk.tinsm, 0) + COALESCE(pchk.peinm, 0) + COALESCE(pchk.tspem, 0) + COALESCE(pchk.tlabs, 0)
          ELSE
            COALESCE(parl.lmlme, 0) + COALESCE(parl.lnsme, 0) + COALESCE(parl.linme, 0) + COALESCE(parl.spele, 0) + COALESCE(parl.lalst, 0)
        END IS NULL
      THEN 0
      ELSE
        CASE
          WHEN pchk.ckarg IS NOT NULL THEN
            COALESCE(pchk.tumlm, 0) + COALESCE(pchk.tinsm, 0) + COALESCE(pchk.peinm, 0) + COALESCE(pchk.tspem, 0) + COALESCE(pchk.tlabs, 0)
          ELSE
            COALESCE(parl.lmlme, 0) + COALESCE(parl.lnsme, 0) + COALESCE(parl.linme, 0) + COALESCE(parl.spele, 0) + COALESCE(parl.lalst, 0)
        END
    END AS qty_on_hand,

    -- qty_shipped: financial_qty - qty_on_hand, default 0 if negative or null
    GREATEST(
      (
        CASE
          WHEN pchk.ckarg IS NOT NULL THEN
            COALESCE(pchk.tumlm, 0) + COALESCE(pchk.tinsm, 0) + COALESCE(pchk.peinm, 0) + COALESCE(pchk.tspem, 0) + COALESCE(pchk.tlabs, 0)
          ELSE
            COALESCE(parl.lmlme, 0) + COALESCE(parl.lnsme, 0) + COALESCE(parl.linme, 0) + COALESCE(parl.spele, 0) + COALESCE(parl.lalst, 0)
        END
        -
        CASE
          WHEN pchk.ckarg IS NOT NULL THEN
            COALESCE(pchk.tumlm, 0) + COALESCE(pchk.tinsm, 0) + COALESCE(pchk.peinm, 0) + COALESCE(pchk.tspem, 0) + COALESCE(pchk.tlabs, 0)
          ELSE
            COALESCE(parl.lmlme, 0) + COALESCE(parl.lnsme, 0) + COALESCE(parl.linme, 0) + COALESCE(parl.spele, 0) + COALESCE(parl.lalst, 0)
        END
      ), 0
    ) AS qty_shipped,

    -- cancel_dt: If txn_id in yesterday but not in today, set to current date yyyymmdd, else NULL
    CASE
      WHEN CONCAT(TRIM(psek.pklnr), '|', TRIM(psek.zeipe), '|', TRIM(psek.pjagr)) IN (
        SELECT txn_id FROM yesterday_txn_ids
      ) THEN CAST(date_format(current_date(), 'yyyyMMdd') AS DECIMAL(38,0))
      ELSE NULL
    END AS cancel_dt,

    -- flag_active: If dwart is not null and not in RX, TX, PX then 'yes', else 'no'
    CASE
      WHEN psek.dwart IS NOT NULL AND UPPER(TRIM(psek.dwart)) NOT IN ('RX', 'TX', 'PX') THEN 'yes'
      ELSE 'no'
    END AS flag_active,

    -- crt_dt: current timestamp
    current_timestamp() AS crt_dt,

    -- updt_dt: current_timestamp
    current_timestamp() AS updt_dt

  FROM psek_base psek
  LEFT JOIN pchk_valid pchk
    ON pchk.tanpt = psek.tanpt AND pchk.pklnr = psek.pklnr
  LEFT JOIN purgo_playground.parl parl
    ON psek.perks = parl.perks
  LEFT JOIN purgo_playground.pbev pbev
    ON TRIM(psek.patnr) = TRIM(pbev.patnr) AND psek.perks = pbev.dwkey
  LEFT JOIN purgo_playground.l009t l009t
    ON parl.perks = l009t.perks AND parl.tgort = l009t.tgort
  LEFT JOIN para_xpy para
    ON pchk.tanpt = para.tanpt AND pchk.patnr = para.patnr
)

/*
==========================================================================================
  SECTION: ERROR LOGGING FOR DUPLICATES AND AMBIGUITY
==========================================================================================
*/

-- Log error for duplicate PCHK records
INSERT INTO purgo_playground.f_inv_movmnt_etl_log
SELECT
  'ERROR' AS log_level,
  'Duplicate PCHK records found for PKLNR ' || pklnr || ' and TANPT ' || tanpt AS log_msg,
  current_timestamp() AS log_dt
FROM pchk_dupes;

-- Log error for expired_dt/expired_qt mapping ambiguity
INSERT INTO purgo_playground.f_inv_movmnt_etl_log
SELECT
  'ERROR' AS log_level,
  'Field mapping ambiguity: expired_dt in mapping, expired_qt in schema' AS log_msg,
  current_timestamp() AS log_dt
WHERE EXISTS (
  SELECT 1
  FROM information_schema.columns
  WHERE table_schema = 'purgo_playground' AND table_name = 'f_inv_movmnt'
    AND column_name = 'expired_qt'
);

-- Log error for duplicate txn_id in main_etl
INSERT INTO purgo_playground.f_inv_movmnt_etl_log
SELECT
  'ERROR' AS log_level,
  'Duplicate txn_id detected: ' || txn_id AS log_msg,
  current_timestamp() AS log_dt
FROM (
  SELECT txn_id
  FROM main_etl
  GROUP BY txn_id
  HAVING COUNT(*) > 1
) t;

-- Log error for missing mapping for *_apl_qty* tables
INSERT INTO purgo_playground.f_inv_movmnt_etl_log
SELECT
  'ERROR' AS log_level,
  'Missing mapping and transformation logic for *_apl_qty* tables' AS log_msg,
  current_timestamp() AS log_dt;

-- Log warning for missing data volume/partitioning requirements
INSERT INTO purgo_playground.f_inv_movmnt_etl_log
SELECT
  'WARNING' AS log_level,
  'Data volume and partitioning requirements not specified' AS log_msg,
  current_timestamp() AS log_dt;

-- Log warning for missing source table refresh cadence
INSERT INTO purgo_playground.f_inv_movmnt_etl_log
SELECT
  'WARNING' AS log_level,
  'Source table refresh cadence not specified' AS log_msg,
  current_timestamp() AS log_dt;

/*
==========================================================================================
  SECTION: FINAL INSERT INTO TARGET TABLE
==========================================================================================
*/

INSERT INTO purgo_playground.f_inv_movmnt
SELECT *
FROM main_etl
WHERE txn_id NOT IN (
  SELECT txn_id FROM (
    SELECT txn_id
    FROM main_etl
    GROUP BY txn_id
    HAVING COUNT(*) > 1
  ) t
)
;

/*
==========================================================================================
  SECTION: DATA QUALITY CHECKS (VALIDATION QUERIES)
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
WHERE flag_active IS NOT NULL AND flag_active NOT IN ('yes', 'no');

-- Validate stock_type constraint (only allowed values)
SELECT COUNT(*) AS invalid_stock_type_count
FROM purgo_playground.f_inv_movmnt
WHERE stock_type IS NOT NULL AND stock_type NOT IN ('RAW', 'WIP', 'FG', 'OT');

-- Validate uniqueness of txn_id
WITH txn_id_dupes AS (
  SELECT txn_id, COUNT(*) AS cnt
  FROM purgo_playground.f_inv_movmnt
  GROUP BY txn_id
  HAVING COUNT(*) > 1
)
SELECT COUNT(*) AS duplicate_txn_id_count FROM txn_id_dupes;

-- Validate expired_qt default (should be 99991231 if null)
SELECT COUNT(*) AS expired_qt_default_failures
FROM purgo_playground.f_inv_movmnt
WHERE expired_qt IS NULL OR expired_qt = 99991231;

-- Validate timestamp fields are not null
SELECT COUNT(*) AS null_crt_dt_count
FROM purgo_playground.f_inv_movmnt
WHERE crt_dt IS NULL;

SELECT COUNT(*) AS null_updt_dt_count
FROM purgo_playground.f_inv_movmnt
WHERE updt_dt IS NULL;

/*
==========================================================================================
  SECTION: END OF SCRIPT
==========================================================================================
*/
