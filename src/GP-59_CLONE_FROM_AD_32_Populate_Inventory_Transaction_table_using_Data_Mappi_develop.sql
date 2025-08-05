USE CATALOG purgo_databricks;

/* 
==========================================================================================
  Databricks SQL Implementation for Inventory Transaction Table (f_inv_movmnt)
  Catalog: purgo_databricks
  Schema: purgo_playground

  - Populates purgo_playground.f_inv_movmnt from source tables as per mapping in Inventory Txn XPY.xlsx
  - Handles all business rules, default/null handling, error management, and join precedence
  - All comments follow Databricks SQL and code documentation guidelines
  - All string fields use STRING, date fields use STRING (yyyymmdd) or TIMESTAMP as per requirements
  - No CHECK constraints (not supported in Databricks SQL)
  - Uses COMMENT ON TABLE for table description
  - All enums/constrained values are documented in code comments
  - All joins and logic as per mapping and scenario requirements
==========================================================================================
*/

/*------------------------------------------------------------------------------
  SECTION: DDL - Create f_inv_movmnt Table (if not exists) with Table Comment
------------------------------------------------------------------------------*/

CREATE TABLE IF NOT EXISTS purgo_playground.f_inv_movmnt (
  txn_id STRING NOT NULL,         -- Unique identifier for inventory movement. Format: <pklnr>|<zeipe>|<pjagr>. Not null, unique.
  inv_loc STRING,                 -- Physical location of the inventory. Default "none" if not present.
  financial_qty DOUBLE,           -- Quantity owned by vendor for a specific division. Not null. Default 0 if not present.
  net_qty DOUBLE,                 -- Sum of available and reserved quantity for transaction calculation. Not null. Default 0 if not present.
  expired_dt STRING,              -- Last date of transaction of products on-hold. Format yyyymmdd. Default "99991231" if not present.
  item_nbr STRING NOT NULL,       -- Unique alphanumeric identifier for product. Not null.
  unit_cost DOUBLE,               -- Unit cost of the product. Rounded to 2 decimals. Null if not present.
  uom_rate DOUBLE,                -- Conversion rate to global unit of measurement. Always 0.76.
  plant_loc_cd STRING,            -- Unique code tagged to a branch for given division. Default "none" if not present.
  inv_stock_reference STRING,     -- Reference identifier for the supplier. See mapping logic. Default "none" if not present.
  stock_type STRING,              -- Inventory product type. One of [RAW, WIP, FG, OT]. Default "OT".
  qty_on_hand DOUBLE,             -- Quantity physically on hand at plant location. Equals net_qty.
  qty_shipped DOUBLE,             -- Quantity shipped to customer. financial_qty - qty_on_hand. Default 0 if negative.
  cancel_dt STRING,               -- Latest date on which all or part of the orders was canceled. Format yyyymmdd or null.
  flag_active STRING,             -- Flag indicating if transaction is open or completed. "yes" or "no". Default "no".
  crt_dt TIMESTAMP,               -- Record create timestamp. Not null.
  updt_dt TIMESTAMP               -- Record update timestamp. Not null.
)
COMMENT 'Inventory Transaction Table populated from PSEK, PCHK, PARL, PBEV, L009T, PARA, VBAX as per Inventory Txn XPY.xlsx mapping.';

/*------------------------------------------------------------------------------
  SECTION: DDL - Create Error Log Table (if not exists)
------------------------------------------------------------------------------*/

CREATE TABLE IF NOT EXISTS purgo_playground.f_inv_movmnt_error_log (
  txn_id STRING,
  error_msg STRING,
  crt_dt TIMESTAMP
)
COMMENT 'Error log for f_inv_movmnt ETL. Captures records rejected due to data quality or type errors.';

/*------------------------------------------------------------------------------
  SECTION: CTE - Source Data Extraction and Transformation
------------------------------------------------------------------------------*/

/*
  CTE: pchk_latest
  - For each (patnr, tanpt, ckarg), get the latest (by irsda DESC) PCHK record
  - Used for expired_dt and financial_qty logic
*/
WITH pchk_latest AS (
  SELECT
    tanpt,
    pklnr,
    patnr,
    perks,
    tgort,
    ckarg,
    irsda,
    ofdat,
    tumlm,
    tinsm,
    peinm,
    tspem,
    tlabs,
    ROW_NUMBER() OVER (
      PARTITION BY patnr, tanpt, ckarg
      ORDER BY irsda DESC
    ) AS rn
  FROM purgo_playground.pchk
),

/*
  CTE: vbax_lookup
  - Used for inv_stock_reference logic when lzbep = 'L' and sdauf is present
*/
vbax_lookup AS (
  SELECT
    vpelm,
    dunnr
  FROM purgo_playground.vbax
),

/*
  CTE: main_joined
  - All joins and field derivations as per mapping and join precedence
*/
main_joined AS (
  SELECT
    -- txn_id: <pklnr>|<zeipe>|<pjagr>
    CONCAT(psek.pklnr, '|', psek.zeipe, '|', psek.pjagr) AS txn_id,

    -- inv_loc: psek.tgort, default 'none'
    COALESCE(psek.tgort, 'none') AS inv_loc,

    -- financial_qty: if pchk.ckarg is not null, sum pchk fields, else sum parl fields, default 0
    CASE
      WHEN pchk.ckarg IS NOT NULL THEN
        COALESCE(pchk.tumlm, 0.0) + COALESCE(pchk.tinsm, 0.0) + COALESCE(pchk.peinm, 0.0) + COALESCE(pchk.tspem, 0.0) + COALESCE(pchk.tlabs, 0.0)
      ELSE
        COALESCE(parl.lmlme, 0.0) + COALESCE(parl.lnsme, 0.0) + COALESCE(parl.linme, 0.0) + COALESCE(parl.spele, 0.0) + COALESCE(parl.lalst, 0.0)
    END AS financial_qty,

    -- net_qty: if pchk.ckarg is not null, sum pchk fields, else sum parl fields, default 0
    CASE
      WHEN pchk.ckarg IS NOT NULL THEN
        COALESCE(pchk.tumlm, 0.0) + COALESCE(pchk.tinsm, 0.0) + COALESCE(pchk.peinm, 0.0) + COALESCE(pchk.tspem, 0.0) + COALESCE(pchk.tlabs, 0.0)
      ELSE
        COALESCE(parl.lmlme, 0.0) + COALESCE(parl.lnsme, 0.0) + COALESCE(parl.linme, 0.0) + COALESCE(parl.spele, 0.0) + COALESCE(parl.lalst, 0.0)
    END AS net_qty,

    -- expired_dt: pchk.ofdat from latest pchk, default '99991231'
    COALESCE(pchk.ofdat, '99991231') AS expired_dt,

    -- item_nbr: psek.patnr, must not be null
    psek.patnr AS item_nbr,

    -- unit_cost: if pbev.xprsv = 'S' then xtprs/peinl, if 'V' then perpr/peinl, else null, round to 2 decimals
    CASE
      WHEN pbev.xprsv = 'S' AND pbev.peinl IS NOT NULL AND pbev.peinl != 0 THEN ROUND(pbev.xtprs / pbev.peinl, 2)
      WHEN pbev.xprsv = 'V' AND pbev.peinl IS NOT NULL AND pbev.peinl != 0 THEN ROUND(pbev.perpr / pbev.peinl, 2)
      ELSE NULL
    END AS unit_cost,

    -- uom_rate: always 0.76
    0.76 AS uom_rate,

    -- plant_loc_cd: psek.perks, default 'none'
    COALESCE(psek.perks, 'none') AS plant_loc_cd,

    -- inv_stock_reference: see mapping logic
    CASE
      WHEN psek.lzbep = 'B' THEN
        CASE WHEN psek.lifdr IS NOT NULL AND TRIM(psek.lifdr) != '' THEN psek.lifdr ELSE 'none' END
      WHEN psek.lzbep = 'L' THEN
        CASE
          WHEN psek.dunnr IS NOT NULL AND TRIM(psek.dunnr) != '' THEN psek.dunnr
          WHEN psek.sdauf IS NOT NULL AND TRIM(psek.sdauf) != '' THEN
            COALESCE(vbax.dunnr, 'none')
          ELSE 'none'
        END
      ELSE 'none'
    END AS inv_stock_reference,

    -- stock_type: map para.ptart to FG/RAW/WIP/OT
    CASE
      WHEN UPPER(para.ptart) = 'FERT' THEN 'FG'
      WHEN UPPER(para.ptart) = 'ROH' THEN 'RAW'
      WHEN UPPER(para.ptart) = 'HALB' THEN 'WIP'
      ELSE 'OT'
    END AS stock_type,

    -- qty_on_hand: equals net_qty
    CASE
      WHEN pchk.ckarg IS NOT NULL THEN
        COALESCE(pchk.tumlm, 0.0) + COALESCE(pchk.tinsm, 0.0) + COALESCE(pchk.peinm, 0.0) + COALESCE(pchk.tspem, 0.0) + COALESCE(pchk.tlabs, 0.0)
      ELSE
        COALESCE(parl.lmlme, 0.0) + COALESCE(parl.lnsme, 0.0) + COALESCE(parl.linme, 0.0) + COALESCE(parl.spele, 0.0) + COALESCE(parl.lalst, 0.0)
    END AS qty_on_hand,

    -- qty_shipped: financial_qty - qty_on_hand, 0 if negative
    CASE
      WHEN
        (CASE
          WHEN pchk.ckarg IS NOT NULL THEN
            COALESCE(pchk.tumlm, 0.0) + COALESCE(pchk.tinsm, 0.0) + COALESCE(pchk.peinm, 0.0) + COALESCE(pchk.tspem, 0.0) + COALESCE(pchk.tlabs, 0.0)
          ELSE
            COALESCE(parl.lmlme, 0.0) + COALESCE(parl.lnsme, 0.0) + COALESCE(parl.linme, 0.0) + COALESCE(parl.spele, 0.0) + COALESCE(parl.lalst, 0.0)
        END) < 0
      THEN 0.0
      ELSE
        (CASE
          WHEN pchk.ckarg IS NOT NULL THEN
            COALESCE(pchk.tumlm, 0.0) + COALESCE(pchk.tinsm, 0.0) + COALESCE(pchk.peinm, 0.0) + COALESCE(pchk.tspem, 0.0) + COALESCE(pchk.tlabs, 0.0)
          ELSE
            COALESCE(parl.lmlme, 0.0) + COALESCE(parl.lnsme, 0.0) + COALESCE(parl.linme, 0.0) + COALESCE(parl.spele, 0.0) + COALESCE(parl.lalst, 0.0)
        END)
        -
        (CASE
          WHEN pchk.ckarg IS NOT NULL THEN
            COALESCE(pchk.tumlm, 0.0) + COALESCE(pchk.tinsm, 0.0) + COALESCE(pchk.peinm, 0.0) + COALESCE(pchk.tspem, 0.0) + COALESCE(pchk.tlabs, 0.0)
          ELSE
            COALESCE(parl.lmlme, 0.0) + COALESCE(parl.lnsme, 0.0) + COALESCE(parl.linme, 0.0) + COALESCE(parl.spele, 0.0) + COALESCE(parl.lalst, 0.0)
        END)
    END AS qty_shipped,

    -- cancel_dt: null (to be handled in orchestration, see below)
    NULL AS cancel_dt,

    -- flag_active: if dwart is not null and not in RX, TX, PX then 'yes', else 'no'
    CASE
      WHEN psek.dwart IS NOT NULL AND UPPER(psek.dwart) NOT IN ('RX', 'TX', 'PX') THEN 'yes'
      ELSE 'no'
    END AS flag_active,

    -- crt_dt: current timestamp
    current_timestamp() AS crt_dt,

    -- updt_dt: current_timestamp
    current_timestamp() AS updt_dt

  FROM purgo_playground.psek AS psek

  -- Join 1: left join pchk_latest (only latest per patnr, tanpt, ckarg)
  LEFT JOIN (
    SELECT * FROM pchk_latest WHERE rn = 1
  ) AS pchk
    ON pchk.tanpt = psek.tanpt AND pchk.pklnr = psek.pklnr

  -- Join 2: left join parl on psek.perks = parl.perks
  LEFT JOIN purgo_playground.parl AS parl
    ON psek.perks = parl.perks

  -- Join 3: left join pbev on trim(psek.patnr) = trim(pbev.patnr) and psek.perks = pbev.dwkey
  LEFT JOIN purgo_playground.pbev AS pbev
    ON TRIM(psek.patnr) = TRIM(pbev.patnr) AND psek.perks = pbev.dwkey

  -- Join 4: left join l009t on parl.perks = l009t.perks and parl.tgort = l009t.tgort
  LEFT JOIN purgo_playground.l009t AS l009t
    ON parl.perks = l009t.perks AND parl.tgort = l009t.tgort

  -- Join 5: left join para on pchk.tanpt = para.tanpt and pchk.patnr = para.patnr
  LEFT JOIN purgo_playground.para AS para
    ON pchk.tanpt = para.tanpt AND pchk.patnr = para.patnr

  -- Join 6: left join vbax_lookup for inv_stock_reference
  LEFT JOIN vbax_lookup AS vbax
    ON psek.sdauf = vbax.vpelm

),

/*------------------------------------------------------------------------------
  SECTION: Data Quality and Error Handling CTE
------------------------------------------------------------------------------*/

/*
  CTE: validated_main
  - Enforces data type validation and required field checks
  - Logs errors for invalid records (not inserted)
*/
validated_main AS (
  SELECT
    *,
    CASE
      WHEN item_nbr IS NULL THEN 'item_nbr (patnr) is required'
      WHEN TRY_CAST(financial_qty AS DOUBLE) IS NULL AND financial_qty IS NOT NULL THEN 'Invalid data type for financial_qty'
      WHEN TRY_CAST(net_qty AS DOUBLE) IS NULL AND net_qty IS NOT NULL THEN 'Invalid data type for net_qty'
      ELSE NULL
    END AS error_msg
  FROM main_joined
),

/*------------------------------------------------------------------------------
  SECTION: Deduplication CTE
------------------------------------------------------------------------------*/

deduped_main AS (
  SELECT
    txn_id,
    inv_loc,
    financial_qty,
    net_qty,
    expired_dt,
    item_nbr,
    unit_cost,
    uom_rate,
    plant_loc_cd,
    inv_stock_reference,
    stock_type,
    qty_on_hand,
    qty_shipped,
    cancel_dt,
    flag_active,
    crt_dt,
    updt_dt,
    error_msg,
    ROW_NUMBER() OVER (PARTITION BY txn_id ORDER BY crt_dt DESC) AS rn
  FROM validated_main
  WHERE error_msg IS NULL
)

/*------------------------------------------------------------------------------
  SECTION: Final Insert - Only Valid Records, Deduplicate txn_id
------------------------------------------------------------------------------*/

INSERT INTO purgo_playground.f_inv_movmnt
SELECT
  txn_id,
  inv_loc,
  COALESCE(TRY_CAST(financial_qty AS DOUBLE), 0.0) AS financial_qty,
  COALESCE(TRY_CAST(net_qty AS DOUBLE), 0.0) AS net_qty,
  COALESCE(expired_dt, '99991231') AS expired_dt,
  item_nbr,
  unit_cost,
  uom_rate,
  plant_loc_cd,
  inv_stock_reference,
  stock_type,
  COALESCE(TRY_CAST(net_qty AS DOUBLE), 0.0) AS qty_on_hand,
  CASE
    WHEN COALESCE(TRY_CAST(financial_qty AS DOUBLE), 0.0) - COALESCE(TRY_CAST(net_qty AS DOUBLE), 0.0) < 0 THEN 0.0
    ELSE COALESCE(TRY_CAST(financial_qty AS DOUBLE), 0.0) - COALESCE(TRY_CAST(net_qty AS DOUBLE), 0.0)
  END AS qty_shipped,
  cancel_dt,
  flag_active,
  crt_dt,
  updt_dt
FROM deduped_main
WHERE rn = 1
;

/*------------------------------------------------------------------------------
  SECTION: Error Logging - Invalid Records
------------------------------------------------------------------------------*/

INSERT INTO purgo_playground.f_inv_movmnt_error_log
SELECT
  txn_id,
  error_msg,
  current_timestamp() AS crt_dt
FROM validated_main
WHERE error_msg IS NOT NULL
;

/*------------------------------------------------------------------------------
  SECTION: Cancel Date Logic (to be handled in orchestration)
  - For records present in yesterday's data but not in today's, set cancel_dt to current date
  - This logic should be implemented in the orchestration layer or as a separate SQL step
------------------------------------------------------------------------------*/

/*
-- Example (to be run in orchestration, not as part of main insert):

MERGE INTO purgo_playground.f_inv_movmnt AS target
USING (
  SELECT txn_id FROM purgo_playground.f_inv_movmnt_yesterday
  WHERE txn_id NOT IN (SELECT txn_id FROM purgo_playground.f_inv_movmnt)
) AS src
ON target.txn_id = src.txn_id
WHEN MATCHED THEN
  UPDATE SET cancel_dt = date_format(current_date(), 'yyyyMMdd')
;
*/

/*------------------------------------------------------------------------------
  SECTION: End of Script
------------------------------------------------------------------------------*/
-- End of f_inv_movmnt population logic
