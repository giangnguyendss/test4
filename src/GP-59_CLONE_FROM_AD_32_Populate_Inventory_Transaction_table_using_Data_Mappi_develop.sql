USE CATALOG purgo_databricks;

/* 
==========================================================================================
  Databricks SQL Implementation for Inventory Transaction Table (f_inv_movmnt)
  Catalog: purgo_databricks
  Schema: purgo_playground

  - Populates purgo_playground.f_inv_movmnt using field mappings and business rules
    from Inventory Txn XPY.xlsx and technical design.
  - Handles all default/null logic, error handling, and join precedence as specified.
  - All comments follow Databricks SQL and code documentation guidelines.
==========================================================================================
*/

/*------------------------------------------------------------------------------
  SECTION: DDL - Create f_inv_movmnt Table (if not exists)
------------------------------------------------------------------------------*/
CREATE TABLE IF NOT EXISTS purgo_playground.f_inv_movmnt (
  txn_id STRING NOT NULL COMMENT 'Unique identifier: <pklnr>|<zeipe>|<pjagr>',
  inv_loc STRING COMMENT 'Physical location of inventory. Default "none" if missing.',
  financial_qty DOUBLE COMMENT 'Qty owned by vendor for division. Default 0 if missing.',
  net_qty DOUBLE COMMENT 'Sum of available/reserved qty for transaction. Default 0 if missing.',
  expired_dt STRING COMMENT 'Last on-hold transaction date (yyyymmdd). Default "99991231" if missing.',
  item_nbr STRING NOT NULL COMMENT 'Unique product identifier. NOT NULL.',
  unit_cost DOUBLE COMMENT 'Unit cost, rounded to 2 decimals. Null if missing.',
  uom_rate DOUBLE COMMENT 'Conversion rate. Always 0.76.',
  plant_loc_cd STRING COMMENT 'Branch code for division. Default "none" if missing.',
  inv_stock_reference STRING COMMENT 'Supplier reference. See mapping logic. Default "none".',
  stock_type STRING COMMENT 'Inventory type: RAW, WIP, FG, OT. Default "OT".',
  qty_on_hand DOUBLE COMMENT 'Equals net_qty.',
  qty_shipped DOUBLE COMMENT 'financial_qty - qty_on_hand. 0 if negative.',
  cancel_dt STRING COMMENT 'Cancel date (yyyymmdd) if txn missing in current load.',
  flag_active STRING COMMENT 'yes/no. "yes" if dwart not in (RX, TX, PX) and not null.',
  crt_dt TIMESTAMP COMMENT 'Record create timestamp.',
  updt_dt TIMESTAMP COMMENT 'Record update timestamp.'
);

/*------------------------------------------------------------------------------
  SECTION: CTE - Source Data Extraction and Join Logic
------------------------------------------------------------------------------*/
/*
  - Join order and precedence as per mapping:
    1. psek LEFT JOIN pchk ON tanpt, pklnr
    2. LEFT JOIN parl ON perks
    3. LEFT JOIN pbev ON trim(patnr), perks=dwkey
    4. LEFT JOIN l009t ON parl.perks, parl.tgort
    5. LEFT JOIN para ON pchk.tanpt, pchk.patnr
    6. LEFT JOIN vbax ON parl.tanpt, parl.patnr, parl.perks
  - All string joins on patnr use TRIM.
  - All default/null handling as per mapping.
  - Only first pchk record per patnr, tanpt, ckarg by irsda DESC for expired_dt.
*/
WITH
/*------------------------------------------------------------------------------
  CTE: pchk_latest - Get latest pchk per patnr, tanpt, ckarg by irsda DESC
------------------------------------------------------------------------------*/
pchk_latest AS (
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
      ORDER BY CAST(irsda AS BIGINT) DESC
    ) AS rn
  FROM purgo_playground.pchk
),
/*------------------------------------------------------------------------------
  CTE: base_join - All required joins and field selection
------------------------------------------------------------------------------*/
base_join AS (
  SELECT
    -- txn_id: <pklnr>|<zeipe>|<pjagr>
    CONCAT(psek.pklnr, '|', psek.zeipe, '|', psek.pjagr) AS txn_id,
    -- inv_loc: psek.tgort, default 'none'
    COALESCE(psek.tgort, 'none') AS inv_loc,
    -- item_nbr: psek.patnr, must not be null
    psek.patnr AS item_nbr,
    -- plant_loc_cd: psek.perks, default 'none'
    COALESCE(psek.perks, 'none') AS plant_loc_cd,
    -- dwart for flag_active
    psek.dwart,
    -- lzbep, lifdr, dunnr, sdauf for inv_stock_reference
    psek.lzbep, psek.lifdr, psek.dunnr, psek.sdauf,
    -- pklnr, zeipe, pjagr for cancel_dt logic
    psek.pklnr, psek.zeipe, psek.pjagr,
    -- pchk fields (latest only)
    pchk.tanpt AS pchk_tanpt,
    pchk.patnr AS pchk_patnr,
    pchk.ckarg AS pchk_ckarg,
    pchk.tumlm, pchk.tinsm, pchk.peinm, pchk.tspem, pchk.tlabs,
    pchk.irsda, pchk.ofdat,
    -- parl fields
    parl.lmlme, parl.lnsme, parl.linme, parl.spele, parl.lalst,
    parl.tanpt AS parl_tanpt,
    parl.patnr AS parl_patnr,
    parl.perks AS parl_perks,
    parl.tgort AS parl_tgort,
    -- pbev fields
    pbev.xprsv, pbev.xtprs, pbev.peinl, pbev.perpr,
    -- para fields
    para.ptart,
    -- vbax fields
    vbax.dunnr AS vbax_dunnr
  FROM purgo_playground.psek
  -- 1. pchk (latest per patnr, tanpt, ckarg)
  LEFT JOIN pchk_latest pchk
    ON pchk.tanpt = psek.tanpt
    AND pchk.pklnr = psek.pklnr
    AND pchk.rn = 1
  -- 2. parl
  LEFT JOIN purgo_playground.parl parl
    ON psek.perks = parl.perks
  -- 3. pbev
  LEFT JOIN purgo_playground.pbev pbev
    ON TRIM(psek.patnr) = TRIM(pbev.patnr)
    AND psek.perks = pbev.dwkey
  -- 4. l009t (not used in mapping logic, but join for completeness)
  LEFT JOIN purgo_playground.l009t l009t
    ON parl.perks = l009t.perks
    AND parl.tgort = l009t.tgort
  -- 5. para
  LEFT JOIN purgo_playground.para para
    ON pchk.tanpt = para.tanpt
    AND pchk.patnr = para.patnr
  -- 6. vbax
  LEFT JOIN purgo_playground.vbax vbax
    ON parl.tanpt = vbax.tanpt
    AND parl.patnr = vbax.patnr
    AND parl.perks = vbax.perks
),
/*------------------------------------------------------------------------------
  CTE: main_logic - Apply all mapping, default, and calculation logic
------------------------------------------------------------------------------*/
main_logic AS (
  SELECT
    txn_id,
    inv_loc,
    -- financial_qty: if pchk.ckarg not null, sum pchk fields, else sum parl fields, else 0
    CASE
      WHEN pchk_ckarg IS NOT NULL THEN
        COALESCE(pchk_tumlm, 0.0) + COALESCE(pchk_tinsm, 0.0) + COALESCE(pchk_peinm, 0.0) + COALESCE(pchk_tspem, 0.0) + COALESCE(pchk_tlabs, 0.0)
      WHEN parl.lmlme IS NOT NULL OR parl.lnsme IS NOT NULL OR parl.linme IS NOT NULL OR parl.spele IS NOT NULL OR parl.lalst IS NOT NULL THEN
        COALESCE(parl.lmlme, 0.0) + COALESCE(parl.lnsme, 0.0) + COALESCE(parl.linme, 0.0) + COALESCE(parl.spele, 0.0) + COALESCE(parl.lalst, 0.0)
      ELSE 0.0
    END AS financial_qty,
    -- net_qty: if pchk.ckarg is not null, sum pchk fields, else sum parl fields, else 0
    CASE
      WHEN pchk_ckarg IS NOT NULL THEN
        COALESCE(pchk_tumlm, 0.0) + COALESCE(pchk_tinsm, 0.0) + COALESCE(pchk_peinm, 0.0) + COALESCE(pchk_tspem, 0.0) + COALESCE(pchk_tlabs, 0.0)
      WHEN parl.lmlme IS NOT NULL OR parl.lnsme IS NOT NULL OR parl.linme IS NOT NULL OR parl.spele IS NOT NULL OR parl.lalst IS NOT NULL THEN
        COALESCE(parl.lmlme, 0.0) + COALESCE(parl.lnsme, 0.0) + COALESCE(parl.linme, 0.0) + COALESCE(parl.spele, 0.0) + COALESCE(parl.lalst, 0.0)
      ELSE 0.0
    END AS net_qty,
    -- expired_dt: pchk.ofdat, default '99991231'
    COALESCE(pchk.ofdat, '99991231') AS expired_dt,
    -- item_nbr: psek.patnr, must not be null (error handling below)
    item_nbr,
    -- unit_cost: if xprsv='S' then xtprs/peinl, if xprsv='V' then perpr/peinl, else null, round(2)
    CASE
      WHEN pbev.xprsv = 'S' AND pbev.peinl IS NOT NULL AND pbev.peinl != 0 THEN ROUND(pbev.xtprs / pbev.peinl, 2)
      WHEN pbev.xprsv = 'V' AND pbev.peinl IS NOT NULL AND pbev.peinl != 0 THEN ROUND(pbev.perpr / pbev.peinl, 2)
      ELSE NULL
    END AS unit_cost,
    -- uom_rate: always 0.76
    0.76 AS uom_rate,
    plant_loc_cd,
    -- inv_stock_reference: see mapping logic
    CASE
      WHEN lzbep = 'B' THEN
        CASE WHEN lifdr IS NOT NULL AND lifdr != '' THEN lifdr ELSE 'none' END
      WHEN lzbep = 'L' THEN
        CASE
          WHEN dunnr IS NOT NULL AND dunnr != '' THEN dunnr
          WHEN sdauf IS NOT NULL AND sdauf != '' AND vbax_dunnr IS NOT NULL AND vbax_dunnr != '' THEN vbax_dunnr
          ELSE 'none'
        END
      ELSE 'none'
    END AS inv_stock_reference,
    -- stock_type: ptart mapping
    CASE
      WHEN para.ptart = 'FERT' THEN 'FG'
      WHEN para.ptart = 'ROH' THEN 'RAW'
      WHEN para.ptart = 'HALB' THEN 'WIP'
      ELSE 'OT'
    END AS stock_type,
    -- qty_on_hand: equals net_qty
    CASE
      WHEN
        (CASE
          WHEN pchk_ckarg IS NOT NULL THEN
            COALESCE(pchk_tumlm, 0.0) + COALESCE(pchk_tinsm, 0.0) + COALESCE(pchk_peinm, 0.0) + COALESCE(pchk_tspem, 0.0) + COALESCE(pchk_tlabs, 0.0)
          WHEN parl.lmlme IS NOT NULL OR parl.lnsme IS NOT NULL OR parl.linme IS NOT NULL OR parl.spele IS NOT NULL OR parl.lalst IS NOT NULL THEN
            COALESCE(parl.lmlme, 0.0) + COALESCE(parl.lnsme, 0.0) + COALESCE(parl.linme, 0.0) + COALESCE(parl.spele, 0.0) + COALESCE(parl.lalst, 0.0)
          ELSE 0.0
        END) IS NULL THEN 0.0
      ELSE
        (CASE
          WHEN pchk_ckarg IS NOT NULL THEN
            COALESCE(pchk_tumlm, 0.0) + COALESCE(pchk_tinsm, 0.0) + COALESCE(pchk_peinm, 0.0) + COALESCE(pchk_tspem, 0.0) + COALESCE(pchk_tlabs, 0.0)
          WHEN parl.lmlme IS NOT NULL OR parl.lnsme IS NOT NULL OR parl.linme IS NOT NULL OR parl.spele IS NOT NULL OR parl.lalst IS NOT NULL THEN
            COALESCE(parl.lmlme, 0.0) + COALESCE(parl.lnsme, 0.0) + COALESCE(parl.linme, 0.0) + COALESCE(parl.spele, 0.0) + COALESCE(parl.lalst, 0.0)
          ELSE 0.0
        END)
    END AS qty_on_hand,
    -- qty_shipped: financial_qty - qty_on_hand, 0 if negative
    CASE
      WHEN
        (CASE
          WHEN pchk_ckarg IS NOT NULL THEN
            COALESCE(pchk_tumlm, 0.0) + COALESCE(pchk_tinsm, 0.0) + COALESCE(pchk_peinm, 0.0) + COALESCE(pchk_tspem, 0.0) + COALESCE(pchk_tlabs, 0.0)
          WHEN parl.lmlme IS NOT NULL OR parl.lnsme IS NOT NULL OR parl.linme IS NOT NULL OR parl.spele IS NOT NULL OR parl.lalst IS NOT NULL THEN
            COALESCE(parl.lmlme, 0.0) + COALESCE(parl.lnsme, 0.0) + COALESCE(parl.linme, 0.0) + COALESCE(parl.spele, 0.0) + COALESCE(parl.lalst, 0.0)
          ELSE 0.0
        END) IS NULL THEN 0.0
      ELSE
        CASE
          WHEN
            (CASE
              WHEN pchk_ckarg IS NOT NULL THEN
                COALESCE(pchk_tumlm, 0.0) + COALESCE(pchk_tinsm, 0.0) + COALESCE(pchk_peinm, 0.0) + COALESCE(pchk_tspem, 0.0) + COALESCE(pchk_tlabs, 0.0)
              WHEN parl.lmlme IS NOT NULL OR parl.lnsme IS NOT NULL OR parl.linme IS NOT NULL OR parl.spele IS NOT NULL OR parl.lalst IS NOT NULL THEN
                COALESCE(parl.lmlme, 0.0) + COALESCE(parl.lnsme, 0.0) + COALESCE(parl.linme, 0.0) + COALESCE(parl.spele, 0.0) + COALESCE(parl.lalst, 0.0)
              ELSE 0.0
            END) < 0 THEN 0.0
          ELSE
            (CASE
              WHEN pchk_ckarg IS NOT NULL THEN
                COALESCE(pchk_tumlm, 0.0) + COALESCE(pchk_tinsm, 0.0) + COALESCE(pchk_peinm, 0.0) + COALESCE(pchk_tspem, 0.0) + COALESCE(pchk_tlabs, 0.0)
              WHEN parl.lmlme IS NOT NULL OR parl.lnsme IS NOT NULL OR parl.linme IS NOT NULL OR parl.spele IS NOT NULL OR parl.lalst IS NOT NULL THEN
                COALESCE(parl.lmlme, 0.0) + COALESCE(parl.lnsme, 0.0) + COALESCE(parl.linme, 0.0) + COALESCE(parl.spele, 0.0) + COALESCE(parl.lalst, 0.0)
              ELSE 0.0
            END)
        END
    END AS qty_shipped,
    -- cancel_dt: handled in post-processing CTE below
    NULL AS cancel_dt,
    -- flag_active: 'yes' if dwart not null and not in (RX, TX, PX), else 'no'
    CASE
      WHEN dwart IS NOT NULL AND UPPER(dwart) NOT IN ('RX', 'TX', 'PX') THEN 'yes'
      ELSE 'no'
    END AS flag_active,
    current_timestamp() AS crt_dt,
    current_timestamp() AS updt_dt
  FROM base_join
),
/*------------------------------------------------------------------------------
  CTE: error_handling - Filter out records with critical errors, log errors
------------------------------------------------------------------------------*/
error_handling AS (
  SELECT
    *,
    CASE
      WHEN item_nbr IS NULL THEN 'item_nbr (patnr) is required'
      WHEN TRY_CAST(financial_qty AS DOUBLE) IS NULL AND financial_qty IS NOT NULL THEN 'Invalid data type for financial_qty'
      WHEN TRY_CAST(net_qty AS DOUBLE) IS NULL AND net_qty IS NOT NULL THEN 'Invalid data type for net_qty'
      ELSE NULL
    END AS error_msg
  FROM main_logic
),
/*------------------------------------------------------------------------------
  CTE: deduplication - Ensure txn_id uniqueness (keep first by crt_dt)
------------------------------------------------------------------------------*/
deduplication AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY txn_id ORDER BY crt_dt ASC) AS rn
  FROM error_handling
),
/*------------------------------------------------------------------------------
  CTE: final_select - Select only valid records, log errors for invalid
------------------------------------------------------------------------------*/
final_select AS (
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
    updt_dt
  FROM deduplication
  WHERE rn = 1
    AND error_msg IS NULL
)

/*------------------------------------------------------------------------------
  SECTION: INSERT - Populate f_inv_movmnt Table
------------------------------------------------------------------------------*/
INSERT OVERWRITE TABLE purgo_playground.f_inv_movmnt
SELECT *
FROM final_select
;

/*------------------------------------------------------------------------------
  SECTION: Error Logging - Log rejected records (if any)
------------------------------------------------------------------------------*/
CREATE TABLE IF NOT EXISTS purgo_playground.f_inv_movmnt_error_log (
  txn_id STRING,
  error_msg STRING,
  crt_dt TIMESTAMP
);

INSERT INTO purgo_playground.f_inv_movmnt_error_log
SELECT
  txn_id,
  error_msg,
  current_timestamp() AS crt_dt
FROM deduplication
WHERE error_msg IS NOT NULL
;

/*------------------------------------------------------------------------------
  SECTION: Validation Query - Data Quality Checks (CTE used directly)
------------------------------------------------------------------------------*/
WITH validation AS (
  SELECT
    COUNT(*) AS total_records,
    SUM(CASE WHEN txn_id IS NULL THEN 1 ELSE 0 END) AS null_txn_id,
    SUM(CASE WHEN item_nbr IS NULL THEN 1 ELSE 0 END) AS null_item_nbr,
    SUM(CASE WHEN TRY_CAST(financial_qty AS DOUBLE) IS NULL AND financial_qty IS NOT NULL THEN 1 ELSE 0 END) AS bad_financial_qty,
    SUM(CASE WHEN TRY_CAST(net_qty AS DOUBLE) IS NULL AND net_qty IS NOT NULL THEN 1 ELSE 0 END) AS bad_net_qty,
    SUM(CASE WHEN inv_loc = 'none' THEN 1 ELSE 0 END) AS inv_loc_none,
    SUM(CASE WHEN plant_loc_cd = 'none' THEN 1 ELSE 0 END) AS plant_loc_cd_none,
    SUM(CASE WHEN inv_stock_reference = 'none' THEN 1 ELSE 0 END) AS inv_stock_reference_none,
    SUM(CASE WHEN stock_type NOT IN ('RAW', 'WIP', 'FG', 'OT') THEN 1 ELSE 0 END) AS bad_stock_type,
    SUM(CASE WHEN flag_active NOT IN ('yes', 'no') THEN 1 ELSE 0 END) AS bad_flag_active,
    SUM(CASE WHEN uom_rate != 0.76 THEN 1 ELSE 0 END) AS bad_uom_rate,
    SUM(CASE WHEN expired_dt IS NULL OR expired_dt = '99991231' THEN 1 ELSE 0 END) AS default_expired_dt,
    SUM(CASE WHEN qty_on_hand != net_qty THEN 1 ELSE 0 END) AS qty_on_hand_mismatch,
    SUM(
      CASE
        WHEN NOT (
          (qty_shipped = 0.0 AND (financial_qty - net_qty) < 0)
          OR (qty_shipped = financial_qty - net_qty AND (financial_qty - net_qty) >= 0)
        ) THEN 1 ELSE 0 END
    ) AS qty_shipped_mismatch
  FROM final_select
)
SELECT * FROM validation
;

-- End of implementation
