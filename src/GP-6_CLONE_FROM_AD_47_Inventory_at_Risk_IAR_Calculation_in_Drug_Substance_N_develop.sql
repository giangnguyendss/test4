-- Databricks SQL script - Real-Time Inventory at Risk Calculation based on DNSA flags in purgo_demo.f_inv_movmnt
-- Purpose: Calculates inventory_at_risk, total_inventory, and percentage_of_inventory_at_risk for life sciences manufacturing (Unity Catalog)
-- Author: Giang Nguyen
-- Date: 2025-07-22
-- Description: Calculates at-risk/total inventory metrics & percent, logs invalid/edge/error cases. Error message emission for negative qty or division by zero, strict business flag inclusion rules.

-------------------------------------------------------------------------------
/* SECTION: Error Handling – Negative financial_qty Detection */
-------------------------------------------------------------------------------

-- Purpose: Emit error if any record has negative financial_qty
WITH negative_qty AS (
  SELECT 
    inventory_id, 
    financial_qty
  FROM purgo_demo.f_inv_movmnt
  WHERE financial_qty < 0
)
SELECT
  CASE 
    WHEN COUNT(1) > 0 
    THEN 'Invalid financial_qty: cannot be negative'
    ELSE NULL
  END AS error_message
FROM negative_qty
;

-------------------------------------------------------------------------------
/* SECTION: Error Handling – Zero or Empty Total Inventory (Division by Zero Protection) */
-------------------------------------------------------------------------------

-- Purpose: Emit error if total_inventory is zero (table empty or all-zeros)
WITH total_inventory_check AS (
  SELECT COALESCE(SUM(financial_qty), 0) AS total_inventory
  FROM purgo_demo.f_inv_movmnt
)
SELECT
  CASE 
    WHEN total_inventory = 0
    THEN 'Total inventory is zero, cannot calculate percentage'
    ELSE NULL
  END AS error_message
FROM total_inventory_check
;

-------------------------------------------------------------------------------
/* SECTION: Logging – Unexpected/Invalid Flag Values */
-------------------------------------------------------------------------------

-- Purpose: Log all records with dnsa_flag or flag_active values outside strict allowed sets
WITH invalid_flags AS (
  SELECT
    inventory_id,
    dnsa_flag,
    flag_active
  FROM purgo_demo.f_inv_movmnt
  WHERE
    (dnsa_flag IS NOT NULL AND dnsa_flag NOT IN ('yes','YES','no','NO'))
    OR
    (flag_active IS NOT NULL AND flag_active NOT IN ('Y','y','N','n'))
)
SELECT
  inventory_id,
  dnsa_flag,
  flag_active,
  'Unexpected flag value encountered' AS warning_message
FROM invalid_flags
;

-------------------------------------------------------------------------------
/* SECTION: Inventory At Risk Calculation and Output */
-------------------------------------------------------------------------------

-- Purpose: Calculate inventory_at_risk, total_inventory, and percentage_of_inventory_at_risk with precise rounding
WITH
  -- CTE for total inventory sum
  total_inventory_cte AS (
    SELECT
      SUM(financial_qty) AS total_inventory
    FROM purgo_demo.f_inv_movmnt
  ),
  -- CTE for inventory at risk (flag_active 'Y'/'y', dnsa_flag 'yes'/'YES', strict match, non-null)
  at_risk_cte AS (
    SELECT
      SUM(financial_qty) AS inventory_at_risk
    FROM purgo_demo.f_inv_movmnt
    WHERE
      flag_active IN ('Y','y')
      AND dnsa_flag IN ('yes','YES')
      AND flag_active IS NOT NULL
      AND dnsa_flag IS NOT NULL
  ),
  -- Result CTE: join metrics, percent logic
  result AS (
    SELECT
      COALESCE(a.inventory_at_risk, 0) AS inventory_at_risk,
      COALESCE(t.total_inventory, 0) AS total_inventory,
      CASE
        WHEN COALESCE(t.total_inventory,0) = 0 THEN NULL
        ELSE ROUND(COALESCE(a.inventory_at_risk,0) / t.total_inventory * 100, 2)
      END AS percentage_of_inventory_at_risk
    FROM at_risk_cte a
    CROSS JOIN total_inventory_cte t
  )
SELECT
  inventory_at_risk,               -- DECIMAL: sum of at-risk inventory, 0 if none
  total_inventory,                 -- DECIMAL: total inventory sum, 0 if table empty
  percentage_of_inventory_at_risk  -- DECIMAL(5,2): percent, NULL if total=0
FROM result
;

-------------------------------------------------------------------------------
/* END OF INVENTORY AT RISK SCRIPT */
-------------------------------------------------------------------------------
