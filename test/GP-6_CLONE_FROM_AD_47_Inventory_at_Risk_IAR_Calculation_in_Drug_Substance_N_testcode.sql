-- Databricks SQL script - Comprehensive test suite for Inventory at Risk calculation based on DNSA event logic (f_inv_movmnt, Unity Catalog)
-- Purpose: Validates schema, constraints, inclusion/exclusion, null/error edge cases, output structure and permissions for f_inv_movmnt Inventory at Risk logic.
-- Author: Giang Nguyen
-- Date: 2025-07-22
-- Description: Checks schema, enforces columns and datatypes, constraints, business logic, edge/error/null/unexpected/privilege scenarios, and output 3-column decimal result.

-------------------------------------------------------------------------------
/* SECTION: Setup and Schema Validation */
-------------------------------------------------------------------------------

-- Purpose: Assert correct columns and datatypes exist in Unity Catalog table "purgo_demo.f_inv_movmnt"
WITH columns_info AS (
  SELECT 
    column_name,
    data_type
  FROM 
    purgo_demo.information_schema.columns
  WHERE 
    table_schema = "purgo_demo"
    AND table_name = "f_inv_movmnt"
)
SELECT
  COUNT(*) AS num_matching_columns
FROM columns_info
WHERE 
  (column_name = "inventory_id"   AND data_type IN ("STRING", "VARCHAR", "CHAR", "TEXT")) OR
  (column_name = "financial_qty"  AND data_type LIKE "DECIMAL%") OR
  (column_name = "dnsa_flag"      AND data_type IN ("STRING", "VARCHAR", "CHAR", "TEXT")) OR
  (column_name = "flag_active"    AND data_type IN ("STRING", "VARCHAR", "CHAR", "TEXT"))
HAVING num_matching_columns = 4
;

-------------------------------------------------------------------------------
/* SECTION: Constraint Enforcement and Error Handling */
-------------------------------------------------------------------------------

-- Purpose: Ensure no NULLs for required fields: inventory_id and financial_qty 
SELECT
  COUNT(*) AS num_nulls_found
FROM purgo_demo.f_inv_movmnt
WHERE inventory_id IS NULL OR financial_qty IS NULL
HAVING num_nulls_found = 0
;

-- Purpose: Raise error on negative financial_qty (cannot be negative)
SELECT
  inventory_id,
  financial_qty
FROM purgo_demo.f_inv_movmnt
WHERE financial_qty < 0
;

-------------------------------------------------------------------------------
/* SECTION: Inventory at Risk & Percentage Calculation (Happy Path) */
-------------------------------------------------------------------------------

-- Purpose: Calculate inventory_at_risk (per flag), total_inventory, percentage_of_inventory_at_risk (rounded 2 decimals)
WITH
  total_inventory_cte AS (
    SELECT SUM(financial_qty) AS total_inventory
    FROM purgo_demo.f_inv_movmnt
  ),
  at_risk_cte AS (
    SELECT SUM(financial_qty) AS inventory_at_risk
    FROM purgo_demo.f_inv_movmnt
    WHERE flag_active IN ("Y", "y") AND dnsa_flag IN ("yes", "YES")
  ),
  result_cte AS (
    SELECT
      a.inventory_at_risk,
      t.total_inventory,
      CASE
        WHEN t.total_inventory = 0 THEN NULL
        ELSE ROUND((a.inventory_at_risk / t.total_inventory) * 100, 2)
      END AS percentage_of_inventory_at_risk
    FROM at_risk_cte a
    CROSS JOIN total_inventory_cte t
  )
SELECT 
  inventory_at_risk,
  total_inventory,
  percentage_of_inventory_at_risk
FROM result_cte
;

-------------------------------------------------------------------------------
/* SECTION: Inclusion/Exclusion Logic for Inventory at Risk */
-------------------------------------------------------------------------------

-- Purpose: Label each record as "included"/"not included" in inventory at risk, as per flags (case strict)
WITH incl_status AS (
  SELECT
    inventory_id,
    financial_qty,
    dnsa_flag,
    flag_active,
    CASE 
      WHEN flag_active IN ("Y", "y") AND dnsa_flag IN ("yes", "YES")
        THEN "included"
      ELSE "not included"
    END AS inclusion_status
  FROM purgo_demo.f_inv_movmnt
)
SELECT 
  inventory_id,
  financial_qty,
  dnsa_flag,
  flag_active,
  inclusion_status
FROM incl_status
ORDER BY inventory_id
;

-------------------------------------------------------------------------------
/* SECTION: Null, Missing, and Special Value Flag Handling */
-------------------------------------------------------------------------------

-- Purpose: Count and list records with NULL for dnsa_flag or flag_active (should not be included in at risk)
WITH flag_nulls AS (
  SELECT
    inventory_id,
    dnsa_flag,
    flag_active
  FROM purgo_demo.f_inv_movmnt
  WHERE flag_active IS NULL OR dnsa_flag IS NULL
)
SELECT
  COUNT(*) AS num_records_with_null_flag
FROM flag_nulls
;

-------------------------------------------------------------------------------
/* SECTION: No Inventory at Risk Edge Case */
-------------------------------------------------------------------------------

-- Purpose: Show result is zero if no records meet at-risk flags
WITH
  total AS (
    SELECT SUM(financial_qty) AS total_inventory FROM purgo_demo.f_inv_movmnt
  ),
  risk AS (
    SELECT SUM(financial_qty) AS inventory_at_risk
    FROM purgo_demo.f_inv_movmnt
    WHERE flag_active IN ("Y", "y") AND dnsa_flag IN ("yes", "YES")
  )
SELECT
  CASE WHEN COALESCE(risk.inventory_at_risk, 0) = 0 THEN 1 ELSE 0 END AS no_at_risk_flag,
  COALESCE(risk.inventory_at_risk, 0) AS inventory_at_risk
FROM risk, total
;

-------------------------------------------------------------------------------
/* SECTION: Zero/Empty Total Inventory Handling */
-------------------------------------------------------------------------------

-- Purpose: Return error message if total_inventory is zero or missing
WITH total_empty AS (
  SELECT COUNT(*) AS cnt, SUM(financial_qty) AS sum_qty
  FROM purgo_demo.f_inv_movmnt
  WHERE financial_qty IS NOT NULL
)
SELECT
  CASE
    WHEN cnt = 0 OR sum_qty = 0 THEN "Total inventory is zero, cannot calculate percentage"
    ELSE "OK"
  END AS zero_inventory_validation
FROM total_empty
;

-------------------------------------------------------------------------------
/* SECTION: Unexpected or Invalid Flag Values Logging */
-------------------------------------------------------------------------------

-- Purpose: List all records having flag values outside the allowed sets, and issue warning message
WITH invalid_flags AS (
  SELECT
    inventory_id,
    dnsa_flag,
    flag_active
  FROM purgo_demo.f_inv_movmnt
  WHERE
    (dnsa_flag IS NOT NULL AND dnsa_flag NOT IN ("yes", "YES", "no", "NO"))
    OR
    (flag_active IS NOT NULL AND flag_active NOT IN ("Y", "y", "N", "n"))
)
SELECT
  inventory_id,
  dnsa_flag,
  flag_active,
  "Unexpected flag value encountered" AS warning_message
FROM invalid_flags
;

-------------------------------------------------------------------------------
/* SECTION: Output Structure and Field Rounding */
-------------------------------------------------------------------------------

-- Purpose: Confirm output would produce 3 decimal columns, percent rounded to 2 decimals
WITH summary AS (
  SELECT
    SUM(CASE WHEN flag_active IN ("Y", "y") AND dnsa_flag IN ("yes", "YES") THEN financial_qty ELSE 0 END) AS inventory_at_risk,
    SUM(financial_qty) AS total_inventory
  FROM purgo_demo.f_inv_movmnt
)
SELECT
  inventory_at_risk, 
  total_inventory,
  CASE
    WHEN total_inventory = 0 THEN NULL
    ELSE ROUND((inventory_at_risk / total_inventory) * 100, 2)
  END AS percentage_of_inventory_at_risk
FROM summary
;

-------------------------------------------------------------------------------
/* SECTION: Unity Catalog Privilege/Access Check */
-------------------------------------------------------------------------------

-- Purpose: Assert SELECT privilege exists (should fail with "Insufficient privileges" if user lacks access)
SELECT "Privilege OK" AS privilege_check FROM purgo_demo.f_inv_movmnt WHERE 1=0
;

-------------------------------------------------------------------------------
/* END OF TEST SCRIPT */
-------------------------------------------------------------------------------
