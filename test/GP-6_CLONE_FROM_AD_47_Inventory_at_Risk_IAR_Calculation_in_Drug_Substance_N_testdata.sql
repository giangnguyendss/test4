-- Databricks SQL script - Comprehensive test data generation for inventory at risk calculation based on DNSA events
-- Purpose: Generate diverse test cases in purgo_demo.f_inv_movmnt for Inventory at Risk calculation scenarios
-- Author: Giang Nguyen
-- Date: 2025-07-22
-- Description: This script creates a CTE with 25 records, covering happy path, edge, error, NULL, and special character data for the f_inv_movmnt table.
-- It covers variations for dnsa_flag, flag_active, negative and zero quantities, special/multibyte chars, and inconsistent values.

-- CTE: Generate comprehensive test records for purgo_demo.f_inv_movmnt to support DNSA-based inventory at risk logic
WITH test_data AS (
  SELECT
    /* Scenario: Happy path, at risk (YES/Y) */
    'A1'    AS inventory_id,
    CAST('100.50' AS DECIMAL(12,2)) AS financial_qty,
    'yes'   AS dnsa_flag,
    'Y'     AS flag_active
  UNION ALL SELECT
    /* Scenario: Not at risk (NO/Y) */
    'A2', CAST('200.00' AS DECIMAL(12,2)), 'no', 'Y'
  UNION ALL SELECT
    /* Scenario: Not at risk (yes/N) */
    'A3', CAST('50.00' AS DECIMAL(12,2)), 'yes', 'N'
  UNION ALL SELECT
    /* Scenario: Happy path, at risk (YES/y) */
    'A4', CAST('75.00' AS DECIMAL(12,2)), 'YES', 'y'
  UNION ALL SELECT
    /* Scenario: Not at risk (no/y) */
    'A5', CAST('25.00' AS DECIMAL(12,2)), 'no', 'y'
  UNION ALL SELECT
    /* Scenario: Not at risk (YES/N) */
    'A6', CAST('30.00' AS DECIMAL(12,2)), 'YES', 'N'
  UNION ALL SELECT
    /* Edge: Zero quantity, flagged at risk */
    'E1', CAST('0.00' AS DECIMAL(12,2)), 'YES', 'Y'
  UNION ALL SELECT
    /* Error: Negative financial_qty (should cause error in downstream logic) */
    'ERR1', CAST('-10.00' AS DECIMAL(12,2)), 'YES', 'Y'
  UNION ALL SELECT
    /* NULL dnsa_flag */
    'N1', CAST('80.00' AS DECIMAL(12,2)), NULL, 'Y'
  UNION ALL SELECT
    /* NULL flag_active */
    'N2', CAST('12.00' AS DECIMAL(12,2)), 'YES', NULL
  UNION ALL SELECT
    /* Both dnsa_flag and flag_active NULL */
    'N3', CAST('35.00' AS DECIMAL(12,2)), NULL, NULL
  UNION ALL SELECT
    /* Not at risk: lowercase only in dnsa_flag */
    'E2', CAST('44.00' AS DECIMAL(12,2)), 'Yes', 'y'
  UNION ALL SELECT
    /* Not at risk: lowercase only in flag_active */
    'E3', CAST('38.00' AS DECIMAL(12,2)), 'YES', 'n'
  UNION ALL SELECT
    /* At risk - alternate happy path */
    'T1', CAST('55.25' AS DECIMAL(12,2)), 'yes', 'Y'
  UNION ALL SELECT
    /* Not at risk: invalid flag values */
    'FG1', CAST('22.00' AS DECIMAL(12,2)), 'yess', 'Y'
  UNION ALL SELECT
    -- Not at risk: flag_active invalid
    'FG2', CAST('18.50' AS DECIMAL(12,2)), 'YES', 'active'
  UNION ALL SELECT
    -- Not at risk: dnsa_flag is numeric string
    'FG3', CAST('16.00' AS DECIMAL(12,2)), '1', 'Y'
  UNION ALL SELECT
    /* Not at risk: flag_active is numeric string */
    'FG4', CAST('20.00' AS DECIMAL(12,2)), 'YES', '1'
  UNION ALL SELECT
    /* Not at risk: extra whitespace in flag, should not match */
    'W1', CAST('29.00' AS DECIMAL(12,2)), 'yes ', 'Y'
  UNION ALL SELECT
    /* Not at risk: nulls (both flags null) */
    'NULL1', CAST('0.00' AS DECIMAL(12,2)), NULL, NULL
  UNION ALL SELECT
    /* Edge: special characters in ID, at risk */
    'SP1*', CAST('60.00' AS DECIMAL(12,2)), 'YES', 'y'
  UNION ALL SELECT
    /* Edge: multi-byte characters in ID */
    '多語B1', CAST('59.10' AS DECIMAL(12,2)), 'yes', 'Y'
  UNION ALL SELECT
    /* At risk: case-insensitive flags not matching, should not include */
    'EX1', CAST('73.55' AS DECIMAL(12,2)), 'Yes', 'Y'
  UNION ALL SELECT
    /* Not at risk: all fields empty string */
    '', CAST('0.00' AS DECIMAL(12,2)), '', ''
  UNION ALL SELECT
    /* Not at risk: valid value, all dnsa/flag_active 'N'/'no' */
    'NR1', CAST('99.99' AS DECIMAL(12,2)), 'no', 'n'
  UNION ALL SELECT
    /* Not at risk: valid value, flag_active NULL, dnsa_flag valid */
    'B9', CAST('12.00' AS DECIMAL(12,2)), 'YES', NULL
)

-- Validation Query: Ensure all generated data is correct and CASTs are valid
-- Purpose: Validate test data covers all specified scenarios for downstream analytics consumption
SELECT
  inventory_id,
  financial_qty,
  dnsa_flag,
  flag_active
FROM test_data
-- No LIMIT: fetch all records
;
