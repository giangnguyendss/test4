-- Test Data Generation for purgo_playground.control_table
-- Covers: happy path, edge, error, null, special/multibyte char scenarios

-- 1. CONTROL TABLE
CREATE OR REPLACE TABLE purgo_playground.control_table (
  country_code STRING,
  source_system STRING,
  brand_name STRING
);

INSERT INTO purgo_playground.control_table (country_code, source_system, brand_name) VALUES
  -- Happy path: valid source systems
  ('US', 'SAP', 'BRANDX'),
  ('DE', 'IQVIA', 'BRANDY'),
  ('JP', 'IQVIA', 'BRANDZ'),
  ('CN', 'SAP', 'BRANDΩ'), -- Multibyte char
  ('FR', 'IQVIA', 'BRAND-É'), -- Special char
  -- Edge: lower/upper case, long string
  ('GB', 'sap', 'BRANDLONGNAME12345678901234567890'),
  ('IT', 'IQVIA', 'BRAND_SPECIAL!@#'),
  -- Error: source system not referenced in sales tables
  ('US', 'NOTUSED', 'BRANDY'),
  -- NULL/empty
  (NULL, 'SAP', 'BRANDNULL'),
  ('US', NULL, 'BRANDNULL2'),
  ('US', 'SAP', NULL);

-- 2. T3_ITM_TERRITORY_SALES
CREATE OR REPLACE TABLE purgo_playground.t3_itm_territory_sales (
  country_code STRING,
  brand_name STRING,
  source_system_name STRING,
  territory_id STRING,
  sales_value DOUBLE,
  sales_month DATE
);

INSERT INTO purgo_playground.t3_itm_territory_sales (country_code, brand_name, source_system_name, territory_id, sales_value, sales_month) VALUES
  -- Happy path
  ('US', 'BRANDX', 'SAP', 'T001', 100.50, DATE'2023-05-01'),
  ('DE', 'BRANDY', 'IQVIA', 'T002', 50.00, DATE'2022-12-01'),
  -- Edge: boundary values
  ('JP', 'BRANDZ', 'IQVIA', 'T003', 0.0, DATE'2024-01-01'), -- zero sales
  ('CN', 'BRANDΩ', 'SAP', 'T004', 99999999.99, DATE'2024-06-30'), -- large value, multibyte
  ('FR', 'BRAND-É', 'IQVIA', 'T005', 1.23, DATE'2024-02-29'), -- leap year, special char
  -- Error: source_system_name not in control_table
  ('US', 'BRANDX', 'NOTINCTL', 'T001', 200.00, DATE'2023-05-01'),
  -- Error: sales_value not numeric (should be excluded)
  ('US', 'BRANDX', 'SAP', 'T001', CAST('INVALID' AS DOUBLE), DATE'2023-05-01'),
  -- NULLs in required fields
  (NULL, 'BRANDX', 'SAP', 'T001', 100.50, DATE'2023-05-01'),
  ('US', NULL, 'SAP', 'T001', 100.50, DATE'2023-05-01'),
  ('US', 'BRANDX', NULL, 'T001', 100.50, DATE'2023-05-01'),
  ('US', 'BRANDX', 'SAP', NULL, 100.50, DATE'2023-05-01'),
  ('US', 'BRANDX', 'SAP', 'T001', NULL, DATE'2023-05-01'),
  ('US', 'BRANDX', 'SAP', 'T001', 100.50, NULL),
  -- Special/multibyte chars
  ('CN', 'BRANDΩ', 'SAP', 'T004', 888.88, DATE'2024-06-01'),
  ('FR', 'BRAND-É', 'IQVIA', 'T005', 2.34, DATE'2024-02-01'),
  ('IT', 'BRAND_SPECIAL!@#', 'IQVIA', 'T006', 77.77, DATE'2024-03-01'),
  -- Edge: lower/upper case, long string
  ('GB', 'BRANDLONGNAME12345678901234567890', 'sap', 'T007', 123.45, DATE'2024-04-01'),
  -- Multiple records for same combination (to test aggregation)
  ('US', 'BRANDX', 'SAP', 'T001', 10.00, DATE'2023-05-01'),
  ('US', 'BRANDX', 'SAP', 'T001', 20.00, DATE'2023-05-01'),
  -- Out-of-range/negative
  ('DE', 'BRANDY', 'IQVIA', 'T002', -5.00, DATE'2022-12-01');

-- 3. T3_TTM_TERRITORY_SALES
CREATE OR REPLACE TABLE purgo_playground.t3_ttm_territory_sales (
  country_code STRING,
  brand_name STRING,
  source_system_name STRING,
  territory_id STRING,
  sales_net_price_local DOUBLE,
  fiscal_date DATE
);

INSERT INTO purgo_playground.t3_ttm_territory_sales (country_code, brand_name, source_system_name, territory_id, sales_net_price_local, fiscal_date) VALUES
  -- Happy path
  ('US', 'BRANDX', 'SAP', 'T001', 200.75, DATE'2023-05-15'),
  ('DE', 'BRANDY', 'IQVIA', 'T002', 75.00, DATE'2022-12-20'),
  -- Edge: boundary values
  ('JP', 'BRANDZ', 'IQVIA', 'T003', 0.0, DATE'2024-01-15'), -- zero
  ('CN', 'BRANDΩ', 'SAP', 'T004', 12345678.90, DATE'2024-06-15'), -- large, multibyte
  ('FR', 'BRAND-É', 'IQVIA', 'T005', 3.21, DATE'2024-02-28'), -- leap year, special char
  -- Error: source_system_name not in control_table
  ('US', 'BRANDX', 'NOTINCTL', 'T001', 300.00, DATE'2023-05-15'),
  -- Error: sales_net_price_local not numeric (should be excluded)
  ('US', 'BRANDX', 'SAP', 'T001', CAST('INVALID' AS DOUBLE), DATE'2023-05-15'),
  -- NULLs in required fields
  (NULL, 'BRANDX', 'SAP', 'T001', 200.75, DATE'2023-05-15'),
  ('US', NULL, 'SAP', 'T001', 200.75, DATE'2023-05-15'),
  ('US', 'BRANDX', NULL, 'T001', 200.75, DATE'2023-05-15'),
  ('US', 'BRANDX', 'SAP', NULL, 200.75, DATE'2023-05-15'),
  ('US', 'BRANDX', 'SAP', 'T001', NULL, DATE'2023-05-15'),
  ('US', 'BRANDX', 'SAP', 'T001', 200.75, NULL),
  -- Special/multibyte chars
  ('CN', 'BRANDΩ', 'SAP', 'T004', 777.77, DATE'2024-06-10'),
  ('FR', 'BRAND-É', 'IQVIA', 'T005', 4.56, DATE'2024-02-10'),
  ('IT', 'BRAND_SPECIAL!@#', 'IQVIA', 'T006', 66.66, DATE'2024-03-10'),
  -- Edge: lower/upper case, long string
  ('GB', 'BRANDLONGNAME12345678901234567890', 'sap', 'T007', 543.21, DATE'2024-04-10'),
  -- Multiple records for same combination (to test aggregation)
  ('US', 'BRANDX', 'SAP', 'T001', 5.00, DATE'2023-05-15'),
  ('US', 'BRANDX', 'SAP', 'T001', 15.00, DATE'2023-05-15'),
  -- Out-of-range/negative
  ('DE', 'BRANDY', 'IQVIA', 'T002', -10.00, DATE'2022-12-20');

-- 4. VALIDATION QUERY CTE (for test validation, not for test data generation)
-- Not included as per instruction: only test data generation code
