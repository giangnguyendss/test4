-- Databricks SQL test data generation for purgo_playground.t3_itm_territory_sales, t3_ttm_territory_sales, control_table
-- Covers: happy path, edge, error, null, special/multibyte, duplicate, out-of-range, invalid, and boundary scenarios

-- 1. CONTROL TABLE: Insert 10 valid and 2 special/edge system/brand/country combos
INSERT INTO purgo_playground.control_table (country_code, source_system, brand_name)
VALUES
  -- Happy path
  ('US', 'SYSTEM_1', 'BRAND_A'),
  ('DE', 'SYSTEM_2', 'BRAND_B'),
  ('FR', 'SYSTEM_3', 'BRAND_C'),
  ('JP', 'SYSTEM_4', 'BRAND_D'),
  ('CN', 'SYSTEM_5', 'BRAND_E'),
  ('GB', 'SYSTEM_6', 'BRAND_F'),
  ('IN', 'SYSTEM_7', 'BRAND_G'),
  ('BR', 'SYSTEM_8', 'BRAND_H'),
  -- Special characters/multibyte
  ('KR', 'SYSTEM_9', 'BRÁND_Ü'), -- accented
  ('RU', 'SYSTEM_10', 'БРЕНД_Ж'), -- cyrillic
  -- Edge: system/brand/country not referenced in sales tables (should not match)
  ('ZZ', 'SYSTEM_X', 'BRAND_X'),
  ('YY', 'SYSTEM_Y', 'BRAND_Y')
;

-- 2. T3_ITM_TERRITORY_SALES: 15 diverse records
INSERT INTO purgo_playground.t3_itm_territory_sales
  (country_code, brand_name, source_system_name, territory_id, sales_value, sales_month)
VALUES
  -- Happy path: valid, matching control_table
  ('US', 'BRAND_A', 'SYSTEM_1', 'T001', 100.0, DATE'2023-05-01'),
  ('DE', 'BRAND_B', 'SYSTEM_2', 'T002', 200.0, DATE'2022-12-01'),
  ('FR', 'BRAND_C', 'SYSTEM_3', 'T003', 0.0, DATE'2024-01-01'),
  -- Only in ITM, not in TTM
  ('JP', 'BRAND_D', 'SYSTEM_4', 'T004', 500.0, DATE'2023-07-01'),
  -- Edge: sales_value = 0, boundary month/year
  ('CN', 'BRAND_E', 'SYSTEM_5', 'T005', 0.0, DATE'2022-01-01'),
  -- Edge: sales_value negative
  ('GB', 'BRAND_F', 'SYSTEM_6', 'T006', -50.0, DATE'2023-12-31'),
  -- Edge: sales_value very large
  ('IN', 'BRAND_G', 'SYSTEM_7', 'T007', 999999999.99, DATE'2024-06-30'),
  -- Special: accented/multibyte brand
  ('KR', 'BRÁND_Ü', 'SYSTEM_9', 'T008', 123.45, DATE'2023-03-15'),
  ('RU', 'БРЕНД_Ж', 'SYSTEM_10', 'T009', 543.21, DATE'2023-04-20'),
  -- NULL handling: sales_value is NULL (should be excluded)
  ('US', 'BRAND_A', 'SYSTEM_1', 'T001', NULL, DATE'2023-05-01'),
  -- NULL handling: sales_month is NULL (should be excluded)
  ('DE', 'BRAND_B', 'SYSTEM_2', 'T002', 100.0, NULL),
  -- NULL handling: country_code is NULL (should be excluded)
  (NULL, 'BRAND_C', 'SYSTEM_3', 'T003', 100.0, DATE'2024-01-01'),
  -- Error: sales_value is not numeric (should be excluded)
  ('FR', 'BRAND_C', 'SYSTEM_3', 'T003', CAST('abc' AS DOUBLE), DATE'2024-01-01'),
  -- Error: source_system_name not in control_table (should be excluded)
  ('US', 'BRAND_A', 'SYSTEM_X', 'T001', 100.0, DATE'2023-05-01'),
  -- Special: special characters in territory_id
  ('BR', 'BRAND_H', 'SYSTEM_8', 'T00@#$', 77.77, DATE'2023-08-08')
;

-- 3. T3_TTM_TERRITORY_SALES: 15 diverse records
INSERT INTO purgo_playground.t3_ttm_territory_sales
  (country_code, brand_name, source_system_name, territory_id, sales_net_price_local, fiscal_date)
VALUES
  -- Happy path: valid, matching control_table
  ('US', 'BRAND_A', 'SYSTEM_1', 'T001', 50.0, DATE'2023-05-15'),
  ('DE', 'BRAND_B', 'SYSTEM_2', 'T002', 0.0, DATE'2022-12-20'),
  ('FR', 'BRAND_C', 'SYSTEM_3', 'T003', 300.0, DATE'2024-01-10'),
  -- Only in TTM, not in ITM
  ('JP', 'BRAND_D', 'SYSTEM_4', 'T004', 250.0, DATE'2023-07-15'),
  -- Edge: sales_net_price_local = 0, boundary month/year
  ('CN', 'BRAND_E', 'SYSTEM_5', 'T005', 0.0, DATE'2022-01-15'),
  -- Edge: sales_net_price_local negative
  ('GB', 'BRAND_F', 'SYSTEM_6', 'T006', -25.0, DATE'2023-12-15'),
  -- Edge: sales_net_price_local very large
  ('IN', 'BRAND_G', 'SYSTEM_7', 'T007', 888888888.88, DATE'2024-06-15'),
  -- Special: accented/multibyte brand
  ('KR', 'BRÁND_Ü', 'SYSTEM_9', 'T008', 321.12, DATE'2023-03-20'),
  ('RU', 'БРЕНД_Ж', 'SYSTEM_10', 'T009', 123.45, DATE'2023-04-25'),
  -- NULL handling: sales_net_price_local is NULL (should be excluded)
  ('US', 'BRAND_A', 'SYSTEM_1', 'T001', NULL, DATE'2023-05-15'),
  -- NULL handling: fiscal_date is NULL (should be excluded)
  ('DE', 'BRAND_B', 'SYSTEM_2', 'T002', 100.0, NULL),
  -- NULL handling: country_code is NULL (should be excluded)
  (NULL, 'BRAND_C', 'SYSTEM_3', 'T003', 100.0, DATE'2024-01-10'),
  -- Error: sales_net_price_local is not numeric (should be excluded)
  ('FR', 'BRAND_C', 'SYSTEM_3', 'T003', CAST('xyz' AS DOUBLE), DATE'2024-01-10'),
  -- Error: source_system_name not in control_table (should be excluded)
  ('US', 'BRAND_A', 'SYSTEM_X', 'T001', 100.0, DATE'2023-05-15'),
  -- Special: special characters in territory_id
  ('BR', 'BRAND_H', 'SYSTEM_8', 'T00@#$', 88.88, DATE'2023-08-18')
;

-- 4. DUPLICATE RECORDS for aggregation test (should sum)
INSERT INTO purgo_playground.t3_itm_territory_sales
  (country_code, brand_name, source_system_name, territory_id, sales_value, sales_month)
VALUES
  -- Duplicates for US/BRAND_A/T001/2023-05
  ('US', 'BRAND_A', 'SYSTEM_1', 'T001', 50.0, DATE'2023-05-01'),
  ('US', 'BRAND_A', 'SYSTEM_1', 'T001', 25.0, DATE'2023-05-01')
;

INSERT INTO purgo_playground.t3_ttm_territory_sales
  (country_code, brand_name, source_system_name, territory_id, sales_net_price_local, fiscal_date)
VALUES
  -- Duplicates for US/BRAND_A/T001/2023-05
  ('US', 'BRAND_A', 'SYSTEM_1', 'T001', 25.0, DATE'2023-05-15'),
  ('US', 'BRAND_A', 'SYSTEM_1', 'T001', 25.0, DATE'2023-05-15')
;

-- 5. EDGE: Out-of-range/invalid combinations (should be excluded)
INSERT INTO purgo_playground.t3_itm_territory_sales
  (country_code, brand_name, source_system_name, territory_id, sales_value, sales_month)
VALUES
  -- Out-of-range month (should be valid, but test boundary)
  ('US', 'BRAND_A', 'SYSTEM_1', 'T001', 10.0, DATE'2023-12-31')
;

INSERT INTO purgo_playground.t3_ttm_territory_sales
  (country_code, brand_name, source_system_name, territory_id, sales_net_price_local, fiscal_date)
VALUES
  -- Out-of-range month (should be valid, but test boundary)
  ('US', 'BRAND_A', 'SYSTEM_1', 'T001', 20.0, DATE'2023-12-31')
;

-- 6. NULL/EMPTY/WHITESPACE/CONTROL CHARACTERS in brand_name/territory_id (should be included if not null)
INSERT INTO purgo_playground.t3_itm_territory_sales
  (country_code, brand_name, source_system_name, territory_id, sales_value, sales_month)
VALUES
  ('GB', '   ', 'SYSTEM_6', 'T010', 10.0, DATE'2023-11-11'), -- whitespace brand
  ('GB', 'BRAND_F', 'SYSTEM_6', '   ', 20.0, DATE'2023-11-11'), -- whitespace territory
  ('GB', 'BRAND_F', 'SYSTEM_6', CHAR(9)||'T011', 30.0, DATE'2023-11-11') -- tab char in territory_id
;

INSERT INTO purgo_playground.t3_ttm_territory_sales
  (country_code, brand_name, source_system_name, territory_id, sales_net_price_local, fiscal_date)
VALUES
  ('GB', '   ', 'SYSTEM_6', 'T010', 5.0, DATE'2023-11-15'), -- whitespace brand
  ('GB', 'BRAND_F', 'SYSTEM_6', '   ', 15.0, DATE'2023-11-15'), -- whitespace territory
  ('GB', 'BRAND_F', 'SYSTEM_6', CHAR(9)||'T011', 25.0, DATE'2023-11-15') -- tab char in territory_id
;

-- 7. MULTIBYTE/UNICODE in brand_name/territory_id
INSERT INTO purgo_playground.t3_itm_territory_sales
  (country_code, brand_name, source_system_name, territory_id, sales_value, sales_month)
VALUES
  ('JP', 'ブランドZ', 'SYSTEM_4', 'テリトリー1', 111.11, DATE'2023-09-09')
;

INSERT INTO purgo_playground.t3_ttm_territory_sales
  (country_code, brand_name, source_system_name, territory_id, sales_net_price_local, fiscal_date)
VALUES
  ('JP', 'ブランドZ', 'SYSTEM_4', 'テリトリー1', 222.22, DATE'2023-09-19')
;

-- 8. NULL in source_system_name (should be excluded)
INSERT INTO purgo_playground.t3_itm_territory_sales
  (country_code, brand_name, source_system_name, territory_id, sales_value, sales_month)
VALUES
  ('US', 'BRAND_A', NULL, 'T001', 100.0, DATE'2023-05-01')
;

INSERT INTO purgo_playground.t3_ttm_territory_sales
  (country_code, brand_name, source_system_name, territory_id, sales_net_price_local, fiscal_date)
VALUES
  ('US', 'BRAND_A', NULL, 'T001', 100.0, DATE'2023-05-15')
;

-- 9. NULL in brand_name (should be excluded)
INSERT INTO purgo_playground.t3_itm_territory_sales
  (country_code, brand_name, source_system_name, territory_id, sales_value, sales_month)
VALUES
  ('US', NULL, 'SYSTEM_1', 'T001', 100.0, DATE'2023-05-01')
;

INSERT INTO purgo_playground.t3_ttm_territory_sales
  (country_code, brand_name, source_system_name, territory_id, sales_net_price_local, fiscal_date)
VALUES
  ('US', NULL, 'SYSTEM_1', 'T001', 100.0, DATE'2023-05-15')
;

-- 10. NULL in territory_id (should be excluded)
INSERT INTO purgo_playground.t3_itm_territory_sales
  (country_code, brand_name, source_system_name, territory_id, sales_value, sales_month)
VALUES
  ('US', 'BRAND_A', 'SYSTEM_1', NULL, 100.0, DATE'2023-05-01')
;

INSERT INTO purgo_playground.t3_ttm_territory_sales
  (country_code, brand_name, source_system_name, territory_id, sales_net_price_local, fiscal_date)
VALUES
  ('US', 'BRAND_A', 'SYSTEM_1', NULL, 100.0, DATE'2023-05-15')
;

-- 11. NULL in both sales_value and sales_net_price_local for a key (should not produce output for that key)
INSERT INTO purgo_playground.t3_itm_territory_sales
  (country_code, brand_name, source_system_name, territory_id, sales_value, sales_month)
VALUES
  ('CA', 'BRAND_I', 'SYSTEM_11', 'T012', NULL, DATE'2023-10-10')
;

INSERT INTO purgo_playground.t3_ttm_territory_sales
  (country_code, brand_name, source_system_name, territory_id, sales_net_price_local, fiscal_date)
VALUES
  ('CA', 'BRAND_I', 'SYSTEM_11', 'T012', NULL, DATE'2023-10-20')
;

-- 12. Out-of-range: sales_value and sales_net_price_local extremely large/small
INSERT INTO purgo_playground.t3_itm_territory_sales
  (country_code, brand_name, source_system_name, territory_id, sales_value, sales_month)
VALUES
  ('IN', 'BRAND_G', 'SYSTEM_7', 'T007', -999999999.99, DATE'2024-06-30')
;

INSERT INTO purgo_playground.t3_ttm_territory_sales
  (country_code, brand_name, source_system_name, territory_id, sales_net_price_local, fiscal_date)
VALUES
  ('IN', 'BRAND_G', 'SYSTEM_7', 'T007', -888888888.88, DATE'2024-06-15')
;

-- 13. Special: brand_name with emoji
INSERT INTO purgo_playground.t3_itm_territory_sales
  (country_code, brand_name, source_system_name, territory_id, sales_value, sales_month)
VALUES
  ('US', 'BRAND_😀', 'SYSTEM_1', 'T013', 13.13, DATE'2023-05-01')
;

INSERT INTO purgo_playground.t3_ttm_territory_sales
  (country_code, brand_name, source_system_name, territory_id, sales_net_price_local, fiscal_date)
VALUES
  ('US', 'BRAND_😀', 'SYSTEM_1', 'T013', 31.31, DATE'2023-05-15')
;

-- 14. Special: territory_id with emoji
INSERT INTO purgo_playground.t3_itm_territory_sales
  (country_code, brand_name, source_system_name, territory_id, sales_value, sales_month)
VALUES
  ('US', 'BRAND_A', 'SYSTEM_1', 'T014😀', 14.14, DATE'2023-05-01')
;

INSERT INTO purgo_playground.t3_ttm_territory_sales
  (country_code, brand_name, source_system_name, territory_id, sales_net_price_local, fiscal_date)
VALUES
  ('US', 'BRAND_A', 'SYSTEM_1', 'T014😀', 41.41, DATE'2023-05-15')
;

-- 15. Special: brand_name and territory_id with newline
INSERT INTO purgo_playground.t3_itm_territory_sales
  (country_code, brand_name, source_system_name, territory_id, sales_value, sales_month)
VALUES
  ('US', 'BRAND_\nA', 'SYSTEM_1', 'T015\n', 15.15, DATE'2023-05-01')
;

INSERT INTO purgo_playground.t3_ttm_territory_sales
  (country_code, brand_name, source_system_name, territory_id, sales_net_price_local, fiscal_date)
VALUES
  ('US', 'BRAND_\nA', 'SYSTEM_1', 'T015\n', 51.51, DATE'2023-05-15')
;
