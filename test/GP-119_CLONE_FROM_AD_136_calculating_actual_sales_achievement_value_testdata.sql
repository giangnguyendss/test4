-- Databricks SQL test data generation for purgo_playground.t3_itm_territory_sales, t3_ttm_territory_sales, control_table
-- Covers: happy path, edge, error, null, special/multibyte, aggregation, type/format, exclusion, join, and output validation

-- 1. CONTROL TABLE TEST DATA
WITH control_table_test_data AS (
  SELECT * FROM VALUES
    -- Happy path: SAP, IQVIA, special chars, multibyte, edge
    ('US',    'SAP',      'BRANDX'),
    ('DE',    'IQVIA',    'BRANDY'),
    ('FR',    'SAP',      'BRANDZ'),
    ('JP',    'SAP',      'ブランドA'),         -- multibyte brand
    ('CN',    'IQVIA',    '品牌B'),             -- multibyte brand
    ('US',    'IQVIA',    'BRAND_SPECIAL!@#'),  -- special chars
    ('GB',    'SAP',      'BRANDY'),
    ('US',    'SAP',      'BRAND_NULL'),        -- for null test
    ('US',    'SAP',      'BRAND_AGG'),
    ('US',    'SAP',      'BRAND_ERR'),
    ('US',    'SAP',      'BRAND_EDGE'),
    ('US',    'SAP',      'BRAND_Ω'),           -- multibyte/special
    ('US',    'SAP',      'BRAND_空'),           -- multibyte
    ('US',    'SAP',      'BRAND-123'),
    ('US',    'SAP',      'BRAND-NULL')
  AS (country_code, source_system, brand_name)
),

-- 2. T3_ITM_TERRITORY_SALES TEST DATA
t3_itm_territory_sales_test_data AS (
  SELECT * FROM VALUES
    -- Happy path
    ('US', 'BRANDX',    'SAP',      'T001', 100.0, DATE'2023-05-01'),
    ('DE', 'BRANDY',    'IQVIA',    'T002',  50.0, DATE'2022-12-01'),
    -- Null sales_value
    ('US', 'BRANDX',    'SAP',      'T001', NULL,  DATE'2023-05-01'),
    -- Null brand_name, country_code, territory_id (should be excluded)
    (NULL, 'BRANDX',    'SAP',      'T001', 100.0, DATE'2023-05-01'),
    ('US', NULL,        'SAP',      'T001', 100.0, DATE'2023-05-01'),
    ('US', 'BRANDX',    'SAP',      NULL,   100.0, DATE'2023-05-01'),
    -- Special chars/multibyte
    ('US', 'BRAND_SPECIAL!@#', 'IQVIA', 'T003', 123.45, DATE'2024-01-15'),
    ('JP', 'ブランドA',  'SAP',      'T004',  88.8,  DATE'2024-02-01'),
    ('CN', '品牌B',      'IQVIA',    'T005',  77.7,  DATE'2024-03-01'),
    -- Edge: min/max values
    ('US', 'BRAND_EDGE', 'SAP',      'T006', 0.0,    DATE'2024-04-01'),
    ('US', 'BRAND_EDGE', 'SAP',      'T006', 999999999.99, DATE'2024-04-01'),
    -- Error: source_system_name not in control_table
    ('US', 'BRAND_ERR',  'NOT_IN_CT','T007',  55.0,  DATE'2024-05-01'),
    -- Error: source_system_name in control_table but for different brand/country
    ('US', 'BRAND_ERR',  'SAP',      'T008',  66.0,  DATE'2024-06-01'),
    -- Aggregation: multiple records for same group
    ('US', 'BRAND_AGG',  'SAP',      'T009',  10.0,  DATE'2024-07-01'),
    ('US', 'BRAND_AGG',  'SAP',      'T009',  20.0,  DATE'2024-07-01'),
    -- Null sales_month (should be excluded)
    ('US', 'BRANDX',     'SAP',      'T001',  100.0, NULL),
    -- Multibyte/special
    ('US', 'BRAND_Ω',    'SAP',      'T010',  42.0,  DATE'2024-08-01'),
    ('US', 'BRAND_空',   'SAP',      'T011',  24.0,  DATE'2024-09-01'),
    -- Edge: month/year boundary
    ('US', 'BRANDX',     'SAP',      'T001',  1.0,   DATE'2023-12-31'),
    ('US', 'BRANDX',     'SAP',      'T001',  2.0,   DATE'2024-01-01'),
    -- For join validation: mismatched month/year
    ('US', 'BRANDX',     'SAP',      'T001',  5.0,   DATE'2023-06-01'),
    -- For output type/format validation
    ('GB', 'BRANDY',     'SAP',      'T012',  77.0,  DATE'2024-10-01')
  AS (country_code, brand_name, source_system_name, territory_id, sales_value, sales_month)
),

-- 3. T3_TTM_TERRITORY_SALES TEST DATA
t3_ttm_territory_sales_test_data AS (
  SELECT * FROM VALUES
    -- Happy path
    ('US', 'BRANDX',    'SAP',      'T001', 200.0, DATE'2023-05-15'),
    ('DE', 'BRANDY',    'IQVIA',    'T002',  75.0, DATE'2022-12-20'),
    -- Null sales_net_price_local
    ('US', 'BRANDX',    'SAP',      'T001', NULL,  DATE'2023-05-15'),
    -- Null brand_name, country_code, territory_id (should be excluded)
    (NULL, 'BRANDX',    'SAP',      'T001', 200.0, DATE'2023-05-15'),
    ('US', NULL,        'SAP',      'T001', 200.0, DATE'2023-05-15'),
    ('US', 'BRANDX',    'SAP',      NULL,   200.0, DATE'2023-05-15'),
    -- Special chars/multibyte
    ('US', 'BRAND_SPECIAL!@#', 'IQVIA', 'T003', 321.54, DATE'2024-01-20'),
    ('JP', 'ブランドA',  'SAP',      'T004',  99.9,  DATE'2024-02-10'),
    ('CN', '品牌B',      'IQVIA',    'T005',  66.6,  DATE'2024-03-15'),
    -- Edge: min/max values
    ('US', 'BRAND_EDGE', 'SAP',      'T006', 0.0,    DATE'2024-04-15'),
    ('US', 'BRAND_EDGE', 'SAP',      'T006', 888888888.88, DATE'2024-04-15'),
    -- Error: source_system_name not in control_table
    ('US', 'BRAND_ERR',  'NOT_IN_CT','T007',  44.0,  DATE'2024-05-15'),
    -- Error: source_system_name in control_table but for different brand/country
    ('US', 'BRAND_ERR',  'SAP',      'T008',  33.0,  DATE'2024-06-15'),
    -- Aggregation: multiple records for same group
    ('US', 'BRAND_AGG',  'SAP',      'T009',  30.0,  DATE'2024-07-15'),
    ('US', 'BRAND_AGG',  'SAP',      'T009',  40.0,  DATE'2024-07-15'),
    -- Null fiscal_date (should be excluded)
    ('US', 'BRANDX',     'SAP',      'T001',  200.0, NULL),
    -- Multibyte/special
    ('US', 'BRAND_Ω',    'SAP',      'T010',  24.0,  DATE'2024-08-15'),
    ('US', 'BRAND_空',   'SAP',      'T011',  42.0,  DATE'2024-09-15'),
    -- Edge: month/year boundary
    ('US', 'BRANDX',     'SAP',      'T001',  3.0,   DATE'2023-12-01'),
    ('US', 'BRANDX',     'SAP',      'T001',  4.0,   DATE'2024-01-31'),
    -- For join validation: mismatched month/year
    ('US', 'BRANDX',     'SAP',      'T001',  6.0,   DATE'2023-07-01'),
    -- For output type/format validation
    ('GB', 'BRANDY',     'SAP',      'T012',  88.0,  DATE'2024-10-15')
  AS (country_code, brand_name, source_system_name, territory_id, sales_net_price_local, fiscal_date)
)

-- 4. INSERT TEST DATA INTO TABLES
-- (Use INSERT INTO ... SELECT FROM CTE for each table)

-- Insert into control_table
INSERT INTO purgo_playground.control_table (country_code, source_system, brand_name)
SELECT country_code, source_system, brand_name FROM control_table_test_data;

-- Insert into t3_itm_territory_sales
INSERT INTO purgo_playground.t3_itm_territory_sales (country_code, brand_name, source_system_name, territory_id, sales_value, sales_month)
SELECT country_code, brand_name, source_system_name, territory_id, sales_value, sales_month FROM t3_itm_territory_sales_test_data;

-- Insert into t3_ttm_territory_sales
INSERT INTO purgo_playground.t3_ttm_territory_sales (country_code, brand_name, source_system_name, territory_id, sales_net_price_local, fiscal_date)
SELECT country_code, brand_name, source_system_name, territory_id, sales_net_price_local, fiscal_date FROM t3_ttm_territory_sales_test_data;
