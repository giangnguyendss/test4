-- Test Data Generation for purgo_playground.f_inv_movmnt
-- Covers: happy path, edge, error, null, special/multibyte char, default, and mapping logic scenarios

WITH test_data AS (
  SELECT
    -- 1. Happy path: All fields present, FERT stock type, LZBEP = B, lifdr present
    '10001|01|0001' AS txn_id,
    'L001' AS inv_loc,
    20.0 AS financial_qty,
    20.0 AS net_qty,
    CAST(20240101 AS DECIMAL(38,0)) AS expired_qt,
    'ABC123' AS item_nbr,
    10.00 AS unit_cost,
    0.76 AS uom_rate,
    '001' AS plant_loc_cd,
    'SUPP001' AS inv_stock_reference,
    'FG' AS stock_type,
    20.0 AS qty_on_hand,
    0.0 AS qty_shipped,
    NULL AS cancel_dt,
    'no' AS flag_active,
    TIMESTAMP('2024-03-21T00:00:00.000+0000') AS crt_dt,
    TIMESTAMP('2024-03-21T00:00:00.000+0000') AS updt_dt

  UNION ALL

    -- 2. Happy path: ROH stock type, LZBEP = L, dunnr present
    SELECT
    '10002|02|0002', 'L002', 14.0, 28.0, CAST(20240102 AS DECIMAL(38,0)), 'DEF456', 30.00, 0.76, '002', 'DUNN002', 'RAW', 28.0, 0.0, NULL, 'no',
    TIMESTAMP('2024-03-21T01:00:00.000+0000'), TIMESTAMP('2024-03-21T01:00:00.000+0000')

  UNION ALL

    -- 3. Happy path: HALB stock type, LZBEP = L, dunnr blank, sdauf present, VBAX.dunnr used
    SELECT
    '10003|03|0003', 'L003', 0.0, 0.0, CAST(99991231 AS DECIMAL(38,0)), 'GHI789', 0.00, 0.76, '003', 'DUNN456', 'WIP', 0.0, 0.0, NULL, 'no',
    TIMESTAMP('2024-03-21T02:00:00.000+0000'), TIMESTAMP('2024-03-21T02:00:00.000+0000')

  UNION ALL

    -- 4. Default values: missing source fields, all nulls/defaults
    SELECT
    '20001|01|0001', 'none', 0.0, 0.0, CAST(99991231 AS DECIMAL(38,0)), 'JKL012', 0.00, 0.76, 'none', 'None', 'OT', 0.0, 0.0, NULL, 'no',
    CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()

  UNION ALL

    -- 5. Error case: duplicate txn_id (should be detected by validation, only one inserted)
    SELECT
    '30001|01|0001', 'L005', 12.0, 10.0, CAST(20240105 AS DECIMAL(38,0)), 'MNO345', 15.00, 0.76, '005', 'SUPP2', 'FG', 10.0, 2.0, NULL, 'yes',
    TIMESTAMP('2024-03-21T03:00:00.000+0000'), TIMESTAMP('2024-03-21T03:00:00.000+0000')

  UNION ALL

    -- 6. Edge: expired_qt is null (should default to 99991231)
    SELECT
    '40001|01|0001', 'L006', 5.0, 5.0, CAST(99991231 AS DECIMAL(38,0)), 'PQR678', 5.00, 0.76, '006', 'None', 'OT', 5.0, 0.0, NULL, 'no',
    TIMESTAMP('2024-03-21T04:00:00.000+0000'), TIMESTAMP('2024-03-21T04:00:00.000+0000')

  UNION ALL

    -- 7. Edge: cancel_dt set (record missing in today's data)
    SELECT
    '50001|01|0001', 'L007', 8.0, 8.0, CAST(20240107 AS DECIMAL(38,0)), 'STU901', 8.00, 0.76, '007', 'SUPP007', 'FG', 8.0, 0.0, CAST(20240320 AS DECIMAL(38,0)), 'no',
    TIMESTAMP('2024-03-21T05:00:00.000+0000'), TIMESTAMP('2024-03-21T05:00:00.000+0000')

  UNION ALL

    -- 8. Special char: item_nbr with special and multibyte chars
    SELECT
    '60001|01|0001', 'L008', 9.0, 9.0, CAST(20240108 AS DECIMAL(38,0)), '特殊字符-ßΩ', 9.99, 0.76, '008', 'SUPP008', 'FG', 9.0, 0.0, NULL, 'yes',
    TIMESTAMP('2024-03-21T06:00:00.000+0000'), TIMESTAMP('2024-03-21T06:00:00.000+0000')

  UNION ALL

    -- 9. Edge: unit_cost division by zero (peinl = 0), should default to 0.00
    SELECT
    '70001|01|0001', 'L009', 0.0, 0.0, CAST(20240109 AS DECIMAL(38,0)), 'DIV0', 0.00, 0.76, '009', 'None', 'OT', 0.0, 0.0, NULL, 'no',
    TIMESTAMP('2024-03-21T07:00:00.000+0000'), TIMESTAMP('2024-03-21T07:00:00.000+0000')

  UNION ALL

    -- 10. Edge: LZBEP = L, dunnr and sdauf blank, inv_stock_reference = None
    SELECT
    '80001|01|0001', 'L010', 11.0, 11.0, CAST(20240110 AS DECIMAL(38,0)), 'NOP123', 11.00, 0.76, '010', 'None', 'RAW', 11.0, 0.0, NULL, 'no',
    TIMESTAMP('2024-03-21T08:00:00.000+0000'), TIMESTAMP('2024-03-21T08:00:00.000+0000')

  UNION ALL

    -- 11. Edge: LZBEP = X, inv_stock_reference = None
    SELECT
    '90001|01|0001', 'L011', 13.0, 13.0, CAST(20240111 AS DECIMAL(38,0)), 'XYZ789', 13.00, 0.76, '011', 'None', 'OT', 13.0, 0.0, NULL, 'no',
    TIMESTAMP('2024-03-21T09:00:00.000+0000'), TIMESTAMP('2024-03-21T09:00:00.000+0000')

  UNION ALL

    -- 12. Edge: stock_type = OT (unknown ptart)
    SELECT
    '10011|01|0001', 'L012', 7.0, 7.0, CAST(20240112 AS DECIMAL(38,0)), 'UNK001', 7.00, 0.76, '012', 'SUPP012', 'OT', 7.0, 0.0, NULL, 'no',
    TIMESTAMP('2024-03-21T10:00:00.000+0000'), TIMESTAMP('2024-03-21T10:00:00.000+0000')

  UNION ALL

    -- 13. Edge: flag_active = yes (dwart = AB)
    SELECT
    '10012|01|0001', 'L013', 6.0, 6.0, CAST(20240113 AS DECIMAL(38,0)), 'YES001', 6.00, 0.76, '013', 'SUPP013', 'FG', 6.0, 0.0, NULL, 'yes',
    TIMESTAMP('2024-03-21T11:00:00.000+0000'), TIMESTAMP('2024-03-21T11:00:00.000+0000')

  UNION ALL

    -- 14. Edge: flag_active = no (dwart = RX)
    SELECT
    '10013|01|0001', 'L014', 5.0, 5.0, CAST(20240114 AS DECIMAL(38,0)), 'NO001', 5.00, 0.76, '014', 'SUPP014', 'FG', 5.0, 0.0, NULL, 'no',
    TIMESTAMP('2024-03-21T12:00:00.000+0000'), TIMESTAMP('2024-03-21T12:00:00.000+0000')

  UNION ALL

    -- 15. Edge: qty_on_hand < financial_qty, qty_shipped = financial_qty - qty_on_hand
    SELECT
    '10014|01|0001', 'L015', 20.0, 15.0, CAST(20240115 AS DECIMAL(38,0)), 'SHIP001', 12.00, 0.76, '015', 'SUPP015', 'FG', 15.0, 5.0, NULL, 'yes',
    TIMESTAMP('2024-03-21T13:00:00.000+0000'), TIMESTAMP('2024-03-21T13:00:00.000+0000')

  UNION ALL

    -- 16. Edge: qty_on_hand = 0, qty_shipped = financial_qty
    SELECT
    '10015|01|0001', 'L016', 10.0, 0.0, CAST(20240116 AS DECIMAL(38,0)), 'SHIP002', 8.00, 0.76, '016', 'SUPP016', 'FG', 0.0, 10.0, NULL, 'no',
    TIMESTAMP('2024-03-21T14:00:00.000+0000'), TIMESTAMP('2024-03-21T14:00:00.000+0000')

  UNION ALL

    -- 17. Edge: qty_on_hand = financial_qty, qty_shipped = 0
    SELECT
    '10016|01|0001', 'L017', 5.0, 5.0, CAST(20240117 AS DECIMAL(38,0)), 'SHIP003', 6.00, 0.76, '017', 'SUPP017', 'FG', 5.0, 0.0, NULL, 'yes',
    TIMESTAMP('2024-03-21T15:00:00.000+0000'), TIMESTAMP('2024-03-21T15:00:00.000+0000')

  UNION ALL

    -- 18. Edge: NULL handling for all nullable fields
    SELECT
    '10017|01|0001', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
    CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()

  UNION ALL

    -- 19. Special char: inv_loc and plant_loc_cd with special/multibyte chars
    SELECT
    '10018|01|0001', '特殊L018', 18.0, 18.0, CAST(20240118 AS DECIMAL(38,0)), 'SPC001', 18.00, 0.76, '特殊P018', 'SUPP018', 'FG', 18.0, 0.0, NULL, 'yes',
    TIMESTAMP('2024-03-21T16:00:00.000+0000'), TIMESTAMP('2024-03-21T16:00:00.000+0000')

  UNION ALL

    -- 20. Edge: expired_qt with max value
    SELECT
    '10019|01|0001', 'L019', 19.0, 19.0, CAST(99991231 AS DECIMAL(38,0)), 'MAX001', 19.00, 0.76, '019', 'SUPP019', 'FG', 19.0, 0.0, NULL, 'yes',
    TIMESTAMP('2024-03-21T17:00:00.000+0000'), TIMESTAMP('2024-03-21T17:00:00.000+0000')

  UNION ALL

    -- 21. Edge: expired_qt with min value
    SELECT
    '10020|01|0001', 'L020', 20.0, 20.0, CAST(19000101 AS DECIMAL(38,0)), 'MIN001', 20.00, 0.76, '020', 'SUPP020', 'FG', 20.0, 0.0, NULL, 'yes',
    TIMESTAMP('2024-03-21T18:00:00.000+0000'), TIMESTAMP('2024-03-21T18:00:00.000+0000')

  UNION ALL

    -- 22. Edge: inv_loc, plant_loc_cd, inv_stock_reference, stock_type all null
    SELECT
    '10021|01|0001', NULL, 21.0, 21.0, CAST(20240121 AS DECIMAL(38,0)), 'NULLS001', 21.00, 0.76, NULL, NULL, NULL, 21.0, 0.0, NULL, 'no',
    TIMESTAMP('2024-03-21T19:00:00.000+0000'), TIMESTAMP('2024-03-21T19:00:00.000+0000')

  UNION ALL

    -- 23. Edge: qty_on_hand, qty_shipped null
    SELECT
    '10022|01|0001', 'L022', 22.0, NULL, CAST(20240122 AS DECIMAL(38,0)), 'QNULL001', 22.00, 0.76, '022', 'SUPP022', 'FG', NULL, NULL, NULL, 'yes',
    TIMESTAMP('2024-03-21T20:00:00.000+0000'), TIMESTAMP('2024-03-21T20:00:00.000+0000')

  UNION ALL

    -- 24. Edge: flag_active = no (dwart = null)
    SELECT
    '10023|01|0001', 'L023', 23.0, 23.0, CAST(20240123 AS DECIMAL(38,0)), 'NOFLAG001', 23.00, 0.76, '023', 'SUPP023', 'FG', 23.0, 0.0, NULL, 'no',
    TIMESTAMP('2024-03-21T21:00:00.000+0000'), TIMESTAMP('2024-03-21T21:00:00.000+0000')

  UNION ALL

    -- 25. Edge: unit_cost with 2 decimal rounding
    SELECT
    '10024|01|0001', 'L024', 24.0, 24.0, CAST(20240124 AS DECIMAL(38,0)), 'ROUND001', 33.3333, 0.76, '024', 'SUPP024', 'FG', 24.0, 0.0, NULL, 'yes',
    TIMESTAMP('2024-03-21T22:00:00.000+0000'), TIMESTAMP('2024-03-21T22:00:00.000+0000')

  UNION ALL

    -- 26. Edge: inv_stock_reference = None (LZBEP not B or L)
    SELECT
    '10025|01|0001', 'L025', 25.0, 25.0, CAST(20240125 AS DECIMAL(38,0)), 'NONE001', 25.00, 0.76, '025', 'None', 'FG', 25.0, 0.0, NULL, 'yes',
    TIMESTAMP('2024-03-21T23:00:00.000+0000'), TIMESTAMP('2024-03-21T23:00:00.000+0000')

  UNION ALL

    -- 27. Special char: flag_active with special char (should be 'yes' or 'no', but test for error)
    SELECT
    '10026|01|0001', 'L026', 26.0, 26.0, CAST(20240126 AS DECIMAL(38,0)), 'FLAGSPC', 26.00, 0.76, '026', 'SUPP026', 'FG', 26.0, 0.0, NULL, 'yës',
    TIMESTAMP('2024-03-21T23:30:00.000+0000'), TIMESTAMP('2024-03-21T23:30:00.000+0000')

  UNION ALL

    -- 28. Edge: All string fields with max length and special chars
    SELECT
    RPAD('99999', 50, 'X') || '|' || RPAD('99', 10, 'Y') || '|' || RPAD('9999', 10, 'Z'),
    RPAD('L999', 30, 'Ω'),
    99.99, 99.99, CAST(20241231 AS DECIMAL(38,0)),
    RPAD('ITEM999', 40, 'ß'),
    99.99, 0.76,
    RPAD('PLANT999', 30, '€'),
    RPAD('SUPP999', 30, '¥'),
    'FG',
    99.99, 0.0, NULL, 'yes',
    TIMESTAMP('2024-03-21T23:59:59.000+0000'), TIMESTAMP('2024-03-21T23:59:59.000+0000')

  UNION ALL

    -- 29. Edge: All numeric fields at min value
    SELECT
    '10027|01|0001', 'L027', 0.0, 0.0, CAST(19000101 AS DECIMAL(38,0)), 'MINNUM', 0.00, 0.76, '027', 'SUPP027', 'FG', 0.0, 0.0, NULL, 'no',
    TIMESTAMP('2024-03-21T00:00:01.000+0000'), TIMESTAMP('2024-03-21T00:00:01.000+0000')

  UNION ALL

    -- 30. Edge: All numeric fields at max value
    SELECT
    '10028|01|0001', 'L028', 9999999999.99, 9999999999.99, CAST(99991231 AS DECIMAL(38,0)), 'MAXNUM', 9999999999.99, 0.76, '028', 'SUPP028', 'FG', 9999999999.99, 0.0, NULL, 'yes',
    TIMESTAMP('2024-03-21T00:00:02.000+0000'), TIMESTAMP('2024-03-21T00:00:02.000+0000')
)

SELECT * FROM test_data
;
