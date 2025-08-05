-- Test Data Generation for purgo_playground.f_inv_movmnt
-- Covers: happy path, edge, error, null/default, special char, uniqueness, data type, and join scenarios

WITH test_data AS (
  SELECT
    -- 1. Happy path: all fields populated, FERT->FG, LZBEP=B, lifdr present
    '1001|01|200' AS txn_id,
    'L001' AS inv_loc,
    18.5 AS financial_qty,
    18.5 AS net_qty,
    '20240610' AS expired_dt,
    'ABC123' AS item_nbr,
    50.00 AS unit_cost,
    0.76 AS uom_rate,
    'PL01' AS plant_loc_cd,
    'SUPP001' AS inv_stock_reference,
    'FG' AS stock_type,
    18.5 AS qty_on_hand,
    0.0 AS qty_shipped,
    NULL AS cancel_dt,
    'yes' AS flag_active,
    current_timestamp() AS crt_dt,
    current_timestamp() AS updt_dt
  UNION ALL
    -- 2. Happy path: ROH->RAW, LZBEP=L, dunnr present
    SELECT
    '1002|02|201', 'L002', 12.4, 12.4, '20240611', 'XYZ789', 100.00, 0.76, 'PL02', 'DUN001', 'RAW', 12.4, 0.0, NULL, 'yes', current_timestamp(), current_timestamp()
  UNION ALL
    -- 3. Happy path: HALB->WIP, LZBEP=L, sdauf present, vbax.dunnr present
    SELECT
    '1003|03|202', 'L003', 15.0, 15.0, '20240612', 'DEF456', 75.50, 0.76, 'PL03', 'VBAXSUPP', 'WIP', 15.0, 0.0, NULL, 'yes', current_timestamp(), current_timestamp()
  UNION ALL
    -- 4. Happy path: ptart not mapped, should be OT
    SELECT
    '1004|04|203', 'L004', 10.0, 10.0, '20240613', 'GHI789', 60.00, 0.76, 'PL04', 'none', 'OT', 10.0, 0.0, NULL, 'yes', current_timestamp(), current_timestamp()
  UNION ALL
    -- 5. Edge: LZBEP=B, lifdr null, should be 'none'
    SELECT
    '1005|05|204', 'L005', 8.0, 8.0, '20240614', 'JKL012', 55.00, 0.76, 'PL05', 'none', 'FG', 8.0, 0.0, NULL, 'yes', current_timestamp(), current_timestamp()
  UNION ALL
    -- 6. Edge: LZBEP=L, dunnr null, sdauf null, should be 'none'
    SELECT
    '1006|06|205', 'L006', 7.0, 7.0, '20240615', 'MNO345', 45.00, 0.76, 'PL06', 'none', 'RAW', 7.0, 0.0, NULL, 'yes', current_timestamp(), current_timestamp()
  UNION ALL
    -- 7. Edge: LZBEP=X, should be 'none'
    SELECT
    '1007|07|206', 'L007', 6.0, 6.0, '20240616', 'PQR678', 40.00, 0.76, 'PL07', 'none', 'WIP', 6.0, 0.0, NULL, 'yes', current_timestamp(), current_timestamp()
  UNION ALL
    -- 8. Edge: dwart in RX, flag_active=no
    SELECT
    '1008|08|207', 'L008', 5.0, 5.0, '20240617', 'STU901', 35.00, 0.76, 'PL08', 'SUPP002', 'FG', 5.0, 0.0, NULL, 'no', current_timestamp(), current_timestamp()
  UNION ALL
    -- 9. Edge: dwart in TX, flag_active=no
    SELECT
    '1009|09|208', 'L009', 4.0, 4.0, '20240618', 'VWX234', 30.00, 0.76, 'PL09', 'SUPP003', 'RAW', 4.0, 0.0, NULL, 'no', current_timestamp(), current_timestamp()
  UNION ALL
    -- 10. Edge: dwart in PX, flag_active=no
    SELECT
    '1010|10|209', 'L010', 3.0, 3.0, '20240619', 'YZA567', 25.00, 0.76, 'PL10', 'SUPP004', 'WIP', 3.0, 0.0, NULL, 'no', current_timestamp(), current_timestamp()
  UNION ALL
    -- 11. Edge: dwart null, flag_active=no
    SELECT
    '1011|11|210', 'L011', 2.0, 2.0, '20240620', 'BCD890', 20.00, 0.76, 'PL11', 'SUPP005', 'OT', 2.0, 0.0, NULL, 'no', current_timestamp(), current_timestamp()
  UNION ALL
    -- 12. Error: item_nbr null (should be rejected, but included for error test)
    SELECT
    '1012|12|211', 'L012', 1.0, 1.0, '20240621', NULL, 15.00, 0.76, 'PL12', 'SUPP006', 'FG', 1.0, 0.0, NULL, 'yes', current_timestamp(), current_timestamp()
  UNION ALL
    -- 13. Error: financial_qty as string (should be rejected, but included for error test)
    SELECT
    '1013|13|212', 'L013', CAST('not_a_number' AS DOUBLE), 1.0, '20240622', 'EFG123', 10.00, 0.76, 'PL13', 'SUPP007', 'RAW', 1.0, 0.0, NULL, 'yes', current_timestamp(), current_timestamp()
  UNION ALL
    -- 14. Error: net_qty as string (should be rejected, but included for error test)
    SELECT
    '1014|14|213', 'L014', 1.0, CAST('not_a_number' AS DOUBLE), '20240623', 'HIJ456', 5.00, 0.76, 'PL14', 'SUPP008', 'WIP', 1.0, 0.0, NULL, 'yes', current_timestamp(), current_timestamp()
  UNION ALL
    -- 15. Null handling: inv_loc null, should be 'none'
    SELECT
    '1015|15|214', NULL, 2.0, 2.0, '20240624', 'KLM789', 12.00, 0.76, 'PL15', 'SUPP009', 'FG', 2.0, 0.0, NULL, 'yes', current_timestamp(), current_timestamp()
  UNION ALL
    -- 16. Null handling: plant_loc_cd null, should be 'none'
    SELECT
    '1016|16|215', 'L016', 3.0, 3.0, '20240625', 'NOP012', 13.00, 0.76, NULL, 'SUPP010', 'RAW', 3.0, 0.0, NULL, 'yes', current_timestamp(), current_timestamp()
  UNION ALL
    -- 17. Null handling: financial_qty null, should be 0
    SELECT
    '1017|17|216', 'L017', NULL, 4.0, '20240626', 'QRS345', 14.00, 0.76, 'PL17', 'SUPP011', 'WIP', 4.0, 0.0, NULL, 'yes', current_timestamp(), current_timestamp()
  UNION ALL
    -- 18. Null handling: net_qty null, should be 0
    SELECT
    '1018|18|217', 'L018', 5.0, NULL, '20240627', 'TUV678', 15.00, 0.76, 'PL18', 'SUPP012', 'OT', 0.0, 0.0, NULL, 'yes', current_timestamp(), current_timestamp()
  UNION ALL
    -- 19. Null handling: expired_dt null, should be '99991231'
    SELECT
    '1019|19|218', 'L019', 6.0, 6.0, NULL, 'WXY901', 16.00, 0.76, 'PL19', 'SUPP013', 'FG', 6.0, 0.0, NULL, 'yes', current_timestamp(), current_timestamp()
  UNION ALL
    -- 20. Null handling: unit_cost null
    SELECT
    '1020|20|219', 'L020', 7.0, 7.0, '20240629', 'ZAB234', NULL, 0.76, 'PL20', 'SUPP014', 'RAW', 7.0, 0.0, NULL, 'yes', current_timestamp(), current_timestamp()
  UNION ALL
    -- 21. Null handling: cancel_dt present (simulate cancel)
    SELECT
    '1021|21|220', 'L021', 8.0, 8.0, '20240630', 'CDE567', 18.00, 0.76, 'PL21', 'SUPP015', 'WIP', 8.0, 0.0, 20240630, 'yes', current_timestamp(), current_timestamp()
  UNION ALL
    -- 22. Special char: item_nbr with special chars
    SELECT
    '1022|22|221', 'L022', 9.0, 9.0, '20240701', 'A!@#$', 19.00, 0.76, 'PL22', 'SUPP016', 'FG', 9.0, 0.0, NULL, 'yes', current_timestamp(), current_timestamp()
  UNION ALL
    -- 23. Special char: inv_loc with multi-byte chars
    SELECT
    '1023|23|222', '多字节', 10.0, 10.0, '20240702', 'FGH890', 20.00, 0.76, 'PL23', 'SUPP017', 'RAW', 10.0, 0.0, NULL, 'yes', current_timestamp(), current_timestamp()
  UNION ALL
    -- 24. Special char: plant_loc_cd with emoji
    SELECT
    '1024|24|223', 'L024', 11.0, 11.0, '20240703', 'IJK123', 21.00, 0.76, 'PL24😀', 'SUPP018', 'WIP', 11.0, 0.0, NULL, 'yes', current_timestamp(), current_timestamp()
  UNION ALL
    -- 25. Special char: inv_stock_reference with special chars
    SELECT
    '1025|25|224', 'L025', 12.0, 12.0, '20240704', 'LMN456', 22.00, 0.76, 'PL25', 'SUPP019@#$', 'OT', 12.0, 0.0, NULL, 'yes', current_timestamp(), current_timestamp()
  UNION ALL
    -- 26. Edge: qty_shipped = financial_qty - net_qty, positive
    SELECT
    '1026|26|225', 'L026', 20.0, 15.0, '20240705', 'OPQ789', 23.00, 0.76, 'PL26', 'SUPP020', 'FG', 15.0, 5.0, NULL, 'yes', current_timestamp(), current_timestamp()
  UNION ALL
    -- 27. Edge: qty_shipped negative, should be 0
    SELECT
    '1027|27|226', 'L027', 10.0, 15.0, '20240706', 'RST012', 24.00, 0.76, 'PL27', 'SUPP021', 'RAW', 15.0, 0.0, NULL, 'yes', current_timestamp(), current_timestamp()
  UNION ALL
    -- 28. Uniqueness: duplicate txn_id, only one should be inserted (simulate by including two, but only one will be loaded in real test)
    SELECT
    '1028|28|227', 'L028', 13.0, 13.0, '20240707', 'UVW345', 25.00, 0.76, 'PL28', 'SUPP022', 'WIP', 13.0, 0.0, NULL, 'yes', current_timestamp(), current_timestamp()
  UNION ALL
    -- 29. Uniqueness: duplicate txn_id (should be rejected in real test)
    SELECT
    '1028|28|227', 'L028', 14.0, 14.0, '20240708', 'XYZ678', 26.00, 0.76, 'PL28', 'SUPP023', 'OT', 14.0, 0.0, NULL, 'yes', current_timestamp(), current_timestamp()
  UNION ALL
    -- 30. All null/defaults (except txn_id, crt_dt, updt_dt)
    SELECT
    '1029|29|228', NULL, NULL, NULL, NULL, NULL, NULL, 0.76, NULL, NULL, NULL, NULL, NULL, NULL, NULL, current_timestamp(), current_timestamp()
)
SELECT
  txn_id,
  COALESCE(inv_loc, 'none') AS inv_loc,
  COALESCE(financial_qty, 0.0) AS financial_qty,
  COALESCE(net_qty, 0.0) AS net_qty,
  COALESCE(expired_dt, '99991231') AS expired_dt,
  item_nbr,
  unit_cost,
  uom_rate,
  COALESCE(plant_loc_cd, 'none') AS plant_loc_cd,
  COALESCE(inv_stock_reference, 'none') AS inv_stock_reference,
  COALESCE(stock_type, 'OT') AS stock_type,
  COALESCE(qty_on_hand, COALESCE(net_qty, 0.0)) AS qty_on_hand,
  CASE
    WHEN COALESCE(financial_qty, 0.0) - COALESCE(net_qty, 0.0) < 0 THEN 0.0
    ELSE COALESCE(financial_qty, 0.0) - COALESCE(net_qty, 0.0)
  END AS qty_shipped,
  cancel_dt,
  COALESCE(flag_active, 'no') AS flag_active,
  crt_dt,
  updt_dt
FROM test_data
;
