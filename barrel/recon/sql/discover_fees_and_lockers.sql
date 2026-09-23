-- S7b: decoded pump fee program (pfeeUxB6...) tables -- SharingConfig / fee-share events.
-- S5:  locker / vesting programs decoded on Dune, for "LP in a verifiable locker".
SELECT table_schema, table_name FROM information_schema.tables
WHERE table_schema LIKE '%pump_fee%' OR table_schema LIKE '%pumpfee%' OR table_schema LIKE '%pump_fees%'
   OR table_name LIKE '%sharingconfig%' OR table_name LIKE '%feeshar%'
   OR table_schema LIKE '%lock%' OR table_schema LIKE '%streamflow%' OR table_schema LIKE '%vesting%' OR table_name LIKE '%lock%'
ORDER BY 1, 2
