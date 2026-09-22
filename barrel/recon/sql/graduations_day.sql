-- 1.2 Graduation events (pump.fun -> PumpSwap), one day, with the fields the
-- universe needs. Source: Dune decoded pump_evt_completepumpammmigrationevent.
-- Validation partner: recon/sql/graduations_day_raw.sql (instruction_calls) and
-- a BigQuery top-level count by the migrate discriminator.
SELECT evt_block_date                      AS day,
       COUNT(*)                            AS migrations,
       COUNT(DISTINCT mint)                AS distinct_mints,
       COUNT(DISTINCT pool)                AS distinct_pools,
       COUNT_IF(quote_mint = 'So11111111111111111111111111111111111111112') AS sol_quoted,
       COUNT_IF(quote_mint <> 'So11111111111111111111111111111111111111112') AS other_quoted,
       COUNT_IF(evt_is_inner)              AS emitted_inner
FROM pumpdotfun_solana.pump_evt_completepumpammmigrationevent
WHERE evt_block_date = DATE '2026-09-01'
GROUP BY 1
