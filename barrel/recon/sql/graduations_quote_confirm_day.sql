-- Confirm the migration event's placeholder quote_mint (System Program ID) means
-- SOL-quoted, by joining to the pool-creation event, whose quote_mint is authoritative.
-- Also return three sample rows for an on-chain check (bonding curve complete, pool exists).
WITH g AS (
  SELECT mint, pool, bonding_curve, quote_mint AS evt_quote, evt_block_slot
  FROM pumpdotfun_solana.pump_evt_completepumpammmigrationevent
  WHERE evt_block_date = DATE '2026-09-01'),
p AS (SELECT pool, quote_mint AS pool_quote FROM pumpdotfun_solana.pump_amm_evt_createpoolevent)
SELECT g.evt_quote, p.pool_quote, COUNT(*) AS n,
       slice(ARRAY_AGG(g.mint ORDER BY g.evt_block_slot), 1, 3)          AS sample_mints,
       slice(ARRAY_AGG(g.bonding_curve ORDER BY g.evt_block_slot), 1, 3) AS sample_curves,
       slice(ARRAY_AGG(g.pool ORDER BY g.evt_block_slot), 1, 3)          AS sample_pools
FROM g LEFT JOIN p ON g.pool = p.pool
GROUP BY 1, 2 ORDER BY n DESC
