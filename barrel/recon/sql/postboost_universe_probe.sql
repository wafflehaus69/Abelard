-- Does the complete -> createpool admission used by the heavy queries still find the graduates of a
-- post-BOOST day (2026-09-01), against the decoded migration event (the ratified source after May 2026)?
WITH comp AS (
  SELECT mint, evt_block_time AS ct FROM pumpdotfun_solana.pump_evt_completeevent
  WHERE evt_block_date = DATE '2026-09-01'),
pc AS (
  SELECT base_mint AS mint, quote_mint, pool, evt_block_time AS pt,
         row_number() OVER (PARTITION BY base_mint ORDER BY evt_block_time) AS rn
  FROM pumpdotfun_solana.pump_amm_evt_createpoolevent
  WHERE evt_block_date BETWEEN DATE '2026-09-01' AND DATE '2026-09-02'),
u AS (
  SELECT c.mint, p.quote_mint FROM comp c JOIN pc p ON p.mint = c.mint AND p.rn = 1 AND p.pt >= c.ct AND p.pt < c.ct + INTERVAL '1' DAY),
mig AS (
  SELECT mint, quote_mint FROM pumpdotfun_solana.pump_evt_completepumpammmigrationevent
  WHERE evt_block_date = DATE '2026-09-01')
SELECT 'complete_to_pool' AS src, quote_mint, count(*) AS n, count_if(mint IN (SELECT mint FROM mig)) AS also_in_other FROM u GROUP BY 1, 2
UNION ALL
SELECT 'migration_event', quote_mint, count(*), count_if(mint IN (SELECT mint FROM u)) FROM mig GROUP BY 1, 2
UNION ALL
SELECT 'createevent_fill', CAST(NULL AS varchar), count(*), count(creator) FROM pumpdotfun_solana.pump_evt_createevent
WHERE evt_block_date = DATE '2026-09-01'
