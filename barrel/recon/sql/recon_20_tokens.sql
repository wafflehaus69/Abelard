-- 1.4: twenty stratum-P tokens for hand reconciliation against Solscan, drawn deterministically
-- with seed 20260922 (xxhash64 of mint || seed), stratified: 10 pre-event era (rule), 10 post
-- (event table). No metadata columns selected (§1).
WITH pre AS (
  SELECT c.mint, p.pool, c.evt_block_time AS grad_time, 'pre_event_rule' AS src
  FROM pumpdotfun_solana.pump_evt_completeevent c
  JOIN (SELECT base_mint, MIN(evt_block_time) AS pt, arbitrary(pool) AS pool, arbitrary(quote_mint) AS q FROM pumpdotfun_solana.pump_amm_evt_createpoolevent
        WHERE evt_block_date BETWEEN DATE '2025-03-20' AND DATE '2026-05-01' GROUP BY 1) p
    ON p.base_mint = c.mint AND p.pt >= c.evt_block_time AND p.pt < c.evt_block_time + INTERVAL '1' DAY AND p.q = 'So11111111111111111111111111111111111111112'
  WHERE c.evt_block_date BETWEEN DATE '2025-03-20' AND DATE '2026-04-30'),
post AS (
  SELECT mint, pool, evt_block_time AS grad_time, 'migration_event' AS src
  FROM pumpdotfun_solana.pump_evt_completepumpammmigrationevent
  WHERE evt_block_date BETWEEN DATE '2026-06-01' AND DATE '2026-09-27' AND quote_mint = '11111111111111111111111111111111'),
ranked AS (
  SELECT *, row_number() OVER (PARTITION BY src ORDER BY xxhash64(to_utf8(mint || '20260922'))) AS rn FROM (SELECT * FROM pre UNION ALL SELECT * FROM post))
SELECT src, rn, mint, pool, grad_time FROM ranked WHERE rn <= 10 ORDER BY src, rn
