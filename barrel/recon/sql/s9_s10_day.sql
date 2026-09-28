-- S9 (wash flag) and S10 (retro sellability) on stratum-P graduates of 2026-09-01, first 24h /
-- first 30 min post-graduation. Distributions only; no per-token export.
--   S9a: 24h quote volume / distinct taker wallets, vs the 95th percentile across the day's graduates
--   S9b: share of 24h volume from wallets that both bought and sold the token within 24h (round-trips)
--   S10: any successful sell within 30 min of graduation by a wallet that is NOT the creator/migrator
WITH g AS (
  SELECT m.mint, m.pool, m.evt_block_time AS g_time, m.user AS migrator, c.creator
  FROM pumpdotfun_solana.pump_evt_completepumpammmigrationevent m
  LEFT JOIN pumpdotfun_solana.pump_evt_createevent c ON c.mint = m.mint AND c.evt_block_date BETWEEN DATE '2026-06-01' AND DATE '2026-09-01'
  WHERE m.evt_block_date = DATE '2026-09-01' AND m.quote_mint = '11111111111111111111111111111111'),
ev AS (
  SELECT pool, user AS w, evt_block_time AS t, 'buy' AS side, CAST(quote_amount_in AS double) AS q FROM pumpdotfun_solana.pump_amm_evt_buyevent WHERE evt_block_date BETWEEN DATE '2026-09-01' AND DATE '2026-09-02'
  UNION ALL
  SELECT pool, user, evt_block_time, 'sell', CAST(quote_amount_out AS double) FROM pumpdotfun_solana.pump_amm_evt_sellevent WHERE evt_block_date BETWEEN DATE '2026-09-01' AND DATE '2026-09-02'),
w24 AS (SELECT g.mint, e.w, e.side, e.q, e.t, g.g_time, g.creator, g.migrator FROM ev e JOIN g ON g.pool = e.pool WHERE e.t >= g.g_time AND e.t < g.g_time + INTERVAL '24' HOUR),
per_wallet AS (SELECT mint, w, SUM(q) AS vol, bool_or(side='buy') AS b, bool_or(side='sell') AS s FROM w24 GROUP BY 1, 2),
per_token AS (
  SELECT mint, SUM(vol)/1e9 AS vol_sol, COUNT(*) AS takers, SUM(vol)/NULLIF(COUNT(*),0)/1e9 AS vol_per_taker_sol,
         SUM(CASE WHEN b AND s THEN vol END)/NULLIF(SUM(vol),0) AS roundtrip_share
  FROM per_wallet GROUP BY 1),
s10 AS (SELECT g.mint, COUNT_IF(w.side='sell' AND w.t < g.g_time + INTERVAL '30' MINUTE AND w.w <> g.migrator AND (g.creator IS NULL OR w.w <> g.creator)) > 0 AS sellable
        FROM g LEFT JOIN w24 w ON w.mint = g.mint GROUP BY 1),
p95 AS (SELECT approx_percentile(vol_per_taker_sol, 0.95) AS cut FROM per_token)
SELECT COUNT(*) AS graduates,
       (SELECT cut FROM p95) AS s9a_p95_vol_per_taker_sol,
       COUNT_IF(pt.vol_per_taker_sol > (SELECT cut FROM p95)) AS s9a_flagged,
       approx_percentile(pt.roundtrip_share, 0.5) AS s9b_roundtrip_p50, approx_percentile(pt.roundtrip_share, 0.9) AS s9b_roundtrip_p90,
       COUNT_IF(pt.roundtrip_share > 0.30) AS s9b_flagged_gt30pct,
       COUNT_IF(s.sellable) AS s10_sellable_30min, COUNT_IF(NOT s.sellable) AS s10_no_organic_sell_30min
FROM per_token pt JOIN s10 s ON s.mint = pt.mint
