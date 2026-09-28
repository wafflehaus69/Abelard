-- A3 / routing X: per-venue volume share for the day's P graduates over their first 24h, from the
-- unified dex_solana.trades (one day + one, literal bounds). Sized here; scope widens later.
WITH g AS (SELECT mint, evt_block_time AS g_time FROM pumpdotfun_solana.pump_evt_completepumpammmigrationevent
           WHERE evt_block_date = DATE '2026-09-01' AND quote_mint = '11111111111111111111111111111111'),
t AS (
  SELECT d.project, d.token_bought_mint_address, d.token_sold_mint_address, d.block_time,
         CAST(d.token_bought_amount_raw AS double) AS bought_raw, CAST(d.token_sold_amount_raw AS double) AS sold_raw
  FROM dex_solana.trades d WHERE d.block_time >= TIMESTAMP '2026-09-01 00:00:00' AND d.block_time < TIMESTAMP '2026-09-03 00:00:00'),
j AS (
  SELECT g.mint, t.project,
         CASE WHEN t.token_bought_mint_address = 'So11111111111111111111111111111111111111112' THEN t.bought_raw
              WHEN t.token_sold_mint_address   = 'So11111111111111111111111111111111111111112' THEN t.sold_raw ELSE 0 END AS sol_vol
  FROM t JOIN g ON (t.token_bought_mint_address = g.mint OR t.token_sold_mint_address = g.mint)
  WHERE t.block_time >= g.g_time AND t.block_time < g.g_time + INTERVAL '24' HOUR),
pm AS (SELECT mint, project, SUM(sol_vol) AS v FROM j GROUP BY 1, 2),
tot AS (SELECT mint, SUM(v) AS tv, COUNT(*) AS venues FROM pm GROUP BY 1),
top AS (SELECT pm.mint, MAX(pm.v / NULLIF(tot.tv,0)) AS top_share, tot.venues FROM pm JOIN tot ON tot.mint = pm.mint GROUP BY 1, 3)
SELECT COUNT(*) AS tokens, approx_percentile(venues, 0.5) AS venues_p50, approx_percentile(venues, 0.9) AS venues_p90,
       approx_percentile(top_share, 0.1) AS topshare_p10, approx_percentile(top_share, 0.5) AS topshare_p50,
       COUNT_IF(top_share < 0.9) AS tokens_with_secondary_venue_gt10pct, COUNT_IF(top_share < 0.6) AS tokens_with_secondary_gt40pct
FROM top
