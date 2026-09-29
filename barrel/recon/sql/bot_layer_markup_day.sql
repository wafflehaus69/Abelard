-- MR-8 ruling 4 dry run: bot-layer markup = price at G / graduation price, per G, one day (2026-09-01,
-- post-BOOST). Price = effective quote reserve / base reserve from the last event's PRE-swap reserves
-- before each horizon; graduation price from the first post-graduation event. Virtual quote reserve
-- (17.585 SOL post-BOOST, from the Pool account) added to both numerator states -- labelled constant.
WITH g AS (SELECT mint, pool, evt_block_time AS g_time FROM pumpdotfun_solana.pump_evt_completepumpammmigrationevent
           WHERE evt_block_date = DATE '2026-09-01' AND quote_mint = '11111111111111111111111111111111'),
ev AS (
  SELECT pool, evt_block_time AS t, CAST(pool_quote_token_reserves AS double) AS q, CAST(pool_base_token_reserves AS double) AS b FROM pumpdotfun_solana.pump_amm_evt_buyevent  WHERE evt_block_date BETWEEN DATE '2026-09-01' AND DATE '2026-09-02'
  UNION ALL
  SELECT pool, evt_block_time, CAST(pool_quote_token_reserves AS double), CAST(pool_base_token_reserves AS double) FROM pumpdotfun_solana.pump_amm_evt_sellevent WHERE evt_block_date BETWEEN DATE '2026-09-01' AND DATE '2026-09-02'),
st AS (SELECT g.mint, e.t, (e.q + 17.585e9) / e.b AS px, g.g_time, row_number() OVER (PARTITION BY g.mint ORDER BY e.t) AS rn
       FROM ev e JOIN g ON g.pool = e.pool WHERE e.t >= g.g_time AND e.t < g.g_time + INTERVAL '5' HOUR),
m AS (SELECT mint, arbitrary(px) FILTER (WHERE rn = 1) AS p0,
             max_by(px, t) FILTER (WHERE t < g_time + INTERVAL '15' MINUTE)  AS p15,
             max_by(px, t) FILTER (WHERE t < g_time + INTERVAL '60' MINUTE)  AS p60,
             max_by(px, t) FILTER (WHERE t < g_time + INTERVAL '240' MINUTE) AS p240
      FROM st GROUP BY 1)
SELECT COUNT(*) AS tokens,
       approx_percentile(p15/p0, 0.1) AS m15_p10, approx_percentile(p15/p0, 0.5) AS m15_p50, approx_percentile(p15/p0, 0.9) AS m15_p90,
       approx_percentile(p60/p0, 0.1) AS m60_p10, approx_percentile(p60/p0, 0.5) AS m60_p50, approx_percentile(p60/p0, 0.9) AS m60_p90,
       approx_percentile(p240/p0, 0.1) AS m240_p10, approx_percentile(p240/p0, 0.5) AS m240_p50, approx_percentile(p240/p0, 0.9) AS m240_p90,
       COUNT_IF(p15/p0 > 1) AS up_at_15, COUNT_IF(p240/p0 > 1) AS up_at_240
FROM m WHERE p0 > 0 AND p15 IS NOT NULL AND p240 IS NOT NULL
