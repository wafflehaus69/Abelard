-- Census (MR-7): organic v1 vs v2 side by side, sample week 2026-08-31..09-06, stratum P.
-- v1: not creator, not migrator, not a same-slot-as-graduation buyer.
-- v2: v1 AND wallet age >= 24h at swap time AND not bot-shaped.
--   wallet age proxy: first PumpSwap buy/sell event in a 60-day lookback (2026-07-02..). A wallet
--   first seen before 2026-07-03 is treated as aged (older than the lookback). LABELLED PROXY for
--   "first on-chain signature".
--   bot-shaped (simplified to the sample window, not per-swap trailing): > 200 distinct pools
--   traded in any 7-day window of 08-24..09-13, OR median (first sell - first buy) per pool < 60 s.
--   S7b fee-share recipients not yet excluded (pending) -- applies to both v1 and v2.
WITH g AS (
  SELECT m.mint, m.pool, m.evt_block_time AS g_time, m.evt_block_slot AS g_slot, m.user AS migrator, c.creator
  FROM pumpdotfun_solana.pump_evt_completepumpammmigrationevent m
  LEFT JOIN pumpdotfun_solana.pump_evt_createevent c ON c.mint = m.mint AND c.evt_block_date BETWEEN DATE '2026-06-01' AND DATE '2026-09-06'
  WHERE m.evt_block_date BETWEEN DATE '2026-08-31' AND DATE '2026-09-06' AND m.quote_mint = '11111111111111111111111111111111'),
ev AS (
  SELECT pool, user AS wallet, evt_block_time AS t, evt_block_slot AS slot, 'buy' AS side FROM pumpdotfun_solana.pump_amm_evt_buyevent  WHERE evt_block_date BETWEEN DATE '2026-07-02' AND DATE '2026-09-13'
  UNION ALL
  SELECT pool, user, evt_block_time, evt_block_slot, 'sell' FROM pumpdotfun_solana.pump_amm_evt_sellevent WHERE evt_block_date BETWEEN DATE '2026-07-02' AND DATE '2026-09-13'),
first_seen AS (SELECT wallet, MIN(t) AS first_t FROM ev GROUP BY 1),
win AS (SELECT * FROM ev WHERE t >= TIMESTAMP '2026-08-24 00:00:00'),
tok7 AS (  -- max distinct pools in the 7-day window ending each day (coarse: by day buckets)
  SELECT wallet, MAX(n) AS max_pools_7d FROM (
    SELECT wallet, d, COUNT(DISTINCT pool) AS n FROM (
      SELECT wallet, pool, CAST(t AS date) AS d FROM win) a
    JOIN (SELECT sequence(DATE '2026-08-30', DATE '2026-09-13', INTERVAL '1' DAY) AS ds) b ON true
    CROSS JOIN UNNEST(b.ds) AS u(day) WHERE a.d BETWEEN u.day - INTERVAL '6' DAY AND u.day GROUP BY wallet, d) x GROUP BY 1),
hold AS (
  SELECT wallet, approx_percentile(date_diff('second', fb, fs), 0.5) AS med_hold_s FROM (
    SELECT wallet, pool, MIN(CASE WHEN side='buy' THEN t END) AS fb, MIN(CASE WHEN side='sell' THEN t END) AS fs FROM win GROUP BY 1,2) h
  WHERE fs IS NOT NULL AND fb IS NOT NULL AND fs >= fb GROUP BY 1),
bundle AS (SELECT DISTINCT s.pool, s.wallet FROM win s JOIN g ON g.pool = s.pool WHERE s.slot = g.g_slot),
sw AS (
  SELECT g.mint, s.wallet, s.t, g.g_time,
         (s.wallet <> g.migrator AND (g.creator IS NULL OR s.wallet <> g.creator) AND NOT EXISTS (SELECT 1 FROM bundle b WHERE b.pool = g.pool AND b.wallet = s.wallet)) AS v1,
         (fs.first_t < TIMESTAMP '2026-07-03 00:00:00' OR s.t >= fs.first_t + INTERVAL '24' HOUR) AS aged,
         (COALESCE(t7.max_pools_7d, 0) > 200 OR COALESCE(h.med_hold_s, 999999) < 60) AS bot
  FROM win s JOIN g ON g.pool = s.pool LEFT JOIN first_seen fs ON fs.wallet = s.wallet
  LEFT JOIN tok7 t7 ON t7.wallet = s.wallet LEFT JOIN hold h ON h.wallet = s.wallet
  WHERE s.t >= g.g_time AND s.t < g.g_time + INTERVAL '7' DAY),
per AS (
  SELECT mint,
         COUNT(DISTINCT CASE WHEN v1 THEN wallet END) AS takers_v1,
         COUNT(DISTINCT CASE WHEN v1 AND aged AND NOT bot THEN wallet END) AS takers_v2,
         date_diff('second', MIN(g_time), MIN(CASE WHEN v1 THEN t END)) AS t_first_v1,
         date_diff('second', MIN(g_time), MIN(CASE WHEN v1 AND aged AND NOT bot THEN t END)) AS t_first_v2
  FROM sw GROUP BY mint)
SELECT COUNT(*) AS graduates,
       approx_percentile(takers_v1, 0.5) AS v1_takers_p50, approx_percentile(takers_v2, 0.1) AS v2_takers_p10, approx_percentile(takers_v2, 0.5) AS v2_takers_p50, approx_percentile(takers_v2, 0.9) AS v2_takers_p90,
       COUNT_IF(takers_v2 = 0) AS v2_zero_organic,
       approx_percentile(t_first_v1, 0.5) AS v1_tfirst_p50, approx_percentile(t_first_v1, 0.9) AS v1_tfirst_p90,
       approx_percentile(t_first_v2, 0.1) AS v2_tfirst_p10, approx_percentile(t_first_v2, 0.5) AS v2_tfirst_p50, approx_percentile(t_first_v2, 0.9) AS v2_tfirst_p90,
       COUNT_IF(t_first_v2 <= 60) AS v2_first_within_60s, COUNT_IF(t_first_v2 <= 900) AS v2_first_within_15min, COUNT_IF(t_first_v2 > 14400) AS v2_first_after_4h
FROM per
