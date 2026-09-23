-- Census metric per MR-6 ruling, ONE WEEK sample (2026-08-31..09-06), stratum P. Two
-- distributions, no threshold:
--   (a) distinct ORGANIC takers in days 0-7 after graduation
--   (b) seconds from graduation to first ORGANIC swap
-- Organic excludes: the creator, the migration authority (event 'user'), and buyers in the
-- graduation slot (S8 proxy until the bundle detector exists). S7b fee-share recipients are
-- NOT yet excluded (SharingConfig history not built) -- labelled on output.
WITH g AS (
  SELECT m.mint, m.pool, m.evt_block_time AS g_time, m.evt_block_slot AS g_slot, m.user AS migrator, c.creator
  FROM pumpdotfun_solana.pump_evt_completepumpammmigrationevent m
  LEFT JOIN pumpdotfun_solana.pump_evt_createevent c ON c.mint = m.mint AND c.evt_block_date BETWEEN DATE '2026-06-01' AND DATE '2026-09-06'
  WHERE m.evt_block_date BETWEEN DATE '2026-08-31' AND DATE '2026-09-06' AND m.quote_mint = '11111111111111111111111111111111'),
sw AS (
  SELECT pool, user AS wallet, evt_block_time AS t, evt_block_slot AS slot FROM pumpdotfun_solana.pump_amm_evt_buyevent  WHERE evt_block_date BETWEEN DATE '2026-08-31' AND DATE '2026-09-13'
  UNION ALL
  SELECT pool, user, evt_block_time, evt_block_slot FROM pumpdotfun_solana.pump_amm_evt_sellevent WHERE evt_block_date BETWEEN DATE '2026-08-31' AND DATE '2026-09-13'),
bundle AS (SELECT DISTINCT s.pool, s.wallet FROM sw s JOIN g ON g.pool = s.pool WHERE s.slot = g.g_slot),
organic AS (
  SELECT g.mint, s.wallet, s.t, g.g_time
  FROM sw s JOIN g ON g.pool = s.pool
  WHERE s.t >= g.g_time AND s.t < g.g_time + INTERVAL '7' DAY
    AND s.wallet <> g.migrator AND (g.creator IS NULL OR s.wallet <> g.creator)
    AND NOT EXISTS (SELECT 1 FROM bundle b WHERE b.pool = g.pool AND b.wallet = s.wallet)),
per_token AS (
  SELECT g.mint, COUNT(DISTINCT o.wallet) AS organic_takers_7d,
         date_diff('second', g.g_time, MIN(o.t)) AS secs_to_first_organic
  FROM g LEFT JOIN organic o ON o.mint = g.mint GROUP BY g.mint, g.g_time)
SELECT COUNT(*) AS graduates,
       COUNT_IF(organic_takers_7d = 0) AS zero_organic,
       approx_percentile(organic_takers_7d, 0.10) AS takers_p10, approx_percentile(organic_takers_7d, 0.5) AS takers_p50,
       approx_percentile(organic_takers_7d, 0.90) AS takers_p90, MAX(organic_takers_7d) AS takers_max,
       approx_percentile(secs_to_first_organic, 0.10) AS t_first_p10, approx_percentile(secs_to_first_organic, 0.5) AS t_first_p50,
       approx_percentile(secs_to_first_organic, 0.90) AS t_first_p90,
       COUNT_IF(secs_to_first_organic <= 60) AS first_within_60s, COUNT_IF(secs_to_first_organic <= 900) AS first_within_15min,
       'S7b fee-share recipients NOT excluded (pending)' AS caveat
FROM per_token
