-- A3 / RUG-A input: aligned-set net flow days 0-7, stratum P, graduations 2025-06-09 .. 2025-06-09.
-- v2: each large table referenced once (see generator docstring).
WITH comp AS (
  SELECT mint, evt_block_time AS ct FROM pumpdotfun_solana.pump_evt_completeevent
  WHERE evt_block_date BETWEEN DATE '2025-06-09' AND DATE '2025-06-09'),
pc AS (
  SELECT base_mint AS mint, quote_mint, evt_block_time AS pt,
         row_number() OVER (PARTITION BY base_mint ORDER BY evt_block_time) AS rn
  FROM pumpdotfun_solana.pump_amm_evt_createpoolevent
  WHERE evt_block_date BETWEEN DATE '2025-06-09' AND DATE '2025-06-10'),
u AS (
  SELECT c.mint, p.pt AS grad_time
  FROM comp c JOIN pc p ON p.mint = c.mint AND p.rn = 1 AND p.pt >= c.ct AND p.pt < c.ct + INTERVAL '1' DAY
  WHERE p.quote_mint = 'So11111111111111111111111111111111111111112'),
cr AS (
  SELECT mint, arbitrary(COALESCE(creator, "user")) AS creator, min(evt_block_time) AS t0, min(evt_block_slot) AS s0
  FROM pumpdotfun_solana.pump_evt_createevent
  WHERE evt_block_date BETWEEN DATE '2025-06-06' AND DATE '2025-06-09' AND mint IN (SELECT mint FROM u)
  GROUP BY 1),
base AS (SELECT u.mint, u.grad_time, cr.creator, cr.t0, cr.s0 FROM u JOIN cr ON cr.mint = u.mint),
buyers0 AS (
  SELECT DISTINCT b.mint, t.user AS w, b.t0
  FROM pumpdotfun_solana.pump_evt_tradeevent t JOIN base b ON t.mint = b.mint AND t.evt_block_slot = b.s0
  -- no is_buy filter: that column is NULL on every 2025 trade event (tradeevent_layout_probe.sql).
  -- Nothing is lost: a wallet that sells in the creation slot had to acquire in that slot first.
  WHERE t.evt_block_date BETWEEN DATE '2025-06-06' AND DATE '2025-06-09'),
keys AS (   -- role F: the creator as sender; role B: a creation-slot buyer as receiver
  SELECT mint, creator AS k, 'F' AS role, t0 FROM base
  UNION ALL
  SELECT mint, w, 'B', t0 FROM buyers0),
hit AS (    -- the only reference to sol_transfers; literal block_time bounds (S8 watchdog rule)
  SELECT k.mint, k.role,
         CASE k.role WHEN 'F' THEN s.to_owner ELSE k.k END AS w,
         CASE k.role WHEN 'B' THEN s.from_owner END AS funder
  FROM tokens_solana.sol_transfers s
  CROSS JOIN UNNEST(ARRAY[s.from_owner, s.to_owner], ARRAY['F', 'B']) AS x(addr, role)
  JOIN keys k ON k.k = x.addr AND k.role = x.role
  WHERE s.block_time >= TIMESTAMP '2025-06-05 00:00:00' AND s.block_time < TIMESTAMP '2025-06-11 00:00:00'
    AND CAST(s.amount AS double) >= 1e6
    AND ((k.role = 'F' AND s.block_time BETWEEN k.t0 - INTERVAL '24' HOUR AND k.t0 + INTERVAL '24' HOUR)
      OR (k.role = 'B' AND s.block_time BETWEEN k.t0 - INTERVAL '24' HOUR AND k.t0))
  GROUP BY 1, 2, 3, 4),
memb AS (
  SELECT mint, role, w, count(*) OVER (PARTITION BY mint, role, funder) AS n_same_funder FROM hit),
seta AS (
  SELECT mint, w, bool_or(src = 'F') AS is_funded, bool_or(src = 'B') AS is_bundle
  FROM (SELECT mint, creator AS w, 'C' AS src FROM base
        UNION ALL
        SELECT mint, w, role FROM memb WHERE role = 'F' OR n_same_funder >= 5)
  WHERE w IS NOT NULL AND __NOT_OWNER(w)__
  GROUP BY 1, 2),
flows AS (  -- the only reference to the token ledger
  SELECT t.token_mint_address AS mint, x.w, x.d, t.block_time AS bt
  FROM tokens_solana.transfers t
  CROSS JOIN UNNEST(ARRAY[t.to_owner, t.from_owner],
                    ARRAY[CAST(t.amount AS double), -CAST(t.amount AS double)]) AS x(w, d)
  WHERE t.block_date BETWEEN DATE '2025-06-06' AND DATE '2025-06-18'
    AND t.token_mint_address IN (SELECT mint FROM base) AND x.w IS NOT NULL),
j AS (
  SELECT s.mint, s.w, s.is_funded, s.is_bundle, f.d, f.bt, b.grad_time
  FROM seta s JOIN base b ON b.mint = s.mint
  LEFT JOIN flows f ON f.mint = s.mint AND f.w = s.w),
agg AS (
  SELECT mint,
    count(DISTINCT w) AS seta_n,
    count(DISTINCT w) FILTER (WHERE is_funded) AS n_funded,
    count(DISTINCT w) FILTER (WHERE is_bundle) AS n_bundle,
    count(DISTINCT w) FILTER (WHERE d IS NOT NULL) AS n_with_flow,
    sum(d) FILTER (WHERE bt <= grad_time + INTERVAL '240' MINUTE) AS hold_entry,
    sum(d) FILTER (WHERE bt > grad_time + INTERVAL '240' MINUTE AND bt < grad_time + INTERVAL '8' DAY) AS net_after_entry,
    sum(d) FILTER (WHERE bt >= grad_time + INTERVAL '0' DAY AND bt < grad_time + INTERVAL '1' DAY) AS f0,
    sum(d) FILTER (WHERE bt >= grad_time + INTERVAL '1' DAY AND bt < grad_time + INTERVAL '2' DAY) AS f1,
    sum(d) FILTER (WHERE bt >= grad_time + INTERVAL '2' DAY AND bt < grad_time + INTERVAL '3' DAY) AS f2,
    sum(d) FILTER (WHERE bt >= grad_time + INTERVAL '3' DAY AND bt < grad_time + INTERVAL '4' DAY) AS f3,
    sum(d) FILTER (WHERE bt >= grad_time + INTERVAL '4' DAY AND bt < grad_time + INTERVAL '5' DAY) AS f4,
    sum(d) FILTER (WHERE bt >= grad_time + INTERVAL '5' DAY AND bt < grad_time + INTERVAL '6' DAY) AS f5,
    sum(d) FILTER (WHERE bt >= grad_time + INTERVAL '6' DAY AND bt < grad_time + INTERVAL '7' DAY) AS f6,
    sum(d) FILTER (WHERE bt >= grad_time + INTERVAL '7' DAY AND bt < grad_time + INTERVAL '8' DAY) AS f7
  FROM j GROUP BY 1)
SELECT u.mint,
       CASE WHEN b.mint IS NULL THEN 'created_outside_window' END AS reason,
       a.seta_n, a.n_funded, a.n_bundle, a.n_with_flow,
       a.hold_entry, a.net_after_entry,
       CASE WHEN a.hold_entry > 0 THEN -coalesce(a.net_after_entry, 0) / a.hold_entry END AS sf_7d,
       a.f0, a.f1, a.f2, a.f3, a.f4, a.f5, a.f6, a.f7
FROM u LEFT JOIN base b ON b.mint = u.mint LEFT JOIN agg a ON a.mint = u.mint