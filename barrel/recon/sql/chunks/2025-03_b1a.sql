-- Heavy tier B1a (build form): aligned-set members grouped by funder, graduations 2025-03-20 .. 2025-03-31.
-- Rows name wallets: run with --private-rows. No threshold, no classification, no verdict here.
WITH comp AS (
  SELECT mint, evt_block_time AS ct FROM pumpdotfun_solana.pump_evt_completeevent
  WHERE evt_block_date BETWEEN DATE '2025-03-20' AND DATE '2025-03-31'),
pc AS (
  SELECT base_mint AS mint, quote_mint, pool, evt_block_time AS pt,
         row_number() OVER (PARTITION BY base_mint ORDER BY evt_block_time) AS rn
  FROM pumpdotfun_solana.pump_amm_evt_createpoolevent
  WHERE evt_block_date BETWEEN DATE '2025-03-20' AND DATE '2025-04-01'),
u AS (
  SELECT c.mint, p.pt AS grad_time, p.pool
  FROM comp c JOIN pc p ON p.mint = c.mint AND p.rn = 1 AND p.pt >= c.ct AND p.pt < c.ct + INTERVAL '1' DAY
  WHERE p.quote_mint = 'So11111111111111111111111111111111111111112'),
cr AS (
  SELECT mint, arbitrary(COALESCE(creator, "user")) AS creator, min(evt_block_time) AS t0, min(evt_block_slot) AS s0
  FROM pumpdotfun_solana.pump_evt_createevent
  WHERE evt_block_date BETWEEN DATE '2025-03-17' AND DATE '2025-03-31' AND mint IN (SELECT mint FROM u)
  GROUP BY 1),
base AS (SELECT u.mint, u.grad_time, u.pool, cr.creator, cr.t0, cr.s0 FROM u JOIN cr ON cr.mint = u.mint),
bal AS (   -- token ledger, one pass: every owner's balance at each entry lag, plus chain supply
  SELECT t.token_mint_address AS mint, x.w,
    sum(x.d) FILTER (WHERE t.block_time <= b.grad_time + INTERVAL '15' MINUTE) AS b15,
    sum(x.d) FILTER (WHERE t.block_time <= b.grad_time + INTERVAL '60' MINUTE) AS b60,
    sum(x.d) FILTER (WHERE t.block_time <= b.grad_time + INTERVAL '240' MINUTE) AS b240,
    sum(x.d) FILTER (WHERE t.block_time > b.grad_time + INTERVAL '240' MINUTE AND t.block_time < b.grad_time + INTERVAL '8' DAY) AS net_after,
    sum(x.d) FILTER (WHERE t.block_time >= b.grad_time + INTERVAL '0' DAY AND t.block_time < b.grad_time + INTERVAL '1' DAY) AS f0,
    sum(x.d) FILTER (WHERE t.block_time >= b.grad_time + INTERVAL '1' DAY AND t.block_time < b.grad_time + INTERVAL '2' DAY) AS f1,
    sum(x.d) FILTER (WHERE t.block_time >= b.grad_time + INTERVAL '2' DAY AND t.block_time < b.grad_time + INTERVAL '3' DAY) AS f2,
    sum(x.d) FILTER (WHERE t.block_time >= b.grad_time + INTERVAL '3' DAY AND t.block_time < b.grad_time + INTERVAL '4' DAY) AS f3,
    sum(x.d) FILTER (WHERE t.block_time >= b.grad_time + INTERVAL '4' DAY AND t.block_time < b.grad_time + INTERVAL '5' DAY) AS f4,
    sum(x.d) FILTER (WHERE t.block_time >= b.grad_time + INTERVAL '5' DAY AND t.block_time < b.grad_time + INTERVAL '6' DAY) AS f5,
    sum(x.d) FILTER (WHERE t.block_time >= b.grad_time + INTERVAL '6' DAY AND t.block_time < b.grad_time + INTERVAL '7' DAY) AS f6,
    sum(x.d) FILTER (WHERE t.block_time >= b.grad_time + INTERVAL '7' DAY AND t.block_time < b.grad_time + INTERVAL '8' DAY) AS f7,
    min(t.block_time) FILTER (WHERE x.d > 0) AS first_in,
    bool_or(x.tag = 'to' AND t.from_owner IS NULL) AS got_mint
  FROM tokens_solana.transfers t
  JOIN base b ON b.mint = t.token_mint_address
  CROSS JOIN UNNEST(
    ARRAY[t.to_owner, t.from_owner, '#SUPPLY'],
    ARRAY[CAST(t.amount AS double), -CAST(t.amount AS double),
          CASE WHEN t.from_owner IS NULL THEN CAST(t.amount AS double)
               WHEN t.to_owner IS NULL THEN -CAST(t.amount AS double) ELSE 0e0 END],
    ARRAY['to', 'from', 'sup']) AS x(w, d, tag)
  WHERE t.block_date BETWEEN DATE '2025-03-17' AND DATE '2025-04-09'
    AND t.token_mint_address IN (SELECT mint FROM base) AND x.w IS NOT NULL
  GROUP BY 1, 2),
buyers0 AS (
  -- no is_buy filter: NULL on every 2025 trade event; a creation-slot seller acquired in that slot
  SELECT DISTINCT b.mint, t.user AS w, b.t0
  FROM pumpdotfun_solana.pump_evt_tradeevent t JOIN base b ON t.mint = b.mint AND t.evt_block_slot = b.s0
  WHERE t.evt_block_date BETWEEN DATE '2025-03-17' AND DATE '2025-03-31'),
keys AS (
  SELECT mint, creator AS k, 'F' AS role, t0 FROM base
  UNION ALL
  SELECT mint, w, 'B', t0 FROM buyers0),
hit AS (
  SELECT k.mint, k.role,
         CASE k.role WHEN 'F' THEN s.to_owner ELSE k.k END AS w,
         CASE k.role WHEN 'B' THEN s.from_owner END AS funder,
         sum(CAST(s.amount AS double)) / 1e9 AS sol
  FROM tokens_solana.sol_transfers s
  CROSS JOIN UNNEST(ARRAY[s.from_owner, s.to_owner], ARRAY['F', 'B']) AS x(addr, role)
  JOIN keys k ON k.k = x.addr AND k.role = x.role
  WHERE s.block_time >= TIMESTAMP '2025-03-16 00:00:00' AND s.block_time < TIMESTAMP '2025-04-02 00:00:00'
    AND CAST(s.amount AS double) >= 1e6
    AND ((k.role = 'F' AND s.block_time BETWEEN k.t0 - INTERVAL '24' HOUR AND k.t0 + INTERVAL '24' HOUR)
      OR (k.role = 'B' AND s.block_time BETWEEN k.t0 - INTERVAL '24' HOUR AND k.t0))
  GROUP BY 1, 2, 3, 4),
memb AS (
  SELECT mint, role, w, sol, count(*) OVER (PARTITION BY mint, role, funder) AS n_same_funder FROM hit),
seta AS (
  -- bundle_n = how many creation-slot traders share this wallet's funder (itself included).
  -- Whether that makes a bundle is decided locally (MR-14 ruling 5); every candidate is returned.
  SELECT mint, w, bool_or(src = 'C') AS is_creator, bool_or(src = 'F') AS is_funded,
         max(nsf) FILTER (WHERE src = 'B') AS bundle_n,
         sum(sol) FILTER (WHERE src = 'F') AS cr_sol
  FROM (SELECT mint, creator AS w, 'C' AS src, CAST(NULL AS double) AS sol, CAST(NULL AS bigint) AS nsf FROM base
        UNION ALL
        SELECT mint, w, role, sol, n_same_funder FROM memb)
  WHERE w IS NOT NULL AND __NOT_OWNER(w)__
  GROUP BY 1, 2),
mm0 AS (
  SELECT s.mint, s.w, s.is_creator, s.is_funded, s.bundle_n, s.cr_sol,
         bs.grad_time, bs.t0, bs.creator, b.first_in, b.b15, b.b60, b.b240, b.net_after, b.got_mint,
         b.f0, b.f1, b.f2, b.f3, b.f4, b.f5, b.f6, b.f7,
         CASE WHEN s.is_creator THEN bs.t0 ELSE b.first_in END AS t_act,
         count(*) OVER (PARTITION BY s.mint) AS n_all,
         count_if(s.is_funded) OVER (PARTITION BY s.mint) AS n_funded_all,
         max(s.bundle_n) OVER (PARTITION BY s.mint) AS bundle_n_max
  FROM seta s JOIN base bs ON bs.mint = s.mint
  LEFT JOIN bal b ON b.mint = s.mint AND b.w = s.w),
mm AS (SELECT * FROM mm0 WHERE (is_creator OR first_in IS NOT NULL) AND NOT coalesce(got_mint, false)),
inb0 AS (   -- one row per (member, sender): first and last transfer before the member's first action
  SELECT m.mint, m.w, s.from_owner AS sender, min(s.block_time) AS t_first, max(s.block_slot) AS slot_last,
         max_by(CAST(s.amount AS double), s.block_slot) / 1e9 AS last_sol
  FROM tokens_solana.sol_transfers s JOIN mm m ON s.to_owner = m.w
  WHERE s.block_time >= TIMESTAMP '2025-03-16 00:00:00' AND s.block_time < TIMESTAMP '2025-04-02 00:00:00'
    AND CAST(s.amount AS double) >= 1e6
    AND s.from_owner <> m.w AND s.block_time < m.t_act
  GROUP BY 1, 2, 3),
inb AS (    -- funder = the last sender before the first action; funded_ts = that funder's FIRST transfer (MR-12.2).
            -- "Last" is by slot, then by address: block_time is whole seconds and two senders can share one
            -- (found on 2025-06-09: two transfers in the same second, one slot apart, gave two different funders).
  SELECT mint, w, sender AS funder, t_first AS funded_ts, last_sol AS funded_sol, n_senders
  FROM (SELECT *, row_number() OVER (PARTITION BY mint, w ORDER BY slot_last DESC, sender DESC) AS rn,
               count(*) OVER (PARTITION BY mint, w) AS n_senders
        FROM inb0)
  WHERE rn = 1),
fan AS (
  SELECT s.from_owner AS a, approx_distinct(s.to_owner) AS fan_out, count(*) AS n_out
  FROM tokens_solana.sol_transfers s
  WHERE s.block_time >= TIMESTAMP '2025-03-16 00:00:00' AND s.block_time < TIMESTAMP '2025-04-02 00:00:00'
    AND CAST(s.amount AS double) >= 1e6
  GROUP BY 1)
SELECT m.mint, i.funder,
       -- b_only_n: NULL for the creator and wallets the creator funded (always in the set); for a
       -- wallet that is a candidate only as a creation-slot trader, its same-funder count
       CASE WHEN m.is_creator OR m.is_funded THEN NULL ELSE m.bundle_n END AS b_only_n,
       arbitrary(m.creator) AS creator,
       count(*) AS n_members, count_if(m.is_creator) AS n_creator, count_if(m.is_funded) AS n_funded,
       max(m.n_all) AS n_all, max(m.n_funded_all) AS n_funded_all, max(m.bundle_n_max) AS bundle_n_max,
       max(f.fan_out) AS fan_out, max(i.n_senders) AS n_senders_max,
       sum(m.b15) AS b15, sum(m.b60) AS b60, sum(m.b240) AS b240, sum(m.net_after) AS net_after,
       sum(m.f0) AS f0, sum(m.f1) AS f1, sum(m.f2) AS f2, sum(m.f3) AS f3, sum(m.f4) AS f4, sum(m.f5) AS f5, sum(m.f6) AS f6, sum(m.f7) AS f7,
       approx_percentile(date_diff('second', i.funded_ts, m.first_in), 0.5) AS lat_p50,
       count(i.funded_ts) FILTER (WHERE m.first_in IS NOT NULL) AS n_lat,
       min(m.first_in) AS first_in_min, arbitrary(m.grad_time) AS grad_time, arbitrary(m.t0) AS t0,
       arbitrary(sup.b15) AS supply15, arbitrary(sup.b60) AS supply60, arbitrary(sup.b240) AS supply240
FROM mm m
LEFT JOIN inb i ON i.mint = m.mint AND i.w = m.w
LEFT JOIN fan f ON f.a = i.funder
LEFT JOIN bal sup ON sup.mint = m.mint AND sup.w = '#SUPPLY'
GROUP BY 1, 2, 3