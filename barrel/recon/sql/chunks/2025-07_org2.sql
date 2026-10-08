-- Organic takers v1 and v2 by cell (org2_n_7d, org2_t_s, first-hour marker), graduations 2025-07-01 .. 2025-07-31.
-- One row per (token, cell); no wallet in the output. The bundle cut and the fee-share rule are applied locally.
WITH comp AS (
  SELECT mint, evt_block_time AS ct
  FROM pumpdotfun_solana.pump_evt_completeevent
  WHERE evt_block_date BETWEEN DATE '2025-07-01' AND DATE '2025-07-31'),
pc AS (
  SELECT base_mint AS mint, pool, quote_mint, creator AS migrator, evt_block_time AS pt, evt_block_slot AS ps,
         row_number() OVER (PARTITION BY base_mint ORDER BY evt_block_time) AS rn
  FROM pumpdotfun_solana.pump_amm_evt_createpoolevent
  WHERE evt_block_date BETWEEN DATE '2025-07-01' AND DATE '2025-08-01'),
u AS (   -- the event query's universe, so rows join on mint and its org1 is the reconciliation target
  SELECT c.mint, p.pool, p.migrator, p.pt AS grad_time, p.ps AS grad_slot
  FROM comp c JOIN pc p ON p.mint = c.mint AND p.rn = 1 AND p.pt >= c.ct AND p.pt < c.ct + INTERVAL '1' DAY
  WHERE p.quote_mint = 'So11111111111111111111111111111111111111112'),
cr AS (
  SELECT mint, arbitrary(COALESCE(creator, "user")) AS creator, min(evt_block_time) AS t0, min(evt_block_slot) AS s0
  FROM pumpdotfun_solana.pump_evt_createevent
  WHERE evt_block_date BETWEEN DATE '2025-04-22' AND DATE '2025-07-31' AND mint IN (SELECT mint FROM u)
  GROUP BY 1),
tok AS (
  -- LEFT JOIN, as the event query: a token created before the 70 days has no creator here and none is set apart.
  -- The two flags are per token, measured from its own graduation day. s8_scope is the aligned-set query's
  -- bound (created within 3 days before it): outside it the same-funder count is not measured, bn is NULL and
  -- the flag says why. cr_own: created within 70 days before it. The lookback above starts at the chunk's
  -- first day, so a late token of a long chunk can have a creator that the same token alone would not.
  SELECT u.mint, u.pool, u.migrator, u.grad_time, u.grad_slot, cr.creator, cr.t0, cr.s0,
         coalesce(cr.t0 >= date_trunc('day', u.grad_time) - INTERVAL '3' DAY, false) AS s8_scope,
         coalesce(cr.t0 >= date_trunc('day', u.grad_time) - INTERVAL '70' DAY, false) AS cr_own
  FROM u LEFT JOIN cr ON cr.mint = u.mint),
slot0 AS (
  -- creation-slot traders, as b1a's buyers0. No is_buy filter: NULL on every 2025 trade event; a
  -- creation-slot seller acquired in that slot
  SELECT DISTINCT k.mint, t."user" AS w, k.t0
  FROM pumpdotfun_solana.pump_evt_tradeevent t JOIN tok k ON t.mint = k.mint AND t.evt_block_slot = k.s0
  WHERE t.evt_block_date BETWEEN DATE '2025-06-28' AND DATE '2025-07-31' AND k.s8_scope),
hitb AS (
  -- The one SOL-transfer reference: senders of at least 0.001 SOL to a creation-slot trader in the 24 h before
  -- creation. Not widened to every taker: that is the join that was cancelled at its cap on b1a.
  SELECT k.mint, k.w, s.from_owner AS funder
  FROM tokens_solana.sol_transfers s JOIN slot0 k ON k.w = s.to_owner
  WHERE s.block_time >= TIMESTAMP '2025-06-27 00:00:00' AND s.block_time < TIMESTAMP '2025-08-01 00:00:00'
    AND CAST(s.amount AS double) >= 1e6
    AND s.block_time BETWEEN k.t0 - INTERVAL '24' HOUR AND k.t0
  GROUP BY 1, 2, 3),
cand AS (
  -- bn = creation-slot traders sharing this wallet's funder, itself included; the largest over its funders.
  -- Whether that makes a bundle is decided locally (MR-14 ruling 5).
  SELECT mint, w, max(nsf) AS bn
  FROM (SELECT mint, w, count(*) OVER (PARTITION BY mint, funder) AS nsf FROM hitb)
  GROUP BY 1, 2),
fsc AS (
  -- Fee-share recipients come as a run-time list, one code per (token, wallet). Should a list ever carry two,
  -- the earlier letter is kept, so a duplicate can never count a taker twice.
  SELECT mint, w, min(code) AS fsx
  FROM (VALUES __FS_CODES__) AS t(mint, w, code)
  WHERE mint IS NOT NULL
  GROUP BY 1, 2),
tk AS (
  -- Swap-event reference 1: every swap on a graduation pool, buy or sell. Owner wallets leave here (MR-3.4),
  -- before any count; the event query does not drop them, so its org1 can be higher on a token the owner traded.
  SELECT pool, w, d, t, slot
  FROM (
    SELECT pool, "user" AS w, evt_block_date AS d, evt_block_time AS t, evt_block_slot AS slot
    FROM pumpdotfun_solana.pump_amm_evt_buyevent
    WHERE evt_block_date BETWEEN DATE '2025-07-01' AND DATE '2025-08-08' AND pool IN (SELECT pool FROM u)
    UNION ALL
    SELECT pool, "user", evt_block_date, evt_block_time, evt_block_slot
    FROM pumpdotfun_solana.pump_amm_evt_sellevent
    WHERE evt_block_date BETWEEN DATE '2025-07-01' AND DATE '2025-08-08' AND pool IN (SELECT pool FROM u))
  WHERE w IS NOT NULL AND __NOT_OWNER(w)__),
wpd AS (
  -- Swap-event reference 2, the last: every pool, grouped at once to (wallet, pool, day). No owner filter is
  -- needed here: this block is only ever joined to takers, and tk has already dropped owner wallets.
  -- Before 2025-06-24 the pool is dropped from the key: those days serve wallet age alone, and the bot week of
  -- every swap of this chunk starts on or after that date, so no token's answer depends on the cut.
  SELECT w, pool, d, min(t) AS tf, min(t) FILTER (WHERE side = 'b') AS fb, min(t) FILTER (WHERE side = 's') AS fs
  FROM (
    SELECT "user" AS w, CASE WHEN evt_block_date >= DATE '2025-06-24' THEN pool END AS pool, evt_block_date AS d,
           evt_block_time AS t, 'b' AS side
    FROM pumpdotfun_solana.pump_amm_evt_buyevent
    WHERE evt_block_date BETWEEN DATE '2025-05-02' AND DATE '2025-08-08'
    UNION ALL
    SELECT "user", CASE WHEN evt_block_date >= DATE '2025-06-24' THEN pool END, evt_block_date, evt_block_time, 's'
    FROM pumpdotfun_solana.pump_amm_evt_sellevent
    WHERE evt_block_date BETWEEN DATE '2025-05-02' AND DATE '2025-08-08')
  WHERE w IS NOT NULL
  GROUP BY 1, 2, 3),
wd AS (
  -- One row per (wallet, day). A round trip is a pool with a buy and a sell that day, first sell not before
  -- first buy; fast = held under 60 seconds. The day's pools travel as 8-byte hashes (two pools of
  -- one wallet-week colliding on 64 bits is not a case to plan for). A day with more than 200 pools
  -- settles every week it falls in by its count alone, so its pools do not travel: no array is ever long.
  SELECT w, d, min(tf) AS tf, count(pool) AS np,
         count_if(pool IS NOT NULL AND fs >= fb) AS rt,
         count_if(pool IS NOT NULL AND fs >= fb AND fs < fb + INTERVAL '60' SECOND) AS fast,
         CASE WHEN count(pool) > 200 THEN CAST(ARRAY[] AS array(varbinary))
              ELSE coalesce(array_agg(xxhash64(to_utf8(pool))) FILTER (WHERE pool IS NOT NULL), CAST(ARRAY[] AS array(varbinary))) END AS ph
  FROM wpd
  GROUP BY 1, 2),
wf AS (
  -- The only window functions on the history side, over the wallet-day table. Each frame ends the day BEFORE
  -- the row's day, so nothing at or after a swap is ever read. t_seen = the wallet's earliest event in the
  -- 60 days before the day. many = more than 200 distinct pools in the 7 days before it:
  -- one day above the line is enough; otherwise the sum of the daily counts is an upper bound, and the
  -- distinct count is taken only where that sum can pass the line (every day's pools are then in its array).
  -- Only the days a swap can fall on leave this block, and only scalars.
  SELECT w, d, t_seen, rt7, fast7, many
  FROM (
    SELECT w, d,
           min(tf) OVER (PARTITION BY w ORDER BY d RANGE BETWEEN INTERVAL '60' DAY PRECEDING AND INTERVAL '1' DAY PRECEDING) AS t_seen,
           sum(rt) OVER (PARTITION BY w ORDER BY d RANGE BETWEEN INTERVAL '7' DAY PRECEDING AND INTERVAL '1' DAY PRECEDING) AS rt7,
           sum(fast) OVER (PARTITION BY w ORDER BY d RANGE BETWEEN INTERVAL '7' DAY PRECEDING AND INTERVAL '1' DAY PRECEDING) AS fast7,
           CASE WHEN max(np) OVER (PARTITION BY w ORDER BY d RANGE BETWEEN INTERVAL '7' DAY PRECEDING AND INTERVAL '1' DAY PRECEDING) > 200 THEN true
                WHEN sum(np) OVER (PARTITION BY w ORDER BY d RANGE BETWEEN INTERVAL '7' DAY PRECEDING AND INTERVAL '1' DAY PRECEDING) > 200
                THEN cardinality(array_distinct(flatten(array_agg(ph) OVER (PARTITION BY w ORDER BY d RANGE BETWEEN INTERVAL '7' DAY PRECEDING AND INTERVAL '1' DAY PRECEDING)))) > 200
                ELSE false END AS many
    FROM wd)
  WHERE d >= DATE '2025-07-01'),
sw AS (
  -- one row per swap in the token's own 7 x 24 h; the flags are those of this swap's wallet on this swap's day
  SELECT k.mint, e.w, e.t, e.slot, k.grad_time,
         CASE WHEN e.w = k.creator THEN 'C' WHEN e.w = k.migrator THEN 'M' ELSE 'O' END AS cls,
         (e.slot = k.grad_slot) AS gs,
         (h.w IS NULL) AS nohist,
         -- aged: an event at least 24 hours before THIS swap, inside the lookback
         coalesce(h.t_seen <= e.t - INTERVAL '24' HOUR, false) AS aged,
         coalesce(h.many, false) AS many,
         -- quick: at least half of the week's round trips were fast (median hold under 60 s); none = not judged
         coalesce(h.rt7 > 0 AND h.fast7 + h.fast7 >= h.rt7, false) AS quick
  FROM tk e JOIN tok k ON k.pool = e.pool
  LEFT JOIN wf h ON h.w = e.w AND h.d = e.d
  WHERE e.t >= k.grad_time AND e.t < k.grad_time + INTERVAL '7' DAY),
pw AS (
  -- one row per (token, wallet). t2 = its first swap that is aged and not bot-shaped at that swap
  SELECT mint, w, cls, grad_time, bool_or(gs) AS gs, min(t) AS t1, min(slot) AS slot1,
         min(t) FILTER (WHERE aged AND NOT many AND NOT quick) AS t2,
         bool_or(aged) AS any_aged, bool_or(NOT many AND NOT quick) AS any_notbot,
         bool_or(many) AS any_many, bool_or(quick) AS any_quick, bool_or(nohist) AS any_nohist
  FROM sw
  GROUP BY 1, 2, 3, 4),
pk AS (
  SELECT p.mint, p.w, p.cls, f.fsx, c.bn, p.gs, p.grad_time, p.t1, p.slot1, p.t2,
         p.any_aged, p.any_notbot, p.any_many, p.any_quick, p.any_nohist
  FROM pw p
  LEFT JOIN fsc f ON f.mint = p.mint AND f.w = p.w
  LEFT JOIN cand c ON c.mint = p.mint AND c.w = p.w),
cell AS (
  SELECT mint, cls, fsx, bn, gs,
         count(*) AS n1, count(t2) AS n2,
         count_if(t1 < grad_time + INTERVAL '1' HOUR) AS n1_1h, count_if(t2 < grad_time + INTERVAL '1' HOUR) AS n2_1h,
         min(t1) AS t1, min(t2) AS t2,
         count_if(any_aged) AS n_aged, count_if(any_notbot) AS n_notbot,
         count_if(any_many) AS n_many, count_if(any_quick) AS n_quick, count_if(any_nohist) AS n_nohist
  FROM pk
  GROUP BY 1, 2, 3, 4, 5)
SELECT k.mint, c.cls, c.fsx, c.bn, c.gs,
       coalesce(c.n1, 0) AS n1, coalesce(c.n2, 0) AS n2, coalesce(c.n1_1h, 0) AS n1_1h, coalesce(c.n2_1h, 0) AS n2_1h,
       date_diff('second', k.grad_time, c.t1) AS t1_s, date_diff('second', k.grad_time, c.t2) AS t2_s,
       -- why v1 and v2 differ: takers with an aged swap, with a swap on a day they were not bot-shaped, and
       -- with a swap on a day each bot leg held. n_nohist is a check: a swap with no history row (must be 0)
       coalesce(c.n_aged, 0) AS n_aged, coalesce(c.n_notbot, 0) AS n_notbot,
       coalesce(c.n_many, 0) AS n_many, coalesce(c.n_quick, 0) AS n_quick, coalesce(c.n_nohist, 0) AS n_nohist,
       k.s8_scope, k.cr_own AS has_creator, 60 AS lb_days, __FS_SUPPLIED__ AS fs_supplied
FROM tok k LEFT JOIN cell c ON c.mint = k.mint