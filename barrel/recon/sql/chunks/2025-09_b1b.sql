-- Heavy tier B1b: c06 (top-10 owners ex-pool, ex-curve, ex-burn, share of chain supply at entry),
-- graduations 2025-09-01 .. 2025-09-30. Token ledger only. One row per token; no wallet in the output.
WITH comp AS (
  SELECT mint, evt_block_time AS ct FROM pumpdotfun_solana.pump_evt_completeevent
  WHERE evt_block_date BETWEEN DATE '2025-09-01' AND DATE '2025-09-30'),
pc AS (
  SELECT base_mint AS mint, quote_mint, pool, evt_block_time AS pt,
         row_number() OVER (PARTITION BY base_mint ORDER BY evt_block_time) AS rn
  FROM pumpdotfun_solana.pump_amm_evt_createpoolevent
  WHERE evt_block_date BETWEEN DATE '2025-09-01' AND DATE '2025-10-01'),
u AS (
  SELECT c.mint, p.pt AS grad_time, p.pool
  FROM comp c JOIN pc p ON p.mint = c.mint AND p.rn = 1 AND p.pt >= c.ct AND p.pt < c.ct + INTERVAL '1' DAY
  WHERE p.quote_mint = 'So11111111111111111111111111111111111111112'),
cr AS (
  SELECT mint, arbitrary(COALESCE(creator, "user")) AS creator, min(evt_block_time) AS t0, min(evt_block_slot) AS s0
  FROM pumpdotfun_solana.pump_evt_createevent
  WHERE evt_block_date BETWEEN DATE '2025-08-29' AND DATE '2025-09-30' AND mint IN (SELECT mint FROM u)
  GROUP BY 1),
base AS (   -- per-token creation bound: the same 3 days for every token, wherever it falls in the chunk
  SELECT u.mint, u.grad_time, u.pool, cr.creator, cr.t0, cr.s0 FROM u JOIN cr ON cr.mint = u.mint
  WHERE cr.t0 >= date_trunc('day', u.grad_time) - INTERVAL '3' DAY),
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
    -- first acquisition, within the same horizon for every token (9 days after graduation)
    min(t.block_time) FILTER (WHERE x.d > 0 AND t.block_time < b.grad_time + INTERVAL '9' DAY) AS first_in,
    min(t.block_slot) FILTER (WHERE x.d > 0 AND t.block_time < b.grad_time + INTERVAL '9' DAY) AS first_in_slot,
    bool_or(x.tag = 'to' AND t.from_owner IS NULL) AS got_mint
  FROM tokens_solana.transfers t
  JOIN base b ON b.mint = t.token_mint_address
  CROSS JOIN UNNEST(
    ARRAY[t.to_owner, t.from_owner, '#SUPPLY'],
    ARRAY[CAST(t.amount AS double), -CAST(t.amount AS double),
          CASE WHEN t.from_owner IS NULL THEN CAST(t.amount AS double)
               WHEN t.to_owner IS NULL THEN -CAST(t.amount AS double) ELSE 0e0 END],
    ARRAY['to', 'from', 'sup']) AS x(w, d, tag)
  WHERE t.block_date BETWEEN DATE '2025-08-29' AND DATE '2025-10-10'
    AND t.token_mint_address IN (SELECT mint FROM base) AND x.w IS NOT NULL
  GROUP BY 1, 2),
rk AS (
  SELECT bl.mint, bl.w, bl.b15, bl.b60, bl.b240,
         (bl.w = '#SUPPLY') AS is_sup,
         (bl.w = b.pool OR coalesce(bl.got_mint, false)) AS excluded,
         row_number() OVER (PARTITION BY bl.mint, (bl.w = '#SUPPLY' OR bl.w = b.pool OR coalesce(bl.got_mint, false)) ORDER BY bl.b15 DESC NULLS LAST) AS r15,
         row_number() OVER (PARTITION BY bl.mint, (bl.w = '#SUPPLY' OR bl.w = b.pool OR coalesce(bl.got_mint, false)) ORDER BY bl.b60 DESC NULLS LAST) AS r60,
         row_number() OVER (PARTITION BY bl.mint, (bl.w = '#SUPPLY' OR bl.w = b.pool OR coalesce(bl.got_mint, false)) ORDER BY bl.b240 DESC NULLS LAST) AS r240
  FROM bal bl JOIN base b ON b.mint = bl.mint)
SELECT mint,
       max(b15) FILTER (WHERE is_sup) AS supply15, max(b60) FILTER (WHERE is_sup) AS supply60, max(b240) FILTER (WHERE is_sup) AS supply240,
       sum(b15) FILTER (WHERE NOT is_sup AND NOT excluded AND r15 <= 10 AND b15 > 0) AS top10_15,
       sum(b60) FILTER (WHERE NOT is_sup AND NOT excluded AND r60 <= 10 AND b60 > 0) AS top10_60,
       sum(b240) FILTER (WHERE NOT is_sup AND NOT excluded AND r240 <= 10 AND b240 > 0) AS top10_240,
       count_if(NOT is_sup AND NOT excluded AND b240 > 0) AS holders240,
       count_if(excluded) AS n_excluded_owners,
       count(b15) AS n_owner_rows_filled
FROM rk GROUP BY 1