-- A3 / RUG-A input: aligned-set net flow days 0-7, stratum P, graduations 2025-06-09 .. 2025-06-15.
WITH comp AS (
  SELECT mint, evt_block_time AS ct FROM pumpdotfun_solana.pump_evt_completeevent
  WHERE evt_block_date BETWEEN DATE '2025-06-09' AND DATE '2025-06-15'),
pc AS (
  SELECT base_mint AS mint, quote_mint, evt_block_time AS pt,
         row_number() OVER (PARTITION BY base_mint ORDER BY evt_block_time) AS rn
  FROM pumpdotfun_solana.pump_amm_evt_createpoolevent
  WHERE evt_block_date BETWEEN DATE '2025-06-09' AND DATE '2025-06-16'),
u AS (
  SELECT c.mint, p.pt AS grad_time
  FROM comp c JOIN pc p ON p.mint = c.mint AND p.rn = 1 AND p.pt >= c.ct AND p.pt < c.ct + INTERVAL '1' DAY
  WHERE p.quote_mint = 'So11111111111111111111111111111111111111112'),
cr AS (
  SELECT mint, arbitrary(COALESCE(creator, "user")) AS creator, min(evt_block_time) AS t0, min(evt_block_slot) AS s0
  FROM pumpdotfun_solana.pump_evt_createevent
  WHERE evt_block_date BETWEEN DATE '2025-06-06' AND DATE '2025-06-15' AND mint IN (SELECT mint FROM u)
  GROUP BY 1),
base AS (SELECT u.mint, u.grad_time, cr.creator, cr.t0, cr.s0 FROM u JOIN cr ON cr.mint = u.mint),
st AS (   -- literal bounds on block_time: partition pruning (rule from the S8 watchdog event)
  SELECT from_owner, to_owner, block_time FROM tokens_solana.sol_transfers
  WHERE block_time >= TIMESTAMP '2025-06-05 00:00:00' AND block_time < TIMESTAMP '2025-06-17 00:00:00'
    AND CAST(amount AS double) >= 1e6),
funded AS (
  SELECT DISTINCT b.mint, s.to_owner AS w
  FROM base b JOIN st s ON s.from_owner = b.creator
  WHERE s.block_time BETWEEN b.t0 - INTERVAL '24' HOUR AND b.t0 + INTERVAL '24' HOUR),
buyers0 AS (
  SELECT DISTINCT b.mint, t.user AS w
  FROM pumpdotfun_solana.pump_evt_tradeevent t JOIN base b ON t.mint = b.mint AND t.evt_block_slot = b.s0
  WHERE t.evt_block_date BETWEEN DATE '2025-06-06' AND DATE '2025-06-15' AND t.is_buy),
bf AS (
  SELECT x.mint, x.w, s.from_owner AS funder
  FROM buyers0 x JOIN base b ON b.mint = x.mint JOIN st s ON s.to_owner = x.w
  WHERE s.block_time BETWEEN b.t0 - INTERVAL '24' HOUR AND b.t0),
bigf AS (SELECT mint, funder FROM bf GROUP BY 1, 2 HAVING count(DISTINCT w) >= 5),
bundle AS (SELECT DISTINCT bf.mint, bf.w FROM bf JOIN bigf ON bigf.mint = bf.mint AND bigf.funder = bf.funder),
seta AS (
  SELECT mint, creator AS w FROM base
  UNION SELECT mint, w FROM funded
  UNION SELECT mint, w FROM bundle),
tr AS (
  SELECT token_mint_address AS mint, from_owner, to_owner, CAST(amount AS double) AS amt, block_time AS bt
  FROM tokens_solana.transfers
  WHERE block_date BETWEEN DATE '2025-06-06' AND DATE '2025-06-24' AND token_mint_address IN (SELECT mint FROM base)),
flows AS (
  SELECT mint, to_owner AS w, amt AS d, bt FROM tr WHERE to_owner IS NOT NULL
  UNION ALL
  SELECT mint, from_owner, -amt, bt FROM tr WHERE from_owner IS NOT NULL),
af AS (
  SELECT f.mint, f.d, f.bt, b.grad_time
  FROM flows f JOIN seta s ON s.mint = f.mint AND s.w = f.w JOIN base b ON b.mint = f.mint),
agg AS (
  SELECT mint,
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
  FROM af GROUP BY 1),
sn AS (
  SELECT s.mint, count(*) AS seta_n,
         count_if(s.w IN (SELECT w FROM funded f WHERE f.mint = s.mint)) AS n_funded,
         count_if(s.w IN (SELECT w FROM bundle x WHERE x.mint = s.mint)) AS n_bundle
  FROM seta s GROUP BY 1)
SELECT u.mint,
       CASE WHEN b.mint IS NULL THEN 'created_outside_window' END AS reason,
       sn.seta_n, sn.n_funded, sn.n_bundle,
       a.hold_entry, a.net_after_entry,
       CASE WHEN a.hold_entry > 0 THEN -coalesce(a.net_after_entry, 0) / a.hold_entry END AS sf_7d,
       a.f0, a.f1, a.f2, a.f3, a.f4, a.f5, a.f6, a.f7
FROM u LEFT JOIN base b ON b.mint = u.mint LEFT JOIN sn ON sn.mint = u.mint LEFT JOIN agg a ON a.mint = u.mint