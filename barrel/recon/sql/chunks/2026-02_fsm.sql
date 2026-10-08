-- Fee-share recipients (S7b), query 2 of 2: the aligned-set member row of each decoded recipient, graduations 2026-02-01 .. 2026-02-28.
-- Rows name wallets: run with --private-rows. No threshold, no classification, no verdict here.
-- The (token, recipient) candidates are the decoded pairs of this chunk, supplied at run time (--pairs-from).
-- One row per candidate, in the columns, windows and predicates of the aligned-set member rows, so that a
-- recipient that is not in that set yet can be added to it locally. is_creator and is_funded mark the ones
-- that are in it already. Whether a candidate matters (held the token, did not receive the mint) is decided
-- locally from first_in and got_mint: every candidate comes back, so a missing row is a fault and not an answer.
WITH comp AS (
  SELECT mint, evt_block_time AS ct FROM pumpdotfun_solana.pump_evt_completeevent
  WHERE evt_block_date BETWEEN DATE '2026-02-01' AND DATE '2026-02-28'),
pc AS (
  SELECT base_mint AS mint, quote_mint, pool, evt_block_time AS pt,
         row_number() OVER (PARTITION BY base_mint ORDER BY evt_block_time) AS rn
  FROM pumpdotfun_solana.pump_amm_evt_createpoolevent
  WHERE evt_block_date BETWEEN DATE '2026-02-01' AND DATE '2026-03-01'),
u AS (
  SELECT c.mint, p.pt AS grad_time, p.pool
  FROM comp c JOIN pc p ON p.mint = c.mint AND p.rn = 1 AND p.pt >= c.ct AND p.pt < c.ct + INTERVAL '1' DAY
  WHERE p.quote_mint = 'So11111111111111111111111111111111111111112'),
cr AS (
  SELECT mint, arbitrary(COALESCE(creator, "user")) AS creator, min(evt_block_time) AS t0, min(evt_block_slot) AS s0
  FROM pumpdotfun_solana.pump_evt_createevent
  WHERE evt_block_date BETWEEN DATE '2026-01-29' AND DATE '2026-02-28' AND mint IN (SELECT mint FROM u)
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
    -- the transaction of that first acquisition (earliest by slot, then by position in the slot)
    min_by(t.tx_id, t.block_slot * 100000 + coalesce(t.tx_index, 0))
      FILTER (WHERE x.d > 0 AND t.block_time < b.grad_time + INTERVAL '9' DAY) AS first_in_tx,
    bool_or(x.tag = 'to' AND t.from_owner IS NULL) AS got_mint
  FROM tokens_solana.transfers t
  JOIN base b ON b.mint = t.token_mint_address
  CROSS JOIN UNNEST(
    ARRAY[t.to_owner, t.from_owner, '#SUPPLY'],
    ARRAY[CAST(t.amount AS double), -CAST(t.amount AS double),
          CASE WHEN t.from_owner IS NULL THEN CAST(t.amount AS double)
               WHEN t.to_owner IS NULL THEN -CAST(t.amount AS double) ELSE 0e0 END],
    ARRAY['to', 'from', 'sup']) AS x(w, d, tag)
  WHERE t.block_date BETWEEN DATE '2026-01-29' AND DATE '2026-03-10'
    AND t.token_mint_address IN (SELECT mint FROM base) AND x.w IS NOT NULL
  GROUP BY 1, 2),
pairs AS (
  SELECT DISTINCT mint, w FROM (VALUES __FS_PAIRS__) AS t(mint, w)
  WHERE mint IS NOT NULL AND w IS NOT NULL AND __NOT_OWNER(w)__),
mm AS (   -- one row per candidate: its token's windows and its own ledger aggregates (empty when it never touched the token)
  SELECT p.mint, p.w, bs.creator, bs.t0, bs.grad_time, (p.w = bs.creator) AS is_creator,
         b.first_in, b.first_in_tx, b.b15, b.b60, b.b240, b.net_after, b.f0, b.f1, b.f2, b.f3, b.f4, b.f5, b.f6, b.f7, b.got_mint,
         CASE WHEN p.w = bs.creator THEN bs.s0 ELSE b.first_in_slot END AS slot_act
  FROM pairs p JOIN base bs ON bs.mint = p.mint
  LEFT JOIN bal b ON b.mint = p.mint AND b.w = p.w),
tr AS (   -- ONE pass over SOL transfers: everything of at least 0.001 SOL a candidate received in the scanned window
  SELECT s.to_owner AS w, s.from_owner AS sender, s.block_time, s.block_slot, s.tx_id, CAST(s.amount AS double) AS amt
  FROM tokens_solana.sol_transfers s
  WHERE s.block_time >= TIMESTAMP '2026-01-28 00:00:00' AND s.block_time < TIMESTAMP '2026-03-11 00:00:00'
    AND CAST(s.amount AS double) >= 1e6
    AND s.to_owner IN (SELECT w FROM pairs)
    AND s.from_owner <> s.to_owner),
snd AS (  -- one row per (candidate, sender); a candidate that received nothing keeps one row with the sender empty.
          -- Both uses of a transfer are read from this one pass: funding, and the creator's payment.
  SELECT m.mint, m.w, t.sender,
         arbitrary(m.is_creator) AS is_creator, arbitrary(m.grad_time) AS grad_time, arbitrary(m.t0) AS t0, arbitrary(m.first_in) AS first_in, arbitrary(m.first_in_tx) AS first_in_tx, arbitrary(m.b15) AS b15, arbitrary(m.b60) AS b60, arbitrary(m.b240) AS b240, arbitrary(m.net_after) AS net_after, arbitrary(m.f0) AS f0, arbitrary(m.f1) AS f1, arbitrary(m.f2) AS f2, arbitrary(m.f3) AS f3, arbitrary(m.f4) AS f4, arbitrary(m.f5) AS f5, arbitrary(m.f6) AS f6, arbitrary(m.f7) AS f7, arbitrary(m.got_mint) AS got_mint,
         min(t.block_time) FILTER (WHERE t.block_slot <= m.slot_act AND (m.first_in_tx IS NULL OR t.tx_id IS NULL OR t.tx_id <> m.first_in_tx)
             AND t.block_time >= date_trunc('day', m.grad_time) - INTERVAL '4' DAY) AS t_first,
         max(t.block_slot) FILTER (WHERE t.block_slot <= m.slot_act AND (m.first_in_tx IS NULL OR t.tx_id IS NULL OR t.tx_id <> m.first_in_tx)
             AND t.block_time >= date_trunc('day', m.grad_time) - INTERVAL '4' DAY) AS slot_last,
         max_by(t.amt, t.block_slot) FILTER (WHERE t.block_slot <= m.slot_act AND (m.first_in_tx IS NULL OR t.tx_id IS NULL OR t.tx_id <> m.first_in_tx)
             AND t.block_time >= date_trunc('day', m.grad_time) - INTERVAL '4' DAY) / 1e9 AS last_sol,
         sum(t.amt) FILTER (WHERE t.sender = m.creator AND t.block_time BETWEEN m.t0 - INTERVAL '24' HOUR AND m.t0 + INTERVAL '24' HOUR) / 1e9 AS cr_sol,
         array_agg(DISTINCT t.tx_id) FILTER (WHERE t.sender = m.creator AND t.block_time BETWEEN m.t0 - INTERVAL '24' HOUR AND m.t0 + INTERVAL '24' HOUR) AS cr_txs
  FROM mm m LEFT JOIN tr t ON t.w = m.w
  GROUP BY 1, 2, 3),
one AS (  -- funder = the last sender before the first action, by slot, then by address; a sender with no funding
          -- transfer sorts after every one that has. The creator's payment sits on one sender row and is spread
          -- over the candidate's rows, so nothing above is read a second time.
  SELECT * FROM (
    SELECT *, row_number() OVER (PARTITION BY mint, w ORDER BY slot_last DESC NULLS LAST, sender DESC) AS rn,
           count(slot_last) OVER (PARTITION BY mint, w) AS n_fund,
           max(cr_sol) OVER (PARTITION BY mint, w) AS cr_sol_m,
           arbitrary(cr_txs) OVER (PARTITION BY mint, w) AS cr_txs_m
    FROM snd)
  WHERE rn = 1)
SELECT o.mint, o.w, o.is_creator, (o.cr_sol_m IS NOT NULL) AS is_funded,
       -- typed as the aligned-set query types it (MR-16): the creator's SOL inside the wallet's own first-acquisition
       -- transaction is a token delivery, in any other transaction it is funding, and both can hold
       CASE WHEN o.cr_sol_m IS NULL THEN NULL
            WHEN coalesce(contains(o.cr_txs_m, o.first_in_tx), false)
                 THEN CASE WHEN cardinality(o.cr_txs_m) > 1 THEN 'both' ELSE 'token_delivery_by_creator' END
            ELSE 'sol_funding' END AS link,
       CAST(NULL AS bigint) AS bundle_n,     -- creation-slot traders are not looked at here
       o.cr_sol_m AS cr_sol, o.grad_time, o.t0, o.first_in, o.b15, o.b60, o.b240, o.net_after,
       o.f0, o.f1, o.f2, o.f3, o.f4, o.f5, o.f6, o.f7, o.got_mint,
       CASE WHEN o.slot_last IS NOT NULL THEN o.sender END AS funder,
       o.t_first AS funded_ts, o.last_sol AS funded_sol, NULLIF(o.n_fund, 0) AS n_senders,
       date_diff('second', o.t_first, o.first_in) AS fund_to_first_buy_s
FROM one o