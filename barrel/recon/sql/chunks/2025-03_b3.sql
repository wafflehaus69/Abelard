-- Heavy tier B3 (build form): early-buyer groups by common funder, cluster_* inputs (H2), graduations 2025-03-20 .. 2025-03-31.
-- Rows name wallets: run with --private-rows. No threshold, no classification, no verdict here.
-- One row per (token, funder) kept by the reduction at the end; one row with a NULL funder for a token with no group.
-- Group sizes, month recipients and the label fact are returned; every cut is local (recon/early_buyers.py).
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
ev AS (      -- one pass over the buys and sells of the pools
  SELECT pool, 'buy' AS side, "user" AS w, evt_block_time AS t,
         -- order inside a second: block time is whole seconds, so slot first, then position in the slot
         evt_block_slot * 100000 + coalesce(evt_tx_index, 0) AS k,
         (evt_block_slot IS NULL) AS no_slot, (evt_tx_index IS NULL) AS no_txi,
         CAST(pool_quote_token_reserves AS double) AS q, CAST(pool_base_token_reserves AS double) AS b,
         -- ix_name is NULL on every decoded buy (buyevent_variant_probe.sql): net, what enters the curve, is the
         -- smaller of the two quote fields, whichever variant emitted the event
         least(CAST(quote_amount_in AS double), CAST(user_quote_amount_in AS double)) AS net,
         CAST(base_amount_out AS double) AS base_amt
  FROM pumpdotfun_solana.pump_amm_evt_buyevent
  WHERE evt_block_date BETWEEN DATE '2025-03-20' AND DATE '2025-04-03' AND pool IN (SELECT pool FROM u)
  UNION ALL
  SELECT pool, 'sell', "user", evt_block_time,
         evt_block_slot * 100000 + coalesce(evt_tx_index, 0),
         (evt_block_slot IS NULL), (evt_tx_index IS NULL),
         CAST(pool_quote_token_reserves AS double), CAST(pool_base_token_reserves AS double),
         CAST(NULL AS double), CAST(base_amount_in AS double)
  FROM pumpdotfun_solana.pump_amm_evt_sellevent
  WHERE evt_block_date BETWEEN DATE '2025-03-20' AND DATE '2025-04-03' AND pool IN (SELECT pool FROM u)),
evh AS (     -- the horizon of each token: graduation to the last lag plus 24 hours, wherever it falls in the chunk
  SELECT u.mint, u.grad_time, ev.*,
         -- virtual quote reserve, derived per pool from its own buys with the expression of the event query
         -- (MR-15 B1): V = B * x / base_out - Q - x at the largest buy of at least 0.001 SOL; none, NULL
         max_by(CASE WHEN ev.side = 'buy' AND ev.base_amt > 0 AND ev.net >= 1e6 THEN ev.b * ev.net / ev.base_amt - ev.q - ev.net END,
                CASE WHEN ev.side = 'buy' AND ev.base_amt > 0 AND ev.net >= 1e6 THEN ev.net END) OVER (PARTITION BY ev.pool) AS v,
         -- E38: the two columns the order key is built from were never null-counted per era; the counts come back
         count_if(ev.no_slot) OVER (PARTITION BY ev.pool) AS ev_no_slot,
         count_if(ev.no_txi) OVER (PARTITION BY ev.pool) AS ev_no_txi
  FROM ev JOIN u ON u.pool = ev.pool
  WHERE ev.t >= u.grad_time AND ev.t < u.grad_time + INTERVAL '240' MINUTE + INTERVAL '24' HOUR),
wal AS (     -- one row per (token, wallet): first early buy, first sell, and the first sell at or after each entry lag
  SELECT mint, w, arbitrary(grad_time) AS grad_time, arbitrary(v) AS v,
    arbitrary(ev_no_slot) AS ev_no_slot, arbitrary(ev_no_txi) AS ev_no_txi,
    min(t) FILTER (WHERE side = 'buy' AND t <= grad_time + INTERVAL '30' MINUTE) AS t_buy,
    min(k) FILTER (WHERE side = 'sell') AS k_any,
    min_by(t, k) FILTER (WHERE side = 'sell') AS s_any,
    min_by(q, k) FILTER (WHERE side = 'sell') AS q_any,
    min_by(b, k) FILTER (WHERE side = 'sell') AS b_any,
    min(k) FILTER (WHERE side = 'sell' AND t >= grad_time + INTERVAL '15' MINUTE AND t < grad_time + INTERVAL '15' MINUTE + INTERVAL '24' HOUR) AS k15,
    min_by(t, k) FILTER (WHERE side = 'sell' AND t >= grad_time + INTERVAL '15' MINUTE AND t < grad_time + INTERVAL '15' MINUTE + INTERVAL '24' HOUR) AS s15,
    min_by(q, k) FILTER (WHERE side = 'sell' AND t >= grad_time + INTERVAL '15' MINUTE AND t < grad_time + INTERVAL '15' MINUTE + INTERVAL '24' HOUR) AS q15,
    min_by(b, k) FILTER (WHERE side = 'sell' AND t >= grad_time + INTERVAL '15' MINUTE AND t < grad_time + INTERVAL '15' MINUTE + INTERVAL '24' HOUR) AS b15,
    sum(CASE WHEN side = 'buy' THEN base_amt ELSE -base_amt END) FILTER (WHERE t < grad_time + INTERVAL '15' MINUTE) AS hold15,
    bool_or(side = 'sell' AND t < grad_time + INTERVAL '15' MINUTE) AS sold15,
    min(k) FILTER (WHERE side = 'sell' AND t >= grad_time + INTERVAL '60' MINUTE AND t < grad_time + INTERVAL '60' MINUTE + INTERVAL '24' HOUR) AS k60,
    min_by(t, k) FILTER (WHERE side = 'sell' AND t >= grad_time + INTERVAL '60' MINUTE AND t < grad_time + INTERVAL '60' MINUTE + INTERVAL '24' HOUR) AS s60,
    min_by(q, k) FILTER (WHERE side = 'sell' AND t >= grad_time + INTERVAL '60' MINUTE AND t < grad_time + INTERVAL '60' MINUTE + INTERVAL '24' HOUR) AS q60,
    min_by(b, k) FILTER (WHERE side = 'sell' AND t >= grad_time + INTERVAL '60' MINUTE AND t < grad_time + INTERVAL '60' MINUTE + INTERVAL '24' HOUR) AS b60,
    sum(CASE WHEN side = 'buy' THEN base_amt ELSE -base_amt END) FILTER (WHERE t < grad_time + INTERVAL '60' MINUTE) AS hold60,
    bool_or(side = 'sell' AND t < grad_time + INTERVAL '60' MINUTE) AS sold60,
    min(k) FILTER (WHERE side = 'sell' AND t >= grad_time + INTERVAL '240' MINUTE AND t < grad_time + INTERVAL '240' MINUTE + INTERVAL '24' HOUR) AS k240,
    min_by(t, k) FILTER (WHERE side = 'sell' AND t >= grad_time + INTERVAL '240' MINUTE AND t < grad_time + INTERVAL '240' MINUTE + INTERVAL '24' HOUR) AS s240,
    min_by(q, k) FILTER (WHERE side = 'sell' AND t >= grad_time + INTERVAL '240' MINUTE AND t < grad_time + INTERVAL '240' MINUTE + INTERVAL '24' HOUR) AS q240,
    min_by(b, k) FILTER (WHERE side = 'sell' AND t >= grad_time + INTERVAL '240' MINUTE AND t < grad_time + INTERVAL '240' MINUTE + INTERVAL '24' HOUR) AS b240,
    sum(CASE WHEN side = 'buy' THEN base_amt ELSE -base_amt END) FILTER (WHERE t < grad_time + INTERVAL '240' MINUTE) AS hold240,
    bool_or(side = 'sell' AND t < grad_time + INTERVAL '240' MINUTE) AS sold240
  FROM evh GROUP BY 1, 2),
early AS (   -- early buyers, owner wallets out before anything is counted; the token count rides on every row,
             -- so this block is read once
  SELECT *, count(*) OVER (PARTITION BY mint) AS n_early
  FROM wal WHERE t_buy IS NOT NULL AND w IS NOT NULL AND __NOT_OWNER(w)__),
fund0 AS (   -- every (token, early buyer, sender) in the 7 days before graduation: the only read of this join
  SELECT e.mint, e.w, s.from_owner AS funder,
         arbitrary(e.grad_time) AS grad_time, arbitrary(e.n_early) AS n_early, arbitrary(e.v) AS v, arbitrary(e.ev_no_slot) AS ev_no_slot, arbitrary(e.ev_no_txi) AS ev_no_txi,
         arbitrary(e.t_buy) AS t_buy, arbitrary(e.k_any) AS k_any, arbitrary(e.s_any) AS s_any, arbitrary(e.q_any) AS q_any, arbitrary(e.b_any) AS b_any,
         arbitrary(e.k15) AS k15, arbitrary(e.s15) AS s15, arbitrary(e.q15) AS q15, arbitrary(e.b15) AS b15, arbitrary(e.hold15) AS hold15, arbitrary(e.sold15) AS sold15,
         arbitrary(e.k60) AS k60, arbitrary(e.s60) AS s60, arbitrary(e.q60) AS q60, arbitrary(e.b60) AS b60, arbitrary(e.hold60) AS hold60, arbitrary(e.sold60) AS sold60,
         arbitrary(e.k240) AS k240, arbitrary(e.s240) AS s240, arbitrary(e.q240) AS q240, arbitrary(e.b240) AS b240, arbitrary(e.hold240) AS hold240, arbitrary(e.sold240) AS sold240
  FROM tokens_solana.sol_transfers s JOIN early e ON s.to_owner = e.w
  WHERE s.block_time >= TIMESTAMP '2025-03-13 00:00:00' AND s.block_time < TIMESTAMP '2025-04-02 00:00:00'
    AND CAST(s.amount AS double) >= 1e6
    AND s.from_owner <> e.w
    AND s.block_time < e.grad_time AND s.block_time >= e.grad_time - INTERVAL '7' DAY
  GROUP BY 1, 2, 3),
fund AS (    -- an owner wallet is no funder either; wk = 1 marks each funded wallet once per token
  SELECT *, row_number() OVER (PARTITION BY mint, w ORDER BY funder) AS wk
  FROM fund0 WHERE __NOT_OWNER(funder)__),
grp AS (     -- one row per (token, funder). No cut of any kind.
  SELECT mint, funder, arbitrary(grad_time) AS grad_time, arbitrary(n_early) AS n_early, arbitrary(v) AS v,
         arbitrary(ev_no_slot) AS ev_no_slot, arbitrary(ev_no_txi) AS ev_no_txi,
         count(*) AS n_buyers, count_if(wk = 1) AS n_first,
         -- the earliest member buys, seconds from graduation: when the group reached any size is read locally
         slice(array_sort(array_agg(date_diff('second', grad_time, t_buy))), 1, 8) AS buy_s,
         min_by(date_diff('second', grad_time, s_any), k_any) AS t_first_s,
         min_by(q_any, k_any) AS q_first, min_by(b_any, k_any) AS b_first,
         min_by(date_diff('second', grad_time, s15), k15) AS t1_15, min_by(q15, k15) AS q1_15, min_by(b15, k15) AS b1_15,
         sum(hold15) AS hold_15, count_if(sold15) AS nsp_15,
         min_by(date_diff('second', grad_time, s60), k60) AS t1_60, min_by(q60, k60) AS q1_60, min_by(b60, k60) AS b1_60,
         sum(hold60) AS hold_60, count_if(sold60) AS nsp_60,
         min_by(date_diff('second', grad_time, s240), k240) AS t1_240, min_by(q240, k240) AS q1_240, min_by(b240, k240) AS b1_240,
         sum(hold240) AS hold_240, count_if(sold240) AS nsp_240
  FROM fund GROUP BY 1, 2),
fan AS (     -- recipients of every sender over the calendar month of the chunk, as recon/gen_fanout.py counts them
  SELECT s.from_owner AS a, approx_distinct(s.to_owner) AS recipients
  FROM tokens_solana.sol_transfers s
  WHERE s.block_time >= TIMESTAMP '2025-03-01 00:00:00' AND s.block_time < TIMESTAMP '2025-04-01 00:00:00'
    AND CAST(s.amount AS double) >= 1e6
    AND s.to_owner <> s.from_owner
  GROUP BY 1),
lab AS (SELECT DISTINCT address FROM cex_solana.addresses),
gl AS (      -- the two facts about the funder. No fan-out row in the month is a measured zero, as recon/actors.py reads it
  SELECT g.*, coalesce(f.recipients, 0) AS recipients, (l.address IS NOT NULL) AS labelled
  FROM grp g LEFT JOIN fan f ON f.a = g.funder LEFT JOIN lab l ON l.address = g.funder),
gf AS (      -- counts over ALL groups of the token, taken before anything is dropped; then the running largest group
  SELECT gl.*,
         count(*) OVER (PARTITION BY mint) AS n_groups,
         sum(n_first) OVER (PARTITION BY mint) AS n_funded,
         sum(n_buyers) OVER (PARTITION BY mint) AS n_pairs,
         max(n_buyers) OVER (PARTITION BY mint) AS n_max_any,
         max(n_buyers) OVER (PARTITION BY mint, labelled ORDER BY CASE WHEN labelled THEN 0 ELSE recipients END, n_buyers DESC, funder ROWS UNBOUNDED PRECEDING) AS runmax
  FROM gl),
ranked AS (  -- rn = 1 is the group that set a new running maximum: larger than every group behind a funder with
             -- no more recipients than its own. A column against a window result; no number decides anything.
  SELECT gf.*, row_number() OVER (PARTITION BY mint, labelled, runmax ORDER BY CASE WHEN labelled THEN 0 ELSE recipients END, n_buyers DESC, funder) AS rn
  FROM gf),
kept AS (SELECT * FROM ranked WHERE rn = 1)
SELECT u.mint, u.grad_time, c.funder, c.labelled, c.recipients, 31 AS window_days,
       c.n_buyers, c.buy_s, c.n_early, c.n_funded, c.n_groups, c.n_pairs, c.n_max_any,
       c.v AS vqr, c.ev_no_slot, c.ev_no_txi,
       c.t_first_s, c.q_first, c.b_first,
       -- one element per entry lag, 15 / 60 / 240 minutes: the first member sell at or after the lag
       ARRAY[c.t1_15, c.t1_60, c.t1_240] AS t1_s,
       ARRAY[c.q1_15, c.q1_60, c.q1_240] AS q_t1,
       ARRAY[c.b1_15, c.b1_60, c.b1_240] AS b_t1,
       ARRAY[(c.q1_15 + c.v) / NULLIF(c.b1_15, 0), (c.q1_60 + c.v) / NULLIF(c.b1_60, 0), (c.q1_240 + c.v) / NULLIF(c.b1_240, 0)] AS px_t1,
       ARRAY[c.hold_15, c.hold_60, c.hold_240] AS net_base,
       ARRAY[c.nsp_15, c.nsp_60, c.nsp_240] AS n_sold_pre
FROM u LEFT JOIN kept c ON c.mint = u.mint