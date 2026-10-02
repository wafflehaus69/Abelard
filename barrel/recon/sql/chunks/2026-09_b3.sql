-- Heavy tier B3: cluster_* inputs (H2), graduations 2026-09-01 .. 2026-09-20. Rows name funders: --private-rows.
WITH comp AS (
  SELECT mint, evt_block_time AS ct FROM pumpdotfun_solana.pump_evt_completeevent
  WHERE evt_block_date BETWEEN DATE '2026-09-01' AND DATE '2026-09-20'),
pc AS (
  SELECT base_mint AS mint, quote_mint, pool, evt_block_time AS pt,
         row_number() OVER (PARTITION BY base_mint ORDER BY evt_block_time) AS rn
  FROM pumpdotfun_solana.pump_amm_evt_createpoolevent
  WHERE evt_block_date BETWEEN DATE '2026-09-01' AND DATE '2026-09-21'),
u AS (
  SELECT c.mint, p.pt AS grad_time, p.pool
  FROM comp c JOIN pc p ON p.mint = c.mint AND p.rn = 1 AND p.pt >= c.ct AND p.pt < c.ct + INTERVAL '1' DAY
  WHERE p.quote_mint = 'So11111111111111111111111111111111111111112'),
early0 AS (  -- wallets that bought on the graduation pool within 30 minutes
  SELECT u.mint, u.pool, u.grad_time, b."user" AS w, min(b.evt_block_time) AS t_buy
  FROM pumpdotfun_solana.pump_amm_evt_buyevent b JOIN u ON u.pool = b.pool
  WHERE b.evt_block_date BETWEEN DATE '2026-09-01' AND DATE '2026-09-21'
    AND b.evt_block_time >= u.grad_time AND b.evt_block_time <= u.grad_time + INTERVAL '30' MINUTE
  GROUP BY 1, 2, 3, 4),
early AS (SELECT * FROM early0 WHERE __NOT_OWNER(w)__),
fund AS (    -- every sender of SOL to an early buyer in the 7 days before graduation
  SELECT e.mint, e.w, s.from_owner AS funder
  FROM tokens_solana.sol_transfers s JOIN early e ON s.to_owner = e.w
  WHERE s.block_time >= TIMESTAMP '2026-08-25 00:00:00' AND s.block_time < TIMESTAMP '2026-09-22 00:00:00'
    AND CAST(s.amount AS double) >= 1e6
    AND s.from_owner <> e.w
    AND s.block_time < e.grad_time AND s.block_time >= e.grad_time - INTERVAL '7' DAY
  GROUP BY 1, 2, 3),
grp AS (SELECT mint, funder, count(*) AS n_buyers FROM fund GROUP BY 1, 2 HAVING count(*) >= 3),
sells AS (   -- first sell by any member of the group, and the price in that event
  SELECT g.mint, g.funder, min(sv.evt_block_time) AS t1,
         -- raw pre-swap reserves at the group's first sell; the price needs the pool's virtual reserve,
         -- which the event query derives, so it is computed locally and never here
         min_by(CAST(sv.pool_quote_token_reserves AS double), sv.evt_block_time) AS q_t1,
         min_by(CAST(sv.pool_base_token_reserves AS double), sv.evt_block_time) AS b_t1
  FROM grp g JOIN fund f ON f.mint = g.mint AND f.funder = g.funder
  JOIN u ON u.mint = g.mint
  JOIN pumpdotfun_solana.pump_amm_evt_sellevent sv ON sv.pool = u.pool AND sv."user" = f.w
  WHERE sv.evt_block_date BETWEEN DATE '2026-09-01' AND DATE '2026-09-29' AND sv.evt_block_time >= u.grad_time
  GROUP BY 1, 2),
fan AS (
  SELECT s.from_owner AS a, approx_distinct(s.to_owner) AS fan_out
  FROM tokens_solana.sol_transfers s
  WHERE s.block_time >= TIMESTAMP '2026-08-25 00:00:00' AND s.block_time < TIMESTAMP '2026-09-22 00:00:00'
    AND CAST(s.amount AS double) >= 1e6
  GROUP BY 1),
ne AS (SELECT mint, count(*) AS n_early FROM early GROUP BY 1)
SELECT g.mint, g.funder, g.n_buyers, ne.n_early, f.fan_out, sl.t1, sl.q_t1, sl.b_t1
FROM grp g JOIN ne ON ne.mint = g.mint LEFT JOIN fan f ON f.a = g.funder
LEFT JOIN sells sl ON sl.mint = g.mint AND sl.funder = g.funder