-- 1.6 slippage v2: pre-BOOST (2026-06-01) vs post-BOOST (2026-09-01), no ix_name filter
-- (older events predate that field). Convention B (reported reserves post-swap, pool got q_in).
-- Implied extra quote reserve V per swap: base_out = B - B*Q/(Q+V+q_in) solved for V.
-- If V is ~0 pre-BOOST and constant-per-pool post-BOOST, it is virtual_quote_reserves.
WITH grads AS (SELECT pool FROM pumpdotfun_solana.pump_evt_completepumpammmigrationevent WHERE evt_block_date BETWEEN DATE '2025-03-20' AND DATE '2026-09-01' AND quote_mint = '11111111111111111111111111111111'),
b AS (
  SELECT evt_block_date AS day, pool, evt_tx_id, CAST(quote_amount_in AS double) AS q_in, CAST(base_amount_out AS double) AS base_out,
         CAST(pool_base_token_reserves AS double) + CAST(base_amount_out AS double) AS B,
         CAST(pool_quote_token_reserves AS double) - CAST(quote_amount_in AS double) AS Q,
         row_number() OVER (PARTITION BY evt_block_date ORDER BY evt_block_slot) AS rn
  FROM pumpdotfun_solana.pump_amm_evt_buyevent
  WHERE evt_block_date IN (DATE '2026-06-01', DATE '2026-09-01') AND pool IN (SELECT pool FROM grads) AND base_amount_out > 0 AND quote_amount_in > 0),
c AS (
  SELECT day, pool, q_in, base_out, B, Q,
         100.0 * (B - B*Q/(Q + q_in) - base_out) / base_out AS err_pct,
         B*q_in/base_out - q_in - Q AS implied_V   -- from base_out = B*q_in/(Q+V+q_in)
  FROM b WHERE rn <= 40)
SELECT day, COUNT(*) AS n, approx_percentile(err_pct, 0.5) AS err_p50, approx_percentile(err_pct, 0.9) AS err_p90,
       approx_percentile(implied_V/1e9, 0.1) AS V_sol_p10, approx_percentile(implied_V/1e9, 0.5) AS V_sol_p50, approx_percentile(implied_V/1e9, 0.9) AS V_sol_p90,
       COUNT(DISTINCT pool) AS pools
FROM c GROUP BY 1 ORDER BY 1
