-- 1.6 slippage, PRE-BOOST day (2026-06-01) vs post-BOOST (2026-09-01), 10 buys each.
-- Conventions: B = reported reserves are post-swap, pool received q_in (net of protocol/creator);
--              C = reported reserves are PRE-swap. Hypothesis: post-BOOST error comes from
--              virtual_quote_reserves (absent from Dune's pinned layout); pre-BOOST error ~0.
WITH grads AS (SELECT pool, evt_block_date AS gday FROM pumpdotfun_solana.pump_evt_completepumpammmigrationevent WHERE evt_block_date BETWEEN DATE '2025-03-20' AND DATE '2026-09-01' AND quote_mint = '11111111111111111111111111111111'),
b AS (
  SELECT evt_block_date AS day, evt_tx_id, CAST(quote_amount_in AS double) AS q_in, CAST(base_amount_out AS double) AS base_out, CAST(lp_fee AS double) AS lp,
         CAST(pool_base_token_reserves AS double) AS base_r, CAST(pool_quote_token_reserves AS double) AS quote_r,
         row_number() OVER (PARTITION BY evt_block_date ORDER BY evt_block_slot) AS rn
  FROM pumpdotfun_solana.pump_amm_evt_buyevent
  WHERE evt_block_date IN (DATE '2026-06-01', DATE '2026-09-01') AND pool IN (SELECT pool FROM grads) AND base_amount_out > 0 AND ix_name = 'buy')
SELECT day, COUNT(*) AS n,
       approx_percentile(100.0 * ((base_r + base_out) - (base_r + base_out) * (quote_r - q_in) / (quote_r - q_in + q_in) - base_out) / base_out, 0.5) AS errB_p50,
       approx_percentile(100.0 * ((base_r + base_out) - (base_r + base_out) * (quote_r - q_in) / (quote_r - q_in + q_in) - base_out) / base_out, 0.9) AS errB_p90,
       approx_percentile(100.0 * (base_r - base_r * quote_r / (quote_r + q_in) - base_out) / base_out, 0.5) AS errC_p50,
       approx_percentile(100.0 * (base_r - base_r * quote_r / (quote_r + q_in) - base_out) / base_out, 0.9) AS errC_p90,
       approx_percentile(100.0 * (base_r - base_r * quote_r / (quote_r + q_in - lp) - base_out) / base_out, 0.5) AS errC_lp_p50
FROM b WHERE rn <= 10 GROUP BY 1 ORDER BY 1
