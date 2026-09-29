-- 1.6 fee pricing per era, stratum P, SOL-quoted pools only, aggregate. One sample day per era:
--   era1 pre-2026-01-10 creator-fee overhaul   -> 2025-10-15
--   era2 2026-01-10 .. BOOST (2026-07-21)       -> 2026-04-15
--   era3 post-BOOST                             -> 2026-09-01
-- Trader cost = gross - net (variant-aware); named legs beside it; residual kept.
WITH pools AS (
  SELECT pool FROM pumpdotfun_solana.pump_amm_evt_createpoolevent
  WHERE evt_block_date BETWEEN DATE '2025-03-20' AND DATE '2026-09-01' AND quote_mint = 'So11111111111111111111111111111111111111112'
    AND base_mint IN (SELECT mint FROM pumpdotfun_solana.pump_evt_completeevent WHERE evt_block_date BETWEEN DATE '2025-03-20' AND DATE '2026-09-01')),
b AS (
  SELECT evt_block_date AS d,
         CASE WHEN ix_name = 'buy_exact_quote_in' THEN CAST(quote_amount_in AS double) ELSE CAST(user_quote_amount_in AS double) END AS gross,
         CASE WHEN ix_name = 'buy_exact_quote_in' THEN CAST(user_quote_amount_in AS double) ELSE CAST(quote_amount_in AS double) END AS net,
         CAST(lp_fee AS double) AS lp, CAST(protocol_fee AS double) AS prot, CAST(coin_creator_fee AS double) AS cre
  FROM pumpdotfun_solana.pump_amm_evt_buyevent
  WHERE evt_block_date IN (DATE '2025-10-15', DATE '2026-04-15', DATE '2026-09-01') AND pool IN (SELECT pool FROM pools) AND quote_amount_in > 1000000
  UNION ALL
  SELECT evt_block_date, CAST(quote_amount_out AS double), CAST(user_quote_amount_out AS double), CAST(lp_fee AS double), CAST(protocol_fee AS double), CAST(coin_creator_fee AS double)
  FROM pumpdotfun_solana.pump_amm_evt_sellevent
  WHERE evt_block_date IN (DATE '2025-10-15', DATE '2026-04-15', DATE '2026-09-01') AND pool IN (SELECT pool FROM pools) AND quote_amount_out > 1000000),
c AS (SELECT d, gross, (gross - net) AS cost, lp, prot, cre, (gross - net - lp - prot - cre) AS residual FROM b WHERE gross > 0 AND net > 0 AND gross >= net)
SELECT CASE WHEN d = DATE '2025-10-15' THEN 'era1_pre_creator_overhaul' WHEN d = DATE '2026-04-15' THEN 'era2_pre_boost' ELSE 'era3_post_boost' END AS era,
       COUNT(*) AS swaps,
       approx_percentile(1e4*cost/gross, 0.1) AS cost_bps_p10, approx_percentile(1e4*cost/gross, 0.5) AS cost_bps_p50, approx_percentile(1e4*cost/gross, 0.9) AS cost_bps_p90,
       approx_percentile(1e4*lp/gross, 0.5) AS lp_bps_p50, approx_percentile(1e4*prot/gross, 0.5) AS prot_bps_p50,
       approx_percentile(1e4*cre/gross, 0.5) AS cre_bps_p50, approx_percentile(1e4*cre/gross, 0.9) AS cre_bps_p90,
       ROUND(100.0 * COUNT_IF(cre > 0) / COUNT(*), 1) AS pct_with_creator_fee,
       ROUND(100.0 * COUNT_IF(abs(residual) <= 1) / COUNT(*), 1) AS pct_identity_holds,
       approx_percentile(1e4*residual/gross, 0.9) AS residual_bps_p90
FROM c GROUP BY 1 ORDER BY 1
