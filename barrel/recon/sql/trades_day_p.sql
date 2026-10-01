-- Per-swap rows, stratum P, one day (2025-06-10). Frozen 20 + 3 schema.
WITH pools AS (
  SELECT pool, arbitrary(base_mint) AS mint
  FROM pumpdotfun_solana.pump_amm_evt_createpoolevent
  WHERE evt_block_date BETWEEN DATE '2025-03-20' AND DATE '2025-06-10' AND quote_mint = 'So11111111111111111111111111111111111111112'
    AND base_mint IN (SELECT mint FROM pumpdotfun_solana.pump_evt_completeevent
                      WHERE evt_block_date BETWEEN DATE '2025-03-20' AND DATE '2025-06-10')
  GROUP BY 1),
s AS (
  SELECT b.evt_tx_id AS signature, b.evt_block_slot AS slot, b.evt_block_time AS block_timestamp, b.evt_tx_index AS tx_index,
         b.pool, p.mint, 'buy' AS side, b.user AS wallet,
         CASE WHEN b.ix_name = 'buy_exact_quote_in' THEN b.quote_amount_in ELSE b.user_quote_amount_in END AS gross_quote,
         CASE WHEN b.ix_name = 'buy_exact_quote_in' THEN b.user_quote_amount_in ELSE b.quote_amount_in END AS net_quote,
         b.lp_fee, b.protocol_fee, b.coin_creator_fee, b.base_amount_out AS base_amount
  FROM pumpdotfun_solana.pump_amm_evt_buyevent b JOIN pools p ON p.pool = b.pool
  WHERE b.evt_block_date = DATE '2025-06-10'
  UNION ALL
  SELECT e.evt_tx_id, e.evt_block_slot, e.evt_block_time, e.evt_tx_index, e.pool, p.mint, 'sell', e.user,
         e.quote_amount_out, e.user_quote_amount_out, e.lp_fee, e.protocol_fee, e.coin_creator_fee, e.base_amount_in
  FROM pumpdotfun_solana.pump_amm_evt_sellevent e JOIN pools p ON p.pool = e.pool
  WHERE e.evt_block_date = DATE '2025-06-10')
SELECT signature, slot, block_timestamp, tx_index,
       'pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA' AS program, pool, mint, side, wallet,
       gross_quote, net_quote, lp_fee, protocol_fee, coin_creator_fee,
       CASE WHEN block_timestamp < TIMESTAMP '2026-07-21 00:00:00' THEN 'pre_boost' ELSE 'post_boost' END AS era,
       'P' AS stratum, 'So11111111111111111111111111111111111111112' AS quote_mint,
       CAST(gross_quote AS double) - CAST(net_quote AS double) AS trader_cost,
       CAST(gross_quote AS double) - CAST(net_quote AS double)
         - CAST(lp_fee AS double) - CAST(protocol_fee AS double) - COALESCE(CAST(coin_creator_fee AS double), 0) AS residual,
       abs(CAST(gross_quote AS double) - CAST(net_quote AS double)
         - CAST(lp_fee AS double) - CAST(protocol_fee AS double) - COALESCE(CAST(coin_creator_fee AS double), 0)) <= 1 AS cost_identity_ok,
       base_amount,
       CAST(block_timestamp AS date) BETWEEN DATE '2025-08-05' AND DATE '2025-08-11' AS degraded,
       CAST(NULL AS boolean) AS birth_is_proxy
FROM s