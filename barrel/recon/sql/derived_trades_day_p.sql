-- 1.3 derived trade table, stratum P only, ONE DAY, as a plain query (no materialization).
-- Columns exactly per DERIVED_TABLE_SCHEMA.md. Validation: conservation on Dune's own rows,
-- gross - net == lp + protocol + creator, per buy variant (ix_name) and for sells.
WITH grads AS (
  SELECT mint, pool FROM pumpdotfun_solana.pump_evt_completepumpammmigrationevent
  WHERE evt_block_date BETWEEN DATE '2025-03-20' AND DATE '2026-09-01'
    AND quote_mint = '11111111111111111111111111111111'),
buys AS (
  SELECT b.evt_tx_id AS signature, b.evt_block_slot AS slot, b.evt_block_time AS block_timestamp, b.evt_tx_index AS tx_index,
         'pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA' AS program, b.pool, g.mint,
         'So11111111111111111111111111111111111111112' AS quote_mint, 'buy' AS side, b.user AS wallet,
         CASE WHEN b.ix_name = 'buy_exact_quote_in' THEN b.quote_amount_in ELSE b.user_quote_amount_in END AS gross_quote,
         CASE WHEN b.ix_name = 'buy_exact_quote_in' THEN b.user_quote_amount_in ELSE b.quote_amount_in END AS net_quote,
         b.base_amount_out AS base_amount, b.lp_fee, b.protocol_fee, b.coin_creator_fee, b.ix_name
  FROM pumpdotfun_solana.pump_amm_evt_buyevent b JOIN grads g ON g.pool = b.pool
  WHERE b.evt_block_date = DATE '2026-09-01'),
sells AS (
  SELECT s.evt_tx_id, s.evt_block_slot, s.evt_block_time, s.evt_tx_index,
         'pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA', s.pool, g.mint,
         'So11111111111111111111111111111111111111112', 'sell', s.user,
         s.quote_amount_out, s.user_quote_amount_out, s.base_amount_in, s.lp_fee, s.protocol_fee, s.coin_creator_fee, 'sell'
  FROM pumpdotfun_solana.pump_amm_evt_sellevent s JOIN grads g ON g.pool = s.pool
  WHERE s.evt_block_date = DATE '2026-09-01'),
t AS (SELECT * FROM buys UNION ALL SELECT * FROM sells)
SELECT ix_name, side, COUNT(*) AS rows, COUNT(DISTINCT mint) AS mints, COUNT(DISTINCT wallet) AS wallets,
       COUNT_IF(abs(CAST(gross_quote AS double) - CAST(net_quote AS double) - CAST(lp_fee + protocol_fee + coin_creator_fee AS double)) <= 1) AS conservation_ok,
       COUNT_IF(abs(CAST(gross_quote AS double) - CAST(net_quote AS double) - CAST(lp_fee + protocol_fee + coin_creator_fee AS double)) > 1)  AS conservation_fail
FROM t GROUP BY 1, 2 ORDER BY 3 DESC
