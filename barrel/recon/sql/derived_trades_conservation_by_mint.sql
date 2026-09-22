-- Are conservation failures concentrated on specific mints (cashback-enabled coins, whose
-- rebate field is absent from Dune's pinned layout)? Per-mint failure rate, one day, P only.
WITH grads AS (
  SELECT mint, pool FROM pumpdotfun_solana.pump_evt_completepumpammmigrationevent
  WHERE evt_block_date BETWEEN DATE '2025-03-20' AND DATE '2026-09-01' AND quote_mint = '11111111111111111111111111111111'),
t AS (
  SELECT g.mint, abs(CAST(user_quote_amount_in AS double) - CAST(quote_amount_in AS double)) - CAST(lp_fee + protocol_fee + coin_creator_fee AS double) AS resid
  FROM pumpdotfun_solana.pump_amm_evt_buyevent b JOIN grads g ON g.pool = b.pool WHERE b.evt_block_date = DATE '2026-09-01'
  UNION ALL
  SELECT g.mint, CAST(quote_amount_out AS double) - CAST(user_quote_amount_out AS double) - CAST(lp_fee + protocol_fee + coin_creator_fee AS double)
  FROM pumpdotfun_solana.pump_amm_evt_sellevent s JOIN grads g ON g.pool = s.pool WHERE s.evt_block_date = DATE '2026-09-01'),
m AS (SELECT mint, COUNT(*) AS n, COUNT_IF(abs(resid) > 1) AS fails, approx_percentile(resid, 0.5) AS resid_p50 FROM t GROUP BY 1)
SELECT CASE WHEN fails = 0 THEN 'mint: 0% fail' WHEN fails = n THEN 'mint: 100% fail' WHEN fails * 1.0 / n > 0.5 THEN 'mint: >50% fail' ELSE 'mint: <=50% fail' END AS bucket,
       COUNT(*) AS mints, SUM(n) AS rows, SUM(fails) AS fails, approx_percentile(resid_p50, 0.5) AS typical_resid_lamports
FROM m GROUP BY 1 ORDER BY rows DESC
