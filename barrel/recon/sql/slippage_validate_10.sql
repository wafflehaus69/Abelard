-- 1.6 slippage validation: for buys on SOL-quoted graduate pools (2026-09-01), reconstruct
-- pre-swap reserves from the event's post-swap reserves, apply constant-product with the LP fee
-- taken on the quote input, and compare predicted base_out to the executed base_amount_out.
-- Fees: lp_fee is charged on quote in; protocol/creator fees leave the pool. Pool receives
-- quote_in_with_lp_fee? -- test both conventions and report which one reproduces execution.
WITH grads AS (SELECT pool FROM pumpdotfun_solana.pump_evt_completepumpammmigrationevent WHERE evt_block_date BETWEEN DATE '2025-03-20' AND DATE '2026-09-01' AND quote_mint = '11111111111111111111111111111111'),
b AS (
  SELECT evt_tx_id, pool, ix_name, CAST(quote_amount_in AS double) AS q_in, CAST(quote_amount_in_with_lp_fee AS double) AS q_in_lp,
         CAST(base_amount_out AS double) AS base_out, CAST(lp_fee AS double) AS lp, CAST(protocol_fee AS double) AS prot, CAST(coin_creator_fee AS double) AS cre,
         CAST(pool_base_token_reserves AS double) AS base_after, CAST(pool_quote_token_reserves AS double) AS quote_after,
         CAST(lp_fee_basis_points AS double) AS lp_bps
  FROM pumpdotfun_solana.pump_amm_evt_buyevent
  WHERE evt_block_date = DATE '2026-09-01' AND pool IN (SELECT pool FROM grads) AND base_amount_out > 0 AND ix_name = 'buy'
  ORDER BY evt_block_slot LIMIT 10),
calc AS (
  SELECT evt_tx_id, pool, q_in, base_out, lp, prot, cre, base_after, quote_after, lp_bps,
         base_after + base_out AS base_before,
         quote_after - q_in AS quote_before_A,          -- convention A: pool received q_in (net of fees)
         quote_after - q_in_lp AS quote_before_B         -- convention B: pool received q_in + lp fee
  FROM b)
SELECT substr(evt_tx_id,1,12) AS tx, q_in/1e9 AS sol_in, base_out,
       -- predicted base out = base_before - k / (quote_before + q_in_eff)
       base_before - (base_before * quote_before_A) / (quote_before_A + q_in) AS pred_A_no_fee,
       base_before - (base_before * quote_before_B) / (quote_before_B + q_in) AS pred_B_net_in,
       base_before - (base_before * quote_before_B) / (quote_before_B + q_in + lp) AS pred_B_gross_in,
       ROUND(100.0 * ((base_before - (base_before * quote_before_B) / (quote_before_B + q_in)) - base_out) / base_out, 4) AS err_pct_B_net_in,
       ROUND(100.0 * ((base_before - (base_before * quote_before_A) / (quote_before_A + q_in)) - base_out) / base_out, 4) AS err_pct_A
FROM calc
