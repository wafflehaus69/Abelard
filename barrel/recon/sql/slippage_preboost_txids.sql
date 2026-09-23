-- Five pre-BOOST buys on SOL-quoted graduate pools (2026-06-01) for chain-side decode.
WITH grads AS (SELECT pool FROM pumpdotfun_solana.pump_evt_completepumpammmigrationevent WHERE evt_block_date BETWEEN DATE '2025-03-20' AND DATE '2026-06-01' AND quote_mint = '11111111111111111111111111111111')
SELECT evt_tx_id, pool, evt_block_slot, CAST(quote_amount_in AS varchar) AS q_in, CAST(base_amount_out AS varchar) AS base_out,
       CAST(pool_base_token_reserves AS varchar) AS base_r, CAST(pool_quote_token_reserves AS varchar) AS quote_r
FROM pumpdotfun_solana.pump_amm_evt_buyevent
WHERE evt_block_date = DATE '2026-06-01' AND pool IN (SELECT pool FROM grads) AND base_amount_out > 0 AND quote_amount_in > 1e7
ORDER BY evt_block_slot LIMIT 5
