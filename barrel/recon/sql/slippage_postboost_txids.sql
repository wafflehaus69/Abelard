-- Five post-BOOST buys (2026-09-01) on SOL-quoted graduate pools born after BOOST, for chain decode with virtual_quote_reserves.
WITH grads AS (SELECT pool FROM pumpdotfun_solana.pump_evt_completepumpammmigrationevent WHERE evt_block_date BETWEEN DATE '2026-07-22' AND DATE '2026-09-01' AND quote_mint = '11111111111111111111111111111111')
SELECT evt_tx_id, pool FROM pumpdotfun_solana.pump_amm_evt_buyevent
WHERE evt_block_date = DATE '2026-09-01' AND pool IN (SELECT pool FROM grads) AND base_amount_out > 0 AND quote_amount_in > 1e7
ORDER BY evt_block_slot LIMIT 5
