-- Five pre-BOOST (2026-06-01) and five post-BOOST (2026-09-01) SELLS on SOL-quoted graduate pools, for chain decode.
WITH grads AS (SELECT pool, evt_block_date AS gd FROM pumpdotfun_solana.pump_evt_completepumpammmigrationevent WHERE evt_block_date BETWEEN DATE '2025-03-20' AND DATE '2026-09-01' AND quote_mint = '11111111111111111111111111111111'),
s AS (SELECT evt_block_date AS day, evt_tx_id, pool, row_number() OVER (PARTITION BY evt_block_date ORDER BY evt_block_slot) AS rn
      FROM pumpdotfun_solana.pump_amm_evt_sellevent
      WHERE evt_block_date IN (DATE '2026-06-01', DATE '2026-09-01') AND quote_amount_out > 1e7 AND base_amount_in > 0
        AND pool IN (SELECT pool FROM grads WHERE (gd < DATE '2026-07-21') = (evt_block_date = DATE '2026-06-01') OR evt_block_date = DATE '2026-09-01'))
SELECT day, evt_tx_id, pool FROM s WHERE rn <= 5 ORDER BY day, rn
