-- Five pre-BOOST (2026-06-01) and five post-BOOST (2026-09-01) sells on SOL-quoted graduate pools.
-- Rewritten after the runaway: partition filter is a plain AND on the outer table, and the
-- graduate subquery carries its own partition filter. No OR on partition columns.
WITH grads AS (
  SELECT pool FROM pumpdotfun_solana.pump_evt_completepumpammmigrationevent
  WHERE evt_block_date BETWEEN DATE '2025-03-20' AND DATE '2026-09-01' AND quote_mint = '11111111111111111111111111111111'),
s AS (
  SELECT evt_block_date AS day, evt_tx_id, pool, row_number() OVER (PARTITION BY evt_block_date ORDER BY evt_block_slot) AS rn
  FROM pumpdotfun_solana.pump_amm_evt_sellevent
  WHERE evt_block_date IN (DATE '2026-06-01', DATE '2026-09-01') AND quote_amount_out > 10000000 AND base_amount_in > 0
    AND pool IN (SELECT pool FROM grads))
SELECT day, evt_tx_id, pool FROM s WHERE rn <= 5 ORDER BY day, rn
