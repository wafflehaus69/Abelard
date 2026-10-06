-- E35 probe before the MR-16 run: are tx_id, tx_index and block_slot filled on both transfer tables,
-- post-BOOST (2026-09), pre-BOOST after the fee change (2026-03) and the calibration era (2025-06)? One hour each.
-- Counts only. Cost on the trial: 12.5 credits for two eras, shown in full at the first status read; cap 30.
SELECT 'sol_transfers 2026-09-01' AS src, count(*) AS n, count(tx_id) AS tx_id, count(tx_index) AS tx_index, count(block_slot) AS block_slot
FROM tokens_solana.sol_transfers
WHERE block_time >= TIMESTAMP '2026-09-01 12:00:00' AND block_time < TIMESTAMP '2026-09-01 13:00:00'
UNION ALL
SELECT 'sol_transfers 2025-06-09', count(*), count(tx_id), count(tx_index), count(block_slot)
FROM tokens_solana.sol_transfers
WHERE block_time >= TIMESTAMP '2025-06-09 12:00:00' AND block_time < TIMESTAMP '2025-06-09 13:00:00'
UNION ALL
SELECT 'transfers 2026-09-01', count(*), count(tx_id), count(tx_index), count(block_slot)
FROM tokens_solana.transfers
WHERE block_date = DATE '2026-09-01' AND block_time >= TIMESTAMP '2026-09-01 12:00:00' AND block_time < TIMESTAMP '2026-09-01 13:00:00'
UNION ALL
SELECT 'transfers 2025-06-09', count(*), count(tx_id), count(tx_index), count(block_slot)
FROM tokens_solana.transfers
WHERE block_date = DATE '2025-06-09' AND block_time >= TIMESTAMP '2025-06-09 12:00:00' AND block_time < TIMESTAMP '2025-06-09 13:00:00'
UNION ALL
SELECT 'sol_transfers 2026-03-10', count(*), count(tx_id), count(tx_index), count(block_slot)
FROM tokens_solana.sol_transfers
WHERE block_time >= TIMESTAMP '2026-03-10 12:00:00' AND block_time < TIMESTAMP '2026-03-10 13:00:00'
UNION ALL
SELECT 'transfers 2026-03-10', count(*), count(tx_id), count(tx_index), count(block_slot)
FROM tokens_solana.transfers
WHERE block_date = DATE '2026-03-10' AND block_time >= TIMESTAMP '2026-03-10 12:00:00' AND block_time < TIMESTAMP '2026-03-10 13:00:00'
