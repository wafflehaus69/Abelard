-- Which pump-program instructions, by name, ran on 2026-09-01 among the migration-related
-- arg-less candidates from the pinned IDL. Reconciles 1,209 decoded migration events vs
-- 667 `migrate` calls. Arg-less instructions have a fixed 8-byte data payload, so equality works.
SELECT CASE WHEN data = from_base58('T5bZvAk4s5f') THEN 'migrate' WHEN data = from_base58('FdiX4bwE4K1') THEN 'migrate_bonding_curve_creator' WHEN data = from_base58('YQq8B6nbicx') THEN 'migrate_v2' END AS instruction, COUNT(*) AS calls, COUNT_IF(is_inner) AS inner_calls, COUNT(DISTINCT tx_id) AS txs
FROM solana.instruction_calls
WHERE block_date = DATE '2026-09-01'
  AND executing_account = '6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P'
  AND data IN (from_base58('T5bZvAk4s5f'), from_base58('FdiX4bwE4K1'), from_base58('YQq8B6nbicx'))
GROUP BY 1 ORDER BY calls DESC