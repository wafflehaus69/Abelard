-- S7b feasibility: pump fee program (pfeeUxB6...) is NOT decoded on Dune. Raw calls on one day,
-- by first data byte / 8-byte discriminator presence, and whether its txs carry Program data logs.
SELECT COUNT(*) AS pfee_calls, COUNT(DISTINCT tx_id) AS txs, COUNT_IF(is_inner) AS inner_calls,
       COUNT(DISTINCT substr(data, 1, 8)) AS distinct_discriminators
FROM solana.instruction_calls
WHERE block_date = DATE '2026-09-01' AND executing_account = 'pfeeUxB6jkeY1Hxd7CsFCAjcbHA9rWtchMGdZ6VojVZ'
