-- pump fee program calls on 2026-09-01 by 8-byte discriminator (hex), to size the rare
-- sharing-config instructions against the get_fees flood.
SELECT to_hex(substr(data, 1, 8)) AS disc_hex, COUNT(*) AS calls, COUNT_IF(is_inner) AS inner_calls, COUNT(DISTINCT tx_id) AS txs
FROM solana.instruction_calls
WHERE block_date = DATE '2026-09-01' AND executing_account = 'pfeeUxB6jkeY1Hxd7CsFCAjcbHA9rWtchMGdZ6VojVZ'
GROUP BY 1 ORDER BY calls DESC
