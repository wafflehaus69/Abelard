-- S7b extraction, one day: pump fee program sharing-config calls (create / update / update_v2)
-- with raw data for Borsh decode per token in Python. Discriminators from the pinned IDL.
SELECT block_slot, block_time, tx_id, tx_signer, to_hex(substr(data, 1, 8)) AS disc, to_hex(data) AS data_hex, account_arguments
FROM solana.instruction_calls
WHERE block_date = DATE '2026-09-01' AND executing_account = 'pfeeUxB6jkeY1Hxd7CsFCAjcbHA9rWtchMGdZ6VojVZ'
  AND to_hex(substr(data, 1, 8)) IN ('C34E564C6F34FBD5', '6FFB31064E4E6A12', 'BD0D8863BBA4ED23')
LIMIT 20
