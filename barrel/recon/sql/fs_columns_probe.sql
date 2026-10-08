-- Column names of the raw instruction table, zero rows. The fee-share queries read tx_success, tx_index,
-- outer_instruction_index and inner_instruction_index, and none of the four has been read from this table here.
-- No wallet in the output: the names are in the result's metadata (column_names), not in rows.
SELECT * FROM solana.instruction_calls WHERE block_date = DATE '2026-09-01' LIMIT 0