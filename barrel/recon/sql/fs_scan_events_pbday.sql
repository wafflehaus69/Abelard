-- Fee-share proving run 1: the sharing-config events of ONE partition (2026-09-01), every token, counted.
-- Rows name wallets: run with --private-rows. No threshold, no classification, no verdict here.
-- It reads what query 1 reads (the same columns, the whole payload) and nothing wider, so its cost is the unit
-- of query 1 per scanned day. No universe join. One row per (event kind, payload length):
--   n against the instruction counts already held for this day (config made 3,288; shares changed 2,516 + 751;
--   reset 3), which were taken without a success filter, as n is here;
--   how many rows have each column filled that query 1 orders and filters by;
--   one payload per layout, the earliest, for the local decoder: the events have never been fetched.
-- A payload holding an owner wallet is never the sample.
SELECT evt, data_len, count(*) AS n, count(DISTINCT tx_id) AS n_tx, count(DISTINCT mint_bin) AS n_mints,
       count(tx_success) AS tx_success_filled, count_if(tx_success) AS n_success,
       count(is_inner) AS is_inner_filled, count_if(is_inner) AS n_inner,
       count(tx_index) AS tx_index_filled, count(outer_instruction_index) AS outer_ix_filled,
       count(inner_instruction_index) AS inner_ix_filled,
       min(block_time) AS t_min, max(block_time) AS t_max,
       min_by(data_hex, block_slot) FILTER (WHERE __NOT_OWNER_HEX(data_hex)__) AS sample_hex
FROM (
  SELECT block_slot, block_time, tx_index, outer_instruction_index, inner_instruction_index, tx_id, tx_success, is_inner,
         to_hex(substr(data, 9, 8)) AS evt, length(data) AS data_len, substr(data, 25, 32) AS mint_bin,
         to_hex(data) AS data_hex
  FROM solana.instruction_calls
  WHERE block_date = DATE '2026-09-01'
    AND executing_account = 'pfeeUxB6jkeY1Hxd7CsFCAjcbHA9rWtchMGdZ6VojVZ'
    AND to_hex(substr(data, 1, 16)) IN ('E445A52E51CB9A1D8569AAC8B874FB58', 'E445A52E51CB9A1D15BAC4B85BE4E1CB', 'E445A52E51CB9A1DCBCC97E27837D6F3'))
GROUP BY 1, 2