-- Independent count of migrate instruction calls to the pump program on the same
-- day, from Dune's raw instruction table, split outer/inner. Reconciles the
-- decoded event count above (one event per successful migrate).
SELECT COUNT(*)                                    AS migrate_calls,
       COUNT_IF(NOT is_inner)                      AS outer_calls,
       COUNT_IF(is_inner)                          AS inner_calls,
       COUNT(DISTINCT tx_id)                       AS txs
FROM solana.instruction_calls
WHERE block_date = DATE '2026-09-01'
  AND executing_account = '6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P'
  AND data = from_base58('T5bZvAk4s5f')
