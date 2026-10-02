-- MR-14 order 2 follow-up: does the decoded pool-creation event carry the mayhem flag, and is it filled?
-- The virtual quote reserve is 0 on every sampled post-BOOST pool in mayhem mode and non-zero on every other.
-- One row, to read the column names (the event carries no token metadata).
SELECT * FROM pumpdotfun_solana.pump_amm_evt_createpoolevent
WHERE evt_block_date = DATE '2026-09-01' LIMIT 1
