-- quote_mint values on the decoded migration event, 2026-09-01. 0/1209 were SOL-quoted,
-- which contradicts the swap data (82% SOL-quoted pools); this shows what the column holds.
SELECT quote_mint, COUNT(*) AS migrations, MIN(evt_block_time) AS first_seen, MAX(evt_block_time) AS last_seen
FROM pumpdotfun_solana.pump_evt_completepumpammmigrationevent
WHERE evt_block_date = DATE '2026-09-01'
GROUP BY 1 ORDER BY migrations DESC LIMIT 10