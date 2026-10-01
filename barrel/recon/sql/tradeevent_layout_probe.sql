-- E35 follow-up: is_buy is NULL on every June-2025 bonding-curve trade. Which columns are filled, per era?
-- 20 minutes per era, literal bounds (the first form, hour() over three whole days, was cancelled at its cap).
SELECT DATE '2025-06-09' AS d, count(*) AS n, count(is_buy) AS is_buy, count(sol_amount) AS sol_amount,
       count(token_amount) AS token_amount, count("user") AS usr, count(evt_block_slot) AS slot,
       count(virtual_sol_reserves) AS vsr, count(ix_name) AS ix_name,
       count_if(ix_name = 'buy') AS ix_buy, count_if(ix_name = 'sell') AS ix_sell
FROM pumpdotfun_solana.pump_evt_tradeevent
WHERE evt_block_date = DATE '2025-06-09'
  AND evt_block_time >= TIMESTAMP '2025-06-09 12:00:00' AND evt_block_time < TIMESTAMP '2025-06-09 12:20:00'
UNION ALL
SELECT DATE '2025-10-15' AS d, count(*) AS n, count(is_buy) AS is_buy, count(sol_amount) AS sol_amount,
       count(token_amount) AS token_amount, count("user") AS usr, count(evt_block_slot) AS slot,
       count(virtual_sol_reserves) AS vsr, count(ix_name) AS ix_name,
       count_if(ix_name = 'buy') AS ix_buy, count_if(ix_name = 'sell') AS ix_sell
FROM pumpdotfun_solana.pump_evt_tradeevent
WHERE evt_block_date = DATE '2025-10-15'
  AND evt_block_time >= TIMESTAMP '2025-10-15 12:00:00' AND evt_block_time < TIMESTAMP '2025-10-15 12:20:00'
UNION ALL
SELECT DATE '2026-09-01' AS d, count(*) AS n, count(is_buy) AS is_buy, count(sol_amount) AS sol_amount,
       count(token_amount) AS token_amount, count("user") AS usr, count(evt_block_slot) AS slot,
       count(virtual_sol_reserves) AS vsr, count(ix_name) AS ix_name,
       count_if(ix_name = 'buy') AS ix_buy, count_if(ix_name = 'sell') AS ix_sell
FROM pumpdotfun_solana.pump_evt_tradeevent
WHERE evt_block_date = DATE '2026-09-01'
  AND evt_block_time >= TIMESTAMP '2026-09-01 12:00:00' AND evt_block_time < TIMESTAMP '2026-09-01 12:20:00'
