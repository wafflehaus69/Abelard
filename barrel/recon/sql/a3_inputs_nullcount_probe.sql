-- E35 probe for a3_seta: are the input columns filled in the era the query touches (June 2025)?
-- One hour of each large table, literal bounds. Counts only.
SELECT 'tradeevent' AS src, count(*) AS n,
       count("user") AS c1, count(is_buy) AS c2, count(evt_block_slot) AS c3, count(mint) AS c4
FROM pumpdotfun_solana.pump_evt_tradeevent
WHERE evt_block_date = DATE '2025-06-09'
  AND evt_block_time >= TIMESTAMP '2025-06-09 12:00:00' AND evt_block_time < TIMESTAMP '2025-06-09 13:00:00'
UNION ALL
SELECT 'sol_transfers', count(*), count(from_owner), count(to_owner), count(amount), count_if(CAST(amount AS double) >= 1e6)
FROM tokens_solana.sol_transfers
WHERE block_time >= TIMESTAMP '2025-06-09 12:00:00' AND block_time < TIMESTAMP '2025-06-09 13:00:00'
UNION ALL
SELECT 'transfers', count(*), count(from_owner), count(to_owner), count(amount), count(token_mint_address)
FROM tokens_solana.transfers
WHERE block_date = DATE '2025-06-09'
  AND block_time >= TIMESTAMP '2025-06-09 12:00:00' AND block_time < TIMESTAMP '2025-06-09 13:00:00'
