-- Fee-share, evidence per era (E38): the fee program's calls by kind on one day per chunk, 19 single-day counts.
-- Counts only; no wallet in the output.
-- It decides which chunks have anything to extract: until it has run, an empty result of query 1 is not a
-- result. An instruction is named by its 8-byte discriminator, an event by its 16-byte prefix (wrapper tag
-- E445A52E51CB9A1D, then the event discriminator), so a kind the pinned IDL lacks in some era shows up under
-- its own hex. The three events query 1 takes are, in order (made, changed, reset):
--   E445A52E51CB9A1D8569AAC8B874FB58 E445A52E51CB9A1D15BAC4B85BE4E1CB E445A52E51CB9A1DCBCC97E27837D6F3
-- The day is the last one query 1 scans for the chunk (its last graduation day + 2).
-- Each day also counts the pump program's calls: a day with no fee-program row is then a day the table had
-- rows on, and the four index and flag columns get a fill count in every era, including the ones before the
-- fee program existed.
SELECT '2025-03' AS chunk, '2025-04-02' AS day,
       CASE WHEN executing_account = '6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P' THEN 'control: pump program, all calls'
            WHEN to_hex(substr(data, 1, 8)) = 'E445A52E51CB9A1D' THEN to_hex(substr(data, 1, 16))
            ELSE to_hex(substr(data, 1, 8)) END AS kind,
       count(*) AS n, count_if(is_inner) AS n_inner, count(tx_success) AS tx_success_filled, count_if(tx_success) AS n_success,
       count(tx_index) AS tx_index_filled, count(outer_instruction_index) AS outer_ix_filled, count(inner_instruction_index) AS inner_ix_filled,
       min(length(data)) AS len_min, max(length(data)) AS len_max
FROM solana.instruction_calls
WHERE block_date = DATE '2025-04-02'
  AND executing_account IN ('pfeeUxB6jkeY1Hxd7CsFCAjcbHA9rWtchMGdZ6VojVZ', '6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P')
GROUP BY 3
UNION ALL
SELECT '2025-04', '2025-05-02',
       CASE WHEN executing_account = '6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P' THEN 'control: pump program, all calls'
            WHEN to_hex(substr(data, 1, 8)) = 'E445A52E51CB9A1D' THEN to_hex(substr(data, 1, 16))
            ELSE to_hex(substr(data, 1, 8)) END,
       count(*), count_if(is_inner), count(tx_success), count_if(tx_success),
       count(tx_index), count(outer_instruction_index), count(inner_instruction_index),
       min(length(data)), max(length(data))
FROM solana.instruction_calls
WHERE block_date = DATE '2025-05-02'
  AND executing_account IN ('pfeeUxB6jkeY1Hxd7CsFCAjcbHA9rWtchMGdZ6VojVZ', '6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P')
GROUP BY 3
UNION ALL
SELECT '2025-05', '2025-06-02',
       CASE WHEN executing_account = '6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P' THEN 'control: pump program, all calls'
            WHEN to_hex(substr(data, 1, 8)) = 'E445A52E51CB9A1D' THEN to_hex(substr(data, 1, 16))
            ELSE to_hex(substr(data, 1, 8)) END,
       count(*), count_if(is_inner), count(tx_success), count_if(tx_success),
       count(tx_index), count(outer_instruction_index), count(inner_instruction_index),
       min(length(data)), max(length(data))
FROM solana.instruction_calls
WHERE block_date = DATE '2025-06-02'
  AND executing_account IN ('pfeeUxB6jkeY1Hxd7CsFCAjcbHA9rWtchMGdZ6VojVZ', '6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P')
GROUP BY 3
UNION ALL
SELECT '2025-06', '2025-07-02',
       CASE WHEN executing_account = '6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P' THEN 'control: pump program, all calls'
            WHEN to_hex(substr(data, 1, 8)) = 'E445A52E51CB9A1D' THEN to_hex(substr(data, 1, 16))
            ELSE to_hex(substr(data, 1, 8)) END,
       count(*), count_if(is_inner), count(tx_success), count_if(tx_success),
       count(tx_index), count(outer_instruction_index), count(inner_instruction_index),
       min(length(data)), max(length(data))
FROM solana.instruction_calls
WHERE block_date = DATE '2025-07-02'
  AND executing_account IN ('pfeeUxB6jkeY1Hxd7CsFCAjcbHA9rWtchMGdZ6VojVZ', '6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P')
GROUP BY 3
UNION ALL
SELECT '2025-07', '2025-08-02',
       CASE WHEN executing_account = '6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P' THEN 'control: pump program, all calls'
            WHEN to_hex(substr(data, 1, 8)) = 'E445A52E51CB9A1D' THEN to_hex(substr(data, 1, 16))
            ELSE to_hex(substr(data, 1, 8)) END,
       count(*), count_if(is_inner), count(tx_success), count_if(tx_success),
       count(tx_index), count(outer_instruction_index), count(inner_instruction_index),
       min(length(data)), max(length(data))
FROM solana.instruction_calls
WHERE block_date = DATE '2025-08-02'
  AND executing_account IN ('pfeeUxB6jkeY1Hxd7CsFCAjcbHA9rWtchMGdZ6VojVZ', '6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P')
GROUP BY 3
UNION ALL
SELECT '2025-08', '2025-09-02',
       CASE WHEN executing_account = '6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P' THEN 'control: pump program, all calls'
            WHEN to_hex(substr(data, 1, 8)) = 'E445A52E51CB9A1D' THEN to_hex(substr(data, 1, 16))
            ELSE to_hex(substr(data, 1, 8)) END,
       count(*), count_if(is_inner), count(tx_success), count_if(tx_success),
       count(tx_index), count(outer_instruction_index), count(inner_instruction_index),
       min(length(data)), max(length(data))
FROM solana.instruction_calls
WHERE block_date = DATE '2025-09-02'
  AND executing_account IN ('pfeeUxB6jkeY1Hxd7CsFCAjcbHA9rWtchMGdZ6VojVZ', '6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P')
GROUP BY 3
UNION ALL
SELECT '2025-09', '2025-10-02',
       CASE WHEN executing_account = '6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P' THEN 'control: pump program, all calls'
            WHEN to_hex(substr(data, 1, 8)) = 'E445A52E51CB9A1D' THEN to_hex(substr(data, 1, 16))
            ELSE to_hex(substr(data, 1, 8)) END,
       count(*), count_if(is_inner), count(tx_success), count_if(tx_success),
       count(tx_index), count(outer_instruction_index), count(inner_instruction_index),
       min(length(data)), max(length(data))
FROM solana.instruction_calls
WHERE block_date = DATE '2025-10-02'
  AND executing_account IN ('pfeeUxB6jkeY1Hxd7CsFCAjcbHA9rWtchMGdZ6VojVZ', '6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P')
GROUP BY 3
UNION ALL
SELECT '2025-10', '2025-11-02',
       CASE WHEN executing_account = '6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P' THEN 'control: pump program, all calls'
            WHEN to_hex(substr(data, 1, 8)) = 'E445A52E51CB9A1D' THEN to_hex(substr(data, 1, 16))
            ELSE to_hex(substr(data, 1, 8)) END,
       count(*), count_if(is_inner), count(tx_success), count_if(tx_success),
       count(tx_index), count(outer_instruction_index), count(inner_instruction_index),
       min(length(data)), max(length(data))
FROM solana.instruction_calls
WHERE block_date = DATE '2025-11-02'
  AND executing_account IN ('pfeeUxB6jkeY1Hxd7CsFCAjcbHA9rWtchMGdZ6VojVZ', '6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P')
GROUP BY 3
UNION ALL
SELECT '2025-11', '2025-12-02',
       CASE WHEN executing_account = '6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P' THEN 'control: pump program, all calls'
            WHEN to_hex(substr(data, 1, 8)) = 'E445A52E51CB9A1D' THEN to_hex(substr(data, 1, 16))
            ELSE to_hex(substr(data, 1, 8)) END,
       count(*), count_if(is_inner), count(tx_success), count_if(tx_success),
       count(tx_index), count(outer_instruction_index), count(inner_instruction_index),
       min(length(data)), max(length(data))
FROM solana.instruction_calls
WHERE block_date = DATE '2025-12-02'
  AND executing_account IN ('pfeeUxB6jkeY1Hxd7CsFCAjcbHA9rWtchMGdZ6VojVZ', '6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P')
GROUP BY 3
UNION ALL
SELECT '2025-12', '2026-01-02',
       CASE WHEN executing_account = '6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P' THEN 'control: pump program, all calls'
            WHEN to_hex(substr(data, 1, 8)) = 'E445A52E51CB9A1D' THEN to_hex(substr(data, 1, 16))
            ELSE to_hex(substr(data, 1, 8)) END,
       count(*), count_if(is_inner), count(tx_success), count_if(tx_success),
       count(tx_index), count(outer_instruction_index), count(inner_instruction_index),
       min(length(data)), max(length(data))
FROM solana.instruction_calls
WHERE block_date = DATE '2026-01-02'
  AND executing_account IN ('pfeeUxB6jkeY1Hxd7CsFCAjcbHA9rWtchMGdZ6VojVZ', '6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P')
GROUP BY 3
UNION ALL
SELECT '2026-01', '2026-02-02',
       CASE WHEN executing_account = '6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P' THEN 'control: pump program, all calls'
            WHEN to_hex(substr(data, 1, 8)) = 'E445A52E51CB9A1D' THEN to_hex(substr(data, 1, 16))
            ELSE to_hex(substr(data, 1, 8)) END,
       count(*), count_if(is_inner), count(tx_success), count_if(tx_success),
       count(tx_index), count(outer_instruction_index), count(inner_instruction_index),
       min(length(data)), max(length(data))
FROM solana.instruction_calls
WHERE block_date = DATE '2026-02-02'
  AND executing_account IN ('pfeeUxB6jkeY1Hxd7CsFCAjcbHA9rWtchMGdZ6VojVZ', '6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P')
GROUP BY 3
UNION ALL
SELECT '2026-02', '2026-03-02',
       CASE WHEN executing_account = '6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P' THEN 'control: pump program, all calls'
            WHEN to_hex(substr(data, 1, 8)) = 'E445A52E51CB9A1D' THEN to_hex(substr(data, 1, 16))
            ELSE to_hex(substr(data, 1, 8)) END,
       count(*), count_if(is_inner), count(tx_success), count_if(tx_success),
       count(tx_index), count(outer_instruction_index), count(inner_instruction_index),
       min(length(data)), max(length(data))
FROM solana.instruction_calls
WHERE block_date = DATE '2026-03-02'
  AND executing_account IN ('pfeeUxB6jkeY1Hxd7CsFCAjcbHA9rWtchMGdZ6VojVZ', '6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P')
GROUP BY 3
UNION ALL
SELECT '2026-03', '2026-04-02',
       CASE WHEN executing_account = '6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P' THEN 'control: pump program, all calls'
            WHEN to_hex(substr(data, 1, 8)) = 'E445A52E51CB9A1D' THEN to_hex(substr(data, 1, 16))
            ELSE to_hex(substr(data, 1, 8)) END,
       count(*), count_if(is_inner), count(tx_success), count_if(tx_success),
       count(tx_index), count(outer_instruction_index), count(inner_instruction_index),
       min(length(data)), max(length(data))
FROM solana.instruction_calls
WHERE block_date = DATE '2026-04-02'
  AND executing_account IN ('pfeeUxB6jkeY1Hxd7CsFCAjcbHA9rWtchMGdZ6VojVZ', '6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P')
GROUP BY 3
UNION ALL
SELECT '2026-04', '2026-05-02',
       CASE WHEN executing_account = '6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P' THEN 'control: pump program, all calls'
            WHEN to_hex(substr(data, 1, 8)) = 'E445A52E51CB9A1D' THEN to_hex(substr(data, 1, 16))
            ELSE to_hex(substr(data, 1, 8)) END,
       count(*), count_if(is_inner), count(tx_success), count_if(tx_success),
       count(tx_index), count(outer_instruction_index), count(inner_instruction_index),
       min(length(data)), max(length(data))
FROM solana.instruction_calls
WHERE block_date = DATE '2026-05-02'
  AND executing_account IN ('pfeeUxB6jkeY1Hxd7CsFCAjcbHA9rWtchMGdZ6VojVZ', '6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P')
GROUP BY 3
UNION ALL
SELECT '2026-05', '2026-06-02',
       CASE WHEN executing_account = '6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P' THEN 'control: pump program, all calls'
            WHEN to_hex(substr(data, 1, 8)) = 'E445A52E51CB9A1D' THEN to_hex(substr(data, 1, 16))
            ELSE to_hex(substr(data, 1, 8)) END,
       count(*), count_if(is_inner), count(tx_success), count_if(tx_success),
       count(tx_index), count(outer_instruction_index), count(inner_instruction_index),
       min(length(data)), max(length(data))
FROM solana.instruction_calls
WHERE block_date = DATE '2026-06-02'
  AND executing_account IN ('pfeeUxB6jkeY1Hxd7CsFCAjcbHA9rWtchMGdZ6VojVZ', '6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P')
GROUP BY 3
UNION ALL
SELECT '2026-06', '2026-07-02',
       CASE WHEN executing_account = '6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P' THEN 'control: pump program, all calls'
            WHEN to_hex(substr(data, 1, 8)) = 'E445A52E51CB9A1D' THEN to_hex(substr(data, 1, 16))
            ELSE to_hex(substr(data, 1, 8)) END,
       count(*), count_if(is_inner), count(tx_success), count_if(tx_success),
       count(tx_index), count(outer_instruction_index), count(inner_instruction_index),
       min(length(data)), max(length(data))
FROM solana.instruction_calls
WHERE block_date = DATE '2026-07-02'
  AND executing_account IN ('pfeeUxB6jkeY1Hxd7CsFCAjcbHA9rWtchMGdZ6VojVZ', '6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P')
GROUP BY 3
UNION ALL
SELECT '2026-07', '2026-08-02',
       CASE WHEN executing_account = '6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P' THEN 'control: pump program, all calls'
            WHEN to_hex(substr(data, 1, 8)) = 'E445A52E51CB9A1D' THEN to_hex(substr(data, 1, 16))
            ELSE to_hex(substr(data, 1, 8)) END,
       count(*), count_if(is_inner), count(tx_success), count_if(tx_success),
       count(tx_index), count(outer_instruction_index), count(inner_instruction_index),
       min(length(data)), max(length(data))
FROM solana.instruction_calls
WHERE block_date = DATE '2026-08-02'
  AND executing_account IN ('pfeeUxB6jkeY1Hxd7CsFCAjcbHA9rWtchMGdZ6VojVZ', '6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P')
GROUP BY 3
UNION ALL
SELECT '2026-08', '2026-09-02',
       CASE WHEN executing_account = '6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P' THEN 'control: pump program, all calls'
            WHEN to_hex(substr(data, 1, 8)) = 'E445A52E51CB9A1D' THEN to_hex(substr(data, 1, 16))
            ELSE to_hex(substr(data, 1, 8)) END,
       count(*), count_if(is_inner), count(tx_success), count_if(tx_success),
       count(tx_index), count(outer_instruction_index), count(inner_instruction_index),
       min(length(data)), max(length(data))
FROM solana.instruction_calls
WHERE block_date = DATE '2026-09-02'
  AND executing_account IN ('pfeeUxB6jkeY1Hxd7CsFCAjcbHA9rWtchMGdZ6VojVZ', '6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P')
GROUP BY 3
UNION ALL
SELECT '2026-09', '2026-09-22',
       CASE WHEN executing_account = '6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P' THEN 'control: pump program, all calls'
            WHEN to_hex(substr(data, 1, 8)) = 'E445A52E51CB9A1D' THEN to_hex(substr(data, 1, 16))
            ELSE to_hex(substr(data, 1, 8)) END,
       count(*), count_if(is_inner), count(tx_success), count_if(tx_success),
       count(tx_index), count(outer_instruction_index), count(inner_instruction_index),
       min(length(data)), max(length(data))
FROM solana.instruction_calls
WHERE block_date = DATE '2026-09-22'
  AND executing_account IN ('pfeeUxB6jkeY1Hxd7CsFCAjcbHA9rWtchMGdZ6VojVZ', '6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P')
GROUP BY 3