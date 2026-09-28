-- S6 diagnosis: the reconstruction gave the pool 2x supply. Per-action totals and owner nulls
-- for the test mint, to see how 'mint'/'burn'/'transfer' rows must be combined.
SELECT action, COUNT(*) AS rows, CAST(SUM(CAST(amount AS double)) AS varchar) AS total_amount,
       COUNT_IF(from_owner IS NULL) AS from_owner_null, COUNT_IF(to_owner IS NULL) AS to_owner_null,
       COUNT(DISTINCT to_owner) AS distinct_to
FROM tokens_solana.transfers
WHERE block_date BETWEEN DATE '2026-08-27' AND DATE '2026-09-23' AND token_mint_address = '7C5mqYVj5P1kXuBvdw7yXjAdb1TWjw8xEVuWZn41pump'
GROUP BY 1 ORDER BY 2 DESC
