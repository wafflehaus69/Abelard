-- MR-11 order 1: recalibrate the exchange fan-out discriminant on Solana SOL transfers.
-- Window = the B1a week scan (2025-06-05 .. 2025-06-16), transfers >= 0.001 SOL.
-- Fan-out = distinct recipients per sender. Histogram by power-of-two bucket, split by whether
-- the sender is a labelled exchange wallet (cex_solana.addresses). Counts only; no threshold here.
WITH fan AS (
  SELECT s.from_owner AS a, approx_distinct(s.to_owner) AS fan_out
  FROM tokens_solana.sol_transfers s
  WHERE s.block_time >= TIMESTAMP '2025-06-05 00:00:00' AND s.block_time < TIMESTAMP '2025-06-17 00:00:00'
    AND CAST(s.amount AS double) >= 1e6
  GROUP BY 1),
lab AS (SELECT DISTINCT address FROM cex_solana.addresses)
SELECT CAST(floor(log2(greatest(f.fan_out, 1))) AS integer) AS log2_bucket,
       count(*) AS senders,
       count(l.address) AS labelled_cex,
       min(f.fan_out) AS lo, max(f.fan_out) AS hi
FROM fan f LEFT JOIN lab l ON l.address = f.a
GROUP BY 1 ORDER BY 1
