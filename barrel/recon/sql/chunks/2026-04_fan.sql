-- Funder fan-out, calendar month of the chunk 2026-04-01 .. 2026-04-30: SOL transfers 2026-04-01 .. 2026-05-01 (exclusive), 30 day(s).
-- Rows name wallets: run with --private-rows or --export. The senders reported are the chunk's own
-- funders, substituted at run time (--funders-from). No threshold, no class, no verdict here.
SELECT s.from_owner AS funder,
       approx_distinct(s.to_owner) AS recipients,
       count(*) AS n_transfers,
       count(DISTINCT CAST(s.block_time AS date)) AS active_days,
       30 AS window_days
FROM tokens_solana.sol_transfers s
WHERE s.block_time >= TIMESTAMP '2026-04-01 00:00:00' AND s.block_time < TIMESTAMP '2026-05-01 00:00:00'
  AND CAST(s.amount AS double) >= 1e6
  AND s.to_owner <> s.from_owner
  AND s.from_owner IN (__FUNDERS__)
GROUP BY 1