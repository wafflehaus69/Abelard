-- Funder fan-out, one-day proving run 2026-09-01: SOL transfers 2026-09-01 .. 2026-09-02 (exclusive), 1 day(s).
-- Rows name wallets: run with --private-rows or --export. The senders reported are the chunk's own
-- funders, substituted at run time (--funders-from). No threshold, no class, no verdict here.
SELECT s.from_owner AS funder,
       approx_distinct(s.to_owner) AS recipients,
       count(*) AS n_transfers,
       count(DISTINCT CAST(s.block_time AS date)) AS active_days,
       1 AS window_days
FROM tokens_solana.sol_transfers s
WHERE s.block_time >= TIMESTAMP '2026-09-01 00:00:00' AND s.block_time < TIMESTAMP '2026-09-02 00:00:00'
  AND CAST(s.amount AS double) >= 1e6
  AND s.to_owner <> s.from_owner
  AND s.from_owner IN (__FUNDERS__)
GROUP BY 1