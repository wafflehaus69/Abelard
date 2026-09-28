-- S7 on one mint (7C5mqYVj..., created 2026-08-27 04:05 UTC, graduated 2026-09-01 23:59 UTC):
-- deployer-linked set = creator + wallets the creator sent SOL to within +/-24h of creation;
-- their net token balance as-of entry (graduation + 240 min) from the transfer ledger, over chain supply.
WITH cr AS (SELECT creator, evt_block_time AS t0 FROM pumpdotfun_solana.pump_evt_createevent
            WHERE evt_block_date = DATE '2026-08-27' AND mint = '7C5mqYVj5P1kXuBvdw7yXjAdb1TWjw8xEVuWZn41pump'),
funded AS (SELECT DISTINCT s.to_owner AS w FROM tokens_solana.sol_transfers s JOIN cr ON s.from_owner = cr.creator
           WHERE s.block_time BETWEEN TIMESTAMP '2026-08-26 04:00:00' AND TIMESTAMP '2026-08-28 05:00:00' AND CAST(s.amount AS double) >= 1e6),
linked AS (SELECT creator AS w FROM cr UNION SELECT w FROM funded),
t AS (SELECT from_owner, to_owner, CAST(amount AS double) AS amt FROM tokens_solana.transfers
      WHERE block_date BETWEEN DATE '2026-08-27' AND DATE '2026-09-02' AND token_mint_address = '7C5mqYVj5P1kXuBvdw7yXjAdb1TWjw8xEVuWZn41pump'
        AND block_time <= TIMESTAMP '2026-09-02 03:59:03'),   -- entry = graduation 23:59:03 + 240 min
bal AS (SELECT owner, SUM(d) AS balance FROM (SELECT to_owner AS owner, amt AS d FROM t WHERE to_owner IS NOT NULL UNION ALL SELECT from_owner, -amt FROM t WHERE from_owner IS NOT NULL) GROUP BY 1),
supply AS (SELECT SUM(CASE WHEN action = 'mint' THEN CAST(amount AS double) WHEN action = 'burn' THEN -CAST(amount AS double) END) AS s FROM tokens_solana.transfers
           WHERE block_date BETWEEN DATE '2026-08-27' AND DATE '2026-09-02' AND token_mint_address = '7C5mqYVj5P1kXuBvdw7yXjAdb1TWjw8xEVuWZn41pump' AND block_time <= TIMESTAMP '2026-09-02 03:59:03')
SELECT (SELECT COUNT(*) FROM linked) AS linked_wallets,
       COUNT(b.owner) AS linked_with_balance,
       CAST(SUM(b.balance) AS varchar) AS linked_balance, CAST((SELECT s FROM supply) AS varchar) AS supply_at_entry,
       ROUND(100.0 * SUM(b.balance) / (SELECT s FROM supply), 4) AS s7_pct
FROM linked l LEFT JOIN bal b ON b.owner = l.w AND b.balance > 0
