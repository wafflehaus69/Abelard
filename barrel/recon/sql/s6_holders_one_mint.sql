-- S6 holder reconstruction test: full SPL transfer ledger for one P mint (7C5mqYVj..., created
-- 2026-08-27) from creation to now, net balance per owner, top 12. Reconciled against the chain's
-- getTokenLargestAccounts today. Mint/burn rows carry no source/destination owner respectively.
WITH t AS (
  SELECT from_owner, to_owner, CAST(amount AS double) AS amt, action
  FROM tokens_solana.transfers
  WHERE block_date BETWEEN DATE '2026-08-27' AND DATE '2026-09-23' AND token_mint_address = '7C5mqYVj5P1kXuBvdw7yXjAdb1TWjw8xEVuWZn41pump'),
flows AS (
  SELECT to_owner AS owner, amt AS delta FROM t WHERE to_owner IS NOT NULL
  UNION ALL SELECT from_owner, -amt FROM t WHERE from_owner IS NOT NULL),
bal AS (SELECT owner, SUM(delta) AS balance FROM flows GROUP BY 1)
SELECT owner, CAST(balance AS varchar) AS balance, (SELECT COUNT(*) FROM t) AS transfer_rows, (SELECT array_join(array_agg(DISTINCT action), ',') FROM t) AS actions
FROM bal WHERE balance > 0 ORDER BY balance DESC LIMIT 12
