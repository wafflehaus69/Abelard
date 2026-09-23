-- S3 via decoded tables that DO carry account_mint: permanent delegate and non-transferable,
-- for five P mints and five Token-2022 stratum-N mints (CP-swap inits 2026-09-01).
WITH p_mints AS (SELECT mint, 'P' AS stratum FROM (VALUES ('GfYX7XWmhcsF1PdoL6LZq5JuBmueK7bW4nwxtisDFGW8'),('FBmPBhgQvzaEvcdWDThNCnP7j83hu9cFtGW3tDj6pump'),
  ('AjdE84dGqhbqDYhh8Pm8oCRMCTpUGQ4oDaRBF5vwpump'),('AaTwXAnMckAhL4ucT8i5GQpdN5MWCGuBzwkeNzBpump'),('7C5mqYVj5P1kXuBvdw7yXjAdb1TWjw8xEVuWZn41pump')) t(mint)),
n_mints AS (
  SELECT base_mint AS mint, 'N' AS stratum FROM (
    SELECT CASE WHEN account_token0Mint = 'So11111111111111111111111111111111111111112' THEN account_token1Mint ELSE account_token0Mint END AS base_mint,
           CASE WHEN account_token0Mint = 'So11111111111111111111111111111111111111112' THEN account_token1Program ELSE account_token0Program END AS prog,
           call_block_slot AS s
    FROM raydium_cp_solana.raydium_cp_swap_call_initialize WHERE call_block_date = DATE '2026-09-01'
      AND call_tx_signer <> '39azUYFWPz3VHgKCf3VChUwbpURdCHRxjWVowf5jUJjg')
  WHERE prog = 'TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb' ORDER BY s LIMIT 5),
m AS (SELECT * FROM p_mints UNION ALL SELECT * FROM n_mints),
pd AS (SELECT account_mint AS mint, delegate FROM spl_token_2022_solana.spl_token_2022_call_initializepermanentdelegate WHERE call_block_date BETWEEN DATE '2026-01-01' AND DATE '2026-09-02'),
nt AS (SELECT account_mint AS mint FROM spl_token_2022_solana.spl_token_2022_call_initializenontransferablemint WHERE call_block_date BETWEEN DATE '2026-01-01' AND DATE '2026-09-02')
SELECT m.stratum, m.mint, arbitrary(pd.delegate) AS permanent_delegate, COUNT(nt.mint) > 0 AS non_transferable
FROM m LEFT JOIN pd ON pd.mint = m.mint LEFT JOIN nt ON nt.mint = m.mint GROUP BY 1, 2 ORDER BY 1, 2
