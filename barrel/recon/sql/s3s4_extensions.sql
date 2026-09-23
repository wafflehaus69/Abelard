-- S3/S4 as-of: Token-2022 extension initialisation calls on ten mints -- five P graduates
-- (sample) and five Token-2022 base mints from stratum-N CP-swap inits on 2026-09-01.
-- Extensions are set at mint initialisation and cannot be removed, so as-of entry == as-of now.
WITH p_mints AS (SELECT mint FROM (VALUES ('GfYX7XWmhcsF1PdoL6LZq5JuBmueK7bW4nwxtisDFGW8'),('FBmPBhgQvzaEvcdWDThNCnP7j83hu9cFtGW3tDj6pump'),
  ('AjdE84dGqhbqDYhh8Pm8oCRMCTpUGQ4oDaRBF5vwpump'),('AaTwXAnMckAhL4ucT8i5GQpdN5MWCGuBzwkeNzBpump'),('7C5mqYVj5P1kXuBvdw7yXjAdb1TWjw8xEVuWZn41pump')) t(mint)),
n_mints AS (
  SELECT base_mint AS mint FROM (
    SELECT CASE WHEN account_token0Mint = 'So11111111111111111111111111111111111111112' THEN account_token1Mint ELSE account_token0Mint END AS base_mint,
           CASE WHEN account_token0Mint = 'So11111111111111111111111111111111111111112' THEN account_token1Program ELSE account_token0Program END AS prog,
           call_block_slot AS s
    FROM raydium_cp_solana.raydium_cp_swap_call_initialize WHERE call_block_date = DATE '2026-09-01'
      AND call_tx_signer <> '39azUYFWPz3VHgKCf3VChUwbpURdCHRxjWVowf5jUJjg')
  WHERE prog = 'TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb' ORDER BY s LIMIT 5),
m AS (SELECT mint, 'P' AS stratum FROM p_mints UNION ALL SELECT mint, 'N' FROM n_mints),
ext AS (
  SELECT account_mint AS mint, 'transfer_fee' AS ext, call_block_slot AS slot FROM spl_token_2022_solana.spl_token_2022_call_transferfeeextension WHERE call_block_date BETWEEN DATE '2026-06-01' AND DATE '2026-09-02'
  UNION ALL SELECT account_mint, 'transfer_hook', call_block_slot FROM spl_token_2022_solana.spl_token_2022_call_transferhookextension WHERE call_block_date BETWEEN DATE '2026-06-01' AND DATE '2026-09-02'
  UNION ALL SELECT account_mint, 'permanent_delegate', call_block_slot FROM spl_token_2022_solana.spl_token_2022_call_initializepermanentdelegate WHERE call_block_date BETWEEN DATE '2026-06-01' AND DATE '2026-09-02'
  UNION ALL SELECT account_mint, 'non_transferable', call_block_slot FROM spl_token_2022_solana.spl_token_2022_call_initializenontransferablemint WHERE call_block_date BETWEEN DATE '2026-06-01' AND DATE '2026-09-02'
  UNION ALL SELECT account_mint, 'confidential_transfer', call_block_slot FROM spl_token_2022_solana.spl_token_2022_call_confidentialtransferextension WHERE call_block_date BETWEEN DATE '2026-06-01' AND DATE '2026-09-02')
SELECT m.stratum, m.mint, array_join(array_agg(DISTINCT e.ext ORDER BY e.ext), ',') AS extensions_found
FROM m LEFT JOIN ext e ON e.mint = m.mint GROUP BY 1, 2 ORDER BY 1, 2
