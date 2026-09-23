-- S1/S2 differential test candidates (MR-7): stratum-N mints (CP-swap inits, Aug 2026, not by the
-- pump migrator) whose mint or freeze authority was changed AFTER entry (pool init + 240 min
-- = +36,000 slots). Both token programs. Returns up to 8 with the change slot and new authority.
WITH inits AS (
  SELECT CASE WHEN account_token0Mint = 'So11111111111111111111111111111111111111112' THEN account_token1Mint ELSE account_token0Mint END AS mint,
         call_block_slot AS init_slot, call_block_time AS init_time
  FROM raydium_cp_solana.raydium_cp_swap_call_initialize
  WHERE call_block_date BETWEEN DATE '2026-08-01' AND DATE '2026-08-31'
    AND call_tx_signer <> '39azUYFWPz3VHgKCf3VChUwbpURdCHRxjWVowf5jUJjg'
    AND (account_token0Mint = 'So11111111111111111111111111111111111111112' OR account_token1Mint = 'So11111111111111111111111111111111111111112')),
sa AS (
  SELECT 'spl' AS prog, account_owned AS mint, call_block_slot AS slot, CAST(authorityType AS varchar) AS atype, newAuthority AS new_auth, call_tx_id
  FROM spl_token_solana.spl_token_call_setauthority WHERE call_block_date BETWEEN DATE '2026-08-01' AND DATE '2026-09-21'
  UNION ALL
  SELECT 't22', account_mint, call_block_slot, CAST(authorityType AS varchar), newAuthority, call_tx_id
  FROM spl_token_2022_solana.spl_token_2022_call_setauthority WHERE call_block_date BETWEEN DATE '2026-08-01' AND DATE '2026-09-21')
SELECT i.mint, i.init_slot, i.init_time, s.prog, s.slot AS change_slot, s.slot - i.init_slot AS slots_after_init, s.atype, s.new_auth, s.call_tx_id
FROM inits i JOIN sa s ON s.mint = i.mint AND s.slot > i.init_slot + 36000
WHERE s.atype LIKE '%MintTokens%' OR s.atype LIKE '%FreezeAccount%'
ORDER BY s.slot LIMIT 8
