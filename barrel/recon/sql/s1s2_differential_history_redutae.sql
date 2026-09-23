-- Re-pull for the one differential mint whose initializeMint fell outside the June-Sept window:
-- created 2024-11-26 (chain, earliest signature). Window starts at creation.
WITH m AS (SELECT 'REDUTAE1gcRYmp8bJVerPu8ALgYFvsmWGbgyS6kJyAD' AS mint)
SELECT 'spl' AS prog, account_mint AS mint, call_block_slot AS slot, 'initializeMint2' AS ev, 'both' AS atype, mintAuthority AS auth, freezeAuthority AS freeze_auth
FROM spl_token_solana.spl_token_call_initializemint2 WHERE call_block_date BETWEEN DATE '2024-11-26' AND DATE '2024-11-27' AND account_mint = (SELECT mint FROM m)
UNION ALL SELECT 'spl', account_mint, call_block_slot, 'initializeMint', 'both', mintAuthority, freezeAuthority
FROM spl_token_solana.spl_token_call_initializemint WHERE call_block_date BETWEEN DATE '2024-11-26' AND DATE '2024-11-27' AND account_mint = (SELECT mint FROM m)
UNION ALL SELECT 't22', account_mint, call_block_slot, 'initializeMint2', 'both', mintAuthority, freezeAuthority
FROM spl_token_2022_solana.spl_token_2022_call_initializemint2 WHERE call_block_date BETWEEN DATE '2024-11-26' AND DATE '2024-11-27' AND account_mint = (SELECT mint FROM m)
UNION ALL SELECT 't22', account_mint, call_block_slot, 'initializeMint', 'both', mintAuthority, freezeAuthority
FROM spl_token_2022_solana.spl_token_2022_call_initializemint WHERE call_block_date BETWEEN DATE '2024-11-26' AND DATE '2024-11-27' AND account_mint = (SELECT mint FROM m)
UNION ALL SELECT 'spl', account_owned, call_block_slot, 'setAuthority', CAST(authorityType AS varchar), newAuthority, NULL
FROM spl_token_solana.spl_token_call_setauthority WHERE call_block_date BETWEEN DATE '2024-11-26' AND DATE '2026-09-21' AND account_owned = (SELECT mint FROM m)
UNION ALL SELECT 't22', account_mint, call_block_slot, 'setAuthority', CAST(authorityType AS varchar), newAuthority, NULL
FROM spl_token_2022_solana.spl_token_2022_call_setauthority WHERE call_block_date BETWEEN DATE '2024-11-26' AND DATE '2026-09-21' AND account_mint = (SELECT mint FROM m)
ORDER BY slot, ev
