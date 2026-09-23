-- Full mint/freeze authority history for the seven differential-test mints, both programs.
WITH mints AS (SELECT mint FROM (VALUES ('p4UmanYqdg1ctQVueb5PZFJwmTaqwhs5NnmQSk7aRY9'),('2fEvrJjYrZ5BRygThRstCqvyJizejXXEiBNhcUvP24Qr'),
  ('AWdvQXYATMyGCGtqdZptwfREqrxRMVQNiKn5WFDgmB5E'),('ALr4PvU7JBjKJwriCMdx2BPLeBHP4epfTKgX8GmoUxDG'),('8qcRRjnNNdLLVaNrQXCbEiD5ZUDDGfjMc3NjE6znB9bD'),
  ('REDUTAE1gcRYmp8bJVerPu8ALgYFvsmWGbgyS6kJyAD'),('HUvgiKD7orDFaVj6jUxcVVvEYSx6fm6FYbaj7ZZZN21T')) t(mint)),
init AS (
  SELECT 'spl' AS prog, account_mint AS mint, call_block_slot AS slot, 'initializeMint2' AS ev, 'both' AS atype, mintAuthority AS auth, freezeAuthority AS freeze_auth
  FROM spl_token_solana.spl_token_call_initializemint2 WHERE call_block_date BETWEEN DATE '2026-06-01' AND DATE '2026-09-21' AND account_mint IN (SELECT mint FROM mints)
  UNION ALL SELECT 'spl', account_mint, call_block_slot, 'initializeMint', 'both', mintAuthority, freezeAuthority
  FROM spl_token_solana.spl_token_call_initializemint WHERE call_block_date BETWEEN DATE '2026-06-01' AND DATE '2026-09-21' AND account_mint IN (SELECT mint FROM mints)
  UNION ALL SELECT 't22', account_mint, call_block_slot, 'initializeMint2', 'both', mintAuthority, freezeAuthority
  FROM spl_token_2022_solana.spl_token_2022_call_initializemint2 WHERE call_block_date BETWEEN DATE '2026-06-01' AND DATE '2026-09-21' AND account_mint IN (SELECT mint FROM mints)
  UNION ALL SELECT 't22', account_mint, call_block_slot, 'initializeMint', 'both', mintAuthority, freezeAuthority
  FROM spl_token_2022_solana.spl_token_2022_call_initializemint WHERE call_block_date BETWEEN DATE '2026-06-01' AND DATE '2026-09-21' AND account_mint IN (SELECT mint FROM mints)),
sa AS (
  SELECT 'spl' AS prog, account_owned AS mint, call_block_slot AS slot, 'setAuthority' AS ev, CAST(authorityType AS varchar) AS atype, newAuthority AS auth, NULL AS freeze_auth
  FROM spl_token_solana.spl_token_call_setauthority WHERE call_block_date BETWEEN DATE '2026-06-01' AND DATE '2026-09-21' AND account_owned IN (SELECT mint FROM mints)
  UNION ALL SELECT 't22', account_mint, call_block_slot, 'setAuthority', CAST(authorityType AS varchar), newAuthority, NULL
  FROM spl_token_2022_solana.spl_token_2022_call_setauthority WHERE call_block_date BETWEEN DATE '2026-06-01' AND DATE '2026-09-21' AND account_mint IN (SELECT mint FROM mints))
SELECT * FROM (SELECT * FROM init UNION ALL SELECT * FROM sa) ORDER BY mint, slot, ev
