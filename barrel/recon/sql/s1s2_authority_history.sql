-- S1/S2 as-of reconstruction: every mint/freeze authority event for five sample mints, from
-- both token programs. As-of state at entry = initial authority from initializeMint2, then
-- the last setAuthority at or before the entry slot (newAuthority NULL = revoked).
WITH mints AS (
  SELECT * FROM (VALUES
    ('GfYX7XWmhcsF1PdoL6LZq5JuBmueK7bW4nwxtisDFGW8', DATE '2026-09-01'),
    ('FBmPBhgQvzaEvcdWDThNCnP7j83hu9cFtGW3tDj6pump', DATE '2026-09-01'),
    ('AjdE84dGqhbqDYhh8Pm8oCRMCTpUGQ4oDaRBF5vwpump', DATE '2026-09-01'),
    ('AaTwXAnMckAhL4ucT8i5GQpdN5MWCGuBzwkeNzBpump', DATE '2026-09-01'),
    ('7C5mqYVj5P1kXuBvdw7yXjAdb1TWjw8xEVuWZn41pump', DATE '2026-08-27')) AS t(mint, create_date)),
init AS (
  SELECT 'spl' AS prog, account_mint AS mint, call_block_slot AS slot, 'initializeMint2' AS ev, 'both' AS authority_type,
         mintAuthority AS mint_auth, freezeAuthority AS freeze_auth, call_tx_id
  FROM spl_token_solana.spl_token_call_initializemint2 WHERE call_block_date BETWEEN DATE '2026-08-27' AND DATE '2026-09-02'
    AND account_mint IN (SELECT mint FROM mints)
  UNION ALL
  SELECT 't22', account_mint, call_block_slot, 'initializeMint2', 'both', mintAuthority, freezeAuthority, call_tx_id
  FROM spl_token_2022_solana.spl_token_2022_call_initializemint2 WHERE call_block_date BETWEEN DATE '2026-08-27' AND DATE '2026-09-02'
    AND account_mint IN (SELECT mint FROM mints)),
setauth AS (
  SELECT 'spl' AS prog, account_owned AS mint, call_block_slot AS slot, 'setAuthority' AS ev, CAST(authorityType AS varchar) AS authority_type,
         newAuthority AS mint_auth, NULL AS freeze_auth, call_tx_id
  FROM spl_token_solana.spl_token_call_setauthority WHERE call_block_date BETWEEN DATE '2026-08-27' AND DATE '2026-09-02'
    AND account_owned IN (SELECT mint FROM mints)
  UNION ALL
  SELECT 't22', account_mint, call_block_slot, 'setAuthority', CAST(authorityType AS varchar), newAuthority, NULL, call_tx_id
  FROM spl_token_2022_solana.spl_token_2022_call_setauthority WHERE call_block_date BETWEEN DATE '2026-08-27' AND DATE '2026-09-02'
    AND account_mint IN (SELECT mint FROM mints))
SELECT prog, mint, slot, ev, authority_type, mint_auth AS new_or_initial_mint_auth, freeze_auth AS initial_freeze_auth, call_tx_id
FROM (SELECT * FROM init UNION ALL SELECT * FROM setauth)
ORDER BY mint, slot, ev
