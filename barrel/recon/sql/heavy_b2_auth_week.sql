-- Heavy tier B2: c01-c02 inputs, graduations 2025-06-09 .. 2025-06-15. History facts as-of grad + 240 min.
-- Token-2022 extensions (c03, c04) are not in this query: they use the raw-call method of
-- VALIDATION_1_5_S3_S4.md and apply only to Token-2022 mints.
-- Scope limit as elsewhere: mints initialised within 3 days before graduation; others n_init = 0.
WITH comp AS (
  SELECT mint, evt_block_time AS ct FROM pumpdotfun_solana.pump_evt_completeevent
  WHERE evt_block_date BETWEEN DATE '2025-06-09' AND DATE '2025-06-15'),
pc AS (
  SELECT base_mint AS mint, quote_mint, pool, evt_block_time AS pt,
         row_number() OVER (PARTITION BY base_mint ORDER BY evt_block_time) AS rn
  FROM pumpdotfun_solana.pump_amm_evt_createpoolevent
  WHERE evt_block_date BETWEEN DATE '2025-06-09' AND DATE '2025-06-16'),
u AS (
  SELECT c.mint, p.pt AS grad_time, p.pool
  FROM comp c JOIN pc p ON p.mint = c.mint AND p.rn = 1 AND p.pt >= c.ct AND p.pt < c.ct + INTERVAL '1' DAY
  WHERE p.quote_mint = 'So11111111111111111111111111111111111111112'),
init AS (
  SELECT 'spl' AS prog, account_mint AS mint, mintAuthority AS ma, freezeAuthority AS fa
  FROM spl_token_solana.spl_token_call_initializemint2 WHERE call_block_date BETWEEN DATE '2025-06-06' AND DATE '2025-06-16'
  UNION ALL
  SELECT 't22', account_mint, mintAuthority, freezeAuthority
  FROM spl_token_2022_solana.spl_token_2022_call_initializemint2 WHERE call_block_date BETWEEN DATE '2025-06-06' AND DATE '2025-06-16'
  UNION ALL
  SELECT 'spl', account_mint, mintAuthority, freezeAuthority
  FROM spl_token_solana.spl_token_call_initializemint WHERE call_block_date BETWEEN DATE '2025-06-06' AND DATE '2025-06-16'
  UNION ALL
  SELECT 't22', account_mint, mintAuthority, freezeAuthority
  FROM spl_token_2022_solana.spl_token_2022_call_initializemint WHERE call_block_date BETWEEN DATE '2025-06-06' AND DATE '2025-06-16'),
setauth AS (
  SELECT account_owned AS mint, CAST(authorityType AS varchar) AS atype, newAuthority AS na, call_block_time AS t, call_block_slot AS slot
  FROM spl_token_solana.spl_token_call_setauthority WHERE call_block_date BETWEEN DATE '2025-06-06' AND DATE '2025-06-16'
  UNION ALL
  SELECT account_mint, CAST(authorityType AS varchar), newAuthority, call_block_time, call_block_slot
  FROM spl_token_2022_solana.spl_token_2022_call_setauthority WHERE call_block_date BETWEEN DATE '2025-06-06' AND DATE '2025-06-16'),
i AS (SELECT u.mint, count(*) AS n_init, arbitrary(i.prog) AS prog, count(i.ma) AS init_mint_auth_filled, count(i.fa) AS init_freeze_auth_filled
      FROM u JOIN init i ON i.mint = u.mint GROUP BY 1),
s AS (SELECT u.mint, count(*) AS n_set, count(s.na) AS n_set_to_address,
             array_join(array_agg(DISTINCT s.atype), ',') AS set_types,
             max_by(s.na IS NULL, s.slot) FILTER (WHERE lower(s.atype) LIKE '%mint%') AS mint_auth_last_is_null,
             max_by(s.na IS NULL, s.slot) FILTER (WHERE lower(s.atype) LIKE '%freeze%') AS freeze_auth_last_is_null
      FROM u JOIN setauth s ON s.mint = u.mint AND s.t <= u.grad_time + INTERVAL '240' MINUTE GROUP BY 1)
SELECT u.mint, coalesce(i.n_init, 0) AS n_init, i.prog, i.init_mint_auth_filled, i.init_freeze_auth_filled,
       coalesce(s.n_set, 0) AS n_set, s.n_set_to_address, s.set_types, s.mint_auth_last_is_null, s.freeze_auth_last_is_null
FROM u LEFT JOIN i ON i.mint = u.mint LEFT JOIN s ON s.mint = u.mint