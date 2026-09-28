-- S7/S8 need native SOL transfer history (deployer -> wallet funding, common funder within 24h).
-- Which Dune tables carry native SOL / system-program transfers?
SELECT table_schema, table_name FROM information_schema.tables
WHERE (table_schema IN ('solana','solana_utils','tokens_solana','system_program_solana','spl_token_solana')
       AND (table_name LIKE '%transfer%' OR table_name LIKE '%account_activity%' OR table_name LIKE '%balance%' OR table_name LIKE '%system%'))
   OR table_schema LIKE 'system_program%'
ORDER BY 1, 2
