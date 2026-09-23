-- Decoded SPL Token / Token-2022 instruction tables on Dune, for S1/S2 (mint & freeze
-- authority as-of entry) and S3/S4 (Token-2022 extensions, transfer fee).
SELECT table_schema, table_name FROM information_schema.tables
WHERE (table_schema LIKE 'spl_token%' OR table_schema LIKE 'token_2022%' OR table_schema LIKE 'tokens_solana%' OR table_schema = 'solana')
  AND (table_name LIKE '%initializemint%' OR table_name LIKE '%setauthority%' OR table_name LIKE '%transferfee%' OR table_name LIKE '%extension%' OR table_name LIKE '%mint%' OR table_name LIKE '%initialize%')
ORDER BY 1, 2
