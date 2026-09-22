-- Which decoded Raydium / Meteora tables exist on Dune, for strata R and N admission.
SELECT table_schema, table_name
FROM information_schema.tables
WHERE (table_schema LIKE 'raydium%' OR table_schema LIKE 'meteora%' OR table_schema LIKE '%launchlab%' OR table_schema LIKE 'dex_solana%')
  AND (table_name LIKE '%pool%' OR table_name LIKE '%initialize%' OR table_name LIKE '%create%' OR table_name LIKE '%trade%' OR table_name LIKE '%swap%')
ORDER BY 1, 2
