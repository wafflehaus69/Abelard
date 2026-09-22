-- Every Meteora-related table on Dune, unfiltered by name, to find a pool-creation event.
SELECT table_schema, table_name FROM information_schema.tables
WHERE table_schema LIKE 'meteora%' OR table_schema LIKE 'dlmm%' OR table_name LIKE '%meteora%'
ORDER BY 1, 2
