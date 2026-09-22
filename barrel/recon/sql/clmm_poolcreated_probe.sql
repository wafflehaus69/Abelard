SELECT CAST(MIN(evt_block_date) AS varchar) AS first_day, CAST(MAX(evt_block_date) AS varchar) AS last_day, COUNT(*) AS rows_sept
FROM raydium_clmm_solana.amm_v3_evt_poolcreatedevent WHERE evt_block_date BETWEEN DATE '2026-09-01' AND DATE '2026-09-21'
