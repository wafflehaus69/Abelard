-- 1.3 export contract: stratum-P universe size over the window, by month (small event table).
SELECT date_trunc('month', evt_block_time) AS month,
       COUNT_IF(quote_mint = '11111111111111111111111111111111') AS p_graduates,
       COUNT_IF(quote_mint <> '11111111111111111111111111111111') AS p_alt_graduates
FROM pumpdotfun_solana.pump_evt_completepumpammmigrationevent
WHERE evt_block_date BETWEEN DATE '2025-03-20' AND DATE '2026-09-27'
GROUP BY 1 ORDER BY 1
