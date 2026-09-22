-- Why did stratum R join null? Coverage and key orientation of the decoded Raydium v4
-- initialize2 table on 2025-02-15: how many inits exist, and on which side pump mints sit.
SELECT COUNT(*)                                            AS init2_calls,
       COUNT_IF(account_coinMint LIKE '%pump')             AS coin_is_pump,
       COUNT_IF(account_pcMint   LIKE '%pump')             AS pc_is_pump,
       COUNT_IF(account_coinMint = 'So11111111111111111111111111111111111111112') AS coin_is_wsol,
       COUNT_IF(account_pcMint   = 'So11111111111111111111111111111111111111112') AS pc_is_wsol,
       COUNT(DISTINCT call_tx_signer)                      AS signers,
       MIN(call_block_time) AS first_seen, MAX(call_block_time) AS last_seen
FROM raydium_amm_solana.raydium_amm_call_initialize2
WHERE call_block_date = DATE '2025-02-15'
