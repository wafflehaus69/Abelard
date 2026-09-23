-- S3/S4 raw-call reconciliation for five stratum-N Token-2022 mints (CP-swap inits 2026-09-01),
-- window 2026-08-30..09-01. Extension indices: 26 TransferFee, 27 ConfidentialTransfer,
-- 32 NonTransferable, 35 PermanentDelegate, 36 TransferHook, 39 MetadataPointer.
WITH m AS (SELECT mint FROM (VALUES ('B6tBzcGXygwNSn1ghSrLYYYphG9UEQfrHvVAtetScoiw'),('D4LLPan8pSdsYmuxnLeq4rFXeXaxbazYLmvNd8ncwBSc'),
  ('DGojxfuXUNER9hMKL2PJ6JmrydRKFgRqDGuJ5pzissd5'),('GbRs1rEFHU4f2qWVmm3aFs9QEFHYgeHigSCoPkCjpePM'),('H2jDWfVqHLk179XunAgo9PEBTwHox5Sd2AJgC6C42k1w')) t(mint))
SELECT m.mint, bytearray_to_bigint(substr(i.data, 1, 1)) AS ix_index, COUNT(*) AS calls, MIN(i.block_slot) AS first_slot
FROM solana.instruction_calls i JOIN m ON contains(i.account_arguments, m.mint)
WHERE i.block_date BETWEEN DATE '2026-08-30' AND DATE '2026-09-01'
  AND i.executing_account = 'TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb'
  AND bytearray_to_bigint(substr(i.data, 1, 1)) IN (0, 20, 26, 27, 32, 35, 36, 39)
GROUP BY 1, 2 ORDER BY 1, 2
