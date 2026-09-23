-- S3/S4 fallback via RAW instruction calls: every Token-2022 program call that references
-- one of the five P sample mints, on each mint's creation day, grouped by the first data
-- byte (Token-2022 instruction index). Extension indices: 26 TransferFeeExtension,
-- 27 ConfidentialTransferExtension, 32 InitializeNonTransferableMint,
-- 35 InitializePermanentDelegate, 36 TransferHookExtension, 39 MetadataPointerExtension.
WITH m AS (SELECT * FROM (VALUES ('FBmPBhgQvzaEvcdWDThNCnP7j83hu9cFtGW3tDj6pump', DATE '2026-09-01'),('AjdE84dGqhbqDYhh8Pm8oCRMCTpUGQ4oDaRBF5vwpump', DATE '2026-09-01'),
  ('AaTwXAnMckAhL4ucT8i5GQpdN5MWCGuBzwkeNzBpump', DATE '2026-09-01'),('7C5mqYVj5P1kXuBvdw7yXjAdb1TWjw8xEVuWZn41pump', DATE '2026-08-27')) t(mint, d))
SELECT m.mint, bytearray_to_bigint(substr(i.data, 1, 1)) AS ix_index, COUNT(*) AS calls, MIN(i.block_slot) AS first_slot
FROM solana.instruction_calls i JOIN m ON i.block_date = m.d
WHERE i.executing_account = 'TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb'
  AND contains(i.account_arguments, m.mint)
GROUP BY 1, 2 ORDER BY 1, 2
