-- Fee-share proving run 2: sharing-config INSTRUCTIONS and events of ONE partition (2026-09-01), every token, as rows.
-- Rows name wallets: run with --private-rows. No threshold, no classification, no verdict here.
-- For the case where the events of run 1 do not reconcile with the instruction counts, and to price the
-- account column that run 1 leaves out. It gives a second encoding to hold the first against: the arguments
-- of an instruction that changes the shares must equal the list in its event (decode_feeshare.crosscheck),
-- and the 20 rows held from the readiness run must come back unchanged.
-- kind is the 8-byte discriminator of an instruction or the 16-byte prefix of an event; one list selects both.
-- Run it with --no-rows first: about 13,000 rows, and fetching rows is billed by size.
WITH c AS (
  SELECT block_slot, block_time, tx_index, outer_instruction_index, inner_instruction_index, tx_id, tx_success, is_inner,
         CASE WHEN to_hex(substr(data, 1, 8)) = 'E445A52E51CB9A1D' THEN to_hex(substr(data, 1, 16)) ELSE to_hex(substr(data, 1, 8)) END AS kind,
         cardinality(account_arguments) AS n_accounts,
         -- the mint is the fifth account of the three instructions the pinned IDL has (held on 20 of 20 rows);
         -- the reset instruction is not in the IDL, so for it this column is only its fifth account
         CASE WHEN to_hex(substr(data, 1, 8)) = 'E445A52E51CB9A1D' THEN NULL ELSE element_at(account_arguments, 5) END AS ix_mint,
         to_hex(data) AS data_hex
  FROM solana.instruction_calls
  WHERE block_date = DATE '2026-09-01'
    AND executing_account = 'pfeeUxB6jkeY1Hxd7CsFCAjcbHA9rWtchMGdZ6VojVZ'
    AND CASE WHEN to_hex(substr(data, 1, 8)) = 'E445A52E51CB9A1D' THEN to_hex(substr(data, 1, 16)) ELSE to_hex(substr(data, 1, 8)) END
        IN ('E445A52E51CB9A1D8569AAC8B874FB58', 'E445A52E51CB9A1D15BAC4B85BE4E1CB', 'E445A52E51CB9A1DCBCC97E27837D6F3', 'C34E564C6F34FBD5', 'BD0D8863BBA4ED23', '6FFB31064E4E6A12', '0A02B65F107F81BA'))
SELECT block_slot, block_time, tx_index, outer_instruction_index, inner_instruction_index, tx_id, tx_success, is_inner,
       kind, n_accounts, ix_mint,
       CASE WHEN __NOT_OWNER_HEX(c.data_hex)__ THEN c.data_hex END AS data_hex,
       NOT __NOT_OWNER_HEX(c.data_hex)__ AS owner_in_payload
FROM c