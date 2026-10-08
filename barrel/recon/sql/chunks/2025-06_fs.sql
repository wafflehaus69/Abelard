-- Fee-share recipients (S7b, c07b), query 1 of 2: the fee program's sharing-config events, raw, graduations 2025-06-01 .. 2025-06-30.
-- Rows name wallets: run with --private-rows. No threshold, no classification, no verdict here.
-- One row per event that wrote a token's recipient list (config made, shares changed, config reset), from the
-- token's creation bound to graduation + 240 minutes. The as-of rule (R7) reads the config in force at entry
-- and the last entry lag is 240 minutes, so nothing later is fetched. A token with no such event keeps one
-- row with the event columns empty: "no event" is then a returned answer, not a token that went missing.
-- Nothing is decoded here: the payload goes back as hex and recon/decode_feeshare.py reads it with the pinned IDL.
-- A payload holding an owner wallet comes back without it (data_hex empty, owner_in_payload true). The row has
-- to exist: without it the config before it would be read as still in force.
WITH comp AS (
  SELECT mint, evt_block_time AS ct FROM pumpdotfun_solana.pump_evt_completeevent
  WHERE evt_block_date BETWEEN DATE '2025-06-01' AND DATE '2025-06-30'),
pc AS (
  SELECT base_mint AS mint, quote_mint, pool, evt_block_time AS pt,
         row_number() OVER (PARTITION BY base_mint ORDER BY evt_block_time) AS rn
  FROM pumpdotfun_solana.pump_amm_evt_createpoolevent
  WHERE evt_block_date BETWEEN DATE '2025-06-01' AND DATE '2025-07-01'),
u AS (
  SELECT c.mint, p.pt AS grad_time, p.pool
  FROM comp c JOIN pc p ON p.mint = c.mint AND p.rn = 1 AND p.pt >= c.ct AND p.pt < c.ct + INTERVAL '1' DAY
  WHERE p.quote_mint = 'So11111111111111111111111111111111111111112'),
cr AS (
  SELECT mint, arbitrary(COALESCE(creator, "user")) AS creator, min(evt_block_time) AS t0, min(evt_block_slot) AS s0
  FROM pumpdotfun_solana.pump_evt_createevent
  WHERE evt_block_date BETWEEN DATE '2025-05-29' AND DATE '2025-06-30' AND mint IN (SELECT mint FROM u)
  GROUP BY 1),
base AS (   -- per-token creation bound: the same 3 days for every token, wherever it falls in the chunk
  SELECT u.mint, u.grad_time, u.pool, cr.creator, cr.t0, cr.s0 FROM u JOIN cr ON cr.mint = u.mint
  WHERE cr.t0 >= date_trunc('day', u.grad_time) - INTERVAL '3' DAY),
bm AS (   -- the mint as its 32 bytes, to meet the event's own copy of them; the conversion runs on this small side
  SELECT mint, from_base58(mint) AS mint_bin, grad_time, t0 FROM base),
ev AS (   -- ONE pass over the raw instruction table. An event is an inner call of the fee program to itself:
          -- 8 bytes of wrapper tag, 8 of event discriminator, then the timestamp (8) and the mint (32)
  SELECT block_slot, block_time, tx_index, outer_instruction_index, inner_instruction_index, tx_id, tx_success,
         to_hex(substr(data, 9, 8)) AS evt, substr(data, 25, 32) AS mint_bin, to_hex(data) AS data_hex
  FROM solana.instruction_calls
  WHERE block_date BETWEEN DATE '2025-05-29' AND DATE '2025-07-02'
    AND executing_account = 'pfeeUxB6jkeY1Hxd7CsFCAjcbHA9rWtchMGdZ6VojVZ'
    -- a failed transaction changed nothing. NULL-safe, as the rent rule is: only a transaction KNOWN to have
    -- failed is left out; a row whose flag is empty comes back with tx_success empty and is not used locally
    AND coalesce(tx_success, true)
    AND to_hex(substr(data, 1, 16)) IN ('E445A52E51CB9A1D8569AAC8B874FB58', 'E445A52E51CB9A1D15BAC4B85BE4E1CB', 'E445A52E51CB9A1DCBCC97E27837D6F3')),
j AS (
  SELECT b.mint, b.grad_time, b.t0, e.block_slot, e.block_time, e.tx_index, e.outer_instruction_index,
         e.inner_instruction_index, e.tx_id, e.tx_success, e.evt, e.data_hex,
         __NOT_OWNER_HEX(e.data_hex)__ AS no_owner
  FROM bm b
  LEFT JOIN ev e ON e.mint_bin = b.mint_bin
    -- per-token window: the same for a token wherever it falls in the chunk
    AND e.block_time >= date_trunc('day', b.grad_time) - INTERVAL '3' DAY
    AND e.block_time <= b.grad_time + INTERVAL '240' MINUTE)
SELECT mint, grad_time, t0, block_slot, block_time, tx_index, outer_instruction_index, inner_instruction_index,
       tx_id, tx_success, evt,
       CASE WHEN no_owner THEN data_hex END AS data_hex,
       NOT no_owner AS owner_in_payload
FROM j