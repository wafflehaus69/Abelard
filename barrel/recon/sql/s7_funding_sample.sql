-- S7 1-hop deployer funding, sample: SOL sent by each of five sample creators in the 24h before
-- their token's creation, and SOL received by each creator in the 24h before (their own funder).
-- Table: tokens_solana.sol_transfers (from_owner, to_owner, amount, block_time). Creators come
-- from pump_evt_createevent for the five S1/S2 sample mints.
WITH m AS (SELECT mint FROM (VALUES ('GfYX7XWmhcsF1PdoL6LZq5JuBmueK7bW4nwxtisDFGW8'),('FBmPBhgQvzaEvcdWDThNCnP7j83hu9cFtGW3tDj6pump'),
  ('AjdE84dGqhbqDYhh8Pm8oCRMCTpUGQ4oDaRBF5vwpump'),('AaTwXAnMckAhL4ucT8i5GQpdN5MWCGuBzwkeNzBpump'),('7C5mqYVj5P1kXuBvdw7yXjAdb1TWjw8xEVuWZn41pump')) t(mint)),
cr AS (SELECT mint, creator, evt_block_time AS t0 FROM pumpdotfun_solana.pump_evt_createevent
       WHERE evt_block_date BETWEEN DATE '2026-08-26' AND DATE '2026-09-01' AND mint IN (SELECT mint FROM m)),
s AS (SELECT from_owner, to_owner, CAST(amount AS double)/1e9 AS sol, block_time FROM tokens_solana.sol_transfers
      WHERE block_time >= TIMESTAMP '2026-08-25 00:00:00' AND block_time < TIMESTAMP '2026-09-02 00:00:00')
SELECT substr(cr.mint,1,8) AS mint, substr(cr.creator,1,8) AS creator,
       COUNT(DISTINCT CASE WHEN s.from_owner = cr.creator AND s.block_time BETWEEN cr.t0 - INTERVAL '24' HOUR AND cr.t0 + INTERVAL '24' HOUR THEN s.to_owner END) AS wallets_funded_by_creator_pm24h,
       SUM(CASE WHEN s.from_owner = cr.creator AND s.block_time BETWEEN cr.t0 - INTERVAL '24' HOUR AND cr.t0 + INTERVAL '24' HOUR THEN s.sol END) AS sol_out_pm24h,
       COUNT(DISTINCT CASE WHEN s.to_owner = cr.creator AND s.block_time BETWEEN cr.t0 - INTERVAL '24' HOUR AND cr.t0 THEN s.from_owner END) AS creators_funders_24h_before,
       arbitrary(CASE WHEN s.to_owner = cr.creator AND s.block_time BETWEEN cr.t0 - INTERVAL '24' HOUR AND cr.t0 THEN substr(s.from_owner,1,8) END) AS a_funder
FROM cr LEFT JOIN s ON s.from_owner = cr.creator OR s.to_owner = cr.creator
GROUP BY 1, 2 ORDER BY 1
