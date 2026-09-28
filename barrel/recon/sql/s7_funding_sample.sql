-- S7 1-hop deployer funding, five sample creators. Rewritten after a cluster-memory failure:
-- two equi-joined legs (from_owner, to_owner), each restricted to the creator's +/-24h window.
WITH m AS (SELECT mint FROM (VALUES ('GfYX7XWmhcsF1PdoL6LZq5JuBmueK7bW4nwxtisDFGW8'),('FBmPBhgQvzaEvcdWDThNCnP7j83hu9cFtGW3tDj6pump'),
  ('AjdE84dGqhbqDYhh8Pm8oCRMCTpUGQ4oDaRBF5vwpump'),('AaTwXAnMckAhL4ucT8i5GQpdN5MWCGuBzwkeNzBpump'),('7C5mqYVj5P1kXuBvdw7yXjAdb1TWjw8xEVuWZn41pump')) t(mint)),
cr AS (SELECT mint, creator, evt_block_time AS t0 FROM pumpdotfun_solana.pump_evt_createevent
       WHERE evt_block_date BETWEEN DATE '2026-08-26' AND DATE '2026-09-01' AND mint IN (SELECT mint FROM m)),
s AS (SELECT from_owner, to_owner, CAST(amount AS double)/1e9 AS sol, block_time FROM tokens_solana.sol_transfers
      WHERE block_time >= TIMESTAMP '2026-08-26 00:00:00' AND block_time < TIMESTAMP '2026-09-02 00:00:00' AND CAST(amount AS double) >= 1e6),
outb AS (SELECT cr.mint, cr.creator, COUNT(DISTINCT s.to_owner) AS wallets_funded, SUM(s.sol) AS sol_out
         FROM cr JOIN s ON s.from_owner = cr.creator AND s.block_time BETWEEN cr.t0 - INTERVAL '24' HOUR AND cr.t0 + INTERVAL '24' HOUR GROUP BY 1, 2),
inb AS (SELECT cr.mint, COUNT(DISTINCT s.from_owner) AS funders, arbitrary(substr(s.from_owner,1,8)) AS a_funder, SUM(s.sol) AS sol_in
        FROM cr JOIN s ON s.to_owner = cr.creator AND s.block_time BETWEEN cr.t0 - INTERVAL '24' HOUR AND cr.t0 GROUP BY 1)
SELECT substr(cr.mint,1,8) AS mint, substr(cr.creator,1,8) AS creator, COALESCE(o.wallets_funded,0) AS wallets_funded_pm24h, ROUND(COALESCE(o.sol_out,0),3) AS sol_out,
       COALESCE(i.funders,0) AS funders_24h_before, i.a_funder, ROUND(COALESCE(i.sol_in,0),3) AS sol_in
FROM cr LEFT JOIN outb o ON o.mint = cr.mint LEFT JOIN inb i ON i.mint = cr.mint ORDER BY 1
