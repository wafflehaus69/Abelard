-- S8 bundle at launch, five sample mints: distinct buyers in the CREATION slot on the bonding
-- curve (pump_evt_tradeevent), and how many of them share a SOL funder in the prior 24h.
WITH m AS (SELECT mint, d FROM (VALUES ('GfYX7XWmhcsF1PdoL6LZq5JuBmueK7bW4nwxtisDFGW8', DATE '2026-09-01'),('FBmPBhgQvzaEvcdWDThNCnP7j83hu9cFtGW3tDj6pump', DATE '2026-09-01'),
  ('AjdE84dGqhbqDYhh8Pm8oCRMCTpUGQ4oDaRBF5vwpump', DATE '2026-09-01'),('AaTwXAnMckAhL4ucT8i5GQpdN5MWCGuBzwkeNzBpump', DATE '2026-09-01'),('7C5mqYVj5P1kXuBvdw7yXjAdb1TWjw8xEVuWZn41pump', DATE '2026-08-27')) t(mint, d)),
cr AS (SELECT c.mint, c.evt_block_slot AS create_slot, c.evt_block_time AS t0 FROM pumpdotfun_solana.pump_evt_createevent c JOIN m ON m.mint = c.mint AND c.evt_block_date = m.d),
buyers AS (SELECT cr.mint, t.user AS w FROM pumpdotfun_solana.pump_evt_tradeevent t JOIN cr ON t.mint = cr.mint AND t.evt_block_slot = cr.create_slot
           JOIN m ON m.mint = cr.mint WHERE t.evt_block_date = m.d AND t.is_buy),
st AS (SELECT to_owner, from_owner, block_time FROM tokens_solana.sol_transfers
       WHERE block_time >= TIMESTAMP '2026-08-26 00:00:00' AND block_time < TIMESTAMP '2026-09-02 00:00:00' AND CAST(amount AS double) >= 1e6),  -- literal bounds: pruning
fund AS (SELECT b.mint, b.w, s.from_owner AS funder FROM buyers b JOIN st s ON s.to_owner = b.w
         JOIN cr ON cr.mint = b.mint WHERE s.block_time BETWEEN cr.t0 - INTERVAL '24' HOUR AND cr.t0),
common AS (SELECT mint, funder, COUNT(DISTINCT w) AS n FROM fund GROUP BY 1, 2)
SELECT substr(b.mint,1,8) AS mint, COUNT(DISTINCT b.w) AS same_slot_buyers,
       COALESCE(MAX(c.n), 0) AS max_buyers_sharing_one_funder,
       CASE WHEN COUNT(DISTINCT b.w) >= 5 AND COALESCE(MAX(c.n),0) >= 5 THEN 'BUNDLE' ELSE 'no' END AS s8
FROM buyers b LEFT JOIN common c ON c.mint = b.mint GROUP BY 1 ORDER BY 1
