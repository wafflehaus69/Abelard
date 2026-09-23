-- Five stratum-P graduates from 2026-09-01 (seeded by slot order: 2 earliest, 2 latest, median),
-- with graduation slot/time and the pump CreateEvent slot (start of the authority history).
WITH g AS (
  SELECT mint, pool, evt_block_slot AS grad_slot, evt_block_time AS grad_time,
         row_number() OVER (ORDER BY evt_block_slot) AS rn, COUNT(*) OVER () AS n
  FROM pumpdotfun_solana.pump_evt_completepumpammmigrationevent
  WHERE evt_block_date = DATE '2026-09-01' AND quote_mint = '11111111111111111111111111111111'),
pick AS (SELECT * FROM g WHERE rn IN (1, 2, n, n-1, CAST(n/2 AS integer))),
c AS (SELECT mint, evt_block_slot AS create_slot, evt_block_time AS create_time, token_program
      FROM pumpdotfun_solana.pump_evt_createevent
      WHERE evt_block_date BETWEEN DATE '2026-08-01' AND DATE '2026-09-01')
SELECT p.mint, p.pool, p.grad_slot, p.grad_time, c.create_slot, c.create_time, c.token_program
FROM pick p LEFT JOIN c ON c.mint = p.mint ORDER BY p.grad_slot
