-- MR-14 order 4: null-count item for the FIRST PAID CHUNK. Not run on the trial.
-- Question: the burned week (graduations 2026-08-10 .. 08-16) showed quote_amount_in LARGER than
-- user_quote_amount_in on most buys per token, and a 20-minute sample of all pools on 2026-08-10
-- showed it on 45% of buys; yet fees_per_era on 2026-09-01 (stratum-P pools, buys above 0.001 SOL)
-- kept every row under a mapping that requires the opposite. Which is it, and when does it change?
-- Counts per day, for stratum-P pools graduated in the month and for all pools, with and without the
-- 0.001 SOL floor that fees_per_era applied. Counts only.
WITH p AS (
  SELECT pc.pool FROM pumpdotfun_solana.pump_amm_evt_createpoolevent pc
  WHERE pc.evt_block_date BETWEEN DATE '2026-08-01' AND DATE '2026-09-01'
    AND pc.quote_mint = 'So11111111111111111111111111111111111111112'
    AND pc.base_mint IN (SELECT mint FROM pumpdotfun_solana.pump_evt_completeevent
                         WHERE evt_block_date BETWEEN DATE '2026-08-01' AND DATE '2026-08-31'))
SELECT b.evt_block_date AS d,
       (b.pool IN (SELECT pool FROM p)) AS stratum_p_pool,
       (b.quote_amount_in > 1000000) AS above_floor,
       count(*) AS buys,
       count_if(b.quote_amount_in > b.user_quote_amount_in) AS quote_field_larger,
       count_if(b.quote_amount_in < b.user_quote_amount_in) AS user_field_larger,
       count_if(b.quote_amount_in = b.user_quote_amount_in) AS equal_fields,
       count(b.ix_name) AS ix_name_filled
FROM pumpdotfun_solana.pump_amm_evt_buyevent b
WHERE b.evt_block_date BETWEEN DATE '2026-08-08' AND DATE '2026-09-02'
GROUP BY 1, 2, 3
