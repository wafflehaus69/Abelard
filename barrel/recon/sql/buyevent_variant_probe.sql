-- E35 follow-up: which of quote_amount_in / user_quote_amount_in is the larger (gross) one, by ix_name and era?
-- The burned-week run returned a NEGATIVE median buy cost post-BOOST. 20 minutes per era, all pools.
SELECT DATE '2025-06-09' AS d, ix_name, count(*) AS n,
       count_if(quote_amount_in > user_quote_amount_in) AS qin_gt_user, count_if(quote_amount_in < user_quote_amount_in) AS qin_lt_user,
       count_if(quote_amount_in = user_quote_amount_in) AS equal_,
       approx_percentile(1e4 * abs(CAST(quote_amount_in AS double) - CAST(user_quote_amount_in AS double)) / greatest(CAST(quote_amount_in AS double), CAST(user_quote_amount_in AS double)), 0.5) AS gap_bps_p50
FROM pumpdotfun_solana.pump_amm_evt_buyevent
WHERE evt_block_date = DATE '2025-06-09' AND evt_block_time >= TIMESTAMP '2025-06-09 12:00:00' AND evt_block_time < TIMESTAMP '2025-06-09 12:20:00'
GROUP BY 2
UNION ALL
SELECT DATE '2025-10-15' AS d, ix_name, count(*) AS n,
       count_if(quote_amount_in > user_quote_amount_in) AS qin_gt_user, count_if(quote_amount_in < user_quote_amount_in) AS qin_lt_user,
       count_if(quote_amount_in = user_quote_amount_in) AS equal_,
       approx_percentile(1e4 * abs(CAST(quote_amount_in AS double) - CAST(user_quote_amount_in AS double)) / greatest(CAST(quote_amount_in AS double), CAST(user_quote_amount_in AS double)), 0.5) AS gap_bps_p50
FROM pumpdotfun_solana.pump_amm_evt_buyevent
WHERE evt_block_date = DATE '2025-10-15' AND evt_block_time >= TIMESTAMP '2025-10-15 12:00:00' AND evt_block_time < TIMESTAMP '2025-10-15 12:20:00'
GROUP BY 2
UNION ALL
SELECT DATE '2026-04-15' AS d, ix_name, count(*) AS n,
       count_if(quote_amount_in > user_quote_amount_in) AS qin_gt_user, count_if(quote_amount_in < user_quote_amount_in) AS qin_lt_user,
       count_if(quote_amount_in = user_quote_amount_in) AS equal_,
       approx_percentile(1e4 * abs(CAST(quote_amount_in AS double) - CAST(user_quote_amount_in AS double)) / greatest(CAST(quote_amount_in AS double), CAST(user_quote_amount_in AS double)), 0.5) AS gap_bps_p50
FROM pumpdotfun_solana.pump_amm_evt_buyevent
WHERE evt_block_date = DATE '2026-04-15' AND evt_block_time >= TIMESTAMP '2026-04-15 12:00:00' AND evt_block_time < TIMESTAMP '2026-04-15 12:20:00'
GROUP BY 2
UNION ALL
SELECT DATE '2026-08-10' AS d, ix_name, count(*) AS n,
       count_if(quote_amount_in > user_quote_amount_in) AS qin_gt_user, count_if(quote_amount_in < user_quote_amount_in) AS qin_lt_user,
       count_if(quote_amount_in = user_quote_amount_in) AS equal_,
       approx_percentile(1e4 * abs(CAST(quote_amount_in AS double) - CAST(user_quote_amount_in AS double)) / greatest(CAST(quote_amount_in AS double), CAST(user_quote_amount_in AS double)), 0.5) AS gap_bps_p50
FROM pumpdotfun_solana.pump_amm_evt_buyevent
WHERE evt_block_date = DATE '2026-08-10' AND evt_block_time >= TIMESTAMP '2026-08-10 12:00:00' AND evt_block_time < TIMESTAMP '2026-08-10 12:20:00'
GROUP BY 2
