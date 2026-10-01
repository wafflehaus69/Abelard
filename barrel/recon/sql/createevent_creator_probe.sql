-- Is the deployer in `creator` or `user` on older create events? One day each era, counts only.
SELECT evt_block_date AS d, count(*) AS n, count(creator) AS has_creator, count("user") AS has_user,
       count_if(creator = "user") AS creator_equals_user
FROM pumpdotfun_solana.pump_evt_createevent
WHERE evt_block_date IN (DATE '2025-06-09', DATE '2025-10-15', DATE '2026-09-01')
GROUP BY 1 ORDER BY 1
