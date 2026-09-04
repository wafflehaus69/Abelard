-- BARREL M0 — D1 probe: is the BigQuery public Solana dataset fit for Gate 0?
--
-- Ruling A1.1 (Mando, 2026-09-04): evaluate this dataset, free, before any vendor spend.
-- Answers three questions and nothing else: freshness, window coverage, and whether the
-- `accounts` table actually answers R8 (as-of authority state).
--
-- ============================================================================
-- READ THIS BEFORE RUNNING ANYTHING
-- ============================================================================
-- "Free" here means free of SUBSCRIPTION, not free of COST. The dataset is free to
-- access; BigQuery bills you for bytes SCANNED, and this is a petabyte-scale dataset.
-- The free allowance is 1 TiB of query processing per month. A single careless
-- `SELECT * FROM transactions` can burn the month's allowance and then bill at
-- ~$6.25/TiB after it.
--
-- So: run every query below with a DRY RUN FIRST and read the byte estimate.
--
--     bq query --dry_run --use_legacy_sql=false 'SELECT ...'
--
-- Anything estimating over ~50 GB, stop and bring the number back before running it.
-- Never SELECT *. Always constrain the partitioning column in the WHERE clause.
--
-- Steps 1 and 2 are metadata-only and scan 0 bytes. Start there; they are free
-- unconditionally and they tell you what the rest of the file should even say.
-- ============================================================================


-- ---------------------------------------------------------------------------
-- STEP 1 — What tables exist, how big, and when were they last written?
-- Metadata only. 0 bytes scanned. This is also a freshness signal in itself:
-- a stale last_modified_time is the cheapest possible answer to D1.
-- ---------------------------------------------------------------------------
SELECT
  table_id,
  row_count,
  ROUND(size_bytes / POW(1024, 4), 2)             AS size_tib,
  TIMESTAMP_MILLIS(creation_time)                 AS created,
  TIMESTAMP_MILLIS(last_modified_time)            AS last_modified
FROM `bigquery-public-data.crypto_solana_mainnet_us.__TABLES__`
ORDER BY size_bytes DESC;


-- ---------------------------------------------------------------------------
-- STEP 2 — Schema of the tables that matter, especially `accounts`.
-- Metadata only. 0 bytes scanned.
--
-- This is the load-bearing question for R8. The `accounts` table is the ONLY
-- reason this candidate was not dismissed: if it carries mint/freeze authority
-- at a slot, S1-S4 become a lookup. If it does not, the as-of reconstruction
-- cost returns in full and this dataset's advantage over Dune evaporates.
--
-- Look for: authority columns on `accounts`, a slot/block_timestamp column to
-- key as-of state on, and whether `accounts` is a full snapshot per slot or
-- only accounts touched in that block. That last distinction decides whether
-- as-of state is a lookup or still a replay.
-- ---------------------------------------------------------------------------
SELECT table_name, ordinal_position, column_name, data_type
FROM `bigquery-public-data.crypto_solana_mainnet_us.INFORMATION_SCHEMA.COLUMNS`
WHERE table_name IN ('accounts', 'transactions', 'blocks', 'token_transfers')
ORDER BY table_name, ordinal_position;


-- ---------------------------------------------------------------------------
-- STEP 3 — Freshness. How far behind head is the newest block?
-- DRY RUN FIRST. Should be cheap if `blocks` is partitioned on timestamp;
-- if the estimate is large, `blocks` is not partitioned the way we hope and
-- that is itself a finding worth reporting before spending the scan.
-- ---------------------------------------------------------------------------
SELECT
  MAX(block_timestamp)                                          AS newest_block,
  TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), MAX(block_timestamp), HOUR) AS hours_behind_head
FROM `bigquery-public-data.crypto_solana_mainnet_us.blocks`
WHERE block_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY);


-- ---------------------------------------------------------------------------
-- STEP 4 — Window coverage: per-week block counts back to the PumpSwap era.
--
-- Gate 0's tolerance (M0_TASKING §2) is >5% gap in ANY week = stop and report.
-- Solana produces roughly one slot per 400ms, so a complete week is on the
-- order of 1.5M slots. What matters here is not the absolute number but that
-- no week collapses relative to its neighbours: a week at 60% of the trailing
-- median is a hole, and holes are what disqualify a universe source.
--
-- 2025-03-20 is the PumpSwap cutover (R2). Ruling A1.2 widened the universe to
-- all venues, so the real window start is earlier and should be extended once
-- the launchpad admission set is enumerated — but start here, because if the
-- dataset cannot cover the recent era cleanly there is no point pricing the
-- older one.
--
-- DRY RUN FIRST. This spans ~18 months.
-- ---------------------------------------------------------------------------
WITH weekly AS (
  SELECT
    DATE_TRUNC(DATE(block_timestamp), WEEK)  AS week,
    COUNT(*)                                 AS blocks_in_week,
    MIN(block_timestamp)                     AS first_seen,
    MAX(block_timestamp)                     AS last_seen
  FROM `bigquery-public-data.crypto_solana_mainnet_us.blocks`
  WHERE block_timestamp >= TIMESTAMP('2025-03-20')
  GROUP BY week
)
SELECT
  week,
  blocks_in_week,
  first_seen,
  last_seen,
  -- trailing median is the honest baseline: chain throughput drifts, so a fixed
  -- expected count would flag drift as a gap ([E8] — measure, don't mandate).
  ROUND(
    100 * blocks_in_week / NULLIF(
      PERCENTILE_CONT(blocks_in_week, 0.5)
        OVER (ORDER BY week ROWS BETWEEN 8 PRECEDING AND 1 PRECEDING), 0),
    1
  ) AS pct_of_trailing_median
FROM weekly
ORDER BY week;


-- ---------------------------------------------------------------------------
-- STEP 5 — Does the venue activity actually land in this dataset?
--
-- Coverage of BLOCKS is not coverage of the PROGRAMS we need. E16: verify the
-- population entering the matcher before trusting the match. This asks whether
-- PumpSwap / pump.fun / Raydium transactions are present and in what mix, for
-- one recent day, cross-checked against the live rates measured in
-- probe_onchain.py (PumpSwap ~29M tx/day as of 2026-09-04).
--
-- A day that returns materially fewer PumpSwap transactions than the live probe
-- implies silent filtering somewhere in the ETL — an E6 aggregation-layer
-- exclusion — and that disqualifies the dataset as a UNIVERSE source however
-- fresh it looks.
--
-- Column name for program ids is a guess until STEP 2 has run; fix it from the
-- real schema before running, do not run this blind.
-- DRY RUN FIRST — one day of Solana transactions is large.
-- ---------------------------------------------------------------------------
-- SELECT
--   program_id,
--   COUNT(*) AS tx_count
-- FROM `bigquery-public-data.crypto_solana_mainnet_us.transactions`,
--   UNNEST(<instructions column from STEP 2>) AS ix
-- WHERE block_timestamp >= TIMESTAMP('2026-09-01')
--   AND block_timestamp <  TIMESTAMP('2026-09-02')
--   AND ix.program_id IN (
--     'pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA',  -- PumpSwap AMM
--     '6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P',  -- pump.fun bonding curve
--     '675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8'  -- Raydium AMM v4
--   )
-- GROUP BY program_id
-- ORDER BY tx_count DESC;


-- ============================================================================
-- WHAT A PASS LOOKS LIKE
-- ============================================================================
-- All four must hold, or the dataset is reported as unfit and Dune becomes the
-- live candidate under D1:
--
--   1. Freshness lag is hours, not days, and there is no unexplained gap.
--   2. No week in STEP 4 falls materially below its trailing median.
--   3. STEP 5's PumpSwap count is the same order of magnitude as the live probe.
--   4. `accounts` carries authority state keyed to a slot (R8) — or, if it does
--      not, the as-of reconstruction cost is re-estimated and reported BEFORE
--      this dataset is chosen on freshness alone.
--
-- Report the byte-scan estimates alongside the results. If answering Gate 0
-- costs more in BigQuery scan than a month of Dune Analyst, that is the finding,
-- and D1 gets re-ruled on numbers instead of on the word "free".
-- ============================================================================
