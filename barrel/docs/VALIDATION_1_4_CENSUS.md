# Readiness 1.4 — census queries, one-week samples (2026-09-22)

`recon/sql/census_week_sample.sql` (post-BOOST week 2026-08-31) and `_preboost.sql` (2026-06-01), stratum P.

| Week | Launches | Graduations | Grad rate | ≥1 swap in 7 d |
|---|---|---|---|---|
| 2026-06-01 (pre-BOOST) | 191,192 | 1,379 | **0.72%** | 100.0% |
| 2026-08-31 (post-BOOST) | 213,286 | 7,236 | **3.39%** | 100.0% |

**Findings**
1. **BOOST multiplied the graduation rate by ~4.7×** on these two weeks (press: ~8×). Two weeks are a shape check, not the window measurement.
2. **The §2 metric "% of graduated tokens with ≥ 1 swap in the following 7 days" is vacuous**: 100.0% in both eras. Every graduate is traded within minutes (snipers; post-BOOST also the protocol buy, though that emits a different event and is not counted here). **It carries no information and should be replaced** — candidates: distinct taker wallets in 7 d (distribution, not a threshold), or time-to-first non-creator, non-migrator swap. Needs a ruling before Gate 0 runs.
3. Cost: ~1.3 credits per one-week sample. A window-wide weekly census is a single query with `GROUP BY week`, not per-week runs.

Twenty-token reconciliation list: not yet drawn (needs the window universe and the seed recorded).
