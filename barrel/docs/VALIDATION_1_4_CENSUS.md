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

## Ruled metric (MR-6), sample week 2026-08-31, stratum P — `recon/sql/census_organic_week.sql`

Organic = not creator, not migration authority, not a same-slot-as-graduation buyer (S8 proxy). **S7b fee-share recipients not yet excluded** (SharingConfig history not built).

| | p10 | p50 | p90 | max |
|---|---|---|---|---|
| Distinct organic takers, days 0–7 (n = 7,236) | 18 | **420** | 3,202 | 33,100 |
| Seconds to first organic swap | 0 | **0** | 1 | — |

Zero graduates with no organic taker. **7,221 / 7,236 have their first "organic" swap within 60 s**; 7,234 within 15 min. Excluding creator, migrator and same-slot buyers does not reach the bot layer — it lands one slot later. The takers distribution is a real activity measure with a wide spread; the time-to-first measure shows that *organic* as currently definable is still bots, so a stronger definition (wallet age, prior trade count, bot-shaped exclusion from the H3 spec) is needed before it can say anything about G = 15. That is a ruling; the measurement is the deliverable.
