# §6 cost trial — projection template (readiness 1.7; filled on purchase day)

**Rebalance date T:** 2026-03-23 (seed 20260922). **Stratum:** P. **Trailing window:** 60 days (2026-01-22 → 2026-03-23), pre-BOOST, outside the calibration slice, clear of DEGRADED.

| Leg | Query | Scope in trial | Credits measured | Unit | Extrapolation basis | Projected (P window) |
|---|---|---|---|---|---|---|
| Universe (pre-event rule) | `stratum_p_preevent_*.sql` | 60 days | | per day | 611 days | |
| Derived trade table, materialization | `derived_trades_*.sql` | 60 days | | per day | 611 days (one pass) | |
| Winners (≥ 5× on price reference) | H3 §1.1 | at T | | per date | 79 dates → **one pass over the materialized table** | |
| Early participants ∪ | H3 §1.2 | at T | | per date | same | |
| Candidate replay + scoring (w/ and w/o surfacing) | H3 §1.3, §2 | at T | | per date | same | |
| Sandwich test (`Transactions.index` leg) | R5 | candidate union | | per date | same; **optional if it dominates** | |
| Fame proxy | §4 | cohort | | per date | same | |
| Exports (aggregates only) | export contract | — | | per 1,000 datapoints | contract row counts | |

**Stop rule:** projected total vs the Analyst allowance (read from the purchase screen). > 40% of the M0 budget on H3 → M0b split (A6). Free-only fallback: sampled window, one contiguous 90-day block per era, pre-chosen.
