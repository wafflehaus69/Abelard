# Live M10 — Build Plan (UNUSUAL_ACTIVITY dossier product)

**Milestone:** M10 live (spec v1.0 §M10 + addenda v1.3 §3.3 / v1.5 §3 / v1.6 §3).
**Trigger:** M0-C returned NO-GO for Detector A (consensus). Per v1.5 §5.3 the live-M10 deferral condition ("until Detector A has a verdict") is now resolved; per v1.6 §3.3 Detector B is the near-term build priority — the one component with a positive (in-sample, one-event) viability read.

This is a **plan for review**, not a build. No code lands until the architect greenlights.

---

## 0. What M10 is (and is not)

- **Is:** an on-command scan over the **L2 collector tape** (the forward archive, running since ~2026-07-10) that surfaces fresh-wallet informed-money footprints as **dossiers** for human review.
- **Is not:** a trade signal. **No EV estimate. Permanently excluded from M9 staging** (spec Rule; v1.6 §3.4). It is an intelligence product with a human in the loop.
- **Detector B, kept separate from Detector A** (consensus) — separately tagged, never fused.

## 1. Evidence this build stands on (and its limits)

From M0-F + M5 (docs/m0f_report.md, m5_report.md), all **in-sample on the single Feb-28 labeled event**:
- Fill-factors (F/S/D/C) flag the dominant insider ~30h pre-news but can't separate the full cluster from war-week noise (no operating point < ~11 FP alone).
- **Latency × fill-factors = 4× FP reduction** (16→4 at equal recall). This conjunction is the detector's core.
- Standalone latency is weak (~9% base rate) → **latency is an ELEVATOR, not a gate** (v1.5 §3): it boosts a wallet that already clears the fill-factor bar; it never suppresses one below what F/S/D/C support.

**Hard caveat carried into every dossier:** this calibration is n=1 event, n=6 labels. Thresholds are "calibrated once," not validated. The dossier product's value is triage for a human, not a verdict.

## 2. Data source & feasibility

- **Input:** the L2 tape (`consensus/data/l2_tape.db`) — deduplicated fills in target-category markets, collector-maintained. On-command scan reads a recent window (e.g. last N hours) of tracked-market fills.
- **Chain enrichment budget (v1.5 §4 / v1.6 §3.3):** Etherscan at 3–5 calls/s. **Enrich only wallets that clear the fill-factor bar first** — the conjunction design already implies this: compute F/S/D/C from the tape (free), take the handful that score high, then pull each one's funding for the latency factor. A few dozen enrichments per scan is fine; hundreds is not. This gate is mandatory, not an optimization.

## 3. Components (most reuse exists)

| Component | Status | Source |
|---|---|---|
| Fill-factor scoring (F/S/D/C, geomean over active) | **built** | `m0f.py` `score_candidates_as_of` |
| Funded→bet latency + CEX classifier | **built** | `m5.py` |
| L2 tape reader | **built** | `tape.py` |
| Cluster membership (record, don't score) | **built** (v1.3: `cluster_boosts_score=false`) | `m0f.py` |
| **NEW: latency-as-elevator integration** | to build | boost composite when latency tight; never suppress |
| **NEW: tier latching** (per wallet, market-family) | partial (`latch_tiers` helper exists) | v1.3 §3.3 |
| **NEW: live-scan orchestration + dossier renderer** | to build | new `m10.py` |
| **NEW: `consensus m10 scan` CLI** (JSON + human dossier) | to build | `cli.py` |

## 4. Scan algorithm (per invocation)

1. **Window select:** recent fills in tracked target-category markets from the L2 tape (config `unusual_lookback_hours`).
2. **Stage 1 candidate extraction:** per (wallet, market), net-directional stake ≥ floor (reuse M0-F stage-1). Pre-scoring excludes: MM/wash (two-sided churn), sports/entertainment categories.
3. **Stage 2 fill-factors:** F/S/D/C from the tape (free). Rank; take the top candidates that clear the fill-factor bar.
4. **Stage 3 enrichment (gated):** for those top candidates only, pull funding → funded→bet latency + CEX class. **Latency elevates** the composite (multiplicative boost / tier bump); loose or absent latency leaves the fill-factor score intact.
5. **Cluster amplifier:** record cluster membership (per-market and cross-market within the target set) in the dossier as **evidence**; per v1.3 it does **not** move the score (cross-market over-fires in saturated regimes).
6. **Tier latching:** tiers latch at their high-water mark per (wallet, market-family) with the crossing timestamp; decay shown as trajectory inside the dossier, never as an alert retraction (v1.3 §3.3).
7. **Dossier:** for each surfaced wallet/cluster — factor breakdown, funding trail + CEX class, latency, cluster membership, tier + crossing time, the market(s), the in-sample caveat footer. No EV.

## 5. Config block (`m10:`, lands with the module)

`unusual_lookback_hours`, `size_floor_usdc`, fill-factor weights (reuse m0f), `latency_elevator_boost`, `latency_tight_minutes`, `enrichment_max_wallets_per_scan` (the 3.3 gate), `tier_thresholds`, `excluded_categories`, `cluster_window_hours`, `cross_market_record_only=true`.

## 6. Non-negotiables (carried from spec)

- No EV, ever. No staging path. Dossier footer states: anomaly detection over public on-chain data; not a validated trade signal; not an allegation about any person.
- Every datum traces to a cached raw record (Rule 1). Missing/failed enrichment is declared, never imputed.
- Latency elevates, never gates (v1.5 §3). Cluster records, never scores (v1.3).
- Adversarial review before the numbers are trusted (project discipline).

## 7. Sequencing

1. `m10.py` scan orchestration + latency-elevator + tier latching, over the L2 tape. Unit-tested against synthetic tapes.
2. `consensus m10 scan` CLI (JSON envelope for orchestration + human dossier).
3. Adversarial multi-lens review; fix before trusting output.
4. A live on-command scan on the current L2 tape as a smoke/acceptance (surfaces whatever fresh footprints exist now — expected quiet in a normal week; the value shows on an event).
5. **Deferred, not now:** the `unusual-review` labeling loop (accumulates a second labeled event over time — the only path out of the n=1 caveat).

## Footnote — Detector A's future (not this milestone)

The L2 confirmation pass for consensus (Detector A) is a **calendar item** (~mid-Sep 2026), passive; the collector is already archiving the data. The gate-attribution diagnostic (docs/m0c_report.md §4b) resolved it to **World B, unambiguously** — zero remaining-edge kills, zero convergences, the circle collapses to ~1 wallet by 2026. So the L2 pass is a **genuine second chance**: a circle re-bootstrapped from current-regime data directly targets the "no current circle" failure. Still, nothing is built for Detector A now — the re-test is passive and months out; this milestone (Detector B) is where the near-term build goes.
