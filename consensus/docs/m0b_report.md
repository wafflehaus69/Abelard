# M0-B — Detector-B Follower-Edge Backtest — REPORT

**Pre-registration:** frozen at **`8cc57c3`** (`docs/m0b_checkpoint.md`, PR-0…PR-8),
amended at **`1d8fd20`** (`docs/m0b_prereg_addendum_v1.12.md`: accepted the larger
outcome-blind cache, froze the mesh-collapse operationalization, recorded the exclusion
finding). Both committed **before any outcome was computed** (Amendment-3 discipline).
Published regardless of outcome — no file-drawer. Date: 2026-07-26.

---

## Headline verdict

> ## **NO-GO** — no *large* edge detected (realized MDE ≈ **6.5pp**).
>
> Detector-B ELEVATED footprints, entered by a realistic +30-min follower, earn a
> size-weighted follower edge of **+2.15pp** on the pre-registered primary cell
> (Thesis-1 ≤30d). That point estimate is **positive but not statistically
> distinguishable from zero** — market-block bootstrap 95% CI **(−3.3pp, +7.0pp)**,
> one-sided *p* ≈ **0.21** — and it is **below the pre-registered +3pp materiality
> floor.** The footprint carries, at most, a faint informational signal that a
> +30-min follower cannot convert into a tradeable edge on this sample.

The M0-B question (PR-0): *does a footprint's flagged side resolve in its favor MORE
than the price a realistic follower pays?* Detection ≠ edge — this is the test that
killed Detector A (M0-C). A GO would authorize only the next design conversation, never
operational staging. **This NO-GO leaves M10 a dossier-only intelligence product.**

**This is not M0-C.** Detector A's "edge" was pure noise walked back to zero. Detector B
shows a *consistently positive but sub-threshold* point estimate with real internal
structure (below) — faint content, not a tradeable edge. That distinction is the finding.

---

## 1. Sample and enrichment (addendum v1.12)

- **Universe:** cached L1 subgraph tape (geopolitics/politics, 2025-01 → 2026-04-28
  freeze); **1,364 resolved markets** with ≥1 footprint; **15,768 stage-1 footprints**
  ($10k net-long crossing at a contested price 0.10–0.90, zero-lookahead). Accepted under
  v1.11 Option 1 — the sample grew from the frozen 936/2,805 via a Jul-23 fills pull
  *before any grading*, so no "expand-until-GO" is possible.
- **Enrichment exclusion (architect deliverable):** F-enrichment excludes **0 of 4,335**
  footprint wallets (0.0%) — every footprint wallet has maker-side subgraph history, so F
  is computable for the whole sample and there is **no selection bias**; post-exclusion
  n_eff = full-sample n_eff. (Mechanism validated: no-history wallets correctly excluded.)
- **Primary cell (frozen, PR-3):** `{composite = ELEVATED 0.50 · latency = OFF · entry_lag
  = +30min · mesh_collapse = OFF (operative per addendum gate) · Thesis-1 ≤30d}`.
  Network-free: no Etherscan; F from the L1 subgraph; grading and the 10,000-resample
  actor+market-block bootstrap entirely on-disk.
- **Primary-cell population:** 15,768 stage-1 → **1,899 ELEVATED** (exact composite ≥ 0.50)
  → **870 Thesis-1 ≤30d, tradeable** (8 not-tradeable, declared+excluded per PR-5 slippage).

---

## 2. Primary verdict — Thesis-1 ≤30d, mesh-OFF, size-weighted

| quantity | value |
|---|---|
| ELEVATED T1 footprints (primary cell) | **870** across **322 market-blocks** |
| primary-cell n_eff (Kish, Amendment 2) | **439.3** |
| **E — size-weighted follower edge** | **+0.0215 (+2.15pp)**  (equal-weighted +2.66pp) |
| E 95% CI (market-block bootstrap, 10k) | **(−0.0325, +0.0698)**  — one-sided *p*(E≤0) ≈ 0.21 |
| benchmark_a (contested-slice) | +0.0069 ;  E−a = +0.0147, CI (−0.0452, +0.0779) |
| benchmark_b (price×stake-matched control) | +0.0001 ;  E−b = +0.0214, CI (−0.0255, +0.0698) |
| hit-rate (diagnostic only) | 0.540 |

**Why NO-GO — the honest independent-n.** By weight, n_eff = 439 looks powered. But the
bootstrap resamples **market-blocks (322)**, not footprints, because footprint outcomes
are perfectly correlated *within* a market (one resolution). The large *between-market*
edge variance is what widens the CI — precisely the "**block count is the binding
constraint**" insight the checkpoint pre-registered. Realized SE(E) ≈ 0.026 ⇒ MDE ≈ 6.5pp:
the test can only certify an edge larger than ~6.5pp, and the true point estimate (~2pp)
sits inside the noise.

### GO rule (PR-5, conjunctive — all must hold)

| # | condition | result |
|---|---|---|
| PR-5.1 | CI-lower(E) > 0 | **FAIL** (−0.0325) |
| PR-5.2a | CI-lower(E − a) > 0 | **FAIL** (−0.0452) |
| PR-5.2b | CI-lower(E − b) > 0 | **FAIL** (−0.0255) |
| PR-5.2 | materiality (E − b) ≥ +0.03 | **FAIL** (+0.0214) |
| PR-5.3 | 2026-Jan-Apr point positive (required) | PASS (+0.0169) |
| PR-5.3 | 2026 not negative (auto-NO-GO if neg) | PASS |
| PR-5.3 | positive in ≥ 2/3 regimes | PASS (3/3) |
| PR-5.4 | n_eff ≥ 30 aggregate | PASS (439) |
| PR-5.4 | n_eff ≥ 10 in 2026 | PASS (84.5) |
| PR-5.5 | month ≤ 40% of weight | PASS (22.4%) |
| PR-5.5 | market-family ≤ 25% | PASS (8.7%) |
| PR-5.5 | ≤7d sub-stratum same sign | PASS (+0.053) |
| PR-5.5 | +60 survives (>0 & beats both, point) | PASS |
| PR-5.5 | equal-weighted same sign | PASS |

The verdict is **well-powered** (not INSUFFICIENT) and fails specifically on
significance + materiality. The point estimate is positive and broad; it is simply too
small and too noisy to be a tradeable signal.

---

## 3. Per-regime decay guard (asymmetric 2026 rule)

| regime | n | n_eff | blocks | E (point) |
|---|---|---|---|---|
| 2025-H1 | 534 | 283 | 150 | +0.0235 |
| 2025-H2 | 149 | 81 | 71 | +0.0168 |
| **2026-Jan-Apr** | 187 | 84 | 101 | **+0.0169** |

All three regimes are **positive** and the live-adjacent 2026 slice is positive and above
its power floor — so the decay guard is *satisfied* (the aggregate is not a stale-regime
artifact). The failure is not regime-instability; it is that even the aggregate edge is
within noise. Per-regime CIs are all wide (each straddles 0), as expected at 71–150 blocks.

---

## 4. Robustness and structure

- **Tier-monotonicity (supporting):** E rises with conviction — ELEVATED 0.50–0.70 =
  **+1.67pp** → CRITICAL ≥0.70 = **+3.57pp**. Moreover CRITICAL **significantly beats the
  contested slice**: E−a CI (**+0.5pp, +14.8pp**). Genuine informational content is
  concentrated in the highest-conviction footprints — but the *primary* verdict cell is
  ELEVATED (0.50), which is NO-GO, and secondaries cannot upgrade it (PR-8).
- **Short-dated concentration:** nested **≤7d = +5.32pp** (vs ≤30d +2.15pp) — the edge lives
  in the shortest horizons, consistent with the Thesis-1 "informed timing" prior. Still not
  significant on its own (CI (−2.0pp, +11.9pp), 181 blocks).
- **+60min slippage step:** E = +2.10pp, still beats both benchmarks on the point estimate.
- **Not whale-driven:** equal-weighted (+2.66pp) agrees in sign; largest single footprint is
  **1.0%** of size-weight; E excluding it = +3.08pp. Broad across months (max 22%) and
  market-families (max 9%).

---

## 5. Mesh-collapse ON sensitivity

Applying the frozen network-free co-trading collapse (sizeCV ≤ 0.35, ≥3 wallets, 12h
first-bet window) merges only ~30 of 870 footprints into actors: **E = +0.0215 (identical),
n_eff 439 → 387.** ON vs OFF makes **no material difference** — the one known coordinated
sybil cluster (Mojtaba) is off-L1, and organic L1 footprints rarely co-trade tightly. The
mesh operationalization gap flagged in the addendum is **moot for this verdict.**

---

## 6. Thesis-2 (>90d) — reported, NON-verdict-bearing

Per PR-6, Thesis-2 is **INSUFFICIENT by structure** for the live platform (a >90d footprint
detected in 2026 cannot resolve before the freeze; 2026 T2 = n=2). On the gradeable 2025
data (429 footprints, 110 markets), the long-dated follower edge is **negative: E = −1.48pp**
(CI (−5.7pp, +2.5pp)). Long-dated footprints show **no informational content** — matching
the M0-B2 a-priori. This cannot upgrade the Thesis-1 verdict and is not extrapolated.

---

## 7. Adversarial review + limitations

The pipeline was adversarially reviewed against the frozen pre-registration (6 dimensions,
independent verification: **12 findings confirmed, 3 refuted**). **Two verdict-affecting
defects were found and FIXED before this verdict:** (1) the +60 robustness gate wrongly
demanded bootstrap significance instead of the pre-registered point-estimate check (biased
toward NO-GO) → corrected; (2) the regime label keyed off the Phase-1 best-case anchor
rather than the exact detection_ts that anchors grading/thesis → corrected to detection_ts.

Verification performed: the fast composite matches `m0f.score_candidates_as_of` **20/20**;
the follower price matches `m0c.price_at` (30/30); the primary point estimates were
reproduced by an **independent code path** (E, n_eff, benchmarks, per-regime, ≤7d,
monotonicity — all identical); and the market-block CI was **reproduced across 3 seeds and
two implementations** (E=0.0215, CI≈(−0.032,+0.071), *p*≈0.21).

Non-verdict-affecting findings (disclosed): matched-control benchmark_b uses a within-cell
fallback for empty (price,stake) cells (only *widens* E−b's CI — conservative, cannot
create a GO); **0 enrichment errors**, so the Rule-1 exclude-on-error path never triggered.

**Residual scope limits (pre-registered, not resolved):** the detector's factor weights +
tier scale inherit the n=1 M0-F calibration (fresh universe for edge, once-calibrated
detector); the +30 follower fill is the one edge-optimistic axis; and — mirroring M0-C — the
live-platform (post-freeze) regime is not reachable on L1 and would need the L2 forward pass.

---

## 8. What this authorizes

**A NO-GO.** Detector-B footprints carry no *tradeable* follower edge beyond the +30-min
entry price on the pre-registered primary cell. **M10 stays dossier-only** — an intelligence
product, never a trade signal; no EV is estimated, ever; no individual behind any wallet is
named.

**Forward hypotheses (flagged, not acted on; require a new dated pre-registration — they
cannot rescue this NO-GO):** the edge concentrates where the theory predicts — in the
**highest-conviction (CRITICAL, which significantly beats the field) and shortest-dated (≤7d,
+5.3pp)** footprints. A future test of a *tighter* cell (CRITICAL ∩ ≤7d) on the L2 forward
tape is the honest next question — but on **this** sample, at the pre-registered bar, the
answer is NO-GO.
