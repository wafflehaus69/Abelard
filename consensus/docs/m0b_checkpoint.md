# M0-B Design Checkpoint + FROZEN Pre-Registration (Addenda v1.10 §4 / v1.11) — committed before any outcome

**Status: NO sweep, no enrichment, and no outcome grading have run.** This document is
outcome-blind by construction — it commits the design *before* results exist. The five
required items (§4.1–§4.5) are below, followed by the **frozen** pre-registration (§5).

---

## v1.11 RULING — applied (decisions resolved)

- **Scope (Decision 1):** run **(A) the cached 936-market sample now, network-free** — the cheap
  on-disk test answers the primary question and gates the rest. **(B)** more mid-band markets in
  the *same* 2025–2026 window is a **conditional follow-on**, pre-approved only if (A) returns GO
  or a near-miss (to adjudicate the per-regime decay guard at power). **(C)** 2023–2024 is
  **declined**: a >90d footprint detected in 2026 cannot resolve before the 2026-04-28 boundary, so
  **Thesis-2 is structurally testable only on stale regimes and can never speak to the live
  platform — that impossibility is itself a recorded finding.** Thesis-2 in this run: **INSUFFICIENT
  by structure**, not softened, not extrapolated.
- **Enrichment (Decision 2):** primary verdict is **network-free** (primary cell is latency-OFF);
  the 36 secondary latency cells are deferred. Caveat carried into the report: M5's value lived in
  the *conjunction* (fill-factors ∧ latency), so a latency-OFF primary tests the **weaker**
  configuration — **a primary NO-GO does NOT close Detector B's edge question; the secondary latency
  cells must still run.** Both reported.
- **Reporting-headline framing (v1.11 §0):** even the "POWERED" aggregate (MDE ≈ 0.064) detects only
  fairly large effects; a realistic 2–5pp detector edge could be missed. **A null verdict is reported
  in the headline as "no *large* edge detected (MDE floor X)," never "no edge."**

---

## 1. Footprint census (§4.1)

Extracted **as-of, zero-lookahead** from the cached L1 subgraph tape (935-market M0-C
mid-band pull). A footprint = a `(wallet, market, outcome-token)` position whose cumulative
**net-long stake first crosses $10k at a contested entry price (0.10–0.90)** before
resolution; `detection_ts` = that crossing. All universe markets are resolved (universe.json
is resolved-only), so every footprint is gradeable.

**936 markets processed → 2,805 footprints.** Raw counts by thesis × regime:

| Thesis | 2025-H1 | 2025-H2 | 2026-Jan-Apr | Total |
|---|---|---|---|---|
| **T1 ≤7d** | 374 | 260 | 573 | 1,207 |
| **T1 8–30d** | 279 | 226 | 236 | 741 |
| mid 31–90d | 238 | 232 | 75 | 545 |
| **T2 >90d** | 207 | 104 | **1** | 312 |
| **Total** | 1,098 | 822 | 885 | **2,805** |

days-to-resolution: median 10, p10 0, p90 101.

**Two coverage facts the reader must see (Rule 1, no silent truncation):**
- **Only 936 of the universe's 9,964 resolved target markets are cached** (the rest were
  lost to M0-C's mid-pull network outage; see docs/m0c_access_pattern.md). This census is a
  mid-band 2025→2026-Apr *sample*, not the population. Completing the pull would multiply
  footprints ~10×.
- **2023 and 2024 regimes are not cached at all** (replay window opens 2025-01-01). The
  regime-decay guard therefore runs on 2025-H1 / 2025-H2 / 2026-Jan-Apr only. Extending to
  2023–2024 — the deep history where Thesis-2 long-dated markets have *resolved* — is a
  separate subgraph pull-back, priced in §4 below, and is the single biggest lever on
  whether Thesis-2 can be tested honestly (per v1.10 §3).

**Right-censoring, correctly handled.** The T2/2026-Jan-Apr cell is **n=1** because a >90d
footprint detected in early 2026 resolves *after* the 2026-04-28 L1 boundary and is excluded,
not zeroed. That cell is structurally **UNDERPOWERED**, reported as such, never NO-GO.

---

## 2. Power / effective-n and the MDE table (§4.2)

The verdict rides on **effective** sample size, not raw count. Two haircuts apply and both
are knowable at pre-registration (they use notionals and market identity observed *at
detection*, independent of outcome):

- **Kish weight-haircut** (notional skew): `n_eff = (Σwᵢ)² / Σwᵢ²  =  n / (1 + CVw²)`.
  Prediction-market notionals are whale-dominated, so `n_eff ≪ n`.
- **News/market-block clustering** (correlated resolutions — the M0-C "22 signals = one
  May-2025 news event" failure): the honest independent-n is bounded by distinct
  markets/news-blocks, so `n_eff,indep ≤ n_eff,Kish`. Significance uses an **actor-level +
  block bootstrap**, never a per-footprint one.

**MDE = k · σ / √n_eff**, primary cell one-sided α=0.05, power 0.80 ⇒ **k = 2.486**;
σ = 0.50 (conservative Bernoulli ceiling) for the planning table, realized weighted SD used
post-hoc. Decision anchors: **plausible-edge ceiling = 0.10** (10pp — a follower entering
+30 min after the price already absorbed the footprint cannot plausibly retain more), and a
**tradeable floor = 0.05** (net of ~2–4% round-trip friction).

| n_eff | MDE (σ=0.50) | label |
|---|---|---|
| 15 | 0.32 | UNDERPOWERED |
| 30 | 0.23 | UNDERPOWERED |
| 50 | 0.18 | UNDERPOWERED |
| 100 | 0.12 | UNDERPOWERED (> 0.10 ceiling) |
| 155 | 0.10 | threshold: powered for the *ceiling* |
| 200 | 0.088 | PARTIALLY POWERED |
| 400 | 0.062 | PARTIALLY POWERED |
| 618 | 0.05 | FULLY POWERED for a trade decision |

**Rule:** MDE > 0.10 ⇒ **UNDERPOWERED = "absence of evidence, cannot resolve"** (never
NO-GO). 0.05 < MDE ≤ 0.10 ⇒ PARTIALLY POWERED (report the detectable floor). MDE ≤ 0.05 ⇒
fully powered. Thesis-2 applies the ceiling on the **annualized** edge.

### Per-bucket effective n (measured, stage-1 $10k-contested level)

Two effective-n columns: **Kish** (weight-haircut) and **market-blocks** (distinct markets =
the independent unit the actor+block bootstrap actually resamples). **The block count is the
binding constraint** — the bootstrap draws whole markets/news-events, so correlated
footprints inside a market count once. MDE is reported at the block count, σ=0.50, k=2.486.

| Thesis × regime | raw n | Kish n_eff | markets (blocks) | MDE @ blocks | power label |
|---|---|---|---|---|---|
| T1 ≤30d · 2025-H1 | 653 | 272 | **83** | 0.136 | UNDERPOWERED (block-limited) |
| T1 ≤30d · 2025-H2 | 486 | 160 | **108** | 0.120 | UNDERPOWERED |
| T1 ≤30d · 2026-Jan-Apr | 809 | 260 | **187** | 0.091 | PARTIAL (beats ceiling) |
| **T1 ≤30d · AGGREGATE** | 1,948 | 686 | **374** | **0.064** | **POWERED** |
| mid 31–90d · AGG | 545 | ~215 | ~121 | ~0.113 | borderline |
| T2 >90d · 2025-H1 | 207 | 50 | 33 | 0.216 | UNDERPOWERED |
| T2 >90d · 2025-H2 | 104 | 41 | 32 | 0.220 | UNDERPOWERED |
| T2 >90d · 2026-Jan-Apr | **1** | 1 | 1 | — | structurally INSUFFICIENT |
| **T2 >90d · AGGREGATE** | 312 | 85 | **51** | **0.174** | **UNDERPOWERED** |

**The honest reckoning — and it is the headline of this checkpoint:**

- **Thesis-1 *aggregate* is genuinely powered** — 374 independent market-blocks, MDE 0.064,
  comfortably below the 0.10 plausible-edge ceiling. On the cached sample, M0-B can deliver a
  real aggregate Thesis-1 verdict.
- **Thesis-1 *per-regime* is block-limited and UNDERPOWERED in 2025** (83 / 108 blocks → MDE
  0.12–0.14, above the ceiling); 2026 is only PARTIAL (0.091). Yet the GO rule's decay guard
  (PR-5.3) *requires* per-regime positivity, especially in 2026. So the decay guard will be
  adjudicated at **partial power** on this sample — a real limitation, not a defect.
- **The ELEVATED fill-bar cuts n further.** The primary cell is a ~¼–⅓ subset of the above,
  so per-regime primary-cell blocks likely fall to ~25–60 → per-regime primary MDE ~0.15–0.25
  ⇒ **per-regime primary cell is expected UNDERPOWERED**, while the *aggregate* primary cell
  should remain testable (~120–150 blocks, MDE ~0.10).
- **Thesis-2 is UNDERPOWERED everywhere** (51 blocks aggregate, MDE 0.17; 2026 = n=1) — which
  matches the a-priori from M0-B2. Expect Thesis-2 = INSUFFICIENT on the cached sample.

**Consequence for the ruling:** the cached 936-market sample can return a **powered aggregate
Thesis-1 GO/NO-GO**, but the **per-regime decay guard and any Thesis-2 test are block-starved
and will read INSUFFICIENT** unless the pull-back (§1/§4) adds markets — more distinct blocks
is the *only* lever that fixes this (a looser threshold would not, and is forbidden). This is
the concrete trade-off behind decision #1 below.

---

## 3. Sweep cell count / degrees of freedom (§4.3)

Grid = composite_threshold {0.30, 0.50, 0.70} × latency_elevator {off, tight, graduated} ×
entry_lag {+30, +60, +120} × mesh_collapse {off, on} = **54 cells.**

- **Strata (2 theses × 3 regimes) multiply *reporting*, not DoF** → 54 × 6 = 324 displayed
  numbers, but the regime axis is *not* a free search axis (the decay guard forces reporting
  all regimes, not picking the best).
- **Primary verdict DoF = 1** (the single pre-registered cell) → **no multiplicity
  correction on the verdict.**
- Secondary best-cell mining is corrected by **max-T permutation** (B=10,000; per footprint
  redraw `outcome* ~ Bernoulli(follower_entry_price)`; p_bestcell from the max over 54 cells),
  Holm as a conservative cross-check. Secondary cells are **labeled** and **cannot upgrade a
  primary NO-GO** (the exact discipline M0-C's walked-back "+10.6%" violated).

---

## 4. Enrichment budget (§4.4)

**The primary cell is `latency_elevator = OFF` — so the verdict requires ZERO network.** All
L1 fills + resolutions are cached; the primary edge computation, both benchmarks, and the
significance bootstrap run entirely on-disk.

Etherscan enrichment is needed **only for the 36 secondary `latency ∈ {tight, graduated}`
cells**, gated behind the fill-bar (only footprints clearing the composite threshold are
enriched), deduped by funder. Estimate: ~850–1,100 fill-bar footprints → ~700–900 distinct
wallets × ~1.5 tokentx calls (funder dedup) ≈ **~1,000–1,400 Etherscan calls**, ~**8–12 min**
wall-clock at the free-tier ~4/s rate (well inside 100k/day). Because it touches only
secondary cells, it can be **run second, or deferred entirely** for a network-free first
verdict — the architect's call.

**Coverage pull-back cost (the real budget question).** Extending the L1 universe from the
cached 936 to the full ~9,964 resolved target markets, and back through 2023–2024, is a
subgraph walk at ~1.6s/token/market (per M0-C's `_pull_full`). Order-of-magnitude:
~9k uncached markets × 2 tokens × ~1.6s ≈ **~8 hours of subgraph pulling** (resumable,
cache-through, no key). This is the gating decision for whether M0-B runs on the mid-band
2025–2026 sample it has now, or the fuller cross-regime universe the sweep design assumes.

---

## 5. Pre-registration (§4.5) — BINDING

*Committed before any M0-B result is seen. The verdict rides on the single primary cell and
the conjunctive GO rule below. No number here may be relaxed, re-binned, or re-anchored after
results are observed; a change is a new, dated pre-registration with the reason stated.
Priors from M0-F / M5 / M0-C / M0-B2 are legitimate; using M0-B's own output to set any of
these is not.*

**PR-1 Primary hypothesis (H1, verdict-bearing).** On the cached L1 universe, **Thesis-1
(resolves ≤30d of detection)** footprints clearing the **ELEVATED** composite gate,
aggregated **per-actor (mesh-collapsed)** and **size-weighted**, earn a follower edge that
is (a) significantly > 0, (b) significantly greater than **both** benchmarks on the identical
markets/windows, and (c) not an artifact of one regime, one month, one market-family, or one
whale. **H0:** edge ≤ max(0, benchmarks), OR positive-but-regime-concentrated (absent/negative
in 2026-Jan-Apr), OR n below the power floor (→ INSUFFICIENT-DATA). **Thesis-2 (>90d)** is
reported with equal prominence but is **not verdict-bearing**; a priori (M0-B2) it is expected
INSUFFICIENT.

**PR-2 Primary metric.** `edgeᵢ = outcomeᵢ − pᵢ(+30)`, `pᵢ(+30)` = first on-tape trade price
on the footprint's side at/after `detection_ts + 30min` (else mid+half-spread, taker-conservative);
`wᵢ` = actor net stake USDC. Statistic **E = Σwᵢ·edgeᵢ / Σwᵢ**. Significance: **actor-level +
news-block bootstrap, 10,000 resamples**, 95% CI on E and on (E − benchmark) recomputed on the
same resampled markets. Report **Kish n_eff**; equal-weighted E reported alongside (sign must
match, else "whale-dependent" → fails PR-5). **Hit-rate diagnostic only.**

**PR-3 Primary cell (a priori most defensible, NOT best-performing):**
`{ composite = ELEVATED 0.50 · latency_elevator = OFF · entry_lag = +30min · mesh_collapse = ON }`,
verdict stratum **Thesis-1 ≤30d** (nested ≤7d reported). Anti-peek core: **three of the four
axes are set to the choice that *reduces* power/edge** (ELEVATED not WATCH; latency OFF;
mesh-collapse ON); only entry_lag is the architect-fixed optimistic default, robustness-guarded
at +60 in the GO. The primary cell is biased toward being *harder* to pass.

**PR-4 Benchmarks (both, paired, frozen):** (a) **contested-slice** = all 0.10–0.90 trades in
the same markets/windows (excl. the footprint actor); (b) **size+price-matched control** =
K=20–50 non-flagged positions matched on market (else regime×category) with calipers
|Δlog stake|≤0.25, |Δprice|≤0.05, |Δt|≤entry_lag, scored through the same +30 path. GO is an
**intersection-union test** (beat both) — Type-I bounded by max(αₐ,α_b), no upward correction.

**PR-5 GO rule — GO requires ALL of:**
1. CI-lower(E) > 0.
2. CI-lower(E − a) > 0 **and** CI-lower(E − b) > 0; **materiality** point (E − b) ≥ **+0.03**.
3. **Regime — asymmetric (v1.11 Amendment 1; 2026 is only marginally powered, MDE≈0.091, so
   demanding *significance* there would be false rigor):**
   - **Required for GO:** the **2026-Jan-Apr point estimate is positive** (its CI is reported).
   - **Automatic NO-GO:** the **2026-Jan-Apr point estimate is negative**, regardless of how
     strong the aggregate is (the aggregate must never rescue a dead recent regime — the M0-C
     lesson). Significance in 2026 is **reported, not required**.
   - Plus: positive in **≥2/3** regime slices.
4. **Power — on the PRIMARY CELL's own n_eff (v1.11 Amendment 2):** the ELEVATED fill-bar cuts the
   primary cell below the census totals, so the **primary-cell `n_eff` is computed and reported
   with the verdict**. Floors: **n_eff ≥ 30 aggregate AND ≥ 10 in 2026-Jan-Apr**. If the primary
   cell's own n_eff is below floor, the verdict is **UNDERPOWERED / INSUFFICIENT-DATA** (absence of
   evidence, correctly labelled) — **never NO-GO**.
5. **Broad, not a spike:** any single month ≤40% of size-weight; any market-family ≤25%; the
   ≤7d sub-stratum matches sign; edge survives entry_lag +60 (>0 and beats both benchmarks);
   equal-weighted matches sign.

Tier-monotonicity (E rising WATCH→ELEVATED→CRITICAL) is reported as supporting evidence, not
a hard gate. **Slippage:** follower stake pre-registered at **$1,000**; a footprint whose
follower fill is not achievable from at-or-better realized volume in `[+30min, +6h]` is
**disqualified (declared, excluded)**, not counted as edge.

**PR-6 Thesis-2 — INSUFFICIENT by structure (v1.11 §1), reported, non-verdict.** A >90d footprint
detected in 2026 cannot resolve before the 2026-04-28 L1 boundary, so Thesis-2 is testable only on
2023–2025 (stale) regimes and **can never speak to the live platform** — recorded as a finding in
its own right, not a number to soften. Where gradeable at all, `edge_ann = (1+E)^(365/median_hold_days) − 1`
is reported with the holding-period distribution + exit-liquidity reality; a Thesis-2 result
**cannot upgrade** a Thesis-1 NO-GO/INSUFFICIENT and is not extrapolated to the live platform.

**PR-7 Computation posture & governance.**
- **Factors:** primary composite is full **F/S/D/C/T** (m0f weights). **F is computed from the
  frozen, keyless L1 subgraph** (the L1 tape source, cache-through), which is L1 access, NOT the
  deferred Etherscan enrichment; a wallet whose F lookup fails is **`data_incomplete` → excluded**
  (Rule 1), never imputed. **"Network-free" = no Etherscan** (latency-OFF) — the verdict cannot be
  contaminated by a live-API failure or rate limit.
- **Pre-registration is FROZEN at this commit.** No threshold, cell, benchmark, or GO condition may
  be altered after outcomes are computed; any unavoidable deviation is reported loudly with reason
  (v1.11 Amendment 3). The report **cites this commit's hash**.
- **No file-drawer:** `docs/m0b_report.md` is published **regardless of outcome** — a NO-GO/UNDERPOWERED
  is as valuable as M0-C's was.

**Deliverable:** `docs/m0b_report.md`, headline **GO / NO-GO / UNDERPOWERED** with the v1.11 §0
"no *large* edge (MDE floor)" framing on any null, primary-cell n_eff stated, Thesis-2
INSUFFICIENT-by-structure, both benchmarks, per-regime with the asymmetric 2026 rule. A GO
authorizes only the next design conversation, never operational staging; M10 stays dossier-only.

---

## What I need from the architect (the checkpoint decision)

1. **Run scope:** M0-B now on the cached **936-market, 2025→2026-Apr sample** (fast, verdict
   in the current regimes) — or first pay the **~8-hour pull-back** to the full universe +
   2023–2024 (the honest Thesis-2 test, and ~10× the n)?
2. **Enrichment:** network-free primary verdict first (primary cell is latency-OFF), with the
   secondary latency cells run/deferred separately — acceptable?
3. **Pre-registration:** approve §5 as written, or amend any bound before it is frozen.
