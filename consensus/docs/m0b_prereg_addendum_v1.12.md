# M0-B Pre-Registration Addendum v1.12 — Sample reconciliation + mesh-collapse operationalization

**Dated 2026-07-24. Parent frozen pre-registration: commit `8cc57c3` (`docs/m0b_checkpoint.md`).**

*This addendum is committed BEFORE any M0-B outcome or grading is computed — the
Amendment-3 discipline (freeze before outcomes, cite the hash, no file-drawer) is
preserved. It records a change to the **sample** (a strictly-larger, outcome-blind
cache) and closes one **operational gap** the committed checkpoint left open
(mesh-collapse). It changes **no** binding threshold, cell, benchmark bin, or GO
condition. Every deviation is reported loudly per PR-7.*

---

## 0. Ruling applied

**v1.11 Option 1** (Mando relaying the architect, 2026-07-24): accept the larger
outcome-blind cache as PR-0's "cached L1 universe"; keep **all** binding parameters
frozen at `8cc57c3`; issue this dated addendum and then run the frozen primary
verdict on the current cache.

---

## 1. Sample reconciliation (report loudly — PR-7)

| | Frozen census (`8cc57c3`, Jul 22) | Current cache (Jul 24) |
|---|---|---|
| Cached markets (nonempty L1 fills) | 936 | **6,122** |
| …with ≥ 1 footprint | 936 | **1,364** |
| Footprints ($10k net-long, contested 0.10–0.90) | 2,805 | **15,768** |
| Footprint wallets | — | **4,335** |

**This is not a bug.** The extractor is byte-identical to the frozen `census2`
(same `_pull_full`, same `since_ts`, same net-crossing at $10k). Reconciliation
confirms **0 out-of-regime footprints**, detection range **2025-01-01 → 2026-04-27**
— every footprint is in-window.

**Cause.** 32,828 goldsky-subgraph rows were fetched on **Jul 23** — the day *after*
the `8cc57c3` commit — by a failed `prefer_cache`+live extraction run that pulled far
more than "timed out early" implied. The cached L1 subset grew 936 → 6,122 markets.
**Only fills were pulled; no grading or outcome was ever computed** on the new data.

**Integrity.** Because the growth *preceded any verdict computation*, an
"expand-until-GO" p-hack is structurally impossible — no M0-B result had been seen
when the sample changed. This is a strictly-larger, outcome-blind sample of the same
universe PR-0 already defines (cached L1 subgraph tape, geopolitics/politics,
2025-01 → 2026-04). It is *not* the 2023–2024 pull-back (declined, and still declined
— replay opens 2025-01-01). It is the mid-band 2025–2026 completion the checkpoint
called "the pull-back," now cached at zero additional cost.

---

## 2. Binding parameters — UNCHANGED

PR-1 … PR-8 (primary hypothesis, metric, primary cell, both benchmarks, the
five-part GO rule, power floors, Thesis-2 framing, secondary/post-hoc handling,
governance) remain **exactly** as frozen at `8cc57c3`. In particular the **power
floors are absolute** — `n_eff ≥ 30` aggregate **and** `n_eff ≥ 10` in 2026-Jan-Apr —
not relative to the census. A larger sample only makes them easier to clear, which
**strengthens, never weakens,** the test. The primary-cell `n_eff` (post-ELEVATED cut)
is still computed and reported *with* the verdict per Amendment 2.

---

## 3. Enrichment exclusion finding (the architect-ordered deliverable)

F-enrichment sources `first_seen` from maker-side L1-subgraph events. The concern was
that pure-taker footprint wallets (no maker history) would be silently excluded,
biasing the surviving sample.

**Result: 0 of 4,335 footprint wallets excluded (0.0%).** Every footprint wallet has
maker-side subgraph history, so F is computable for the **entire** sample. The
exclusion mechanism was validated (two no-history wallets correctly returned
`first_seen = None → EXCLUDED`), so the zero is real, not an artifact.

**Consequences (per the exclusion instruction):** the excluded set is empty → there is
nothing to test for systematic difference; **post-exclusion `n_eff` = full-sample
`n_eff`**; the pre-registered floors apply to the full surviving sample; **no stated
limitation on the verdict arises from enrichment.**

---

## 4. Updated stage-1 census + power (the declined pull-back, now cached)

Stage-1 = $10k net-long, contested 0.10–0.90, zero-lookahead (the census level,
*before* the ELEVATED-0.50 cut). `MDE = 2.486·σ/√n_eff`, σ = 0.50, reported at the
market-**block** count (the unit the actor+block bootstrap resamples). Kish `n_eff`
column exposes whale-domination.

| Thesis × regime | raw n | Kish n_eff | blocks | MDE @ blocks | label |
|---|---|---|---|---|---|
| T1 ≤30d · 2025-H1 | 4,822 | 1,018 | **331** | 0.068 | **POWERED** (was UNDER, 83 blk) |
| T1 ≤30d · 2025-H2 | 2,643 | 699 | **323** | 0.069 | **POWERED** (was UNDER, 108 blk) |
| T1 ≤30d · 2026-Jan-Apr | 3,554 | 961 | **489** | 0.056 | **POWERED** (was PARTIAL, 187 blk) |
| **T1 ≤30d · AGGREGATE** | **11,019** | **2,523** | **1,130** | **0.037** | **FULLY POWERED** (was 374 blk) |
| mid 31–90d · AGG | 3,187 | ~430 | 357 | 0.066 | powered |
| T2 >90d · 2025-H1 | 1,140 | 44.5 | 143 | 0.104 (Kish-binding 0.186) | UNDERPOWERED (whale) |
| T2 >90d · 2025-H2 | 420 | 93.0 | 70 | 0.149 | UNDERPOWERED |
| T2 >90d · 2026-Jan-Apr | 2 | 1.4 | 2 | — | structurally INSUFFICIENT |
| **T2 >90d · AGGREGATE** | **1,562** | **75.4** | **181** | 0.092 (**Kish-binding 0.143**) | **UNDERPOWERED (whale-dominated)** |

**Headline.** The per-regime Thesis-1 decay guard — which the checkpoint said would be
adjudicated *underpowered* (§2) — is now **well-powered in every regime** (MDE
0.056–0.069, all below the 0.10 plausible-edge ceiling). The T1 aggregate is fully
powered (MDE ≈ 0.037). **Thesis-2** rises to 181 blocks (from 51) but its Kish `n_eff`
is only 75 (DEFF 25.6 in 2025-H1 — whale-dominated), so on the binding haircut it stays
**UNDERPOWERED**; per PR-6 it remains **non-verdict-bearing, stale-regime-only, and
cannot speak to the live platform** (2026 is n=2, structurally insufficient). The extra
2025 T2 data lets us *compute* a Thesis-2 number; it does not make it verdict-bearing.

---

## 5. Mesh-collapse operationalization — FROZEN here (closing the `8cc57c3` gap)

The committed checkpoint set `mesh_collapse = ON` in the primary cell (PR-3) and
"aggregated per-actor (mesh-collapsed)" in PR-1, but **never operationalized the
collapse** — the design-draft's PR-7 gate ("mesh-collapse thresholds frozen from the
`m10_labels.jsonl` precedent *before* the run; if not frozen, mesh_collapse ON cannot be
primary — the collapse rule would be a live DoF tuned on M0-B") was dropped from the
committed text. Closing it now, pre-outcome:

**The collapse must be network-free** (the primary cell excludes Etherscan, so the
funder-mesh density signal is unavailable). The only network-free coordination signal
is co-trading structure. The documented precedent discriminators are themselves
network-free: **Mojtaba** (coordinated, 20→1) had `sizeCV 0.03`, `1.5 h` entry window;
the two organic controls (**US-invade-Iran**, **Fed-no-change**, *not* collapsed) had
`sizeCV 0.66`, `9–125 h` windows.

**Frozen network-free collapse rule (from precedent + pre-existing config, not tuned on
M0-B):** within a market, a group of **≥ `cluster_min` (=3, config)** same-side
footprints whose first-bet timestamps span **≤ `cluster_window_hours` (=12, config)**
**and** whose net-stake **`sizeCV ≤ 0.35`** (strictly between the coordinated 0.03 and
organic 0.66 precedents; a value pinned *from* the precedent, chosen before any M0-B
outcome) collapses to **one actor** (stakes summed; one bootstrap unit).

**Gate verdict.** All three labeled coordination cases (Mojtaba + the two organic
controls) are **LIVE-tape (L2, Jul-2026), post-L1-freeze — none are in the L1 cache**,
so this rule **cannot be gate-verified against the precedent on L1**. Per the PR-7 gate,
an operationalization that cannot be verified against precedent **must not silently
carry the verdict**. Therefore:

- **Operative primary = `mesh_collapse = OFF` (per-wallet).** Clean, no unverified DoF.
- **`mesh_collapse = ON` (the frozen rule above) is computed and reported as a
  pre-registered SENSITIVITY**, with the ON−OFF delta shown.

This is a **pre-registered deviation** from the frozen primary cell (mesh ON → OFF),
**driven by the gate, not chosen ad-hoc**, and reported loudly. Both settings are
already sweep axes, so redesignating "operative" (should the architect prefer ON)
requires **no recompute** — only a relabel.

**Note on direction.** OFF does *not* anti-sybil-deflate, so it forgoes the checkpoint's
"reduces-n / anti-flatter" intent for this axis. That is acceptable here because (i) the
one known coordinated cluster (Mojtaba) is off-L1, so no sybil inflation is present in
this sample to correct; (ii) the ON sensitivity quantifies any residual; (iii) the
equal-weighted and broad-not-spike GO checks (PR-5.5.i/ii/v) already guard against a
single-cluster or single-whale artifact. If the ON sensitivity materially moves the
verdict, that is itself the finding and the architect rules.

---

## 6. Operational definitions reaffirmed / pinned (pre-outcome)

Reaffirming the checkpoint's frozen definitions and pinning the residual implementation
choices before the run:

- **detection_ts** = the first as-of timestamp at which the footprint's composite crosses
  **ELEVATED (0.50)**, tier-latched; **one detection per (actor, market-family)**.
- **Composite** = weighted geometric mean over active **F/S/D/C/T** exactly as `m0f.py`
  computes, weights `m0f.factor_weights` `{F:1.0,S:1.0,A:0.7,D:0.8,C:0.8,T:0.5,P:1.0}`;
  **A and P excluded on L1** (declared, not imputed). Scoring **scope = per-market fills**
  (the natural unit; trailing-volume and detection are per-market). If the composite is
  computed via a prefix-sum reimplementation for speed, it is **verified equal to
  `score_candidates_as_of`** on a random sample before use.
- **F-enrichment** sources `first_seen`/`prior_fills` from the **L1 subgraph** (cache-
  through; PR-7-sanctioned — "network-free" means **no Etherscan**, not no subgraph).
- **follower_entry** `p(+30)` = first on-tape trade price on the footprint's side at/after
  `detection_ts + 30 min` (`price_at`, 6 h window), taker-conservative. A footprint with
  **no realizable fill in `[+30 min, +6 h]` is disqualified — declared and excluded**
  (PR-5 slippage), never imputed.
- **Benchmark bins** = entry-price decile × stake decile, non-gate wallets, contested
  0.10–0.90, same markets/windows, same +30 path, resolved-only, size-weighted (PR-4).

---

## 7. Parent + posture

Parent frozen pre-registration: **`8cc57c3`**. This addendum's own commit hash is cited
in `docs/m0b_report.md`. **No file-drawer:** the report is published regardless of
outcome. A GO authorizes only the next design conversation — never operational staging;
**M10 stays dossier-only.**
