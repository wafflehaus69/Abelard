# M0-C — Consensus Replay Report (Powered Run)

**Milestone:** M0-C (spec v1.0 §M0-C + addenda v1.2 §4 / v1.4 §5 / v1.5 §5)
**Run date:** 2026-07-15 · **Data:** L1 archival tape (network-recovered) · **Scope:** 935 mid-band markets, 2025-01 → 2026-04.

---

## 0. Headline verdict

**NO-GO on the current regime.** The consensus mechanic shows **no demonstrable, regime-stable edge**. The sweep's mechanical "GO" is an artifact of aggregating across regimes: the apparent +9–10% edge is concentrated almost entirely in a single ~3-month window in **spring 2025** and has **decayed to zero by 2026** — the period closest to the live platform.

This is the outcome v1.2 §4 explicitly anticipated: *"if the edge decays over time, that trend line is the real finding."* It is a success of the backtest process, not a failure of the build — the harness did its job by refusing to let a stale artifact reach a live pipeline.

## 1. The run

- **935 markets** (of ~3,692 mid-band; the rest lost to a mid-pull network outage, see docs/m0c_access_pattern.md), **977,101 realized wallet-market edges**. Mega-markets excluded by the 200k–3M cap (v1.4 §5.1).
- Zero-lookahead replay: as-of roster (decayed size-weighted edge-over-entry; win-rate never ranked), consensus scan (participation floor × agreement × remaining-edge + freshness + price-ceiling gates), outcome measured at the **owner-realistic +30-min entry**, first-signal-per-market dedup.
- 81-cell parameter sweep (participation_floor × agreement × K × max_edge_paid).

## 2. What the mechanical decision rule said (and why it misleads)

The code's rule — ≥1 cell with ≥10 tradeable signals at positive expectancy — returned **GO** (27 of 81 cells qualified). Best cell (pf=3, ag=0.7, k=25, mep=0.2): **23 tradeable signals, 78% hit, +10.6% mean realized edge**.

Two features of that "GO" are red flags, both confirmed below:
- Every qualifying cell sits at **maximum permissiveness** — k=25 (largest circle), ag=0.7 (loosest agreement), pf=3 (lowest floor). That is the overfitting signature of 81-way multiple testing on a small signal count.
- The **regime slices contradict the aggregate**: at default params, 2025-H1 had 9 signals at **−3.1% (negative)** edge, and 2025-H2 / 2026 had **zero**.

## 3. The real finding — regime decomposition (best cell)

Temporal distribution of the best cell's 23 tradeable signals, by market resolution:

| Regime | resolved-market supply | tradeable signals | mean edge | wins |
|---|---|---|---|---|
| 2025-H1 | 215 | **22** | **+9.2%** | 17/22 |
| 2025-H2 | 246 | 1 | +42% (n=1) | 1/1 |
| **2026-Jan–Apr** | **474** | **0** | — | — |

And within 2025-H1, the signals concentrate further: **15 of 22 resolved in a single month (May 2025)**.

**The supply confound is ruled out.** 2026-Jan–Apr has the *most* resolved markets (474 — 2.2× the 215 in 2025-H1) yet produces **zero** consensus signals. The mechanic is not starved for markets in 2026; it stops firing. The edge is not "thin in 2026" — it is **absent**.

**Even the 2025-H1 edge claim is soft (v1.6 §1).** The best cell was selected **post-hoc** from an 81-cell sweep, clustered at maximum permissiveness (largest circle, loosest agreement), in-sample, n=23 concentrated in one quarter (two-thirds in May 2025) — plausibly a handful of correlated markets driven by common news, not 22 independent bets. The defensible summary is: **"possibly a modest edge in early 2025; demonstrably not firing now"** — not "+10.6% edge that decayed." The 10.6% figure is a post-hoc-selected in-sample number and should not be quoted as a measured edge.

**Scope limits stand as pre-registered (v1.6 §1), none of which rescue the verdict:** mid-band only (mega-markets excluded), pre-June-2026-split regime, network-recovered 935/3,692 universe. The decay pattern is the dominant signal regardless; these bound *what was measured*, not the conclusion.

## 4. Interpretation (offered, not asserted)

Why would a consensus signal be present in spring 2025 and gone by 2026?
- **Roster dispersion / platform maturation.** The winners-circle roster is built from resolved-trade edge on frozen-tape data; the co-participation of skilled wallets that produced convergence in early 2025 does not persist.
- **Pre-split regime.** v1.2 §4 warned the replay is almost entirely pre-June-2026-split; the decay is already complete *before* the split boundary, suggesting the mechanic was fading independent of it.
- **Arbitrage.** The simplest reading: any edge from "follow the proven wallets' consensus" was competed away as the platform grew.

## 4b. Gate-attribution — WHY zero signals in 2026 (addendum v1.6 §2)

The decisive question: are the 2026 zeros **World A** (convergences happen but the remaining-edge gate kills them — edge arbitraged, mechanic honestly dead) or **World B** (participation fails — a current-regime re-bootstrap might revive it)?

> **CORRECTION (2026-07-15).** An earlier draft of this section concluded "World B, the roster collapsed to ~1 wallet." **That was wrong — a bug in the first diagnostic**, which mis-built the as-of roster. The corrected diagnostic mirrors the validated `replay()` path exactly (real roster via the sweep precompute), and a separate roster-supply reconciliation confirms it. The corrected findings replace it below. The top-line NO-GO is unchanged; only the *mechanism* changed.

**Fact 1 — the roster is HEALTHY in 2026, not collapsed.** Eligible-wallet supply grows monotonically: 281 (2025-05) → 3,963 (2026-01) → **7,983 (2026-04)**; the top-K roster is full (15/25) throughout, and the 15th-best wallet carries **+45% edge** by 2026-04. There is no circle collapse.

**Fact 2 — censoring is ruled out.** Roster supply by trade *resolution* (the real as-of roster) vs by trade *placement* (a censoring-free upper bound) converges from 14× (early 2025, lots of unresolved recent bets) to **1.0× by 2026-04**. Near the freeze there is a huge resolved history, so right-censoring barely thins the 2026 roster.

**Fact 3 — the real mechanism is DISPERSAL, not collapse.** Correct gate-attribution over the 2026-Jan–Apr slice (best cell, real roster median size 25) — roster-wallet co-participation per trading market:

| roster wallets holding a position in the same market | (market,date) evals |
|---|---|
| 0 | 2,858 |
| 1 | 141 |
| 2 | **5** |
| **≥3 (participation floor)** | **0** |

The 25 skilled wallets exist and are strong, but they **almost never hold positions in the same 2026 market** — ≥3 on one market happens *zero* times, so no convergence ever forms. In 2025-H1 the same mechanic produced 22 convergences; by 2026 the circle members have **gone their separate ways across a much larger market set**. Kill distribution confirms: 0 remaining-edge kills, 0 convergences. Still **not World A** (nothing is arbitraged, because nothing converges) — but the cause is dispersal, not a vanished circle.

**Consequence for the L2 confirmation pass — tempered, not a clean green light (v1.8 §2–3).** Dispersal is visible in **Jan–Apr 2026, before the June split** — so whatever spread the circle apart was already operating pre-split, and the split adds a second, permanent thinning (US sharps migrated off international Polymarket). That is the architect's mechanism **(c) — real, possibly-persistent dispersal**, the one that makes September a formality. *However*, two confounds prevent asserting (c) cleanly: the 2026 universe here is **network-truncated (935/3,692)** and **horizon-biased** (median 3 days to resolution — only fast-resolving markets appear before the freeze; docs §4b-supply). Co-participation is therefore **under-measured** — roster wallets may converge on markets absent from this sample. **Net:** the data leans toward real dispersal (c), but the truncation/horizon confounds keep it from being conclusive. Per v1.8 §3's decision rule, September is downgraded from "genuine second chance" to **"run it, but expect a formality,"** and the **cross-venue spread (v1.7 §3) rises to the primary forward hope**, not a parallel one. The full L2 archive (complete market coverage, forward resolution) is what would actually distinguish dispersal-real from sampling-artifact.

## 5. Verdict and consequence

- **Detector A (consensus — the primary product): no tradeable-edge verdict in its favor.** On current-regime data it does not demonstrably have edge. It remains viable as an *intelligence* tool (surfacing where skilled money agrees) but is **not** established as a trade signal.
- **M0-C is necessary-not-sufficient (v1.2 §4), and here it fails even the necessary test for the current regime.** The only path to a current-regime verdict is the **L2 forward-archive confirmation pass** — running the same mechanic on live-collected data once the collector has ~60–90 days, which aligns with the owner's paper-trading window. Until then, there is no basis to trade Detector A.

## 6. Scope limits (v1.4 §5, stated plainly)

1. **Mega-markets excluded for tractability** — verdict scoped to the mid-volume band; may not generalize to the highest-liquidity markets.
2. **935 of ~3,692 mid-band markets** (network-truncated pull; resumable now via `--resume`). A powered but partial sample — directional, and the regime-decay signal is strong enough that completing the remaining markets is unlikely to reverse it (2026 already has 474 markets and zero signals).
3. **In-sample, single-window.** No out-of-sample validation; the L2 confirmation pass is the out-of-sample test.
4. The sweep's mechanical GO/NO-GO aggregates across regimes and therefore **masks decay** — a known limitation; this report's regime decomposition is the authoritative verdict, not the `decision` field in sweep.json.
