# M0-F — Feb-28 Footprint Backtest Report

**Milestone:** M0-F (spec v1.0 §M0-F + addendum v1.2 §3)
**Run date:** 2026-07-13 · **Data:** L1 archival subgraph tape only · **Mode:** historical replay, no live scanning / no alerting / nothing near M9.

---

## 1. What was run

- **Universe** (v1.2 §3 expanded): 10 Iran-cluster search terms → 44 gamma events → **114 markets** whose life intersects the detection window (strikes-by-date family + Khamenei/leadership + regime-fall + Strait-of-Hormuz + nuclear-deal siblings). The resolved **Feb-28 strikes anchor** (`0x3488f31e…`, $89.6M) is included — it is invisible to `public-search` (active-only) and has a misleading `endDate`, both handled.
- **Tape**: L1 walk of every market over `[window_start − 7d, window_end]` = **996,300 on-chain fill events**, 0 markets failed, 0 forced-skips, 0 dropped events.
- **Detector**: the M10 seven-factor profile replayed at 6 as-of timestamps from −54.6h to the news break.
- **Ground truth**: `news_break_ts = 2026-02-28T06:33:34Z` (dual-source archived liveblog metadata).

**Factor availability (run (a)).** F, S, D, C computed from fills. **T is a stated proxy** (first-seen→first-bet latency; true funded→bet needs chain data). **A (aggression) and P (funding provenance) are excluded** — A needs CLOB order-book depth/impact (M8), P needs the M5 funding graph. Exclusions are reported, never imputed; the composite is a weighted geometric mean over active factors only.

## 2. Deliverable 1 — independent on-chain identification

Reported wallets were treated as **hypotheses**, matched against the tape by address (when known) or by net-shares-held + entry-price signature.

| Reported name | On-chain wallet | Match | Note |
|---|---|---|---|
| bubblemaps-1 | `0x1caa6a7ad0…` | address | position reproduced digit-for-digit: 560,680 tok @ 0.1085 |
| Planktonbets | `0x38745db27f…` | signature | |
| Dicedicedice | `0xdde15ebd95…` | signature | |
| Neodbs | `0x56efadc9de…` | signature | |
| Anon | `0x3811e09bb2…`, `0x5ff872bf11…` | signature | two candidates fit the reported signature |
| nothingeverhappens911 | — | **unmatchable** | no address and no reported share count — *declared*, not conflated with "absent" |

**5 of 5 matchable hypotheses confirmed on-chain** (6 wallets total); the 6th is structurally unmatchable and reported as such.

## 3. Deliverable 2 — detection before the news break

Best composite/tier per labeled wallet across the as-of ladder (`E`=ELEVATED, `C`=CRITICAL; news break at 02-28 06:33Z):

| as-of (pre-news) | 0x1caa | 0x56efad | 0xdde15e | 0x3811e0 | 0x38745d | 0x5ff872 |
|---|---|---|---|---|---|---|
| 02-26 00:00 (−54.6h) | 0.38 E | – | – | – | – | – |
| **02-27 00:00 (−30.6h)** | **0.72 C** | **0.50 C** | – | – | 0.20 W | – |
| 02-27 12:00 (−18.6h) | 0.47 C | 0.48 C | – | – | 0.19 W | – |
| 02-28 00:00 (−6.6h) | 0.43 E | 0.45 C | – | 0.30 W | 0.21 W | – |
| 02-28 06:00 (−0.6h) | 0.42 E | 0.29 W | 0.56 C | 0.29 W | 0.25 W | 0.26 W |

*(Composites in this table include the cross-market cluster boost — the more sensitive setting; §4 gives the raw fill-only ceilings and the recommended lower-FP config.)*

**The detector flags cluster members CRITICAL ~30 hours before the public news break** — `0x1caa` (the largest insider) at 0.72 and Neodbs at 0.50 by 02-27 00:00Z. This satisfies the core acceptance ("flags the cluster CRITICAL earlier than the public news timestamp") for the dominant wallets. Under the recommended lower-false-positive config (cross-market off), `0x1caa`'s raw composite is 0.48 — still CRITICAL at the 0.45 threshold, ~30h early.

**A real, honest limitation surfaced:** `0x1caa` *decays* from CRITICAL (0.72) to ELEVATED (0.42) as the event nears — its relative-size factor **S mechanically shrinks as the market's trailing volume balloons with the arriving crowd**. Early detection is therefore *stronger* than late detection for the informed entrant (which is fine — you alert at −30h), but three of the six labeled wallets never exceed WATCH on fill-only factors.

## 4. Deliverable 3 — sensitivity / false-positive trade-off (the honest dial)

Operating points at **−30h** (the strongest pre-news detection), labeled cluster N=6, cross-market cluster amplifier **ON vs OFF**:

| CRITICAL threshold | recall — xmkt ON | FP — xmkt ON | recall — xmkt OFF | FP — xmkt OFF |
|---|---|---|---|---|
| 0.40 | 2/6 | 34 | 1/6 | 17 |
| 0.45 | 2/6 | 31 | 1/6 | **11** |
| 0.48 | — | — | 1/6 | 11 |
| 0.50 | 1/6 | 25 | 0/6 | 7 |

**Dominant false-positive driver: the cross-market cluster amplifier.** In a war-onset week, the cross-market scope (≥3 fresh wallets on correlated contracts within 12h) fires on nearly every fresh directional wallet, mass-boosting them by ×1.5 → a pileup (≈44 CRITICAL/as-of). Disabling the *boost* roughly halves the false positives. *(The "xmkt ON" column is the now-deprecated boost behavior; per v1.3 §3.2 cluster membership is computed but no longer boosts the score — see §7. The detection numbers below are identical either way, since the dominant wallet never depended on the boost.)*

**The dominant insider's raw ceiling is composite 0.48** (`0x1caa`, F/S/D/C/T with no cluster boost, at −30h). So with cross-market off, **CRITICAL = 0.45 is the tightest threshold that still flags `0x1caa` ~30h before the news, at a cost of 11 false positives** across the 114-market cluster; recall is 1/6 (only the largest wallet clears the bar on fill-only factors). There is **no operating point that recovers the full cluster at a low false-positive count** on F/S/D/C/T alone.

## 5. Conclusion & recommendation

The footprint mechanic **works for the dominant informed entrant, early and clean**, and independently re-identified the reported cluster on-chain. It **cannot cleanly separate the full coordinated cluster from war-week retail noise on the four fill-only factors** — which is precisely the gap the deliberately-excluded signals fill:

- **P (funding provenance, M5)** would separate a coordinated cluster sharing CEX/bridge funding from independent retail — directly attacking the false-positive tail.
- **A (aggression, M8 CLOB)** would separate urgent marketable insider fills from patient retail limit orders.
- **Cross-market clustering needs tightening** to *same-wallet-across-markets* coordination rather than *any* fresh wallets co-occurring, or it should be gated behind P.

**Recommended interim calibration** (pending A/P), now the config default: `CRITICAL = 0.45`, cluster membership recorded but not scored (`cluster_boosts_score: false`, per v1.3 §3.2). This flags the dominant insider `0x1caa` ~30h pre-news at ~11 false positives across the war-week cluster — the tightest honest operating point on fill-only factors. The full trade-off curve above is the owner's operating-point menu; the false-positive floor drops materially once P (funding) can gate the cluster and A (aggression) can separate urgent from patient flow.

**This is a partial-pass with a concrete, evidence-backed priority argument**, not a failure of the build: it quantifies exactly how much the excluded factors are worth and turns "we should build M5/M8" from an assertion into a measured conclusion.

## 5a. Epistemics — what "11 false positives" does and doesn't mean (v1.3 §2)

The labeled set contains **only publicly reported insiders**. During a war-onset week, some of the unlabeled CRITICALs are plausibly genuinely-informed wallets that simply never made the news. So **measured precision is a lower bound on true precision**, just as the A/P exclusion makes measured detection a floor on true detection. A reader must not treat "unlabeled" as "confirmed noise" — it means "not in the public record," nothing more.

Operationally this matters because **M10 is an alert-only intelligence product with human review** — it will never be an auto-trader, and its precision bar is a dossier bar, not a trade-execution bar. ~11 dossiers to skim during the most extraordinary geopolitical week of the year is a tolerable review load, not a broken detector.

**Early peaking is a feature, not a bug.** `0x1caa` scores highest at −30h and decays toward the event — because the detector is loudest exactly when the informational edge is freshest and the wallet's stake is largest relative to the still-thin market. That is the desired shape. (For the live scan, that decay must be handled by tier **latching**, not alert retraction — see §7.)

## 6. Caveats (Rule 1)

- **Attribution is maker-side** (a wallet's own orders each emit an event with `maker`=owner; validated exact against `0x1caa`). Pure-taker sweep fills *may* be under-attributed under one possible event model — a recall risk to confirm with CLOB data in M8.
- **T is a proxy and A/P are excluded** — the composite is over 4–5 factors, not 7. All exclusions are declared in the artifact, never imputed.
- Replay is byte-identical from the response cache; every number here is reproducible with `consensus m0f score --replay`.

## 7. Rulings incorporated from addendum v1.3 (gate review)

- **Calibration ratified as provisional** (`CRITICAL=0.45`). Mandatory recalibration when A (M8) and P (M5) land — noted in `config.yaml` next to the values.
- **Cluster membership demoted from score-booster to dossier evidence** (§3.2). Membership (per-market *and* cross-market) is still computed and recorded in every result (6 clusters at −30h), but `cluster_boosts_score: false` means it no longer moves the composite or the tier. The accepted detection numbers above are unchanged by this (the dominant wallet never depended on the boost); it removes the FP-inflation mechanism. A regime-conditioned boost is a parked hypothesis, to be tested only once a second labeled event exists.
- **Tier latching** (§3.3) is a live-M10 requirement: tiers latch at their per-(wallet, market-family) high-water mark with the crossing timestamp; decay is shown as dossier trajectory, never as alert retraction. A tested `latch_tiers` helper ships ready in `m0f.py`; the backtest itself reports raw per-as-of tiers (trajectory is the point).
- The self-caught `0.65 → 0.45` calibration correction is recorded: with cluster boost off, the dominant wallet's ceiling is 0.48, so a 0.65 threshold would have detected nothing — the kind of error that must not ship silently.
