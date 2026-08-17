# CONSENSUS — retrospective and trade-line closeout

**Per addendum v1.21 Part A. This document closes the CONSENSUS trade line.**
Date: 2026-08-16.

CONSENSUS set out to find a tradeable signal in public Polymarket order flow. It did not
find one. Five hypotheses were pre-registered and five were killed by evidence. This is
the record of what was tested, what killed it, and — more useful than any of the
individual verdicts — *why the whole class failed*, which is the one durable asset the
project produced.

---

## 1. The arc, factually

| # | Hypothesis | Metric | What killed it | Artifact |
|---|---|---|---|---|
| 1 | **Detector A** — copy the "winners circle" when skilled wallets converge | apparent +10% aggregate edge across 27/81 sweep cells | **Regime decomposition.** 22 of 23 tradeable signals fell in spring 2025 (15 in May alone), 1 in Dec, and **zero in 2026-Jan–Apr despite 2026 carrying 2.2× the market supply** (474 vs 215). Supply confound ruled out: the mechanic simply stopped firing. The aggregate was a stale artifact. | `docs/m0c_report.md` |
| 2 | **Detector B** — follow a fresh-wallet informed footprint (≤30d) | follower edge **E = +2.15pp**, 870 footprints / **322 market-blocks** | **Noise and materiality.** Market-block bootstrap 95% CI **(−3.3pp, +7.0pp)**, one-sided *p* ≈ 0.21, and below the pre-registered +3pp floor. Realized MDE ≈ 6.5pp — the test could only certify an edge far larger than the one present. Well-powered NO-GO, not an underpowered one. | `docs/m0b_report.md` (`ae8ff01`) |
| 3 | **Detector B, tighter cell** — CRITICAL ∩ ≤7d | **+8.64pp, p = 0.005**, beats the contested slice by +14pp | **Regime decay again.** 2025-H1 **+10.4pp**; 2026-Jan–Apr **−9.45pp**. The aggregate significance was a stale-regime artifact, and the live-adjacent regime was already negative *before* any forward window opened. Declined rather than tested. | `docs/m0b_forward_checkpoint.md` |
| 4 | **Cross-venue lead-lag (M0-X)** — read international price, act on Kalshi | overlap census | **Structural thinness.** 3,254 relevant settled Kalshi events × 9,964 resolved international markets produced 2,972 candidate pairs, but after excluding anonymised markets and enforcing 1:1, only **17 likely-EXACT** — below the pre-registered floor of 30. The moat was real; the market wasn't. | `docs/m0x_gate0_candidates.md` |
| 5 | **Information-bearing tape (Deliverable A, v1.20)** | IB blocks available to power any detector test | **Structural starvation:** reported **1 IB block**, powered date **2028**. The venue does not emit enough information-bearing events to validate *any* such detector, on any timeline that matters. | *architect-relayed; see §6 provenance note* |

Supporting instrumentation, measured on the live store 2026-08-15: **141 of 198 resolved
footprints (71%) were carry-band** — entered at odds where the outcome is near-automatic
and winning carries no information. The contested slice spanned **22 of 65** resolved
blocks. Projected honestly on the contested rate, a powered re-test sat ~66 days out
against the ~15 days the all-block count implied — a 4× inflation, which is what triggered
the structural question in the first place.

---

## 2. The synthesis, stated once

**Every hypothesis tested was an *observation edge*: predicting from other participants'
public behaviour on a transparent, liquid, heavily-arbitraged venue.** The failure
mechanism was identical each time — anything visible to us is visible to everyone, and is
priced before we could act. Detector A decayed to zero as the crowd doing it grew;
Detector B's faint residue never cleared a realistic follower's entry; the tighter cell
was already negative in the live regime; the cross-venue moat had no market behind it.

**Beneath that sits the harder finding.** Even a *perfect* observation detector could not
have been validated here, because the venue's information-bearing event tape is too sparse
to power a test at all. That is a different and stronger statement than "we didn't find
it":

> **It is structurally not findable here.** Not "our detectors were bad." Not "we needed
> more data." The tape does not contain enough information-bearing events to distinguish a
> real edge from noise on any horizon worth waiting for.

That distinction matters for what comes next. A weak detector invites a better detector. A
starved venue invites a *different venue or a different question* — and forecloses the
entire family of "try another variation here," which is exactly the trap a less
disciplined project would now be walking into with hypothesis six.

---

## 3. The reduced steady state

### Keeps running
The **dossier scanner + dashboard**, unchanged, in its two settled roles:
- an **intelligence tool** feeding the owner's own judgement — verifiable on-chain facts
  about coordinated and informed footprints, surfaced for a human to weigh;
- a **standing tripwire** — if a genuine information event ever drops a real footprint on
  this venue, the scanner will surface it. It costs approximately nothing, is read-only,
  requires no capital, and is compliance-clean.

Operationally: `com.consensus.dossier` nightly on Basilic (scan → persist → stamp outcomes
→ evaluate alerts → refresh dashboard), alert bar 0.80, quiet weeks stay quiet.

### Stops
- The **powered-test countdown**. It was counting toward a test the tape cannot supply.
- **All further edge-hunting on this venue.**
- **Any fifth hypothesis of the same class.** No new detector gets built here. That is the
  finding, not an obstacle to route around.

### Reopen condition (single, explicit)
A **different question or a different venue** — never another variation of observation-edge
on Polymarket. A reopen requires **new pre-registration**, not a resumed search. If a
future proposal cannot name which structural protection it has that CONSENSUS lacked
(speed, access, synthesis, or a structural counterparty), it is presumptively dead before
it is tested.

---

## 4. Assets retained

Reusable infrastructure for any future scoped question:

| Asset | What it is | Reuse value |
|---|---|---|
| **L1 archive** | Goldsky subgraph tape, frozen Apr-2026, 13 GB cache | Deep-history replay for any Polymarket-adjacent question |
| **L2 collector** | live forward tape on Basilic, 5.7M+ fills, launchd, gap-declaring | The only forward record; keeps accruing |
| **Funding graph (m5)** | wallet→funder resolution + CEX classifier | Actor identity for any on-chain analysis |
| **`resolution.py` chokepoint** | single owner of the "unresolved" tri-state | Pattern worth porting to any project with a third state |
| **Dossier store** | idempotent, frozen-as-scored, resolution-backfilled | The labelled dataset — outcomes stamped, factors frozen |
| **Dashboard** | pre-rendered static HTML, fail-loud, no JS dependency | The reporting pattern generalises |
| **Backtest harness** | block bootstrap, pre-registration discipline, GO/NO-GO machinery | Directly reusable for the next hypothesis, whatever venue |

---

## 5. What the discipline bought

Five hypotheses, four of which looked attractive on first inspection and one of which
(#3) was *statistically significant in aggregate* at p=0.005. A less disciplined build
would have traded at least two of them. The backtest-first rule spent compute instead of
capital, and the pre-registration rule stopped a significant-looking result from being
read as a real one.

The recurring failure that adversarial review caught, in every layer it was applied to:
**a third state — unresolved, unmeasured, unrendered — quietly collapsing into a
confident-looking value.** It appeared seven times (scoring, three fail-open call sites,
the peak/alert path, and twice in rendering). The F-imputation instance alone accounted
for ~83% of CRITICAL alerts before it was caught; shipping on the pre-review threshold
would have paged ~9×/week on phantom signals *and* satisfied the quiet-week acceptance
against inflated data — a false pass. Review-before-trust is the second durable asset here.

---

## 6. Provenance note (Rule 1)

Milestones 1–4 above are cited from committed artifacts in this repository, verified
directly. **Milestone 5 (Deliverable A, v1.20 — "1 IB block, powered date 2028") was
relayed by the architect in addendum v1.21 and has no committed artifact in `docs/`.** It
is recorded here as stated, not as independently verified. If that verdict is later
challenged, the underlying computation would need to be re-run or its artifact located —
it is the load-bearing claim behind the structural closure, and it should not rest on a
relay indefinitely.

Everything else in this document traces to a committed report or a measured figure from
the live store.

---

## 7. Closing

CONSENSUS answered its question. The answer was no, five times, for one reason, and the
reason generalises: **observation edges on transparent, arbitraged venues are competed
away, and this venue in particular cannot supply enough information-bearing events to
prove otherwise.**

What remains — the scanner, the dashboard, the tape, the harness — runs itself, costs
nothing, and watches. The trade line is closed.
