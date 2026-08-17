# CD-2-VERIFY — thesis layer verification

Produced 2026-08-14 on `cd-2-build`. **95 tests pass.** Nothing merged, nothing pushed.
All figures computed by the built daemon against live SEC data.

---

## C1 — EX-FILING FEES ingest, hand-verified against three real prospectuses

The prospectus body is prose; its filing-fee exhibit is a clean XBRL instance in the `ffd`
taxonomy, filed the day after pricing. **This makes debt issuance event-time and structured.**

**A parser gap had to be closed first.** The exhibit discriminates tranches with a *typed*
dimension — `ffd:OfferingAxis` → `<dei:lineNo>1</dei:lineNo>` — not an `explicitMember`. Our
parser read only explicit members, so every tranche of a multi-tranche offering collapsed into one
indistinguishable context. `ixbrl._parse_contexts` now handles typed members.

| issuer | form | accession | debt tranches | Σ principal | `TtlOfferingAmt` | gap |
|---|---|---|---|---|---|---|
| META | 424B2 | 0001193125-26-201738 | 6 | **25,000,000,000** | 24,967,390,000.00 | 32,610,000.00 |
| ORCL | 424B2 | 0001193125-26-035603 | 8 | **25,000,000,000** | 24,961,775,000.00 | 38,225,000.00 |
| AMZN | 424B5 | 0001104659-26-081786 | 8 | **25,000,000,000** | 24,923,417,500.00 | 76,582,500.00 |

CD-R1 predicted Meta's $32.61M and Oracle's $38,225,000 gaps from manual reading; the built ingest
reproduces both exactly, and AMZN is a fresh third case. **The gap is offering discount and is
reported, never reconciled away** — registered principal is face value, `TtlOfferingAmt` is net.

Worth noting on its own: **all three issuers registered exactly $25.000B**. Three separate
25-billion-dollar debt programmes inside seven months, from three different balance sheets.

**Traps handled, each measured rather than assumed:** preliminary prospectuses gated on
`FnlPrspctsFlg`; tranche-sum vs stated total both retained; exchange offers excluded as non-cash;
discovery deliberately **not** filtered on 8-K item codes (Meta's debt 8-K was filed under 8.01/9.01,
so an item filter drops real events); use-of-proceeds recorded verbatim with **no attribution claim**.

Watermarks advance only on success-with-items and never backwards; dedup keys are content-derived
(`cik:accession`), never time-derived (E12).

---

## C5 — divergence recomputed by hand, two issuers

**META** — capex concept `PaymentsToAcquirePropertyPlantAndEquipment`:

| quarter | capex | derivation | issuance |
|---|---|---|---|
| 2025-06-30 | 16,538,000,000 | ytd-diff | no fact |
| 2025-09-30 | 18,829,000,000 | ytd-diff | no fact |
| 2025-12-31 | 21,383,000,000 | ytd-diff | 29,906,000,000 |
| 2026-03-31 | 18,997,000,000 | native | no fact |

TTM capex **75,747,000,000** · TTM issuance **29,906,000,000** · ratio **39.5%** — daemon agrees to
the digit.

**CRWV** — four consecutive issuance quarters, no gaps:

TTM capex **16,597,000,000** · TTM issuance **14,334,000,000** · ratio **86.4%** — daemon agrees.

The contrast is the point: Meta's credit shows up in one lumpy quarter, CoreWeave borrows every
quarter. **On a quarterly ratio Meta would read 0%, 0%, 140%, 0%** — which is why the metric is
TTM-only.

---

## Live gate demonstrations

| gate | subject | result |
|---|---|---|
| **typed-dimension tranching** | META fee exhibit | 6 tranches discriminated; without it, one context |
| **preliminary prospectus** | `FnlPrspctsFlg=false` | not counted as an event |
| **tranche-sum ≠ total** | all three prospectuses | both figures retained, gap reported |
| **UNCOVERED-UNTAGGED** | MSFT commitments | status published, `points == []`, **never a zero** |
| **ISSUANCE-REFUSED** | FRMI | ratio withheld, cause named |
| **ISSUANCE-NET-NEGATIVE** | WULF | **new gate**, see below |
| **ISSUANCE-NO-WINDOW-OVERLAP** | MSFT | **new gate**, see below |
| **concentration disclosure** | builder bucket | top-2 = 68% published with the subtotal |

### Two gates this phase had to add

**`ISSUANCE-NET-NEGATIVE` — WULF.** Its six-concept issuance stack sums to **−$0.88B**. At least one
contributing concept is stated net of repayments, so the sum is not gross issuance at all.
Publishing "−40% of capex funded by credit" would be worse than publishing nothing, so the ratio is
withheld and the cause named. This is the R-B6-1 stack rule meeting a case it did not anticipate:
collapse/sum/refuse assumed every member was a gross inflow.

**`ISSUANCE-NO-WINDOW-OVERLAP` — MSFT.** Issuance resolves cleanly, but no debt quarter overlaps the
capex TTM window. That is distinct from a refusal *and* from a true zero — and MSFT is precisely the
issuer where the distinction bites, because it **explicitly tags 0 proceeds**, which is a fact rather
than an absence. The bucket ratio therefore names MSFT as excluded rather than folding a silent zero
into the denominator (E16).

---

## C4 — composition aggregate

| bucket | TTM capex | n | top-2 share |
|---|---|---|---|
| hyperscaler | **$552.8B** | 5 | 55% |
| builder | $28.8B | 9 | 68% |
| reit | $8.7B | 2 | 100% |
| **TOTAL** | **$590.2B** | 16 of 22 contribute | — |

Excluded and named: RIOT (`CAPEX-UNRESOLVED`), FRMI, KEEL, SPCX, NBIS (`SHORT-HISTORY`), SNOW.

The decomposition earns its keep: **hyperscalers are 94% of the total**, so a blended headline would
be a hyperscaler number wearing a sector label, and the builder bucket — the leveraged tier where
the thesis says the break happens — would be invisible inside it.

## C5 — divergence, current state

| bucket | TTM credit / TTM capex | excluded |
|---|---|---|
| hyperscaler | **57%** | MSFT (no window overlap) |
| builder | **79%** | WULF (net-negative) |
| reit | **84%** | — |

Per-issuer extremes: CORZ **289%**, CIFR **103%**, CRWV **86%**, EQIX 86%, DLR 81%, GOOGL 68%,
AMZN 59%, ORCL 51%, META 39%, APLD 1%.

Offered as measurement, not interpretation: the **REIT bucket is the most credit-dependent** at 84%,
and three builders exceed 100% — borrowing more than they spend on capex in the same window.

## C6 — artifacts

`bucket_capex_ttm.png`, `divergence_ttm.png`, `forward_commitments.png`, and
`cd2_thesis_layer.pdf` (ReportLab, NW pattern). Captions are generated from the same objects that
produce the numbers, so a chart cannot ship without its composition and coverage line.

## C3 — RIOT deposits

Built (inside budget). Keyed on a **verified line-mapping**, not a concept name: RIOT's
`PaymentsToAcquireMachineryAndEquipment` is "Deposits on equipment", while HUT tags the identical
concept to its purchases line. `equipment_deposits()` returns `None` for any issuer without a
verified mapping — absence of a mapping is not absence of the concept (E23).

---

## Open for ruling

1. **WULF net-negative issuance** — identify which contributing concept is net-of-repayments and
   exclude it, or leave WULF's ratio permanently withheld.
2. **MSFT zero-vs-absent** — MSFT tags 0 debt proceeds explicitly. Should a tagged zero enter the
   bucket denominator as a real 0%, rather than being excluded?
3. **Three builders above 100%** (CORZ 289%, CIFR 103%, CRWV 86% approaching) — is that the
   thesis signal or a series artifact? It wants eyes before it feeds any classifier.
4. **FRMI branch (b′)** remains recorded-not-coded from CD-1.
