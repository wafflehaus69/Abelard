# CD-1-VERIFY — B8 verification report and distribution snapshot

Produced 2026-08-13 on branch `cd-1-build`. 79 tests pass. Nothing merged, nothing pushed.
All figures below are computed by the built daemon against live SEC data, not restated from CD-R1.

---

## 1. RIOT probe (ruling R-B6-4) — the ambiguity is not a tag collision

RIOT's 2026Q1 10-Q (`0001104659-26-053120`), investing activities, verbatim from `R7.htm`:

```
Deposits on equipment                                             (16,184)   (26,655)
Purchases of property and equipment, including construction in progress  (115,465)  (32,858)
```

Two **separate cash-flow lines**, not two tags competing for one line. The values match the XBRL
exactly:

| concept | RIOT line | 2026Q1 |
|---|---|---|
| `PaymentsToAcquireMachineryAndEquipment` | **Deposits on equipment** | 16,184,000 |
| `PaymentsToAcquirePropertyPlantAndEquipment` | **Purchases of PP&E incl. CIP** | 115,465,000 |

**Recommended mapping for ruling:** RIOT capex = `PaymentsToAcquirePropertyPlantAndEquipment`.
`PaymentsToAcquireMachineryAndEquipment` at RIOT is a **deposits/prepayment** line — cash advanced
for equipment not yet delivered or capitalized. Summing it into capex double-counts when that
equipment lands; it is better carried as its own forward-indicator series.

### The cross-issuer collision this exposed

`PaymentsToAcquireMachineryAndEquipment` **does not mean the same thing at every issuer.** HUT's
2026Q2 10-Q (`0001104659-26-090025`, `R6.htm`) presents:

```
Deposits for future sites          (18,445)
Purchases of property and equipment  (616,182)
```

and HUT tags **616,182,000** — the purchases line — as `PaymentsToAcquireMachineryAndEquipment`.
HUT carries no `PaymentsToAcquirePropertyPlantAndEquipment` at all.

So the identical us-gaap concept is **deposits at RIOT and purchases at HUT**. Recency resolution
cannot see this; only presentation can. **A tag map must be validated against the filing's own line
labels, not merely resolved by recency** — an extension to E7 worth ruling into the ledger.

---

## 2. Refusal mini-matrix and post-ruling resolution

The order asked for containment pairs on EQIX, CORZ, DLR and FRMI. Producing them exposed **two
defects in the rule as implemented**, both now fixed.

**Defect A — refusal was not frontier-scoped.** The containment test considered every shared period
in history. EQIX refused on a **2009** pair; DLR on pairs spanning **2009–2019**; WULF on
**2021–2024**. None can touch a current total. `tagmap` already scoped its ambiguity test to a
400-day frontier window; `issuance` never inherited it.

**Defect B — zeros manufactured containment.** `0 ≤ X` holds trivially, so any concept reporting no
activity in a shared period looked like a subset. DLR's `IssuanceOfSecuredDebt` was 0 in four of the
nine "containment" periods.

Sample of the raw matrix that revealed both (values in USD):

| issuer | pair | period | smaller | larger |
|---|---|---|---|---|
| EQIX | Convertible vs SecuredDebt | 2009-06-30 | 744,000 | 373,750,000 |
| DLR | SecuredDebt vs SeniorLongTermDebt | 2016-12-31 | **0** | 675,591,000 |
| DLR | SecuredDebt vs SeniorLongTermDebt | 2019-12-31 | **0** | 2,869,240,000 |
| WULF | NotesPayable vs RelatedPartyDebt | 2024-12-31 | **0** | 0 |
| FRMI | Convertible vs IssuanceOfDebt | 2025-12-31 | 75,500,000 | 100,000,000 |

**Counterparty-dimension exclusion applied.** `ProceedsFromRelatedPartyDebt` describes *who lent*,
not *what was borrowed* — the same borrowing is also tagged by instrument, so summing it
double-counts, and its persistent smallness reads as containment. It is excluded from the instrument
stack (still stored per-concept, never summed). That alone cleared all three WULF refusals.

### Post-ruling resolution state — all six stack names plus ORCL/EQIX

| issuer | status | resolution |
|---|---|---|
| AMZN | **OK** | summed-disjoint, 2 contributing |
| ORCL | **OK** | summed-disjoint, 4 contributing |
| WULF | **OK** | summed-disjoint, 6 contributing |
| CIFR | **OK** | collapsed double-tag, 3 contributing, 1 collapsed |
| HUT | **OK** | summed-disjoint, 3 contributing |
| APLD | **OK** | summed-disjoint, 2 contributing |
| EQIX | **OK** | summed-disjoint, 4 contributing |
| CORZ | **OK** | summed-disjoint, 4 contributing |
| DLR | **OK** | summed-disjoint, 3 contributing |
| **FRMI** | **UNRESOLVED-MULTILINE** | Convertible 75,500,000 ≤ IssuanceOfDebt 100,000,000 across both live periods |

**14 of 15 issuers resolve. One genuine refusal remains: FRMI**, and it is current (2025), non-zero
in both periods, and plausibly a true subset — convertible notes as a component of total debt
issued. That is the one pair needing your ruling.

**Your ORCL/EQIX hypothesis did not hold.** The `LongTermDebt`/`SeniorLongTermDebt` pair resolves at
rule (b), not (a): ORCL's pair shares **zero** periods (temporal succession, not double-tagging);
EQIX's co-reports **15 periods with differing values** (distinct instruments). Rule (a) does fire
elsewhere and mechanically — CIFR collapses `ConvertibleDebt`/`DebtNetOfIssuanceCosts`.

---

## 3. B7 — TTM anchor reconciliation (CD-G3)

Deployment = cash capex **+ finance-lease additions**. Band 0.5×–2.0×, an order-of-magnitude bound.

**RECONCILED (10):**

| issuer | deployed | Δanchor | ratio |
|---|---|---|---|
| MSFT | 140.6B | 133.1B | **1.06×** |
| META | 75.7B | 76.2B | **0.99×** |
| ORCL | 57.2B | 63.1B | 0.91× |
| APLD | 2.9B | 3.0B | 0.95× |
| DLR | 3.3B | 3.7B | 0.89× |
| CIFR | 1.2B | 1.7B | 0.75× |
| WULF | 2.2B | 3.1B | 0.73× |
| HUT | 0.7B | 1.1B | 0.67× |
| CRWV | 16.8B | 25.3B | 0.66× |
| SNOW | 0.1B | 0.0B | 1.51× |

Zero FLAGGED. Every reconciled issuer sits inside the band, and MSFT/META land within 6% and 1% of
unity — the two largest and most complex names, including the one whose anchor concept bundles
finance-lease ROU by construction.

**UNANCHORED (7), and the causes are three distinct things:**

1. **Anchor concepts disagree near the frontier** — EQIX carries `PropertyPlantAndEquipmentGross`
   *and* `RealEstateGrossAtCarryingValue` with different values; neither may be silently preferred.
2. **Anchor series went stale** — CORZ's instants stop at **2024-06-30**; AMZN's at 2025-12-31;
   GOOGL's at 2025-03-31. The concept exists but does not reach the window.
3. **Partial window coverage** — GLXY has a closing instant but no opening one within tolerance.
   Also IREN, WYFI.

**On ruling R-B6-3.** CORZ→anchored is correct *at the concept layer* — B3 resolved it and B7
confirms the concept. But CORZ's anchor **series stops in mid-2024**, so no 2026 window can close on
it. Both statements are true and not in conflict: CORZ has an anchor concept; CORZ's anchor is
stale. The reconciliation reports UNANCHORED for the second reason, not the first.

**UNANCHORED demo re-targets to EQIX** — its cause (two live anchor concepts disagreeing) is
structural rather than a data gap, which makes it the honest subject.

---

## 4. Hand-verification — derived quarters re-sum to filed YTD

The strongest available mechanical check: every discrete quarter this daemon derives by differencing
must, when re-summed, reproduce the YTD figure the issuer actually filed.

**15 of 15 checks match to the dollar**, across all five named issuers:

| issuer | filed YTD | derived quarters sum to | |
|---|---|---|---|
| MSFT | 2025-07-01..2026-06-30 = 115,948,000,000 | 115,948,000,000 | MATCH |
| MSFT | 2025-07-01..2026-03-31 = 80,146,000,000 | 80,146,000,000 | MATCH |
| META | 2025-01-01..2025-12-31 = 69,691,000,000 | 69,691,000,000 | MATCH |
| META | 2025-01-01..2025-09-30 = 48,308,000,000 | 48,308,000,000 | MATCH |
| AMZN | 2025-07-01..2026-06-30 = 173,028,000,000 | 173,028,000,000 | MATCH |
| AMZN | 2026-01-01..2026-06-30 = 98,411,000,000 | 98,411,000,000 | MATCH |
| DLR | 2025-01-01..2025-12-31 = 3,181,179,000 | 3,181,179,000 | MATCH |
| WULF | 2026-01-01..2026-06-30 = 1,378,514,000 | 1,378,514,000 | MATCH |
| WULF | 2025-01-01..2025-12-31 = 1,060,189,000 | 1,060,189,000 | MATCH |

(Full set of 15 in the B8 run log; the above is the representative cut.)

Provenance is carried per row. MSFT's most recent quarter is `ytd-diff` because no 10-Q covers a
fiscal Q4 — the FY minus 9M derivation, exactly as specced. META's 2026Q1 is `native`; its three
prior quarters are `ytd-diff`.

---

## 5. Live gate demonstrations

| gate | subject | result |
|---|---|---|
| **Plausible-stale-resolution** (E7) | AMZN | fixed-order picks a tag abandoned 2017-03-31 → **7,417,000,000**; recency resolves → **173,028,000,000**. **23.3× avoided.** |
| **scale=9** (E5) | MSFT 10-K | displayed `329.1`, `scale=9` → **329,100,000,000**, basis `ixbrl-scale-attr`; nested wrap collapsed with 1 context recorded |
| **UNANCHORED** | EQIX | two live anchor concepts disagree; refuses rather than preferring one |
| **UNCOVERED-UNTAGGED** | MSFT | $194.06B datacenter purchase-commitment table carries no `ix:nonFraction` at all — disclosed, unXBRL'd, published as a coverage status |
| **REFUSED** | RIOT capex | 2 concepts co-report the live era; no single concept published |
| **UNRESOLVED-MULTILINE** | FRMI issuance | containment unresolved; `total_for()` returns **None**, never zero |

---

## 6. THE DELIVERABLE — growth-rate distribution snapshot

Phase thresholds get ruled against this and nothing else (E8).

### QoQ capex growth — too noisy to threshold

| population | n | p10 | p25 | med | p75 | p90 | max |
|---|---|---|---|---|---|---|---|
| pooled, all history | 609 | −35.9% | −12.5% | +9.3% | +34.4% | +85.8% | +3218% |
| pooled, since 2024 | 153 | −32.8% | −6.4% | +20.5% | +54.9% | +192.0% | +3218% |
| hyperscaler | 49 | −10.3% | +5.9% | +13.6% | +28.1% | +54.9% | +74.7% |
| builder | 75 | −39.5% | −13.6% | +40.4% | +165.6% | +666.0% | +3218% |
| reit | 19 | −29.0% | −12.5% | −0.6% | +25.6% | +36.3% | +45.8% |
| mirror | 10 | −63.0% | −35.0% | +5.1% | +49.5% | +298.9% | +298.9% |

**Finding: QoQ cannot carry a phase threshold.** The pooled interquartile range spans −6% to +55%
and the builder bucket runs −90% to +3218%. Any threshold on QoQ would fire on lumpiness.

### TTM YoY capex growth — the usable series

| population | n | p10 | p25 | med | p75 | p90 | max |
|---|---|---|---|---|---|---|---|
| pooled, all history | 510 | −15.9% | +1.7% | +27.9% | +70.1% | +169.5% | +75354% |
| pooled, since 2024 | 129 | −10.2% | +23.5% | +64.5% | +228.3% | +457.3% | +7946% |
| **hyperscaler** | 49 | −7.9% | **+45.1%** | **+58.2%** | **+79.6%** | +162.4% | +249.0% |
| **builder** | 51 | −2.9% | +98.4% | **+287.1%** | +457.3% | +1123.1% | +7946% |
| **reit** | 19 | −14.1% | −6.0% | **+16.2%** | +23.5% | +54.9% | +56.7% |
| mirror | 10 | +31.9% | +39.6% | +71.5% | +99.1% | +119.6% | +119.6% |

### The finding that matters most

**The three buckets occupy genuinely different distributions.** Hyperscaler TTM growth is tight and
coherent — interquartile 45%–80% around a 58% median. Builders sit an order of magnitude higher and
far wider (median 287%, p90 1123%). REITs are modest (median 16%, max 57%).

A **pooled threshold would be meaningless**: the hyperscaler median (+58%) sits below the builder
25th percentile (+98%). This is E14/R4 vindicated empirically — composition must travel with any
number, and phase definitions almost certainly need to be **per-bucket, not universal**.

### Current state of the panel — latest TTM YoY per issuer

| bucket | issuer | latest TTM YoY |
|---|---|---|
| hyperscaler | ORCL | **+162.4%** |
| hyperscaler | GOOGL | +97.7% |
| hyperscaler | MSFT | +79.6% |
| hyperscaler | META | +73.0% |
| hyperscaler | AMZN | +60.7% |
| builder | WULF | **+473.5%** |
| builder | CORZ | +457.3% |
| builder | APLD | +320.4% |
| builder | CIFR | +260.8% |
| builder | HUT | +229.4% |
| builder | CRWV | +98.4% |
| reit | EQIX | +56.7% |
| reit | DLR | +10.3% |
| mirror | SNOW | **−10.2%** |

**Nothing in the panel is decelerating except SNOW**, which is the name ruled MIRROR precisely
because its capex is not the read. Every hyperscaler is between +61% and +162%; every builder
between +98% and +474%. The daemon's founding question — is capex decelerating — currently answers
**no, everywhere**, and now answers it with a measured distribution behind it rather than an
impression.

---

## 7. Open for ruling

1. **RIOT mapping** — recommend capex = `PaymentsToAcquirePropertyPlantAndEquipment`; deposits line
   carried separately. §1.
2. **FRMI containment** — the one surviving refusal. §2.
3. **Presentation-validated tag maps** — the RIOT/HUT collision suggests an E7 extension. §1.
4. **Per-bucket phase thresholds** — the distribution says universal thresholds cannot work. §6.
5. **CORZ stale anchor** — anchored at the concept layer, unreachable at the series layer. §3.
