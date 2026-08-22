# CD-3-VERIFY — the supplier leg, NVDA's side of the ledger

Produced 2026-08-21 on `main`. **188 tests pass.** Every figure below read out of a live SEC
filing, not from companyfacts.

New module `suppliers.py`, new bucket `supplier` (NVDA, AMD, AVGO, MU, SMCI), new dashboard view
`/suppliers`, new tables `supplier_dc_facts` / `supplier_instances`.

---

## 1. The premise held — and the API blindness is total

CD-R2 §2.3.3 predicted the supplier leg would be **parser-only**. Confirmed, and it is not a partial
gap. NVDA's entire companyfacts record contains, of anything segment-related, exactly two things:
`Revenues` (the consolidated total) and `NumberOfReportableSegments` (a count). There is no Data
Center line at any depth of the API.

Inside the filing, the same quarter carries all of this:

| dimension | quarter to 2026-04-26 |
|---|---|
| `Revenues`, undimensioned — *all the API has* | $81.615B |
| `ProductOrServiceAxis=DataCenterMember` | **$75.246B** |
| `ProductOrServiceAxis=HyperscaleMember` | **$37.869B** |
| `ProductOrServiceAxis=AICloudsIndustrialEnterpriseMember` | $37.377B |
| `StatementBusinessSegmentsAxis=ComputeAndNetworkingSegmentMember` | $74.550B |

`ixbrl.py` was built in CD-1 for precisely this and needed no change to do it.

**A new disclosure worth naming.** NVDA began tagging a `HyperscaleMember` revenue line — the most
direct cross-check on the hyperscaler bucket that exists anywhere in this daemon, since it is
NVIDIA's own attribution of revenue to that customer class. It appears in **one filing only**
(n=1), so it is not yet a series and nothing is built on it. It is flagged here because the second
observation makes it one, and DataCenter = Hyperscale + AICloudsIndustrialEnterprise exactly
($37.869B + $37.377B = $75.246B), so the split is clean when it arrives.

## 2. Coverage is three of five — two measured, one RULED, two refused by name

| supplier | datacenter line | result |
|---|---|---|
| **NVDA** | `DataCenterMember` on `ProductOrServiceAxis` | **COVERED** — 17 quarters, 2022Q2–2026Q2, TTM $229.9B |
| **AMD** | `DataCenterMember` on `StatementBusinessSegmentsAxis` | **COVERED** — 17 quarters, 2022Q1–2026Q2, TTM $22.2B |
| AVGO | none; segments are InfrastructureSoftware / SemiconductorSolutions | UNCOVERED-NO-DC-MEMBER |
| **MU** | none; ruled mapping `CMBU + CDBU` | **MAPPED-BUSINESS-UNITS** — 7 quarters, 2024Q4–2026Q2, TTM $52.5B |
| SMCI | no segment axis at all, across 14 instances | UNCOVERED-NO-DC-MEMBER |

**MU was the refusal worth arguing about, and it was RULED IN on 2026-08-21 (Mando).** Cloud Memory
(CMBU) and Core Data Center (CDBU) are summed as datacenter revenue; Mobile-and-Client (MCBU),
Automotive-and-Embedded (AEBU) and All-Other are excluded. The stated rationale: *memory sold into
cloud and datacenter is the demand being cross-checked, and HBM — the AI-relevant product — sits in
exactly those two units.*

The ruling's conditions are implemented, not just honoured in spirit. The leg publishes
**`MAPPED-BUSINESS-UNITS`, never `COVERED`**; the status travels with the figure into the snapshot
and every renderer; `/suppliers` prints a dedicated block naming the units summed, the units
excluded, the ruling's date and author, and the sentence *"this figure is mapped, not measured"*; and
a period missing either mapped unit is **skipped rather than half-summed**, because a partial sum
understates the line and reads as a decline. `test_suppliers.py` pins each of those conditions.

Micron also only exists as a series **because of E26**. `CMBU`/`CDBU` appear no earlier than the
2025-08-28 instance — before that Micron reported CNBU/MBU/EBU/SBU — so every pre-2025 quarter is
recoverable *solely* because the newer filings restate them. Under the old fewer-dimensions
preference this leg would have been empty.

AVGO stays in the roster despite being uncovered on this leg: its **$128.1B of unrecorded purchase
obligations** is the largest forward-commitment figure anywhere in the universe except Meta's.

## 3. Three things vary across two issuers, and all three were measured

Resolution keys on the **member**, on any axis, case-insensitively. That is not defensive coding —
each relaxation was forced by a failure observed on disk:

| what varies | NVDA | AMD | what breaks if you assume otherwise |
|---|---|---|---|
| **axis** | `ProductOrServiceAxis` | `StatementBusinessSegmentsAxis` | keying on axis drops one of the two covered names |
| **case** | — | 10-Q `DataCenterMember`, 10-K `Datacenter­Member` | exact match lost **every Q4** — Q4 is derivable only from the 10-K — leaving AMD with no TTM at all |
| **concept** | `Revenues` | `RevenueFromContractWithCustomer…` | see §4 |
| **qualifier** | bare | `+ ConsolidationItemsAxis=OperatingSegmentsMember` | treating any extra axis as a breakdown refused AMD outright on the first run |

A `DataCenter × geography` fact is still correctly excluded as a breakdown of the line rather than
the line.

## 4. E7 reproduced a second time, in mirror image

The revenue concept migrates across the supplier bucket in **opposite directions**:

| | `Revenues` | `RevenueFromContractWithCustomerExcludingAssessedTax` |
|---|---|---|
| **NVDA** | **live**, n=119, through 2026-04-26 | dead, stops 2022-01-30 |
| AMD / AVGO / MU / SMCI | dead, stops 2017–2018 | **live**, through 2026 |

Any fixed candidate order resolves the wrong concept for one side or the other. Recency-per-issuer
is the only rule that gets both right, and this is now its third live citation after AMZN's capex
migration and NVDA's own (`PaymentsToAcquirePropertyPlantAndEquipment` stops 2020-07-26,
`PaymentsToAcquireProductiveAssets` runs live).

## 5. AMD filed its segment members rotated — and it caught a bug of mine

AMD's 10-Q for the quarter ending 2024-03-30 tags segment revenue with the **members shifted by one
position**. Verified against raw XML, independent of my parser:

| value | tagged member (ctx) | what AMD reports it as |
|---|---|---|
| $2,337M | `ClientMember` (c-40) | Data Center |
| $1,368M | `GamingMember` (c-42) | Client |
| $922M | **`DataCenterMember`** (c-44) | **Gaming** |

The decisive test is that AMD's own later filing restates the same quarter as
`DataCenterMember = $2,337M`. So the values are right and the labels were wrong, and AMD fixed it.

**My first implementation published the $922M.** It de-duplicated across filings preferring *fewer
dimensions* and never *newer filing* — so a stale mis-tagged fact beat its own correction, and the
leg would have published a **2.5× undercount** and called it Data Center revenue. Fixed: newest
instance always wins, dimension count is only a tiebreaker within one instance, and **restatements
are counted and published** rather than silently applied. AMD carries **5 superseded periods**;
NVDA carries none.

This is the strongest argument yet for the daemon's recency doctrine — it is not only about issuers
migrating tags over time, but about issuers being *wrong* and correcting themselves.

## 6. The cross-check — the same dollar, from the other side

Supplier datacenter revenue TTM against hyperscaler capex TTM. **A ratio, never a sum**: adding
NVIDIA's revenue to Microsoft's capex double-counts one dollar. It is a corroboration, not a
reconciliation, and it is not expected to reach 100%.

| quarter | NVDA+AMD DC revenue TTM | hyperscaler capex TTM | ratio |
|---|---|---|---|
| 2024Q1 | $54.8B | $163.6B | 33.5% |
| 2024Q3 | $92.1B | $206.8B | 44.5% |
| 2025Q1 | $129.1B | $270.9B | 47.7% |
| 2025Q3 | $161.7B | $357.2B | 45.3% |
| 2026Q1 | $212.5B | $482.1B | 44.1% |
| 2026Q2 | $252.1B | $477.0B | **52.8%** ⚠ |

**The finding is the plateau, not the last point.** The ratio climbed from 33.5% to ~45% through
2024 and has held between 44% and 48% for five consecutive quarters. Two independent sets of
filers, on four different fiscal calendars, move together — the buildout corroborates from both
sides of the invoice.

The 2026Q2 reading of 52.8% is **partly arithmetic**. The capex denominator fell from 5 members to
4 because Meta has not filed 2026Q2 yet. The snapshot detects a denominator membership drop and
publishes the warning beside the number:

> capex denominator fell from 5 to 4 members at 2026Q2 — the move in the ratio is partly a
> membership change, not a change in spending

## 7. Never blended, structurally

`trend.AGGREGATED_BUCKETS` is `("hyperscaler", "builder", "reit")`. `supplier` is not in it and
cannot be summed into the panel by any code path, so the ruling is enforced by construction rather
than by memory. Two tests pin it, including one that walks the live roster.

## 8. The nightly stays cheap

Parsing filings is expensive, so facts are cached **per instance** rather than per period — the
restatement resolution needs to know which filing a value came from. Only instances absent from the
cache are fetched. Instances that yield nothing are recorded too, so AVGO, MU and SMCI are not
re-fetched fourteen times every night forever.

Measured live, both runs against the same DB:

```
run 1  [capex-scan] updated: 33 of 35 issuers had new filings ... | supplier instances harvested: 70
run 2  [capex-scan] no-op ...                                     | supplier instances harvested: 0
```

A broken instance costs one filing, not the leg, and is reported into the scan's errors.

## 9. Open

1. ~~**MU's business-unit mapping** — §2.~~ **RULED 2026-08-21: admitted as `MAPPED-BUSINESS-UNITS`
   (CMBU + CDBU summed; MCBU / AEBU / All-Other excluded), with the mapping disclosed on
   `/suppliers`.** See §2. The largest supplier capex spender is in the cross-check.
2. **NVDA's `HyperscaleMember`** — n=1. Worth a standing watch: at n=2 it becomes the most direct
   supplier-side read on the hyperscaler bucket available, and it should probably then replace
   DataCenter as the cross-check numerator.
3. **Suppliers do not classify — CD-3b MEASURED 2026-08-21, HELD FOR RATIFICATION.**
   `tools/measure_supplier_deadband.py`, same P1 methodology (|Δ TTM YoY| per series, recent window
   from 2023-01-01, band = p25) and importing P1's own statistics so the two cannot drift apart.

   | series | n | p25 |
   |---|---|---|
   | NVDA | 9 | 8.9pp |
   | AMD | 5 | 13.5pp |
   | MU | 0 | — (7 quarters yields 4 TTM and no YoY yet) |
   | **pool** | **14** | **8.9pp → proposed band 9pp** |

   **Two things to weigh before ratifying.** The pool is **n=14**, thinner than the n=13–14 that
   already carries a re-measurement obligation for the bucket-sum bands. And the supplier
   distribution is *far* fatter than any spending series — median |Δ| **20.7pp**, max **106.7pp**,
   against a 6pp hyperscaler band — so a p25 band of 9pp on this series suppresses only the quietest
   quarter and lets three-quarters of observed moves through. That may be exactly right, because
   NVIDIA's datacenter growth genuinely does move 40pp in a quarter, but it is a different
   signal-to-noise regime from the one p25 was chosen against. Ratify, adjust, or defer.

   Original entry: measurement pass ordered (CD-3b, 2026-08-21). There is no
   `issuer:supplier` dead-band, so the five names carry no phase state, and Mando ruled that the
   current refusal is correct behaviour until ratified bands exist. Bands must be **measured, not
   guessed** (ratified doctrine), and that
   measurement is a separate pass — `tools/measure_deadband.py` extends to it directly.
4. **TSM still excluded** — IFRS-only, structural, unchanged from CD-R2 §2.3.2.
5. **Fiscal offsets** — four of five suppliers are off-calendar (NVDA Jan, AVGO Nov, MU Sep,
   SMCI Jun). Calendar-quarter keying absorbs this exactly as it does for ORCL, but the cross-check
   is therefore comparing quarters that are up to six weeks apart at the edges.
