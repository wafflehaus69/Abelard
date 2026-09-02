# CD-GAP2A-VERIFY — priority amendments

Executed 2026-09-02. Every figure measured against live SEC data or the live
Basilic panel. A0, A1 and A6 are merged and deployed; A2–A5 are in this pass.
A7 has its own document (`recon/CD-R3-POWER-RECON.md`).

---

## A1 — DLR was not behind on filing

**Mando's read was right, and it was three names.**

DLR filed a 10-Q for period 2026-06-30 on **2026-07-31**, accession
`0001104659-26-089296`. It sat on the submissions index for 33 days while the
panel called it late. Sweeping all 35 issuers:

| ticker | filed period | companyfacts stops at | verdict |
|---|---|---|---|
| DLR | 2026-06-30 | 2026-03-31 | INGEST-GAP |
| AMT | 2026-06-30 | 2026-03-31 | INGEST-GAP |
| RIOT | 2026-06-30 | (no capex concept) | concept refused, not an ingest gap |

**Two defects, compounding.**

*First: the fallback had never run.* `freshness` — the module written for exactly
this companyfacts lag, carrying its own E5/E6 doctrine and a full test file — was
imported by `scan.py` for one function, `latest_periodic_filing`. `assess`,
`needs_fallback` and `fetch_fallback_facts` were called by nothing outside the
module's own tests. **This is the third instance of the same pattern in this
codebase**: the RIOT ruling recorded and never encoded (§CD-GAP1 P2), `prose.py`
built and never wired (§CD-GAP1 open 7), and now the freshness fallback. Three is
a pattern, not a coincidence, and it argues for a standing check that every
module with a doctrine docstring has a caller.

*Second: the watermark recorded sighting, not ingestion.* `check_issuer` gated on
`filing.filing_date <= watermark`. DLR's watermark was set to `2026-07-31` — the
filing date itself — on 2026-08-22. From then on the gate answered "current,
nothing newer than watermark" every night, `refresh_issuer` was never called, and
the fallback could not have run even if it had been wired. **A temporary API lag
became a permanent hole**, and DLR would have stayed dark until its Q3 filing in
late October.

That is the whole explanation for the dark reit bucket: DLR stranded left EQIX
alone above the two-member floor.

**Fixed at both layers.** The fallback runs in the scan; the watermark holds when
the filed period did not arrive; and `check_issuer` reopens a closed watermark
when the published panel is behind, so the system heals instead of waiting for
the next filing. Verified live — DLR $3.26B → **$3.47B**, 63 → 64 quarters; AMT
$1.80B → **$1.82B**, 25 → 26.

### The bug inside the fix

The first cut filled the index inside `refresh_issuer`, whose only consumer is
the `views` list, while `snapshot.build` consumes an index fetched separately by
`_indexed_all`. **It shipped.** The Basilic run reported `updated: 2 of 35 — AMT,
DLR` and published a snapshot still sitting on their previous quarter. I had
verified the issuer *view* and called it done; the view is not what a reader
sees. `test_the_fill_reaches_the_published_snapshot_not_just_the_view` asserts on
the published snapshot, which is the only assertion that would have caught it.

---

## A2 — a supplier's phase is its datacenter revenue

**Correction owned by Mando, encoded here.** An earlier audit recorded NVDA's
capex `DECELERATING-CONFIRMED` as "the supply side's biggest name confirmed its
rate-bend". That reads a buildout signal off the wrong series:

| series | TTM | phase |
|---|---|---|
| NVDA's own capex | $7.4B | DECELERATING +60.5% |
| NVDA datacenter revenue | **$277.8B** | **PLATEAU +89.5%** |

A factor of 37 between them. The first is offices and test equipment; the second
is the other side of the hyperscalers' invoice.

**The layout invited the error.** The phase board gave supplier capex the same
weight and the same colour as a hyperscaler's, while the dcrev phase lived on
page 17. Now suppliers show **dcrev as the primary row**, with capex demoted to a
labelled secondary — demoted, not hidden, because it is still a real series.

**The alert analog.** `dcrev` transitions were computed, rendered, and could
never fire: they were not in `snap["transitions"]` at all, so the one supplier
state with thesis meaning was structurally unalertable. They now join the
transition record, and the analog of "any hyperscaler entering DECELERATING" is
keyed on `dcrev:`, never on supplier capex. Pinned both ways —
`test_supplier_own_capex_decelerating_does_not_alert`.

`CD-3-VERIFY.md` §3 carries the correction inline.

---

## A3 — one quarter ahead of the demand panel

Four of five suppliers close off-calendar (NVDA July, MU August, SMCI June), so
they routinely file a quarter the hyperscalers will not report until late
October. The cross-check correctly refuses a ratio there — no denominator — and
the numerator was being discarded with it.

Published now on the suppliers view as a supplier's **own discrete quarter**
against its own prior quarter and its own year-ago quarter. Explicitly **not** a
TTM, **not** a ratio, **not** a phase state, and in **no** aggregate. It is the
earliest signal the panel carries and it is one name at a time.

---

## A4 — commitment deltas, threshold measured and HELD

SMCI went **$10.10B → $34.20B** between two snapshots — a server assembler
committing to 3.4× the components — and nothing said so. It was visible only by
diffing two PDFs by hand.

Deltas are now computed per issuer, against that issuer's own previous
observation on its own concept. **Same basis by construction**: nothing is
compared across issuers and no delta is summed, so the cross-issuer
comparability problem raised in GAP2 P2 does not arise here. The **gap** between
observations travels with the move, because a stock is disclosed on the issuer's
own schedule and 3× over one quarter is not 3× over eight.

### The distribution (E8), measured 2026-09-02

308 observation-to-observation pairs across 21 disclosing issuers:

| | p50 | p75 | p90 | p95 | max |
|---|---|---|---|---|---|
| multiple | 1.000× | 1.210× | **2.000×** | 3.203× | 2372× |

**A bare multiple is a bad gate.** At the measured p95 of 3.203× it fires 15
times and **six of those move less than $1B**:

| ticker | multiple | move | base |
|---|---|---|---|
| WULF | 846× | +$0.118B | $0.000B |
| MARA | 830× | +$0.485B | $0.001B |
| CLSK | 6.54× | +$0.041B | $0.007B |

None of those is a forward-demand event; each is a small denominator.

### Proposed, and held for ratification

> **multiple ≥ 2.0× AND absolute move ≥ $1B**

2.0× is the measured p90; $1B removes the near-zero-base tail. The pair fires on
**16 of 308 pairs (5.2%)**, and every one is materially large — SMCI's citing
case at 3.39× / +$24.10B, META +$53.24B, AVGO +$128.06B, AMD +$13.54B, ORCL
+$7.77B.

**The caveat to weigh before ratifying:** *five of those sixteen are SMCI.* One
issuer would account for a third of all commitment alerts. Its stock genuinely is
that volatile — $1.60B → $11.60B → $3.90B → $10.10B → $34.20B across five
quarters. Whether that is the signal working or one name capturing the channel is
a judgement, not a measurement, so it is Mando's.

`COMMITMENT_JUMP_MULTIPLE` and `COMMITMENT_JUMP_MIN_DELTA` are both **None**.
Deltas are published; nothing alerts. `commitment_alert_lines` returns `[]` while
either is unset, and requires both together.

---

## A5 — IREN is a coincidence, and the gate is real

IREN's credit/capex published as exactly **+100%**. Measured:

```
ttm_capex    $2,998,006,000   PaymentsToAcquirePropertyPlantAndEquipment
ttm_issuance $3,000,000,000   ProceedsFromConvertibleDebt
ratio        1.000665
```

**Not the same number and not the same fact.** The concepts are disjoint.
Convertible notes are issued in round amounts and a real capex figure landed
0.07% away from one. What made it look like an identity was the display rounding
1.000665 to "+100%".

But the shape is exactly what one fact resolved into both legs would produce, and
that would make the ratio a tautology at the precise point the thesis is tested.
So the gate ships, and it distinguishes the two cases rather than crying wolf:

- **`SUSPECT-IDENTITY`** — legs agree to three significant figures, concepts
  disjoint. Published with a flag; the ratio stands.
- **`RATIO-TAUTOLOGY`** — the same concept feeds both legs. The ratio is
  **refused**, because one fact divided by itself is not a measurement.

Swept across all 35 issuers: **one hit, IREN, disjoint concepts, coincidence.**
No other name is within three significant figures. Zero is excluded deliberately
— an explicitly tagged zero on both legs is a real double zero (E16).

---

## A6 — three label sites, not two

`_nice_ticks` computed its magnitude from the digit count of `int(raw)` and
floored it at 1.0 for any `raw < 1`. **Every YoY and ratio chart is fractional**
— a TTM YoY of +84.7% is 0.847 — so a panel spanning −0.2 to +0.9 produced ticks
at 0 and 1, of which one was in range, and the chart rendered a single gridline
reading "+0%". Dollar axes are unchanged and pinned by test.

`quarter_axis` drew every Nth label **and** the last unconditionally, so the
final two overprinted whenever the length was not a multiple of N — "2026Q1" and
"2026Q2" as "20226Q2" on Leg 1. Fixed; then the live render showed the **phase
grid** doing the same thing in its own label loop. One `label_picks` helper now
serves the SVG axis, the SVG grid and the PDF axis.

---

## Open

1. **A4 ratification** — the 2.0× / $1B pair, and the SMCI concentration caveat.
2. **The third instance.** Three modules built, tested, documented and never
   wired. Worth a standing check rather than a third apology.
3. **Tag-map audit** (from CD-R3) — `PaymentsForConstructionInProcess` and
   `PaymentsForProceedsFromProductiveAssets` are absent from `CANDIDATES[CAPEX]`.
   Verified against the current roster: four members carry them and **all four are
   historical** (AMZN stops 2018, ORCL 2011, SMCI 2015, AMD six months stale), so
   the live panel is unaffected. Worth an audit, not an alarm.
4. **GAP2 P1–P8** remain held.
