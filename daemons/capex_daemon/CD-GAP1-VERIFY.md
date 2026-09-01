# CD-GAP1-VERIFY — graduated disclosure

Produced 2026-08-26 on `cd-gap1`. **227 tests pass.** Nothing merged, nothing
pushed. Every figure computed against live SEC data.

**P7 executed and NBIS admitted — both ruled "yes" by Mando 2026-08-26.** See
§P7 and §P3-ADMITTED at the end.

---

## P2 — RIOT: the ruling never reached the panel

**Answer to the order's question: not "history too short". The ruling was
recorded and never encoded.**

RIOT's capex mapping was ruled as R-B6-4 and written into CD-1-SPEC §428 and
CD-1-VERIFY §1. Nothing in the code ever read it. The resolver saw what it
always sees:

| concept | latest | n |
|---|---|---|
| `PaymentsToAcquirePropertyPlantAndEquipment` | 2026-06-30 = $176.196M | 116 |
| `PaymentsToAcquireMachineryAndEquipment` | 2026-06-30 = $41.376M | 25 |

Two concepts, one period, different values — so `_ambiguous_overlap` refused,
returning `UNRESOLVED-MULTILINE`, and RIOT rendered a dash. **The guard was
right.** Absent a ruling, picking one is a coin flip presented as an answer.
What was missing was anything for it to defer to.

`tagmap.RULED_CONCEPTS` is now that thing: a per-`(cik, kind)` election, marked
`RULED-CONCEPT` wherever it is published so it can never pass as an ordinary
automatic resolution — the same discipline as MU's `MAPPED-BUSINESS-UNITS`.

Verified against the ruling's own citation:

```
without the ruling : UNRESOLVED-MULTILINE
with the ruling    : PaymentsToAcquirePropertyPlantAndEquipment  (R-B6-4)
2026Q1             : 115,465,000
```

115,465,000 is exactly the "Purchases of property and equipment, including
construction in progress" figure CD-1-VERIFY quotes from the filing's cash-flow
statement — not the 16,184,000 "Deposits on equipment" line. **RIOT now
classifies: PLATEAU, +27.7%, $0.284B TTM, 47 quarters, 19 YoY points.**

The excluded concept is dropped from the span map entirely rather than
out-ranked, so it cannot re-enter through the overlap check — summing it would
double-count when the deposited equipment lands (E23).

## P1 — the dash was carrying four different meanings

`INSUFFICIENT-HISTORY` covered a builder with $1.16B of TTM capex, a filer that
stopped tagging in 2020, a refused concept mapping, and a deliberately
non-aggregated sidecar. Split by cause, with every pre-eligible row publishing
what it does have:

| ticker | cause | held | short | TTM | interim (non-ladder) | classifies |
|---|---|---|---|---|---|---|
| FRMI | THIN-MATURING | 6 | 4 | $1.16B | +40.1% | ~2027-08-13 |
| GLXY | THIN-MATURING | 8 | 2 | $1.53B | −14.4% | ~2027-02-12 |
| IREN | THIN-MATURING | 8 | 2 | $3.00B | +901.0% | ~2027-02-12 |
| WYFI | THIN-MATURING | 8 | 2 | $0.48B | +529.0% | ~2027-02-12 |
| DGXX | THIN-MATURING | 4 | 6 | — | — | ~2028-02-11 |
| KEEL | THIN-MATURING | 4 | 6 | — | — | ~2028-02-11 |
| BTBT | SIDECAR | 10 | 0 | $0.48B | +516.9% | n/a |
| BABA | **TAGGING-CEASED** | 0 | 10 | — | — | — |
| NBIS | FPI-ANNUAL-BASIS | 0 | 10 | — | — | — |
| SPCX | NO-DATA | 0 | 10 | — | — | — |

**10 contiguous quarters** is the classification threshold, measured rather than
assumed: TTM needs 4, a TTM YoY needs 8, the ladder needs `N_CONFIRM + 1 = 3`
YoY points.

The interim read is annualised half-over-half with no dead-band and no
confirmation window. It is labelled `non-ladder` everywhere, never aggregates
and never alerts. Its job is to stop the panel saying "no information" about a
name growing at +901%, not to short-circuit the ladder.

## P5 — a bug in my own first cut

The first version measured lateness against the **panel frontier** — the most
advanced filer in the panel — and flagged **seven perfectly current names as
behind**, because one off-calendar issuer (NVIDIA closes January, Micron
September) had already reached the next calendar quarter.

Another issuer's fiscal year has no bearing on whether this one is late. It now
computes each name's own next deadline (period end + 45 days, the conservative
10-Q lag) and distinguishes *due* from **OVERDUE**. On the live panel that
correctly yields zero overdue names and a next-filing date of 2026-11-14.

## P4 — BABA: the premise fails, and the real finding is worse

The order describes "annual extension-tagged capex ($18B+ spender, currently
invisible)". Measured:

* BABA's companyfacts carries **`dei` and `us-gaap` only — no extension
  namespace at all**.
* Its sole capex concept, `PaymentsToAcquireOtherPropertyPlantAndEquipment`,
  has **its newest fact at 2020-09-30**, in both CNY and USD.

So there is no annual capex to render on an ANNUAL-BASIS row. Alibaba's capex
tagging **stopped six years ago**. Rendering an annual row would have meant
rendering 2020 data as though it were current.

This produced a new cause, `TAGGING-CEASED`, because "reports annually" and
"stopped reporting" look identical from an empty quarterly series and are not
the same fact. Getting BABA into the panel needs a source that is not
companyfacts — a separate build, not a rendering change.

## P3 — NBIS prose probe: extraction works, the BASIS does not match

Bounded exactly as ordered: regex-tier, zero LLM, one issuer.

**The naive approach fails ~100%.** "Capital expenditures" appears in the
forward-looking-statements boilerplate of nearly every release. Keying on the
phrase returns boilerplate and nothing else.

**Anchoring on the table works.** 2 of 41 EX-99 exhibits carry a real capex
table, and it is highly regular:

| release | period | 2025 | 2026 |
|---|---|---|---|
| 2026-05-20 | Three months ended Mar 31 | $543.9M | $2,472.9M |
| 2026-08-12 | Six months ended Jun 30 | $1,054.5M | $8,130.3M |

Yielding 2026Q1 = $2,472.9M and 2026Q2 = 8,130.3 − 2,472.9 = **$5,657.4M** by
the same YTD differencing the normalizer already performs on XBRL.

**Annual anchors (the ordered gate) — three, in one table.** The FY2024 20-F:
`Year ended December 31 | 2022 14.6 | 2023 83.4 | 2024 807.7`. H1-2025 at
$1,054.5M already exceeds all of FY2024, consistent with the releases' own
language about substantially increasing the pace. The anchors corroborate.

**No row is published, and this is the finding.** Three different labels appear
for what may or may not be one measure:

* interim releases: "Purchases of property and equipment **and intangible assets**"
* the 20-F: "Capital expenditures"
* the panel: PP&E payments only

Intangibles are not PP&E. A series built from the interim line is a **broader
measure than every other name in the panel**, and [E23] is the standing rule
that concept identity is not semantic identity. Admitting it is a ruling, not a
parse. `tools/nbis_prose_probe.py` is re-runnable and report-only.

## P6 — commitments: do not promote, for a sharper reason than before

A window ≥6 quarters now exists — but only by relaxing the ratified
`COVERAGE_FLOOR` from 0.95 to 0.80. At the ratified floor: **NONE**.

And the window that appears is:

| name | 2026Q2 | share |
|---|---|---|
| META | $349.3B | **99.8%** |
| EQIX | $0.7B | 0.2% |

Two nominal members, one of which is 99.8% of the line. That is precisely the
"one member wearing a bucket label" defect CD-PH1 caught and added
`MIN_BUCKET_MEMBERS` to prevent — it would satisfy the member count while
failing the thing the count was protecting.

**Recommendation: leave it refused.** The refusal is now for a better reason
than before: not "no window exists" but "the only window that exists is one
issuer with a rounding error attached".

## Open

1. **P7 LANDLORD merge** — awaiting the word the relay did not carry.
2. **NBIS basis** — yours: admit the broader "PP&E + intangibles" line under a
   marked basis, or hold. The extractor is built and verified either way.
3. **BABA** — needs a non-companyfacts source or stays dark; not a rendering
   problem.
4. **SPCX** — 2 duration facts, no derivable quarter, currently `NO-DATA`. Worth
   a look at whether its filings are semi-annual before assuming it matures.
5. **Commitments** — re-measure again when a third disclosing name reaches six
   quarters; the concentration, not the count, is the gate.

---

## P7 — LANDLORD merge, ruled YES 2026-08-26

`reit` + `host` are one bucket. Members and preserved sub-types:

| ticker | bucket | sub-type |
|---|---|---|
| DLR | landlord | reit |
| EQIX | landlord | reit |
| AMT | landlord | **reit** |
| IRM | landlord | **reit** |
| CCOI | landlord | host |

Four of the five are REITs, two of them (AMT, IRM) having sat under `host` — the
split really was thin on its own terms. `reit` and `host` remain VALID bucket
names so a pre-merge roster still loads; nothing assigns them any more.

**Bands re-measured on the merged pool the same day, and applied:**

| class | n | p25 | applied |
|---|---|---|---|
| `issuer:landlord` | **68** | 4.7pp | **5pp** |
| `bucketsum:landlord` | 14 | 3.8pp | **4pp** |

n=68 against reit's 27 and host's 41 measured apart — the merge improved the
measurement basis as well as the coverage. Stamped `2026-08-26` in
`DEAD_BAND_MEASURED`, applied under the order's "rides automatically" clause and
adjustable on a word.

**The bucket is alive: PLATEAU, $7.71B, +30.4%, 3 matched members** — against
`INSUFFICIENT-MEMBERSHIP` at 1 before. With five names, one late filing can no
longer dark it; `test_five_members_clears_the_floor_with_room` pins that.

**A consequence worth stating: the headline number moved.** `host` was never an
aggregated bucket, so AMT, IRM and CCOI have entered the total panel for the
first time — **$603.3B → $608.9B, 16 → 20 matched members**. The dollar change is
small because they are small against hyperscalers, but the panel is measuring a
slightly different population than it was this morning.

Member states are genuinely mixed, and the bucket reads PLATEAU over them:

| AMT | EQIX | DLR | IRM | CCOI |
|---|---|---|---|---|
| ACCELERATING +18.0% | PLATEAU +56.7% | PLATEAU +10.3% | **CONTRACTING −4.4%** | **CONTRACTING −28.0%** |

Two of five contracting inside a bucket reading PLATEAU is exactly the
breadth-against-dollar-weight divergence the panel exists to expose.

## P3-ADMITTED — NBIS on a marked broader basis, ruled YES 2026-08-26

`prose.py` reads the capex table out of Nebius's earnings exhibits. Verified
end-to-end against live filings:

```
2025Q1  $543.9M      2026Q1  $2,472.9M
2025Q2  $510.6M      2026Q2  $5,657.4M     (= 8,130.3 − 2,472.9)
```

Anchors recorded and checked: the FY2024 20-F's `2022 14.6 | 2023 83.4 |
2024 807.7`. H1-2025 alone ($1,054.5M) exceeds all of FY2024, consistent with
the releases' own language about substantially increasing the pace.

**Two properties are marked, not assumed.**

*The basis is broader.* The line is "property and equipment **and intangible
assets**" where the panel is PP&E-only. Every row carries
`basis=PPE-PLUS-INTANGIBLES` and the `DERIVED-FROM-PROSE` cause, and NBIS sits in
`fpi`, which no aggregate reads — so the broader measure is structurally unable
to contaminate a sum rather than merely discouraged from it (E23).

*It can never classify, and that is structural, not a countdown.* Nebius reports
Q1, H1 and FY, so Q3 is never separate and Q4 exists only inside an H2 lump. The
series is permanently non-contiguous, which means no TTM, no TTM YoY and no phase
state — ever, from this source. `test_the_series_can_never_be_contiguous_and_never_classifies`
pins it. The row publishes a **level** and a like-for-like **half-over-half**
growth read (**+671%**), and must not be rendered as maturing toward eligibility.

**The trap the tests keep alive:** "capital expenditures" appears in the
forward-looking-statements boilerplate of nearly every release, so a
phrase-keyed extractor matches ~100% of them and returns nothing. The fixture
for that case is Nebius's actual boilerplate sentence.

**238 tests pass.**
