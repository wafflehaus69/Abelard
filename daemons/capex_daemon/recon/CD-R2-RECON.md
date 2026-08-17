# ORDER CD-R2 — ROSTER RECON

**Report-only.** Executed 2026-08-14 on worktree `cd-r2`. No code, no placements — roster
ratification is Mando's. Evidence tags: `[CURL-VERIFIED]` fetched live this session ·
`[PARSER]` extracted with the Capex Daemon's own inline-XBRL parser · `[FTS-RESULT]` EDGAR
full-text search · `[WEB]` · `[INFERENCE]`.

**Governing test — the standing admission rule (Mando, 2026-08-14):**
> *"anyone building datacenters or owning/hosting the property belongs — track the whole spending curve"*

Admission turns on **building** datacenters or **owning/hosting** the property. Not sector, not
self-description, and explicitly **not SIC** — SIC 6199 "Finance Services" is assigned to six
operators already in this universe.

**E15 applied:** every prior sweep verdict (2026-08-07) is treated as expired and re-verified live.

---

## TASK 2.3 — SUPPLIERS

Ruled in as a **separate bucket with cross-check semantics**: supplier datacenter revenue is a read
on *someone else's* spending, and is **never blended into the spending aggregate** — a supplier's
revenue and a builder's capex are the same dollar counted at opposite ends.

### 2.3.1 Filer type and cadence `[CURL-VERIFIED]`

| ticker | CIK | entityType | forms | FYE | latest periodic |
|---|---|---|---|---|---|
| NVDA | 1045810 | operating | 10-K/10-Q | **Jan (0131)** | 10-Q 2026-05-20, period 2026-04-26 |
| AMD | 2488 | operating | 10-K/10-Q | Dec (1226) | 10-Q 2026-08-05, period 2026-06-27 |
| AVGO | 1730168 | operating | 10-K/10-Q | **Nov (1101)** | 10-Q 2026-06-09, period 2026-05-03 |
| MU | 723125 | operating | 10-K/10-Q | **Sep (0903)** | 10-Q 2026-06-25, period 2026-05-28 |
| SMCI | 1375365 | operating | 10-K/10-Q | **Jun (0630)** | 10-Q 2026-05-11, period 2026-03-31 |
| TSM | 1046179 | **other** | **20-F/6-K** | Dec | 6-K 2026-08-14, period 2026-06-30 |

**Four of six are off-calendar** (NVDA Jan, AVGO Nov, MU Sep, SMCI Jun). Any supplier cross-check
against a calendar-quarter capex panel inherits the offset problem already open under ruling (b).

### 2.3.2 TSM — a structural exclusion, not a coverage gap `[CURL-VERIFIED]`

TSM's companyfacts carries taxonomies `dei, ifrs-full, srt` and **zero `us-gaap` concepts**.

It reports under **IFRS, not US GAAP**. Every concept in all three Capex Daemon tag maps — capex,
debt, anchor — is a `us-gaap` concept. **None of them exists for TSM.** This is not a thin series or
a missing tag; the entire resolution layer is inapplicable, and it is compounded by FPI cadence
(20-F annual, 6-K interim, no 10-Q).

Bringing TSM in requires a **parallel IFRS concept map**, which is a separate build, not a roster
addition. Recommend **EXCLUDE from the supplier bucket for now**, re-entered only if an IFRS map is
ever ruled.

### 2.3.3 Datacenter segment revenue — absent from the API for every supplier `[CURL-VERIFIED]`

No supplier exposes a datacenter/DC-segment revenue line in companyfacts. The only segment-related
concepts present are `NumberOfReportableSegments` and `NumberOfOperatingSegments` — **counts, not
revenue**. Segment revenue is dimension-qualified and therefore dropped by the aggregation API (E6),
exactly as the IRM probe below demonstrates in full.

Consequence: **the supplier leg is parser-only.** It cannot be built on Leg A at all.

### 2.3.4 What IS available per supplier `[CURL-VERIFIED]`

| ticker | inventory | purchase commitments | capex | note |
|---|---|---|---|---|
| NVDA | `InventoryNet` **25,797,000,000** (2026-04-26) | `UnrecordedUnconditional…` 22,700,000,000; `PurchaseObligation` 45,774,000,000 | `PaymentsToAcquireProductiveAssets` 1,757,000,000 | **capex tag migrated** — `PaymentsToAcquirePropertyPlantAndEquipment` stops 2020 |
| AMD | 8,468,000,000 (2026-06-27) | `UnrecordedUnconditional…` **30,276,000,000** | 1,197,000,000 | clean |
| AVGO | 4,328,000,000 | `UnrecordedUnconditional…` **128,110,000,000** | 481,000,000 | the largest forward commitment in the probe |
| MU | 8,567,000,000 | `UnrecordedUnconditional…` 6,700,000,000 — **stale, 2023-08-31** | **19,602,000,000** | largest supplier capex by far |
| SMCI | 11,103,376,000 | `PurchaseObligation` 10,100,000,000 | 133,769,000 | asset-light assembler |
| TSM | — | — | — | IFRS; nothing applicable |

Two observations offered without recommendation:

- **AVGO's $128.1B of unrecorded purchase obligations** is larger than any single forward-commitment
  figure in the builder universe except Meta's. As a cross-check on the buildout it is potentially
  the strongest supplier-side series available.
- **NVDA repeats the AMZN tag-migration pattern** — a capex concept abandoned in 2020 with a live
  successor. The recency resolver handles it, but it confirms the pattern is not rare.

### 2.3.5 Quant boundary — the premise does not hold on disk `[CURL-VERIFIED]`

The order states *"Quant owns TSMC monthly revenue; propose the split, don't implement."*

**There is no `quant_daemon` in the monorepo.** `daemons/` contains biz, capex, chatter, common,
news_watch, research, scout and smart_money. Quant is planned, not built, so there is currently no
owner to split *from* (E3 — reporting rather than assuming).

What does exist is **`news_watch_daemon/themes/ai_capex_cycle.yaml`** — active since 2026-05-13, 439
lines — whose stated material signals include *"foundry capacity announcements (TSMC, Samsung,
Intel)"*. So TSMC is already watched, by News Watch, as narrative.

**Proposed split (for ruling, not implementation):**

| surface | owner | rationale |
|---|---|---|
| TSMC **monthly revenue releases** | Quant, when it exists | High-frequency numeric series; not an SEC surface at all |
| TSMC **foundry announcements** | News Watch (`ai_capex_cycle`, already live) | Narrative signal, already covered |
| TSMC **20-F financials** | Capex Daemon — **only if an IFRS map is ruled** | Currently inapplicable per §2.3.2 |

### 2.3.6 A cross-daemon overlap that affects CD-2 `[CURL-VERIFIED]`

`ai_capex_cycle.yaml` lists as a material signal: *"hyperscaler earnings-call capex guidance changes
(META, MSFT, GOOG, AMZN, ORCL — both raises and cuts are signal)"*.

That is the **announced-vs-reported guidance leg** the Capex Daemon deferred to phase 2 under R6.
News Watch may already be capturing it. Before CD builds a guidance leg, someone should check what
that theme actually produces — building a second capture of the same signal would be duplicated
work, and the two could disagree.

Flagged, not resolved; it is a cross-daemon question and out of this order's scope.

---

## TASK 2.2 — HOSTS

### 2.2.1 IRM (Iron Mountain, CIK 1020569) — the mandatory check

**(a) Does a datacenter segment exist, and is it API-visible?**

`[PARSER]` Our own inline-XBRL parser, run against IRM's 10-Q instance (`irm-20260630_htm.xml`,
accession 0001020569-26-000071, 1,238 facts), finds the segment axis verbatim:

```
StatementBusinessSegmentsAxis -> GlobalDataCenterBusinessMember
StatementBusinessSegmentsAxis -> GlobalRecordsandInformationManagementBusinessMember
StatementBusinessSegmentsAxis -> CorporateAndOtherMember
StatementBusinessSegmentsAxis -> ReportableSegmentAggregationBeforeOtherOperatingSegmentMember
```

77 facts are dimensioned to `GlobalDataCenterBusinessMember`, 56 of them revenue.

**API-visibility test, run explicitly** `[CURL-VERIFIED]`:

| figure | value | in companyfacts? |
|---|---|---|
| Q2-2026 datacenter segment revenue | 262,871,000 | **ABSENT** |
| H1-2026 datacenter segment revenue | 517,596,000 | **ABSENT** |
| Q2-2025 datacenter segment revenue | 189,401,000 | **ABSENT** |
| consolidated revenue (control) | 3,965,211,000 | **present** |

E6 reproduced cleanly on a new surface: the consolidated control passes through, every
dimension-qualified segment figure is dropped. **Segment data is parser-only.**

**(b) The figures** `[PARSER]`

| period | datacenter segment revenue | YoY |
|---|---|---|
| Q2 2025 (Apr–Jun) | 189,401,000 | — |
| Q2 2026 (Apr–Jun) | **262,871,000** | **+38.8%** |
| H1 2025 | 362,598,000 | — |
| H1 2026 | **517,596,000** | **+42.7%** |

Datacenter is **13.1%** of IRM's H1-2026 consolidated revenue.

**(c) Is datacenter capex separable? NO — and this is the material limitation.** `[PARSER]`

Zero capex facts in the filing carry the segment dimension. IRM's capex concepts —
`PaymentsToAcquirePropertyPlantAndEquipment`, `PaymentsToAcquireIntangibleAssets`,
`CapitalExpendituresIncurredButNotYetPaid` — are **all undimensioned**, i.e. consolidated only.

So IRM satisfies the admission rule (it owns and hosts datacenter property) but the daemon can see
its datacenter **revenue** and not its datacenter **spending**. Since the spending curve is the
whole point, IRM enters — if ratified — as a partially-measurable name and must carry a coverage
status saying so. Proposed status: `SEGMENT-REVENUE-ONLY`, capex `UNSEPARABLE`.

---

## SECTIONS PENDING — Tasks 2.1 and 2.4

(populated from the parallel sweep legs)
