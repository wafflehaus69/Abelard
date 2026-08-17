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

## TASKS 2.1 / 2.4 — SWEEP RESULTS, AND A VERIFICATION THAT DID NOT RUN

Four sweeps returned **120 candidates**, of which **21 distinct names carried INCLUDE**.

> ⚠️ **The adversarial verification agent failed** — it hit a session limit and returned nothing.
> Under the CD-R1 precedent an unverified sweep recommendation is not a finding. **I re-verified all
> 21 INCLUDE candidates myself**, mechanically: CIK→ticker resolution against the SEC ticker map, and
> capex resolution through the daemon's own recency resolver against live companyfacts.
>
> **The verification refuted or downgraded a majority of them.** What follows is my verdict, not the
> sweeps'.

### 2.1a — CIK integrity `[CURL-VERIFIED]`

No wrong CIKs. But five resolve to a **preferred or warrant ticker** rather than the common line —
CLSK→`CLSKW`, SLNH→`SLNHP`, PLD→`PLDGP`, GPUS→`GPUS-PD`, GDS→`GDHLF`. The entity is right in every
case; the *display ticker* is not. That is an E10 finding with a real consequence: a panel resolving
display tickers from `company_tickers.json` will label Soluna as "SLNHP". **Display resolution must
prefer the common-share line, not the first match.**

`StratCap Digital Infrastructure` (CIK 1868516) **has no ticker at all** — a non-traded REIT. It
files, so it is visible; it is not publicly traded, so whether it belongs is a judgment call.

### 2.1b — Capex verification, run through our own resolver `[CURL-VERIFIED]`

| ticker | taxonomy | resolved capex concept | quarters | TTM | verdict |
|---|---|---|---|---|---|
| PLD | us-gaap | `PaymentsToDevelopRealEstateAssets` | 60 | **$2.770B** | material |
| IRM | us-gaap | `PaymentsToAcquirePropertyPlantAndEquipment` | 70 | **$2.146B** | material, **not separable** |
| AMT | us-gaap | `PaymentsToAcquirePropertyPlantAndEquipment` | 51 | **$1.799B** | material |
| BTBT | us-gaap | `PaymentsToAcquirePropertyPlantAndEquipment` | 10 | **$0.483B** | material, **double-counts WYFI** |
| MARA | us-gaap | `PaymentsToAcquirePropertyPlantAndEquipment` | 46 | $0.344B | material |
| CCOI | us-gaap | `PaymentsToAcquirePropertyPlantAndEquipment` | 64 | $0.158B | modest |
| DGXX | ifrs-full + us-gaap | `PaymentsToAcquirePropertyPlantAndEquipment` | 4 | $0.098B | modest, thin |
| CLSK | us-gaap | `PaymentsToAcquireProductiveAssets` | 45 | $0.091B | modest |
| GPUS | us-gaap | `PaymentsToAcquirePropertyPlantAndEquipment` | 63 | $0.032B | small, **Q2 late (NT 10-Q)** |
| SLNH | us-gaap | `PaymentsToAcquirePropertyPlantAndEquipment` | 56 | $0.030B | small |
| AVX | us-gaap | `PaymentsToAcquirePropertyPlantAndEquipment` | 12 | $0.004B | **trivial** |
| STRATCAP | us-gaap | `PaymentsToAcquireProductiveAssets` | 8 | $0.004B | **trivial, non-traded** |
| PWCM | us-gaap | `PaymentsToAcquirePropertyPlantAndEquipment` | 22 | $0.002B | **trivial** |
| CDP | us-gaap | **MULTILINE** — refuses | — | — | resolver refuses |
| HIVE | ifrs-full + us-gaap | **MULTILINE** — refuses | — | — | resolver refuses |
| VNET | us-gaap | resolves, but 1 quarter ending **2024-12-31** | 1 | — | **stale** |
| CSQR | us-gaap | resolves, **0 derivable quarters** | 0 | — | **no series** |
| AIB | us-gaap | **NO CAPEX CONCEPT** | — | — | **REFUTED** |
| GDS | us-gaap | **NO CAPEX CONCEPT** | — | — | **REFUTED** |
| IOND | *(none)* | **NO CAPEX CONCEPT** | — | — | **REFUTED** |
| BTDR | **ifrs-full only** | **NO us-gaap CAPEX** | — | — | **structural exclusion** |

**Four claims refuted outright.** AIB's cited `PaymentsToAcquireLandHeldForUse` of $1.2M is not a
capex program and not in any tag map. GDS's claimed us-gaap capex does not resolve — consistent with
CD-R1's finding that GDS is annual, CNY-only. IOND's figure came from a **424B4 prospectus cash-flow
statement**, i.e. prose in a registration document, not XBRL. BTDR is `ifrs-full` only — the **same
structural exclusion as TSM**, and the sweep said so itself.

### 2.1c — Two findings worth more than the names

**BTBT consolidates WYFI, which is already in the universe.** `[CURL-VERIFIED]` Bit Digital holds
majority ownership of WhiteFiber and consolidates it, so BTBT's $483M TTM capex **contains** WYFI's
$0.4B. Admitting both uncorrected double-counts the same spend. This is the first
**consolidation-overlap** hazard in the roster and it will recur — the sector is full of
parent/subsidiary pairs that both file.

**IFRS is now a recurring exclusion, not a TSM quirk.** BTDR joins TSM: `ifrs-full`, no us-gaap
concepts, invisible to all three tag maps. Any future roster question about a foreign operator hits
this wall. It is one decision — build an IFRS map or don't — not a series of per-name calls.

### 2.4 — Equipment complex (enumeration only, no placements)

The sweep returned **50 candidates, all WATCH or EXCLUDE, zero INCLUDE** — correctly, since the
equipment complex supplies the buildout rather than building or owning it, and therefore **fails the
admission rule on its face**. Under the standing rule these are not roster candidates at all; they
would be a supplier-class bucket if ruled, and that is Mando's call, not a recon output.

Enumerated space, for the record: power/thermal (VRT, ETN, Generac, Modine, nVent, AAON, Munters),
electrical construction (PWR, FIX, EME, MYRG, IESC, Sterling), grid/turbines (GEV, Cummins,
Caterpillar), plus cooling and switchgear specialists. Full list in the workflow journal.

**A caution on this bucket that the CD-R1 finding already implies:** News Watch's `ai_capex_cycle`
theme explicitly covers *"industrial gas, transformer manufacturing, electrical equipment"* as
second-order demand. Admitting the equipment complex to the Capex Daemon would duplicate coverage
that already exists narratively. The clean boundary is **NW covers announcements; Capex covers filed
financials** — and for most equipment names the datacenter share is blended into a general
industrial line, so there is no clean financial series to cover anyway.

---

## RATIFICATION TABLE

One table, one look. **Recommendation column is mine after verification**, not the sweeps'.

| # | name | CIK | bucket | TTM capex | recommendation | why |
|---|---|---|---|---|---|---|
| 1 | **IRM** Iron Mountain | 1020569 | **host** | $2.146B consolidated | **INCLUDE, degraded** | Owns/hosts; DC segment revenue +42.7% H1 YoY, parser-only. **Capex UNSEPARABLE** — carries `SEGMENT-REVENUE-ONLY` |
| 2 | **AMT** American Tower | 1053507 | host | $1.799B | **INCLUDE, degraded** | Owns datacenter property (CoreSite). Segment capex dimension-qualified → parser-only. Towers dominate the consolidated line |
| 3 | **MARA** MARA Holdings | 1507605 | builder | $0.344B | **INCLUDE** | Owns/builds 19 sites, ~1.9GW; HPC via Exaion. Clean admission, 46 quarters |
| 4 | **CLSK** CleanSpark | 827876 | builder | $0.091B | **INCLUDE** | Owns and operates own facilities, converting power to datacenter use. Uses `ProductiveAssets` — tag-map row needed |
| 5 | **DGXX** Digi Power X | 1854368 | builder | $0.098B | **INCLUDE** | Converts powered sites to AI datacenters. Renamed from Digihost — CIK-keyed |
| 6 | **CCOI** Cogent | 1158324 | host | $0.158B | **INCLUDE, weak** | Owns datacenters, but they are a small share of a telecom capex line |
| 7 | **BTBT** Bit Digital | 1710350 | builder | $0.483B | **CONFLICT — Mando rules** | Real builder, but **consolidates WYFI already in universe**. Admit BTBT *or* WYFI, or net them |
| 8 | **SLNH** Soluna | 64463 | builder | $0.030B | **WATCH** | Genuinely builds (25MW built, 100–350MW pipeline) but capex is small today |
| 9 | **GPUS** Hyperscale Data | 896493 | host | $0.032B | **WATCH** | Owns/hosts, converting to HPC. **Five prior names**; Q2 late via NT 10-Q |
| 10 | **HIVE** HIVE Digital | 1720424 | builder | resolver **refuses** | **WATCH** | Owns Swedish datacenters; capex split across two concepts — needs a ruled tag-map row first |
| 11 | **CDP** COPT Defense | 860546 | host | resolver **refuses** | **WATCH** | MULTILINE capex; datacenter share of a defense-property REIT unquantified |
| 12 | **PLD** Prologis | 1045609 | — | $2.770B | **EXCLUDE** | Logistics REIT. Datacenter is a small side program; admitting it admits all industrial REITs |
| 13 | **AVX** AVAX One | 1826397 | — | $0.004B | **EXCLUDE** | $4M TTM is not a spending curve |
| 14 | **PWCM** PowerCompute | 1640384 | — | $0.002B | **EXCLUDE** | $2M TTM |
| 15 | **STRATCAP** | 1868516 | — | $0.004B | **EXCLUDE** | Non-traded, no ticker, $4M |
| 16 | **CSQR** Csquare | 2105398 | — | 0 quarters | **EXCLUDE** | No derivable series |
| 17 | **VNET** VNET Group | 1508475 | — | stale 2024-12-31 | **EXCLUDE** | One quarter, 18 months stale |
| 18 | **AIB** AIB Data Centers | 2070542 | — | none | **EXCLUDE — claim refuted** | No capex concept; cited figure was $1.2M of land |
| 19 | **GDS** GDS Holdings | 1526125 | — | none | **EXCLUDE — claim refuted** | No resolvable us-gaap capex |
| 20 | **IOND** Ionic Digital | 2007691 | — | none | **EXCLUDE — claim refuted** | Figure came from a 424B4 prospectus, not XBRL |
| 21 | **BTDR** Bitdeer | 1899123 | — | IFRS only | **EXCLUDE — structural** | `ifrs-full`, no us-gaap. Same wall as TSM |
| 22 | **TSM** TSMC | 1046179 | supplier | IFRS only | **EXCLUDE — structural** | `ifrs-full`, zero us-gaap concepts |
| 23 | **NVDA / AMD / AVGO / MU / SMCI** | — | **supplier** | see §2.3.4 | **Mando's call** | Ruled in as a bucket; parser-only, never blended into the spending aggregate |
| 24 | Equipment complex (50 names) | — | — | — | **EXCLUDE from roster** | Fails admission on its face; duplicates NW coverage |

### Premises-verdict

| premise | verdict | evidence |
|---|---|---|
| A sweep can enumerate the roster without verification | **FAILS** | The verification agent died; my own check refuted 4 of 21 INCLUDEs outright and downgraded most of the rest |
| Datacenter segment data is reachable | **HOLDS — parser only** | IRM segment revenue extracted; all three figures absent from companyfacts, consolidated control present |
| Suppliers can be built on the API | **FAILS** | No supplier exposes DC-segment revenue in companyfacts; supplier leg is parser-only |
| The us-gaap tag maps cover foreign operators | **FAILS** | TSM and BTDR are `ifrs-full` with zero us-gaap concepts |
| Roster names are independent entities | **FAILS** | BTBT consolidates WYFI — first consolidation-overlap hazard |
| The admission rule is decidable from filings | **HOLDS** | Every INCLUDE carries quoted filing language; the equipment complex fails it cleanly |

### Open for Mando

1. **BTBT/WYFI consolidation** — admit which, or net?
2. **IFRS map** — one decision covering TSM, BTDR and every future foreign operator.
3. **Display-ticker resolution** — prefer common shares over the first map match (SLNHP, PLDGP…).
4. **IRM/AMT degraded admission** — in with `SEGMENT-REVENUE-ONLY`, or out until capex separates?
5. **Non-traded filers** (StratCap) — admissible in principle?
6. **Supplier bucket placement** — NVDA/AMD/AVGO/MU/SMCI confirmed; TSM excluded pending IFRS.
