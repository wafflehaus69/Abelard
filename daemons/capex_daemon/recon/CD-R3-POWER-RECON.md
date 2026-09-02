# ORDER CD-R3 — POWER-LEG RECON

**Report-only. Read-only. No admissions, no roster changes, no code touched.** Executed 2026-09-02.
Every number below was fetched live this session through the daemon's own `edgar` client, its
declared User-Agent, and the `config.PACE` 0.15s floor; concept resolution ran through the daemon's
own `tagmap.resolve` against `tagmap.CANDIDATES[tagmap.CAPEX]`, and filing-level facts came out of
the daemon's own `ixbrl.parse_instance`.

Evidence tags: `[API]` SEC companyfacts, fetched live · `[PARSER]` extracted with the Capex Daemon's
inline-XBRL parser from the named accession · `[SUB]` EDGAR submissions API · `[SEARCH]` EDGAR
company search.

**Scope measured:** 68 tickered candidates enumerated across utilities, IPPs and generation/grid
equipment; **63 measured against companyfacts** (5 have no entry in `company_tickers.json`).
**43 latest 10-Qs parsed fact-by-fact** (113,162 facts) for dimensions and prose. Plus **14
subsidiary registrants** resolved by name and 13 measured, and **7 foreign registrant status checks**.
**85 names carry a verdict in the table.**

---

## 1. THE ANSWER

**A power leg is buildable, but not as a capex leg — and calling it one would be the single most
expensive mistake available here.** Total utility capex is measurable, deep and clean: **58 of 63** names
resolve a capex concept, 57 yield at least one derivable quarter, and 47 carry an unbroken run of 12
or more quarters — many of 60–73 — running to 2026-06-30. It is also
**almost entirely not an AI number.** A utility's capital program is poles, wires, substations,
storm hardening, gas mains, solar farms and rate-base growth that would exist with or without a
single GPU, and **not one issuer in this complex publishes a datacenter-attributable capex figure in
XBRL.** Zero. Measured, not assumed: across 63 companyfacts documents there is **no concept whose
name contains "DataCenter", "LargeLoad", "Hyperscale", "Colocation" or "ContractedCapacity"**, and
across 43 parsed filings there is **no segment axis or member naming a data center** — the
`GlobalDataCenterBusinessMember` escape hatch that rescued IRM in CD-R2 does not exist here.

What *is* disclosed, and what a power leg would therefore actually measure, is **contracted load in
megawatts** — signed electric service agreements, PPAs and interconnection commitments naming a
datacenter counterparty, quantified in MW or GW and stated in prose. That is a genuine, high-signal,
regex-extractable series (E2-compatible), and it is a **demand-commitment** series, not a spending
series: it reads the buildout from the grid side, the way the supplier bucket reads it from the
invoice side. There is exactly **one** issuer in the entire complex that puts a datacenter-attributable
number into XBRL — Dominion's `HighLoadMember` revenue line — and it is dimension-qualified, so it is
parser-only. **The honest finding is that datacenter-attributable capital spending is not disclosed
by anyone; datacenter-attributable contracted load is disclosed by about a dozen, in prose, in
megawatts.**

---

## 2. THE THREE MEASUREMENTS THAT DECIDE THIS

### 2.1 No datacenter concept exists in the aggregation API `[API]`

63 companyfacts documents indexed, 826 concepts in the largest. Regex
`datacent|largeload|hyperscale|colocat|contractedcapacity` over every concept key in every document:

| result | count |
|---|---|
| names carrying a datacenter-named concept | **0 of 63** |
| names carrying `RevenueRemainingPerformanceObligation` | 32 |
| names carrying `RevenueRemainingPerformanceObligationPercentage` | 5 |

RPO is the only attribution-adjacent concept present anywhere, and **it is total-company RPO**. This
is E23 in its purest form: the concept exists, it is large, it is not the thing.

### 2.2 No datacenter dimension exists in the filings either `[PARSER]`

43 latest 10-Qs parsed. Strict scan of every axis→member pair (2,000–8,000 facts per filing,
52–267 distinct pairs each): **0 of 43 carry a datacenter/large-load/hyperscale/colocation member.**

Broadening the regex to catch load classes under non-obvious names
(`highload|largeload|largeprimary|extralarge|largecommercial|specialcontract|…`) found **three**:

| ticker | axis → member | facts | is it datacenter-defined? |
|---|---|---|---|
| **D** | `ContractWithCustomerSalesChannelAxis -> HighLoadMember` | 8 | **YES — defined in the filing** |
| EXC | `MajorCustomersAxis -> LargeCommercialIndustrialMember` | 56 | No — conventional rate class |
| WEC | `MajorCustomersAxis -> LargecommercialandindustrialcustomersMember` | 4 | No — conventional rate class |

EXC and WEC are E23 decoys: the member name reads like attribution and is not. Dominion's is real,
because Dominion defines it in the same filing.

### 2.3 The one XBRL-quantified series in the complex — Dominion `HighLoadMember` `[PARSER]`

Accession **0001193125-26-327487** (10-Q, period 2026-06-30), `d-20260630_htm.xml`. Dominion's own
footnote defines the member:

> "Represents customers in Virginia, including certain data centers, with actual or anticipated
> forecast demand of 25 MW or higher and annual load factor of 75% or higher."

Concept `RevenueFromContractWithCustomerExcludingAssessedTax`, dims
`ContractWithCustomerSalesChannelAxis=HighLoadMember; ProductOrServiceAxis=ElectricityUsRegulatedMember`:

| period | high-load revenue | YoY |
|---|---|---|
| Q2 2025 | 422,000,000 | — |
| **Q2 2026** | **814,000,000** | **+92.9%** |
| H1 2025 | 805,000,000 | — |
| **H1 2026** | **1,450,000,000** | **+80.1%** |

Every fact is dual-tagged, once consolidated and once at
`LegalEntityAxis=VirginiaElectricAndPowerCompanyMember`, at **identical values** — all of it is VEPCO.

**E6 control, run explicitly** `[API]`:

| figure (2025-04-01..2025-06-30) | value | in companyfacts? |
|---|---|---|
| `RevenueFromContractWithCustomerExcludingAssessedTax`, HighLoad-dimensioned | 422,000,000 | **ABSENT** |
| same concept, consolidated (control) | 3,718,000,000 | **present** |
| H1: HighLoad-dimensioned | 805,000,000 | **ABSENT** |
| H1: consolidated (control) | 7,803,000,000 | **present** |

E6 reproduced cleanly on a new surface with a passing control. **This series is parser-only.**

Two cautions that must travel with it. First, it is **revenue, not capex** — it measures what
Dominion bills high-load customers, not what Dominion spends to serve them. Second, the member is
**"high load", not "data center"**: it is defined by a 25 MW / 75% load-factor threshold and includes
non-datacenter industrial load. It is the closest thing to attribution in the complex and it is still
a proxy.

---

## 3. E23, IN THE WILD — the numbers that read like AI numbers and are not

This complex is unusually rich in large figures sitting next to the words "data center". Every one of
these was extracted this session; none of them is a datacenter capex number.

| the number | who | what it actually is |
|---|---|---|
| **$78 billion five-year capital plan** | AEP 10-Q | AEP's **entire** capital plan. The words "large load additions" appear in the same sentence. Overwhelmingly transmission and distribution. |
| **$94.4 billion in RPO** | GEV 10-K | GE Vernova **Power segment** backlog, in a sentence that also says "hyperscalers and data centers". Gas turbines sold to everyone. |
| **$176.3 billion RPO** | GEV 10-Q | Total company. Splits **only** Product $87.8B / Service $88.5B. No end-market cut exists. |
| **$20 billion in data centers** | WEC 10-Q | **Microsoft's** announced capex, quoted inside a utility filing. Not WEC's spend — and MSFT is already in the roster, so extracting it would double-count. |
| **6.8 GW** | PEG 10-Q | A **PJM market-wide** resource shortfall, not PEG's datacenter load. |
| **6,800 MW** | FE 10-Q | A **PJM Board** procurement plan, not FirstEnergy's. |
| **150 MW / 100 MW / 75 MW** | AEP, NRG, OGE, TXNM | Regulatory **thresholds** defining a tariff class. Not quantities of anything. |
| **$44.1B / $33.6B / $24.1B RPO** | CAT, PWR, ETN | Total backlog. No datacenter cut in any of them. |

A regex that pairs "data center" with a nearby number flags 25 of 43 filings. **Hand-inspection
reduces that to roughly a dozen genuine issuer-attributed quantities.** Any extractor built here must
distinguish *the issuer's own contracted datacenter load* from *a threshold*, *a market-wide RTO
figure*, *a counterparty's capex*, and *a total-company backlog*. That is a parser design constraint,
and it is the whole difficulty of this leg.

---

## 4. AN E7 FINDING THAT AFFECTS THE DAEMON TODAY, POWER LEG OR NOT

Recon surfaced a live plausible-stale-resolution class in the existing tag map. Reporting it because
it is measured and it is cheap to miss; **not proposing a change** — the candidate set is Mando's.

**Utilities tag capex under concepts absent from `tagmap.CANDIDATES[CAPEX]`.** The utility-standard
line is `PaymentsForConstructionInProcess`, which is not in the set. Measured `[PARSER]`, H1-2026,
undimensioned, from the accessions in the table:

| ticker | concept the daemon resolves | H1-2026 value | the live construction line | H1-2026 value | ratio |
|---|---|---|---|---|---|
| **AEP** | `PaymentsToAcquireProductiveAssets` | **1,315,000,000** | `PaymentsForConstructionInProcess` | **5,606,000,000** | **4.26x** |
| **ED** | `PaymentsToAcquirePropertyPlantAndEquipment` *(dead since 2022-12-31)* | — | `PaymentsForConstructionInProcess` | 2,473,000,000 | — |
| **D** | resolver **refuses (MULTILINE)** on two dead tags | — | `PaymentsForProceedsFromProductiveAssets` | 5,807,000,000 | — |

On the API leg `[API]`, AEP's daemon-resolved TTM is **$4.418B**; adding `PaymentsForConstructionInProcess`
to the candidate set produces a 71-quarter series with TTM **$9.183B** — **2.08x** — and correctly
trips the MULTILINE refusal rather than silently electing one. **ED is the sharper case**: the daemon's
resolved concept yields a series whose newest derivable quarter is **2019-12-31**, while ED's live
construction line runs to **2026-03-31**. That is a six-year-stale published series, reached by exactly
the mechanism E7 exists to prevent.

**Two large utilities have no us-gaap capex concept at all** `[PARSER]` — their capex is a
**custom-namespace extension tag**, which companyfacts drops entirely (E6):

| ticker | accession | the actual capex tag | H1-2026 |
|---|---|---|---|
| **NEE** | 0000753308-26-000060 | `www.nexteraenergy.com:CapitalExpendituresIndependentPowerInvestmentsAndNuclearFuelPurchases` | **19,389,000,000** |
| **NEE** | " | `www.nexteraenergy.com:CapitalExpendituresOfFPL` | 5,780,000,000 |
| **DTE** | 0000936340-26-000146 | `www.dteenergy.com:PaymentsToAcquireProductiveAssetsIncludingPaymentsToAcquireBusinessesNetOfCashAcquired` | **2,721,000,000** |

NextEra — the largest power name on any candidate list — is **structurally invisible to Leg A**. Not
thin, not stale: absent. Parser-only or nothing.

**Consequence for any future power leg:** it would be **parser-first**, not API-first. That inverts
the daemon's current architecture, where Leg A is the history spine.

---

## 5. THE SUBSIDIARY-REGISTRANT PATH — measured, and mostly closed `[SEARCH]` `[API]` `[PARSER]`

The most attributable capex available is not datacenter-tagged, it is **legal-entity-tagged**:
multi-registrant utilities dimension their capex by operating subsidiary, and some of those
subsidiaries sit squarely in datacenter-dense territory. Measured from the parsed instances, H1-2026:

| parent | `LegalEntityAxis` member | H1-2026 capex | share of consolidated |
|---|---|---|---|
| **D** | `VirginiaElectricAndPowerCompanyMember` | **4,839,000,000** | **83.3%** of 5,807 |
| **SO** | `GeorgiaPowerMember` | **4,233,000,000** | **63.8%** of 6,639 |
| ETR | `EntergyLouisianaMember` | 2,419,557,000 | 48.1% of 5,031 |
| EXC | `CommonwealthEdisonCoMember` | 1,671,000,000 | 36.7% of 4,558 |
| AEP | `AEPTexasInc.Member` | 1,449,000,000 | 25.8% of 5,606 |
| EVRG | `EvergyKansasCentralIncMember` | 719,700,000 | 39.7% of 1,812 |

This is territory attribution, **not datacenter attribution** — Georgia Power's $4.2B is Vogtle,
distribution and everything else. It narrows the denominator; it does not isolate the numerator.

**The standalone-filer path is mostly closed.** 14 subsidiary registrants were resolved by name and
13 measured against their own companyfacts. Result: **8 of 13 have no resolvable capex concept in
their own companyfacts at all** — Georgia Power, Alabama Power, ComEd, Florida Power & Light, Pacific
Gas & Electric Co, Appalachian Power, Indiana Michigan Power, Ameren Illinois. Their capex exists only
as `LegalEntityAxis`-dimensioned facts inside the parent's combined filing, and dimension-qualified
facts are dropped by the aggregation API. Ohio Power returns 404 on companyfacts outright.

**Three exceptions, and one is excellent:**

| registrant | CIK | concept | quarters | run | newest | TTM |
|---|---|---|---|---|---|---|
| **Oncor Electric Delivery** | 1193311 | `PaymentsForConstructionInProcess` | **64** | **64** | 2026-06-30 | **$8.090B** |
| Wisconsin Electric Power | 107815 | `PaymentsToAcquirePropertyPlantAndEquipment` | 62 | 56 | 2026-06-30 | — |
| Virginia Electric & Power | 103682 | `PaymentsForProceedsFromProductiveAssets` (2 facts only) | — | — | 2026-06-30 | — |
| DTE Electric | 28385 | `PaymentsToAcquirePropertyPlantAndEquipment` | 18 | 18 | **2014-12-31** | stale |

**Oncor is the cleanest capex series in this entire recon** — 64 consecutive derivable quarters,
undimensioned, standalone, live, and it is the ERCOT wires company serving the Dallas datacenter
cluster. It also has **zero datacenter attribution**, which is precisely the point of this report.

---

## 6. THE ONE-LOOK TABLE

Attribution type: **XBRL-qty** = a datacenter-attributable number tagged in XBRL · **prose-qty** = a
number the issuer attributes to its own datacenter/large-load book, in narrative only ·
**qualitative** = the subject is discussed with no issuer-attributed magnitude · **none** = not
discussed. Capex column is the daemon's own recency resolution against live companyfacts; "q" is
derivable quarters, "run" the consecutive run ending at the newest.

### 6.1 Regulated utilities

| ticker | CIK | capex concept + newest period | q / run | DC found? | attribution type | verdict |
|---|---|---|---|---|---|---|
| **D** | 715957 | **MULTILINE — refuses**; live line is `PaymentsForProceedsFromProductiveAssets` (not in map), 2026-03-31 | 0 / 0 | **YES** | **XBRL-qty** + prose-qty (25 MW def.) | **The one real find.** HighLoad revenue +80% H1 YoY, parser-only. Capex unresolvable under the current map |
| **SO** | 92122 | `PaymentsToAcquirePropertyPlantAndEquipment` 2026-03-31 | 65 / 45 | YES | **prose-qty** — "approximately 16 gigawatts of new contracts (Large Load Contracts)" since 2023 | Strongest prose disclosure in the complex; capex separable only to `GeorgiaPowerMember` |
| **EVRG** | 1711269 | `PaymentsToAcquirePropertyPlantAndEquipment` 2026-03-31 | 35 / 35 | YES | **prose-qty** — ESAs "to serve data centers with a projected peak steady state load of approximately 2,600 MWs" | Cleanest single MW sentence; named ESA construct, recurring |
| **DTE** | 936340 | **NONE in API** — custom-ns tag only | 0 / 0 | YES | **prose-qty (MW + USD)** — "1.0 gigawatt data center agreement… expected to increase capital expenditures by approximately $5.0 billion through 2032" | **The only DC→capex dollar link found.** Capex invisible to Leg A |
| **NI** | 1111711 | `PaymentsToAcquirePropertyPlantAndEquipment` 2026-06-30 | 67 / 50 | YES | **prose-qty (USD)** — assets supporting DC customers "estimated to be between $9.25 to $9.75 billion"; issuer partitions "base business… excluding capital expenditures related to serving data center customers" | Only issuer that states the attribution split itself. 43 DC mentions |
| **AEE** | 1002910 | `PaymentsToAcquirePropertyPlantAndEquipment` 2026-06-30 | 72 / 72 | YES | **prose-qty** — ESAs "representing 2.8 gigawatts of demand" | Good series, good sentence |
| **XEL** | 72903 | `PaymentsToAcquireProductiveAssets` 2026-06-30 | 72 / 72 | YES | **prose-qty** — Google DC ESA, "$1.1 billion of benefits", 1,900 MW charge | 72/72 is the deepest clean run measured |
| **WEC** | 783325 | `PaymentsToAcquirePropertyPlantAndEquipment` 2026-06-30 | 68 / 2 | YES | **prose-qty** — "up to 2.6 GWs of load growth in the Milwaukee-to-Chicago corridor" | **Carries the $20B Microsoft E23 trap.** run=2 |
| **CNP** | 1130310 | `PaymentsToAcquirePropertyPlantAndEquipment` 2026-03-31 | 24 / 9 | YES | **prose-qty** — "approximately 14 gigawatts" Houston Electric large-user estimate | Estimate, not contracted |
| **LNT** | 352541 | `PaymentsToAcquirePropertyPlantAndEquipment` 2026-03-31 | 28 / 1 | YES | **prose-qty** — ESA with "contracted peak demand of approximately 370 MW" | Good sentence, **run=1** |
| **POR** | 784977 | `PaymentsToAcquirePropertyPlantAndEquipment` 2026-06-30 | 68 / 68 | YES | **prose-qty** — 119 MW; "12% of PGE's total retail energy deliveries" | Small utility, real disclosure |
| **AEP** | 4904 | `PaymentsToAcquireProductiveAssets` 2026-03-31 — **misses the live construction line 4.26x larger** | 40 / 7 | YES | **qualitative** (thresholds only) | 26 "large load" hits, no contracted total. **Carries the $78B trap** |
| **FE** | 1031296 | `PaymentsToAcquireProductiveAssets` 2026-03-31 | 71 / 53 | YES | **qualitative** | 16 large-load hits; the 6,800 MW is PJM's, not FE's |
| **PEG** | 788784 | `PaymentsToAcquirePropertyPlantAndEquipment` 2026-06-30 | 71 / 71 | YES | **qualitative** | The 6.8 GW is a PJM shortfall |
| **TXNM** | 1108426 | `PaymentsToAcquireProductiveAssets` 2026-03-31 | 67 / 67 | YES | **qualitative** | 1,318 MW serves "retail customers **and** a data center" — jointly attributed, not separable |
| **OGE** | 1021635 | `PaymentsToAcquireProductiveAssets` 2026-03-31 | 67 / 67 | YES | **qualitative** | Google special contract named; 75 MW is a threshold |
| **ETR** | 65984 | `PaymentsToAcquirePropertyPlantAndEquipment` 2026-03-31 | 45 / 10 | YES | **qualitative** | **33 DC mentions, 18 ESA mentions, zero attributed magnitudes** |
| **PPL** | 922224 | `PaymentsToAcquirePropertyPlantAndEquipment` 2026-03-31 | 71 / 71 | YES | **qualitative** | 11 DC + 6 hyperscale hits, tariff discussion only |
| **DUK** | 1326160 | `PaymentsToAcquirePropertyPlantAndEquipment` 2026-03-31 | 69 / 67 | YES | **qualitative** | Surprisingly thin for its exposure |
| **PNW** | 764622 | `PaymentsToAcquireProductiveAssets` 2026-06-30 | 68 / 68 | YES | **qualitative** | Only DC dollar is a $34M switchgear equity stake |
| **SRE** | 1032208 | `PaymentsToAcquireProductiveAssets` 2026-03-31 | 65 / 65 | YES | **qualitative** | Oncor sits below; see subsidiary table |
| **CMS** | 811156 | `PaymentsToAcquirePropertyPlantAndEquipment` 2026-06-30 | 58 / 2 | YES | **qualitative** | run=2 |
| **PCG** | 1004980 | `PaymentsToAcquirePropertyPlantAndEquipment` 2026-06-30 | 70 / 68 | YES | **qualitative** | 1 DC mention |
| **AES** | 874761 | `PaymentsToAcquirePropertyPlantAndEquipment` 2026-06-30 | 72 / 72 | YES | **qualitative** | Global mix; territory attribution meaningless |
| **IDA** | 1057877 | `PaymentsToAcquirePropertyPlantAndEquipment` 2025-12-31 | 65 / 65 | YES | **qualitative** | 3 large-load hits, no magnitude |
| **EXC** | 1109357 | `PaymentsToAcquirePropertyPlantAndEquipment` 2026-03-31 | 65 / 62 | **NO** | **none** | Zero DC/large-load prose in the 10-Q. `LargeCommercialIndustrialMember` is a rate class, an E23 decoy |
| **ED** | 1047862 | `PaymentsToAcquirePropertyPlantAndEquipment` — **newest derivable 2019-12-31**; live line is `PaymentsForConstructionInProcess` to 2026-03-31 | 40 / 40 | **NO** | **none** | **Six-year-stale resolution.** No DC prose at all |
| **ES** | 72741 | `PaymentsToAcquirePropertyPlantAndEquipment` 2026-06-30 | 64 / 64 | **NO** | **none** | Zero DC prose |
| **NEE** | 753308 | **NONE in API** — custom-ns tag only ($19.4B H1) | 0 / 0 | **NO** | **none** | Largest name, **structurally invisible to Leg A**, and its 10-Q says nothing about data centers |

### 6.2 IPPs, merchant and nuclear generators

| ticker | CIK | capex concept + newest period | q / run | DC found? | attribution type | verdict |
|---|---|---|---|---|---|---|
| **TLN** | 1622536 | `PaymentsToAcquirePropertyPlantAndEquipment` 2026-06-30 | 21 / 12 | YES | **prose-qty** — AWS PPA, "up to 960 MW of long-term power to the AWS Data Campus from Susquehanna" | **Named counterparty, named site, quantified.** The purest single datapoint in the recon |
| **CEG** | 1868275 | `PaymentsToAcquirePropertyPlantAndEquipment` 2026-06-30 | 22 / 22 | YES | **prose-qty** — "380 MW agreement with Dallas-based CyrusOne" | Clean; 22/22 |
| **NRG** | 1013871 | `PaymentsToAcquirePropertyPlantAndEquipment` 2026-06-30 | 70 / 35 | YES | **qualitative** | 13 large-load hits; 75 MW is SB6's statutory threshold |
| **VST** | 1692819 | `PaymentsToAcquirePropertyPlantAndEquipment` 2026-06-30 | 39 / 39 | YES | **qualitative** | 3 hyperscale + 4 DC hits, no magnitude |
| **CWEN** | 1567683 | `PaymentsToAcquirePropertyPlantAndEquipment` 2026-06-30 | 54 / 47 | **NO** | **none** | PPA discussion only, no DC |
| **XIFR** | 1603145 | `PaymentsToAcquirePropertyPlantAndEquipment` 2026-03-31 | 51 / 51 | **NO** | **none** | ex-NextEra Energy Partners; ticker `NEP` is dead |
| **ORA** | 1296445 | `PaymentsToAcquirePropertyPlantAndEquipment` 2026-06-30 | 40 / 37 | **NO** | **none** | Geothermal; no DC disclosure |
| **BWXT** | 1486957 | `PaymentsToAcquirePropertyPlantAndEquipment` 2026-06-30 | 64 / 64 | **NO** | **none** | Naval/nuclear; RPO is total |
| **LEU** | 1065059 | `PaymentsToAcquirePropertyPlantAndEquipment` 2026-06-30 | 56 / 40 | **NO** | **none** | Fuel cycle |
| **OKLO** | 1849056 | `PaymentsToAcquirePropertyPlantAndEquipment` 2026-06-30 | 12 / 12 | **NO** | **none** | Pre-revenue SMR; capex real but tiny, no DC prose in the 10-Q |
| **SMR** | 1822966 | `PaymentsToAcquirePropertyPlantAndEquipment` 2026-06-30 | 19 / 4 | **NO** | **none** | NuScale; run=4 |
| **NNE** | 1923891 | `PaymentsToAcquirePropertyPlantAndEquipment` 2026-06-30 | 3 / 1 | **NO** | **none** | 3 quarters. No series |
| **BEP** | 1533232 | **NONE** — `ifrs-full` only | 0 / 0 | **NO** | **none** | **Structural exclusion.** Same wall as TSM/BTDR |
| **BEPC** | 1791863 | **NONE** — `ifrs-full` only | 0 / 0 | **NO** | **none** | **Structural exclusion** |
| **NEP** | — | not in `company_tickers.json` | — | — | — | Renamed to XPLR; ticker retired. CIK-keyed lookup survives it (E10) |

### 6.3 Generation and grid equipment

| ticker | CIK | capex concept + newest period | q / run | DC found? | attribution type | verdict |
|---|---|---|---|---|---|---|
| **GEV** | 1996810 | `PaymentsToAcquireProductiveAssets` 2026-06-30 | 14 / 14 | **weak** | **qualitative** | RPO **$176.3B** Q2-26 / $150.2B FY25, split **only** Product/Service. 10-Q has **zero** DC mentions; the 10-K's "$94.4B" sits beside "hyperscalers and data centers" and is a segment total. **Biggest E23 trap in the file** |
| **VRT** | 1674101 | `PaymentsToAcquirePropertyPlantAndEquipment` 2026-06-30 | 30 / 30 | YES | **qualitative** | 31 DC mentions in the 10-K, **zero DC-attributed numbers**. Its only DC dollar is a lawsuit. Effectively a pure-play that discloses no attribution |
| **PWR** | 1050915 | `PaymentsToAcquirePropertyPlantAndEquipment` 2026-06-30 | **73 / 73** | YES | **qualitative** | Deepest series measured. RPO $33.6B, undimensioned |
| **ETN** | 1551182 | `PaymentsToAcquirePropertyPlantAndEquipment` 2026-06-30 | 58 / 58 | YES | **qualitative** | 14 DC hits; the $1.43B is the Fibrebond acquisition price, not DC revenue |
| **POWL** | 80420 | `PaymentsToAcquirePropertyPlantAndEquipment` 2026-06-30 | 64 / 64 | YES | **qualitative** | Says DC projects are "becoming a larger component of our backlog" — direction, no magnitude. RPO $2.4B total |
| **BE** | 1664703 | `PaymentsToAcquirePropertyPlantAndEquipment` 2026-03-31 | 35 / 35 | YES | **qualitative** | Oracle AI-datacenter warrant named; no MW or revenue attributed |
| **CMI** | 26172 | `PaymentsToAcquirePropertyPlantAndEquipment` 2026-06-30 | 72 / 72 | YES | **qualitative** | "higher demand… especially in data center applications" — direction only |
| **CAT** | 18230 | `PaymentsToAcquirePropertyPlantAndEquipment` 2026-06-30 | 72 / 72 | YES | **qualitative** | Same pattern. RPO $44.1B, undimensioned |
| **GNRC** | 1474735 | `PaymentsToAcquirePropertyPlantAndEquipment` 2026-06-30 | 64 / 64 | YES | **qualitative** | 20 DC hits; RPO is extended **warranties**, irrelevant |
| **NVT** | 1720635 | `PaymentsToAcquirePropertyPlantAndEquipment` 2026-06-30 | 38 / 38 | **NO** | **none** | No DC prose in the 10-Q |
| **MOD** | 67347 | `PaymentsToAcquirePropertyPlantAndEquipment` 2026-06-30 | 65 / 65 | **NO** | **none** | — |
| **HUBB** | 48898 | `PaymentsToAcquireProductiveAssets` 2026-03-31 | 67 / 67 | **NO** | **none** | — |
| **EMR** | 32604 | `PaymentsToAcquirePropertyPlantAndEquipment` 2026-06-30 | 72 / 72 | **NO** | **none** | — |
| **ROK** | 1024478 | `PaymentsToAcquirePropertyPlantAndEquipment` 2026-06-30 | 70 / 3 | **NO** | **none** | run=3 |
| **TT** | 1466258 | `PaymentsToAcquirePropertyPlantAndEquipment` 2026-06-30 | 41 / 2 | **NO** | **none** | run=2 |
| **JCI** | 833444 | `PaymentsToAcquireProductiveAssets` 2026-03-31 | 70 / 34 | **NO** | **none** | — |
| **WCC** | 929008 | `PaymentsToAcquireProductiveAssets` 2026-06-30 | 68 / 68 | **NO** | **none** | — |
| **ATKR** | 1666138 | `PaymentsToAcquirePropertyPlantAndEquipment` 2026-06-26 | 44 / 44 | **NO** | **none** | — |
| **AAON** | 824142 | `PaymentsToAcquireMachineryAndEquipment` 2026-06-30 | 60 / 14 | **NO** | **none** | Rare concept — third-order candidate in the map |

### 6.4 Foreign equipment makers — filer status, verified rather than assumed `[SUB]` `[SEARCH]`

| name | ticker | CIK | what it actually files | verdict |
|---|---|---|---|---|
| **Siemens Energy AG/ADR** | SMERY / SMEGF | 1830056 | **7 filings, all `F-6EF` / `F-6 POS` / `F-6` / `EFFECT`** — ADR registration only. **No 20-F, no 6-K, no periodic report ever** | **NOT USABLE.** It has an EDGAR presence, which is why this had to be checked rather than assumed. It files no financials |
| **Mitsubishi Heavy Industries** | — | — | 20-F search on "mitsubishi" returns **only MITSUBISHI UFJ FINANCIAL GROUP**. MHI is absent from EDGAR | **NOT AN SEC FILER** |
| **Schneider Electric** | — | — | Company search returns **only Schneider National** (a trucking company) | **NOT AN SEC FILER** |
| **ABB Ltd** | ABBNY / ABLZF | 1091587 | Real historic filer: **23 × 20-F, 280 × 6-K**. But `PaymentsToAcquireProductiveAssets` **stops at 2023-12-31**, annual-only, 48 facts, **0 derivable quarters** | **EXCLUDE — 2.5 years stale, annual, FPI cadence** |
| **Vestas Wind Systems** | VWSYF | 1330306 | `SUPPL`, `F-6EF`, `ARS`, and a **`12G3-2B`** — the Rule 12g3-2(b) reporting exemption. No 20-F | **NOT USABLE** — exempt from reporting |
| Siemens AG | — | 1135644 | Historic 20-F registrant | Distinct entity from Siemens Energy; not a power-equipment read |
| Hitachi Ltd | HTHIY / HTHIF | 47710 | Registrant exists; not measured this session | **UNMEASURED — named, not silently dropped** |

### 6.5 Subsidiary registrants `[SEARCH]` `[API]`

| registrant | CIK | own-companyfacts capex | verdict |
|---|---|---|---|
| **Oncor Electric Delivery** | 1193311 | `PaymentsForConstructionInProcess`, **64 q / run 64**, 2026-06-30, TTM **$8.090B** | **Best standalone series in the recon.** ERCOT wires to the Dallas cluster. Zero DC attribution. Concept **not in the daemon's map** |
| **Wisconsin Electric Power** | 107815 | `PaymentsToAcquirePropertyPlantAndEquipment`, 62 q / run 56, 2026-06-30 | Usable series; no DC attribution |
| **Virginia Electric & Power** | 103682 | `PaymentsForProceedsFromProductiveAssets`, 2 facts, 2026-06-30 | Thin in own facts; **rich in the parent's filing** — carries D's entire HighLoad series |
| **DTE Electric** | 28385 | `PaymentsToAcquirePropertyPlantAndEquipment`, 18 q, newest **2014-12-31** | **Stale by 11 years** |
| Georgia Power | 41091 | **none resolvable** | Capex exists only as `LegalEntityAxis`-dimensioned facts in SO's combined filing → dropped by the API (E6) |
| Alabama Power | 3153 | **none resolvable** | same mechanism |
| Commonwealth Edison | 22606 | **none resolvable** | same mechanism |
| Florida Power & Light | 37634 | **none resolvable** | same mechanism |
| Pacific Gas & Electric Co | 75488 | **none resolvable** | same mechanism |
| Appalachian Power | 6879 | **none resolvable** | same mechanism |
| Indiana Michigan Power | 50172 | **none resolvable** | same mechanism |
| Ameren Illinois | 18654 | **none resolvable** | same mechanism |
| Public Service Co of Oklahoma | 81027 | concept resolves, **0 derivable quarters** | No series |
| Ohio Power | 73986 | **companyfacts 404** | Door closed at the API; parser path untried |
| Entergy Louisiana / Evergy Kansas Central | — | name search returned no match | **Search-string artifact, not a finding.** Both appear as `LegalEntityAxis` members in the parent filings (measured above). CIK unresolved this session |

---

## 7. WHAT A POWER LEG COULD NOT TELL YOU

These limits are structural, not effort-limited. They must travel with any finding this leg produces.

1. **It cannot tell you how many dollars a utility spends on data centers.** Nobody discloses it. The
   closest anyone comes is DTE's "expected to increase capital expenditures by approximately $5.0
   billion through 2032" — a *forward projection tied to one contract*, not a reported spend, and the
   only instance found in 43 filings.

2. **It cannot be summed into the existing capex aggregate. Ever.** Utility capex is a rate-base
   number. Adding Southern's $6.6B and Dominion's $5.8B to the datacenter total would inflate it with
   substations and gas mains — the same class of error the `host` bucket exists to prevent, an order
   of magnitude larger. This is the E23 boundary and it is not negotiable by any threshold.

3. **It measures commitments, not construction.** A 960 MW AWS PPA is a signed intention with a
   multi-year ramp. Contracted load can be signed, re-priced, delayed or cancelled without a dollar
   moving. The series would lead spending by years and would not confirm it.

4. **The unit is megawatts, not dollars,** so it cannot be blended with any existing panel series and
   cannot be dead-banded against any existing measured distribution. A power leg needs its own
   distribution measured before any band exists (E8) — and this recon deliberately proposes none.

5. **The number is undated within the quarter and often cumulative-since-inception.** Southern's "16
   gigawatts of new contracts" is *since 2023*; Evergy's 2,600 MW covers "three new projects and the
   expansion of two". Differencing prose totals across quarters is not the same operation as
   differencing a YTD cumulative, and issuers restate these narrative totals without marking them.

6. **Prose is not a schema.** There is no tag, no context, no unit declaration and no restatement
   discipline. A wording change breaks the extractor silently. Every other Capex Daemon series rests
   on XBRL contexts; this one would rest on sentences.

7. **It double-counts against the roster if handled carelessly.** WEC's 10-Q quotes Microsoft's $20B
   Wisconsin datacenter investment. MSFT is already a roster member. The same dollar is visible from
   three sides now — hyperscaler capex, supplier revenue, utility narrative — and only the first is a
   spending number.

8. **Coverage is lopsided and thin where it matters most.** NextEra and DTE — two of the most
   datacenter-exposed names — have **no API capex at all**. Dominion's capex does not resolve under
   the current map. The three biggest power stories in the complex are the three hardest to measure.

---

## 8. PREMISES-VERDICT

| premise | verdict | evidence |
|---|---|---|
| Utilities/IPPs/equipment makers have measurable capex | **HOLDS** | 58 of 63 resolve a concept, 47 with a run of 12+ quarters; PWR 73/73, XEL 72/72, AEE 72/72 |
| That capex is an AI-buildout number | **FAILS — decisively** | Zero datacenter-attributable capex disclosed by any of 63 names. E23 applies at full force |
| Datacenter attribution exists as a segment/dimension, as it did for IRM | **FAILS** | 0 of 43 filings carry a datacenter member. Only Dominion's `HighLoadMember` is datacenter-defined, and it is revenue |
| Backlog/RPO carries a datacenter cut | **FAILS** | 32 names carry RPO; every RPO fact examined across 9 issuers and both form types splits only by product/service or satisfaction period |
| Datacenter attribution exists at all | **HOLDS — in prose, in megawatts** | ~12 issuers quantify their own contracted DC/large-load book: SO 16 GW, EVRG 2,600 MW, TLN 960 MW, CEG 380 MW, AEE 2.8 GW, LNT 370 MW, DTE 1.0 GW |
| The daemon's capex tag map covers utilities | **FAILS** | `PaymentsForConstructionInProcess` and `PaymentsForProceedsFromProductiveAssets` are absent from the candidate set; AEP resolves 4.26x low at filing level, ED's series is 6 years stale |
| companyfacts sees every issuer's capex | **FAILS** | NEE and DTE tag capex in a custom namespace; both return no capex concept at all |
| IFRS remains a recurring wall | **HOLDS** | BEP and BEPC join TSM and BTDR — `ifrs-full` only |
| Foreign equipment makers are reachable | **FAILS, verified per name** | Siemens Energy files F-6 only; Mitsubishi Heavy and Schneider Electric are not EDGAR registrants; Vestas is 12g3-2(b) exempt; ABB is 2.5 years stale |
| Subsidiary registrants offer cleaner attribution | **MOSTLY FAILS** | 8 of 13 have no own-companyfacts capex. Oncor is the exception and has zero DC attribution |

---

## 9. OPEN FOR MANDO — no admissions proposed

1. **Is a megawatt series in scope at all?** Every existing panel series is dollars. A power leg's
   only real product is **contracted datacenter load in MW**, which cannot be summed with, compared
   to, or dead-banded against anything the daemon currently publishes. If the answer is "the daemon
   measures spending in dollars", this leg does not exist and the recon ends here cleanly.

2. **Prose extraction — is it admissible as a series, or only as an annotation?** E2 is satisfied
   (regex/parser-tier, zero LLM). E1 is the problem: prose has no schema, no restatement discipline,
   and a wording change fails silently. A middle position exists — extract the MW figures as
   **evidence attached to a name**, never as a published series.

3. **Dominion `HighLoadMember` — one-name exception, or not worth a leg?** It is the only
   XBRL-quantified datacenter-attributable series in the entire complex, it is parser-only, it grew
   +80% H1 YoY, and it is *revenue*, not capex — the same shape as IRM's admission under
   `SEGMENT-REVENUE-ONLY`. Precedent exists. Whether one name justifies a bucket does not.

4. **The capex tag-map gap is live and independent of this order.** `PaymentsForConstructionInProcess`
   and `PaymentsForProceedsFromProductiveAssets` are missing from `CANDIDATES[CAPEX]`. No current
   roster member is affected *that this recon checked*, but the gap is real and ED demonstrates the
   failure mode is silent 6-year staleness, not an error. **Should the candidate set be re-audited
   against the existing roster before anything else?** That is a correctness question, not a power
   question, and it may be the most valuable thing in this file.

5. **NextEra and DTE tag capex in a custom namespace.** If a power leg is ever built, Leg A cannot be
   its spine — the architecture inverts to parser-first. Is that acceptable, or is
   "invisible to companyfacts" a structural exclusion the way IFRS is?

6. **Oncor.** 64 consecutive quarters, live, standalone, undimensioned, ERCOT wires to Dallas —
   the best capex series this recon found, and a **wholly-owned subsidiary of Sempra with no
   datacenter attribution whatsoever**. It is an excellent series measuring the wrong thing. Named
   here so the refusal is explicit rather than silent.

7. **Cross-daemon boundary, again.** CD-R2 flagged that News Watch's `ai_capex_cycle` theme already
   covers "transformer manufacturing, electrical equipment" as second-order demand. Datacenter PPAs
   and large-load tariff filings are announcement-shaped, which is News Watch's surface, not Capex's.
   **Before any power leg is built, someone should check what that theme already produces.** The clean
   boundary CD-R2 proposed — NW covers announcements, Capex covers filed financials — would place most
   of this leg's actual content on the News Watch side.

---

## APPENDIX — accessions cited

All parsed with the daemon's own `ixbrl.parse_instance` from the extracted instance alongside each
primary document.

| ticker | form | period | accession |
|---|---|---|---|
| D | 10-Q | 2026-06-30 | 0001193125-26-327487 |
| SO | 10-Q | 2026-06-30 | 0000092122-26-000054 |
| AEP | 10-Q | 2026-06-30 | 0000004904-26-000059 |
| EVRG | 10-Q | 2026-06-30 | 0001711269-26-000100 |
| DTE | 10-Q | 2026-06-30 | 0000936340-26-000146 |
| NEE | 10-Q | 2026-06-30 | 0000753308-26-000060 |
| NI | 10-Q | 2026-06-30 | 0001111711-26-000088 |
| XEL | 10-Q | 2026-06-30 | 0000072903-26-000154 |
| WEC | 10-Q | 2026-06-30 | 0000783325-26-000087 |
| ES | 10-Q | 2026-06-30 | 0001628280-26-051906 |
| AEE | 10-Q | 2026-06-30 | 0001002910-26-000023 |
| CNP | 10-Q | 2026-06-30 | 0001130310-26-000041 |
| LNT | 10-Q | 2026-06-30 | 0000352541-26-000043 |
| POR | 10-Q | 2026-06-30 | 0001193125-26-326379 |
| ETR | 10-Q | 2026-06-30 | 0000065984-26-000283 |
| EXC | 10-Q | 2026-06-30 | 0001109357-26-000080 |
| PPL | 10-Q | 2026-06-30 | 0000922224-26-000044 |
| ED | 10-Q | 2026-06-30 | 0001047862-26-000142 |
| TLN | 10-Q | 2026-06-30 | 0001622536-26-000066 |
| CEG | 10-Q | 2026-06-30 | 0001868275-26-000104 |
| VST | 10-Q | 2026-06-30 | 0001692819-26-000019 |
| NRG | 10-Q | 2026-06-30 | 0001013871-26-000020 |
| GEV | 10-Q | 2026-06-30 | 0001996810-26-000148 |
| GEV | **10-K** | 2025-12-31 | 0001996810-26-000015 |
| VRT | 10-Q | 2026-06-30 | 0001628280-26-050609 |
| VRT | **10-K** | 2025-12-31 | 0001674101-26-000008 |
| POWL | 10-Q | 2026-06-30 | 0000080420-26-000107 |
| POWL | **10-K** | 2025-09-30 | 0000080420-25-000152 |
| ETN | 10-Q | 2026-06-30 | 0001551182-26-000030 |
| PWR | 10-Q | 2026-06-30 | 0001050915-26-000025 |
| BE | 10-Q | 2026-06-30 | 0001628280-26-050247 |
| CAT | 10-Q | 2026-06-30 | 0000018230-26-000046 |
| CMI | 10-Q | 2026-06-30 | 0000026172-26-000029 |
| GNRC | 10-Q | 2026-06-30 | 0001437749-26-025669 |
| DUK | 10-Q | 2026-06-30 | 0001326160-26-000040 |
| FE | 10-Q | 2026-06-30 | 0001031296-26-000123 |
| PEG | 10-Q | 2026-06-30 | 0001193125-26-332943 |
| TXNM | 10-Q | 2026-06-30 | 0001108426-26-000048 |
| OGE | 10-Q | 2026-06-30 | 0001021635-26-000025 |
| PNW | 10-Q | 2026-06-30 | 0000764622-26-000041 |
| SRE | 10-Q | 2026-06-30 | 0001032208-26-000045 |
| CMS | 10-Q | 2026-06-30 | 0000811156-26-000028 |
| PCG | 10-Q | 2026-06-30 | 0001004980-26-000048 |
| AES | 10-Q | 2026-06-30 | 0000874761-26-000144 |
| IDA | 10-Q | 2026-06-30 | 0001057877-26-000129 |
| CWEN | 10-Q | 2026-06-30 | 0001628280-26-053421 |

All 43 sampled filings are 10-Qs for period 2026-06-30 except the three 10-Ks marked above. The
remaining accessions not listed here are recorded in this session's working set and were used only
for the aggregate counts in §2.1 and §2.2, never for a per-name claim.
