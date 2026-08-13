# ORDER CD-1 — CAPEX DAEMON BUILD SPEC

**Status: DRAFT — awaiting ratification by Abelard and Mando. No code written against this yet.**
Drafted by ClaudeCode 2026-08-13 from `recon/CD-R1-RECON.md` (recon 2026-08-07, re-checked
2026-08-13), rulings R1–R6 incl. amendments R2a/R4a, and `doctrine/ENGINEERING.md` E1–E19.

Every constant below is either **measured** (evidence cited) or marked **OPEN** and left unset.
Per E8, no threshold ships without an observed distribution behind it.

---

## 1. What it is

A structured sensor over AI capital expenditure across hyperscalers, neoclouds and datacenter
landlords. It publishes a normalized quarterly capex panel, a credit-to-capex series, and forward
commitment series — plus the coverage and provenance needed to tell whether any number is
trustworthy.

**The daemon is dumb; Abelard judges.** It extracts, normalizes, reconciles and reports. It does not
classify a regime, call a top, or interpret a divergence.

## 2. What it does NOT do

- **No phase classification in v1.** The acceleration → deceleration → contraction call requires
  thresholds that do not yet have an observed distribution. CD-1 ships the *instrumentation* that
  produces the distribution; the thresholds are a later ruling. (E8, distribution-first.)
- **No guidance/announced-capex leg.** Held at phase 2 per R6. CD-1 measures *reported* capex and
  *contracted* forward commitments — the latter turns out to cover much of what the guidance leg was
  wanted for (§7.3).
- **No NPORT-P / fund-side SPV monitoring.** Horizon list, separate probe.
- **No cross-daemon calls.** Joins to Quant/Price happen at the Abelard layer, on the shared
  canonical timestamp (E13).
- **No LLM in the data path** (E2). Zero LLM calls in ingest, normalization or reconciliation.
- **No trading signal, no recommendation.**

---

## 3. Universe and tiers

Ratified 2026-08-07. Tier membership is **computed, not hand-maintained** — the tier is a function of
measured coverage, re-evaluated every scan.

| Tier | Definition | Members at ratification |
|---|---|---|
| **CORE** | ≥12 consecutive derivable quarters, current | MSFT, GOOGL, AMZN, META, ORCL, DLR, EQIX, RIOT, CORZ, WULF, CIFR, HUT, APLD (13) |
| **DEGRADED-SHORT** | 4–11 consecutive derivable quarters | CRWV, IREN, GLXY, WYFI (4) |
| **THIN** | <4 consecutive derivable quarters | FRMI, KEEL, SPCX (3) |
| **ANNUAL-DEGRADED** | FPI; no structured quarterly capex exists | NBIS, BABA (+BIDU, VNET, GDS, TSM, BTDR optional) |
| **MIRROR** | demand/supply sensor; revenue or backlog is the read, not own capex | SNOW + 25 names |

**R1 — graduation is automatic.** THIN → DEGRADED-SHORT → CORE on reaching 4 and 12 consecutive
derivable quarters. No per-name ruling. A tier **downgrade** is a loud event: it means coverage
regressed and must appear in the coverage report, never silently.

THIN and ANNUAL-DEGRADED are distinct by construction: THIN is a maturation queue with a known
schedule (SPCX graduates on its Q3 10-Q, ~Nov 2026); ANNUAL-DEGRADED is a permanent structural
ceiling.

**Universe file** is data, not code: `capex_daemon/data/universe.csv`, keyed on CIK, carrying
`cik,ticker_display,bucket,tier_override,notes`. `tier_override` exists only to force a name out
(EXCLUDE); it can never force a name *up* a tier — tiers are earned by measured coverage.

---

## 4. Identity layer (R3 / E10)

**All entity state keys on CIK, zero-padded to 10 digits.** Ticker and name are display attributes
resolved at read time and re-resolved on **every scan**.

- Resolution source: `https://www.sec.gov/files/company_tickers.json` + `submissions/CIK##########.json`.
- **Name-change detection compares normalized `name` values across scans** — never `name` against the
  `formerNames` list. (Corrected 2026-08-13: APLD carries a `formerNames` entry identical to its
  current name, which trips the naive comparison; see E10 citation correction.)
- A detected change writes an **identity-discontinuity marker** row; it never rewrites history.
  Precedent: KEEL←Bitfarms (2026-03-31), CIFR←Cipher Mining (2026-02-23), IREN←Iris Energy.
- **SIC is not used for anything.** SIC 6199 "Finance Services" is assigned to KEEL, WYFI, IREN, HUT,
  CORZ and CIFR — six datacenter operators. Screening or bucketing on SIC is barred (R5).

---

## 5. Data legs

### Leg A — XBRL aggregation API (history, primary)

`data.sec.gov/api/xbrl/companyfacts` and `/companyconcept`. Cheap, keyless, deep history.
**Known and permanent exclusions** (E6): dimension-qualified facts, custom-namespace facts
(`meta_`, `baba_`), and — temporarily — not-yet-ingested filings.

### Leg B — Filing-level inline-XBRL instance parser (**the new primitive**)

The only route to figures Leg A structurally cannot return, and the freshness path.

- Discovery: `submissions/CIK##########.json` parallel arrays (`form`, `filingDate`,
  `accessionNumber`, `primaryDocument`, `items`, `isXBRL`, `isInlineXBRL`).
- Instance: `<accession>/<name>_htm.xml`, or the iXBRL primary document; `FilingSummary.xml` maps
  `R#.htm` fragments when a rendered table is wanted.
- Must parse: `contextRef` → period (`instant` / `startDate`+`endDate`) **and dimensions**
  (`xbrldi:explicitMember`, axis + member), `unitRef`, and **`scale` / `decimals`**.
- **`scale` is a G1 hazard unique to this leg** (E5): MSFT's $329.1B not-yet-commenced lease figure
  carries `scale="9"`. Ignoring it is wrong by nine orders of magnitude.
- **Nested-fact trap, measured:** that same figure is wrapped in **two nested `ix:nonFraction`
  elements** with different `contextRef`s — `LeaseContractualTermAxis` → `FinanceLeaseMember` and
  → `OperatingLeaseMember` — carrying the *identical* value. There is no finance/operating split.
  The parser must **deduplicate on (concept, period, unit, value)** across nested facts or it
  overstates by exactly 2×.

Justification for Leg B is now split cleanly by mechanism:
- *Correctness* — dimensioned and custom-namespace facts are permanently invisible to Leg A.
  Measured: Meta VIE exposure $45.95B (dimensioned, `DataCenterCampusInLouisianaMember`) vs the
  $5.58B Leg A returns — an 8.2× silent understatement.
- *Freshness* — Leg A lagged Meta's Q2 10-Q by 8–14 days (absent 2026-08-07, present 2026-08-13).
  A deceleration sensor cannot be two weeks blind during the post-filing window.

### Leg C — Debt events via EX-FILING FEES XBRL

Not the prospectus body (unstructured). The **filing-fee exhibit** is a clean instance, namespace
`http://xbrl.sec.gov/ffd/*`, one context per tranche via `ffd:OfferingAxis` / `dei:lineNo`.

Fields used: `ffd:OfferingSctyTp` (hard `"Debt"` discriminator, no NLP), `ffd:AmtSctiesRegd`
(per-tranche face), `ffd:TtlOfferingAmt`, `ffd:RegnFileNb`, and **`ffd:FnlPrspctsFlg`**.

Five measured traps, all mandatory to handle:

1. **Preliminary 424B2s carry blank amounts** (`"$    "`, "SUBJECT TO COMPLETION"). Gate on
   `FnlPrspctsFlg = true`; a preliminary is skipped, not parsed.
2. **Sum of tranches ≠ `TtlOfferingAmt`.** ORCL: 8 tranches = $25,000,000,000 vs
   `TtlOfferingAmt` 24,961,775,000 ($38,225,000 gap). Report both; never reconcile silently.
3. **Exchange offers are not new money.** MSFT's recent 424B3s state "will not receive any cash
   proceeds." Must be excluded, or the daemon books phantom issuance.
4. **The 8-K item filter is unreliable.** Meta's debt 8-K filed under items **8.01/9.01**, not
   1.01/2.03. Do not gate discovery on item codes alone.
5. **Use-of-proceeds is not reliably attributable.** MSFT/META are boilerplate general-corporate;
   ORCL names capital expenditures. **CD-1 does not attempt debt→AI-capex attribution.** It records
   the text verbatim and reports issuance unattributed.

---

## 6. Three resolution maps (R2 / E7)

Capex, debt and PP&E-anchor concepts are each **issuer- and era-specific**. All three resolve the
same way:

**Resolution rule.** Among candidate concepts present for the issuer, select by **most recent
observation**, tie-broken on fact count. Never a fixed global preference order.

**Provenance rule.** The resolved concept name is written **on every series row**. A series without
its resolved tag is not interpretable and must not be published.

Measured failure this rule prevents (E7): fixed-preference resolution returned AMZN capex from
`PaymentsToAcquirePropertyPlantAndEquipment`, abandoned in 2017 — **$7.42B against a true $173.03B,
23× low, no error, no null, entirely plausible.**

Known heterogeneity to seed the candidate sets (all `[CURL-VERIFIED]`):

| map | observed variants |
|---|---|
| capex | `PaymentsToAcquirePropertyPlantAndEquipment` (most), `PaymentsToAcquireProductiveAssets` (AMZN post-2017, GLXY, KEEL), `PaymentsToAcquireOtherPropertyPlantAndEquipment` (EQIX), `PaymentsToDevelopRealEstateAssets` (DLR), `PaymentsToAcquireMachineryAndEquipment` (HUT) |
| debt | `ProceedsFromDebtMaturingInMoreThanThreeMonths` (MSFT), `ProceedsFromDebtNetOfIssuanceCosts` (GOOGL), `ProceedsFromIssuanceOfLongTermDebt` (AMZN/META/CRWV), `ProceedsFromIssuanceOfSeniorLongTermDebt` (ORCL), **seven variants across eras for WULF** |
| PP&E anchor | `PropertyPlantAndEquipmentGross`, `PropertyPlantAndEquipmentAndFinanceLeaseRightOfUseAssetBeforeAccumulatedDepreciationAndAmortization` (META — **bundles finance-lease ROU**), `RealEstateInvestmentPropertyAtCost` (DLR), **absent entirely** (CORZ) |

`ProceedsFromIssuanceOfLongTermDebt` — the concept named in the original thesis — is **absent for
MSFT and GOOGL**, the #1 and #2 spenders. A single-tag debt leg reports zero for both.

**EQIX double-tags** the same value under two capex concepts. Resolution selects one; summation
across concepts is barred.

---

## 7. Series construction

### 7.1 Discrete quarters

Facts are deduplicated on `(start, end)` keeping the latest `filed`. **Series key on `start`/`end`
only — never on `fy`/`fp`**, which describe the *report* a fact appeared in, not its period (measured:
META's 2023 FY fact carries `fy=2025`).

- Native 3-month facts (80–100 day duration) pass through.
- Otherwise derive by differencing consecutive cumulative periods **within a fiscal-year cohort**
  (facts sharing a `start`).
- **The fiscal-Q4 discrete quarter is never natively tagged for anyone** — always FY minus 9M.
- Every row records `provenance ∈ {native, derived}`.

### 7.2 Calendar alignment

SEC's own `frame` (e.g. `CY2026Q1`) is used where present, **but is not trusted for off-calendar
filers**: SNOW's Feb–Apr period is labelled `CY2026Q1` (Jan–Mar), a one-month shift. Off-grid filers
carry an explicit `calendar_offset_days` and are excluded from calendar-quarter aggregates unless the
offset is within tolerance — **OPEN: tolerance unset pending the offset distribution across the
universe.** Known off-grid: ORCL (Nov/Feb/May), APLD (May FYE), IREN (June FYE), SNOW (Jan FYE).

### 7.3 Forward commitments (the leading indicator)

Contracted-but-unspent obligations lead reported capex and are largely structured:

- `UnrecordedUnconditionalPurchaseObligationBalanceSheetAmount` — META **$278.99B** at 2026-06-30,
  undimensioned, in Leg A. Trajectory $34.1B → $279.0B across six quarters (8.2×).
- `ContractualObligation` — META $349.31B, undimensioned.
- MSFT's equivalents are **Leg B only**: $329.1B not-yet-commenced datacenter leases (dimensioned)
  and a **$194.06B** purchase-commitment table that is **not tagged at all** — prose/HTML, no
  `ix:nonFraction`, no R-file. That one figure is the sole v1 exception to "no HTML scraping";
  **OPEN: include as a table-parse target, or record as a declared gap?**
- `NotYetCommenced` concepts: **absent across all six probe issuers.** Not available as a uniform
  series.

### 7.4 Credit-to-capex

Numerator = debt issuance (Leg A quarterly + Leg C event-level). Denominator = capex.
**Computed on TTM only** — measured quarterly ratios are 0% in non-issuance quarters and 120–148% in
issuance quarters, which is not a usable series. TTM values at recon: GOOGL 68%, AMZN 47%, ORCL 51%,
CRWV 86%, META 39%, **MSFT 0%** (explicitly tagged zero — Microsoft funds capex from operating cash
flow and finance leases, not bonds).

**Finance-lease additions are part of capital deployment, not a footnote.**
`RightOfUseAssetObtainedInExchangeForFinanceLeaseLiability` runs 11.5–32% of MSFT's total deployment
and swings quarter to quarter, changing series *shape*. Quarterly for MSFT; effectively annual for
META; absent for WULF — so coverage is per-issuer and must be declared.

---

## 8. Gates

Every gate **reports**; none silently corrects (E1, E5).

| Gate | Rule | Grounding |
|---|---|---|
| **CD-G1 Unit/scale** | Never assume scale. Leg A returns absolute integers; Leg B must read `scale`/`decimals`. Unresolvable → `undetermined` basis, surfaced. | E5, `scale="9"` observed |
| **CD-G1c Currency** | Currency is a G1 dimension. Same-CIK multi-currency facts are discriminated, never merged. NBIS carries capex in **both RUB and USD**; GDS is CNY-only. | E5 extension |
| **CD-G2 Provenance** | Every series row carries its resolved concept, `provenance`, source leg, accession, and `filed` date. | E7 |
| **CD-G3 Anchor** | TTM capex+finance-leases reconciled against ΔPP&E-anchor. Flag outside **0.5×–2.0×**. Unavailable anchor → row marked `unanchored`, never silently passed. | R2a, measured |
| **CD-G4 Coverage** | Tier, consecutive-quarter count, and every gap are published with the panel. | E1, E14 |
| **CD-G5 Identity** | CIK-keyed; name compared across scans; discontinuity markers written. | R3, E10 |

**CD-G3 is an order-of-magnitude bound, not a precision bound.** Measured TTM ratios: MSFT 0.98×,
META 1.01×, RIOT 0.98×, ORCL 0.91×, EQIX 0.73×. Quarterly residuals range ±20% (MSFT/META) to −694%
(RIOT) — hence TTM only. The 0.5–2.0× band catches the 23× class without firing on business noise.
Anchor unavailable for WULF, APLD, DLR (no annual anchor pair with four derivable quarters) and CORZ
(no anchor concept at all).

---

## 9. Outputs (R4 / R4a / E14)

**Aggregate = total + three-bucket decomposition, always co-presented.** Never a blended headline.

| bucket | members |
|---|---|
| Hyperscalers | MSFT, GOOGL, AMZN, META, ORCL |
| Leveraged builders | CORZ, WULF, CIFR, HUT, APLD, RIOT (+CRWV, KEEL on graduation) |
| REIT landlords | DLR, EQIX |

- Each bucket subtotal carries a **top-2 concentration share**. (The builder bucket spans two orders
  of magnitude — CRWV $16.60B TTM against RIOT $0.28B.)
- The per-name table is always one level below the aggregate, never optional.
- **Sums and composition only. No weighted index** without a separate ruled decision.
- Every published aggregate carries its coverage: names included, names excluded, and why.

---

## 10. Package layout and reuse

`daemons/capex_daemon/` following the flat convention (biz/chatter pattern):

```
capex_daemon/
  AGENTS.md  SOUL.md  pyproject.toml
  capex_daemon/
    __init__.py __main__.py cli.py config.py orchestrator.py storage.py
    identity.py        # CIK resolution, rename detection      (§4)
    facts_api.py       # Leg A                                  (§5)
    ixbrl.py           # Leg B — the new primitive              (§5)
    feeexhibit.py      # Leg C                                  (§5)
    tagmap.py          # recency resolution, all three maps     (§6)
    normalize.py       # discrete quarters, calendar alignment  (§7)
    gates.py           # CD-G1..G5                              (§8)
    panel.py           # buckets, aggregates, concentration     (§9)
    data/universe.csv
  recon/CD-R1-RECON.md
  tests/
```

**Reused unchanged:** `abelard_common.http_client.HttpClient` (retry/backoff, 429 + `Retry-After`,
forced UTF-8, redaction, injected logger); `abelard_common.errors`.
**Conventions adopted from Smart Money:** `EDGAR_CONTACT` env with fail-loud, UA template,
`PACE = 0.15` request floor, `company_tickers.json` resolution.
**Pattern extended:** `marketcap.py`'s concept-fallback list becomes §6's recency resolver.
**Genuinely new:** `ixbrl.py`. Nothing in the monorepo reads `contextRef`, `explicitMember`,
`nonFraction` or `FilingSummary.xml` today.

---

## 11. Build sequence and acceptance

Sequenced so each phase is verifiable before the next depends on it (E3).

| # | Phase | Acceptance |
|---|---|---|
| 1 | `identity.py` + universe file | All 20 INCLUDE names resolve by CIK; KEEL/CIFR/IREN renames produce discontinuity markers; APLD produces **none** |
| 2 | `facts_api.py` + `tagmap.py` | AMZN resolves to `PaymentsToAcquireProductiveAssets` and returns **$173.03B** TTM, not $7.42B |
| 3 | `normalize.py` | Reproduces the CD-R1 panel exactly: MSFT 72 / ORCL 64 / DLR 63 quarters, 0 gaps; META 2026Q1 = $18,997,000,000 |
| 4 | `gates.py` | MSFT/META/RIOT/ORCL/EQIX TTM anchor ratios reproduce 0.98/1.01/0.98/0.91/0.73; WULF/APLD/DLR/CORZ report `unanchored` |
| 5 | `ixbrl.py` | Extracts META H1-2026 capex **$49,113,000,000** from `meta-20260630_htm.xml`; recovers the Louisiana VIE **$46.03B**; MSFT $329.1B **once**, not twice |
| 6 | `feeexhibit.py` | META 2026-05-01 424B2 yields 6 tranches summing to $25,000,000,000 with `TtlOfferingAmt` 24,967,390,000 reported separately; the preliminary 424B2 is skipped |
| 7 | `panel.py` + CLI | Aggregate publishes three buckets + concentration + coverage |

Every acceptance figure above is a value measured in CD-R1 — the recon doubles as the test fixture.

---

## 12. Open questions for ratification

1. **Phase-classification thresholds** — deliberately unset. CD-1 produces the distribution; the
   thresholds are a later ruling. Confirm this split.
2. **MSFT's $194.06B untagged commitment table** (§7.3) — parse the HTML table, or record as a
   declared gap? It is the single largest forward number with no XBRL representation, and admitting
   it opens an HTML-scraping surface the rest of the spec avoids.
3. **Calendar-offset tolerance** (§7.2) — unset pending the offset distribution.
4. **ANNUAL-DEGRADED breadth** — BIDU/VNET/GDS/TSM/BTDR in, or NBIS+BABA only?
5. **Aggregate cadence** — publish on filing arrival (event-driven, E-doctrine preference) or on a
   fixed quarterly close? Filing waves are ragged; ORCL and APLD never align.
