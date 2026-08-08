# ORDER CD-R1 — CAPEX DAEMON RECON REPORT

**Executed:** 2026-08-07 · report-only · no code, no commits, no repo writes
**Evidence tags:** `[CURL-VERIFIED]` fetched live this session · `[FTS-RESULT]` EDGAR full-text search · `[WEB]` web source · `[INFERENCE]` reasoning

Access primitives verified live: `data.sec.gov/api/xbrl/companyfacts`, `/companyconcept`, `/frames`,
`data.sec.gov/submissions`, `efts.sec.gov/LATEST/search-index`, `www.sec.gov/Archives/...`.
All SEC calls carried a declared User-Agent. `[CURL-VERIFIED]`

---

## TASK 2 — XBRL COMPANYFACTS PROBE

### 2.1 Concept presence by issuer

`[CURL-VERIFIED]` — companyfacts pulled for all six probe issuers plus AMZN/ORCL/GOOGL/CRWV
added during the probe to test tag divergence.

| Issuer | CIK | FY end | Filer | us-gaap concepts | companyfacts bytes |
|---|---|---|---|---|---|
| MSFT | 789019 | Jun 30 | 10-K/10-Q | 562 | 4,881,196 |
| META | 1326801 | Dec 31 | 10-K/10-Q | 456 | 2,700,046 |
| SNOW | 1640147 | Jan 31 | 10-K/10-Q | 361 | 1,559,957 |
| WULF | 1083301 | Dec 31 | 10-K/10-Q | 512 | 2,340,019 |
| SPCX | 1181412 | Dec 31 | 10-Q only (1) | 174 | 124,544 |
| NBIS | 1513845 | Dec 31 | **20-F/6-K (FPI)** | 520 | 1,568,737 |

### 2.2 Capex tag — NOT uniform

`[CURL-VERIFIED]`

- Canonical `PaymentsToAcquirePropertyPlantAndEquipment` present for MSFT, META, SNOW, WULF, SPCX, NBIS, GOOGL, ORCL, CRWV.
- **AMZN migrated tags.** `PaymentsToAcquirePropertyPlantAndEquipment` covers 2007-12-31..2017-03-31;
  `PaymentsToAcquireProductiveAssets` covers 2016-12-31..2026-06-30. A single-tag extractor gets
  Amazon's history *or* its present, never both.
- SNOW additionally tags `PaymentsToDevelopSoftware` (capitalized software, separate line).

### 2.3 Unit scale

`[CURL-VERIFIED]` companyfacts exposes values under a `units` key (`"USD"`), as **absolute integers**.
There is no `decimals`/`scale` attribute at this layer — no scale inference required, no G1 hazard
from the API itself.

**One live multi-unit trap:** NBIS carries `PaymentsToAcquirePropertyPlantAndEquipment` in **both RUB
and USD**. RUB series 2009-12-31..2023-12-31 (Yandex N.V. legacy), USD 2011-12-31..2025-12-31.
For FY2023 the two disagree economically (90,641,000,000 RUB vs 82,900,000 USD) — they are not the
same reporting entity's continuing operations. **The same CIK carries two economically different
entities across the Yandex/Nebius divestiture.** Any NBIS backfill must start post-divestiture.

### 2.4 Finance leases — the hidden leg, quarterly only for Microsoft

`[CURL-VERIFIED]`

`RightOfUseAssetObtainedInExchangeForFinanceLeaseLiability`:
- **MSFT** — 120 facts, 54 native quarterly, through 2026-06-30. Fully quarterly-observable.
- **META** — 27 facts, only 4 native quarterly, through 2025-12-31. Effectively **annual**.
- **WULF** — tag absent entirely (operating-lease ROU only).

Microsoft's lease-financed capacity is material and volatile:

| Quarter | cash capex | finance-lease additions | lease share of deployment |
|---|---|---|---|
| 2024Q3 | 14.92B | 4.33B | 22.5% |
| 2024Q4 | 15.80B | 6.43B | 28.9% |
| 2025Q1 | 16.75B | 3.24B | 16.2% |
| 2025Q2 | 17.08B | 6.50B | 27.6% |
| 2025Q3 | 19.39B | 9.15B | 32.0% |
| 2025Q4 | 29.88B | 6.33B | 17.5% |
| 2026Q1 | 30.88B | 4.01B | 11.5% |
| 2026Q2 | 35.80B | 5.12B | 12.5% |

Ignoring this leg understates Microsoft's capital deployment by 11.5–32% — and because the share
swings, it changes the **shape** of the series, not just its level. TTM: cash capex $115.9B +
lease additions $24.6B = **$140.6B deployed**, against OCF $182.9B.

### 2.5 Debt tags — no canonical tag survives the universe

`[CURL-VERIFIED]` Six issuers, five different tags:

| Issuer | tag actually used |
|---|---|
| MSFT | `ProceedsFromDebtMaturingInMoreThanThreeMonths` |
| GOOGL | `ProceedsFromDebtNetOfIssuanceCosts` |
| AMZN | `ProceedsFromIssuanceOfLongTermDebt` |
| META | `ProceedsFromIssuanceOfLongTermDebt` |
| ORCL | `ProceedsFromIssuanceOfSeniorLongTermDebt` |
| CRWV | `ProceedsFromIssuanceOfLongTermDebt` |
| WULF | **seven** tags used inconsistently across eras |

`ProceedsFromIssuanceOfLongTermDebt` — the tag named in the order — is **absent for MSFT and GOOGL**,
the #1 and #2 spenders. A naive extractor reports zero debt for both.

WULF's seven: `ProceedsFromConvertibleDebt`, `ProceedsFromIssuanceOfDebt`,
`ProceedsFromIssuanceOfLongTermDebt`, `ProceedsFromIssuanceOfSecuredDebt`, `ProceedsFromNotesPayable`,
`ProceedsFromRelatedPartyDebt`, `ProceedsFromShortTermDebt`. Its `ProceedsFromIssuanceOfLongTermDebt`
goes stale at 2022-12-31 and the series continues under a different tag — a **within-issuer tag
migration**, not just a cross-issuer one.

### 2.6 Commitments — Microsoft tags nothing

`[CURL-VERIFIED]`

- **MSFT: zero tags** matching `PurchaseObligation|PurchaseCommitment|ContractualObligation|OtherCommitment|UnrecordedUncond` **in companyfacts**. This is an API artifact, not a disclosure gap — see §5.6, where the filing itself is shown to tag a $329.1B figure that companyfacts drops.
- **META:** rich, current. `ContractualObligation` (40 facts, through 2026-03-31) plus the full
  maturity ladder (`DueInNextTwelveMonths`, `DueInSecondYear`…`DueAfterFifthYear`) and
  `UnrecordedUnconditionalPurchaseObligationBalanceSheetAmount` through 2026-03-31.
- **SNOW:** `PurchaseObligation` family present but **10-K only** (annual).
- **NBIS:** `PurchaseObligation` family stale — ends 2019-12-31 (Yandex era). Nothing recent.

**`NotYetCommenced` — zero matches across all six issuers.** The finance-lease/operating-lease
not-yet-commenced figure, which is where hyperscalers park contracted-but-undelivered datacenter
capacity, is **not XBRL-tagged by anyone in the probe set**. This is the leading-indicator surface
and it is structurally invisible to the XBRL leg.

### 2.7 Calendar normalization — solvable, with two distinct failure modes

`[CURL-VERIFIED]`

**Fiscal misalignment is largely solved upstream:** SEC assigns its own `frame` label
(e.g. `CY2026Q1`) to calendar-aligned periods. MSFT's Jul–Sep period is correctly framed CY2025Q3.

**Failure mode A — genuine off-grid quarters.** ORCL reports on a Nov 30 / Feb 28 / May 31 grid and
*never* coincides with a calendar quarter. SNOW reports Feb–Apr, and SEC nonetheless labels that
period `CY2026Q1` (Jan–Mar) — **a one-month misalignment baked into SEC's own frame assignment.**
Trusting `frame` blindly for off-calendar filers silently shifts them by up to a month.

**Failure mode B — `fy`/`fp` are not the period's fiscal year.** They describe the *report in which
the fact was tagged*, so comparative prior-year figures carry the current report's `fy`/`fp`.
Example `[CURL-VERIFIED]`: META's 2023-01-01..2023-12-31 fact carries `fy=2025`. **Series must be
keyed on `start`/`end`, never on `fy`/`fp`.** Facts must also be deduped on `(start,end)` keeping
the latest `filed` — the same period recurs with different filed dates, and `frame` is sometimes
only attached on the later restatement.

**Discrete quarters:** MSFT reports native 3-month cash-flow figures (102 quarterly facts). META,
SNOW, WULF and most others report **YTD only** — discrete quarters must be derived by differencing
consecutive YTD periods within a fiscal-year cohort. The fiscal-Q4 discrete quarter is *never*
natively tagged for anyone; it is always FY minus 9M.

**TTM feasibility audit** — last 12 discrete quarters, derived:

| Issuer | quarters | span | gaps | TTM capex | TTM YoY |
|---|---|---|---|---|---|
| MSFT | 12 | 2023Q3..2026Q2 | 0 | $115.9B | +79.6% |
| GOOGL | 12 | 2023Q3..2026Q2 | 0 | $132.4B | +97.7% |
| AMZN | 12 | 2023Q3..2026Q2 | 0 | $173.0B | +60.7% |
| META | 12 | 2023Q2..2026Q1 | 0 | $75.7B | +73.0% |
| ORCL | 12 | 2023Q3..2026Q2 | 0 | $55.7B | +162.4% |
| CRWV | 9 | 2024Q1..2026Q1 | 0 | $16.6B | +98.4% |
| WULF | 12 | 2023Q3..2026Q2 | 0 | $2.2B | +473.5% |
| SNOW | 12 | 2023Q3..2026Q2 | 0 | $0.1B | **−10.2%** |
| NBIS | **0** | — | — | — | **TTM IMPOSSIBLE** |
| SPCX | **0** | — | — | — | **TTM IMPOSSIBLE** |

Zero gaps across twelve quarters for every domestic 10-Q filer.

### 2.8 NBIS — structured quarterly data does not exist

`[CURL-VERIFIED]` NBIS filing mix: 239 × 6-K, 16 × 20-F, 0 × 10-Q. `entityType: "other"`.
Capex is tagged **annually only**, in 20-F.

The most recent 6-K (0001104659-26-087080, filed 2026-07-27) contains exactly three files:
the `.txt` wrapper, the 6-K cover, and `ex99-1.htm`. **No XBRL instance, no schema.**
Nebius quarterly capex exists only as prose/HTML inside an earnings-release exhibit.

### 2.9 Freshness — the aggregation API can silently omit a whole filing

`[CURL-VERIFIED]` and independently reproduced on a second endpoint.

META filed its Q2 10-Q on **2026-07-30** (accession 0001628280-26-050705, period 2026-06-30).
As of 2026-08-07, **companyfacts contains zero facts with `end=2026-06-30` for META**; the latest
`filed` date anywhere in its companyfacts is 2026-04-30. Confirmed separately via `companyconcept`
(max end = 2026-03-31).

This is **not** a uniform API lag — that hypothesis was tested and refuted:

| Issuer | filed | present in companyfacts? |
|---|---|---|
| GOOGL | 2026-07-23 | yes |
| MSFT | 2026-07-29 | yes |
| META | 2026-07-30 | **NO** |
| AMZN | 2026-07-31 | yes |
| WULF | 2026-08-05 | yes (2 days) |

**Mitigation verified.** Meta's filing *does* carry full XBRL —
`meta-20260630_htm.xml` plus `_cal/_def/_lab/_pre` linkbases. Parsing the filing-level instance
directly recovered the missing quarter:

| concept | H1 2025 | H1 2026 |
|---|---|---|
| `PaymentsToAcquirePropertyPlantAndEquipment` | 29,479,000,000 | **49,113,000,000** |
| `NetCashProvidedByUsedInOperatingActivities` | 49,587,000,000 | 64,088,000,000 |
| `ProceedsFromIssuanceOfLongTermDebt` | **0** | **24,910,000,000** |

Implied META 2026Q2 discrete capex = 49.113 − 18.997 = **$30.116B** (vs 2025Q2 $16.538B, +82%).

So: an issuer's most important quarter can be missing from the structured API for 8+ days with no
error signal, while the data sits fully tagged in the filing. A daemon reading only companyfacts
would have shown Meta flat.

---

## TASK 3 — SNOW CONTRIBUTION RULING INPUT

`[CURL-VERIFIED]` Capex ÷ revenue (`RevenueFromContractWithCustomerExcludingAssessedTax`),
each issuer on its own fiscal quarters, last 8 discrete quarters.

**SNOW**

| Quarter end | capex | revenue | ratio |
|---|---|---|---|
| 2024-07-31 | 5,043,000 | 868,823,000 | 0.58% |
| 2024-10-31 | 13,440,000 | 942,094,000 | 1.43% |
| 2025-01-31 | 11,277,000 | 986,770,000 | 1.14% |
| 2025-04-30 | 44,989,000 | 1,042,074,000 | 4.32% |
| 2025-07-31 | 16,665,000 | 1,144,969,000 | 1.46% |
| 2025-10-31 | 23,905,000 | 1,212,909,000 | 1.97% |
| 2026-01-31 | 16,069,000 | 1,283,994,000 | 1.25% |
| 2026-04-30 | 10,451,000 | 1,390,951,000 | 0.75% |

**MSFT:** 22.75% → 22.70% → 23.90% → 22.34% → 24.97% → 36.76% → 37.25% → **39.78%**
**META:** 20.92% → 20.35% → 29.81% → 30.58% → 34.81% → 36.75% → 35.70% → **33.74%**

Numbers only — the ruling is Mando's. Two observations offered without recommendation:
SNOW's ratio sits **20–50× below** the hyperscalers and its TTM capex is **−10.2% YoY** while every
other name in the panel is between +60% and +473%. Snowflake also ranks **#795** of 2,437 filers in
the CY2026Q1 capex frame.

---

## TASK 6 — SPCX BACKFILL ASSESSMENT (orchestrator-verified portion)

`[CURL-VERIFIED]` Registrant: `SPACE EXPLORATION TECHNOLOGIES CORP`, CIK **1181412**,
ticker SPCX, Nasdaq, `entityType: operating`, SIC 7370, FY end **12-31**.

The listing-date claim from the prior session is **CONFIRMED from the filing record**:

| date | form | meaning |
|---|---|---|
| 2026-03-30 | DRS | confidential draft registration |
| 2026-05-07 | DRS/A | |
| 2026-05-20 | S-1 | public registration |
| 2026-06-01, 06-03 | S-1/A | |
| 2026-06-10 | 8-A12B ×2 + CERT ×2 | exchange registration + certification |
| 2026-06-11 | EFFECT | registration effective |
| **2026-06-12** | **424B4** | **pricing prospectus — listing date** |
| 2026-06-22/23/26 | 8-K ×3 | senior notes launch → pricing → closing |
| 2026-08-04 | 10-Q | **first and only periodic report**, period 2026-06-30 |

The CIK dates to 2002 and carries 15 Form D private-placement filings — the pre-IPO history is
Reg D, not financial statements.

**XBRL status.** 174 us-gaap concepts. Capex, OCF, debt, finance leases, and
`UnrecordedUnconditionalPurchaseObligationBalanceSheetAmount` all present. But only **two duration
facts per concept**, both **six-month YTD** (2025-01-01..2025-06-30 and 2026-01-01..2026-06-30),
from the single 10-Q. **Zero discrete quarters derivable** — there is no Q1 stub filing to difference
against, because the IPO fell mid-Q2. The first derivable discrete quarter arrives with the Q3 10-Q
(~Nov 2026), as 9M − 6M.

**Scale — and why this name matters more than "adjacency".** `[CURL-VERIFIED]`

| | 2025-12-31 | 2026-06-30 |
|---|---|---|
| Assets | 92,079,000,000 | 192,770,000,000 |
| PP&E net | 42,602,000,000 | 65,736,000,000 |
| Long-term debt | 21,659,000,000 | **38,285,000,000** |
| Stockholders' equity | 2,573,000,000 | 127,224,000,000 |

H1 2026 capex **$28,476,000,000** (vs H1 2025 $6,965,000,000); H1 2026 OCF $3,466,000,000.
PP&E net rose $23.1B in six months, corroborating the gross capex figure. Long-term debt rose
**$16.6B in six months** — against $28.5B of capex, i.e. debt funded ~58% of the build, and the
June senior-notes 8-K trio is the visible event. On an annualized basis SPCX would rank among the
largest capex programs on earth.

---

## TASK 5 — OFF-BALANCE-SHEET VISIBILITY (orchestrator-verified portion)

### 5.1 The named vehicle is invisible; the structure is not

`[FTS-RESULT]` EDGAR full-text search across all Meta filings:

- `"Hyperion"` → **2 hits, neither a periodic report** (a DEF 14A 2026-04-16 and a third-party
  PX14A6G 2026-05-11). **Zero hits in any 10-K or 10-Q.**
- `"variable interest entity" + "data center"` → **5 hits**, including the 10-K (2026-01-29) and
  the 10-Qs of 2025-10-30, 2026-04-30 and 2026-07-30.

So searching for the press-reported *name* finds nothing. Searching for the *structure* finds it
immediately, in the periodic reports, every quarter.

### 5.2 The structure is disclosed, quantified, and quarterly

`[CURL-VERIFIED]` Meta's Q2 2026 10-Q states it is **not** the primary beneficiary and does not
consolidate the VIE, while providing "construction management, administrative and property
management services to the Venture." Maximum exposure to loss is disclosed as **$46.03 billion**
(2026-06-30) and $45.95 billion (2025-12-31), comprising equity investment carrying value, lease
commitments, estimated future funding commitments, and the maximum residual-value-guarantee
threshold. Other unconsolidated VIEs add $6.41B / $5.58B.

### 5.3 It is XBRL-tagged — under a custom dimension

`[CURL-VERIFIED]` from `meta-20260630_htm.xml`. Meta maintains a dedicated extension member,
**`DataCenterCampusInLouisianaMember`**:

| concept | date | value | dimension |
|---|---|---|---|
| `VariableInterestEntityEntityMaximumLossExposureAmount` | 2026-06-30 | 46,030,000,000 | **Louisiana** |
| `VariableInterestEntityEntityMaximumLossExposureAmount` | 2025-12-31 | 45,950,000,000 | **Louisiana** |
| `VariableInterestEntityEntityMaximumLossExposureAmount` | 2026-06-30 | 6,410,000,000 | (consolidated) |
| `UnrecordedUnconditionalPurchaseObligationBalanceSheetAmount` | 2026-06-30 | 278,990,000,000 | (consolidated) |
| `UnrecordedUnconditionalPurchaseObligationBalanceSheetAmount` | 2025-10-31 | 12,310,000,000 | **Louisiana** |
| `UnrecordedUnconditionalPurchaseObligationResidualValueGuaranteeMaximum` | 2025-10-31 | 28,000,000,000 | **Louisiana** |
| `UnrecordedUnconditionalPurchaseObligationBalanceSheetAmount` | 2026-07-29 | 68,000,000,000 | SubsequentEvent |

### 5.4 The real blind spot is the API layer, not the disclosure

`[CURL-VERIFIED]` — proven by direct comparison. For period 2025-12-31 both the $45.95B (Louisiana)
and $5.58B (other VIEs) facts were tagged in the 10-K filed 2026-01-29. companyfacts contains
**only the $5.58B**. Explicit test: the value 45,950,000,000 does not appear anywhere in Meta's
companyfacts.

**companyfacts exposes only undimensioned (default-context) facts.** Any figure an issuer reports
solely under a dimension is silently dropped. `UnrecordedUnconditionalPurchaseObligationResidualValueGuaranteeMaximum`
($28B Louisiana RVG) is absent from companyfacts entirely for this reason.

The disclosure is not the blind spot. **The API is.** Filing-level instance parsing recovers it.

### 5.5 Forward commitments — the leading indicator, and it is structured

`[CURL-VERIFIED]` Meta total purchase obligations (undimensioned):

| as of | value | QoQ |
|---|---|---|
| 2024-12-31 | 34,120,000,000 | — |
| 2025-03-31 | 35,270,000,000 | +3.4% |
| 2025-06-30 | 52,560,000,000 | +49.0% |
| 2025-09-30 | 58,140,000,000 | +10.6% |
| 2025-12-31 | 103,770,000,000 | +78.5% |
| 2026-03-31 | 182,880,000,000 | +76.2% |
| 2026-06-30 | **278,990,000,000** | +52.6% |

**8.2× in six quarters**, plus a further $68B tagged as a subsequent event on 2026-07-29. This is a
contracted-forward-spend series that leads reported capex, it is quarterly, and it is machine-readable.
It is the closest thing in the probe to the "announced rather than reported" primary variable — and
it required no guidance-capture leg to obtain.

### 5.6 Microsoft: $329.1 billion, tagged, and invisible to the API

`[CURL-VERIFIED]` MSFT FY2026 10-K (0001193125-26-323660, filed 2026-07-29), lease footnote:
as of 2026-06-30 Microsoft holds additional leases **"primarily for datacenters, that had not yet
commenced of $329.1 billion"**, commencing FY2027–FY2033 with terms of 1 to 20 years.

For scale: that forward pipeline is **2.8× Microsoft's entire TTM cash capex** ($115.9B).

Is it tagged? **Yes.** In the inline XBRL it is
`us-gaap:UnrecordedUnconditionalPurchaseObligationBalanceSheetAmount`, `scale="9"`, `decimals="-8"`,
instant 2026-06-30 — carrying
`dimension="us-gaap:LeaseContractualTermAxis" → msft:OperatingLeaseMember`.

**Correction from adversarial verification — a 2× trap.** My initial read saw one tagged fact. There
are **two nested `ix:nonFraction` elements** wrapping the same displayed value, with different
`contextRef`s, dimensioned on the same axis to **`msft:FinanceLeaseMember`** and
**`msft:OperatingLeaseMember`** respectively. There is **no finance-vs-operating split to recover** —
the identical figure is duplicated into both contexts. A parser that treats the two facts as
separable overstates by exactly 2×. (Same structure in the 10-Q, where the figure is $196.6B.)

Because that dimension is present, companyfacts drops it. **This is the sole reason §2.6 reported
"MSFT tags no commitments at all."** The disclosure is complete, quantified, quarterly-refreshed,
and machine-readable — and entirely absent from the API the daemon was specced against.

Two corollaries:

1. **The premise "XBRL is structurally insufficient" HOLDS — but not for the reason the order gave.**
   The data is not hidden in unfilable private-credit structures. It is filed, tagged, and precise.
   It is dropped at the *aggregation* layer, because the AI-specific figures are exactly the ones
   issuers place under dimensions. The remedy is filing-level inline-XBRL parsing, not a news sensor.
2. **`scale` is a live G1 hazard at the filing level.** companyfacts returns absolute integers
   (§2.3), but inline XBRL carries `scale="9"` — the displayed "329.1" means $329,100,000,000.
   Any filing-level parser that ignores `scale` is wrong by nine orders of magnitude. The mitigation
   in §2.9 inherits this hazard; the API layer does not have it.

**Symmetry worth noting.** Meta's forward commitment is visible via the API ($279.0B, undimensioned);
Microsoft's is not ($329.1B, dimensioned). Both are enormous, both lead reported capex, and neither
is reachable by a single uniform extraction method.

### 5.7 MSFT's second number has no tags at all

`[CURL-VERIFIED]` Separate from the lease figure, MSFT's 10-K carries a **$194.06B** purchase-commitment
table whose own footnote (d) says the commitments "primarily relate to datacenters and include open
purchase orders and take-or-pay contracts" — the closest thing Microsoft publishes to compute offtake.

Every figure in that table (`194,060`, `169,008`, `34,566`, `443,506`, `743,821`, `29,848`) appears
exactly once in the document and **none is inside an `ix:nonFraction`**. No R-file renders it
(113 reports, zero matching "contractual"). It is absent from the 10-Q entirely. This one is
genuinely prose/HTML-locked — not an API artifact.

### 5.8 Hyperion: the strong thesis is refuted, a narrower blind spot confirmed

`[CURL-VERIFIED]` / `[FTS-RESULT]`

**Refuted:** the money is on file, in two independent places. Meta's FY2025 10-K discloses the Venture
as an unconsolidated VIE with $45.95B max loss exposure, ~$27B development commitment, $12.31B lease
commitment and the ~$28B RVG — and E&Y flags the consolidation conclusion as a Critical Audit Matter.
Separately the SPV's bond is reported at position level in NPORT-P filings by the mutual funds holding it.

**Confirmed, precisely — three independent blind spots:**

1. **Naming.** "Hyperion" → 0 hits in any Meta 10-K/10-Q/8-K. "Beignet" → 0. "Blue Owl" appears once,
   in the Q3-2025 10-Q, and is gone by the 10-K. Meta says only "the Venture." Every press-derived
   search key fails against the borrower's filings.
2. **No issuer.** Beignet Investor LLC has **no CIK and files nothing** — 144A-for-life means no
   registration, no prospectus, no periodic reporting. Issuer-side monitoring is structurally impossible.
3. **Lender-side decay.** Blue Owl Real Estate Net Lease Trust's Q3-2025 10-Q names everything —
   "Beignet Investor issued $27,293,849 in senior secured notes... 6.581%... maturity May 30, 2049"
   — but only in a subsequent-events note. By the FY2025 10-K the position dissolves into
   "Investments in unconsolidated real estate affiliates." The lender asymmetry is a flash, not a feed.

**The durable sensor is NPORT-P** — quarterly, structured XML, position-level. With a correction from
adversarial verification: **the CUSIP does not solve the alias problem, it creates a second one.**
PIMCO reports the identical instrument as `<name>BEIGNET INVESTOR LLC</name>`,
`<title>PROJECT BEIGNET SR SEC 144A</title>`, `<cusip>990AAXQG4</cusip>` — a 990-series *placeholder*.
The real CUSIP `076912AA2` appears **zero** times in the PIMCO documents. Measured recall:
issuer-name search 2,559 hits (NPORT-P 2,347, N-CSR 106, N-CSRS 75, N-MFP3 16, 11-K 10) versus
CUSIP search 774 total / 704 NPORT-P. **Name is the higher-recall key; both are required.**

Other surfaces probed: hyperscaler VIE disclosure beyond Meta is **absent** (MSFT 0 hits, ORCL 0 hits;
the concept returns `NoSuchKey` for all three). Datacenter ABS-15G filings exist and identify issuers
(Aligned, Vantage, Stack, Cologix, QTS) but carry no collateral data. BDC schedules of investments are
per-position and extractable but have **no datacenter/AI sector taxonomy**, and name-matching
false-positives are demonstrated, not hypothetical (Crescent Capital BDC holds "Crusoe Bidco Limited").

---

## TASK 1 — UNIVERSE DISCOVERY

Three independent sweeps plus a filer-type verification pass over the union. **94 candidates
classified; Mando ratifies.** Counts: INCLUDE 20, MIRROR 25, WATCH 28, EXCLUDE 21.

### INCLUDE (20)

All seeds survive. MSFT, GOOGL, AMZN, META, ORCL, CRWV, WULF, NBIS (with the caveat below), plus
**EQIX, DLR, APLD, CIFR, IREN, HUT, CORZ, GLXY, RIOT** and three names not on the order's list:

| ticker | CIK | note |
|---|---|---|
| **KEEL** | 1812477 | former 40-F FPI → domestic; **discrete 89-day quarterly capex already tagged** |
| **WYFI** | 2042022 | pure-play AI infrastructure; one 10-K + three 10-Qs of history |
| **FRMI** | 2071778 | REIT-classified AI campus developer; two 10-Qs of history |

Per-name extraction facts that matter: **IREN** converted FPI→domestic but tags 10-Q capex
**year-to-date**, not discrete. **APLD** has a May FYE (off-calendar). **CORZ** still files
independently. **GLXY**'s Helios capex sits inside a broker-dealer SIC and needs segment separation.

### MIRROR (25)

Supply- and demand-side sensors whose *revenue or backlog* is the read, not their own capex:
NVDA, AVGO, TSM, MU, INTC, ANET, COHR, LITE, DELL, HPE, SMCI-adjacent names, plus power (CEG, TLN,
VST, GEV, NRG-adjacent) and buildout contractors (VRT, ETN, PWR, FIX). **SNOW lands here**, consistent
with Task 3. **TSLA** is MIRROR-classified but carries a genuinely notable finding — see below.

### WATCH (28) — including five structural invisibilities

**Private, zero SEC-visible capex:** OpenAI, Anthropic, xAI, ByteDance, Nscale, Firmus, Together AI
(no operating-company registrant); **Crusoe** (CIK 1924674) and **Lambda** (CIK 1954966) are **Form D
only** — no periodic reports, no XBRL. `[CURL-VERIFIED]`

**ABS-only entities** — Aligned (1872843), Vantage (1727983), Stack (1765411), Cologix (1896619),
QTS-ABS (2095799): ABS-15G only, no financial statements, no XBRL, no capex.

**Deregistered:** SWCH (no filings after 2023-02-14), QTS (after 2022-02-15), CONE (2022). A roster
CIK mislabeling for QTS/CONE was caught and corrected in the verification pass.

### Task 1 failed premises

1. **BABA is unsupportable as an XBRL name.** `[CURL-VERIFIED]` Alibaba's FY2026 capex is
   US$18.275B (RMB 126,063M), +46.6% YoY — by size the largest spender missing from the seed. But the
   line is tagged `baba:PaymentsToAcquireLandUseRightsPropertyAndEquipment` — a **company extension
   element**. companyfacts exposes only `dei/us-gaap/srt/ecd/ffd`, so it returns **no capex concept
   whatsoever** for Alibaba. The order's named primitive silently returns zero for it.
   **General hazard: "absent from companyfacts" does not mean "untagged."**
2. **No FPI on the roster yields quarterly capex from SEC XBRL.** BABA — none at all. BIDU — annual;
   last quarter-length capex fact filed 2018-11-07. VNET — annual; only interim facts are nine-month.
   GDS — annual, **CNY-only with no USD unit**. TSM — annual, IFRS. BTDR — semiannual.
3. **NBIS has zero interim XBRL, ever.** All 8,748 facts sit on 20-F/20-F/A; **literally zero on any
   6-K**. Its Q1-2026 6-K does disclose capex in EX-99.1/99.2 — plain HTML, no inline XBRL.
4. **HIVE has never filed a 10-Q.** Its INCLUDE was premature; interim history exists only on legacy
   6-Ks under the non-standard `PaymentsToAcquireMachineryAndEquipment`.
5. **TSLA — the strongest argument for sub-annual cadence found anywhere in this recon.**
   `[CURL-VERIFIED]` 1H2026 capex $8.282B vs 1H2025 $3.886B = **+113.1%**. But FY2025 $8.527B vs
   FY2024 $11.342B — **the annual series reads as deceleration while the half-year cut reads as
   violent acceleration.** Tesla also carries a PP&E component literally labeled "AI infrastructure":
   $10,823M at 2026-06-30 vs $6,816M at 2025-12-31 (+58.8% in six months).

---

## TASK 4 — DEBT-EVENT SURFACE

### 4.1 The headline: there IS a structured, event-time debt surface

`[CURL-VERIFIED]` The prospectus *body* is unstructured, but the **EX-FILING FEES XBRL companion** to
a final 424B is a clean instance. Meta's
`d109954dexfilingfees_htm.xml` (13,836 bytes, namespace `http://xbrl.sec.gov/ffd/2026`) carries one
context per tranche via typed dimension `ffd:OfferingAxis` / `dei:lineNo` 1..6:

- `ffd:OfferingSctyTp = "Debt"` — a hard type discriminator, no NLP required
- `ffd:AmtSctiesRegd` = 3/2/6/4/6/4 billion — exact face principal per tranche, summing to $25,000,000,000
- `ffd:TtlOfferingAmt` = 24,967,390,000.00 · `ffd:NetFeeAmt` = 3,447,996.56 · `ffd:RegnFileNb` = 333-295425
- **`ffd:FnlPrspctsFlg = true`** — distinguishes final from preliminary

Filed 2026-05-01, one day after pricing. **This is event-time, tranche-level and machine-readable —
not quarterly and lagged.** Note the `ffd` taxonomy already appears in companyfacts for META, WULF
and SPCX, so the fee-exhibit facts are not exotic.

### 4.2 Five traps, all confirmed live

1. **Preliminary 424B2s have blank amounts** (`"$    "`, "SUBJECT TO COMPLETION"). Rated
   NOT_EXTRACTABLE. `FnlPrspctsFlg` is the discriminator.
2. **Sum-of-tranches ≠ total.** ORCL: 8 tranches sum to exactly $25,000,000,000 against
   `TtlOfferingAmt` 24,961,775,000.00 — a $38,225,000 gap. Meta: same shape, $32,610,000 gap.
3. **MSFT's recent 424B3s are exchange offers, not new money** — "The Company will not receive any
   cash proceeds from the issuance of the Registered Notes." A regex over 424B principal books
   phantom issuance. This reconciles with MSFT's explicitly-tagged $0 debt proceeds (§2.5).
4. **The 8-K item filter is unreliable.** Meta's debt 8-K (2026-05-04) was filed under
   **Items 8.01/9.01 — not 1.01 or 2.03**. The order's assumed item filter would miss it.
5. **Use-of-proceeds is boilerplate — but not universally.** MSFT and META are pure
   general-corporate-purposes. **ORCL explicitly names capital expenditures**: "used for general
   corporate purposes, which may include capital expenditures, repayment…". So event-level
   attribution of debt to AI capex is weak-to-impossible from prose, but not uniformly absent.

### 4.3 Event discovery is solved

`[CURL-VERIFIED]` `submissions/CIK##########.json` carries parallel arrays including `items`,
`isXBRL` and `isInlineXBRL`, so 424B*/FWP/8-K filtering works **without opening any document**, and
`/Archives/.../index.json` reveals whether XBRL exists before download.

---

## CORRECTIONS FROM ADVERSARIAL VERIFICATION

53 claims independently re-checked across two verifiers; **40 CONFIRMED, 6 OVERSTATED, 5 REFUTED,
2 UNVERIFIABLE**. Every cited accession and URL resolved — several byte-exact. No fabricated
identifiers were found. Material corrections, all folded in above:

- MSFT $329.1B is wrapped in **two nested `ix:nonFraction` elements**, both dimensioned; no
  finance/operating split exists (2× overstatement risk). §5.6
- **Meta's largest numbers are undimensioned** and do reach companyfacts — $278.99B purchase
  obligations and **$349.31B `ContractualObligation`**. The dimension problem is total for MSFT,
  partial for META. §5.4
- NPORT-P CUSIP does not solve the alias problem; it creates a second one. §5.8
- MSFT's 2017 fee table **does** carry an explicit Total row ($17,000,000,000) — the fee-table
  surface is *more* structured than first reported. §4.1

---

## REUSE — WHAT SMART MONEY ALREADY SOLVES

Per Mando's steer, the recon's conclusions expressed against existing code:

- **Reusable unchanged:** `abelard_common.http_client.HttpClient` (retry/backoff, 429 + `Retry-After`,
  forced UTF-8, redaction, injected logger); `smart_money.form4` `UA_TMPL` / `TICKERS_URL` /
  `PACE = 0.15` / `ticker_to_cik()` / `EDGAR_CONTACT` fail-loud.
- **Pattern to extend:** `marketcap.py`'s concept-fallback list is structurally the per-issuer tag map
  §2.2/§2.5 proves mandatory — but it resolves against one global order, whereas capex needs
  per-issuer *and* per-era resolution (AMZN's 2017 migration, WULF's seven tags).
- **Doctrine that already governs this:** the SM-P2 **G1 unit-scale-or-fail-loud** gate
  (`queries.py:816`) — explicit `"undetermined"` basis, `_magnitude_warning` bounds reported never
  silently corrected. Extends to inline-XBRL `scale="9"` (§5.6) and needs a **currency** arm for
  NBIS RUB/USD and GDS CNY-only (§2.3, Task 1).
- **Precedent for the dimension problem:** `marketcap.py` already resolves multi-class share counts to
  UNBANDABLE for the same root cause — dimensioned facts absent from the API.
- **The one genuinely new primitive:** inline-XBRL instance parsing (`contextRef`, `explicitMember`,
  `nonFraction`, `FilingSummary.xml`). Nothing in the repo reads any of it; Smart Money parses flat
  schemas only. This recon says it is not optional.
- **Format:** `recon/SOURCE_VERDICTS.md` is the precedent this report follows.

---

## PREMISES-VERDICT TABLE

| # | Design premise | Verdict | Evidence |
|---|---|---|---|
| 1 | Announced-vs-reported split requires a separate guidance-capture leg | **PARTIAL** | A structured forward-commitment series already exists: META purchase obligations $34.1B→$279.0B in six quarters, plus `ContractualObligation` $349.31B, both undimensioned and in the API. MSFT's equivalents ($329.1B leases, $194.06B commitments) need filing-level parsing. Neither required an earnings-call feed. §2.6, §5.5, §5.6, §5.7 |
| 2 | TTM normalization to calendar quarters is feasible | **HOLDS** for domestic 10-Q filers; **FAILS** for FPIs and new registrants | 8 of 10 probe issuers yield 12 consecutive discrete quarters, zero gaps. NBIS and SPCX yield **zero**. No FPI on the 94-name roster yields quarterly capex from XBRL. Off-grid filers (ORCL Nov/Feb/May; SNOW Feb–Apr mislabeled `CY2026Q1`) need explicit handling. §2.7, §2.8, Task 1 |
| 3 | Debt events are mechanically extractable | **HOLDS**, via a surface the order did not name | Not the prospectus body (unstructured) but the **EX-FILING FEES XBRL** exhibit: tranche-level principal, `OfferingSctyTp="Debt"`, `FnlPrspctsFlg`, at event time. Five traps confirmed (exchange offers, preliminary blanks, tranche-sum gaps, 8-K item mislabeling, boilerplate proceeds). §4.1, §4.2 |
| 4 | Off-balance-sheet exposure is invisible; XBRL is structurally insufficient | **PARTIAL — right conclusion, wrong mechanism** | The disclosure is complete and quantified ($46.03B VIE exposure, $28B RVG, E&Y Critical Audit Matter). The blindness is at the **API layer**, via three independent exclusions: dimensioned facts, custom-namespace tags (`meta_`, `baba_`), and un-ingested filings. All recoverable by parsing the filing instance. §5.3, §5.4, §5.8 |
| 5 | Universe completeness | **PARTIAL** | 94 candidates classified, all seeds survive, three new INCLUDEs (KEEL, WYFI, FRMI). But a structural hole is confirmed and permanent: OpenAI, Anthropic, xAI, ByteDance, Crusoe, Lambda, Nscale, Firmus, Together have **no periodic-reporting presence**; Beignet Investor LLC has **no CIK at all**. Frames API is a magnitude cross-check, not a universe screen. Task 1, §5.8 |

**Bonus premise, untested by the order but decisive:** *annual data is sufficient.* **FAILS.**
Tesla's annual capex series reads as deceleration (FY2025 $8.5B vs FY2024 $11.3B) while its half-year
cut reads +113.1%. A daemon on annual cadence would have called the phase backwards.

---

## RATIFICATION — MANDO'S RULINGS, 2026-08-07

Tiering ruled: CORE (full quarterly panel) / MIRROR (demand-side sensors) / ANNUAL-DEGRADED (FPIs).
BABA **IN** at the degraded tier — excluding the largest non-US spender biases the aggregate, and the
aggregate is a headline output. NBIS **stays ruled-in** at annual tier. `ai_capex_financing` ships v1
at **reduced scope** (SPV/private-credit naming events only); NPORT-P moves to the horizon list as a
separate probe.

### Roster review — orchestrator-verified, not taken on the sweep's word

Mando flagged KEEL/WYFI/FRMI as unrecognized and likely post-cutoff. All three verified live.
**All three are real, CIK-matched registrants — and all three are post-cutoff renames or listings,
which is exactly why they were unfamiliar.**

| ticker | CIK | identity `[CURL-VERIFIED]` |
|---|---|---|
| **KEEL** | 1812477 | **= Bitfarms Ltd**, renamed 2026-03-31 (`formerNames`). 10-K: "converting existing Infrastructure Assets from Bitcoin Mining to HPC data centers"; 264 occurrences of "data center". **The WULF/Hunt-B archetype.** |
| **WYFI** | 2042022 | WhiteFiber, Inc. (ex-White Fiber). IPO 2025-08-08. Latest quarterly capex **$169,168,417**; CIP $294.3M; PP&E net $432.0M. |
| **FRMI** | 2071778 | Fermi Inc. (ex-Fermi LLC). IPO 2025-10-01. Latest quarterly capex **$441,188,000**; PP&E gross $1.43B. Contested proxy in progress (47 DFAN14A, 3 PRRN14A). |

**A ruling I nearly got wrong.** My first pass read KEEL as a finance company — SIC 6199, mortgage
servicing rights, derivative payments — and I was about to downgrade it to WATCH. Reading its 10-K
reversed that. **Mando's instruction to verify rather than accept was load-bearing.**

**Consequent finding: SIC screening is useless for this universe.** SIC 6199 "Finance Services" is
assigned to **KEEL, WYFI, IREN, HUT, CORZ and CIFR** — six datacenter/AI-infrastructure operators.
GLXY is 6211 (broker-dealer), FRMI is 6798 (REIT), APLD is 7374. The order's suggested SIC screen
would have missed most of the neocloud tier.

**Rename hazard is live, not historical:** KEEL←Bitfarms (2026-03-31), CIFR←Cipher Mining (2026-02-23,
confirming the order's suspicion about "Cipher Digital"), IREN←Iris Energy (2024-11-29),
FRMI←Fermi LLC, WYFI←White Fiber. APLD shows a `formerNames` boundary dated **2026-08-06 — one day
before this recon** — while its `name` field still reads "Applied Digital Corp." Entity identity must
be resolved by CIK and re-checked, never cached by ticker or name.

### Ratified tiers — assigned on measured coverage, not on sweep claims

Method: resolve the capex tag **by recency** per issuer (see hazard note below), derive discrete
quarters, then count *consecutive* quarters ending at the most recent observation.

**CORE — 13 names** (≥12 consecutive quarters, current through 2026Q1/Q2):

| ticker | consec Q | latest | TTM capex |
|---|---|---|---|
| MSFT | 72 | 2026Q2 | $115.95B |
| ORCL | 64 | 2026Q2 | $55.66B |
| DLR | 63 | 2026Q1 | $3.26B |
| META | 59 | 2026Q1 | $75.75B |
| EQIX | 50 | 2026Q2 | $5.41B |
| GOOGL | 47 | 2026Q2 | $132.40B |
| AMZN | 36 | 2026Q2 | $173.03B |
| RIOT | 23 | 2026Q1 | $0.28B |
| CORZ | 22 | 2026Q2 | $1.48B |
| WULF | 21 | 2026Q2 | $2.23B |
| CIFR | 19 | 2026Q2 | $1.23B |
| HUT | 16 | 2026Q2 | $0.71B |
| APLD | 16 | 2026Q2 | $2.87B |

**Amendment to the ruling:** CORE is **13, not 8**. The "8 clean domestic filers" figure came from my
probe set of 10; the twelve un-audited INCLUDEs had never been tested. Nine of them qualify.

**DEGRADED-SHORT — 4** (4–11 consecutive quarters; recent listings, will graduate to CORE):
CRWV (9 Q, $16.60B TTM — flagship neocloud, short only because it listed in 2025), IREN (7 Q, $1.85B —
*also* June FYE and YTD-tagged, so doubly degraded), GLXY (7 Q, $1.44B), WYFI (7 Q, $0.39B).

**THIN — 3** (<4 quarters): FRMI (2), KEEL (1), SPCX (0 until the Q3 10-Q, ~Nov 2026).

**Recommended amendment — a fourth bucket.** THIN and ANNUAL-DEGRADED should not share a tier. An FPI
is *permanently* annual; a THIN name is a **maturation queue** that becomes CORE on a known schedule
(SPCX ~Nov 2026, FRMI/KEEL through 2026–27). Collapsing them would either hold new names back or
imply FPIs will improve. Their zero TTM figures are an artifact of <4 quarters, **not** zero capex —
FRMI's latest quarter alone is $441.2M.

**ANNUAL-DEGRADED (FPI):** NBIS, BABA, plus BIDU / VNET / GDS / TSM / BTDR if Mando wants breadth.
**MIRROR:** SNOW plus the 25 supply- and demand-side names.

### Hazard demonstrated on live data during ratification

The audit's first pass resolved capex tags by a fixed global preference order and silently returned
**stale series** for three names: AMZN ($7.42B TTM from a tag abandoned in 2017 — true value
$173.03B), EQIX ($1.74B from a tag ending 2018 — true value $5.41B), and APLD ($0.17B — true value
$2.87B). No error, no gap, no null; just a plausible wrong number, 23× low in Amazon's case.
**Resolution must be by recency per issuer, and the resolved tag must be recorded with the series.**
This is §2.2/§2.5's finding reproduced accidentally, which is the strongest evidence for it.

### Open items carried forward

1. **NPORT-P probe** — horizon list, separate small recon. Fund-side monitoring of unfiled SPV debt
   is a novel surface; name-keyed (2,559 hits) not CUSIP-keyed (774), per §5.8.
2. **NBIS ↔ guidance-leg symmetry** (Mando's observation, recorded): the phase-2 prose-magnitude
   extraction that reads announced figures is the same machinery that would recover NBIS quarterlies
   from 6-K EX-99.1 HTML. One build serves both.
3. **APLD rename in flight** as of 2026-08-06 — re-resolve before any ingest.
