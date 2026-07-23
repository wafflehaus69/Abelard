# PHASE4_OVERLAP — smart_money_daemon SM-A1 Phase 4

Generated 2026-07-23. Raw counts and row-level backing only. NO composite score, NO ranking, NO verdict — those are Mando's.

## Method + data (as-of)

- **13F holdings**: 694 rows, periods 2024-03-31..2026-03-31 (as-of = filing period end; STALE ~45d by construction). Confirmed filer set only.
- **Form 4 corpus**: 22026 rows, tx 2023-02-22..2026-07-20. Backfilled issuer set (overlay + registry + trump_network), 36-month depth.
- **Congress**: 35548 stock rows, tx 2012-09-13..3031-04-30.
- Join key = uppercased ticker across surfaces (13F OpenFIGI / Form 4 issuer symbol / congress normalized). Cross-source symbol mismatch is a coverage limit — see gaps.

## SMID banding — BLOCKED-ON-METHOD

Market-cap method is Mando's decision (recon candidate: SEC companyfacts keyless). Not chosen at run time, so **(a) and (d) are reported FULL-UNIVERSE ONLY** and the SMID cut is marked blocked. No market-cap proxy is substituted. Proposed bands (pending): micro <$300M, small $300M-$2B, mid $2B-$10B.

## Per-principal 13F holdings summary

| cik | quarters | holding_rows | distinct_tickers | latest_period |
|---|---|---|---|---|
| 1536411 | 8 | 535 | 191 | 2026-03-31 |
| 2045724 | 6 | 126 | 45 | 2026-03-31 |
| 1562087 | 4 | 14 | 6 | 2025-09-30 |
| 1846021 | 8 | 12 | 3 | 2025-12-31 |
| 2059583 | 6 | 6 | 1 | 2026-03-31 |
| 2106825 | 1 | 1 | 1 | 2025-12-31 |

## (a) Multi-principal convergence — 2+ tracked 13F filers, same period (full-universe)

19 (ticker, period) pairs held by >=2 confirmed filers.

| ticker | period | n_filers | filer_ciks | overlay |
|---|---|---|---|---|
| AMZN | 2024-12-31 | 2 | 1536411,1562087 |  |
| AVGO | 2025-06-30 | 2 | 1536411,2045724 |  |
| BE | 2025-12-31 | 2 | 1536411,2045724 |  |
| BE | 2026-03-31 | 2 | 1536411,2045724 |  |
| EQT | 2025-03-31 | 2 | 1536411,2045724 |  |
| EQT | 2025-06-30 | 2 | 1536411,2045724 |  |
| EQT | 2025-09-30 | 2 | 1536411,2045724 |  |
| INTC | 2026-03-31 | 2 | 1536411,2045724 |  |
| MU | 2026-03-31 | 2 | 1536411,2045724 |  |
| SEI | 2025-09-30 | 2 | 1536411,2045724 |  |
| SNDK | 2025-09-30 | 2 | 1536411,2045724 |  |
| SNDK | 2026-03-31 | 2 | 1536411,2045724 |  |
| TSLA | 2024-12-31 | 2 | 1536411,1562087 | conviction |
| TSLA | 2025-03-31 | 2 | 1536411,1562087 | conviction |
| TSM | 2026-03-31 | 2 | 1536411,2045724 |  |
| VST | 2024-12-31 | 2 | 1536411,2045724 |  |
| VST | 2025-03-31 | 2 | 1562087,2045724 |  |
| VST | 2025-06-30 | 2 | 1562087,2045724 |  |
| VST | 2025-09-30 | 2 | 1536411,2045724 |  |

## (b) Institutional x insider — 13F holding + discretionary open-market Form 4 buy

Excludes 10b5-1 planned transactions (plan_flag=0 only). The highest-interest join — pairs a position with a decision.

### 90d window — 1 tickers
| ticker | insider_buys | distinct_buyers | n_13f_filers | buyers | overlay |
|---|---|---|---|---|---|
| ABCL | 1 | 1 | 1 | Montalbano John S. |  |

### 180d window — 1 tickers
| ticker | insider_buys | distinct_buyers | n_13f_filers | buyers | overlay |
|---|---|---|---|---|---|
| ABCL | 4 | 3 | 1 | Montalbano John S.,Thermopylae Holdings Ltd.,Booth Andrew |  |

## (c) Institutional x congressional — 13F holding intersects a congressional disclosure

139 tickers held by a confirmed 13F filer AND traded by Congress.

| ticker | congress_members | congress_buys | n_13f_filers | overlay |
|---|---|---|---|---|
| AAPL | 101 | 273 | 2 |  |
| MSFT | 101 | 243 | 2 |  |
| AMZN | 85 | 211 | 2 |  |
| GOOGL | 60 | 83 | 1 |  |
| NVDA | 58 | 234 | 3 |  |
| INTC | 46 | 75 | 2 |  |
| SBUX | 44 | 73 | 1 |  |
| TMO | 38 | 46 | 1 |  |
| LLY | 37 | 64 | 1 |  |
| META | 37 | 82 | 1 | conviction |
| AVGO | 36 | 70 | 2 |  |
| BAC | 35 | 76 | 1 |  |
| TSLA | 34 | 103 | 2 | conviction |
| DHR | 33 | 35 | 1 |  |
| GS | 32 | 63 | 1 |  |
| WFC | 31 | 63 | 1 |  |
| ADBE | 27 | 46 | 1 |  |
| TSM | 25 | 46 | 2 |  |
| USB | 25 | 25 | 1 |  |
| AMD | 22 | 74 | 1 |  |
| KMI | 21 | 25 | 1 |  |
| PANW | 21 | 33 | 1 |  |
| MU | 19 | 30 | 2 |  |
| TFC | 17 | 6 | 1 |  |
| DAL | 16 | 24 | 1 |  |
| WBD | 16 | 6 | 1 |  |
| COF | 14 | 27 | 1 |  |
| CMG | 13 | 12 | 1 |  |
| FCX | 13 | 17 | 1 |  |
| PLTR | 13 | 30 | 1 |  |
| MRVL | 12 | 17 | 1 |  |
| SYF | 11 | 13 | 1 |  |
| WAB | 11 | 11 | 1 |  |
| KEY | 10 | 11 | 1 |  |
| AAL | 9 | 27 | 1 |  |
| ADSK | 9 | 8 | 1 |  |
| BAH | 9 | 9 | 1 |  |
| GLW | 9 | 40 | 2 |  |
| ILMN | 9 | 11 | 1 |  |
| IQV | 9 | 12 | 1 |  |
| CFG | 8 | 13 | 1 |  |
| COHR | 8 | 9 | 2 |  |
| DASH | 8 | 29 | 1 |  |
| GEV | 8 | 6 | 1 |  |
| HAS | 8 | 9 | 1 |  |
| DHI | 7 | 17 | 1 |  |
| HBAN | 7 | 4 | 1 |  |
| VST | 7 | 8 | 3 |  |
| WDC | 7 | 8 | 1 |  |
| CEG | 6 | 6 | 1 |  |
| HUM | 6 | 9 | 1 |  |
| LEN | 6 | 5 | 1 |  |
| MTB | 6 | 7 | 1 |  |
| SE | 6 | 7 | 1 |  |
| UAL | 6 | 9 | 1 |  |
| VRT | 6 | 10 | 1 |  |
| Z | 6 | 6 | 1 |  |
| APP | 5 | 13 | 1 |  |
| BWXT | 5 | 26 | 1 |  |
| CLF | 5 | 22 | 1 |  |

## (d) New positions — QoQ adds / exits / material size changes (full-universe)

Adds, exits, and >=2x size changes reported SEPARATELY.

### Adds — 207
| cik | period | ticker | value |
|---|---|---|---|
| 1536411 | 2024-09-30 | TFC | 9987 |
| 1536411 | 2024-09-30 | ABCB | 9608 |
| 1536411 | 2024-09-30 | HBAN | 9867 |
| 1536411 | 2024-09-30 | SBUX | 5099 |
| 1536411 | 2024-09-30 | USB | 9942 |
| 1536411 | 2024-09-30 | FT2 | 9624 |
| 1536411 | 2024-09-30 | RARE | 4746 |
| 1536411 | 2024-09-30 | TSM | 9961 |
| 1536411 | 2024-09-30 | VRNA | 19738 |
| 1536411 | 2024-09-30 | PCVX | 4910 |
| 1536411 | 2024-09-30 | TRP | 5485 |
| 1536411 | 2024-09-30 | CNM | 21099 |
| 1536411 | 2024-09-30 | FCNCA | 9542 |
| 1536411 | 2024-09-30 | ADSK | 10027 |
| 1536411 | 2024-09-30 | KRE | 116218 |
| 1536411 | 2024-09-30 | CFG | 9799 |
| 1536411 | 2024-09-30 | ARM | 3090 |
| 1536411 | 2024-09-30 | AVGO | 41397 |
| 1536411 | 2024-09-30 | TRVC | 20495 |
| 1536411 | 2024-09-30 | SPRY | 7785 |
| 1536411 | 2024-09-30 | TEVA | 25732 |
| 1536411 | 2024-09-30 | WDC | 4876 |
| 1536411 | 2024-09-30 | USX1 | 23427 |
| 1536411 | 2024-09-30 | XPO | 4999 |
| 1536411 | 2024-09-30 | KEY | 9780 |
| 1536411 | 2024-09-30 | MTB | 10010 |
| 1536411 | 2024-12-31 | BWXT | 33325 |
| 1536411 | 2024-12-31 | LLY | 48011 |
| 1536411 | 2024-12-31 | WBD | 49231 |
| 1536411 | 2024-12-31 | LYV | 10313 |
| 1536411 | 2024-12-31 | WFC | 11035 |
| 1536411 | 2024-12-31 | AAL | 16034 |
| 1536411 | 2024-12-31 | TSLA | 15215 |
| 1536411 | 2024-12-31 | PCT | 16323 |
| 1536411 | 2024-12-31 | SNREN | 5736 |
| 1536411 | 2024-12-31 | DAL | 49473 |
| 1536411 | 2024-12-31 | GOOGL | 14516 |
| 1536411 | 2024-12-31 | UAL | 101353 |
| 1536411 | 2024-12-31 | IQV | 4225 |
| 1536411 | 2024-12-31 | MIR | 5980 |
| 1536411 | 2024-12-31 | AMZN | 72048 |
| 1536411 | 2024-12-31 | MU | 34412 |
| 1536411 | 2024-12-31 | SKAA | 72272 |
| 1536411 | 2024-12-31 | BN | 35057 |
| 1536411 | 2024-12-31 | SLM | 69509 |
| 1536411 | 2024-12-31 | ELF | 4683 |
| 1536411 | 2025-03-31 | EXE | 10876 |
| 1536411 | 2025-03-31 | CCC | 50523 |
| 1536411 | 2025-03-31 | COF | 35442 |
| 1536411 | 2025-03-31 | AR | 10450 |

(showing 50 of 207)

### Exits — 190
| cik | period | ticker | was_value |
|---|---|---|---|
| 1536411 | 2024-09-30 | AAPL | 5139 |
| 1536411 | 2024-09-30 | MSGE | 23763 |
| 1536411 | 2024-09-30 | OPCH | 51849 |
| 1536411 | 2024-09-30 | VCYT | 5633 |
| 1536411 | 2024-09-30 | AES | 5530 |
| 1536411 | 2024-09-30 | CNK | 8665 |
| 1536411 | 2024-09-30 | IQV | 12763 |
| 1536411 | 2024-09-30 | EQT | 4475 |
| 1536411 | 2024-09-30 | LYV | 9665 |
| 1536411 | 2024-09-30 | BAH | 12903 |
| 1536411 | 2024-09-30 | GPCR | 1480 |
| 1536411 | 2024-09-30 | TEO | 1034 |
| 1536411 | 2024-09-30 | NWSA | 18852 |
| 1536411 | 2024-09-30 | SPHR | 4292 |
| 1536411 | 2024-09-30 | TPD | 4881 |
| 1536411 | 2024-09-30 | NVDA | 26445 |
| 1536411 | 2024-09-30 | SE | 4778 |
| 1536411 | 2024-09-30 | BLDR | 4067 |
| 1536411 | 2024-09-30 | NWS | 18052 |
| 1536411 | 2024-09-30 | ANETEUR | 18321 |
| 1536411 | 2024-12-31 | SBUX | 5099 |
| 1536411 | 2024-12-31 | CPT | 35937 |
| 1536411 | 2024-12-31 | NRIX | 993 |
| 1536411 | 2024-12-31 | TRP | 5485 |
| 1536411 | 2024-12-31 | SLN | 7043 |
| 1536411 | 2024-12-31 | CNM | 21099 |
| 1536411 | 2024-12-31 | MSFT | 18544 |
| 1536411 | 2024-12-31 | ADSK | 10027 |
| 1536411 | 2024-12-31 | BCYC | 4304 |
| 1536411 | 2024-12-31 | CFG | 9799 |
| 1536411 | 2024-12-31 | AVGO | 41397 |
| 1536411 | 2024-12-31 | SPRY | 7785 |
| 1536411 | 2024-12-31 | ADBE | 17861 |
| 1536411 | 2024-12-31 | MAA | 37933 |
| 1536411 | 2024-12-31 | XPO | 4999 |
| 1536411 | 2024-12-31 | MTB | 10010 |
| 1536411 | 2025-03-31 | TFC | 10129 |
| 1536411 | 2025-03-31 | BWXT | 33325 |
| 1536411 | 2025-03-31 | HBAN | 10920 |
| 1536411 | 2025-03-31 | USB | 24087 |
| 1536411 | 2025-03-31 | TLSI | 2139 |
| 1536411 | 2025-03-31 | WBD | 49231 |
| 1536411 | 2025-03-31 | LYV | 10313 |
| 1536411 | 2025-03-31 | WFC | 11035 |
| 1536411 | 2025-03-31 | PCVX | 7062 |
| 1536411 | 2025-03-31 | GEV | 32811 |
| 1536411 | 2025-03-31 | SNREN | 5736 |
| 1536411 | 2025-03-31 | TRVC | 5114 |
| 1536411 | 2025-03-31 | WDC | 24875 |
| 1536411 | 2025-03-31 | PLTR | 3155 |

(showing 50 of 190)

### Size changes (>=2x) — 97
| cik | period | ticker | from | to | dir |
|---|---|---|---|---|---|
| 1536411 | 2024-09-30 | KMI | 134185 | 57699 | down_2x |
| 1536411 | 2024-09-30 | VST | 225717 | 46391 | down_2x |
| 1536411 | 2024-09-30 | GEV | 50950 | 7458 | down_2x |
| 1536411 | 2024-09-30 | MSFT | 178999 | 18544 | down_2x |
| 1536411 | 2024-09-30 | PANW | 30161 | 14941 | down_2x |
| 1536411 | 2024-09-30 | GTM | 75112 | 22092 | down_2x |
| 1536411 | 2024-09-30 | MAA | 91868 | 37933 | down_2x |
| 1536411 | 2024-09-30 | PLTR | 19503 | 1552 | down_2x |
| 1536411 | 2024-09-30 | NTRA | 213860 | 452812 | up_2x |
| 1536411 | 2024-12-31 | USB | 9942 | 24087 | up_2x |
| 1536411 | 2024-12-31 | ARGT | 18697 | 45369 | up_2x |
| 1536411 | 2024-12-31 | TSM | 9961 | 21233 | up_2x |
| 1536411 | 2024-12-31 | VRNA | 19738 | 41219 | up_2x |
| 1536411 | 2024-12-31 | IVVD | 583 | 253 | down_2x |
| 1536411 | 2024-12-31 | GEV | 7458 | 32811 | up_2x |
| 1536411 | 2024-12-31 | IM8N | 1460 | 40973 | up_2x |
| 1536411 | 2024-12-31 | YPF | 9122 | 71745 | up_2x |
| 1536411 | 2024-12-31 | TECK/B | 53133 | 17491 | down_2x |
| 1536411 | 2024-12-31 | KRE | 116218 | 33442 | down_2x |
| 1536411 | 2024-12-31 | ARM | 3090 | 22007 | up_2x |
| 1536411 | 2024-12-31 | TRVC | 20495 | 5114 | down_2x |
| 1536411 | 2024-12-31 | TEVA | 25732 | 198303 | up_2x |
| 1536411 | 2024-12-31 | WDC | 4876 | 24875 | up_2x |
| 1536411 | 2024-12-31 | FCX | 68737 | 28899 | down_2x |
| 1536411 | 2024-12-31 | USX1 | 23427 | 54725 | up_2x |
| 1536411 | 2024-12-31 | CRNX | 15790 | 6274 | down_2x |
| 1536411 | 2024-12-31 | PLTR | 1552 | 3155 | up_2x |
| 1536411 | 2025-03-31 | UAL | 101353 | 25458 | down_2x |
| 1536411 | 2025-03-31 | TSM | 21233 | 99397 | up_2x |
| 1536411 | 2025-03-31 | AMZN | 72048 | 26021 | down_2x |
| 1536411 | 2025-03-31 | AAL | 16034 | 1411 | down_2x |
| 1536411 | 2025-03-31 | TSLA | 15215 | 4882 | down_2x |
| 1536411 | 2025-03-31 | IM8N | 40973 | 104445 | up_2x |
| 1536411 | 2025-03-31 | BN | 35057 | 16551 | down_2x |
| 1536411 | 2025-03-31 | DAL | 49473 | 15048 | down_2x |
| 1536411 | 2025-06-30 | BMA | 32606 | 15509 | down_2x |
| 1536411 | 2025-06-30 | CCC | 50523 | 9485 | down_2x |
| 1536411 | 2025-06-30 | APP | 10652 | 31317 | up_2x |
| 1536411 | 2025-06-30 | IM8N | 104445 | 226786 | up_2x |
| 1536411 | 2025-06-30 | ROKU | 34769 | 96723 | up_2x |
| 1536411 | 2025-06-30 | YPF | 72722 | 22141 | down_2x |
| 1536411 | 2025-06-30 | DAL | 15048 | 39025 | up_2x |
| 1536411 | 2025-06-30 | BCS | 58634 | 28814 | down_2x |
| 1536411 | 2025-06-30 | FCX | 38545 | 9745 | down_2x |
| 1536411 | 2025-09-30 | LEN | 9944 | 35219 | up_2x |
| 1536411 | 2025-09-30 | BAC | 16179 | 51035 | up_2x |
| 1536411 | 2025-09-30 | DHI | 9978 | 34767 | up_2x |
| 1536411 | 2025-09-30 | TWLO | 24931 | 51326 | up_2x |
| 1536411 | 2025-09-30 | YPF | 22141 | 2786 | down_2x |
| 1536411 | 2025-09-30 | CHYM | 16110 | 872 | down_2x |

(showing 50 of 97)

## (e) Mando-book intersection (read-only)

- Tickers surfaced in (a)-(d) that are in conviction_book: CRWV META TSLA
- ... in watchlist: none

## (f) Named-case sanity check — WULF / XE / CCXI

- **WULF**: PRESENT — 13F: filers ['1536411'] first 2024-06-30 DUQUESNE first-period 2024-06-30
- **XE**: NOT PRESENT on any surface in any period. NEGATIVE COVERAGE FINDING — the name is absent from the assembled dataset (13F confirmed-filer set + backfilled Form 4 issuers + congress). Absence here reflects ingest scope, not market reality.
- **CCXI**: PRESENT — Congress: 4 rows first 2022-03-15

## Coverage gaps (MANDATORY)

- 13F holdings with UNMAPPED cusip (no ticker, excluded from ticker joins, never dropped): 76 rows; CUSIP map failure 24/252 (9.5%).
- 13F filer set is the 6 Mando-confirmed CIKs ONLY — not all managers.
- Form 4 corpus covers ONLY backfilled issuers (overlay + registry + trump_network); an insider buy on any other issuer is invisible here.
- Cross-surface ticker mismatch (renames, foreign/OTC suffixes, share classes) can hide a real overlap. Joins are by ticker string, not CUSIP/CIK.
- SMID banding BLOCKED-ON-METHOD (above).

## Standing warnings (verbatim)

- 13F is stale by construction — roughly a 45-day filing lag; a holding shown here may already be closed.
- 13F is a PARTIAL view — it omits shorts, most derivatives, non-US listings, cash, and private positions.
- Confidential treatment is granted for some positions — absence from a 13F is NOT evidence of absence of a position.
- Survivorship governs this whole exercise — convergence is a funnel-narrowing PRIOR, NOT a demonstrated edge, and NOT a sizing input.
- Thiel power-law mismatch — copying selection without the sizing and holding period reproduces the losses and discards the compensating mechanism.
- Compliance — everything here is public filings analyzed behind a standard information wall. No recommendations, rankings, or verdicts are made.

