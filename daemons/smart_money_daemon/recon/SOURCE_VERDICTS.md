# SOURCE_VERDICTS — Phase 0 recon, ORDER SM-0/1

Recon date: 2026-07-17. Host: Orban WSL. All raw responses cached in `data/raw/recon/`.
Doctrine: live curl = canonical. Anything not curl-verified is marked INFERENCE.

---

## G1 — EDGAR Form 4 direct (keyless)

**Verdict: CERTIFIED**

- Evidence: `GET /Archives/edgar/daily-index/2026/QTR3/form.20260716.idx` → HTTP 200,
  981 Form 4 rows for 2026-07-16. Declared User-Agent
  `Abelard-SmartMoney mdiba personal research <EDGAR_CONTACT>` on every call, 0.15s sleep floor honored.
  Raw: `g1_form.20260716.idx`, `g1_formidx_headers.txt`.
- Complete filing fetched and parsed: AGCO CORP accession 0001452301-26-000012,
  doc `wk-form4_1784234582.xml` → HTTP 200. Raw: `g1_form4.xml`.
- Parse completeness — ALL required fields present and reliable:
  - reporting owner: `rptOwnerName` Audia Damon J, `rptOwnerCik` 0001452301, officer flag + title
  - issuer: CIK 0000880266, name, `issuerTradingSymbol` AGCO
  - transaction: code A, date 2026-07-14, shares 145.559, price 107.73, acquiredDisposed A
  - ownership after: `sharesOwnedFollowingTransaction` 50475.559 with `footnoteId F1` flag
  - 10b5-1 checkbox: `aff10b5One` element present (value 0 here)
- Notes: filing index.json per accession lists the ownership XML by name; XML schema X0609.
  No rate-limit headers observed in response; SEC hard cap 10 req/s is policy, not header-enforced.
  **Primary insider source. No paid substitute needed.**

## G2 — EDGAR 13F direct (keyless)

**Verdict: CERTIFIED — putCall capture PROVEN**

- Evidence: `data.sec.gov/submissions/CIK0002045724.json` → HTTP 200, name
  "Situational Awareness LP", six 13F-HR filings, latest 2026-05-18 accession 0002045724-26-000008.
  Info table `salp13fq1xml.xml` → HTTP 200. Raw: `g2_submissions.json`, `g2_infotable.xml`.
- Parse: 42 infoTable rows. putCall distribution: `{Put: 11, Call: 5, (none): 26}` — column
  present with real values.
- Net-directionality demo (parser: `recon/g2_parse_13f.py`, run against cache, deterministic):
  e.g. NVIDIA net_opt −$1.568B (pure put), COREWEAVE long $556M + call $141M,
  SANDISK long $724M + call $389M, VANECK ETF (SMH) put −$2.04B.

## G3 — Form4API (form4api.com) [was UNRESOLVED, URL supplied by Mando 2026-07-17]

**Verdict: DROP (key-gated free tier, redundant to G1)**

- Evidence: homepage HTTP 200 (`g3_home.html`), docs scraped (`g3_docs.html`).
  Endpoints: /v1/filings, /v1/insiders/{cik}/transactions, /v1/insiders/leaderboard,
  /v1/signals, /v1/sentiment, /v1/form144. Free tier exists (marketing claims generous
  daily limit, no credit card) but every call requires `X-Api-Key: fapi_live_...` from
  account signup.
- Keyless probe: `GET api.form4api.com/v1/filings/recent` → **HTTP 401**
  `MISSING_API_KEY` (`g3_nokey_body.json`). Not retried.
- Marginal value vs G1: parsing convenience, name resolution, derived scorecards/sentiment.
  None of it is data we cannot compute from certified G1. Account creation is Mando's call;
  cost-first doctrine says EDGAR-direct is primary. Revisit only if EDGAR parse volume
  becomes an engineering burden.

## G4 — Finnhub free key (key from .env, never logged)

**Verdicts (status codes verbatim):**

| Endpoint | HTTP | Verdict |
|---|---|---|
| a) /stock/insider-transactions?symbol=CRWV | 200 | **CERTIFIED** |
| b) /stock/congressional-trading?symbol=NVDA | 403 | **PAYWALLED** (hypothesis resolved: not free) |
| c) /stock/candle SPY daily | 403 | **PAYWALLED** (hypothesis resolved: restricted) |

- G4a sample (raw `g4a_body.json`): Intrator Michael N, CRWV, code S, 2026-07-08,
  price 89.8808, share/change/filingDate fields present, `source: sec`. Useful as a
  cross-check or convenience feed beside G1, not a replacement.
- G4b/G4c bodies: `{"error":"You don't have access to this resource."}` — no retry loops.
- G4c PAYWALLED → G6 price-fallback gate activated.

## G5 — Senate Stock Watcher (congressional PRIMARY candidate)

**Verdict: DEAD**

- Evidence: bulk S3 `aggregate/all_transactions.json` → **HTTP 403 AccessDenied**
  (`g5_all_transactions.json` contains the S3 error XML). senatestockwatcher.com →
  connection failure. Backing GitHub repo `timothycarambat/senate-stock-watcher-data`
  last pushed **2021-03-16**.
- Order stop-condition triggered — BUT recon found a live free replacement path
  (official primary sources, both curl-proven today):

### G5-alt-A — Senate eFD direct (efdsearch.senate.gov, keyless)
- CSRF handshake + agreement POST + DataTables endpoint `/search/report/data/` →
  **HTTP 200 JSON**. 19 Senate PTRs filed since 2026-06-01, freshest 2026-07-16
  (Tuberville). Raw: `g5_efd_data.json`.
- PTR detail page (electronic filings) is a clean HTML table — parsed live:
  `['06/09/2026','Self','WAB','Westinghouse...','Stock','Sale (Full)','$1,001 - $15,000']`.
  Raw: `g5_efd_ptr_tuberville.html`.
- Caveats: history requires paginated enumeration (2012→now); paper-scan filings
  are unparseable and must be excluded-and-counted; session/CSRF adds fragility.

### G5-alt-B — House Clerk FD bulk (disclosures-clerk.house.gov, keyless)
- `financial-pdfs/2026FD.zip` → HTTP 200. Index XML: 1387 filings, **303 PTRs in 2026,
  latest filed 2026-07-13** — fresh. Yearly zips exist per year (INFERENCE for pre-2026,
  same URL pattern, not yet probed). Raw: `g5_2026FD.zip`, `2026FD.xml`.
- Per-DocID PTR PDF fetched (Yakym #20034984) → HTTP 200, digital PDF with full text
  layer: asset, type code, transaction type P, tx date, notification date, amount range.
  Parseable; scanned/handwritten PTRs exist and must be excluded-and-counted.
  **This closes the House gap the order assumed** — free path now covers BOTH chambers,
  at the cost of real parsing engineering (PDF tables + HTML scrape vs one bulk JSON).

## G6 — Price series fallback (activated by G4c PAYWALLED)

**Stooq verdict: DEGRADED-BLOCKED.** `stooq.com/q/d/l/?s=spy.us&i=d` → HTTP 200 but body
is a JavaScript SHA-256 proof-of-work browser-verification page, all 6 tickers probed
(`g6_*.csv` contain the challenge HTML). This is bot-detection; we do not automate around
it. Not usable as a curl source.

**Replacement — Yahoo Finance v8 chart endpoint: CERTIFIED-UNOFFICIAL (designated price source)**
- `query1.finance.yahoo.com/v8/finance/chart/{T}?period1=...&period2=...&interval=1d`
  with browser UA → HTTP 200 JSON for SPY, WAB, PCYO (small-cap): 2649 rows,
  **2016-01-04 → 2026-07-17**, adjclose present. Raw: `g6_yahoo_*.json`.
- Caveat: unofficial endpoint, no ToS-blessed API, can change without notice — wrap in
  fail-loud client, cache aggressively. abelard_common hoist candidate (Price Daemon
  convergence); implement daemon-local for now per order.

---

## AUX — additional-source sweep (Mando request 2026-07-17, cost-first)

Not gates; recorded for the registry decision:

- **Capitol Trades** — free website, normalized congressional data, **no public API**; scraping their frontend is their ToS problem, skip.
- **Quiver Quantitative** — the paid congressional upgrade path if free-path engineering is rejected; API is subscription.
- **FMP / AInvest / SOV.AI / FinBrain / Apify actors** — all key-gated or paid credits; nothing keyless. No free advantage over eFD+House Clerk direct.
- **OpenInsider** — free HTML screener for Form 4 (INFERENCE, not probed); redundant to certified G1.
- **Finnhub insider-transactions (G4a)** — the one genuinely free keyed extra; keep as cross-check.

## Decision required from Mando (Phase 0 gate)

1. G5 DEAD triggers the order's STOP for congressional scope. Options:
   a. **Build free path**: Senate eFD scrape + House Clerk PDF parse (both chambers, official,
      $0, but the largest engineering line-item in the order — PDF/HTML parsing + history
      backfill; Phase 1 "one bulk file" ingest no longer matches reality).
   b. **Quiver test moves up** (paid, per order's stated fallback).
   c. Hybrid: free path Senate-only first (eFD is the cleaner parse), House later.
2. Confirm Yahoo v8 as designated price source given Stooq's bot-wall.
3. G3 Form4API: confirm DROP, or Mando creates a free account if the convenience is wanted.
