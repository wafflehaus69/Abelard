# O1_P0_RECON — options chamber recon (report and stop)

Run 2026-07-27 (probes from Orban + Basilic). P0 is a measure-and-report gate.
Two verdicts + measured nightly snapshot cost. Mando ruling on this report:
**P1 proceed; P2 chain depth = rule-based near-dated (<=75 days, floor 4, cap 6);
retention proposal deferred to the P5 gate.**

## Verdict 1 — Yahoo options endpoint: VIABLE, DEGRADED-class access
- Endpoint `query1.finance.yahoo.com/v7/finance/options/{ticker}` returns the
  chain (HTTP 200, 73 KB for TSLA in 0.4s).
- Fields present (all the order needs): strike, volume, openInterest,
  impliedVolatility, lastPrice, lastTradeDate, bid, ask, inTheMoney,
  contractSymbol, expiration.
- Coverage good across the scoped range including small-caps: TSLA 22 expiries /
  GLD 24 / WULF 20 / ONDS 12 / MOG-A 4 (thin). Ticker normalization needed
  dot->dash (`MOG.A` -> `MOG-A`).
- ACCESS CONSTRAINTS (DEGRADED-class rules apply):
  1. Crumb+cookie required (unlike the v8 chart endpoint prices use): a
     cookie -> getcrumb -> request dance, and it is aggressively rate-limited
     (bursts got 429 Too Many Requests / Invalid Crumb; ~2s pacing is clean).
     Collector needs robust session management, retry on 401/429, generous
     pacing; long-term reliability from a fixed collector IP is a risk to watch.
  2. Default response = FRONT EXPIRY ONLY (one options block). A full chain needs
     one request per expiry (`?date=<epoch>`) — ~15-24 for liquid, 4-12 small-cap.
     The near-dated depth rule (<=75d, floor 4, cap 6) caps this to 4-6 requests
     per ticker.
- OI SEMANTICS (verified): openInterest is the OCC-settled figure from the PRIOR
  publication (T+1), never same-day — so vol/OI is "today's flow vs prior
  positioning", not the mechanically self-referential same-day ratio the order
  warned against. OI lags volume by ~1 trading day; the collector must record
  OI's as-of alongside each snapshot and label the offset. Empirical 2-snapshot
  confirmation (watch OI update with the lag) deferred to P2.

## Verdict 2 — Form 4 Table II: PASS
Five live derivative-carrying filings parsed cleanly. Every field extracts:
securityTitle (stock options / warrants / performance rights),
conversionOrExercisePrice, transactionDate / exerciseDate / expirationDate,
underlyingSecurityTitle + underlyingSecurityShares, transactionCoding code (M/A),
transactionShares, transactionPricePerShare. Empty fields on performance-rights
are legitimate (no exercise price/expiry), not parse failures.

Ledger count: form4_backfill_seen = 175,219; distinct accessions with
non-derivative rows = 148,893; seen-with-NO-non-derivative-rows (derivative-only
or parse-fail) = 26,326. Higher than the order's ~19K estimate because the
scheduled universal ingest has grown the corpus since SM-U1.

## Measured nightly snapshot cost (~60 scoped tickers)
| scope | requests | data/night | wall @2s | 1-yr raw |
|---|---|---|---|---|
| front-expiry only | ~60 | ~2-4 MB | ~2 min | ~1 GB |
| full chain (all expiries) | ~900 | ~30-50 MB | ~30 min | ~11-18 GB |
| RULE near-dated <=75d floor4 cap6 (chosen) | ~240-360 | ~8-14 MB | ~8-12 min | ~3-5 GB |

Raw-chain growth is the binding storage cost — a retention rule is required
(proposed at the P5 gate, not improvised). Near-dated concentration is where UOA
lives, so the depth rule is also signal-appropriate, not only cost-driven.
