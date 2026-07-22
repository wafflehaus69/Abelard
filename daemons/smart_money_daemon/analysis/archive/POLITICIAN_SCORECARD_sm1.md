# POLITICIAN_SCORECARD — smart_money_daemon Phase 2

**Chamber scope: HOUSE + SENATE.** house coverage from 2015-05-08; senate coverage from 2012-09-13. Ranking anchor date (last completed EOD): 2026-07-20.

MTM snapshot block: MTM quotes: 342 tickers, asof_unix min=1784577600 max=1784656765

## Top 15 by t-stat (clock A 63d, completed horizons only)

| person | t_stat | wavg_excess_A63 | hit_rate_63A | n_completed_A63 | n_open | copyability_gap_63 | median_lag_days | active_last_12mo | top5_tickers |
|---|---|---|---|---|---|---|---|---|---|
| McCormick, David H. | 5.6312 | 0.1701 | 0.9091 | 22 | 0 | 0.0628 | 25.0000 | True | BITB |
| Moore, Tim | 3.5101 | 0.0868 | 0.5664 | 113 | 17 | -0.0241 | 26.0000 | True | HOG F HY AAL CNC |
| Wyden, Ron | 2.9329 | 0.0352 | 0.5054 | 186 | 0 | 0.0111 | 22.0000 | False | AAPL AMZN NVDA MSFT KLAC |
| Sullivan, Dan | 2.8469 | 0.0903 | 0.7250 | 40 | 0 | -0.0558 | 29.0000 | False | MSFT GOOG AMZN ZTS DIS |
| Roberts, Pat | 2.8009 | 0.0317 | 0.5347 | 245 | 0 | -0.0015 | 22.0000 | False | AAPL NFLX NVDA NOC BA |
| Loeffler, Kelly | 2.7719 | 0.0531 | 0.6739 | 46 | 0 | 0.0634 | 42.0000 | False | DD CME PRU CVX ICE |
| Fields, Cleo | 2.6548 | 0.0486 | 0.5512 | 205 | 22 | -0.0140 | 20.0000 | True | NVDA AMZN GOOG AAPL META |
| Foxx, Virginia | 2.6435 | 0.0246 | 0.5438 | 274 | 2 | 0.0120 | 22.0000 | True | HTGC ARLP FLNG ET ASC |
| Axne, Cindy | 2.5898 | 0.0297 | 0.6635 | 104 | 0 | 0.0384 | 461.0000 | False | DHR V AMZN MA SBUX |
| Franklin, Scott | 2.4349 | 0.0607 | 0.7143 | 21 | 0 | 0.0332 | 29.0000 | False | CMCSA COST CSCO MDT WMT |
| Mullin, Markwayne | 2.3285 | 0.0284 | 0.5430 | 302 | 11 | 0.0110 | 29.0000 | True | ADBE LRN AMAT GS LRCX |
| Guest, Michael Patrick | 2.0067 | 0.0433 | 0.5500 | 40 | 0 | 0.0390 | 16.0000 | True | CHRD AMZN TSLA NVDA UBER |
| Johnson, Julie | 1.8089 | 0.0305 | 0.5205 | 73 | 0 | 0.0093 | 28.0000 | True | MMM COF PGR FIS PKG |
| Beyer, Donald Sternoff | 1.5572 | 0.0823 | 0.5960 | 99 | 0 | -0.0462 | 28.0000 | False | AAPL NDAQ MS GS EL |
| Jacobs, Christopher L. | 1.5570 | 0.0758 | 0.5217 | 69 | 0 | 0.1014 | 38.0000 | False | RC FHN DOC GEVO IIPR |

## Top 15 by clock-B 63d weighted excess (the follow-able list)

| person | wavg_excess_B63 | wavg_excess_A63 | n_completed_B63 | n_open | median_lag_days | active_last_12mo | top5_tickers |
|---|---|---|---|---|---|---|---|
| Sullivan, Dan | 0.1461 | 0.0903 | 40 | 0 | 29.0000 | False | MSFT GOOG AMZN ZTS DIS |
| Beyer, Donald Sternoff | 0.1285 | 0.0823 | 99 | 0 | 28.0000 | False | AAPL NDAQ MS GS EL |
| Moore, Tim | 0.1109 | 0.0868 | 113 | 17 | 26.0000 | True | HOG F HY AAL CNC |
| McCormick, David H. | 0.1073 | 0.1701 | 22 | 0 | 25.0000 | True | BITB |
| Evans, Dwight | 0.0769 | 0.0367 | 43 | 0 | 31.0000 | True | NVDA INTC GOOGL AMZN IBM |
| Fields, Cleo | 0.0626 | 0.0486 | 204 | 22 | 20.0000 | True | NVDA AMZN GOOG AAPL META |
| Scott, Austin | 0.0597 | 0.2313 | 18 | 0 | 27.5000 | False | KPLT PLUG FCEL T BLDP |
| Green, Mark | 0.0407 | 0.0090 | 169 | 0 | 14.0000 | False | NGL ET USAC AM MPLX |
| Roberts, Pat | 0.0331 | 0.0317 | 245 | 0 | 22.0000 | False | AAPL NFLX NVDA NOC BA |
| Franklin, Scott | 0.0275 | 0.0607 | 21 | 0 | 29.0000 | False | CMCSA COST CSCO MDT WMT |
| Wyden, Ron | 0.0241 | 0.0352 | 186 | 0 | 22.0000 | False | AAPL AMZN NVDA MSFT KLAC |
| Kean, Thomas H. | 0.0237 | 0.0096 | 58 | 13 | 35.0000 | True | CHKP ABT AMZN FCNCA AMCR |
| Donalds, Byron | 0.0218 | 0.0182 | 74 | 14 | 31.0000 | True | TTD BRO PGR PYPL GDDY |
| Johnson, Julie | 0.0212 | 0.0305 | 73 | 0 | 28.0000 | True | MMM COF PGR FIS PKG |
| Moran, Jerry | 0.0201 | 0.0084 | 112 | 0 | 147.0000 | False | GOOG BRK-B WFC DIS XOM |

## RECOMMENDED REGISTRY — PROPOSAL ONLY, final selection is Mando's

- Beyer, Donald Sternoff
- Fields, Cleo
- Franklin, Scott
- Johnson, Julie
- McCormick, David H.
- Moore, Tim
- Roberts, Pat
- Sullivan, Dan
- Wyden, Ron

## Methodology notes

- Purchases scored, sells descriptive. Stock assets only, resolvable ticker, floor >= 20 lifetime stock purchases.
- Two clocks: A from tx_date (skill), B from disclosure_date (copyability). Horizons 21/63/126 trading days on the SPY calendar, excess vs SPY, adjusted close.
- Ranking uses completed EOD horizons only, deterministic given the prices table. Open purchases marked to market against labeled quotes, never feeding the ranking.
- Weights: band midpoint capped at p90 ($32,500) times recency decay (full <= 24mo, linear to 0.25 at 60mo).

## Data-quality caveats

- 1749 trade events excluded for missing price coverage, 373 tickers had no usable series. Full list in DATA_QUALITY.md.
- Universe: HOUSE + SENATE. Paper and unparsed-layout filings skipped (never OCRed), counted in DATA_QUALITY.md. Open-ended top amount band stored with NULL high, midpoint uses its low bound.
- Amount ranges are disclosure bands, not exact sizes. Band midpoints are a coarse size proxy.
- Person rows canonicalized (honorific/whitespace splits merged) before scoring; see merge_persons.
