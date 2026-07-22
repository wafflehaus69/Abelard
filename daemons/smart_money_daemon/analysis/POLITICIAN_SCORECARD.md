# POLITICIAN_SCORECARD — smart_money_daemon Phase 2 (SM-2 processing layer)

**Chamber scope: HOUSE + SENATE.** house coverage from 2015-05-08; senate coverage from 2012-09-13. Ranking anchor date (last completed EOD): 2026-07-20.

SM-2 corrections applied: F1 cluster correction (accumulation episodes scored as one event, 30-day window); F2 registry sectioning + registry.json; F5 amendment supersede filter; F6 QQQ style-tilt column. See analysis/archive/POLITICIAN_SCORECARD_sm1.md for the pre-SM-2 baseline.

MTM snapshot block: MTM quotes: 335 tickers, asof_unix min=1784577600 max=1784664327

## Top 15 by t-stat (clock A 63d, completed clustered events only)

| person | t_stat | wavg_excess_A63 | wavg_excess_A63_vs_QQQ | hit_rate_63A | n_completed_A63 | n_fills_total | n_open | copyability_gap_63 | median_lag_days | active_last_12mo | top5_tickers |
|---|---|---|---|---|---|---|---|---|---|---|---|
| Wyden, Ron | 2.1812 | 0.0455 | -0.0086 | 0.5000 | 72 | 212 | 0 | 0.0350 | 25.0000 | False | LLY AAPL DIS GOOG AMZN |
| Sullivan, Dan | 2.1361 | 0.1042 | 0.0675 | 0.7647 | 17 | 40 | 0 | -0.0579 | 29.0000 | False | MSFT GOOG AMZN ZTS DIS |
| Franklin, Scott | 2.0818 | 0.0620 | 0.0660 | 0.6154 | 13 | 21 | 0 | 0.0336 | 29.0000 | False | CMCSA COST LMT CSCO MDT |
| Guest, Michael Patrick | 2.0067 | 0.0433 | 0.0289 | 0.5500 | 40 | 40 | 0 | 0.0390 | 16.0000 | True | CHRD AMZN TSLA NVDA UBER |
| Roberts, Pat | 1.8567 | 0.0280 | -0.0028 | 0.5195 | 154 | 269 | 0 | 0.0005 | 18.0000 | False | NFLX AAPL NVDA NOC AMZN |
| Axne, Cindy | 1.8180 | 0.0315 | 0.0177 | 0.6140 | 57 | 117 | 0 | 0.0402 | 345.0000 | False | DHR V AMZN MA SBUX |
| Fields, Cleo | 1.6557 | 0.0910 | 0.0793 | 0.5636 | 55 | 213 | 16 | -0.0457 | 20.0000 | True | GOOG GOOGL NVDA TSM AAPL |
| Jacobs, Christopher L. | 1.5995 | 0.0812 | 0.0757 | 0.5312 | 64 | 109 | 0 | 0.1061 | 38.0000 | False | RC FHN DOC GEVO IIPR |
| Loeffler, Kelly | 1.5107 | 0.0375 | -0.0516 | 0.6190 | 21 | 53 | 0 | 0.0211 | 42.0000 | False | ICE ORCL AIG WDC EMR |
| Mullin, Markwayne | 1.4446 | 0.0195 | 0.0179 | 0.5217 | 184 | 312 | 1 | -0.0015 | 29.0000 | True | MSFT AMAT ADBE LRCX GS |
| Johnson, Julie | 1.4016 | 0.0337 | 0.0362 | 0.5179 | 56 | 79 | 0 | 0.0105 | 28.0000 | True | MMM COF PGR FIS PKG |
| Evans, Dwight | 1.3237 | 0.0371 | 0.0104 | 0.5000 | 42 | 45 | 0 | -0.0398 | 31.0000 | True | NVDA INTC GOOGL AMZN IBM |
| Beyer, Donald Sternoff | 1.3150 | 0.0833 | 0.0354 | 0.5890 | 73 | 105 | 0 | -0.0439 | 30.0000 | False | AAPL NDAQ MS GS EL |
| Moore, Tim | 1.2718 | 0.0658 | 0.0486 | 0.5882 | 34 | 118 | 10 | -0.0150 | 17.0000 | True | HOG VZ LGIH AAL CBRL |
| Lowenthal, Alan S. | 1.1924 | 0.0426 | 0.0321 | 0.5405 | 37 | 97 | 0 | 0.0322 | 9.0000 | False | GTLS SCI ZTS CRM NVS |

## Top 15 by clock-B 63d weighted excess (the follow-able list)

| person | wavg_excess_B63 | wavg_excess_A63 | n_completed_B63 | n_open | median_lag_days | active_last_12mo | top5_tickers |
|---|---|---|---|---|---|---|---|
| Sullivan, Dan | 0.1621 | 0.1042 | 17 | 0 | 29.0000 | False | MSFT GOOG AMZN ZTS DIS |
| Fields, Cleo | 0.1367 | 0.0910 | 54 | 16 | 20.0000 | True | GOOG GOOGL NVDA TSM AAPL |
| Beyer, Donald Sternoff | 0.1271 | 0.0833 | 73 | 0 | 30.0000 | False | AAPL NDAQ MS GS EL |
| Moore, Tim | 0.0808 | 0.0658 | 34 | 10 | 17.0000 | True | HOG VZ LGIH AAL CBRL |
| Evans, Dwight | 0.0769 | 0.0371 | 42 | 0 | 31.0000 | True | NVDA INTC GOOGL AMZN IBM |
| Scott, Austin | 0.0593 | 0.1869 | 14 | 0 | 28.0000 | False | KPLT PLUG FCEL T BLDP |
| Green, Mark | 0.0566 | 0.0180 | 62 | 0 | 15.0000 | False | NGL ET USAC AM USDP |
| Moran, Jerry | 0.0424 | 0.0057 | 50 | 0 | 31.0000 | False | GOOG BRK-B WFC DIS XOM |
| Franklin, Scott | 0.0284 | 0.0620 | 13 | 0 | 29.0000 | False | CMCSA COST LMT CSCO MDT |
| Kean, Thomas H. | 0.0277 | 0.0061 | 52 | 11 | 34.5000 | True | CHKP ABT AMZN FCNCA AMCR |
| Roberts, Pat | 0.0275 | 0.0280 | 154 | 0 | 18.0000 | False | NFLX AAPL NVDA NOC AMZN |
| Johnson, Julie | 0.0231 | 0.0337 | 56 | 0 | 28.0000 | True | MMM COF PGR FIS PKG |
| Donalds, Byron | 0.0218 | 0.0176 | 35 | 7 | 33.5000 | True | TTD BRO PGR PYPL GDDY |
| Mullin, Markwayne | 0.0210 | 0.0195 | 184 | 1 | 29.0000 | True | MSFT AMAT ADBE LRCX GS |
| Cassidy, Bill | 0.0206 | 0.0063 | 40 | 0 | 32.0000 | False | DIS AAPL NVS CSCO XOM |

## RECOMMENDED REGISTRY — PROPOSAL ONLY, final selection is Mando's

Active names (traded in the last 12 months, forward-signal candidates):

- Evans, Dwight
- Fields, Cleo
- Johnson, Julie
- Moore, Tim
- Mullin, Markwayne

## VALIDATION COHORT — strong record, NO forward signal

Strong on both lists but inactive in the last 12 months. Use to validate methodology, NOT to follow — they are not currently trading.

- Beyer, Donald Sternoff
- Franklin, Scott
- Roberts, Pat
- Sullivan, Dan

## NON-PERFORMER / QUALITATIVE WATCH — not a skill claim

Included in registry.json regardless of t-stat. Their signal value is composition or flow, not stock-picking skill; scores are shown honestly but they are NOT ranked performers.

- McCormick, David H. (btc_flow_sentinel), cluster-corrected t-stat 0.02 — cluster corrected picking t stat near zero tracked as crypto flow sentinel not a picker
- Foxx, Virginia (qualitative_watch), cluster-corrected t-stat 1.0 — non index book energy bdc shipping composition watch not skill claim

## Methodology notes

- Purchases scored, sells descriptive. Stock assets only, resolvable ticker, floor >= 20 lifetime stock purchases.
- Two clocks: A from tx_date (skill), B from disclosure_date (copyability). Horizons 21/63/126 trading days on the SPY calendar, excess vs SPY, adjusted close.
- Ranking uses completed EOD horizons only, deterministic given the prices table. Open purchases marked to market against labeled quotes, never feeding the ranking.
- Weights: band midpoint capped at p90 ($48,003) times recency decay (full <= 24mo, linear to 0.25 at 60mo).

## Data-quality caveats

- 1083 trade events excluded for missing price coverage, 373 tickers had no usable series. Full list in DATA_QUALITY.md.
- Universe: HOUSE + SENATE. Paper and unparsed-layout filings skipped (never OCRed), counted in DATA_QUALITY.md. Open-ended top amount band stored with NULL high, midpoint uses its low bound.
- Amount ranges are disclosure bands, not exact sizes. Band midpoints are a coarse size proxy.
- Person rows canonicalized (honorific/whitespace splits merged) before scoring; see merge_persons.
