# DATA_QUALITY — smart_money_daemon

## Senate eFD ingest (Phase 1a)

- Coverage: full browser index harvest of the eFD PTR corpus (search endpoint WAF-blocked, see recon/EFD_WAF_FINDING.md). Verified coverage from earliest trade **2012-06-14**; no year-walk applies.
- Filings seen: 1831 — status breakdown: {'electronic': 1562, 'paper': 269}
- Trade rows ingested: 13192
- Amendments among filings: 0

### Per-year filings and rows

| year | electronic | paper | trade_rows |
|---|---|---|---|
| 2014 | 80 | 0 | 571 |
| 2015 | 139 | 0 | 1126 |
| 2016 | 143 | 28 | 1285 |
| 2017 | 156 | 35 | 1315 |
| 2018 | 155 | 46 | 1135 |
| 2019 | 156 | 47 | 1251 |
| 2020 | 133 | 28 | 1681 |
| 2021 | 108 | 20 | 738 |
| 2022 | 89 | 18 | 799 |
| 2023 | 93 | 13 | 1085 |
| 2024 | 104 | 11 | 811 |
| 2025 | 131 | 17 | 882 |
| 2026 | 75 | 6 | 513 |

### Skipped filings per person per year (no OCR, never guessed)

| person_name | year | status | n |
|---|---|---|---|
| Blumenthal, Richard | 2025 | paper | 17 |
| Burr, Richard M | 2017 | paper | 16 |
| Burr, Richard M | 2019 | paper | 16 |
| Burr, Richard M | 2018 | paper | 14 |
| Feinstein, Dianne | 2018 | paper | 12 |
| Feinstein, Dianne | 2019 | paper | 12 |
| Boozman, John | 2018 | paper | 11 |
| Boozman, John | 2017 | paper | 11 |
| Boozman, John | 2021 | paper | 11 |
| Burr, Richard M | 2016 | paper | 10 |
| Boozman, John | 2019 | paper | 10 |
| Blumenthal, Richard | 2024 | paper | 10 |
| Blumenthal, Richard | 2020 | paper | 10 |
| Boozman, John | 2022 | paper | 9 |
| Boozman, John | 2020 | paper | 9 |
| Warner, Mark R | 2016 | paper | 9 |
| Blumenthal, Richard | 2019 | paper | 9 |
| Blumenthal, Richard | 2018 | paper | 8 |
| Blumenthal, Richard | 2022 | paper | 8 |
| Feinstein, Dianne | 2020 | paper | 7 |
| Blumenthal, Richard | 2023 | paper | 7 |
| Blumenthal, Richard | 2021 | paper | 7 |
| Blumenthal, Richard | 2026 | paper | 6 |
| Boozman, John | 2023 | paper | 6 |
| Blumenthal, Richard | 2017 | paper | 5 |
| Boozman, John | 2016 | paper | 5 |
| Blumenthal, Richard | 2016 | paper | 3 |
| Feinstein, Dianne | 2017 | paper | 3 |
| Burr, Richard M | 2020 | paper | 2 |
| Feinstein, Dianne | 2016 | paper | 1 |
| Burr, Richard M | 2021 | paper | 1 |
| Feinstein, Dianne | 2021 | paper | 1 |
| Feinstein, Dianne | 2022 | paper | 1 |
| Fetterman, John | 2024 | paper | 1 |
| Inhofe, James M | 2018 | paper | 1 |

(269 skipped filings total; table shows top 40)

### Row-level quality

- Rows with no ticker: 3079
- Rows with negative disclosure lag: 10
- Open-ended top band rows (amt_high NULL): 1
- Side distribution: {'purchase': 6669, 'sale_full': 3736, 'sale_partial': 2673, 'exchange': 114}

### Asset-type distribution (non-stock ingested, tagged, filtered only in Phase 2)

| asset_type | rows |
|---|---|
| Stock | 9489 |
| Municipal Security | 1050 |
|  | 781 |
| Other | 748 |
| Stock Option | 478 |
| Corporate Bond | 388 |
| Commodities/Futures Contract | 130 |
| Non-Public Stock | 121 |
| Cryptocurrency | 7 |

## House Clerk ingest (Phase 1b, pdfplumber text-layer extraction)

- Coverage: year-walk to electronic horizon **2013**; earliest trade 2015-05-08.
- Filings seen: 8294 — status breakdown: {'unparsed_layout': 3449, 'paper': 2437, 'electronic': 2407, 'fetch_failed': 1}
- Trade rows ingested: 32070
- Amendments among filings: 0

### Per-year filings and rows

| year | electronic | fetch_failed | paper | unparsed_layout | trade_rows |
|---|---|---|---|---|---|
| 2014 | 0 | 0 | 390 | 285 | 0.0000 |
| 2015 | 0 | 0 | 341 | 390 | 0.0000 |
| 2016 | 0 | 0 | 327 | 450 | 0.0000 |
| 2017 | 0 | 0 | 297 | 506 | 0.0000 |
| 2018 | 77 | 0 | 299 | 456 | 1653.0000 |
| 2019 | 141 | 0 | 171 | 379 | 2938.0000 |
| 2020 | 153 | 0 | 138 | 444 | 3563.0000 |
| 2021 | 146 | 0 | 118 | 419 | 3671.0000 |
| 2022 | 385 | 0 | 116 | 120 | 3062.0000 |
| 2023 | 388 | 0 | 81 | 0 | 4211.0000 |
| 2024 | 394 | 0 | 51 | 0 | 2738.0000 |
| 2025 | 449 | 0 | 69 | 0 | 7667.0000 |
| 2026 | 274 | 1 | 39 | 0 | 2567.0000 |

### Skipped filings per person per year (no OCR, never guessed)

| person_name | year | status | n |
|---|---|---|---|
| Lowenthal, Alan S. | 2020 | unparsed_layout | 124 |
| Lowenthal, Alan S. | 2018 | unparsed_layout | 70 |
| Lowenthal, Alan S. | 2021 | unparsed_layout | 66 |
| Sessions, Pete | 2017 | unparsed_layout | 57 |
| Lowenthal, Alan S. | 2019 | unparsed_layout | 55 |
| Lowenthal, Alan S. | 2016 | unparsed_layout | 31 |
| Lowenthal, Alan S. | 2015 | unparsed_layout | 28 |
| Delaney, John K. | 2015 | paper | 26 |
| Lowenthal, Alan S. | 2017 | unparsed_layout | 25 |
| Dingell, Debbie | 2016 | unparsed_layout | 25 |
| Delaney, John K. | 2016 | paper | 25 |
| Delaney, John K. | 2017 | paper | 25 |
| Delaney, John K. | 2018 | paper | 25 |
| Renacci, James B. | 2018 | paper | 24 |
| Renacci, James B. | 2016 | paper | 24 |
| Renacci, James B. | 2015 | paper | 24 |
| Renacci, James B. | 2017 | paper | 24 |
| Lowenthal, Alan S. | 2014 | unparsed_layout | 23 |
| Marchant, Kenny | 2015 | paper | 23 |
| Black, Diane | 2015 | paper | 22 |
| Lowenthal, Alan S. | 2022 | unparsed_layout | 21 |
| Dingell, Debbie | 2018 | unparsed_layout | 21 |
| Black, Diane | 2014 | paper | 20 |
| Renacci, James B. | 2014 | paper | 20 |
| Frelinghuysen, Rodney P. | 2016 | paper | 18 |
| Delaney, John K. | 2014 | paper | 18 |
| Dingell, Debbie | 2017 | unparsed_layout | 18 |
| Langevin, James R. | 2019 | unparsed_layout | 18 |
| Larson, John B. | 2018 | paper | 17 |
| Curbelo, Carlos | 2016 | unparsed_layout | 17 |
| Neugebauer, Randy | 2015 | unparsed_layout | 17 |
| Sessions, Pete | 2018 | unparsed_layout | 17 |
| Black, Diane | 2016 | paper | 17 |
| Black, Diane | 2018 | paper | 17 |
| Conaway, K. Michael | 2015 | unparsed_layout | 16 |
| Gianforte, Greg | 2018 | unparsed_layout | 16 |
| Frelinghuysen, Rodney P. | 2015 | paper | 16 |
| Marchant, Kenny | 2017 | paper | 16 |
| Gibbs, Bob | 2014 | unparsed_layout | 15 |
| Frelinghuysen, Rodney P. | 2018 | paper | 15 |

(5887 skipped filings total; table shows top 40)

### Row-level quality

- Rows with no ticker: 3856
- Rows with negative disclosure lag: 19
- Open-ended top band rows (amt_high NULL): 74
- Side distribution: {'purchase': 15953, 'sale': 10705, 'sale_partial': 5259, 'exchange': 153}

### Asset-type distribution (non-stock ingested, tagged, filtered only in Phase 2)

| asset_type | rows |
|---|---|
| Stock | 26053 |
| Government Security | 1607 |
| Corporate Bond | 767 |
| OT | 629 |
| Stock Option | 480 |
| HN | 157 |
| PS | 85 |
| OI | 63 |
| Cryptocurrency | 57 |
| VA | 34 |
| OL | 31 |
| AB | 27 |
| ET | 11 |
| RS | 3 |
| SA | 1 |

## Survivorship (F3, missing-price tickers)

- delisted_presumed: 345
- data_gap: 104
- Status: probed via Yahoo v8 (last probe 2026-07-22)
- Heuristic: no_yahoo_data_and_last_trade_gt_24mo => delisted_presumed else data_gap

**Bias statement:** excluded missing-series tickers are more likely losers (delisting skews down), so measured returns are inflated. Direction is known, magnitude is unmeasured. Returns are NEVER imputed for a missing series.

delisted_presumed tickers: 3V64.TI ABB ABC ABMD ACST ADS ADSW AESE AGN AGR AJINY AJRD AKRO ALLK ALTM ALXN ALYF AMEH AMGP ANDX ANTM ANZBY APEN APHA AQUA ARGO ARNA ASGN ASXC ATASY ATHA ATUS ATVI AVLR AVP AXLL AZEK BAF BAMXY BBL BCEL BGNE BKCC BKEP BKI BLFSD BLL BOWX BPL BPMP BRK.A BRKS BRMK CADE CAJ CATM CBLK CBS CCH CCLAY CCMP CCP CDAY CDEV CELG CEQP CERN CHK CHL CHNG CIT CIVI CLNY CLR CMA CMD CMLF CMLFU CMLTU COMM CONE COT COUP CPE CS CSLT CTL CTLT CTRL CTXS CVET CWEN.A CXO DCP DGNR DISCA DISH DLPH DMTK DNKN DNSKY DPLO DPM DPSGY DRE DRQ DWDP ECA ECOL ECOM EMKR ENBL ENLC ENOB ENV EQM ERI ESGC ESTE ESV ETM ETWO.W EVBG EVOP EXPI FDC FEI FEYE FII FL FLIR FLT FMBA FNFV.V FRC GEAGY GFN GLOP GMLP GMRE GNKOQ GOV GPP GPS GRUB GSAH HAYN HBI HDELY HDS HES HFC HHC HLUYY HRC HRS HSAC HYH INGIY INST INSW.V IPHI IS ISEE ITC ITTOY JEC JIH JMP KAR KMI.W KORS KSU LABL LGF-B LGF.B LGP LHCG LL LLL LLTC LM09.SG LMACU LMRK LMST LN LPT LTD LUKOY MAG MANT MAXR MFGP MGP MHLD MICRD MIK MINI MNK MNRL MPW MRO MRTX MRWSY MTBC MTSC MXIM MYL NBL NBLX NEE-PC NEWR NEX NILSY NKLA NLOK NLSN NPSND NRZ NS NTDTY NTT NUAN NVEE NVIGF NYCB NYLD OFC OMI ORAN ORCC PARA PBFX PCH PCPL PCPL.U PCRFY PDRDY PDYPY PHLD PKI PLL PLYA POL PRAH PRSP PSXP PTLA PX PXD PYCR QRTEA QRTEP QUMU RAI RBS RDS-B RDS.A RDS.B RHT RLGY RP RTL RTN RVNC RXN SAVA SBUX.SW SCU SGEN SHLX SHYF SIRE SIVB SIX SKVKY SMFKY SMLP SNE SNH SNP SOLO SPLK SRCL SRLP SSBK STAR STAY STOR SUP SWMAY SYNH SZEVY TCO TCP TCS TEF TELL TFFP TGNA TLND TLRD TLSYY TMK TOSYY TPCO TRHC TRIT TSS TUP TWKS TWTR UA-C UNVR UPMKY UTX VAR VBTX VGR VIAB VIAC VMW VRTU VSLR VSM VTNR VTRSV WBK WCAGY WETF WFM WLTW WMGI WOPEY WPG WPX WRK WRK.V XLNX XON YNDX ZAYO ZEN ZI ZUO

## Date integrity (filer typos, all chambers)

- Out-of-range tx_date rows (excluded from scorecard): 4
- Out-of-range disclosure_date rows: 0
- Negative-lag rows (disclosure before trade, excluded both clocks): 29 — by chamber {'house': 19, 'senate': 10}

| name | chamber | ticker | tx_date | disclosure_date | filing_id |
|---|---|---|---|---|---|
| Cohen, Steve | house | SONY | 2026-12-26 | 2026-02-09 | 20033889 |
| Foxx, Virginia | house | MMP | 2220-04-07 | 2022-05-04 | 20020914 |
| Foxx, Virginia | house | NEWT | 2202-09-19 | 2022-10-12 | 20021790 |
| Sessions, Pete | house | IBM | 3031-04-30 | 2021-05-03 | 20018672 |

## Notes

- disclosure_date = filing date (eFD PTR date for senate, Clerk index FilingDate for house).
- Paper and unparsed-layout filings counted, never OCRed, never guessed, per order.
- Amendment PTRs ingested as filed — possible re-reports of the same transaction. Flagged for Mando, dedup policy not specced in SM-1.
- House extraction library: pdfplumber (chosen over pypdf for positional words enabling layout-versioned column parsing).
