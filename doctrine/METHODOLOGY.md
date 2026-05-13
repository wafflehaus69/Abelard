# METHODOLOGY.md

This document is the full research reference. AGENTS.md contains the
condensed principles loaded every session. METHODOLOGY.md is the expanded
reference you consult when you need to execute on those principles — the
book you open when you need to look something up, not the rules you carry
by heart.

Five sections:

1. Source Hierarchy — Extended
2. Research Process for Investment Theses — Full
3. Geopolitical Research Process
4. Data Sources Reference Card
5. Output Standards

---

## 1. Source Hierarchy — Extended

### Tier 1 — Primary Sources (HIGHEST trust)

**SEC filings (audited financial data)**

- EDGAR full-text search: sec.gov/cgi-bin/browse-edgar
- EDGAR direct company page: sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK=<ticker>
- Key filings to pull:
  - 10-K — annual report, audited, comprehensive
  - 10-Q — quarterly, unaudited but reviewed
  - 8-K — material event disclosure (contracts, management changes,
    material transactions)
  - DEF 14A — proxy statement, executive compensation and governance
  - 13F — institutional holdings (filed quarterly, 45 days after quarter-end)
  - S-1 / S-3 — registration statements for IPOs and secondary offerings
- Use for: revenue by segment, operating margin, capex guidance, debt
  structure, management compensation, strategic direction statements
- Do not substitute: aggregator sites (Yahoo Finance, Google Finance,
  Macrotrends) frequently misattribute segment-level data or lag filings
  by days

**Government databases**

- USGS Mineral Resources Program: usgs.gov/minerals
  - Mineral Commodity Summaries for rare earths, critical minerals,
    reserves by country
- USAspending.gov: federal contract awards, grants, transactions
- SAM.gov: federal procurement opportunities and awards
- Federal Register: regulatory actions, proposed rules, executive orders
- DoD OIG: defense audits and oversight findings
- US Treasury OFAC: treasury.gov/ofac for sanctions status by country and
  entity
- State Department: state.gov for country reports, bilateral relations
- CENTCOM: centcom.mil for official military operations statements

**Academic and peer-reviewed sources**

- ScienceDirect: peer-reviewed journal articles across engineering,
  chemistry, materials science
- AIAA: American Institute of Aeronautics and Astronautics — propulsion,
  aerospace engineering
- Nature, Science: original research in fundamental science
- Use for: technical specifications, chemistry details, engineering
  constraints, physical property claims

**Company primary sources**

- Investor relations page on company website — always has the authoritative
  filings, presentations, and press releases
- Earnings call transcripts — Seeking Alpha or direct from company IR
- Investor day presentations — strategic direction, long-term targets
- Press releases — official announcements of contracts, acquisitions,
  management changes

### Tier 2 — Tier 1 News (HIGH trust, cross-verify)

**Financial news wires**

- Reuters: reuters.com
- Bloomberg: bloomberg.com (subscription)
- Associated Press: apnews.com
- Wall Street Journal: wsj.com (subscription)
- Financial Times: ft.com (subscription)
- Use for: breaking news, financial reporting, confirmed events

**Defense-specific outlets**

- Defense News: defensenews.com
- Breaking Defense: breakingdefense.com
- ENR (Engineering News-Record): enr.com — construction, infrastructure
- SpaceNews: spacenews.com
- Use for: defense contracts, military programs, industrial base
  developments

**Commodity and energy specialists**

- Rystad Energy: rystadenergy.com — oil, gas, energy transition data
- S&P Global Platts: spglobal.com/commodityinsights — commodity prices,
  supply analysis
- Wood Mackenzie: woodmac.com — energy sector research
- IEA: iea.org — international energy data and forecasts
- EIA: eia.gov — US energy data
- Use for: energy prices, production data, supply chain analysis

### Tier 3 — Sell-Side Analysis (MEDIUM trust, use to identify consensus)

**Investment bank research**

- Goldman Sachs, Morgan Stanley, BofA, JPMorgan, Citi, Barclays,
  Deutsche Bank
- Use to identify: consensus estimates, price targets, sum-of-parts
  models, institutional positioning
- The value is identifying where Mando's thesis diverges from consensus,
  not in accepting the consensus view

**Specialist outlets**

- Mining.com: mining sector news
- World Pipelines: pipeline industry
- Oilfield Technology: upstream oil and gas operations
- Use for: sector-specific technical and operational context

### Tier 4 — Background Context (LOW trust, verify upstream)

- Wikipedia: wikipedia.org — historical context, technical definitions
- Use only for context, never as primary citation. Always verify the
  underlying source.

### Signal Sources (Not Facts)

Social media platforms are signal about what people believe and what
money might react to, not sources of fact:

- Reddit (r/investing, r/wallstreetbets, r/stocks, specialty subs)
- X/Twitter (FinTwit, macro accounts, defense/energy specialists)
- Stocktwits
- Specialized Discord and Telegram channels
- Substack publications

Rules for these:
- Use as leading indicator, never as established fact
- Always trace claims to primary sources before acting
- Distinguish single-post noise from multi-account thematic signal over
  days
- Never state a social-media claim as established without primary
  confirmation

### Never Sources

- AI-generated summaries without cited primary sources — circular
  reference risk, no independent verification
- Any source whose claim chain cannot be traced to a primary document
  or named human source

---

## 2. Research Process for Investment Theses — Full

### Step 1: Verify the Claim Exists

Before analyzing a thesis, confirm the underlying factual assertion is
real. This is the step most commonly skipped, and skipping it produces
analysis of beliefs rather than facts.

- Find the specific document, filing, or statement that establishes the
  claim
- If the claim is "Company X is the sole producer of Y," find the DoD
  document, USGS data, or company filing that establishes the monopoly
- If the claim is "Country A bought B barrels of oil from Country C,"
  find the customs data, shipping records, or official trade disclosure
- If you cannot find primary documentation for the claim, the thesis is
  not yet research-ready. State this explicitly.

### Step 2: Pull Primary Financials

For any public company, the baseline is the most recent 10-K plus the
last two 10-Qs directly from EDGAR. Do not substitute aggregator data.

What to extract:
- Revenue by segment (not just consolidated revenue)
- Operating margin by segment
- Gross margin trend over 3+ years
- Capex guidance and actual capex trend
- Debt structure: maturity schedule, covenants, convertibles
- Cash and liquidity position
- Management guidance vs. historical realization
- Segment capacity data if disclosed (especially for capacity-constrained
  businesses)

How to read a 10-K efficiently:
- MD&A section first — management's framing of the business and results
- Segment disclosures second — where the money actually comes from
- Risk factors third — what management thinks could go wrong (read the
  specific risks, skip generic boilerplate)
- Footnotes on debt, pensions, contingencies — the real liabilities often
  live here
- Executive compensation (DEF 14A) — what management is actually
  incentivized to do

### Step 3: Find Current Price and What It Implies

Market cap divided by current revenue = price-to-sales.
Enterprise value divided by operating earnings = EV/EBIT.
Price-to-book for asset-heavy businesses.

Compare to relevant peer group. The correct comparable is rarely the
obvious one:
- NEU at specialty chemicals multiple when the business is defense
  monopoly — wrong comparable
- FTI at subsea contractor multiple when the thesis is LNG rebuild
  beneficiary — wrong comparable
- A treasury-crypto company at specialty software multiple — wrong
  comparable (mNAV is the right frame)

When the current multiple diverges materially from the correct comparable
multiple, ask why:
1. Market is ignorant (pre-narrative opportunity)
2. Genuine risk priced in (value trap)
3. Narrative mismatch (the market has the wrong frame)

The answer decides the trade.

### Step 4: Name the Catalyst

Every pre-narrative thesis requires a specific, named event that forces
the market to reassign the correct multiple. Without an identifiable
catalyst, a thesis can be correct indefinitely without generating returns.

Catalysts must be:
- Specific (not "positive news")
- Named (the actual event, not "some kind of announcement")
- Temporally bounded (happens at an identifiable time or within a
  definable window)
- Verifiable (you can tell when it has happened)

Examples of strong catalysts:
- "First Flagship Fund contract award naming AECOM as PM, mid-2026"
- "Congressional testimony specifically naming AMPAC in missile
  replenishment context"
- "Venezuelan hydrocarbons law passage"
- "SpaceX IPO roadshow week of June 8, 2026"

Examples of weak catalysts (rewrite or abandon thesis):
- "Eventual recognition by the market"
- "Some kind of positive news"
- "Sentiment shift"
- "When people realize"

### Step 5: Name the Thesis-Breaker

For every thesis, identify the specific falsifiable condition that would
make it wrong. Not vague risks — specific events.

Write the thesis-breaker before you write the thesis. If you cannot
identify a falsifying condition, you do not have a thesis. You have a
belief.

Examples of strong thesis-breakers:
- "QatarEnergy awards rebuild contracts exclusively to Korean or Chinese
  contractors"
- "Second North American ammonium perchlorate producer is stood up with
  government funding"
- "Chinese SiC semiconductor producers achieve defense-grade certification"
- "Extended BTC bear market forces convertible debt dilution at weak
  prices"

Examples of weak thesis-breakers (insufficient, rewrite):
- "Market conditions change"
- "Macro environment shifts"
- "Company underperforms expectations"
- "Something goes wrong"

### Step 6: Check Who Already Knows

Before entering, assess how discovered the thesis already is.

- Analyst coverage count and recent initiations: search Bloomberg,
  FactSet, or scrape analyst reports if available. Fewer analysts
  means more pre-narrative.
- Institutional 13F filings: SEC EDGAR, filter by company CIK, review
  holdings by major institutional holders over recent quarters. Look
  for accumulation or distribution patterns.
- Recent sell-side initiation reports: Seeking Alpha alpha sheets,
  investment bank research portals. A new initiation with a high
  target is a narrative-forming event.
- Google Trends and search volume on the ticker or thesis keywords:
  sudden increases signal narrative developing.
- CNBC, Bloomberg, WSJ coverage frequency: if the thesis has been on
  front pages, narrative is discovered.

Signals that the window is closing:
- 5+ analysts with updated price targets within 6 months
- Stock up 50%+ in 12 months
- Multiple major institutions showing accumulation in recent 13Fs
- Thesis appearing in mainstream financial media

If the window is closing, the thesis may still be valid but the
pre-narrative opportunity has passed. Size accordingly or pass.

---

## 3. Geopolitical Research Process

### Real-Time Situation Tracking

For active conflict or rapidly developing geopolitical situations, the
primary sources are:

- CENTCOM official releases: centcom.mil
- State Department daily briefings: state.gov/briefings
- UN Security Council verbatim records
- Tier 1 news wires (Reuters, AP, AFP) for breaking confirmed events
- Lloyd's List Intelligence for shipping and maritime situation
- MarineTraffic.com for real-time vessel tracking
- BIMCO (Baltic and International Maritime Council) for shipping industry
  circulars
- IEA emergency reports for energy supply impact

Update cycles matter. News from 48 hours ago is not current in a
rapidly evolving situation. When asked about current status, retrieve
fresh data before answering.

### Probability Assessment

Do not assign probabilities based on rhetoric alone. Assess structural
conditions:

- What does each party need domestically?
- What are their stated red lines versus their revealed preferences
  (what they have actually done)?
- What is each party's BATNA (Best Alternative to Negotiated Agreement)?
- What is the historical pattern of this actor's behavior in similar
  situations? The pattern is the data.
- What are the domestic constraints on escalation or de-escalation for
  each party?

Rhetoric and stated positions are cheap. Structural conditions and
revealed behavior are expensive to fake. Weight them accordingly.

### Alliance and Treaty Analysis

For geopolitical developments, map the treaty obligations and actual
alliance behaviors. Stated positions diverge from actual positions
regularly.

- UN voting records: more reliable than diplomatic statements
- Bilateral treaty text: check the actual obligations, not the summary
- Actual military actions taken: most reliable signal of alliance
  commitment
- Trade flows: who is still trading with whom despite stated sanctions
  or tensions

A country's behavior at the UN, its actual military deployments, and
its revealed trade patterns tell you more about its real alignment than
any official statement.

### Fog of War — Information as Battleground

In active conflicts and politically-charged situations, the information
environment is itself a theater of operations. Every party — adversary,
ally, and own government — has incentives to mislead, and every public
statement is produced with an audience in mind. Assume this by default.

The rules:

1. **Political statements are positioning, not reporting.** American
   politicians in particular tend to speak out of both sides of their
   mouths — claiming to pursue peace while positioning for attack,
   expressing restraint while greenlighting escalation, or denying
   negotiations that are actively underway. The stated position is
   often a signal to one audience while the operational reality serves
   another.

2. **Adversary statements are also signals, not facts.** When Iran
   publicly denies negotiations and then announces a concluded deal,
   the denial served a domestic audience and the announcement served
   a different one. Neither statement was "the truth" in a simple
   sense — they were sequential moves in an information operation.

3. **Allies lie too.** Shared interests do not produce shared honesty.
   Allied governments routinely misrepresent their capabilities,
   intentions, and knowledge to each other and to the public. Treat
   ally statements with the same scrutiny as adversary statements.

4. **Things change on a whim.** Positions that are firm on Monday can
   reverse by Friday as domestic pressure shifts, intelligence updates,
   or third-party actors move. A ceasefire announced today can be
   violated tomorrow. A deal described as "done" can collapse before
   implementation.

5. **Half-truths dominate.** What is reported to the public is often
   technically accurate but materially misleading — specific facts
   chosen to support a narrative while omitting the context that would
   change the interpretation.

Practical implications:

- Take all politically-charged information with a grain of salt,
  regardless of source
- Weight revealed behavior (what actors actually do) far more heavily
  than stated positions (what they say)
- Track the pattern of past statements vs. past actions for each major
  actor — the gap between rhetoric and behavior is the actor's real
  operating range
- When reporting on an active situation to Mando, distinguish between
  "confirmed facts" (primary source, verifiable), "stated positions"
  (claims made by named actors, not independently verified), and
  "analytical inference" (your interpretation of what is likely given
  the pattern)
- Never present a single source's statement as "what is happening."
  Present it as "what [actor] said, for whatever that is worth."

The seneschal's discipline: do not be the mechanism by which propaganda
reaches Mando's decision-making. Filter aggressively. When uncertain,
say so.

### Economic Impact Chain Tracing (Cascade Analysis)

For supply chain disruptions or infrastructure damage, trace the full
cascade:

1. Primary physical disruption (what was damaged or interrupted)
2. Immediate feedstock impact (what inputs to what industries)
3. Derivative industry impact (second-order effects on downstream
   manufacturing)
4. End-product availability (third-order effects on consumers and
   dependent industries)
5. Alternative sourcing timeline (how long until substitution capacity
   comes online)
6. Price regime impact (where does pricing land across the chain)
7. Winners and losers in the disruption

Use this framework for any supply chain disruption: helium, fertilizer,
rare earths, LNG, semiconductors, maritime routes.

### Rumor and Intelligence-Sourced Information

Some of Mando's highest-conviction calls come from information sourced
through relationships — "the billionaire room with generals," industry
whispers, proximity to people with actual visibility. These are
legitimate signals, but handle them differently:

- Treat as HIGH signal, but require primary-source confirmation before
  stating as fact
- Use to direct where to look, not as what to report
- If the primary source never emerges, discount the signal over time
- Never attribute unsourced intelligence to specific named sources

---

## 4. Data Sources Reference Card

### Company and Financial Data

| Need | Source |
|------|--------|
| SEC filings (10-K, 10-Q, 8-K, DEF 14A, 13F, S-1) | EDGAR: sec.gov/cgi-bin/browse-edgar |
| EDGAR full-text search | efts.sec.gov/LATEST/search-index |
| Earnings call transcripts | Seeking Alpha, company IR page |
| Investor presentations | Company IR page |
| Analyst coverage and targets | Bloomberg Terminal, FactSet, Refinitiv |
| Options and derivatives positioning | CBOE: cboe.com, OCC: theocc.com, CFTC COT reports |
| Short interest data | NYSE ThresholdLists, Nasdaq ThresholdLists |
| Institutional holdings (13F) | SEC EDGAR, filter by CIK |
| Insider transactions (Form 4) | SEC EDGAR, real-time via RSS |

### Government and Contracts

| Need | Source |
|------|--------|
| Federal contract awards | USAspending.gov, SAM.gov |
| Defense contracts (detailed) | Defense News contract database, DoD contract announcements |
| Regulatory actions and rules | Federal Register: federalregister.gov |
| Sanctions status | US Treasury OFAC: treasury.gov/ofac |
| Country reports | State Department: state.gov |
| Pentagon press briefings | defense.gov/News/Transcripts |
| Congressional testimony | congress.gov, committee pages |

### Minerals and Commodities

| Need | Source |
|------|--------|
| Rare earth and mineral data | USGS Mineral Resources: usgs.gov/minerals |
| USGS Mineral Commodity Summaries | usgs.gov/centers/nmic |
| Critical minerals pricing | IMARC Group, Metal Bulletin, SMM (Shanghai Metals Market) |
| Energy market data (oil/gas) | IEA: iea.org, EIA: eia.gov |
| Energy analysis and forecasts | Rystad Energy, Wood Mackenzie, S&P Platts |

### Shipping and Maritime

| Need | Source |
|------|--------|
| Real-time vessel tracking | MarineTraffic.com |
| Shipping intelligence | Lloyd's List Intelligence |
| Shipping industry circulars | BIMCO |
| Port traffic data | IHS Markit, port authority pages |

### Geopolitical and Conflict

| Need | Source |
|------|--------|
| Military operations | CENTCOM: centcom.mil |
| Conflict tracking | ACLED: acleddata.com |
| Defense and security research | SIPRI, International Crisis Group |
| Treaties and foreign relations | UN Treaty Collection: treaties.un.org |
| UN voting records | UN Digital Library |

### Technical and Engineering

| Need | Source |
|------|--------|
| Chemistry and materials science | ScienceDirect, peer-reviewed journals |
| Aerospace and propulsion | AIAA publications |
| General scientific research | Nature, Science, ArXiv for preprints |
| Engineering and construction | ENR (Engineering News-Record) |

### Venezuela-Specific (Active Thesis)

| Need | Source |
|------|--------|
| Sanctions status | OFAC: treasury.gov/ofac |
| Oil production data | OPEC monthly report, IEA country notes |
| Political developments | Reuters Caracas, AP, El Nacional |

### Crypto and Digital Assets

| Need | Source |
|------|--------|
| BTC on-chain data | Glassnode, CryptoQuant, Bitcoin Treasuries |
| Treasury-crypto company holdings | Bitcoin Treasuries: bitcointreasuries.net |
| Stablecoin reserves and flows | Issuer attestations (Circle, Tether), DeFiLlama |
| DEX/CEX volumes | CoinGecko, CoinMarketCap, DeFiLlama |
| Protocol TVL | DeFiLlama |

---

## 5. Output Standards

When presenting investment analysis to Mando, use this structure. Every
analysis should have the same shape so he can compare across positions
efficiently.

### Standard Investment Analysis Structure

1. **Ticker and Full Company Name**
2. **Current Price and Market Cap** (retrieved, not estimated — if
   retrieved data is unavailable, say so explicitly)
3. **Revenue and Operating Margin** — last reported quarter and trailing
   twelve months
4. **Current Multiple vs. Correct Comparable** — name the gap explicitly
5. **Thesis** — one sentence stating what the market does not yet
   understand
6. **Catalyst** — specific named event that forces re-rating
7. **Thesis-Breaker** — specific falsifiable condition that invalidates
   the thesis
8. **Conviction** — HIGHEST / HIGH / MEDIUM / WATCH
9. **Narrative Status** — PRE-NARRATIVE / PARTIALLY DISCOVERED / ALREADY
   RAN

### Uncertainty Disclosure

When you do not have current or verified data, say so explicitly. Do not
paper over uncertainty with confident generalizations.

Acceptable phrasings:
- "I don't have current data on this — let me retrieve it."
- "My information on this may be stale — here is what I have, and here
  is what you should verify."
- "I found [X source] but could not confirm [Y claim] from primary
  documentation. The thesis depends on [Y]; I recommend confirming
  before acting."

Mando can handle not knowing. He cannot handle being wrong because his
seneschal performed false certainty.

### Disagreement Protocol

When Mando states something the data contradicts, respond directly:
- "The data I have shows X, which conflicts with that assessment."
- "Here is the source."
- "Here is where the discrepancy might come from."

Do not soften disagreement into meaninglessness. Do not bury corrections
in excessive caveats. State the disagreement, provide the evidence, and
let Mando decide.

### Format for Thesis Updates

When news arrives affecting a thesis, update the THESES.md record and
report in chat:

- Name the thesis affected
- State what happened
- State what this means for catalyst or thesis-breaker status
- Recommend action (hold, trim, add, abandon) with reasoning
- Update THESES.md record accordingly

### What Not to Produce

- Analysis without cited sources
- Price or multiple data stated from memory when retrievable data is
  available
- Catalyst language that is vague or unspecific
- Thesis-breaker language that is not falsifiable
- Disagreement wrapped in so much caveating that the disagreement is
  obscured
- Agreement with a position just because Mando holds it

---

## When to Update This File

METHODOLOGY.md is standing doctrine. Update it when:

- A new primary source emerges that belongs in the reference card
- A pattern of research failure emerges that requires a new rule
- A specific methodology (e.g., how to read a particular kind of filing)
  proves useful repeatedly and should be captured

Do not update for:

- Specific current events (those go in memory files)
- Thesis-specific notes (those go in THESES.md or research/ subfolder)
- Opinions about the world (those go in WORLDVIEW.md)

When you update, tell Mando what changed and why.
