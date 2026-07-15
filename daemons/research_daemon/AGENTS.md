### Research Daemon

**Status:** Operational. Predates News Watch; may warrant a retrofit
SOUL.md when convenient.
**Model tier:** None. Read-only fetcher; no LLM in the daemon itself.
The judgment layer is me, against the JSON envelopes the daemon emits.
**Repository location:** `daemons/research_daemon/` in the Abelard
monorepo (`github.com/wafflehaus69/Abelard`).
**Doctrine:** No SOUL.md yet (pre-dates the SOUL.md-as-first-class
discipline). Tracked as a retrofit candidate.
**Read interface I use:** `daemons/research_daemon/SKILL.md` — the
output contract, command catalog, and usage patterns.

#### What it is

Read-only market and SEC research. Wraps Finnhub (free tier, 60 req/min)
for quotes / news / insider trades / 13F holdings, and SEC EDGAR for
filings (10-K, 10-Q, 8-K, DEF 14A). Two monitoring sweeps on top of
the deep-read calls compact output to "what changed / what's material"
across 10–40 tickers.

#### What it produces for me

Every subcommand emits one JSON envelope on stdout with
`{status, data_completeness, data, source, timestamp, error_detail,
warnings[]}`. Logs on stderr. Exit 0 iff `status == "ok"` (partial
completeness still exits 0). Warning `reason` is a closed enum —
pattern-match, don't parse prose.

I parse the JSON and reason. The daemon does not summarize, interpret,
or cache. Every call hits the upstream fresh.

#### What it does NOT do

- Does not provide real-time intraday tick data or options/futures/
  crypto chains.
- Does not provide analyst estimates, earnings dates, or guidance.
- Does not have write capability of any kind. Read-only by design.
- Does not summarize or interpret filings — body text is returned, I
  locate sections myself within byte-offset pagination.
- Does not expose Finnhub volume on quotes (free-tier gap; standing
  warning documents it — don't treat as failure).

#### Write surfaces

None. Read-only daemon by design.

#### My read commands

| Mando's question | My command |
|---|---|
| "What's [ticker] trading at?" | `research-daemon fetch-quote X` |
| "What's the news on [ticker]?" | `research-daemon fetch-news X --days 7` |
| "Any insider activity on [ticker]?" | `research-daemon fetch-insider-transactions X --days 30` |
| "Who's holding [ticker]?" | `research-daemon fetch-institutional-holdings X --top-n 10 --num-quarters 2` |
| "Get me [ticker]'s latest 10-K." | `research-daemon fetch-sec-filing X 10-K --limit 1 --include-body` |
| "Any 8-Ks lately on [ticker]?" | `research-daemon fetch-sec-filing X 8-K --limit 10` |
| "Sweep the watchlist for institutional moves." | `research-daemon detect-institutional-changes T1 T2 T3 --min-change-pct 10` |
| "Sweep the watchlist for material insider buys." | `research-daemon detect-insider-activity T1 T2 T3 --lookback-days 30 --min-value-usd 100000` |

The daemon's SKILL.md has the full pattern catalog including
morning-sweep and deep-dive templates.

#### Operational notes

- Rate limit: Finnhub free tier is 60 req/minute. Daemon retries 429s
  with exponential backoff, but large sweeps can still hit the wall —
  space them or stage across minutes.
- EDGAR requires a descriptive `User-Agent`. `EDGAR_USER_AGENT` env
  var is required at daemon startup.
- 13F filings lag ~45 days behind quarter-end by construction. Weight
  `reported_at` + `latest_filed_at` before time-sensitive use.
- Required env: `FINNHUB_API_KEY`, `EDGAR_USER_AGENT`. Daemon fails
  loudly on startup without them.

#### Relationship to my doctrine

Research Daemon is the primary-source layer for AGENTS.md's research
discipline. SEC filings and Form 4 trades come back as structured
primary-source data, not aggregator scrape. When Mando asks about a
position, this is where I start.

Insider activity and 13F changes are the read layer for the
portfolio-monitoring side of THESES.md — institutional QoQ moves on a
watchlist name surface as catalyst signal; material insider buys on a
held name are position-management signal.
