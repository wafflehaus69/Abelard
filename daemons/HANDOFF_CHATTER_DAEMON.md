# BUILD HANDOFF — ChatterDaemon (extended BizDaemon)

Issued by: Abelard (architect layer), relayed via the BizDaemon build session
Target: a NEW Claude Code session
Repo: `C:\Users\mdiba\Code\Abelard\` → new package `daemons/chatter_daemon/`
(working name — rename if you prefer `biz_daemon_x` / `social_daemon`)

This document is self-contained. Read it top to bottom before writing code. It
gives you: (1) the doctrine, (2) the existing BizDaemon architecture you are
extending and exactly where to read it, (3) the proposed multi-source design,
(4) the baseline filter-word lists to bundle, (5) a suggested build order.

---

## 0. What we are building

ChatterDaemon generalizes BizDaemon from a single source (4chan `/biz/ /smg/`)
to a **multi-source retail-chatter sensor**. New sources:

- **StockTwits** — `$CASHTAG` streams + trending symbols.
- **Reddit** — r/wallstreetbets, r/stocks, r/investing, r/options, etc.
  (configurable subreddit set).
- **Google Trends** — search-interest velocity for tickers/company names.

Plus:

- **Ticker + company-name extraction** across all sources (reuse and extend the
  existing four-layer filter and S&P-500 name resolver).
- An **attention protocol**: not just raw counts, but **cross-source
  confirmation** and **velocity/acceleration** across scrapes (is a name
  surging, and is it surging in more than one venue?).

It stays a **dumb sensor**. It extracts, counts, classifies stance, and emits
structured JSON. It performs **no materiality judgment, no trade signal**.
Abelard does the contrarian/confirm read at his layer against THESES.md and the
conviction list. Same split as BizDaemon.

---

## 1. Doctrine (non-negotiable — inherited from the OpenClaw daemons)

- **Scripts-first / LLM-last.** All fetching, extraction, validation, counting,
  velocity, and persistence are pure Python. The ONLY LLM call is the sentiment
  classification pass (Haiku tier).
- **Fail loudly. Never fake data. Never emit empty-success.** A failed fetch, a
  missing API key, a rate-limit, or a Haiku error surfaces as a structured
  entry in the `errors` array — never a silent empty result. A source that
  returns nothing legitimately (e.g. no trending names) says so explicitly.
- **No credentials in logs.** All keys/tokens from env only, never written to a
  log line or an error message. Redact query params.
- **Structured JSON out, never prose.** One canonical output object per run.
- **On-demand only (per source).** No resident loop. Entry point is a callable
  that runs one multi-source scrape and returns JSON, plus a CLI wrapper.
  Abelard invokes it. (A scheduler may wrap it later; not in v1.)
- **Respect each API.** Rate-limit per source; send conditional requests where
  supported; honor `Retry-After`. Polling is acknowledged and bounded by
  on-demand invocation.
- **Window-alignment.** The orchestrator stamps ONE canonical `scrape_ts` at
  entry and threads it through every source and downstream module. Do not
  recompute `now` in leaf modules.
- **Cost telemetry captured before artifact persistence** — a disk-write failure
  must not lose the Haiku cost record.
- **US equities only.** No crypto, no shitcoins. The Finnhub US symbol set is
  the validation gate; a token/name counts only if it resolves to a real
  US-listed symbol.
- **Read-only, public data.** Public APIs only. The daemon writes nowhere except
  its own local SQLite. It posts nothing and (in v1) alerts no one
  autonomously. The Ameriprise compliance wall holds: no firm systems, no client
  data — there is nothing here to wall off, and it stays that way.

---

## 2. The existing BizDaemon — your reference architecture

**READ THESE FILES FIRST.** BizDaemon is feature-complete, tested (89 passing),
and encodes every convention you should mirror. Treat it as the template; do not
reinvent patterns it already establishes.

Path: `C:\Users\mdiba\Code\Abelard\daemons\biz_daemon\`

```
biz_daemon/
  pyproject.toml            # nested-package layout, requires-python>=3.12,
                            # deps: requests, anthropic, python-dotenv;
                            # dev: pytest, requests-mock; console_script + data
  SOUL.md                   # the operational-identity doc — match its tone
  .env.example              # env contract; .env auto-loaded by config.py
  .gitignore
  biz_daemon/               # the importable package
    __init__.py
    __main__.py             # enables `python -m biz_daemon`
    config.py               # frozen Config.from_env(); loads .env (override=False,
                            #   shell wins); redacting logger; ConfigError;
                            #   BizDaemonError(stage=...).to_error(); all thresholds
    fourchan_client.py      # throttled fetch (>=1s), If-Modified-Since/304,
                            #   FORCE resp.encoding="utf-8" (critical — see §2.1),
                            #   HTML cleaning, loud-fail, NoSmgThreadError
    ticker_universe.py      # Finnhub /stock/symbol US set, SQLite-cached 24h TTL,
                            #   static fallback file, key never logged
    blacklist.py            # denylist + common-words loaders; add/remove file
                            #   maintenance helpers (CLI-backed)
    extractor.py            # THE PRECISION CORE — read this closely (see §2.2)
    sentiment.py            # Haiku per-post-per-ticker; batched + chunked;
                            #   prompt-cached system+schema; stop_reason guard;
                            #   aggregation; Cost telemetry; loud-fail per batch
    storage.py              # SQLite: universe cache + snapshot log (velocity
                            #   substrate); cost folded in before disk write
    orchestrator.py         # on-demand entry; canonical scrape_ts; assembles the
                            #   output contract; persists; never crashes uncaught
    cli.py                  # one-shot; --json (default) / --table; blacklist
                            #   add|list|remove subcommands; one JSON per run
    tableview.py            # pure-presentation table renderer (consumes payload)
    data/
      biz_slang_blacklist.txt   # the denylist (seed in §4)
      common_words.txt          # the wordlist (seed in §4)
      us_symbols_fallback.txt   # static universe fallback
      sp500_names.txt           # company-name -> ticker map (name resolution)
  tests/                    # pytest; one file per module; mock HTTP with real
                            #   requests.Response objects; FakeAnthropic client;
                            #   conftest with logger-reset + cfg/conn fixtures
```

Sibling daemons worth reading for patterns you'll need:

- `daemons/research_daemon/` — the closest flat-package template; read
  `envelope.py` (structured response envelope + closed warning-reason enum) and
  `http_client.py` (retry/backoff, 429 handling, URL redaction).
- `daemons/news_watch_daemon/` — read for: the **AlertSink** protocol
  (`src/news_watch_daemon/alert/sink.py` — a runtime-checkable Protocol with
  `channel_name` + `dispatch() -> DispatchResult`, never raises); the
  **attention/Pass-E frequency-discovery** modules (directly relevant to your
  attention protocol); the **claude-api LLM client** wiring
  (`synthesize/llm_client.py` — streaming, injected client, usage capture); and
  the SQLite **migrations** registry in `db.py`.

The **claude-api skill** is the source of truth for model calls. Invoke it at
build time for the live Haiku model ID and the SDK call pattern (prompt caching,
usage telemetry). As of this handoff the Haiku model is `claude-haiku-4-5`;
re-verify via the skill, do not pin from memory.

### 2.1 Critical lesson already learned — UTF-8 decode

`requests` infers response encoding from headers/chardet and falls back to a
platform default (cp1252 on Windows). Social APIs serve UTF-8. If you let
requests guess, non-ASCII (em-dashes, smart quotes, accents, emoji) gets
mis-decoded into mojibake, and any ticker wedged against that punctuation loses
its regex `\b` word boundary and **silently fails to extract** (we lost ~60% of
tickers to this). **For every source: force UTF-8** — set `resp.encoding="utf-8"`
before `.json()`/`.text`, or `resp.content.decode("utf-8")`. Add a regression
test per source adapter using a real `requests.Response` whose `.encoding` is
mis-set, asserting tickers adjacent to non-ASCII still extract.

### 2.2 The extractor (reuse wholesale, extend for prose)

`extractor.py` is the precision core. Per text blob it runs:

- **(a) Cashtag** `$([A-Za-z]{1,5}(?:\.[A-Za-z])?)` — high confidence,
  uppercased, validated against the universe. **Bypasses every filter below.**
  (StockTwits is almost all cashtags — this path will carry most StockTwits
  signal.)
- **(b) Length rule** — bare candidate < 2 letters rejected (single letters need
  a cashtag). `BARE_MIN_LEN = 2`.
- **(c) Wordlist rule** — bare candidate whose lowercased form is a common
  English word is rejected, UNLESS in the `word_ticker_allowlist`.
- **(d) Denylist rule** — bare candidate in the slang denylist rejected
  (case-insensitive).
- Bare candidate must also be in the universe (real symbol).
- **Name resolution** — whole-word, case-insensitive matching of S&P-500 company
  names in prose -> ticker, universe-gated, folded into the SAME per-post set so
  a name + its symbol count once.

The mention metric is **distinct posts** (one post spamming GME ×10 counts once).
Reddit/StockTwits will need **more bare-token discipline** than 4chan because
prose is longer — lean on the wordlist/denylist and prefer cashtag + name
resolution. Consider per-source confidence weighting (cashtag > name > bare).

---

## 3. Proposed architecture for ChatterDaemon

Mirror BizDaemon's module conventions. Suggested layout:

```
daemons/chatter_daemon/
  pyproject.toml
  SOUL.md                      # author a narrow one (scope, dumb/judgment split,
                               #   US-equities, read-only, fail-loud, per-source
                               #   compliance posture)
  .env.example
  chatter_daemon/
    __init__.py
    __main__.py
    config.py                  # frozen Config.from_env() + .env; per-source keys,
                               #   thresholds, subreddit list, attention params
    universe.py                # Finnhub US symbol set (lift ticker_universe.py)
    extractor.py               # lift from biz_daemon; add per-source confidence
    names.py                   # name-resolution + map loaders (lift)
    filters.py                 # denylist + common-words + allowlist loaders (lift
                               #   blacklist.py; rename for clarity)
    sentiment.py               # lift; now classifies normalized records, multi-source
    storage.py                 # SQLite: universe cache + per-source snapshot log
                               #   + the velocity substrate for attention
    sources/
      __init__.py
      base.py                  # Source protocol (see below)
      stocktwits.py
      reddit.py
      google_trends.py
      fourchan.py              # optional: port /smg/ as one more source
    attention.py               # the attention protocol (see §3.3)
    orchestrator.py            # canonical scrape_ts; fan out to sources; merge;
                               #   extract; attention; sentiment; assemble; persist
    cli.py                     # --json/--table; per-source flags; filter CLI
    tableview.py               # extend to show per-source columns
    data/
      slang_blacklist.txt      # §4
      common_words.txt         # §4
      us_symbols_fallback.txt  # lift
      company_names.txt        # lift sp500_names.txt; extend beyond S&P 500
  tests/
```

### 3.1 Source adapter protocol

Each source normalizes its native payload into a common record so the extractor,
sentiment, and attention layers are source-agnostic.

```python
# sources/base.py
from dataclasses import dataclass, field
from typing import Protocol

@dataclass(frozen=True)
class ChatterPost:
    source: str            # "stocktwits" | "reddit" | "google_trends" | "fourchan"
    post_id: str           # stable per-source id (string — ids aren't all ints)
    text: str              # cleaned, UTF-8, ready for the extractor
    author: str | None = None
    created_unix: int | None = None
    explicit_symbols: tuple[str, ...] = ()  # source-tagged cashtags/symbols, if any
    meta: dict = field(default_factory=dict)  # source-specific extras (score, etc.)

@dataclass(frozen=True)
class SourceResult:
    source: str
    posts: list[ChatterPost]
    warnings: list[str] = field(default_factory=list)  # non-fatal degradations
    error: str | None = None   # fatal -> folds into the top-level errors array

class Source(Protocol):
    name: str
    def fetch(self, *, scrape_ts: int) -> SourceResult: ...
```

Rules:
- A source failure is **isolated**: it returns `SourceResult(error=...)`, the
  orchestrator records it in `errors`, and the OTHER sources still produce
  output. Never let one dead source sink the whole scrape (mirror the per-batch
  loud-fail pattern already in `sentiment.py`).
- `explicit_symbols` lets StockTwits/Reddit hand you author-tagged `$TICKERS`
  directly; still validate them against the universe.
- Each source forces UTF-8 and respects its own rate limit.

### 3.2 Source notes & where to read the APIs

- **StockTwits** — public REST: streams by symbol
  (`/api/2/streams/symbol/{SYMBOL}.json`) and trending
  (`/api/2/trending/symbols.json`). Watch the documented rate limits (200
  req/hr/IP unauthenticated, more with a token). Posts carry an explicit
  `symbols` array — populate `explicit_symbols` from it. Confirm current
  endpoints/limits at build time; the API has changed over the years and may
  require an app token.
- **Reddit** — use **PRAW** (read-only mode needs a script-app `client_id` +
  `client_secret`; no user login required for public reads). Pull `hot`/`new`
  from a configurable subreddit set; include submission title + selftext +
  top-level comments. Respect the 60 req/min rule (PRAW handles it). Reddit text
  is long and noisy — name resolution + cashtag will outperform bare tokens.
- **Google Trends** — no official API; the community **pytrends** library is the
  usual route but is brittle and rate-limited (429s are common). Treat it as a
  **velocity signal, not a mention source**: query interest-over-time for the
  candidate tickers/names surfaced by the other sources, and feed the
  rising-interest delta into the attention protocol. Do NOT block a scrape on
  Trends — degrade gracefully if it 429s (warning, not fatal).

For all three: keep an adapter-level "force UTF-8 + structured error + rate
limit" discipline. Add a per-source `requests-mock`/fixture test.

### 3.3 The attention protocol

BizDaemon already persists per-scrape snapshots (the **velocity substrate**) but
intentionally did not compute velocity. ChatterDaemon's attention protocol is
where that pays off. Compute, per ticker, per scrape:

1. **Raw salience** — distinct-post mentions per source and total.
2. **Cross-source confirmation** — number of distinct sources mentioning it
   (1, 2, 3+). A name surging on StockTwits *and* WSB *and* Trends is a stronger
   signal than the same count on one venue.
3. **Velocity / acceleration** — change in mentions vs the previous N snapshots
   from `storage` (e.g. mentions now vs trailing average). Flag "accelerating".
4. **Attention flag** — a tunable rule combining the above, e.g.:
   `attention = (total_mentions >= N) OR (sources >= 2 AND accelerating) OR
   (google_trends_breakout)`. Keep N and the velocity window in config; start
   conservative and tune from live runs (BizDaemon started N=5).

Sentiment eligibility stays **decoupled** from the attention flag (BizDaemon
runs Haiku at mentions >= 3 while the ● marker is >= 5). Carry that forward:
classify a slightly broader set than you flag.

Emit attention components in the output so Abelard can audit *why* something
flagged (sources, velocity, trends), not just that it did.

### 3.4 Output contract (extend, don't break the shape)

Keep BizDaemon's envelope shape; add source breakdown and attention detail:

```json
{
  "scrape_ts": 1718500000,
  "sources": [
    {"source": "stocktwits", "posts": 1200, "ok": true},
    {"source": "reddit", "posts": 800, "ok": true},
    {"source": "google_trends", "posts": 0, "ok": false}
  ],
  "tickers": [
    {
      "ticker": "GME",
      "mentions": 41,
      "by_source": {"stocktwits": 22, "reddit": 19},
      "sources": 2,
      "velocity": {"prev_avg": 12.0, "delta": 29, "accelerating": true},
      "attention": true,
      "sentiment": {"directional": 30, "neutral": 11, "pct_bullish": 63,
                    "pct_bearish": 37, "read": "bullish"},
      "sample_post_ids": ["stocktwits:5582...", "reddit:t3_abc..."]
    }
  ],
  "cost": {"haiku_calls": 4, "input_tokens": 0, "output_tokens": 0},
  "errors": ["google_trends: rate limited (429); skipped"]
}
```

`errors` is always present. The full validated long tail is returned (low
salience visible, `attention:false`), never dropped.

---

## 4. Baseline filter-word lists (bundle these)

Three lists working together, with this precedence (highest first):

1. **Cashtag** (`$TICKER`) — bypasses everything below; validate vs universe only.
2. **Length** — bare 1-char rejected.
3. **Wordlist** — bare common-English word rejected, UNLESS in the allowlist.
4. **Denylist** — bare slang/acronym rejected.
5. **Universe** — bare must be a real US symbol.

Comparison is case-insensitive (denylist/allowlist uppercased; wordlist
lowercased). These lists are seeds — they GROW after live scrapes via the
filter-maintenance CLI. Bias toward precision: a missed mention is cheaper than a
false ticker polluting the attention tier.

### 4.1 `data/slang_blacklist.txt` — denylist (one token/line, uppercase)

Seed = BizDaemon's `/biz/` list + finance/StockTwits/Reddit slang + observed
live false positives (TV, TACO, UP… were polluting the attention tier on live
4chan runs). Cashtags bypass this, so blocking a token that is also a real
ticker only affects the bare-prose path.

```
# --- sentiment / hype slang ---
FUD
FOMO
YOLO
REKT
WAGMI
NGMI
HODL
PND
DCA
MOON
MOONING
PUMP
DUMP
BAGS
BAG
BAGHOLDER
TENDIES
STONK
STONKS
APE
APES
APING
DIAMOND
PAPER
HANDS
SQUEEZE
BTFD
BTD
COPE
SEETHE
MALD
GUH
LFG
HOLD
HODLING
PORN
GAINS
GAINZ
LOSS
LOSSES
PRINT
PRINTING
# --- due-diligence / analysis shorthand ---
DD
DYOR
NFA
ATH
ATL
ITM
OTM
EPS
PE
PEG
TA
FA
PT
ROI
RSI
MACD
EMA
SMA
VWAP
IV
# --- market / instrument shorthand ---
IPO
ETF
ETFS
QE
QT
EOD
EOY
YTD
ATM
CALLS
CALL
PUTS
PUT
STRIKE
SHORT
LONG
SHORTS
LONGS
# --- corporate / regulator acronyms ---
CEO
CFO
COO
CTO
SEC
FED
FOMC
IRS
ESG
DEI
# --- macro prints ---
CPI
PPI
GDP
NFP
# --- chat / forum filler ---
IMO
IMHO
OP
TLDR
GM
GN
FYI
AH
PR
LOL
LMAO
LMFAO
GG
EZ
OOF
TBH
IIRC
AFAIK
ELI5
TIL
EDIT
NSFW
# --- geos / currencies ---
USA
USD
EUR
GBP
JPY
CAD
AUD
EU
UK
US
UAE
NYC
NYT
# --- generic English / collision noise observed live ---
AI
IT
ON
ALL
GO
OR
BE
AM
PM
TV
TACO
UP
AS
CC
PC
MC
BY
HARD
HELP
HOUR
BOOM
BOSS
ANY
EYES
DIPS
DROP
MOVE
SPAM
CIA
BOAT
AINT
HVAC
MARS
LINE
WELL
GOOD
BEST
FAST
PLAY
FUN
LIFE
LOVE
NICE
NEXT
OPEN
```

NOTE: some of the trailing "generic English" tokens (OPEN/PLAY/LIFE/LOVE/RUN…)
ARE real tickers (Opendoor, etc.). They are denylisted on the BARE path only —
`$OPEN` still works, and you can promote any of them to the allowlist (4.3) if
they have conviction relevance. Keep this section under active audit.

### 4.2 `data/common_words.txt` — wordlist (one word/line, lowercase)

Lift BizDaemon's `common_words.txt` verbatim as the seed — it is a curated set of
short, high-frequency English words (only words ≤5 letters matter, since bare
candidates are capped at 5 chars). It rejects bare tokens like FOR, YOU, JUST,
LIFE, LOVE, KNOW, CUT, HAS, HIS, HIT, BULL, etc. Copy the file from:

`C:\Users\mdiba\Code\Abelard\daemons\biz_daemon\biz_daemon\data\common_words.txt`

Then EXTEND for longer-prose sources (Reddit/StockTwits sentences contain more
ordinary words than 4chan greentext). Add high-frequency words up to 5 letters
that collide with tickers, e.g.: are, can, did, get, got, had, her, him, his,
how, its, let, may, new, now(*), old, our, out, see, two, use, was, way, who,
why, win, yes, yet, big, buy, sell, hold, gain, fast, slow, good, best, bull,
bear, calls, puts, money, stock, trade, price, value, today, week, year, time,
high, low, open(*), close, green, red. (* = also tickers; keep them in the
wordlist AND in the allowlist so the allowlist override is what rescues them —
that is how the override is made load-bearing.)

DO NOT add real-ticker strings that are NOT common words (avgo, soxl, tqqq,
nvda, dram, mrvl, etc.) — those must pass.

### 4.3 `word_ticker_allowlist` (config seed) — real tickers that ARE common words

Tickers that collide with denylist/wordlist entries but you want counted on the
bare path. Cashtag always works regardless; this rescues the BARE form. Seed
(BizDaemon's + conviction additions):

```
NOW    # ServiceNow
META   # Meta Platforms
CORN   # Teucrium Corn ETF
COIN   # Coinbase
MSTR   # MicroStrategy / Strategy
```

Candidate additions for the architect to decide (each reintroduces a collision —
add only with intent): ON (ON Semi), ALL (Allstate), OPEN (Opendoor), HOOD
(Robinhood), RUN (Sunrun), CAR (Avis), PLAY, KEY, ALLY, PLUG, RUN, AI(?).
Keep the allowlist conservative; grow it by audit from live false-negatives.

### 4.4 Company-name map — `data/company_names.txt`

Lift `sp500_names.txt` (154 entries, 147 tickers, collision-pruned — apple,
oracle, intel, visa, ford, gap, target, block, etc. deliberately dropped because
they false-match in prose). Format: `name<TAB>TICKER`, ticker is the last
whitespace token, matched whole-word + case-insensitive, universe-gated. For
Reddit/StockTwits you may extend beyond the S&P 500 to meme/retail names that
are typed as words (e.g. gamestop→GME, blackberry→BB, palantir→PLTR,
robinhood→HOOD, opendoor→OPEN, sofi→SOFI). Apply the SAME pruning discipline:
drop any name that is a common word or false-matches in prose; when unsure, drop.

---

## 5. Suggested build order

Scaffold first, one capability at a time, ask before adding heavy deps, and flag
spec issues rather than silently deviating.

1. **Scaffold** the package (pyproject, nested package, data/, tests/) mirroring
   biz_daemon. Wire `python -m chatter_daemon` + console script. Load `.env`.
2. **Lift the reusable core** from biz_daemon: `config.py` (extend), `universe.py`
   (`ticker_universe.py`), `extractor.py`, `names.py`, `filters.py`
   (`blacklist.py`), `storage.py`, `sentiment.py`, `tableview.py`. Get the
   existing extractor/filter/sentiment tests passing against the new package.
3. **Bundle the filter data** from §4 (denylist, wordlist, allowlist, names,
   fallback symbols).
4. **Source adapters one at a time**, each: `fetch() -> SourceResult`, force
   UTF-8, rate-limit, structured error, a mock/fixture test (incl. the non-ASCII
   regression). Order: StockTwits → Reddit → Google Trends (Trends last; it is a
   velocity feed, not a mention source, and is the flakiest).
5. **Orchestrator**: canonical `scrape_ts`, fan out to enabled sources, isolate
   per-source failure, merge into one mention table (distinct-post across all
   sources, with `by_source` breakdown), run the extractor + name resolution.
6. **Attention protocol** (`attention.py`): cross-source confirmation + velocity
   off the snapshot substrate. Emit components in the payload.
7. **Sentiment**: Haiku pass on the eligibility set (decoupled from attention
   flag), batched + chunked + prompt-cached + `stop_reason` guard + per-batch
   loud-fail + cost telemetry. Verify the live model id via the claude-api skill.
8. **CLI + table**: `--json` default, `--table` human view (per-source columns),
   filter-maintenance subcommands (`filters add|list|remove`).
9. **Tests green**, then a live smoke run per source. Tune thresholds (attention
   N, velocity window, sentiment floor) from live output; seed the first round of
   denylist additions from observed false positives.

---

## 6. Open verification flags (report back to Abelard)

1. StockTwits endpoint shapes + current rate limits / token requirement.
2. Reddit script-app credentials available? (`client_id`/`client_secret` in env.)
3. pytrends viability / 429 behavior — confirm it degrades gracefully.
4. Live Haiku model id via the claude-api skill before pinning.
5. Attention thresholds (N, velocity window, cross-source rule) — start
   conservative, tune from the first live multi-source runs.
6. Company-name map extension beyond S&P 500 — which retail/meme names to add,
   and confirm each survives the prose-collision pruning.

---

## 7. Env contract (seed `.env.example`)

```
# --- required ---
FINNHUB_API_KEY=...            # US symbol universe (read from env only, never logged)
ANTHROPIC_API_KEY=...          # Haiku sentiment pass (required at sentiment time)

# --- sources (fill the ones you enable) ---
STOCKTWITS_TOKEN=...           # if the API requires it
REDDIT_CLIENT_ID=...
REDDIT_CLIENT_SECRET=...
REDDIT_USER_AGENT=chatter-daemon/0.1 by <you>
# google trends via pytrends needs no key

# --- optional overrides (defaults in config.py) ---
CHATTER_DB_PATH=...
CHATTER_SUBREDDITS=wallstreetbets,stocks,investing,options
CHATTER_ATTENTION_N=5
CHATTER_SENTIMENT_MIN=3
LOG_LEVEL=INFO
```

The daemon loads `.env` at startup (resolved next to pyproject.toml, regardless
of cwd); shell env vars override `.env`. Missing `.env` must not crash — it
no-ops and falls through to the loud `ConfigError` only if a REQUIRED key is
truly absent.

---

END OF HANDOFF. Build dumb, fail loud, structured JSON out. Abelard judges.
```
