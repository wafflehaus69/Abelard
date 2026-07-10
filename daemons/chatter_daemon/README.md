# ChatterDaemon

A multi-source retail-chatter sensor for OpenClaw — the multi-source generalization
of BizDaemon. It extracts US-equity ticker mentions, counts distinct-post salience,
and classifies stance across five public surfaces (StockTwits, Reddit/WSB, Google
Trends, Finnhub company-news, 4chan `/smg/`) against named **watchlists**. It is a
**dumb sensor**: it extracts, counts, and classifies, and emits structured JSON. It
performs no materiality judgment and no trade signal — Abelard judges. See
[SOUL.md](SOUL.md).

## Status

**Order 1 (spine).** Watchlist config primitive, the normalized-record schema (the
binding contract for all five plugins + the aggregator), the `Source` adapter
protocol, and the orchestrator spine (one canonical timestamp, every window derived
from it once). **No source plugins yet** — a run fans out over zero sources and
emits the canonical timestamp, the derived windows, and the validated watchlist
summaries with an empty record list.

## Compliance posture

Built for an **external party's portfolio**, over **public data only**. It touches
no Ameriprise systems, client data, CRM, or firm communications, and produces
mechanical counts/sentiment — never recommendations.

> **Standing note — Reddit's free API tier is non-commercial.** The Reddit plugin
> (Order 6) uses the official Reddit API's free tier, which is licensed for
> non-commercial use. If this daemon is ever handed to the external party as a
> product, that crosses into commercial use requiring a paid Reddit agreement — a
> licensing decision Mando owns, outside the daemon's scope.

## Install (editable, monorepo)

ChatterDaemon depends on the shared `abelard-common` library (the `DaemonError`
contract today; the noise filter / alias map / fetch primitives arrive with the
plugins). It is wired via an editable install, not a published dependency, so a
freshly recreated venv must install both:

```
python -m venv .venv
# Windows
.venv/Scripts/python -m pip install -e ../common -e .[dev]
# POSIX
.venv/bin/python    -m pip install -e ../common -e .[dev]
```

## Run

The whole pipeline — scan, persist, raw-history dump, and the client-facing PDF — is
**one command** (WSL today, macOS later; all config lives in `.env`, auto-loaded):

```
bash scripts/run.sh
```

That is two steps you can also run by hand:

```
# 1. Scan: aggregate against the baseline, persist to archive/, dump history/, emit JSON.
python -m chatter_daemon scan --all                       # every list in watchlists/
python -m chatter_daemon scan --watchlist barber_growth   # one list

# 2. Render the client-facing PDF from a persisted scan.
python -m chatter_daemon report archive/<YYYY-MM>/<scan_id>.json
```

One JSON object (the scan envelope) is written to stdout per scan; logs go to stderr.
Exit 0 unless every attempted source failed.

### Twitter/X (Order 17/21)

Twitter is a subprocess source (the `twitter` CLI, v0.8.x) and is **gated OFF by
default** — enable it only on a host that has the CLI. In `.env` set
`CHATTER_TWITTER_ENABLED=1`, `CHATTER_TWITTER_BINARY=<abs path>`, and the session
cookies `TWITTER_AUTH_TOKEN` / `TWITTER_CT0` (read ambient by the CLI, never logged;
they expire — refresh when searches start failing). X meters authenticated search to
~25 requests/rolling-window per account, so per-ticker searches beyond ~25 time out.
Set `CHATTER_TWITTER_PRIORITY` (must-have names — searched first, always land) and
`CHATTER_TWITTER_MAX_TICKERS` (cap to the top-N that fit the quota; the rest are
skipped that scan, logged). Twitter also ranks the queue by **Finnhub news volume**, so
the noisiest names win the budget first. See [.env.example](.env.example).

## Portfolios (watchlists)

A watchlist lives in `watchlists/` as either `{name}.json` or a human-editable
`{name}.csv` (one format per name — having both is ambiguous and fails loud).
`barber_growth` ships as **CSV**, so you can edit the portfolio in any spreadsheet:

| column | meaning |
|---|---|
| `symbol` | ticker (1–5 uppercase letters, optional `.CLASS`) |
| `names` | company-name aliases, pipe-separated (free-text matching + the Trends query) |
| `name_match` | `true`/`false` — match the name in free text (`false` for collision words: MU, NOW, CAT…) |
| `is_etf` | `true`/`false` — documents expected news/chatter silence |
| `enabled` | `true`/`false` — `false` excludes the row from scanning |
| `ambiguous_name` | `true`/`false` — the Trends query term is ambiguous |
| `notes` | free-text annotation (commas allowed; the CSV quotes them) |

Add / remove / reorder rows to change the portfolio — no code change. To trim which
names Twitter covers, shorten the list or lower `CHATTER_TWITTER_MAX_TICKERS`.
