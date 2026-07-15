# Chatter Daemon — Agent Doctrine

> **STUB — pending Mando review.** Sourced from
> `daemons/chatter_daemon/SOUL.md` + `README.md`. Entry point:
> `python -m chatter_daemon`.

**Status:** Order-by-order build. README declares **Order 1 (spine)**
complete: the watchlist config primitive, the normalized-record schema
(binding contract for the five plugins + aggregator), the `Source` adapter
protocol, and the orchestrator spine (one canonical timestamp, all windows
derived once) — with **no source plugins yet** (a run fans out over zero
sources, emitting `canonical_ts` + derived windows + validated watchlist
summaries with an empty record list). SOUL.md describes the full five-source
target vision. Later orders add plugins (Reddit at Order 6; Twitter/X at
Order 17/21, gated OFF by default).

## What it is

A multi-source retail-chatter sensor — the multi-source generalization of
BizDaemon. Against a named watchlist, on demand, it measures how loudly each
US-equity ticker is discussed across five public surfaces (StockTwits,
Reddit / WSB, Google Trends, Finnhub company-news, 4chan /smg/), counts
distinct-post salience, and (Reddit only) runs one batched Haiku pass to
classify stance. An optional Twitter/X subprocess source is gated off. A
dumb sensor.

## What it produces for me

One JSON scan envelope per scan on stdout (logs on stderr; exit 0 unless
every attempted source failed). A single `canonical_ts` with every window
(24h / 7d / monthly) derived from it once; per-source records; cross-source
overlap; velocity substrate. The full pipeline (`bash scripts/run.sh`) also
persists to `archive/<YYYY-MM>/<scan_id>.json`, dumps raw `history/`, and
renders a client-facing PDF. Local SQLite. Cost telemetry captured before
persistence.

## What it does NOT do

- Does not predict prices or recommend trades.
- Does not decide materiality — it reports count / spread / velocity
  substrate (ATTENTION mode flags overlap but does not interpret).
- Does not intersect chatter with Abelard's themes or conviction list.
- Does not drop the long tail (`mention_count:0` is honest data).
- Does not run more than one LLM call type (Reddit stance only, gated above
  a mention floor, never on noise).
- US equities only. Per-source isolation — one dead source never sinks the
  scan. Never fakes data.

## Write surfaces

None external. It writes only its own local SQLite (+ archive / history /
PDF artifact files). It posts nothing; in v1 it alerts no one autonomously.

## My read commands / inputs

- `python -m chatter_daemon scan --all` (every watchlist) or
  `scan --watchlist <name>`
- `python -m chatter_daemon report archive/<YYYY-MM>/<scan_id>.json`
- `bash scripts/run.sh` (whole pipeline)

Watchlists live in `watchlists/` as `{name}.json` or `{name}.csv`
(`barber_growth` ships CSV). Editable install:
`pip install -e ../common -e .[dev]`. Env in `.env` (Twitter gated via
`CHATTER_TWITTER_ENABLED` etc.; credentials read ambient, never logged).

## Compliance note

Built for an external party's portfolio, public data only — no Ameriprise
systems or client data. Standing note: Reddit's free API tier is
non-commercial; productizing would require a paid Reddit agreement
(Mando's call).
