# Biz Daemon — Agent Doctrine

> **STUB — pending Mando review.** Sourced from `daemons/biz_daemon/SOUL.md`
> (no README exists). Entry point: `python -m biz_daemon`.

**Status:** Operational, on-demand — no scheduler, no resident loop. Built
2026-06-03; 43 tests. A dumb sensor: it measures, it does not judge.

## What it is

A retail-mania / contrarian sensor over 4chan /biz/ Stock Market General
(/smg/). On demand it finds the live /smg/ thread, pulls every post,
validates US-equity ticker mentions against the Finnhub symbol universe,
counts how many distinct anons mention each name, and runs a single batched
Haiku pass to classify the crowd's stance (bullish / bearish / neutral) on
attention-tier tickers.

## What it produces for me

One structured JSON object per invocation, then it stops. Per ticker:
distinct-anon mention count, an attention flag, and sentiment (null for the
low-mention long tail where `attention:false`). An `errors` array (fail
loud). Cost telemetry folded into the snapshot payload before disk write. It
also persists a local SQLite snapshot log — the substrate for future
velocity / acceleration logic.

## What it does NOT do

- Does not predict prices or recommend trades.
- Does not decide whether chatter is material — it reports the count only.
- Does not intersect /smg/ chatter with Abelard's themes or conviction list
  (that is Abelard's layer).
- Does not drop the long tail — every validated ticker is returned.
- Does not compute velocity, and does not retry Haiku (an SDK error surfaces
  as a structured error).
- US equities only — no crypto / shitcoins. Never fakes data and never emits
  empty success; zero live /smg/ threads surfaces explicitly as
  "no live /smg/ thread found."

## Write surfaces

None external. It writes nowhere except its own local SQLite snapshot log.
It posts nothing and alerts no one autonomously.

## My read commands / inputs

On-demand invocation (`python -m biz_daemon`): one invocation = one scrape =
one JSON. It reads only 4chan's public JSON API and Finnhub's symbol
endpoint (>=1s throttle, If-Modified-Since / 304). Env: `FINNHUB_API_KEY`
(read from the environment only, never written to a log line or error).
