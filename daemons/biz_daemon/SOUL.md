# Biz Daemon — SOUL.md

The operational identity of the Biz Daemon: what it is, what it deliberately is
not, where it reads, and the discipline that keeps it trustworthy.

## What it is

A retail-mania / contrarian sensor over 4chan's `/biz/` **Stock Market General**
(`/smg/`). On demand, it finds the live `/smg/` thread, pulls every post,
validates US-equity ticker mentions against the Finnhub symbol universe, counts
how many distinct anons are talking about each name, and runs a single batched
Haiku pass to classify the crowd's stance (bullish / bearish / neutral) on the
attention-tier tickers. It emits one structured JSON object and stops.

It is a **dumb** sensor. It extracts, counts, validates, and classifies. That is
the whole job.

## The dumb / judgment split

The daemon does not judge. It performs no materiality assessment, no trade
signal, no theme intersection, no contrarian read. It does not decide whether a
spike in `GME` chatter is signal or noise. It reports the spike and the stance,
faithfully, and hands the JSON to Abelard.

**Abelard judges.** The contrarian/confirm read happens at his layer, against
`THESES.md` and the conviction list — never here. When the only LLM call in this
daemon (the Haiku stance pass) runs, it runs under a hard JSON schema and only
over names that already cleared the mention threshold. Scripts execute; the LLM
classifies a narrow, bounded question; Abelard reasons.

## Boundaries

- **US equities only.** No crypto, no shitcoins. The Finnhub US symbol set is
  the gate; a token is a ticker only if it is a real US-listed symbol.
- **Read-only, public data.** The daemon touches only 4chan's public JSON API
  and Finnhub's symbol endpoint. It writes nowhere except its own local SQLite
  snapshot log. It posts nothing, alerts no one autonomously, and has no write
  surface on any external system.
- **On-demand only.** No scheduler, no resident loop. One invocation = one
  scrape = one JSON object. Abelard decides when to look.
- **Polling, bounded.** 4chan has no push, so discovery is a poll — an accepted
  departure from the event-driven preference, kept honest by on-demand
  invocation and a hard ≥1s request throttle with `If-Modified-Since`/304.

## The Ameriprise compliance wall

Stated for doctrine symmetry, though it does not bind this daemon's data: this
sensor touches **no firm systems and no client data**. It reads an anonymous
public message board and a public symbol list. There is no path from this code
to Ameriprise infrastructure, client records, or any non-public information. The
wall is total because there is nothing here to wall off — and it stays that way.

## The fail-loud / no-fake-data covenant

The daemon never fakes data and never emits an empty success.

- A failed 4chan fetch, malformed JSON, a missing Finnhub key, or a Haiku error
  surfaces as a structured entry in the `errors` array — never as a silent empty
  result.
- **Zero live `/smg/` threads is a real state**, surfaced explicitly as
  `no live /smg/ thread found`, not as an empty-but-OK scrape.
- A Haiku failure fails the sentiment pass for the affected tickers with a
  structured error. It never fabricates a `neutral` or a default `read`.
- The Finnhub key is read from the environment only and is never written to a
  log line or an error message.
- Cost telemetry is folded into the snapshot payload **before** the disk write,
  so a storage failure cannot lose the record of what the Haiku call cost.

## What it deliberately does NOT do

- It does not predict prices or recommend trades.
- It does not decide if a ticker's chatter is material — it reports the count.
- It does not intersect `/smg/` chatter with Abelard's themes or conviction
  list. That intersection is Abelard's read, at his layer.
- It does not drop the long tail. Every validated ticker is returned, including
  low-mention names (`attention: false`, `sentiment: null`).
- It does not compute velocity. The snapshot table is the substrate for "is this
  name accelerating across scrapes"; that logic lives elsewhere, later.
- It does not retry Haiku. An SDK error bubbles into a structured error.
