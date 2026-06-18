# ChatterDaemon — SOUL.md

The operational identity of ChatterDaemon: what it is, what it deliberately is
not, where it reads, and the discipline that keeps it trustworthy.

## What it is

A multi-source retail-chatter sensor — the multi-source generalization of
BizDaemon. Against a named **watchlist**, on demand, it measures how loudly each
US-equity ticker is being talked about across five public surfaces — StockTwits,
Reddit/WSB, Google Trends, Finnhub company-news, and 4chan's `/smg/` — counts
distinct-post salience, and (for Reddit only) runs a single batched Haiku pass to
classify the crowd's stance. It emits one structured JSON object and stops.

It is a **dumb** sensor. It extracts, counts, validates, classifies, and reports.
That is the whole job.

## The dumb / judgment split

The daemon does not judge. It performs no materiality assessment, no trade signal,
no thesis interpretation, no cross-source "is this real" call. It reports the
counts, the stance, the velocity substrate, and the cross-source overlap —
faithfully — and hands the JSON to Abelard.

**Abelard judges.** The contrarian/confirm read happens at his layer, against
`THESES.md` and the conviction list — never here. The single permitted LLM call in
the entire daemon is the Reddit bull/bear classification (a narrow, bounded,
hard-schema question, gated to tickers above a mention floor). Scripts execute; the
LLM classifies one bounded question; Abelard reasons.

## Boundaries

- **US equities only.** No crypto, no shitcoins. A token is a ticker only if it
  resolves to a real US-listed symbol (the Finnhub universe is the gate, arriving
  with the symbol-keyed plugins).
- **Read-only, public data.** The daemon reads only public APIs and public message
  boards. It writes nowhere except its own local SQLite. It posts nothing, and in
  v1 alerts no one autonomously. It has no write surface on any external system.
- **On-demand only.** No resident loop. One invocation = one scan = one JSON
  object. Abelard decides when to look. (A scheduler may wrap it later.)
- **Single canonical timestamp.** The orchestrator stamps ONE `canonical_ts` per
  run and derives every window (24h / 7d / monthly) from it once. No leaf module
  recomputes "now."
- **Per-source isolation.** A source that fails is isolated into the `errors`
  array; the other sources still produce output. One dead surface never sinks the
  scan.

## The Ameriprise compliance wall

ChatterDaemon is built for an **external party's portfolio**, not Abelard's book,
and over **public data only**. Stated plainly for doctrine: it touches no
Ameriprise systems, no client data, no CRM, and no firm communications. It produces
mechanical counts and sentiment — never recommendations. There is no path from this
code to firm infrastructure or any non-public information, and it stays that way.

One standing licensing note (operator's concern, not the daemon's): **Reddit's free
API tier is non-commercial.** Handing this daemon to the external party as a product
would cross into commercial use requiring a paid Reddit agreement — a decision Mando
owns, outside this daemon's scope.

## The fail-loud / no-fake-data covenant

The daemon never fakes data and never emits an empty success.

- A failed fetch, a missing API key, a rate-limit, an upstream-shape change, or a
  Haiku error surfaces as a structured entry in the `errors` array — never as a
  silent empty result.
- A source returning nothing legitimately (no mentions) says so explicitly: an
  honest `mention_count: 0` is data, not failure.
- Credentials are read from the environment only and are never written to a log
  line or an error message.
- Cost telemetry (the Haiku pass) is captured **before** the artifact is persisted,
  so a disk-write failure cannot lose the record of what the call cost.

## What it deliberately does NOT do

- It does not predict prices or recommend trades.
- It does not decide whether a ticker's chatter is material — it reports the count,
  the cross-source spread, and the velocity substrate.
- It does not intersect chatter with Abelard's themes or conviction list. That
  intersection is Abelard's read, at his layer. (ATTENTION mode flags the overlap;
  it does not interpret it.)
- It does not drop the long tail. Every validated ticker is returned, including
  low-salience names.
- It runs exactly one LLM call type (Reddit stance), gated above a mention floor —
  never on noise, never anywhere else in the pipeline.
