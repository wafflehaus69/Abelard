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

## Run (Order 1)

```
chatter-daemon --watchlist barber_growth   # load + validate one list, emit the scan envelope
chatter-daemon --all                        # every list in watchlists/
```

One JSON object (the scan envelope) is written to stdout per invocation; logs go to
stderr. Exit 0 iff `errors == []`, else exit 1.
