# CONSENSUS

Polymarket **winners-circle** signal system. It curates a roster of provably-skilled
Polymarket *international* wallets, detects when several converge on the same side of
the same market while edge remains, maps that market to a **legally tradeable**
Kalshi / Polymarket-US contract, and alerts the owner.

> **Read intl, trade legal.** Signals are read from Polymarket international on-chain
> data (public, read-only). Execution venues are Kalshi / Polymarket-US only. This
> codebase never places, signs, or stages an order on international Polymarket.

**Advisory-first.** Phases 1–4 produce research artifacts and alerts. Order staging
(M9) is design-only until an explicit, post-compliance go-ahead.

**No synthesized data.** A failed or partial fetch surfaces as a loud error or an
empty result and is logged as a gap — never filled with an estimate, mock, or
interpolation. Every signal traces back to a raw response persisted on disk.

**LLMs are not in the data path.** Ingestion is plain Python over REST/RPC.

---

## Status — M1 (data layer) built

The current milestone is **M1** (accepted): a deterministic, cached, rate-limited,
read-only data layer over Polymarket data-api + gamma-api and Kalshi public market
data, with an Etherscan-V2 Polygon fetcher (key-gated) staged for the M5 funding
graph. Standalone Polygonscan V1 was sunset 2025-08-15 — the unified Etherscan key
covers Polygon via `api.etherscan.io/v2/api?chainid=137`.

Endpoint schemas were verified live before coding (see the reference notes).
Gate 0 finding (2026-07-10): data-api caps history at limit<=1000 x offset<=3000
(newest ~4,000 records per dimension) — deep/archival tape comes from the Goldsky
orderbook subgraph instead; data-api serves the live/recent end.

## Setup

This package consumes the monorepo shared lib `abelard_common` (installed editable,
matching the other daemons — it is not on PyPI):

```powershell
cd C:\Users\mdiba\Code\Abelard\consensus
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -e ..\daemons\common     # abelard_common: http client + DaemonError
pip install -e .[dev]                 # consensus + pytest/requests-mock
```

Secrets (optional for M1) go in a git-ignored `.env` (see `.env.example`):
`ETHERSCAN_API_KEY` (free, register at etherscan.io) is only needed by the M5
funding-graph fetcher.

## Run

```powershell
# M1 acceptance: fetch one market, one wallet, one Kalshi page; print counts.
consensus data smoke

# Inspect individual fetchers:
consensus data trades   --market <conditionId>
consensus data trades   --wallet <proxyAddress>
consensus data positions --wallet <proxyAddress>
consensus data market   --market <conditionId>
consensus data kalshi   --limit 10 --status open

# Any command takes --json for a structured summary, and --config <path>.
```

`consensus data smoke` exits non-zero if any source is a gap, so cron/scripts can
detect a degraded run. Logs go to stderr; stdout carries only the report.

## Configuration

All behavior lives in `config.yaml` (pydantic-validated; unknown keys are a startup
error). The file grows one module at a time — M1 populates `data_layer` + `categories`;
the scoring/scan/crosswalk/alert/unusual-activity parameter blocks (spec §6) are added
as those modules land. Secrets never live in the yaml.

The raw-response cache (SQLite) defaults to `data/consensus_cache.db` (git-ignored).
Every live fetch is appended there verbatim; the backtest harness (M0) replays from it
with no-lookahead `as_of` reads.

## Tests

```powershell
pytest
```

Hermetic — no network (requests-mock intercepts), cache in a tmp dir.

## Layout

```
config.yaml              single source of configuration (grows per module)
consensus/
  config.py              pydantic config + env secrets + redacting logger
  errors.py              ConsensusError(DaemonError) + stage subclasses
  models.py              typed records; from_api() drops bad records, never fabricates
  cache.py               SQLite raw-response store (append-only, as-of replay)
  fetching.py            DataLayer: cache-through fetch, replay, gap-counting parse
  sources_polymarket.py  data-api + gamma-api fetchers
  sources_kalshi.py      Kalshi public market-data fetchers
  sources_polygon.py     Polygonscan ERC-20 transfers (M5 funding graph; key-gated)
  cli.py                 `consensus` CLI (owner-facing; --json optional)
tests/                   hermetic pytest suite
```
