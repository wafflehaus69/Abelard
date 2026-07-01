# Research Daemon

Read-only data fetching for Abelard. The daemon exposes deterministic Python
capabilities through a `research-daemon` CLI that emits JSON envelopes on
stdout. It makes no judgments, issues no writes, and never executes arbitrary
code.

## Status

Complete at the Python + CLI layer. 279 tests passing, all hermetic
(HTTP mocked, no network required). OpenClaw workspace registration and
the actual wire-up to Abelard happen on the Mac mini after migration.

## Capabilities

**Deep-read** — targeted queries for research:

| Subcommand | Purpose |
|---|---|
| `fetch-quote <ticker>` | Current price, day range, 52-week range. |
| `fetch-news <ticker> [--days N]` | Recent company news within a window. |
| `fetch-insider-transactions <ticker> [--days N]` | Form 4 insider trades. |
| `fetch-institutional-holdings <ticker> [--top-n N] [--num-quarters N]` | 13F top holders, flat or multi-quarter. |
| `fetch-sec-filing <ticker> <type> [--limit N] [--include-body] [--max-body-chars N] [--offset-chars N]` | Recent SEC filings of a given type (metadata or body). |

**Monitoring** — sweeps across a ticker list, filtering for signal:

| Subcommand | Purpose |
|---|---|
| `detect-institutional-changes <tickers...> [--min-change-pct N]` | QoQ 13F position changes: new / closed / increased / reduced. |
| `detect-insider-activity <tickers...> [--lookback-days N] [--min-value-usd N] [--no-first-time-detection] [--first-time-lookback-days N]` | Material insider buys, cluster buys, first-time filers. |

## Requirements

- Python 3.12+
- Finnhub free-tier API key — https://finnhub.io
- A descriptive SEC User-Agent string (required by EDGAR fair-access policy)

## Setup

The daemon runs WSL-native. Source lives in the Abelard monorepo on the
Windows-mounted drive; venv and `.env` live on ext4 for speed and real
POSIX permissions.

```bash
# Source (in the monorepo, no move needed):
SRC=/mnt/c/Users/mdiba/Code/Abelard/daemons/research_daemon

# 1. Native venv on ext4 (faster imports than /mnt/c-hosted venv).
python3.12 -m venv ~/.venvs/research_daemon
~/.venvs/research_daemon/bin/pip install -e "$SRC[dev]"

# 2. Credentials on ext4 (mode 600, off the NTFS mount).
mkdir -p ~/.openclaw/research_daemon
cp "$SRC/.env.example" ~/.openclaw/research_daemon/.env
chmod 600 ~/.openclaw/research_daemon/.env
# Edit ~/.openclaw/research_daemon/.env — fill FINNHUB_API_KEY and EDGAR_USER_AGENT.
```

Verify:

```bash
set -a; source ~/.openclaw/research_daemon/.env; set +a
~/.venvs/research_daemon/bin/research-daemon --help
~/.venvs/research_daemon/bin/research-daemon fetch-quote AAPL
```

For convenience, add a shell alias:

```bash
alias research-daemon='~/.venvs/research_daemon/bin/research-daemon'
```

## Environment variables

| Variable | Required | Purpose |
|---|---|---|
| `FINNHUB_API_KEY` | yes | Finnhub free-tier key (60 req/min). |
| `EDGAR_USER_AGENT` | yes | Descriptive string (e.g. `ResearchDaemon contact@example.com`). Required by SEC. |
| `LOG_LEVEL` | no | `DEBUG` / `INFO` (default) / `WARNING` / `ERROR`. |

## CLI contract

- **Envelope → stdout.** Exactly one JSON document per invocation.
- **Logs → stderr.** Never mixed with the envelope.
- **Exit 0** iff `envelope.status == "ok"`. Partial completeness (`data_completeness: "partial"`) still exits 0; check `warnings`.
- **Exit 1** for any other status or unhandled exception. An envelope is emitted in those cases too, so callers always have structured output to parse.

## Response envelope

```json
{
  "status":            "ok" | "error" | "rate_limited" | "not_found",
  "data_completeness": "complete" | "partial" | "metadata_only" | "none",
  "data":              { ...capability-specific... } | null,
  "source":            "finnhub" | "edgar" | "yahoo",
  "timestamp":         "2026-04-24T18:04:05Z",
  "error_detail":      null | "human-readable string",
  "warnings": [
    {
      "field":      "volume",
      "reason":     "not_available_on_free_tier",
      "source":     "finnhub",
      "suggestion": "upgrade Finnhub or add a secondary source"
    }
  ]
}
```

### Completeness semantics

- `complete` — every field the capability normally returns is populated.
- `partial` — primary data valid, some fields null or degraded. Inspect `warnings`.
- `metadata_only` — only metadata available (e.g. SEC filing URL without body).
- `none` — no usable data (always paired with non-ok `status`).

### Warning reason enum (closed set)

`not_available_on_free_tier` · `upstream_timeout` · `upstream_error` ·
`rate_limited` · `not_found` · `stale_data` · `parse_error` ·
`missing_field` · `insufficient_history`

Extend in `research_daemon/envelope.py` when a new capability needs it.

## Running tests

```bash
pytest
```

All tests hermetic: HTTP mocked via `requests-mock`, yfinance patched at `yf.Ticker`. 266 tests as of this writing.

## Known limitations

**Volume is not returned by `fetch-quote`.** Finnhub free-tier `/quote` doesn't include it, and `/stock/candle` is restricted for US equities. Standing warning (`reason: not_available_on_free_tier`) always present. Options when we revisit: upgrade Finnhub, add a secondary source, or accept the gap.

**Institutional holdings use Yahoo/yfinance, not Finnhub.** Finnhub's `/institutional/ownership` endpoint is Enterprise-tier and 403s on free-tier keys. The daemon uses yfinance's `institutional_holders` + `mutualfund_holders` DataFrames instead. Consequences:
- `cik` is null on every holder (yfinance doesn't expose CIK).
- `portfolio_percent` (% of the holder's portfolio) is unavailable. `percent_of_shares_held` (% of the float held by this holder) is populated instead — different metric, don't conflate.
- `num_quarters >= 2` is rejected — yfinance returns only the current snapshot.
- Per-holder `qoq_pct_change` + derived `shares_change_qoq` come from yfinance's `pctChange` column and give real QoQ signal.

**13F data is stale by design.** Filings are due ~45 days after quarter-end; `as_of_quarter` + `reported_at` + `latest_filed_at` let Abelard weight staleness.

**`detect-institutional-changes` cannot detect `new_positions` or `closed_positions`.** The Yahoo snapshot doesn't include prior-quarter holders, so a fully-exited holder is invisible and a genuinely-new holder can't be distinguished from "always held, just moved into the top-100". Standing `insufficient_history` warning documents this. `increased_positions` and `reduced_positions` work fine via per-holder pctChange. Persistent snapshot storage would fix this — deliberately not implemented.

**Top-100 cap on `detect-institutional-changes`.** Holders outside the top-100 in yfinance's return are invisible to the diff. Acceptable for monitoring; use the deep-read `fetch-institutional-holdings` on a specific ticker if a miss is suspected.

**No section-specific extraction on `fetch-sec-filing`.** 10-K HTML is too heterogeneous for reliable anchor detection. Use `--offset-chars` + `--max-body-chars` for byte-level pagination; Abelard locates sections within the returned text.

**10b5-1 plan flag is not exposed by Finnhub's insider-transactions endpoint.** `detect-insider-activity` is buys-only by design — sales without 10b5-1 context are low signal.

## Architectural principles (non-negotiable)

- Scripts execute. The LLM (Abelard) judges.
- Event-driven, never polling.
- Structured JSON out, no prose.
- Fail loudly — no fake data, no silent empty successes.
- No credentials in logs; secrets are redacted by `http_client` URL scrubbing and `config.configure_logging`.

## Deployment

Target: always-on Mac mini (Apple Silicon) running under OpenClaw's non-main
sandbox. Migration path:

1. `git clone` or `rsync` this tree to the Mac mini.
2. `pipx install .` (or `pip install` inside a venv).
3. Export required env vars (`FINNHUB_API_KEY`, `EDGAR_USER_AGENT`).
4. Drop `SKILL.md` into the OpenClaw workspace's `skills/research-daemon/`
   directory. OpenClaw's runtime handles the rest — the skill manifest
   declares `research-daemon` as the required binary, points Abelard at
   the subcommands, and Abelard consumes the JSON envelopes on stdout.
5. The included `Dockerfile` produces a minimal runtime image; use it if
   OpenClaw invokes daemons via containers rather than via the host PATH.

## OpenClaw skill manifest

See [`SKILL.md`](SKILL.md) for the agent-facing skill definition. Format
matches the canonical OpenClaw `SKILL.md` pattern (YAML frontmatter +
markdown instructions) — verified against reference skills in
`github.com/openclaw/openclaw/skills/` (specifically `github`, `weather`,
and `xurl`).
