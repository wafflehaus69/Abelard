# News Watch Daemon

Narrative-state engine for the OpenClaw multi-agent system. Maintains living,
versioned narrative documents for tracked themes (e.g. `us_iran_escalation`,
`fertilizer_supply`).

## Status

Foundation pass. Skeleton + theme config + SQLite schema + CLI argparse.
No scrape, synthesis, or alert logic yet — those land in subsequent briefs.

## Architecture

Two-layer:

- **Scrape** (dumb, cheap, frequent) — pulls headlines from sources,
  deduplicates, tags with theme keywords, writes to SQLite. No LLM.
- **Synthesis** (smart, expensive, rare) — periodically reads accumulated
  headlines per theme; an LLM writes/updates a versioned narrative document
  with thesis, evidence, velocity, counter-evidence.
- **Alert** (sparing) — fires when a narrative shifts materially.
- **Query** — read-only access for Abelard.

Every layer is a CLI subcommand. Cron-driven; no long-running process.
SQLite is the only state. JSON envelope on stdout, logs on stderr.

## Invocation contract

- One JSON envelope per invocation on stdout.
- Logs on stderr.
- Exit 0 iff `status == "ok"`. Stubs return `not_implemented` → exit 1.

## Subcommands (foundation pass)

| Command | Status |
| --- | --- |
| `scrape` | stub |
| `synthesize [--theme T]` | stub |
| `alert-check` | stub |
| `themes list` | real |
| `themes load` | real |
| `theme show <id>` | stub |
| `theme history <id> [--days N]` | stub |
| `headlines recent [--theme T] [--hours N]` | stub |
| `alerts recent [--days N]` | stub |
| `status` | real |
| `db init` | real |
| `db migrate` | real |

## Env vars

See `.env.example`.

## Tests

```
pip install -e .[dev]
pytest -q
```

All tests hermetic. No network. No LLM calls.
