# News Watch Daemon

Narrative-state engine for the OpenClaw multi-agent system. Scrapes headlines
across many sources, tags them against configurable themes, synthesizes
per-theme narratives and a cross-theme **Full Brief** with an attention pass,
and can alert on material shifts. Cron-driven; no long-running process. SQLite
is the only state. Every invocation emits one JSON envelope on stdout and logs
on stderr.

## Quickstart — one pass

From a cold start (fresh machine, DB not yet created), a single command applies
the schema, loads themes, scrapes + runs the attention pass + synthesizes, and
renders a PDF:

```bash
news-watch-daemon run --pdf /path/to/brief.pdf
```

`run` chains, in order: ensure schema (idempotent) → ensure themes (idempotent)
→ `full-brief` (scrape → attention → Pass C/E/F synthesis → JSON artifact) →
optional PDF. It aborts before any scrape/LLM spend if setup fails. The JSON
artifact's full path is printed to stderr; `--pdf`/`--out` land a PDF/JSON copy
wherever you want in the same pass.

Configuration is read from `.env` automatically (see **Configuration** below) —
no shell wrapper needed.

## Preflight

Before a run, `doctor` validates the environment read-only and reports every
problem at once — secrets, config files, writable output dirs, the
`signal-cli`/`java` binaries, DB schema, and active themes:

```bash
news-watch-daemon doctor
```

Exit 1 only on a **blocking** problem (no schema, no active themes, a missing
config file, an unwritable output dir); non-blocking gaps (a missing secret,
`signal-cli` absent) are warnings and exit 0.

## Subcommands

| Command | Purpose |
| --- | --- |
| `run [--pdf F] [--out F] [--no-scrape] [--window-hours N] [--quiet\|--json-only]` | One-pass cycle: ensure schema + themes, then full-brief (+ optional PDF). |
| `full-brief [--pdf F] [--out F] [--no-scrape] [--window-hours N] [--quiet\|--json-only]` | Assemble the composite brief (Pass C synthesis + Pass E attention + convergence + frequency diagnostic + Pass F footprint + cost). Writes a JSON artifact; renders text/JSON/PDF. |
| `read-brief <path> [--pdf F]` | Reload a persisted brief artifact and re-render it (text or PDF). No scrape/LLM/DB write. |
| `scrape` | One scrape sweep of all enabled sources; chains the attention pass as a follow-on. |
| `synthesize [--theme T] [--window-hours N] [--dry-run]` | Per-theme narrative synthesis (event-trigger over all active themes, or forced for one). |
| `attention [--dry-run] [--top-candidates-limit N]` | One standalone ATTENTION cycle (word-frequency counter → threshold gate → per-term LLM). |
| `doctor` | Preflight-check env, paths, deps, and DB state. |
| `status` | DB schema version, component heartbeats, per-source health. |
| `themes list` / `themes load` | Inspect / upsert the theme registry from `themes/*.yaml`. |
| `headlines recent [--theme T] [--ticker S] [--hours N] [--limit N]` | Inspect ingested headlines. |
| `briefs list [--theme T] [--limit N]` / `briefs show <id>` | Inspect archived briefs. |
| `proposals list` / `show <id>` / `approve <id> [--dry-run]` / `reject <id> [--reason R]` | Review drift-watcher keyword proposals. |
| `alert-sink test [--message M]` | Verify the configured alert transport. |
| `trigger-log tail [--limit N]` | Inspect the append-only trigger-gate log. |
| `translate [--source S] [--limit N] [--dry-run]` | Manual Pass F translation pass over pending non-English rows. |
| `db init` / `migrate` / `backfill-language` / `backfill-translation [--dry-run]` / `retag [--dry-run]` | Database administration. `retag` re-evaluates existing headlines against current theme configs (additive, idempotent); run it after any matcher/keyword change. |

## Configuration

`.env` is loaded in-process at startup (`config.load_env_file`): a bare
`news-watch-daemon <cmd>` picks it up with no wrapper. Real environment
variables always win over the file; set `NEWS_WATCH_NO_ENV_FILE=1` to disable
auto-load, or `NEWS_WATCH_ENV_FILE=/path/.env` to point elsewhere.

`NEWS_WATCH_DB_PATH` (absolute) is the only **required** variable. Everything
else is optional with a sensible default — see [`.env.example`](.env.example)
for the full list, defaults, and the degrade behavior of each source/secret.
`doctor` reports which optional pieces are missing.

Alerts dispatch via `signal-cli` (which needs a Java runtime). If it isn't
installed the daemon degrades cleanly — briefs are still archived and surface in
the next `full-brief`; only the push is skipped. `doctor` flags its absence.

## Invocation contract

- Exactly one JSON envelope per invocation on stdout; logs/warnings/tracebacks
  on stderr.
- Exit 0 iff `status == "ok"`. `full-brief`/`run` additionally use exit 2 when
  the brief assembled but a primary path (scrape / Pass C / Pass E) failed.

## Tests

```bash
pip install -e .[dev]
pytest -q
```

All tests are hermetic — no network, no LLM calls, and the real `.env` is never
read during the suite.
