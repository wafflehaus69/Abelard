---
name: news-watch-daemon
description: "Use news-watch-daemon for narrative-state queries: recent briefs Mando's daemon has produced, recent headlines filtered by theme or tracked ticker, and drift-watcher proposals awaiting review. Read-only from Abelard's perspective."
metadata:
  {
    "openclaw":
      {
        "emoji": "📰",
        "requires": { "bins": ["news-watch-daemon"] },
        "install":
          [
            {
              "id": "pipx",
              "kind": "pipx",
              "package": "news-watch-daemon",
              "bins": ["news-watch-daemon"],
              "label": "Install news-watch-daemon (pipx)",
            },
          ],
      },
  }
---

# News Watch Daemon Skill

Narrative-state engine: scrapes news, tags against Mando's themes,
synthesizes Briefs via Sonnet when triggers fire, alerts via Signal
when material. Every Abelard-facing subcommand emits a JSON envelope
on stdout; logs on stderr. Scripts execute, you judge — this skill
never interprets. Parse the JSON, then decide.

## When to use

✅ **USE this skill when:**

- Mando asks "what did the daemon brief about" / "what alerted today"
  → `briefs list` then `briefs show` on entries that look material.
- Mando asks about a tracked ticker over a time window →
  `headlines recent --ticker X --hours N`.
- Mando asks about a specific theme over a time window →
  `briefs list --theme X`; if the brief summaries don't carry enough
  context, follow with `headlines recent --theme X --hours N`.
- Mando asks "what is the drift watcher suggesting" →
  `proposals list`; drill into individual entries with
  `proposals show`.
- Mando asks for the daemon's health → `status`.
- Mando wants to see what the gate has been firing on lately →
  `trigger-log tail`.

## When NOT to use

❌ **DON'T use this skill when:**

- You need real-time intraday pricing → that's `research-daemon`
  (`fetch-quote`).
- You need SEC filings or insider transactions → `research-daemon`.
- Mando says "approve" / "reject" / "force a synthesis" — those are
  operator-facing commands (`proposals approve`, `proposals reject`,
  `synthesize`). You should surface what the daemon shows, not run
  the write surface yourself.
- Mando asks you to send him a Signal message — the daemon dispatches
  alerts; Abelard does not invoke `alert-sink test`.

## Output contract

Every invocation emits one JSON envelope on stdout:

```json
{
  "status":            "ok" | "error" | "rate_limited" | "not_found",
  "data_completeness": "complete" | "partial" | "metadata_only" | "none",
  "data":              { ... } | null,
  "source":            "internal" | "finnhub" | "telegram" | "rss",
  "timestamp":         "2026-05-13T18:04:05Z",
  "error_detail":      null | "string",
  "warnings": [
    { "field": "...", "reason": "...", "source": "...", "suggestion": "..." }
  ]
}
```

- Always parse stdout as JSON. Do not scrape prose.
- Exit 0 iff `status == "ok"` (partial completeness still exits 0).
- Check `data_completeness` and `warnings` before acting — partial
  means at least one record degraded; see warnings.
- Warning `reason` is a closed enum — pattern-match, don't parse prose:
  `not_implemented` · `upstream_timeout` · `upstream_error` ·
  `rate_limited` · `not_found` · `stale_data` · `parse_error` ·
  `missing_field` · `insufficient_new_material` · `source_disabled` ·
  `config_drift`

## Read commands

### Recent briefs (summary view)

```bash
# Last 30 briefs across all themes, newest-first.
news-watch-daemon briefs list

# Filter to one theme.
news-watch-daemon briefs list --theme us_iran_escalation

# Cap the result.
news-watch-daemon briefs list --limit 10
```

Returns compact summaries: `brief_id`, `generated_at`, `themes_covered`,
`events_count`, `max_materiality_score` (high water mark across the
brief's events), and the dispatch shape — `alerted`, `channel`,
`suppressed_reason`. Use this to scan; pull full payloads selectively.

If any brief on disk is corrupt or schema-drifted,
`data_completeness` flips to `"partial"` and a `parse_error` warning
names the affected `brief_id`. Readable briefs still return.

### Single brief (full payload)

```bash
news-watch-daemon briefs show nwd-2026-05-13T14-32-08Z-a1b2c3d4
```

Returns the full Brief object: events with materiality_score +
source_headlines + thesis_links, narrative, dispatch state,
synthesis_metadata (model_used, token counts, cache telemetry,
theses_doc availability), envelope_health.

Missing brief returns `status: "not_found"` — pattern-match on the
status, don't parse the detail string.

### Recent headlines

```bash
# Last 24 hours, any theme.
news-watch-daemon headlines recent

# By theme.
news-watch-daemon headlines recent --theme us_iran_escalation

# By tracked ticker (matches both tracked-list AND cashtag-extracted
# tickers — single filter covers both extraction sources).
news-watch-daemon headlines recent --ticker NVDA --hours 48

# By theme + window cap.
news-watch-daemon headlines recent --theme fed_policy_path --hours 72 --limit 100
```

Each headline carries: `headline_id`, `source` (plugin instance, e.g.
`telegram:bloomberg`), `publisher` (raw upstream, e.g. `Reuters`),
`headline`, `url`, `published_at`, `themes` (tag list), `tickers`
(extracted symbols), `entities`.

`--hours` clamps to [1, 168]; `--limit` clamps to [1, 500].

### Drift proposals

```bash
# All pending proposals.
news-watch-daemon proposals list

# Detail on one.
news-watch-daemon proposals show dp-2026-05-14T08-12-00Z-deadbeef
```

Each proposal: `proposal_id`, `theme_id`, `proposed_keyword`,
`suggested_tier` (`primary` | `secondary` | `exclusion`),
`evidence_count`, sample headlines, Haiku's `notes` rationale.

These are read-only from Abelard's perspective. Mando approves or
rejects via the daemon's CLI.

### Trigger-log tail

```bash
news-watch-daemon trigger-log tail --limit 50
```

Append-only JSONL of every trigger-gate decision (fire AND suppress).
Use this when Mando asks "why didn't synthesis run today" or "what
fired the last brief". Each entry: `decision` (`fire` / `suppress`),
`reason`, `themes_in_scope`, `matched_headline_ids`, window
timestamps.

### Status

```bash
news-watch-daemon status
```

Schema version, daemon heartbeats (last scrape time + duration), and
per-source health (last attempt, last success, failure counter).
If `schema_version` is 0, the DB isn't initialized — surface that to
Mando, don't run other reads.

## Patterns

### "What alerted?"

```bash
# 1. Scan recent briefs. Suppressed ones are still in the list — note
#    their `suppressed_reason` field.
news-watch-daemon briefs list --limit 20

# 2. Pull full payload for any that look material (alerted=true, or
#    high max_materiality_score even if suppressed_reason='dedup_recent').
news-watch-daemon briefs show <brief_id>
```

### "What's been happening with NVDA / LHX / [ticker]?"

```bash
news-watch-daemon headlines recent --ticker NVDA --hours 72 --limit 100
```

Tickers are extracted both from tracked-list matches (per
`tracked_tickers.yaml`) and from cashtag patterns (`$NVDA`) in the
headline text. A single `--ticker` filter catches both.

### "What's happening with [theme] this week?"

```bash
# 1. Scan briefs covering that theme.
news-watch-daemon briefs list --theme us_iran_escalation --limit 20

# 2. If the briefs don't carry enough context (e.g. all suppressed
#    below threshold), drop to the headline layer.
news-watch-daemon headlines recent --theme us_iran_escalation --hours 168
```

### "Is the daemon healthy?"

```bash
news-watch-daemon status
```

Look at:
- `schema_version` — should be ≥ 2.
- `heartbeats` — `scrape.status="ok"` and `scrape.last_at_unix` within
  the expected cadence (Mando's deploy uses a 15-minute scrape sweep).
- `source_health` — `failure_count` should be near zero for all
  active sources; spikes indicate upstream issues.

## Design principles (what the daemon does NOT do)

- **No interpretation in the read layer.** Scripts return structured
  data. You judge.
- **No real-time data.** Briefs are written at synthesis time; the
  archive is the source of truth. If Mando needs intraday prices,
  use `research-daemon fetch-quote`.
- **No write capabilities exposed to Abelard.** The daemon has two
  write surfaces (SignalSink for external alerts, theme_mutator for
  approved drift proposals). Both are operator-facing; neither is in
  Abelard's reach.
- **No silent failures.** Corrupt briefs surface as
  `data_completeness: "partial"` + a `parse_error` warning; missing
  briefs surface as `status: "not_found"`; gate suppressions surface
  in the brief's `dispatch.suppressed_reason`.

## Notes

- Every timestamp field is paired: `<name>_at_unix` (int epoch) +
  `<name>_at` (ISO-8601 UTC). Iterate over either without per-record
  type checking.
- `materiality_score` is Sonnet's 0.0–1.0 self-rating. The daemon's
  dispatch threshold is configured (`synthesis_config.yaml`,
  default 0.55). A brief with `dispatch.alerted=False` and
  `suppressed_reason="below_materiality_threshold"` means Sonnet
  wrote a brief but nothing in it cleared the bar.
- `dispatch.suppressed_reason="dedup_recent"` means events above
  threshold existed, but every one of them already appeared in a
  brief generated within the last `dedup_window_hours` (default 6).
  Mando has already seen this.
- `synthesis_metadata.cache_creation_input_tokens` vs
  `cache_read_input_tokens` shows prompt-cache hit/miss for the
  brief's underlying call. A long-running daemon will show
  predominantly read tokens; cold restarts show creation tokens.
- `briefs list` returns up to 500 summaries by default; for windows
  longer than that, paginate with `--limit` and progressively
  narrower `--theme` filters.
