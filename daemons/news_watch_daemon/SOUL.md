# News Watch Daemon — SOUL.md

The operational identity of the News Watch Daemon: what it is, what it
deliberately is not, where it writes, and the discipline that keeps it
trustworthy.

## What it is

A narrative-state engine. It scrapes headlines from Finnhub, RSS feeds,
and Telegram channels; tags each headline against Mando's themes
(`themes/*.yaml`); clusters near-duplicate wire variants into single
logical events; and — when a trigger fires — calls Claude Sonnet to
synthesize a `Brief`. Each Brief is archived to disk in
`<archive>/YYYY-MM/<brief_id>.json` and, if it crosses the materiality
gate's threshold and survives dedup against recent briefs, dispatched
via the configured `AlertSink` (Signal Note-to-Self in production).

A separate Haiku call (the drift watcher) scans untagged headlines for
recurring patterns and proposes new keywords for existing themes.
Proposals land in `pending.json`; Mando reviews them via
`proposals approve|reject` and the approved keyword is appended to the
target theme's YAML in place — via ruamel.yaml, with rollback on
post-mutation validation failure.

The daemon is **not** the user-facing layer. Abelard is. The daemon
emits structured JSON envelopes; Abelard interprets. Scripts execute,
the LLM judges — and that judgment, when it runs in the daemon, is
always under a hard schema (Pydantic `Brief`) and always inside a gate
(trigger + materiality + dedup).

## What it deliberately does NOT do

- It does not predict prices or recommend trades.
- It does not paraphrase or summarize headlines outside the synthesis
  prompt's grounded-event contract (hard rule: don't introduce events
  from training data).
- It does not retry failed Sonnet calls. SDK-level errors bubble up
  and surface in the envelope.
- It does not auto-apply drift proposals. Every theme-keyword change
  passes through Mando's `proposals approve`.
- It does not mute itself. If the materiality gate suppresses a brief,
  that's a *decision the gate made*, not silence — every gate decision
  is recorded in `trigger_log.jsonl` for §14 calibration review.
- It does not retain LLM context between calls. Prompt caching makes
  the prefix cheap; it does not make the daemon stateful.

## Write surfaces — the discipline

**Two write surfaces require extra care.** They are deliberately
greppable, atomic, and reversible.

### 1. SignalSink (external)

`src/news_watch_daemon/alert/signal_sink.py`. The daemon's only
external write — it composes a brief into a Signal message and
invokes `signal-cli send --note-to-self`. Defenses:

- `_assert_destination_allowed` is a named, greppable function that
  every dispatch path flows through before any subprocess invocation.
  It refuses any value other than the configured allowed destination.
- `tests/test_alert_signal_sink_readonly.py` is a paranoid grep test
  asserting no recipient identifiers (phone numbers, group IDs) appear
  anywhere in the module's text. Future additions of such literals
  would have to deliberately update the grep test, surfacing the
  architectural change at review time.
- Single retry on transient failure. No retry storm. Failures surface
  via `DispatchResult.success=False`; the sink never raises from
  dispatch.

### 2. theme_mutator (internal config — gated)

`src/news_watch_daemon/synthesize/theme_mutator.py`. Approved drift
proposals append a keyword to a theme YAML's `primary` / `secondary` /
`exclusions` list. Although gated through `proposals approve` (which is
gated through Mando), this is a real write surface and must be treated
with the same care as SignalSink — a bad mutation breaks the keyword
regex pipeline for the entire daemon.

Defenses (three layers):

- Cross-tier dedup: the mutator refuses to add a keyword that already
  exists in any of primary/secondary/exclusions, regardless of target
  tier. Drift's own filter is a fourth layer below this one.
- Round-trip via `ruamel.yaml`: comments, the multi-line `brief: |`
  block, blank lines, and field ordering all survive mutation.
- Post-write re-validation via `load_theme`. If validation fails (an
  unlikely edge case but possible if ruamel ever produces an output
  Pydantic refuses), the mutator restores the original file bytes and
  raises `ThemeMutationError`. Mando never sees a half-broken YAML.

The two surfaces above are the ones to grep-audit before merging any
change that touches `alert/` or `synthesize/theme_mutator.py`.

### Other write surfaces (routine internal state)

- SQLite at `$NEWS_WATCH_DB_PATH`: headlines, tags, themes, source
  health, daemon heartbeats. Atomic via transactions; idempotent
  schema migrations.
- Brief archive at `$NEWS_WATCH_BRIEF_ARCHIVE`: append-only JSON files
  partitioned by YYYY-MM; atomic writes via tmpfile + `os.replace`.
- Trigger log at `$NEWS_WATCH_TRIGGER_LOG`: append-only JSONL, never
  rotated. Calibration value compounds.
- Proposals store at `$NEWS_WATCH_PROPOSALS_PATH`: `pending.json`
  (atomic rewrite) + `resolved.jsonl` (append-only audit).
- TelegramBotSink (alternative external sink): same paranoid-grep
  discipline as SignalSink; not currently wired in production.

## Architectural invariants

- **Word-boundary regexes for every theme keyword.** Substring matches
  (e.g. `mica` matching inside `chemical`) are a fail loud, not a fail
  silent. The Pass C Step 1 fix wraps every `re.escape(keyword)` in
  `\b...\b`.
- **Brief schema is the contract.** Anything Sonnet returns is parsed
  through `Brief`; out-of-schema events are an aggregated
  `SynthesisError`, not a silent drop. Same for drift: every Haiku
  proposal passes `DriftProposal.model_validate` with orchestrator-
  minted `proposal_id` so Haiku can never spoof an existing ID.
- **Cache prefix is byte-stable.** `SYSTEM_PROMPT` constants in
  `synthesize/prompt.py` and `synthesize/drift_prompt.py` are module-
  level strings. The build-time test
  `test_prompt_schema_lists_brief_event_fields` enforces that the
  schema example in the prompt stays aligned with `Brief.Event`.
- **Channel-name discipline.** `AlertSink.channel_name` and
  `DispatchResult.channel` are free strings; `Brief.dispatch.channel`
  is the closed `Literal["signal", "telegram_bot"]`. Production sinks
  return the exact literal; mapping happens at the orchestrator.
- **Thinking disabled on both LLM call paths.** Initial design used
  adaptive thinking per the claude-api skill's recommendation for
  "anything remotely complicated." Live smoke #3 (2026-05-14) showed
  adaptive thinking consumes the entire output budget on a
  structured-JSON task — model emits only thinking blocks, no text.
  Both synthesis and drift now run `thinking={"type": "disabled"}`:
  the judgment lives in the prompt (materiality tiers, hard rules,
  output schema), not in opaque reasoning. Reinstate with explicit
  `budget_tokens` (capped well below max_tokens) post-calibration if
  output quality warrants the cost. The `effort` parameter is also
  omitted — it errors on Haiku 4.5 per the same skill snapshot.
- **Model-ID source of truth: the `claude-api` skill, not the brief.**
  Step 9 pinned `claude-sonnet-4-6` (the brief specified "Sonnet 4.7",
  which doesn't exist as a public model). When `claude-api` reports a
  newer current ID, update `synthesis_config.yaml`'s `default_model`
  + the six theme YAMLs' `synthesis.model` + `theme_config.Synthesis`
  default. The comment in `synthesis_config.yaml` documents the
  rationale.

## Failure modes

- **ANTHROPIC_API_KEY unset.** `synthesize` (without `--dry-run`)
  returns an error envelope pointing at the missing key. Other CLI
  surfaces work without it.
- **Theses doc unavailable.** Synthesis runs the no-theses prompt
  variant (single cache breakpoint instead of two) and records a
  WARN in `synthesis_metadata.theses_doc_warning`. Briefs without
  thesis_links are still material-shape; the absence is non-silent.
- **Trigger gate suppresses.** Returns `synthesis_run=False` with the
  gate's reason. No archive write; one trigger_log entry.
- **Materiality gate suppresses.** Brief is archived with
  `dispatch.alerted=False` and `dispatch.suppressed_reason=<reason>`.
  No `AlertSink.dispatch` call. Archive value compounds for §14.
- **Sink construction fails.** The brief is archived with
  `dispatch.alerted=True` (gate said dispatch), but the
  `dispatch_result` payload in the CLI envelope records the
  construction error. Operator can fix sink config and re-dispatch.
- **Sink dispatch fails (transport).** Single retry on signal-cli /
  Bot API transient failures; on second failure, `DispatchResult.
  success=False` with stderr surfaced. The brief is re-written with
  `dispatch.alerted=False` and `suppressed_reason=dispatch_failed:...`
  so audit shows the gate said yes but the wire didn't.
- **Corrupt brief in archive.** `briefs list` surfaces it as one
  `parse_error` warning, `data_completeness=partial`, and keeps
  going. Readable briefs still return.
- **Theme YAML mutation produces invalid YAML.** Post-write
  validation fails → mutator restores original bytes → raises
  `ThemeMutationError`. Pending proposal stays for retry/reject.

## Operational tasks

```bash
# Initialize the DB schema.
news-watch-daemon db init

# Load theme YAMLs into the registry.
news-watch-daemon themes load

# Single scrape sweep.
news-watch-daemon scrape

# Dry-run a synthesis cycle (no LLM call).
news-watch-daemon synthesize --dry-run

# Real synthesis cycle (requires ANTHROPIC_API_KEY).
news-watch-daemon synthesize

# Force a synthesis for one theme (bypasses trigger gate).
news-watch-daemon synthesize --theme us_iran_escalation

# Verify the alert path end-to-end without running synthesis.
news-watch-daemon alert-sink test

# Review drift proposals.
news-watch-daemon proposals list
news-watch-daemon proposals show <proposal_id>
news-watch-daemon proposals approve <proposal_id>
news-watch-daemon proposals reject <proposal_id> --reason "noisy"

# Read recent briefs + headlines (also what Abelard calls).
news-watch-daemon briefs list --limit 30
news-watch-daemon briefs show <brief_id>
news-watch-daemon headlines recent --ticker NVDA --hours 24

# Inspect trigger gate history.
news-watch-daemon trigger-log tail --limit 20
```

## Test discipline

Every commit on `main` passes the full pytest suite. Categories:

- **Hermetic unit tests** for every module under `synthesize/`,
  `scrape/`, `alert/`. No network, no real LLM, no real signal-cli.
- **Paranoid grep tests** for the two external write surfaces
  (`test_alert_signal_sink_readonly.py`, `test_alert_telegram_bot_sink_readonly.py`).
  These assert that no recipient-identifier literals appear in the
  source — architecture is the safeguard.
- **CLI envelope contract tests** assert that every leaf produces
  exactly one JSON envelope on stdout, nothing else, with the
  expected status / data_completeness / warnings shape.
- **Smoke runners** under `tools/synthesis_smoke.py` and
  `tools/drift_smoke.py`. These hit the real Anthropic API and are
  manual — never wired to CI. They're the bridge between hermetic
  testing and the live-smoke procedure (Step 16).

## Maintenance notes

- **When you add a new write surface**, add it to the Write Surfaces
  section above. Decide if it needs the SignalSink-tier paranoid grep
  discipline, or if it's routine internal state.
- **When you add a new env var**, document it in
  `src/news_watch_daemon/config.py`'s module docstring, in
  `.env.example`, and in this file's Operational Tasks if it changes
  the run shape.
- **When you change `SYSTEM_PROMPT` (synthesis or drift)**, the cache
  prefix invalidates. The prompt-schema-alignment test catches the
  most common regression (schema-example drift from `Brief.Event`).
- **When the `claude-api` skill reports a new model ID**, update the
  six theme YAMLs + `synthesis_config.yaml` default + the Pydantic
  default in `theme_config.Synthesis`. Treat the bump as a real
  change, not a typo fix — write a commit message that names the
  model ID and links any behavior delta.
- **When you build a new CLI subcommand**, register it in `cli.py`'s
  `_NESTED_DEST` + `HANDLERS`, update `test_cli_argparse.py`'s
  `ALL_LEAVES` + top-level command set, and add a dedicated
  `test_cli_<name>.py`.
