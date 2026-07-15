### News Watch Daemon

**Status:** Operational. Pass C shipped (2026-05-14).
**Model tier:** Sonnet 4.6 (synthesis judgment), Haiku 4.5 (drift
detection). The model-ID source of truth is the `claude-api` skill,
not memory — when a newer model ships, update theme YAMLs +
`synthesis_config.yaml` + the Pydantic default.
**Repository location:** `daemons/news_watch_daemon/` in the Abelard
monorepo (`github.com/wafflehaus69/Abelard`).
**Doctrine:** `daemons/news_watch_daemon/SOUL.md` — read this when
making any change that touches the daemon's behavior, write surfaces,
or test discipline.
**Read interface I use:** `daemons/news_watch_daemon/SKILL.md` — the
output contract, command catalog, and usage patterns.

#### What it is

A narrative-state engine. Scrapes news (Finnhub + RSS + Telegram
channels @CIG_telegram / @bloomberg / @trading / @chainlinkbreadcrumbs),
tags against the six active themes (us_iran_escalation, fed_policy_path,
ai_capex_cycle, china_us_decoupling, russia_ukraine_war,
tokenized_finance_infrastructure), clusters near-duplicate wire
variants, synthesizes Briefs via Sonnet on trigger fire, dispatches
material Briefs to Mando's Signal Note-to-Self via signal-cli linked
device on his real phone.

#### What it produces for me

Structured JSON Briefs at `~/.openclaw/news_watch/briefs/YYYY-MM/*.json`.
Each Brief carries: clustered events with materiality scores, source
headlines with publisher/url/timestamp, thesis_links (when THESES.md
is readable), Sonnet's narrative prose, dispatch state with
suppression reasons, synthesis metadata including cache telemetry.

I read these Briefs via the daemon's CLI, not by parsing the JSON
files directly. The CLI is the contract; the file layout is the
daemon's implementation detail.

#### What it does NOT do

- Does not predict prices or recommend trades.
- Does not auto-apply drift proposals — every theme-keyword change
  passes through Mando's approval.
- Does not retry failed Sonnet calls; SDK errors surface in the
  envelope.
- Does not read filings (that's Research Daemon's scope).
- Does not read intraday prices (Price Daemon's scope, future).

#### Write surfaces (operator-facing only — not mine)

Two write surfaces in the daemon. Both are operator-facing. I do not
invoke either of them.

1. **SignalSink** — dispatches Briefs to Mando's Signal Note-to-Self.
   The daemon's only external write. Hardened with destination
   validation and a paranoid grep test.
2. **theme_mutator** — appends approved drift keywords to theme YAMLs.
   Gated through `proposals approve` (operator command). Round-trip
   safe via ruamel.yaml with rollback on validation failure.

#### My read commands

| Mando's question | My command |
|---|---|
| "What did the daemon alert today?" | `news-watch-daemon briefs list --limit 20` then `briefs show` on entries that look material |
| "What's happening with [ticker]?" | `news-watch-daemon headlines recent --ticker X --hours N` |
| "What's the daemon seeing on [theme]?" | `news-watch-daemon briefs list --theme X` then `headlines recent --theme X` if needed |
| "Why didn't the daemon alert on X?" | `news-watch-daemon trigger-log tail --limit 50` |
| "What's the drift watcher proposing?" | `news-watch-daemon proposals list` then `proposals show <id>` |
| "Is the daemon healthy?" | `news-watch-daemon status` |

The daemon's SKILL.md has the full pattern catalog. Consult it when
in doubt.

#### Operational notes

- The daemon runs continuously on Mando's always-on host (Orban now,
  Mac mini after migration).
- Brief archive at `~/.openclaw/news_watch/briefs/` is append-only
  and the source of truth — Signal is a notification copy.
- Trigger log at `~/.openclaw/news_watch/trigger_log.jsonl` is
  append-only, never rotated, never pruned. Historical record of
  what looked interesting at any given timestamp.
- ANTHROPIC_API_KEY required in daemon environment for synthesis;
  not required for the read commands I use.

#### Relationship to my doctrine

The News Watch Daemon serves the **material-not-quiet interruption
bar** in my MEMORY.md: it alerts when something material happens,
stays silent otherwise. The materiality gate is calibrated to err
toward false negatives (one missed alert) over false positives
(noise-trained dismissal of the channel).

The daemon's outputs feed my **cascade frame** (WORLDVIEW.md) — Briefs
identify events that move named theses, including thesis-breakers, so
my reasoning operates against fresh state rather than stale
assumptions.

When Mando asks "what's changed," the daemon's archive is the first
place I look.

