# CONSENSUS on Basilic — Operator Handoff (for the Polymarket Signal System agent)

**You are Claude Code working on CONSENSUS, the Polymarket winners-circle signal
system.** This document is how you reach the always-on data on **Basilic** (the
Mac mini), read and interpret the L2 tape, keep collection healthy, and change
the code safely. Everything here was verified live on 2026-07-16 during the
collector's migration to Basilic.

Read this first, then `consensus/docs/deploy_collector_basilic.md` (the deploy
runbook) and `consensus/docs/m0c_report.md` (the current verdict).

---

## 0. The ground rules you inherit (non-negotiable — spec §0)

These override any local optimization. If a change would violate one, stop and
ask Mando.

1. **NO synthesized data, ever.** Fail loud / emit `NO_DATA`; never fabricate,
   interpolate, or mock. (Rule #1's origin: a prior prototype narrated fake data
   as live.) Every gap is *declared*, never silently filled.
2. **Signal source ≠ execution venue.** You read **international** Polymarket
   data. You never trade it. Execution is Kalshi / Polymarket-US only, and only
   advisory until a compliance gate that has not been cleared.
3. **LLMs are not in the data path.** Collection, scoring, replay are plain
   Python REST/RPC. No model call decides a fill, a score, or a signal.
4. **Determinism.** Every algorithm knob lives in `config.yaml` (validated by
   `consensus/config.py`), never hard-coded. The file grows one module at a time.
5. **You never auto-commit or auto-push.** Edit → run tests → report to Mando →
   Mando reviews the diff → Mando authorizes → then commit. Stop at the commit
   boundary. (Spec §7: report, don't silently redesign.)
6. **No credentials in logs.** The collector needs no secret at all (see §4).

## 1. Current state (so you know what matters)

- **Verdict: Detector A (consensus) is NO-GO** — no current-regime tradeable
  edge. The mechanism is *dispersal*: the skilled roster is healthy but its
  wallets almost never co-participate on the same 2026 market. Full reasoning in
  `docs/m0c_report.md §4b`. CONSENSUS is viable as an **intelligence tool**, not
  a trade signal.
- **The L2 collector is the high-value always-on job.** The entire September
  confirmation pass depends on *uninterrupted* forward collection from ~July 2026
  onward. data-api only exposes the newest ~4,000 records per filter — anything
  the collector misses in real time is **unrecoverable**. Treat continuity as
  sacred: never leave the collector stopped.
- **Near-term build is Detector B** (M10 unusual-activity dossier product) — see
  `docs/m10_build_plan.md`. Detector A live pipeline (M4/M6/M7/M8) is on HOLD;
  M9 (order staging) is deferred until there is a trade signal to stage.

## 2. Reaching Basilic

```bash
ssh wafflehaus@basilic          # Tailscale MagicDNS -> 100.106.84.115
```
- **Non-interactive ssh does not source `~/.zprofile`** — for anything needing
  the brew PATH (python3, node), wrap it: `ssh wafflehaus@basilic "zsh -lc '<cmd>'"`.
  Commands that use absolute venv paths (below) don't need this.
- **Tailscale caveat (from the Orban/Windows side):** the operator box runs
  NordVPN, whose kill switch can knock Tailscale into `NoState`. If you see
  `ssh: Could not resolve hostname basilic`, run on Orban:
  `& "C:\Program Files\Tailscale\tailscale.exe" up` then `... ping basilic` to
  warm the link (first pings DERP-relay, then a direct path forms). Start
  Tailscale *after* Nord.
- Basilic runs **macOS arm64, Python 3.14.6**, user `wafflehaus`, timezone
  America/New_York.

## 3. Where the data lives (all under `~/Code/Abelard/consensus/`)

| Path | What it is |
|---|---|
| `data/l2_tape.db` | **The L2 forward archive (SQLite).** The irreplaceable asset. ~1.5 GB and growing ~0.5 GB/day. |
| `data/collector_envelopes.jsonl` | One JSON envelope per collection pass (machine-readable run log). |
| `data/collector.launchd.out` / `.err` | launchd stdout/stderr (the collector's own INFO/WARNING/ERROR logs land in `.err`). |
| `data/consensus_cache.db` | Raw-response cache for the backtests (data-api/gamma/subgraph replays read from here). |
| `config.yaml` | Every algorithm knob (see §7). |
| `.venv/` | The consensus venv (Python 3.14.6). Built fresh on Basilic — never copy a venv across hosts. |

The monorepo is at `~/Code/Abelard`; consensus is a top-level sibling of
`daemons/`. `origin/main` is the source of truth; Basilic is a checkout.

## 4. How collection happens (the launchd job)

A single launchd agent runs one collection pass every 120 s:

```bash
# Is it registered / what was its last exit code?
launchctl list | grep consensus        # -> "<pid> 0 com.consensus.collector" (0 = healthy)

# The plist:
cat ~/Library/LaunchAgents/com.consensus.collector.plist

# Reload after editing the plist (NOT needed after editing .py — see §8):
launchctl bootout gui/$(id -u) ~/Library/LaunchAgents/com.consensus.collector.plist
launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.consensus.collector.plist
```

- **No secret required.** The collector polls only Polymarket data-api + gamma
  (public, read-only). It has zero Etherscan/chain imports. `ETHERSCAN_API_KEY`
  is only for M5 enrichment, which is not deployed here.
- **Cadence:** `StartInterval` fires every 120 s regardless of how long a pass
  takes. A built-in single-instance lock (`lock_stale_minutes: 30`) makes
  overlapping passes skip safely.
- **Run one pass by hand** (emits the JSON envelope on stdout):
  ```bash
  cd ~/Code/Abelard/consensus && .venv/bin/python -m consensus.cli collect run
  ```
- **launchd is gui-domain:** it needs `wafflehaus` logged in. For unattended
  reboots, auto-login should be enabled.

## 5. Reading & interpreting the tape

### The human status view (start here)
```bash
cd ~/Code/Abelard/consensus
.venv/bin/python -m consensus.cli collect status          # human TEXT report (default)
.venv/bin/python -m consensus.cli --json collect status   # machine JSON, if you need to parse it
```
(Default `collect status` prints a text report — don't pipe the *default* to a JSON
parser; add top-level `--json` for the machine-readable form.)
Fields: tape size, `fills`, `span` (unix UTC), `markets` split by tier
(`hot / quiet / dormant`), `polls`, `declared gaps`, `unresolved strays`, and a
sample of recent gaps.

**Health signs:** `fills` climbs pass over pass; `gaps` are only the two benign
kinds below; the launchd job's last exit is `0`. Baseline from the migration:
583k fills at bootstrap → 926k within ~15 min as the backlog drained.

### The machine view
```bash
tail -1 data/collector_envelopes.jsonl | python3 -m json.tool   # last pass
```
Envelope shape: `status` (`ok`/`degraded`), `result.tape.{fills,size_bytes,
markets,polls,gaps_declared,unresolved_strays,newest_fill_ts}`, and an `errors`
array (a failed pass writes structured errors here — never a silent gap).

### Interpreting gaps — two kinds, both by design
1. **`bootstrap truncated at offset cap (pre-history in L1/L3)`** — a market's
   history predates collection and is older than the data-api offset cap. The
   deep past is gone from data-api; that's *why L2 exists*. Benign.
2. **`market window rolled past stored tape`** — during the initial backlog
   drain, a dormant market's data-api window rolled before the poller reached it.
   Benign for dormant markets.
   
**What would actually be worrying:** repeated `hi_ts` gaps on **hot** markets
(actively-trading current-regime markets) — that would mean the coverage
guarantee is failing. The market lane (hot: every 2 min) is the guarantee; the
global 4k-window lane is telemetry + hot-promotion only.

### Query the tape directly (read-only; SQLite WAL allows readers while the collector writes)
Tables (`sqlite3 data/l2_tape.db '.tables'`):

- **`l2_trades`** — the fills. Columns: `fill_key` (PK), `condition_id`,
  `proxy_wallet`, `side`, `asset`, `outcome`, `price`, `size`, `timestamp`,
  `transaction_hash`, `slug`, `parse_ok`, `lane`, `first_seen_poll`, `raw`.
  Indexed on `(condition_id, timestamp)`, `(proxy_wallet, timestamp)`, `timestamp`.
- **`l2_markets`** — tracked roster. `condition_id` (PK), `slug`, `question`,
  `tags`, `source` (`enumeration`/`stray`), `adopted_ts`, `active`, `end_date`,
  `tier` (`hot`/`quiet`/`dormant`), `hot_until_ts`, `last_polled_ts`,
  `newest_fill_ts`, `last_new_fills`, `close_seen_ts`.
- **`l2_gaps`** — `id`, `lane`, `condition_id`, `lo_ts`, `hi_ts`, `declared_ts`,
  `reason`. The honest holes.
- **`l2_polls`** — per-poll telemetry (pages, raw/new/dupe/skipped/unparsed
  records, `overlap_found`, `gap_declared`, `error`).
- **`l2_strays`** — condition_ids seen in global fills but not yet enumerated.
- **`l2_meta`** — key/value cursor + state.

Example — most-active markets in the last 24 h:
```bash
sqlite3 data/l2_tape.db "SELECT slug, COUNT(*) c FROM l2_trades
  WHERE timestamp > strftime('%s','now')-86400 GROUP BY condition_id
  ORDER BY c DESC LIMIT 10;"
```

**Prefer the code path over raw SQL for anything analytical** — the `DataLayer`
and `m0c.replay()` functions honor the tape's semantics (frozen-tape
cached==fresh, gap declaration, zero-lookahead). Raw SQL is fine for
observability; use the API when correctness of the *signal* matters.

## 6. The CLI surface (verified)

```
consensus [--config PATH] [--json] {data, collect, m0c, m0f, m5}

  data   {smoke, trades, cache-stats, positions, activity, market, kalshi, subgraph}
         smoke      -> 5/5 sources OK proves data-api/gamma/kalshi reachable
         trades     -> fills for a --market or --wallet
         subgraph   -> L1 archival tape (on-chain fill events, deep history)
  collect {run, status}         # §4, §5
  m0c    {universe, sweep}      # historical replay + parameter sweep + GO/NO-GO
  m0f    {universe, pull, score}   # Feb-28 footprint backtest (historical study on L1)
  m5     {latency-scan}            # funded->bet latency factor + FP curve (needs ETHERSCAN_API_KEY)
```
Always confirm exact flags with `... <subcommand> --help` before relying on them.
`--json` on the top level emits a machine summary; `collect run` always emits its
envelope regardless.

**Backtest run modes — this matters, learn it before re-running a powered sweep:**
- `m0c/m0f/m5` fetch **live** by default (populating `data/consensus_cache.db`), then
  `--replay` re-serves from that cache **offline** and deterministically. First run
  live once; reproduce with `--replay`.
- **`m0c sweep --resume`** is the one you'll want after a network drop. The powered
  L1 pull is a **multi-hour, network-fragile** operation (it died on a DNS drop once);
  `--resume` serves already-walked pages from the frozen-tape cache and only fetches
  the un-walked tail (`DataLayer.prefer_cache`). Without it, a re-run restarts from
  zero. On a truncated pull, `--replay` also salvages a verdict from the markets that
  *did* complete (partial markets fail cleanly and drop — no silent truncation).
- **The GO/NO-GO is regime-aware.** `m0c sweep` will report **NO-GO even when
  aggregate cells look positive** if the most-recent regime slice is empty/negative
  (`regime_decay: true` in the report) — the aggregate can otherwise mask a decayed
  mechanic. That's the current verdict's whole basis; don't read a positive `best_cell`
  as a GO. See `_sweep_decision()` in `m0c.py`.

## 7. Config (`config.yaml`) — the only place knobs live

Blocks that exist today: `meta` (regime_floor_date 2026-06-01), `logging`,
`categories`, `data_layer` (endpoints, http, cache_path, smoke ids), `collector`
(tape_path, tags, tiers, budgets), `m5`, `m0f`, `m0c`. Collector knobs you may
touch: `max_markets_per_run` (500 budget), `tiers.hot_interval_minutes` (2),
`drain_minutes` (360), `enumeration_interval_minutes` (30). **Change a knob here,
never in Python.** The `FUTURE` comment block at the bottom lists knobs that
arrive as scoring/scan/alert/unusual modules land — add them when you build the
module, not before.

## 8. Making changes safely

**The normal path (preferred):**
1. Edit in the monorepo working copy (on Orban or wherever you checkout).
2. Run the tests **in a dev checkout** — `cd consensus && .venv/bin/python -m
   pytest -q` (~199 tests; the suite is the contract — a red suite blocks the
   change). **Note: Basilic's consensus venv is runtime-only and has NO pytest**
   (it's the collector box, not the test box). If you must run tests on Basilic,
   first `.venv/bin/pip install -e '.[dev]'`; better, test in a dev checkout and
   keep Basilic lean.
3. Report the diff to Mando. **He commits and pushes.**
4. On Basilic: `cd ~/Code/Abelard && git pull` — the collector picks up new code
   on its next pass automatically (launchd re-execs `python` every 120 s, so a
   `.py` change needs **no** launchctl reload; only a *plist* change does — §4).
   Heads-up: Basilic's working tree already has a few untracked files from the
   news-watch/morning-briefs work (`scripts/email_briefs.py`, `scripts/
   morning_briefs.sh`, the `daemons/*/tools/` scripts). If those get committed on
   Orban and pushed, a Basilic `git pull` will refuse ("untracked files would be
   overwritten") until they're reconciled — expect it, don't force past it.

**Hotfix path (when you must patch Basilic before a commit):**
- `scp` the changed file to Basilic. Safe for the collector because each pass is
  a fresh `python` exec — but never overwrite a file mid-pass if a long single
  process holds it. Then reconcile with Mando so the committed tree matches.

**Key code entry points** (`consensus/consensus/`):
- `collector.py` → `class Collector` — the collection orchestrator.
- `fetching.py` → `class DataLayer` (attr `prefer_cache`: set True to replay a
  frozen tape as if live — how backtests resume without re-fetching).
- `m0c.py` → `load_market_data()`, `build_sweep_precompute()` (10–15× faster,
  outcome-identical), `replay()` (the zero-lookahead backtest core).
- `config.py` → pydantic config models. `errors.py` → `ConsensusError` /
  `DataLayerError` (the fail-loud contract).

## 9. Don't-break list

- **Never delete, move, or truncate `data/l2_tape.db`.** It cannot be rebuilt —
  data-api's 4k window means the past is gone.
- **Never leave the collector stopped.** If you `bootout` it to reload, `bootstrap`
  it back immediately. Every stopped minute is unrecoverable current-regime tape.
- **Don't add a secret requirement to the collector.** It needs none.
- **Don't migrate the old Windows tape onto Basilic.** The fresh forward archive
  is intentional (the Windows tape is gappy).
- **Don't `setup.sh --force`** anywhere in the monorepo — it wipes venvs.
- **The Windows `ConsensusCollector` scheduled task may still be running.** As of
  this handoff, Basilic and Windows could both be collecting into separate tapes.
  Basilic is the intended single source of truth; retiring the Windows task
  (`schtasks /Delete /TN ConsensusCollector /F`) is **Mando's call**, done only
  after Basilic collection is confirmed. Don't run two divergent tapes long-term,
  but don't delete his job without his go-ahead.

## 10. Reference docs (in `consensus/docs/`)

- `deploy_collector_basilic.md` — the 7-step deploy runbook (Step 7 = retire
  Windows, owner-gated).
- `m0c_report.md` — the NO-GO verdict + §4b dispersal correction.
- `m5_report.md`, `m0f_report.md` — the funded→bet latency and Feb-28 footprint
  studies.
- `m10_build_plan.md` — the near-term Detector B build (plan, not yet code).

When in doubt, the disk state and live `collect status` / `curl` output override
any inference — verify against the running system, then act.
