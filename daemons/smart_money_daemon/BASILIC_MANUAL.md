# SmartMoneyDaemon on Basilic — Engineer Access Manual

**You manage the SmartMoneyDaemon.** It runs in production on **Basilic** (the
always-on Mac mini) as a scheduled delta-scan. This manual is how you reach that
box, inspect the daemon, read its outputs, and change its code safely.
Everything here was verified live on Basilic 2026-07-22.

The daemon emits **positioning events**, never a leaderboard. It does mechanical
extraction and structured JSON out; Abelard interprets. Scripts execute, LLM
judges — no LLM calls anywhere in this daemon.

---

## 1. Ground rules (inherited — don't violate)
- **Dumb-daemon invariant.** Extracts and classifies, emits structured JSON.
  It does not interpret or dispatch — it enqueues notable events to
  `abelard_queue`; Abelard's consumer decides push or suppress.
- **Fail loud, never fake.** A blocked source is marked DEGRADED in the
  envelope. Zero events on a quiet day is SUCCESS with empty events, never an
  error, never a fabricated event.
- **No credentials in logs.** The `.env` holds live secrets (§4).
- **You never auto-commit or auto-push.** Edit -> run tests -> report to Mando
  -> he commits/pushes -> Basilic pulls. Stop at the commit boundary.

## 2. Reaching Basilic
```bash
ssh wafflehaus@basilic          # Tailscale MagicDNS -> 100.106.84.115
```
- Non-interactive ssh does not load the brew PATH. Wrap bare `python3`/`node`:
  `ssh wafflehaus@basilic "zsh -lc '<cmd>'"`. Commands using the daemon's
  absolute `.venv/bin/...` paths do not need this.
- Tailscale drops to a wedged state when the operator box's NordVPN kill switch
  engages. If ssh says `Could not resolve hostname basilic`, on Windows start
  Tailscale AFTER Nord, and if the Tailscale service is logged out restart it.
- Basilic: macOS arm64, **Python 3.14.6**, user `wafflehaus`, TZ
  America/New_York.

## 3. Where your daemon lives
Monorepo root `~/Code/Abelard` (`origin/main`). Your daemon:
`~/Code/Abelard/daemons/smart_money_daemon/`

| Path | What it is |
|---|---|
| `.venv/bin/python -m smart_money.scan` | The delta-scan entry point (Python 3.14 venv, built on Basilic — never copy a venv across hosts). |
| `smart_money/` | Package source. `scan.py` is the runtime; `scorecard.py` is periodic analysis. |
| `config/overlay.yaml` | **Mando-owned** conviction_book + watchlist. The daemon reads it, never writes it. |
| `scripts/run_scan.sh` | The launchd runner (§6). |
| `tests/` | pytest suite (8 tests). |

**Runtime state lives OUTSIDE the repo**, under `~/.openclaw/smart_money/`:

| Path | What it is |
|---|---|
| `smart_money_v0.db` | Canonical DB — trades, persons, prices, watermarks, 13F baseline, scan_events. |
| `scans/scan_<ts>.json` | One envelope per scan (sources, counts, events, queue block). |
| `logs/scan.log` | Appended per run by the wrapper. `launchd.out` / `launchd.err` are launchd-level. |

## 4. Your `.env` (secrets — never print to logs)
`daemons/smart_money_daemon/.env` (gitignored, mode 600). Keys:
`EDGAR_CONTACT` (declared in the User-Agent on all EDGAR calls),
`SMART_MONEY_DB_PATH` (absolute path to the state-home DB — set absolute, not
`~`, so launchd resolves it), `FINNHUB_API_KEY`, `ABELARD_QUEUE_DB_PATH`
(the abelard_queue sink). Inspect key names with `grep -oE '^[A-Z_]+' .env`;
never echo values.

## 5. Inspecting the daemon
```bash
cd ~/Code/Abelard/daemons/smart_money_daemon
.venv/bin/python -m smart_money.scan          # run one delta-scan (cheap, no LLM)
.venv/bin/python -m pytest -q                 # or run the test functions directly
```
Read the newest envelope:
```bash
ls -t ~/.openclaw/smart_money/scans/*.json | head -1
```
Inspect state (read-only, safe when not mid-scan):
```bash
sqlite3 ~/.openclaw/smart_money/smart_money_v0.db \
  "SELECT source, watermark_ts FROM watermarks"
```

## 6. How it runs in production
launchd job **`com.abelard.smart-money`**, once daily **22:30 America/New_York**
(after EDGAR's daily index posts), via `scripts/run_scan.sh`. `RunAtLoad` is
false — it fires only on schedule.
```bash
launchctl list | grep smart-money          # registered? last exit code (col 2)?
launchctl start com.abelard.smart-money    # trigger one run by hand
tail -8 ~/.openclaw/smart_money/logs/scan.log
```
Plist at `~/Library/LaunchAgents/com.abelard.smart-money.plist` (host-specific,
NOT in the repo). Only editing the plist needs a reload; a code change just
needs `git pull`.

## 7. The three legs and what to expect
- **Leg A congressional** — House Clerk current-year index refresh (works via
  requests). **Senate eFD is DEGRADED**: the site put a WAF on its search
  endpoint that 503s scripted clients (detail pages still work). The browser
  index-refresh adapter (Playwright) is the outstanding follow-up to un-degrade
  Senate; until then Senate new-filings are not caught. Amendment supersede
  policy is active in this path.
- **Leg B Form 4 tail** — EDGAR daily index, filtered to overlay tickers or
  insider registry entries. Open-market P/S become events; A/M/G counted only.
- **Leg C 13F** — checks each registry CIK (Aschenbrenner 0002045724) for a
  filing newer than the stored baseline. The Q1-2026 baseline is seeded, so the
  mid-August Q2 filing produces a real diff.

Watermark discipline: per-source watermarks advance ONLY on ok-with-items to
the newest ingested item's disclosure timestamp, never to now(). A 3-day
overlap window plus filing_id dedup makes re-fetch free and reruns idempotent.

## 8. Making changes safely
1. Edit in the monorepo working copy (on the operator box).
2. Run the suite: `.venv/bin/python -m pytest -q`. A red suite blocks the change.
3. Report the diff to Mando; he commits and pushes.
4. On Basilic: `cd ~/Code/Abelard && git pull`. Next scheduled run uses it.

## 9. Don't-break list
- **Don't delete or hand-edit `~/.openclaw/smart_money/smart_money_v0.db`** — it
  is the accumulated corpus, the watermarks, and the 13F baseline.
- **Don't reset the watermarks** — that re-emits the whole history as new events.
- **Don't make the daemon dispatch externally** — it enqueues; Abelard sends.
- **Don't `setup.sh --force`** — it wipes venvs.
- **Never commit `.env`** (gitignored; keep it that way).

## 10. Reference
Package source under `smart_money/`; `recon/SOURCE_VERDICTS.md` and
`recon/EFD_WAF_FINDING.md` (why Senate is degraded); the monorepo `AGENTS.md` at
the repo root. When in doubt, disk state and a fresh `scan` envelope override any
assumption — verify against the running system, then act.

## 11. SM-R1 reporting dashboard + brief (live-certified 2026-07-27)
Read-only reporting layer on top of the corpus. Both surfaces open the DB
`mode=ro` — they can never write, and there are no write endpoints.

- **Dashboard.** `http://100.106.84.115:8787` — Basilic's Tailscale IP, reachable
  from any tailnet machine, never a public interface. Five views: `/` (front page
  = sentinels, principal convergence, ownership pressure, overlay-flagged events),
  `/clusters` (buy clusters + sell context feed), `/sentinels`, `/ticker?symbol=X`.
  Filter state is in the URL (`?window=&anchor=&floor=&symbol=`); the print button
  is `GET /brief.pdf?<same params>`.
- **launchd** `com.abelard.smart-money-dash` — always-on, restart-on-crash, binds
  the Tailscale IP via `scripts/run_dash.sh`. Check: `launchctl list | grep
  smart-money-dash` (column 2 = last exit, 0 = healthy). Logs:
  `~/.openclaw/smart_money/logs/dashboard.{out,err}.log`. Restart:
  `launchctl kickstart -k gui/$(id -u)/com.abelard.smart-money-dash`.
- **Scheduled brief** `com.abelard.smart-money-brief` — 23:15 local (after the
  22:30 scan), renders the full PDF brief to
  `~/.openclaw/smart_money/scans/SMART_MONEY_BRIEF_<date>.pdf`. Referenced, never
  emailed. Force a run: `launchctl start com.abelard.smart-money-brief`.
- **Deps.** The venv needs `reportlab`, `pytest`, and the editable
  `abelard_common` (`pip install -e ../common`) — installed at cert. The query
  layer `smart_money/queries.py` is the SINGLE SOURCE OF TRUTH; no SQL lives in the
  dashboard or the brief.
- **Editorial line.** Clusters (buy and sell) are CONTEXT, never headlines, never
  alerts. The sell feed's `elevated` tint (ratio >= 3.0, >=3 sellers/yr) is a
  display cue on a ranked context feed, not a verdict. Nothing here writes to the
  alert path.

---

## 12. Capex Daemon — INSTALLED 2026-08-22

Installed by Mando's authorization 2026-08-22. Full operating notes live in
`daemons/capex_daemon/OPERATIONS.md`.

**The job** — launchd `com.abelard.capex`, **23:40 America/New_York**, via
`daemons/capex_daemon/scripts/run_scan.sh`. `RunAtLoad` false, so it fires only
on schedule and a reboot cannot masquerade as a filing night. `ExitTimeOut` 900s
encodes the fifteen-minute rule below.

```bash
launchctl list | grep capex             # registered? last exit code (col 2)?
launchctl start com.abelard.capex       # trigger one run by hand
tail -8 ~/.openclaw/capex_daemon/logs/scan.log
```

Plist at `~/Library/LaunchAgents/com.abelard.capex.plist` (host-specific).
A reference copy is in `daemons/capex_daemon/deploy/`. Only editing the plist
needs a reload; a code change just needs `git pull`.

**Verified at install**, on Basilic, not inferred from the dev box: 197 tests on
Python 3.14.6 arm64; first ingest 33 of 35 issuers in **64s** with 240
transitions backfilled and none alerted; second run a clean no-op; a
`launchctl start` run completing in **11s** at exit 0 with launchd-level stderr
empty. Basilic reproduced the dev box's figures to the decimal — panel
$603.3B TTM +84.1% over 16 matched members, supplier cross-check 53.8%, NVDA
$229.9B / MU $52.5B mapped / AMD $22.2B — from independent SEC fetches on a
different OS and Python version.

Build the venv **on Basilic** — never copy one across hosts, same rule as SM.

**Three things the job definition must carry**

1. **`EDGAR_CONTACT`.** SEC requires a declared User-Agent contact;
   `config.edgar_contact()` fails loud rather than sending a blank one. Supplied
   from `daemons/capex_daemon/.env` (gitignored, mode 600), so the plist needs no
   environment block. Missing it fails the first request — correct behaviour, but
   a confusing first failure if the file is simply absent.
2. **A timeout.** A full ingest is now **35 issuers plus a supplier harvest**
   (CD-3): five suppliers x up to 14 filing documents each, parsed for the
   dimension-qualified datacenter line that companyfacts does not carry. First
   run is a few minutes at the 0.15s pacing floor; steady state is far cheaper
   because only instances absent from the cache are fetched. Past ~15 minutes
   means EDGAR is degraded and the run should be killed rather than left
   hanging.
3. **Alert on exit status alone.** Exit `0` on a clean run *and* on a clean
   no-op; non-zero **only** when something broke. No log parsing needed.

**Slot.** No ordering dependency on SM — capex reads SEC directly and shares no
state, so it cannot race the 22:30 scan. Anywhere after 22:30 works; later is
marginally better since SEC dissemination settles through the evening.

**What a night looks like.** Most nights, nothing:

```
[capex-scan] no-op: 35 issuers checked, none with a new filing since watermark
```

That is measured, not aspirational — a second run immediately after a full
ingest produces exactly this line. Cost is ~35 EDGAR requests on a quiet night,
~70 on a filing night, and **zero LLM calls**.

**Capex venv deps.** `pip install -e ../common -e .` is sufficient: `reportlab`
is a declared dependency of the capex daemon itself (it renders the nightly
phase page). **Do not install matplotlib** — the PNG pipeline was retired
2026-08-21, and until then it was an *undeclared* module-scope import that made
`python -m capex_daemon scan` fail on a venv built from `pyproject` alone. If a
future change reintroduces a plotting dependency, it must be declared.

**First run is loud and that is correct.** It backfills the entire phase history
at once and alerts **none** of it, reporting a count instead:

```
[capex-scan] updated: 33 of 35 issuers had new filings: ... | first run: 240 transitions backfilled, none alerted
```

Rediscovering history is not news. Every later run alerts only genuine new
transitions. Those numbers are measured, from the first live run on 2026-08-21.

**`--rebuild`.** `python -m capex_daemon scan --rebuild` recomputes and
republishes the snapshot and the PDF even with nothing filed. Needed because the
snapshot is derived from code as much as from data: after a code change a
correct database can sit behind a stale published view with no filing due for
weeks to dislodge it. It does **not** re-ingest and does **not** touch
watermarks. Not for the nightly slot — this is a hand-run recovery tool.

**Don't-break note.** Watermarks are per-issuer (`scan:<cik10>`), hold a filing
date rather than `now()`, only move forward, and do not advance when a refresh
fails. Hand-editing them silently skips filings.

---

## 13. Capex dashboard + News Watch nightly — INSTALLED 2026-08-24 (CD-DASH1)

**`com.abelard.capex-dash`** — always-on, `RunAtLoad` + `KeepAlive`, mirroring
`com.abelard.smart-money-dash`. Serves the capex snapshot read-only at
**`http://100.106.84.115:8788`** — the Tailscale address only. All seven views
verified over the tailnet from a second device. `scripts/run_dash.sh` resolves
the Tailscale IPv4 and **refuses to start without one** rather than widening the
bind. Failure modes catalogued in `daemons/capex_daemon/OPERATIONS.md`.

`/health` returns `hours_since_scan` and flips `ok` false when the nightly stops,
so liveness is machine-checkable without scraping HTML.

**`com.abelard.news-watch`** — **21:30** America/New_York, ahead of
smart-money 22:30, smart-money-brief 23:15 and capex 23:40, so the evening block
runs in order without overlap. `RunAtLoad` false and **no `KeepAlive`**: the
cycle spends real LLM money and runs to completion, so a KeepAlive would restart
it the instant it finished — an unbounded spend loop wearing the same shape as
the dashboard's restart-on-crash.

Supervised first run 2026-08-24: **$0.5339** (pass_c $0.2862, pass_e $0.1921,
theme_segments $0.0556), against a ~$0.25–0.30 expectation. The overrun is
backlog, not per-run cost — the corpus had not moved since Jul 29, so one run
absorbed ~4 weeks (2,578 → 4,085 headlines). A nightly on a 24h window should
land far lower; the second run is the one that establishes the real figure.

**The evening block**

| job | slot | posture |
|---|---|---|
| `com.abelard.news-watch` | 21:30 | fire-and-finish, LLM spend |
| `com.abelard.smart-money` | 22:30 | fire-and-finish |
| `com.abelard.smart-money-brief` | 23:15 | fire-and-finish |
| `com.abelard.capex` | 23:40 | fire-and-finish, no LLM |
| `com.abelard.capex-dash` | — | always-on |
| `com.abelard.smart-money-dash` | — | always-on |

---

## 14. Price substrate — AUTHORED, NOT YET INSTALLED (PS-1 / PS-1B)

Written 2026-09-02 under ORDER PS-1B Phase D. **The plist is not loaded.** This
section describes a job that does not yet run; when it is installed, change this
heading and date it, the way §12 is dated. Reading it as a description of
something running is the mistake §13 currently invites (see the note at the end).

**What it is.** One price store, one writer, many readers —
`daemons/common/abelard_common/prices/`. Unadjusted closes are facts and are
insert-only, enforced by SQLite triggers; adjusted closes are a view, rebuilt on
demand and never cached as a fact. Corrections, fills and quarantines are
append-only overlays, so a fact never moves and the record of having doubted it
survives the doubt being resolved.

**The job** — launchd `com.abelard.prices`, **21:00 America/New_York**, via
`daemons/common/scripts/run_prices.sh`. `RunAtLoad` false. `ExitTimeOut` 3600s.
Reference plist in `daemons/common/deploy/`.

Four legs in a fixed order, worst exit code returned:

| leg | what it does | why here |
|---|---|---|
| `nightly` | appends the session's closes | first, so the store is current before anything reads it |
| `reference --no-fred` | CL=F, ^VIX, SPY, IVV, RSP, XLE | the reconciliation benchmark comes from here |
| `reconcile` | index-level check on the session just appended | needs both of the above |
| `verify -n 18` | Tiingo cross-check, 30-day rotation | last, because it is the metered leg and a quota refusal must never cost the nightly append |

**Environment.** The library takes explicit paths and never reads the
environment; `run_prices.sh` is the layer that does.

| variable | source | consequence if missing |
|---|---|---|
| `ABELARD_PRICES_DB_PATH` | set by the runner, absolute | launchd does not expand `~`; a relative path silently creates a second, empty store |
| `EDGAR_CONTACT` | `~/.openclaw/prices/.env`, falling back to the SM `.env` | SEC returns 403; `universe-sync` fails at the first request |
| `TIINGO_API_TOKEN` | `~/.openclaw/prices/.env`, mode 600 | `verify` exits 2. The other three legs are unaffected — Yahoo is primary and unmetered |

Exit codes: **0** clean · **1** something lagged, a fact changed, the vendor was
degraded, or a verification disagreed — all "look at it", not "it is broken" ·
**2** could not start · **3** the wrapper could not find the package or the venv.

**The hard dependency, and it runs the other way from the usual one.** Smart
Money at **22:30** will read the price store's freshness ledger as a precondition
once PS-1 Phase 4 lands. So prices must be current before SM starts, which is
why 21:00 and not a slot in the 22:30–23:40 block. At v1 scope the night is
~35 minutes (append ~9 min over 516 names; the verify sweep ~22 min, floored by
the 72s pace), finishing near 21:35 and clearing 22:30 with an hour of slack.
**That hour is the margin that matters** — if the universe is ever widened to the
Russell 2000, the append grows roughly 4x and the margin is what gets spent.

**Tiingo is metered and the code enforces it, not memory.** 50 requests/hour,
1,000/day, 2 GB/month, read off the account page 2026-09-02. Every call is
logged to `vendor_calls` and a sweep that would breach any meter refuses
**before the first request** — a sweep that dies at request 43 of 60 has spent
the quota and left the store half-verified. Header auth only; a token in a query
string is a test failure.

**Build the venv on Basilic** — never copy one across hosts, same rule as SM and
capex. `daemons/common/.venv` already exists there at Python 3.14.7.

```bash
launchctl list | grep prices
tail -20 ~/.openclaw/prices/logs/prices.log
cd ~/Code/Abelard/daemons/common && .venv/bin/python -m abelard_common.prices.cli status
```

**Expect exit 1, from the first night, until PS-1C.** Three MNST vendor-corruption
detections (2026-07-20, 07-23, 07-31) are unadjudicated, and an unadjudicated
corruption holds the store unclean by design. Yahoo serves 1,400 of MNST's 1,410
sessions unadjusted while declaring its 2:1 split, so the name is quarantined and
out of the panel; Mando ruled 2026-09-03 that the remedy is a source-override
mechanism (PS-1C), not correction rows. **A 1 on this job means "look at it", not
"it is broken" — and right now the thing to look at is already known.** What
would be new is a 2 (could not start), a 3 (bad wrapper or venv), a reconciliation
FAIL, or a fact-change event.

**Before it is installed** — Phase D.3 requires `universe-sync` and a full 5-year
backfill run by hand, in the foreground, on Basilic, with throughput recorded.
Do not load the plist onto an empty store: the first `nightly` would have no
history to compare against and every name would look like a first write.

**Correction to §13 while I was here.** §13 records `com.abelard.news-watch` as
installed 2026-08-24 with a supervised first run. It is **not in
`launchctl list`** on Basilic as of 2026-09-02 20:55 EDT. The run happened; the
job was never loaded. That is E32 (authored is not activated) and it is exactly
what CR-R0 R4.3 flagged. Left as-is rather than edited away, because the gap
between a document saying "installed" and a host saying otherwise is the finding.
