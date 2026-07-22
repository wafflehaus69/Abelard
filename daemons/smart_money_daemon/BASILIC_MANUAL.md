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
