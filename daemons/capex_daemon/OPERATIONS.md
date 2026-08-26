# Capex Daemon — Operations (CD-OPS1)

**INSTALLED on Basilic 2026-08-22** as launchd `com.abelard.capex`, 23:40 America/New_York (BASILIC_MANUAL 12). What follows described the pre-install state: the daemon is schedulable, and installing the cron/launchd entry was Mando's
machine and Mando's hands; this document is the hook and nothing more.

Target host ruled 2026-08-14: **BASILIC**.

## The nightly command

```bash
cd /path/to/Abelard/daemons/capex_daemon && ./.venv/bin/python -m capex_daemon scan
```

Add `--json` for the full result object, `--no-render` to skip chart regeneration, `--outdir` to
place artifacts somewhere other than the state home.

Requires `EDGAR_CONTACT` in the environment or in `daemons/capex_daemon/.env` — SEC requires a
declared User-Agent contact and `config.edgar_contact()` fails loud rather than sending a blank one.

## What a night looks like

**Most nights: nothing.** Filings arrive in waves around period ends, so the common case is

```
[capex-scan] no-op: 30 issuers checked, none with a new filing since watermark
```

exit 0. That is measured, not aspirational — a second run immediately after a full ingest produces
exactly this line.

**A night with filings:**

```
[capex-scan] updated: 3 of 30 issuers had new filings: MSFT, META, WULF
```

Only affected issuers are refetched and re-derived. Chart artifacts regenerate **only** when the
panel actually changed, because a bucket subtotal and its concentration share are whole-panel
products and cannot be computed from the changed issuers alone (E14).

**Exit status is the alert signal.** `0` on a clean run *and* on a clean no-op; **non-zero only when
something broke**. A nightly slot can alert on exit status alone without parsing output.

## Cost per run

Zero LLM calls — this daemon has none anywhere (E2). EDGAR requests are bounded: one submissions
index per universe member (30), plus one companyfacts document per *affected* issuer. A no-op night
is 30 requests; a heavy filing night is ~60. All paced at the standing 0.15s floor, well inside
SEC's 10 req/s policy.

## Idempotency, and why it holds

Watermarks are per-issuer, keyed `scan:<cik10>`, holding the newest **filing date** ingested for
that issuer — a content-derived value, never `now()` (E12). Three properties, each covered by a test:

- A watermark **only moves forward**. Backwards writes and equal-value writes are refused.
- A **zero-item run leaves it exactly where it was.**
- A **failed refresh does not advance it.** The watermark is a claim that data was ingested; moving
  it after a failure would silently skip that filing forever.

## Placement alongside the existing nightly work

Smart Money runs at 22:30. Capex is quarterly-cadence data with a daily freshness check, so it has
no competition for a slot and no ordering dependency on SM — it reads SEC directly and shares no
state. Anywhere after 22:30 is fine; later is marginally better, since SEC's own dissemination
settles through the evening.

## Basilic migration note

Basilic is the scheduling surface the other daemons are moving to, so this hook is written to be
**scheduler-agnostic**: one command, no arguments required, exit-status-signalled, no wrapper
script, no state outside the daemon's own DB. Whatever Basilic wants to invoke — cron line, systemd
timer, or its own job definition — the payload is the single command above.

Two things a Basilic job definition should carry:

1. **A timeout.** A full ingest of 30 issuers is a couple of minutes at the pacing floor; anything
   past ~15 minutes means EDGAR is degraded, and the run should be killed rather than left hanging.
2. **`EDGAR_CONTACT` in the job environment**, or the run fails loud on the first request — which is
   the intended behaviour, but is a confusing first failure if the variable is simply missing.

No log rotation is needed: the scan writes one line to stdout and its state to SQLite.

---

## Dashboard failure modes (CD-DASH1 P1, catalogued 2026-08-24)

`com.abelard.capex-dash` serves the snapshot read-only on **`100.106.84.115:8788`**
— the Tailscale address only, never `0.0.0.0`. Each mode below was either
observed during the install or is enforced by a code path that has a test.

### Snapshot missing — REFUSE (503)

`_read_only_snapshot()` returns `None` when no scan has ever run, and every
route answers `503 no snapshot yet — run capex-daemon scan`. Refusing is right
here: there is no data, and a page rendered from nothing would invite a reader
to conclude the panel is empty rather than unbuilt.

### Scan not running — SERVE, with a banner

**Measured on the wrong quantity first, and the correction matters.** The banner
originally compared `snapshot.generated_unix` against 36h. But most nights are
no-ops *by design* — nothing filed, nothing to rebuild — so a perfectly healthy
daemon serves a snapshot days old. After two clean consecutive nightlies the
dashboard was telling readers "the nightly scan has not completed", while
`scan.log` showed it completing at 03:40 UTC on both nights.

That banner would have been permanently lit on a quarterly-cadence panel, which
is worse than none: it trains the reader to ignore it.

Staleness is now measured on `meta_kv['last_scan_unix']`, stamped by every
completed cycle *including no-ops*, because "it ran" and "it changed something"
are different facts. The snapshot's own stamp is still shown — when the panel
last changed is what a reader needs to interpret the numbers. `/health` reports
`hours_since_scan` and flips `ok` to false, so the condition is machine-checkable
without scraping HTML.

Serving beats refusing: stale history is still history, and withholding it
because a cron slot was missed helps nobody.

### Port collision — the launcher dies loudly, launchd retries

Observed live while restarting the service: a second bind to 8788 raises
`OSError: [Errno 48] Address already in use` and the process exits non-zero.
With `KeepAlive` and `ThrottleInterval 10` launchd restarts it every 10s, so a
genuine collision shows as a **restart loop with a repeating traceback** in
`dashboard.err.log` rather than as a silent outage.

Diagnose with `lsof -nP -iTCP:8788 -sTCP:LISTEN`. Note that `launchctl list`
showing a PID plus a `-15` in the status column is NORMAL after any restart —
that is the previous instance's SIGTERM, not the current one's health. Check the
socket, not the status column.

### No Tailscale address — REFUSE TO START

`scripts/run_dash.sh` resolves the Tailscale IPv4 and exits 1 if there is none,
rather than falling back to a broader bind. A dashboard that quietly becomes
reachable from somewhere it should not be is worse than one that is down. This
also means a wedged Tailscale (see BASILIC_MANUAL §2, the NordVPN kill-switch
interaction) presents as a dashboard restart loop with a one-line reason.

### launchd death

`KeepAlive true` restarts on crash. A job that is loaded but not running shows
`-` in the PID column of `launchctl list | grep capex-dash`. Reload with
`launchctl unload <plist> && launchctl load <plist>`; force a restart with
`launchctl kickstart -k gui/$(id -u)/com.abelard.capex-dash`. The plist is
host-specific at `~/Library/LaunchAgents/`; the reference copy lives in
`deploy/`, and only a plist edit needs a reload — a code change needs `git pull`
and a restart.

---

## On-command verbs (CD-DASH2, 2026-08-25)

**Files may be automated. Outward sends may not.** Two things run on a schedule
and two run only when Mando says so. The split is enforced by which jobs exist,
not by anyone remembering the rule.

### Scheduled (files only)

| job | slot | what it does |
|---|---|---|
| `com.abelard.capex` | 23:40 | nightly scan; zero LLM calls |
| `com.abelard.capex-dash` | always-on | serves the snapshot read-only |
| `com.abelard.queue-digest` | 06:00 | writes a dated digest; **sends nothing** |

`queue-digest` may hold a schedule because it *cannot* send: `run_digest` is
unreachable from `send_telegram`, `run_dispatch` and `requests`, proved over the
module call graph in `abelard_queue/tests/test_digest_cannot_send.py` — with the
control assertion that `run_dispatch` MUST reach a send, since a check that
cannot detect the bad case proves nothing about the good one.

Its wrapper maps exit 2 to 0: a non-empty queue is this job's normal steady
state, and letting launchd record that as failure makes a real failure
indistinguishable from a working Tuesday.

### On command only

**News Watch** — ruled off-schedule by Mando 2026-08-25. It spends real LLM
money, so it spends it when he says. `com.abelard.news-watch` is unloaded and
absent from `launchctl list`; the plist stays in
`daemons/news_watch_daemon/deploy/` as the on-command definition.

```bash
ssh wafflehaus@basilic 'cd ~/Code/Abelard/daemons/news_watch_daemon && \
  ./.venv/bin/news-watch-daemon run --quiet'
```

Cost: the supervised first run was **$0.5339**, but that absorbed four weeks of
backlog (2,578 → 4,085 headlines) after the corpus sat still since Jul 29. A
run over a normal 24h window should be far lower. **The steady-state figure is
still unmeasured** — the next commanded run establishes it, and until then no
number here should be quoted as the per-run cost.

**Queue dispatch** — the outward verb. No schedule, and it will not acquire one.

```bash
ssh wafflehaus@basilic 'cd ~/Code/Abelard/abelard_queue && \
  ./.venv/bin/abelard-queue dispatch'          # send items already marked push
ssh wafflehaus@basilic 'cd ~/Code/Abelard/abelard_queue && \
  ./.venv/bin/abelard-queue triage --no-haiku' # rules-only, free; writes decisions
```

`triage` writes a decision onto every row it touches, so it is an on-command
verb too even though it sends nothing — a decision written before the item was
read is a review pre-empted. `--no-haiku` keeps it free and mechanical; without
it, undecided items go to a cheap LLM tier.

### Diagnostic traps — things that look like faults and are not

Catalogued 2026-08-26 after a triage that found no fault. Both of these were
mistaken for outages during real diagnosis, so they are written down beside the
genuine failure modes.

**`curl 127.0.0.1:8788` refusing connection is CORRECT.** The service binds the
Tailscale address only. A loopback connect that *succeeds* would mean it had
bound a wildcard, which is the posture we forbid. So:

```
curl: (7) Failed to connect to 127.0.0.1 port 8788 after 0 ms: Couldn't connect to server
```

is the security property working, not the service being down. Test the service
on the address it actually binds:

```bash
curl -sS -m 5 "http://$(tailscale ip -4 | head -1):8788/health"
```

This is the single likeliest misdiagnosis, because loopback is the reflex check
and it is guaranteed to fail here.

**A PID beside a `-15` in `launchctl list` is normal.** Column 2 is the LAST
exit status, which after any restart is the previous instance's SIGTERM — not
the health of the process currently running. `com.abelard.capex-dash` showed
`79813  -15` while serving correctly for 25 hours with empty logs. Check the
socket (`lsof -nP -iTCP:8788 -sTCP:LISTEN`) and the process start time
(`ps -o lstart,etime -p <pid>`), never the status column alone.

### Untested: behaviour across a reboot

**The Tailscale-bind boot race is unexercised, not disproven.** `run_dash.sh`
resolves the Tailscale IPv4 and exits 1 if there is none, so a launchd start
that beats the interface up should fail, be restarted by `KeepAlive` after
`ThrottleInterval` 10s, and self-heal within seconds. That is the design intent
and it has never actually happened: Basilic has been up 41 days and the job was
installed 2026-08-24, so it has served continuously and never started from cold.

The signature to watch for, should it ever occur:

```
OSError: [Errno 49] Can't assign requested address
```

with `run_dash: no Tailscale IPv4 address - refusing to start` as the earlier,
cleaner variant when the CLI is up but the interface is not. Either presents as
a restart loop in `dashboard.err.log`, never as a silent outage.

If it does occur, the ruled remedy is bind-retry with backoff inside the server
(retry for a bounded window at startup rather than dying) — **never** a
`KeepAlive` crashloop as the mechanism, and **never** binding `0.0.0.0`.
