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
