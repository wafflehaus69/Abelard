# RUNBOOK — running FDU on Basilic

Basilic is the always-on collection host and the right home for the one-time
backfill, which runs for the better part of a day. Orban is fine for `scan`,
`enrich` and `leads`, which are minutes.

**Nothing here has been executed.** The branch is unpushed and Basilic has not
been touched beyond the read-only capability checks recorded below. Deployment
waits on Mando's word.

## Host facts, verified live 2026-08-21

| | |
|---|---|
| Reach | `ssh wafflehaus@basilic` (Tailscale MagicDNS) — confirmed |
| Platform | Darwin 25.5.0 **arm64** (macOS) |
| Disk | 359 GiB free of 460 GiB |
| Abelard clone | `~/Code/Abelard`, present |
| `python3` | **3.9.6** — this is `/usr/bin/python3` |
| Homebrew python | **3.14.6** at `/opt/homebrew/bin/python3` |

**The Python gotcha, and it will bite a scheduled job.** A non-interactive
`ssh basilic 'python3 -V'` resolves to the system 3.9.6, because the homebrew
PATH is only set up by a login shell. FDU requires >=3.12. Every scheduled
invocation must use the **absolute** interpreter path, never bare `python3`.

The July 2026 host recon recorded `python3 = 3.14.6`; that was measured through
a login shell and is misleading for automation. Corrected here per [E15].

## Install

```bash
ssh wafflehaus@basilic
cd ~/Code/Abelard && git fetch && git checkout fdu-pa1-recon
/opt/homebrew/bin/python3 -m venv fdu/.venv
fdu/.venv/bin/python -m pip install -e daemons/common -e 'fdu[dev]'
fdu/.venv/bin/python -m pytest fdu/tests -q      # expect 75 passed
```

## The backfill

Sized from measurement, not estimate: 15 documents took 36.5 s wall including
14 s of deliberate pacing, i.e. **~1.5 s of fetch+parse per document**.

| pacing | per doc | 23,794 documents |
|---|---|---|
| `--delay 2.0` (default) | 3.5 s | **~23 h** |
| `--delay 1.0` | 2.5 s | **~16.5 h** |

Run it in chunks so it is resumable and interruptible. `enrich --backfill`
selects firms with no `adv_detail` row, largest AUM first, so re-running simply
continues where it stopped.

```bash
cd ~/Code/Abelard
export FDU_STATE_HOME=~/.openclaw/fdu
while :; do
  fdu/.venv/bin/python -m fdu_daemon.cli enrich --backfill --limit 500 --delay 1.5 || break
  fdu/.venv/bin/python -m fdu_daemon.cli status | grep coverage
  [ -f ~/.openclaw/fdu/HALT ] && { echo halted; break; }
done
```

Transfer is ~49 GB inbound over the run. **Nothing is written to disk** beyond
the ledger — documents are parsed in memory and dropped — so the disk cost is
the SQLite file, tens of MB.

To stop it at any point: `touch ~/.openclaw/fdu/HALT`. The loop exits at the
next chunk boundary and the current chunk stops after its in-flight document.
Remove the file to resume.

## Daily cadence, once backfilled

```bash
# launchd or cron. ABSOLUTE interpreter path -- see the gotcha above.
cd ~/Code/Abelard && \
  fdu/.venv/bin/python -m fdu_daemon.cli scan && \
  fdu/.venv/bin/python -m fdu_daemon.cli enrich --limit 300
```

~14 s for the scan, ~4 minutes for a typical day's ~66 documents.

## Before scheduling anything

1. `git worktree list` on Basilic — the clone was on `main` with an uncommitted
   mode-bit change to `daemons/smart_money_daemon/scripts/run_scan.sh` as of the
   July recon. Reconcile before checking out a branch, or the checkout complains.
2. Confirm `~/.openclaw/` on Basilic is not inside a cloud-sync tree. A sync
   client corrupts SQLite mid-write and the failure is intermittent. Orban was
   verified clear; **Basilic has not been checked** and must be before the
   ledger lives there.
3. Decide Q6 — whether a contact address is declared in the User-Agent. FDU
   currently declares none, by ruling R-PA1-2. This does not affect
   `reports.adviserinfo.sec.gov`, which serves us without one.
