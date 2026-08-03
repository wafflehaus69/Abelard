# Deploy — M10-D dossier scan on Basilic (scheduled)

The nightly cycle for the intelligence product (M10-D §3.5). Read-only over the L2
tape; writes only to the dossier store. **No execution surface, no EV, no trade
signal** — an alert is a pointer to a dossier for human review.

`consensus dossier run` does one full cycle: scan the recent L2 window → persist
footprints to the store (capture-wide) → stamp resolved outcomes onto prior dossiers
(the §6 labeling job) → evaluate alert thresholds. It emits a JSON envelope with
`--json` (daemon convention).

## Prerequisite — the key must be resolvable

Factor F's Etherscan fallback and the funding/CEX enrichment read
`ETHERSCAN_API_KEY` from the environment, and the CLI loads it via `load_dotenv()`
**relative to the current working directory**. So the plist MUST set
`WorkingDirectory` to the consensus dir, or the key is invisible even though
`consensus/.env` exists — a silent degradation, not an error (the scan just runs
without the elevator and reports funding as unresolved).

`~/Code/Abelard/consensus/.env` must contain `ETHERSCAN_API_KEY=...` (mode 600,
git-ignored).

## launchd job

Write `~/Library/LaunchAgents/com.consensus.dossier.plist` (absolute paths only —
no `~` inside a plist):

```xml
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN"
  "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key><string>com.consensus.dossier</string>
  <key>ProgramArguments</key>
  <array>
    <string>/Users/USERNAME/Code/Abelard/consensus/.venv/bin/python</string>
    <string>-m</string><string>consensus.cli</string>
    <string>--json</string>
    <string>dossier</string><string>run</string>
  </array>
  <key>WorkingDirectory</key><string>/Users/USERNAME/Code/Abelard/consensus</string>
  <key>StartCalendarInterval</key>
  <dict><key>Hour</key><integer>23</integer><key>Minute</key><integer>30</integer></dict>
  <key>RunAtLoad</key><false/>
  <key>StandardOutPath</key><string>/Users/USERNAME/Code/Abelard/consensus/data/dossier.launchd.out</string>
  <key>StandardErrorPath</key><string>/Users/USERNAME/Code/Abelard/consensus/data/dossier.launchd.err</string>
</dict>
</plist>
```

Load it:
```bash
launchctl load ~/Library/LaunchAgents/com.consensus.dossier.plist
```

Daily at 23:30 local. A 24h lookback with a daily cadence means consecutive runs
see overlapping windows; that is intended and safe — the store is idempotent on
`dossier_id`, and a footprint that persists across runs pages **once** (it re-pages
only on material escalation).

`RunAtLoad` is false deliberately: loading the job should not fire an unscheduled
scan that consumes alerts.

## Verifying a run

```bash
cd ~/Code/Abelard/consensus && .venv/bin/python -m consensus.cli --json dossier run
```

Envelope fields worth checking: `status` (`degraded` means declared gaps or
enrichment errors — the scan is still valid, the gaps are recorded),
`result.stored` (inserted/updated), `result.resolutions.stamped`, `result.alerts`.

Browse and render from the store:
```bash
.venv/bin/python -m consensus.cli dossier list --min-tier-peak ELEVATED
.venv/bin/python -m consensus.cli dossier show <dossier_id>
```

## What is NOT enabled

The **cluster/coordination alert arm is HELD** (v1.16 §2.2). Measured over 14 daily
replayed scans on the live tape: 1,700 dossiers, only 13 clustered footprints
(rosters of 3–4), and **0 of 13 resolved to an actor count** — every one had a
member below the enrichment gate, so the funding mesh could not be collapsed. The
arm therefore has nothing to calibrate against and stays disabled rather than
shipping at a threshold that cannot fire, or firing on raw wallet count (which
would overstate coordination — the failure the collapse exists to prevent).
Single-wallet alerting runs normally.
