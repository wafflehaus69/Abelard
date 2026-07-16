# Deploy the L2 Collector to Basilic (runbook)

**Goal:** move the CONSENSUS L2 forward-collector off the fragile Windows box onto Basilic (always-on, launchd) so the **September confirmation pass** rests on uninterrupted collection. **Collector only** — no backtests, no live scan.

**Prepared by ClaudeCode for Mando to execute on Basilic.** Nothing here runs itself.

---

## 0. Decisions already made (so you don't have to re-derive them)

- **No secret needed.** The collector polls only Polymarket data-api + gamma (public, read-only). It has zero Etherscan/Polygon imports — `.env` / `ETHERSCAN_API_KEY` is **not** required. (The key is only for M5/enrichment, which is not being deployed.)
- **Fresh tape on Basilic — do NOT migrate the 5.9 GB Windows tape.** Reasons: the collector self-bootstraps (re-enumerates markets via gamma on first run); the Windows tape has declared gaps from network deaths; and the confirmation pass needs a *clean continuous* forward archive, which a fresh start on Basilic (~now) provides through mid-Sep (~60 d) to mid-Oct (~90 d). A 5.9 GB transfer buys nothing but a gappy prefix.
- **Basilic becomes the single source of truth.** Once it's confirmed collecting (Step 6), **retire the Windows scheduled task** (Step 7) so there aren't two divergent tapes.
- **Cadence unchanged:** one `collect run` every 120 s (the tiers self-throttle; market-lane is the coverage guarantee).
- **Disk:** steady-state growth ≈ **0.5 GB/day** → budget **~45 GB** for a 90-day window. Confirm Basilic has the headroom before starting.

## 1. Pull the code (already pushed)

```bash
cd ~/Code/Abelard          # or wherever the monorepo lives on Basilic
git fetch origin && git checkout main && git pull   # should land at d4c5f0e or later
```

## 2. Python env + install

```bash
cd ~/Code/Abelard/consensus
python3 -m venv .venv
source .venv/bin/activate
pip install -e ../daemons/common     # abelard_common (http client + DaemonError)
pip install -e .                     # consensus (runtime only; add [dev] if you want pytest)
```

Smoke-check the CLI resolves:
```bash
consensus data smoke        # 5/5 sources OK expected; proves data-api/gamma/kalshi reachable
```

## 3. Config

`config.yaml` is in the repo; paths are relative to it, so the tape lands at
`consensus/data/l2_tape.db` automatically. No edits needed for a standard deploy.
(If Basilic should keep the tape elsewhere, set `collector.tape_path` to an
absolute path.)

## 4. First run by hand (bootstrap)

```bash
cd ~/Code/Abelard/consensus
.venv/bin/python -m consensus.cli collect run   # emits a JSON envelope on stdout
```
First run enumerates ~15 k markets (slow, minutes) and writes the initial tape.
Expect `status: degraded` with a single global-lane informational gap — that is by
design, not an error. Confirm the envelope's `result.tape.fills` is nonzero.

## 5. launchd job (every 120 s)

Write `~/Library/LaunchAgents/com.consensus.collector.plist` (adjust the two paths
to Basilic's absolute paths — no `~` inside a plist):

```xml
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN"
  "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key><string>com.consensus.collector</string>
  <key>ProgramArguments</key>
  <array>
    <string>/Users/USERNAME/Code/Abelard/consensus/.venv/bin/python</string>
    <string>-m</string><string>consensus.cli</string>
    <string>collect</string><string>run</string>
  </array>
  <key>WorkingDirectory</key><string>/Users/USERNAME/Code/Abelard/consensus</string>
  <key>StartInterval</key><integer>120</integer>
  <key>RunAtLoad</key><true/>
  <key>StandardOutPath</key><string>/Users/USERNAME/Code/Abelard/consensus/data/collector.launchd.out</string>
  <key>StandardErrorPath</key><string>/Users/USERNAME/Code/Abelard/consensus/data/collector.launchd.err</string>
</dict>
</plist>
```

Load it:
```bash
launchctl load ~/Library/LaunchAgents/com.consensus.collector.plist
```
(`StartInterval` fires every 120 s regardless of run duration; the collector's
built-in single-instance lock skips a pass if a previous one is still running, so
overlap is safe.)

## 6. Verify it's collecting

Wait ~10 min, then:
```bash
cd ~/Code/Abelard/consensus
.venv/bin/python -m consensus.cli collect status   # tape size, fills, tiers, declared gaps
tail -f data/collector_envelopes.jsonl             # one JSON envelope per pass
```
Healthy signs: `fills` climbing pass over pass, `gaps_declared` only from the global
lane (informational), `unresolved_strays` bounded. If `collect status` shows the tape
growing across two consecutive checks, Basilic is the live collector.

## 7. Retire the Windows collector (only after Step 6 confirms Basilic)

On the Windows box:
```powershell
schtasks /Delete /TN "ConsensusCollector" /F
```
From here Basilic is the sole source of truth for the September confirmation pass.

## 8. Ongoing

- **Monitoring:** `consensus collect status` any time; the launchd `.out/.err` files
  catch crashes. A pass that fails writes a structured `errors` array to its envelope,
  never a silent gap.
- **Disk watch:** ~0.5 GB/day. Check free space monthly against the September/October horizon.
- **Nothing else to do** until the confirmation pass window opens (~mid-Sep 2026). The
  collector needs no attention in between; it self-bootstraps and self-throttles.

---

### Note on what this does and doesn't buy

This gives the confirmation pass a **clean, continuous, current-regime forward archive** — which is exactly what the M0-C corrected finding (docs/m0c_report.md §4b) says is missing: the backtest ran on a network-truncated + horizon-biased 2026 sample, so it could not tell dispersal-real from sampling-artifact. Basilic's full-coverage forward tape is what resolves that in September. It does **not** change the current verdict (Detector A: NO-GO, no current-regime edge); it preserves the option to re-test properly.
