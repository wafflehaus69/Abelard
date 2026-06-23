# Scheduling ChatterDaemon on the Mac mini

After `scripts/setup.sh`, schedule the two run modes. Both read the gitignored `.env`
beside `daemons/chatter_daemon/pyproject.toml`, so **no keys go in the scheduler**.

Pick ONE of cron (simple) or launchd (native macOS).

## Paths
- daemon dir:  `~/Code/Abelard/daemons/chatter_daemon`   (adjust to your clone path)
- venv python: `<daemon dir>/.venv/bin/python`
- logs:        `~/chatter-logs/`  — run `mkdir -p ~/chatter-logs` first

## Option A — cron (`crontab -e`)

NOTE: escape `%` as `\%` in cron; times are the box's **local** TZ — tune to your
market hours.

```cron
CHATTER=$HOME/Code/Abelard/daemons/chatter_daemon
# watchlist scan — weekdays, after the US close
30 16 * * 1-5  cd "$CHATTER" && .venv/bin/python -m chatter_daemon scan --all >> "$HOME/chatter-logs/scan-$(date +\%F).json" 2>> "$HOME/chatter-logs/scan.err"
# attention discovery — daily
0 17 * * *     cd "$CHATTER" && .venv/bin/python -m chatter_daemon attention   >> "$HOME/chatter-logs/attn-$(date +\%F).json" 2>> "$HOME/chatter-logs/attn.err"
```

## Option B — launchd (macOS native)

Save as `~/Library/LaunchAgents/com.abelard.chatter.scan.plist`, edit the two absolute
paths + Hour/Minute, then `launchctl load ~/Library/LaunchAgents/com.abelard.chatter.scan.plist`.

```xml
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0"><dict>
  <key>Label</key><string>com.abelard.chatter.scan</string>
  <key>WorkingDirectory</key><string>/Users/YOU/Code/Abelard/daemons/chatter_daemon</string>
  <key>ProgramArguments</key>
  <array>
    <string>/Users/YOU/Code/Abelard/daemons/chatter_daemon/.venv/bin/python</string>
    <string>-m</string><string>chatter_daemon</string><string>scan</string><string>--all</string>
  </array>
  <key>StandardOutPath</key><string>/Users/YOU/chatter-logs/scan.out</string>
  <key>StandardErrorPath</key><string>/Users/YOU/chatter-logs/scan.err</string>
  <key>StartCalendarInterval</key><dict>
    <key>Hour</key><integer>16</integer><key>Minute</key><integer>30</integer>
  </dict>
</dict></plist>
```

Duplicate the plist with a `.attention` label and `attention` args for the discovery run.

## Maturation (set expectations)

Salience is live on run 1. The Order-7 baselines + velocity z-scores need **~5 scans
(≈1 trading week)** before spikes flag — early `building` states are expected, not a bug.
StockTwits/Finnhub/​/smg/ carry every run; Google Trends is best-effort (it rate-limits).
