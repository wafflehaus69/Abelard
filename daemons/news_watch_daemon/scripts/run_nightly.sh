#!/bin/zsh
# News Watch nightly runner for launchd (com.abelard.news-watch).
# Mando-authorized deploy 2026-08-24 (CD-DASH1 P4).
#
# `run` is the documented cold-start cycle: ensure schema + themes (both
# idempotent), scrape, attention, synthesis, assemble the brief. It costs real
# LLM spend, so it runs ONCE nightly and never on load.
#
# Slot 21:30 America/New_York: ahead of Smart Money's 22:30 scan, its 23:15
# brief and capex's 23:40, so the evening block runs in order and nothing
# overlaps. A cycle takes minutes, not tens of minutes.
cd ~/Code/Abelard/daemons/news_watch_daemon || exit 2
LOG=~/.openclaw/news_watch/logs/nightly.log
mkdir -p ~/.openclaw/news_watch/logs
echo ">>> news-watch run $(date -u +%Y-%m-%dT%H:%M:%SZ)" >> $LOG
.venv/bin/news-watch-daemon run --quiet >> $LOG 2>&1
rc=$?
echo "<<< exit $rc $(date -u +%Y-%m-%dT%H:%M:%SZ)" >> $LOG
exit $rc
