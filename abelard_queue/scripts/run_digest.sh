#!/bin/zsh
# Alert-digest writer for launchd (com.abelard.queue-digest).
# Mando-authorized deploy 2026-08-25 (CD-DASH2 P2).
#
# WRITES A FILE. SENDS NOTHING. This is the only queue subcommand that gets a
# schedule, and the reason it may have one is that it cannot reach an outward
# send — proved over the call graph in tests/test_digest_cannot_send.py, not
# asserted here. Dispatch stays on Mando's command; see OPERATIONS.md.
#
# 06:00 America/New_York: after the whole nightly block (smart-money 22:30,
# its brief 23:15, capex 23:40), so each morning's file reflects last night's
# scans rather than the previous morning's.
#
# Exit 2 means the queue is NOT empty. That is information, not breakage: a
# scheduled digest exiting 0 with a full queue is the silence it exists to end.
cd ~/Code/Abelard/abelard_queue || exit 3
LOG=~/.openclaw/abelard_queue/logs/digest.log
mkdir -p ~/.openclaw/abelard_queue/logs
echo ">>> digest $(date -u +%Y-%m-%dT%H:%M:%SZ)" >> $LOG
.venv/bin/abelard-queue digest >> $LOG 2>&1
rc=$?
echo "<<< exit $rc $(date -u +%Y-%m-%dT%H:%M:%SZ)" >> $LOG
# 2 = items waiting, which is a normal steady state for this job.
[ $rc -eq 2 ] && exit 0
exit $rc
