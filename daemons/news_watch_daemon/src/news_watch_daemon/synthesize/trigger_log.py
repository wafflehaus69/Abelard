"""Append-only JSONL log of trigger-gate decisions.

Every call to `evaluate_gate` should be followed by `write_entry` so
the historical record of "what looked interesting at any timestamp"
accumulates. The log is the load-bearing artifact for the §14
calibration gate — Mando reviews this file to tune trigger thresholds.

Retention discipline (Pass C §9 retention note): the file is
**append-only and never rotated.** Archive value compounds; querying
"what fired during the Iran ceasefire window" months from now must
return a real answer. No rotation logic in this module.

POSIX O_APPEND semantics give us per-line atomicity for writes under
PIPE_BUF (~4 KiB on Linux). One JSONL entry is well under that —
typically ~300 bytes. On Windows, FILE_APPEND_DATA is similarly atomic
for small writes.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from .trigger import TriggerDecision


def write_entry(log_path: Path, decision: TriggerDecision) -> None:
    """Append one JSONL entry for the given gate decision.

    Creates the parent directory if missing. Each entry is exactly one
    line of compact JSON terminated by `\n`. Never raises on
    well-formed inputs; OS-level I/O errors propagate.
    """
    log_path.parent.mkdir(parents=True, exist_ok=True)
    now = datetime.now(timezone.utc)
    entry = {
        "timestamp_unix": int(now.timestamp()),
        "timestamp": now.isoformat(timespec="seconds").replace("+00:00", "Z"),
        "decision": "fire" if decision.fire else "suppress",
        "reason": decision.reason,
        "themes_in_scope": list(decision.themes_in_scope),
        "matched_headline_ids": list(decision.matched_headline_ids),
        "window_since_unix": decision.window_since_unix,
        "window_until_unix": decision.window_until_unix,
    }
    line = json.dumps(entry, ensure_ascii=False, separators=(",", ":")) + "\n"
    with log_path.open("a", encoding="utf-8") as f:
        f.write(line)


def read_last_n(log_path: Path, last_n: int) -> list[dict]:
    """Read the last N entries from the trigger log, oldest-first.

    Missing file returns []. Corrupt lines (rare; only possible from
    interrupted writes given the append-atomic semantics) are skipped.
    The file is loaded fully — at the volumes this daemon emits
    (~hundreds of entries per day) this is trivial. If the file ever
    grows to GB scale, replace with a tail-read.
    """
    if last_n <= 0:
        return []
    if not log_path.is_file():
        return []
    with log_path.open("r", encoding="utf-8") as f:
        lines = f.readlines()
    out: list[dict] = []
    for line in lines[-last_n:]:
        stripped = line.strip()
        if not stripped:
            continue
        try:
            out.append(json.loads(stripped))
        except json.JSONDecodeError:
            # Defensive: never crash on a corrupt log line.
            continue
    return out


__all__ = ["read_last_n", "write_entry"]
