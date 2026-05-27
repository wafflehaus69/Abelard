"""Append-only JSONL audit trail of per-source noise-filter drops.

Task 1 (2026-05-27). When a source plugin's `noise_filter` matches a
fetched message (currently: TelegramSource's per-channel sponsor /
promo / affiliate substring filter), the message is dropped before
insertion into the headlines table and one entry is appended here.

The audit trail exists so the drop is reconstructible months later
without leaving a forensic gap. Six months from now the question "did
the filter eat post X?" must be answerable by inspecting this file —
not by re-deriving from logs that may have been rotated or lost.

Retention discipline (matches trigger_log.py / cross_source_log.py /
synthesize archive): append-only, never rotated. Storage cost is
negligible at observed Ateobreaking volumes (~1 sponsor / day, ~1 KB
each).

Per-entry schema:

  {
    "filtered_at_unix":    int,            # paired-timestamp convention
    "filtered_at":         "ISO-8601 UTC", # ditto
    "channel":             "Ateobreaking", # channel username, no @ prefix
    "msg_id":              "170758",       # Telegram message ID, stringified
    "matched_pattern":     "gnuvpn",       # the literal pattern that hit
    "full_text":           "...verbatim, UNTRUNCATED..."
  }

Note: `full_text` is NOT truncated (unlike cross_source_log which
caps at 280 chars). The audit-trail purpose is to fully reconstruct
the dropped message; truncation defeats that.

POSIX O_APPEND gives per-line atomicity for writes under PIPE_BUF
(~4 KiB on Linux). Long sponsor posts (e.g. GnuVPN ad at 830 chars)
exceed that and could theoretically interleave under concurrent
writes, but the daemon serializes scrape sweeps — no concurrent
writers in practice. On Windows, FILE_APPEND_DATA has similar
small-write atomicity semantics.

Best-effort writes: I/O errors propagate from this module; callers
(currently TelegramSource._on_filtered) wrap in try/log-on-failure
so a disk-full or permission-denied audit-log write does not abort
the scrape.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def write_filter_entry(
    log_path: Path,
    *,
    channel: str,
    msg_id: str,
    matched_pattern: str,
    full_text: str,
    now_unix: int | None = None,
) -> None:
    """Append one JSONL entry for a noise-filter drop.

    Args:
        log_path: Path to the append-only JSONL file. Parent directory
            is created if missing.
        channel: Source channel username (no `@` prefix).
        msg_id: Source-native message ID, stringified.
        matched_pattern: The literal noise_filter substring that matched
            (case-insensitive match in caller; the original literal is
            stored here for audit clarity).
        full_text: Verbatim message text — UNTRUNCATED. The audit
            purpose requires full reconstruction.
        now_unix: Optional override for `filtered_at_unix`; defaults to
            current UTC time.

    Never raises on well-formed inputs. OS-level I/O errors propagate.
    """
    log_path.parent.mkdir(parents=True, exist_ok=True)
    now = (
        datetime.fromtimestamp(now_unix, tz=timezone.utc)
        if now_unix is not None
        else datetime.now(timezone.utc)
    )
    entry: dict[str, Any] = {
        "filtered_at_unix": int(now.timestamp()),
        "filtered_at": now.isoformat(timespec="seconds").replace("+00:00", "Z"),
        "channel": channel,
        "msg_id": msg_id,
        "matched_pattern": matched_pattern,
        "full_text": full_text,
    }
    line = json.dumps(entry, ensure_ascii=False, separators=(",", ":")) + "\n"
    with log_path.open("a", encoding="utf-8") as f:
        f.write(line)


def read_filter_entries(log_path: Path) -> list[dict[str, Any]]:
    """Read the full audit log into memory. Skips malformed lines defensively.

    For operational tooling and future filter-rate analysis. At observed
    Ateobreaking volumes (~1 sponsor / day) full-load is trivial. If the
    file ever grows to GB scale, switch to a streaming reader.
    """
    if not log_path.is_file():
        return []
    out: list[dict[str, Any]] = []
    with log_path.open("r", encoding="utf-8") as f:
        for line in f:
            stripped = line.strip()
            if not stripped:
                continue
            try:
                out.append(json.loads(stripped))
            except json.JSONDecodeError:
                # Defensive: never crash on a corrupt log line.
                continue
    return out


__all__ = ["read_filter_entries", "write_filter_entry"]
