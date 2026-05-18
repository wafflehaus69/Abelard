"""Append-only JSONL log of cross-source duplicate observations.

Pass C end / Pass D foundation (2026-05-17). When the dedup pathway
detects a headline whose `dedupe_hash` was already observed FROM A
DIFFERENT SOURCE (either earlier in the same scrape sweep or in the
DB within the 72-hour dedup window), it appends one entry here
recording both observations.

The destructive default — silently dropping the second-source
observation — destroys the data needed to answer "for messages that
appeared in both channels, what was the time delta." This log
preserves that data without changing dedup behavior: the headline is
still deduplicated for downstream synthesis/alerting, but the cross-
source observation is recorded for empirical primary-source decisions.

Initial driving use case: political_volatility theme tracks
@real_DonaldJTrump (curated mirror) and @TrumpTruthSocial_Alert
(mechanical scraper) in parallel. After ~2 weeks of operation, the
log answers: which channel arrives first on average; which carries
more or fewer duplicates; what's the latency distribution. That
analysis is Pass D scope. This module only emits the data.

Retention discipline (matches trigger_log.py / synthesize archive):
append-only, never rotated. The empirical value compounds for
calibration questions weeks or months from now.

Per-entry schema:

  {
    "observed_at_unix":          int,
    "observed_at":               "ISO-8601 UTC",
    "dedupe_hash":               "32-char hex prefix of SHA256",
    "first_source":              "telegram:real_DonaldJTrump",
    "first_observed_at_unix":    int,
    "second_source":             "telegram:TrumpTruthSocial_Alert",
    "second_observed_at_unix":   int,
    "latency_seconds":           int,   # second - first (always >= 0)
    "headline":                  "verbatim, truncated to 280 chars"
  }

Same-source duplicates (e.g. a channel returning an edited message a
second time, or a 72h-window re-fetch from the same source) are NOT
logged here — that's noise for the cross-source question. The
orchestrator only calls `write_observation` when first_source !=
second_source.

POSIX O_APPEND gives per-line atomicity for writes under PIPE_BUF
(~4 KiB on Linux). One entry is well under that — typically ~400
bytes. On Windows, FILE_APPEND_DATA has similar small-write atomicity.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


_HEADLINE_TRUNCATE_CHARS = 280


def write_observation(
    log_path: Path,
    *,
    dedupe_hash: str,
    first_source: str,
    first_observed_at_unix: int,
    second_source: str,
    second_observed_at_unix: int,
    headline: str,
    now_unix: int | None = None,
) -> None:
    """Append one JSONL entry for a cross-source duplicate observation.

    Args:
        log_path: Path to the append-only JSONL file. Parent directory
            is created if missing.
        dedupe_hash: The shared dedupe_hash both sources produced.
        first_source: Source name that observed the headline FIRST
            (chronologically, by fetched_at).
        first_observed_at_unix: Unix seconds when first_source's
            fetched_at landed.
        second_source: Source name observing the same hash NOW.
            Caller MUST ensure first_source != second_source — this
            function does not gate the write.
        second_observed_at_unix: Unix seconds for the current
            observation.
        headline: Verbatim text of the second observation (truncated
            to 280 chars for log compactness; the full text remains
            in the headlines table under first_source).
        now_unix: Optional override for the audit `observed_at_unix`
            top-level field; defaults to current UTC time.

    Never raises on well-formed inputs. OS-level I/O errors propagate.
    """
    log_path.parent.mkdir(parents=True, exist_ok=True)
    now = (
        datetime.fromtimestamp(now_unix, tz=timezone.utc)
        if now_unix is not None
        else datetime.now(timezone.utc)
    )
    truncated = (
        headline[:_HEADLINE_TRUNCATE_CHARS] if headline is not None else ""
    )
    entry: dict[str, Any] = {
        "observed_at_unix": int(now.timestamp()),
        "observed_at": now.isoformat(timespec="seconds").replace("+00:00", "Z"),
        "dedupe_hash": dedupe_hash,
        "first_source": first_source,
        "first_observed_at_unix": first_observed_at_unix,
        "second_source": second_source,
        "second_observed_at_unix": second_observed_at_unix,
        "latency_seconds": max(0, second_observed_at_unix - first_observed_at_unix),
        "headline": truncated,
    }
    line = json.dumps(entry, ensure_ascii=False, separators=(",", ":")) + "\n"
    with log_path.open("a", encoding="utf-8") as f:
        f.write(line)


def read_observations(log_path: Path) -> list[dict[str, Any]]:
    """Read the full log into memory. Skips malformed lines defensively.

    For Pass D calibration tooling. At the volumes this log emits
    (dozens of cross-source dupes per day at most) full-load is
    trivial. If the file ever grows to GB scale, switch to a streaming
    reader.
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


__all__ = ["read_observations", "write_observation"]
