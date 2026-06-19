"""Run persistence (Order 7) — atomic JSON archive + fail-loud load, mirroring News
Watch's `synthesize/archive.py`.

Layout: `<archive_root>/YYYY-MM/{scan_id}.json`. The YYYY-MM partition is extracted
from the scan_id, so the path is deterministic from the id alone (no index). The
scan_id is STABLE per run — `cd-{canonical_ts}-{hash(watchlist names)}` — so a re-run
overwrites in place rather than accumulating duplicates.

Writes are atomic: write to a hidden `.{scan_id}.{rnd}.tmp` in the partition, then
`os.replace` to the final name (atomic same-volume rename on POSIX and Windows). On
any failure the tmp is cleaned up and the final is left untouched.

Cost telemetry is already folded into the `AggregatedScanResult` before this writes,
so a write failure can't lose the API-call cost (doctrine #4) — it stays on the
returned object regardless of the disk outcome.
"""

from __future__ import annotations

import hashlib
import json
import os
import tempfile
from pathlib import Path
from typing import Iterable

from .schema import AggregatedScanResult


class ArchiveError(RuntimeError):
    """Raised when a run cannot be persisted or loaded. Names the offending path."""


def make_scan_id(canonical_ts: str, watchlist_names: Iterable[str]) -> str:
    """Stable, deterministic id for one run: `cd-{ts}-{hash8}`. The hash of the
    sorted watchlist names disambiguates same-second runs; sorting makes it
    order-independent."""
    stamp = canonical_ts.replace(":", "-")  # 2026-06-19T14:32:08Z -> ...T14-32-08Z
    digest = hashlib.sha256(
        ",".join(sorted(watchlist_names)).encode("utf-8")
    ).hexdigest()[:8]
    return f"cd-{stamp}-{digest}"


def _partition(scan_id: str) -> str:
    """`YYYY-MM` extracted from a `cd-YYYY-MM-...` scan_id. Fail loud if malformed."""
    parts = scan_id.split("-")
    if len(parts) < 4 or parts[0] != "cd":
        raise ArchiveError(f"malformed scan_id (no cd prefix or too few parts): {scan_id!r}")
    year, month = parts[1], parts[2]
    if not (len(year) == 4 and year.isdigit()):
        raise ArchiveError(f"malformed scan_id (bad year): {scan_id!r}")
    if not (len(month) == 2 and month.isdigit()):
        raise ArchiveError(f"malformed scan_id (bad month): {scan_id!r}")
    return f"{year}-{month}"


def write_result(archive_root: Path, result: AggregatedScanResult) -> Path:
    """Persist the run atomically. Returns the final file path. Fail loud on an
    unwritable archive."""
    try:
        partition = archive_root / _partition(result.scan_id)
        partition.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        raise ArchiveError(
            f"cannot create archive partition under {archive_root}: {exc}"
        ) from exc

    final = partition / f"{result.scan_id}.json"
    try:
        fd, tmp_path = tempfile.mkstemp(
            prefix=f".{result.scan_id}.", suffix=".tmp", dir=partition
        )
    except OSError as exc:
        raise ArchiveError(f"cannot create temp file in {partition}: {exc}") from exc

    tmp = Path(tmp_path)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(
                result.model_dump(mode="json"),
                f,
                indent=2,
                ensure_ascii=False,
                sort_keys=False,
            )
        os.replace(tmp, final)
    except Exception as exc:
        try:
            tmp.unlink()
        except OSError:
            pass
        raise ArchiveError(f"failed to write {final}: {exc}") from exc
    return final


def load_result(path: Path) -> AggregatedScanResult:
    """Load + validate a persisted run from an explicit path. Total over valid
    inputs: returns a validated `AggregatedScanResult` or raises `ArchiveError`
    naming the specific failure (missing / not-a-file / unreadable / malformed JSON /
    schema mismatch). Never returns a partial result."""
    if not path.exists():
        raise ArchiveError(f"file does not exist: {path}")
    if not path.is_file():
        raise ArchiveError(f"path is not a regular file: {path}")
    try:
        raw_text = path.read_text(encoding="utf-8")
    except OSError as exc:
        raise ArchiveError(f"could not read {path}: {exc}") from exc
    try:
        raw = json.loads(raw_text)
    except json.JSONDecodeError as exc:
        raise ArchiveError(f"malformed JSON in {path}: {exc}") from exc
    try:
        return AggregatedScanResult.model_validate(raw)
    except Exception as exc:  # pydantic ValidationError surface
        raise ArchiveError(f"schema mismatch in {path}: {exc}") from exc


__all__ = ["ArchiveError", "load_result", "make_scan_id", "write_result"]
