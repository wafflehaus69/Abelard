"""Run persistence (Order 7 + 8) — atomic JSON archive + fail-loud load, mirroring
News Watch's `synthesize/archive.py`.

Layout: `<archive_root>/YYYY-MM/{id}.json`. The YYYY-MM partition is extracted from the
id, so the path is deterministic from the id alone (no index). Ids are STABLE per run
(`cd-{ts}-{hash}`) so a re-run overwrites in place. Watchlist scans, ATTENTION scans,
and ATTENTION cold-archive roll-ups all share the same atomic-write core; they differ
only in the validated model on load.

Writes are atomic: write to a hidden `.{id}.{rnd}.tmp` in the partition, then
`os.replace` to the final name. On any failure the tmp is cleaned up and the final is
left untouched. Cost telemetry is folded into the result before this writes, so a write
failure can't lose it.
"""

from __future__ import annotations

import hashlib
import json
import os
import tempfile
from pathlib import Path
from typing import Any, Iterable

from .schema import AggregatedScanResult, AttentionResult, ColdRollup


class ArchiveError(RuntimeError):
    """Raised when a run cannot be persisted or loaded. Names the offending path."""


def make_scan_id(canonical_ts: str, watchlist_names: Iterable[str]) -> str:
    """Stable, deterministic id for one run: `cd-{ts}-{hash8}`. The hash of the sorted
    names disambiguates same-second runs; sorting makes it order-independent."""
    stamp = canonical_ts.replace(":", "-")  # 2026-06-19T14:32:08Z -> ...T14-32-08Z
    digest = hashlib.sha256(
        ",".join(sorted(watchlist_names)).encode("utf-8")
    ).hexdigest()[:8]
    return f"cd-{stamp}-{digest}"


def make_rollup_id(day: str, generated_ts: int) -> str:
    """Date-stamped id for a cold-archive prune batch: `cd-{YYYY-MM-DD}-attnroll-{h}`.
    The leading date keeps it YYYY-MM-partition-compatible with `_partition`."""
    digest = hashlib.sha256(f"{day}:{generated_ts}".encode("utf-8")).hexdigest()[:8]
    return f"cd-{day}-attnroll-{digest}"


def _partition(file_id: str) -> str:
    """`YYYY-MM` extracted from a `cd-YYYY-MM-...` id. Fail loud if malformed."""
    parts = file_id.split("-")
    if len(parts) < 4 or parts[0] != "cd":
        raise ArchiveError(f"malformed id (no cd prefix or too few parts): {file_id!r}")
    year, month = parts[1], parts[2]
    if not (len(year) == 4 and year.isdigit()):
        raise ArchiveError(f"malformed id (bad year): {file_id!r}")
    if not (len(month) == 2 and month.isdigit()):
        raise ArchiveError(f"malformed id (bad month): {file_id!r}")
    return f"{year}-{month}"


def _write_json(archive_root: Path, file_id: str, payload: dict[str, Any]) -> Path:
    """Atomic write of `payload` to `<archive_root>/YYYY-MM/{file_id}.json`."""
    try:
        partition = archive_root / _partition(file_id)
        partition.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        raise ArchiveError(
            f"cannot create archive partition under {archive_root}: {exc}"
        ) from exc

    final = partition / f"{file_id}.json"
    try:
        fd, tmp_path = tempfile.mkstemp(prefix=f".{file_id}.", suffix=".tmp", dir=partition)
    except OSError as exc:
        raise ArchiveError(f"cannot create temp file in {partition}: {exc}") from exc

    tmp = Path(tmp_path)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2, ensure_ascii=False, sort_keys=False)
        os.replace(tmp, final)
    except Exception as exc:
        try:
            tmp.unlink()
        except OSError:
            pass
        raise ArchiveError(f"failed to write {final}: {exc}") from exc
    return final


def _load_json(path: Path) -> Any:
    """Read + parse a persisted JSON file, fail-loud on every bad path (the schema
    validation is the caller's)."""
    if not path.exists():
        raise ArchiveError(f"file does not exist: {path}")
    if not path.is_file():
        raise ArchiveError(f"path is not a regular file: {path}")
    try:
        raw_text = path.read_text(encoding="utf-8")
    except OSError as exc:
        raise ArchiveError(f"could not read {path}: {exc}") from exc
    try:
        return json.loads(raw_text)
    except json.JSONDecodeError as exc:
        raise ArchiveError(f"malformed JSON in {path}: {exc}") from exc


def write_result(archive_root: Path, result: AggregatedScanResult) -> Path:
    """Persist a watchlist scan atomically. Fail loud on an unwritable archive."""
    return _write_json(archive_root, result.scan_id, result.model_dump(mode="json"))


def write_attention_result(archive_root: Path, result: AttentionResult) -> Path:
    """Persist an ATTENTION scan atomically."""
    return _write_json(archive_root, result.scan_id, result.model_dump(mode="json"))


def write_cold_rollup(archive_root: Path, rollup: ColdRollup) -> Path:
    """Persist a prune batch (cold archive) atomically. Indefinite long-term memory."""
    return _write_json(archive_root, rollup.rollup_id, rollup.model_dump(mode="json"))


def load_result(path: Path) -> AggregatedScanResult:
    """Load + validate a watchlist scan from an explicit path. Total over valid
    inputs: a validated `AggregatedScanResult` or `ArchiveError` naming the failure."""
    raw = _load_json(path)
    try:
        return AggregatedScanResult.model_validate(raw)
    except Exception as exc:  # pydantic ValidationError surface
        raise ArchiveError(f"schema mismatch in {path}: {exc}") from exc


def load_attention_result(path: Path) -> AttentionResult:
    """Load + validate an ATTENTION scan from an explicit path."""
    raw = _load_json(path)
    try:
        return AttentionResult.model_validate(raw)
    except Exception as exc:
        raise ArchiveError(f"schema mismatch in {path}: {exc}") from exc


def peek_scan_mode(path: Path) -> str | None:
    """Read just the `scan_mode` discriminator so the read path can dispatch to the
    right loader. Fail-loud on a bad path / malformed JSON (the typed load then
    validates the full schema)."""
    raw = _load_json(path)
    return raw.get("scan_mode") if isinstance(raw, dict) else None


__all__ = [
    "ArchiveError",
    "load_attention_result",
    "load_result",
    "make_rollup_id",
    "make_scan_id",
    "peek_scan_mode",
    "write_attention_result",
    "write_cold_rollup",
    "write_result",
]
