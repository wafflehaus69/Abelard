"""Filesystem archive for Briefs — the source of truth Abelard reads.

Layout:

    <archive_root>/
        2026-05/
            nwd-2026-05-13T14-32-08Z-a1b2c3d4.json
            nwd-2026-05-13T16-12-44Z-9e8d7c6b.json
            ...
        2026-06/
            ...

YYYY-MM partitions are extracted from the brief_id so the partition
path is deterministic from the id alone — no separate index needed.

Writes are atomic: we write to a hidden `.{brief_id}.{rnd}.tmp` file in
the partition dir, then `os.replace` to the final name. POSIX gives us
atomicity for free; on Windows `os.replace` uses MoveFileEx with
MOVEFILE_REPLACE_EXISTING which is atomic for same-volume renames.

`list_brief_ids` returns IDs newest-first without parsing the files —
that's the index Abelard reads through. Theme filtering happens at the
CLI layer (Step 12) by loading and inspecting `themes_covered`. With
<1000 briefs the full scan is cheap; if it ever becomes a hot path we
add a sidecar index then.
"""

from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path

from .brief import Brief


class ArchiveError(RuntimeError):
    """Raised when a brief cannot be located, parsed, or persisted."""


def _month_partition(brief_id: str) -> str:
    """Extract `YYYY-MM` from a brief_id.

    brief_id format is `nwd-YYYY-MM-DDTHH-MM-SSZ-{hex}`. Splitting on
    `-` yields `['nwd', 'YYYY', 'MM', 'DDTHH', 'MM', 'SSZ', '{hex}']`.
    Indices 1 and 2 are the year and month.
    """
    parts = brief_id.split("-")
    if len(parts) < 4 or parts[0] != "nwd":
        raise ArchiveError(f"malformed brief_id (no nwd prefix or too few parts): {brief_id!r}")
    year, month = parts[1], parts[2]
    if not (len(year) == 4 and year.isdigit()):
        raise ArchiveError(f"malformed brief_id (bad year): {brief_id!r}")
    if not (len(month) == 2 and month.isdigit()):
        raise ArchiveError(f"malformed brief_id (bad month): {brief_id!r}")
    return f"{year}-{month}"


def _partition_dir(archive_root: Path, brief_id: str) -> Path:
    return archive_root / _month_partition(brief_id)


def write_brief(archive_root: Path, brief: Brief) -> Path:
    """Persist a Brief atomically. Returns the final file path.

    Caller-owned directory creation: `archive_root` is mkdir-ed if it
    doesn't exist, and so is the YYYY-MM partition.
    """
    archive_root.mkdir(parents=True, exist_ok=True)
    partition = _partition_dir(archive_root, brief.brief_id)
    partition.mkdir(parents=True, exist_ok=True)
    final = partition / f"{brief.brief_id}.json"

    fd, tmp_path = tempfile.mkstemp(
        prefix=f".{brief.brief_id}.",
        suffix=".tmp",
        dir=partition,
    )
    tmp = Path(tmp_path)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(
                brief.model_dump(mode="json"),
                f,
                indent=2,
                ensure_ascii=False,
                sort_keys=False,
            )
        os.replace(tmp, final)
    except Exception:
        # Clean up the tmp file on any failure; final is left untouched.
        try:
            tmp.unlink()
        except OSError:
            pass
        raise
    return final


def read_brief(archive_root: Path, brief_id: str) -> Brief:
    """Load a Brief by id. Raises ArchiveError on missing or corrupt file."""
    path = _partition_dir(archive_root, brief_id) / f"{brief_id}.json"
    if not path.is_file():
        raise ArchiveError(f"brief not found: {brief_id}")
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ArchiveError(f"corrupt brief at {path}: {exc}") from exc
    try:
        return Brief.model_validate(raw)
    except Exception as exc:  # noqa: BLE001 — pydantic validation surface
        raise ArchiveError(f"schema mismatch for brief at {path}: {exc}") from exc


def list_brief_ids(
    archive_root: Path,
    *,
    limit: int | None = None,
) -> list[str]:
    """Return brief_ids newest-first.

    Scans YYYY-MM partitions in reverse (newest month first), then
    each partition's `*.json` files in reverse (newest brief first
    within month — works because brief_ids embed the ISO timestamp).

    `limit`, when set, short-circuits the scan once that many ids are
    collected. No file contents are loaded — IDs come from the file
    stems only.
    """
    if not archive_root.is_dir():
        return []
    ids: list[str] = []
    partitions = sorted(
        (p for p in archive_root.iterdir() if p.is_dir() and _looks_like_partition(p.name)),
        reverse=True,
    )
    for partition in partitions:
        files = sorted(
            (p for p in partition.iterdir() if p.is_file() and p.suffix == ".json"
             and not p.name.startswith(".")),
            reverse=True,
        )
        for f in files:
            ids.append(f.stem)
            if limit is not None and len(ids) >= limit:
                return ids
    return ids


def _looks_like_partition(name: str) -> bool:
    """`YYYY-MM` directory name check."""
    if len(name) != 7 or name[4] != "-":
        return False
    year, month = name[:4], name[5:]
    return year.isdigit() and month.isdigit()


__all__ = [
    "ArchiveError",
    "list_brief_ids",
    "read_brief",
    "write_brief",
]
