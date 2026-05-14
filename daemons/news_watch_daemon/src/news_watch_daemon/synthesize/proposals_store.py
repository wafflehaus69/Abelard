"""Drift-proposal on-disk store.

Pass C Step 11. Two files under `proposals_path`:

  - pending.json     — JSON array of active DriftProposal objects.
                       Mutated by `proposals add` (drift watcher
                       appends), `proposals approve`, `proposals reject`.
  - resolved.jsonl   — Append-only JSONL of resolution events.
                       One line per approve/reject. Never rotated;
                       historical audit value compounds.

Resolution-event shape (resolved.jsonl line):

  {
    "proposal_id": "dp-...",
    "theme_id": "us_iran_escalation",
    "proposed_keyword": "Bab el-Mandeb",
    "suggested_tier": "secondary",
    "action": "approve",          // or "reject"
    "resolved_at": "2026-05-14T08:32:00Z",
    "reason": "matches existing maritime signals",  // optional
    "applied_to_yaml": true       // true iff theme YAML was mutated
  }

Atomic write for pending.json (tmpfile + os.replace). The audit log
is append-only and not atomic — duplicates on crash mid-write are
preferred over lost records.

Pure module: file I/O is the only side effect. No network, no SDK.
"""

from __future__ import annotations

import json
import logging
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from .brief import DriftProposal


_LOG = logging.getLogger("news_watch_daemon.synthesize.proposals_store")

_PENDING_NAME = "pending.json"
_RESOLVED_NAME = "resolved.jsonl"


class ProposalsStoreError(RuntimeError):
    """Raised when proposals store I/O or validation fails."""


def _pending_path(root: Path) -> Path:
    return root / _PENDING_NAME


def _resolved_path(root: Path) -> Path:
    return root / _RESOLVED_NAME


def _ensure_dir(root: Path) -> None:
    root.mkdir(parents=True, exist_ok=True)


def read_pending(root: Path) -> list[DriftProposal]:
    """Read all pending proposals. Missing file = empty list.

    Returns proposals in the order they appear on disk (drift watcher
    appends new ones in evidence-count-descending order).

    Raises:
        ProposalsStoreError: file unreadable or malformed.
    """
    path = _pending_path(root)
    if not path.is_file():
        return []
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ProposalsStoreError(
            f"pending.json is malformed at {path}: {exc}"
        ) from exc
    if not isinstance(raw, list):
        raise ProposalsStoreError(
            f"pending.json root must be a list at {path}; got {type(raw).__name__}"
        )
    try:
        return [DriftProposal.model_validate(item) for item in raw]
    except Exception as exc:  # pydantic ValidationError surface
        raise ProposalsStoreError(
            f"pending.json contains invalid proposal at {path}: {exc}"
        ) from exc


def write_pending(root: Path, proposals: list[DriftProposal]) -> None:
    """Atomically rewrite pending.json.

    Uses tmpfile + os.replace so a concurrent reader never sees a
    half-written file. Creates the directory if missing.
    """
    _ensure_dir(root)
    path = _pending_path(root)
    payload = [p.model_dump(mode="json") for p in proposals]
    # Atomic write via tmpfile + os.replace (cross-platform on Win + macOS).
    fd, tmp_name = tempfile.mkstemp(
        prefix="pending-", suffix=".json.tmp", dir=str(root),
    )
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fp:
            json.dump(payload, fp, indent=2, ensure_ascii=False)
            fp.write("\n")
        os.replace(tmp_name, path)
    except Exception:
        # Clean up the tmpfile if rename failed.
        try:
            os.unlink(tmp_name)
        except OSError:
            pass
        raise


def append_proposals(root: Path, new: list[DriftProposal]) -> int:
    """Add new proposals to pending.json, dedup by proposal_id.

    Returns the count of proposals actually added (after dedup).
    Existing proposals are preserved; new proposals with a
    proposal_id matching an existing entry are skipped.
    """
    if not new:
        return 0
    existing = read_pending(root)
    existing_ids = {p.proposal_id for p in existing}
    deduped: list[DriftProposal] = []
    added = 0
    for p in new:
        if p.proposal_id in existing_ids:
            _LOG.warning(
                "skipping duplicate proposal_id %s during append", p.proposal_id,
            )
            continue
        existing_ids.add(p.proposal_id)
        deduped.append(p)
        added += 1
    if added > 0:
        write_pending(root, list(existing) + deduped)
    return added


def find_proposal(
    root: Path, proposal_id: str,
) -> Optional[DriftProposal]:
    """Locate a single proposal by ID. None if not found."""
    for p in read_pending(root):
        if p.proposal_id == proposal_id:
            return p
    return None


def remove_proposal(
    root: Path, proposal_id: str,
) -> Optional[DriftProposal]:
    """Remove a proposal by ID. Returns the removed entry (or None).

    Atomically rewrites pending.json.
    """
    existing = read_pending(root)
    kept: list[DriftProposal] = []
    removed: Optional[DriftProposal] = None
    for p in existing:
        if p.proposal_id == proposal_id and removed is None:
            removed = p
        else:
            kept.append(p)
    if removed is not None:
        write_pending(root, kept)
    return removed


def append_resolved(
    root: Path,
    *,
    proposal: DriftProposal,
    action: str,
    applied_to_yaml: bool,
    reason: Optional[str] = None,
    when: Optional[datetime] = None,
) -> None:
    """Append one resolution event to resolved.jsonl.

    Append-only — never rewritten. Best-effort: a crash mid-write may
    leave a partial line, but the next read will skip malformed lines
    (see `read_resolved`).

    Args:
        root: proposals directory.
        proposal: the proposal being resolved (full payload recorded
            for audit; theme YAML edits may make the proposed_keyword
            ambiguous without this).
        action: "approve" or "reject".
        applied_to_yaml: True iff the theme YAML was mutated. False on
            reject, and on approve when the user passed --dry-run or
            the YAML edit was skipped.
        reason: optional free-text reason (used on reject).
        when: timestamp; defaults to UTC now.
    """
    if action not in ("approve", "reject"):
        raise ProposalsStoreError(f"action must be 'approve' or 'reject'; got {action!r}")
    _ensure_dir(root)
    ts = when if when is not None else datetime.now(timezone.utc)
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    else:
        ts = ts.astimezone(timezone.utc)
    record: dict[str, Any] = {
        "proposal_id": proposal.proposal_id,
        "theme_id": proposal.theme_id,
        "proposed_keyword": proposal.proposed_keyword,
        "suggested_tier": proposal.suggested_tier,
        "action": action,
        "resolved_at": ts.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "applied_to_yaml": applied_to_yaml,
    }
    if reason is not None:
        record["reason"] = reason
    line = json.dumps(record, ensure_ascii=False)
    with _resolved_path(root).open("a", encoding="utf-8") as fp:
        fp.write(line)
        fp.write("\n")


def read_resolved(root: Path) -> list[dict[str, Any]]:
    """Read every resolution event. Malformed lines are skipped + logged.

    For Step 14's calibration sweep — diffs approval-rate and
    reject-reason patterns over time.
    """
    path = _resolved_path(root)
    if not path.is_file():
        return []
    records: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as fp:
        for i, line in enumerate(fp, start=1):
            line = line.rstrip("\n")
            if not line:
                continue
            try:
                records.append(json.loads(line))
            except json.JSONDecodeError as exc:
                _LOG.warning(
                    "skipping malformed line %d in %s: %s", i, path, exc,
                )
    return records


__all__ = [
    "ProposalsStoreError",
    "append_proposals",
    "append_resolved",
    "find_proposal",
    "read_pending",
    "read_resolved",
    "remove_proposal",
    "write_pending",
]
