"""Path-based loader for persisted Full Brief artifacts (read-brief subcommand).

Full Briefs persist as structured JSON only (see fullbrief/orchestrator.py
`write_brief` → synthesize/archive.py): `{brief_id}.json` under the
`<archive_root>/YYYY-MM/` partition, written via
`json.dump(envelope.model_dump(mode="json"), indent=2, ensure_ascii=False)`.
No pre-rendered text artifact is persisted — the human-readable form is
produced on demand by `fullbrief/render.py::render_full_brief`.

This module is the LEAF load+validate function for the `read-brief
<path>` subcommand. Per the daemon's fail-loud doctrine it is
total-over-valid-inputs: it returns a validated `FullBriefEnvelope` or
raises `FullBriefLoadError` naming the SPECIFIC failure. The CLI
subcommand owns failure-case handling (error message + nonzero exit);
this function never prints, never exits, never returns a partial brief.

Note the existing `synthesize/archive.py::read_brief` loads by
(archive_root, brief_id) and discriminates across three brief types.
`read-brief` instead takes an explicit filesystem PATH to a single
artifact and is Full-Brief-specific, so it gets its own narrow loader
rather than overloading the id-keyed reader.
"""

from __future__ import annotations

import json
from pathlib import Path

from .brief import FullBriefEnvelope


class FullBriefLoadError(RuntimeError):
    """Raised when a persisted Full Brief cannot be located, parsed, or validated.

    The message always names the offending path and the specific failure
    (missing file, malformed JSON, wrong brief_type, schema mismatch) so
    the operator can act without opening the file.
    """


def load_full_brief_from_path(path: Path) -> FullBriefEnvelope:
    """Load + validate a persisted Full Brief artifact from an explicit path.

    Total over valid inputs: returns a fully-validated `FullBriefEnvelope`
    or raises `FullBriefLoadError`. Never renders a partial brief.

    Failure modes (all → `FullBriefLoadError`, message names the path):
      - path does not exist
      - path exists but is not a regular file (e.g. a directory)
      - file is unreadable (OSError)
      - file is not valid JSON
      - JSON is the wrong brief type (e.g. a Pass C Brief or AttentionBrief)
      - JSON does not conform to the FullBriefEnvelope schema (missing a
        required composite section, extra fields, out-of-bounds value, …)
    """
    if not path.exists():
        raise FullBriefLoadError(f"file does not exist: {path}")
    if not path.is_file():
        raise FullBriefLoadError(f"path is not a regular file: {path}")

    try:
        raw_text = path.read_text(encoding="utf-8")
    except OSError as exc:
        raise FullBriefLoadError(f"could not read {path}: {exc}") from exc

    try:
        raw = json.loads(raw_text)
    except json.JSONDecodeError as exc:
        raise FullBriefLoadError(f"malformed JSON in {path}: {exc}") from exc

    # Explicit brief_type guard BEFORE schema validation: a wrong-type
    # artifact (Pass C `theme_event` / Pass E `attention`) is a clearer,
    # more actionable error than the wall of pydantic field errors that
    # `extra="forbid"` would otherwise produce. brief_type is optional in
    # the payload (it has a model default), so only reject when it is
    # present AND wrong — absent means "let the schema fill the default".
    if isinstance(raw, dict):
        brief_type = raw.get("brief_type")
        if brief_type is not None and brief_type != "full_brief":
            raise FullBriefLoadError(
                f"not a Full Brief artifact (brief_type={brief_type!r}, "
                f"expected 'full_brief'): {path}"
            )

    try:
        return FullBriefEnvelope.model_validate(raw)
    except Exception as exc:  # noqa: BLE001 — pydantic ValidationError surface
        raise FullBriefLoadError(f"schema mismatch in {path}: {exc}") from exc


__all__ = [
    "FullBriefLoadError",
    "load_full_brief_from_path",
]
