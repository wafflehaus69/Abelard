"""Response envelope — every capability returns this shape, no exceptions.

All envelope-level meta (status, data_completeness, error_detail, warnings)
lives at the top. `data` is the pure capability-specific payload — never
mixed with metadata, so Abelard's schema per capability stays clean.

Warnings are structured, not prose, so they are programmatically
inspectable. Reasons are a closed enum — extend the enum rather than
inventing ad-hoc strings.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Literal

Status = Literal["ok", "error", "rate_limited", "not_found"]
Source = Literal["edgar", "finnhub"]

# "complete"      — all fields this capability normally returns are populated
# "partial"       — one or more fields null/degraded; see warnings for which
# "metadata_only" — only metadata available (e.g. SEC filing URL without body)
# "none"          — no usable data (always paired with status != "ok")
Completeness = Literal["complete", "partial", "metadata_only", "none"]

# Closed set of warning reasons. Abelard can pattern-match on these without
# parsing prose. Add new members here when a capability needs them.
WarningReason = Literal[
    "not_available_on_free_tier",
    "upstream_timeout",
    "upstream_error",
    "rate_limited",
    "not_found",
    "stale_data",
    "parse_error",
    "missing_field",
    "insufficient_history",
]


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def make_warning(
    *,
    field: str,
    reason: WarningReason,
    source: Source,
    suggestion: str | None = None,
) -> dict[str, Any]:
    """Construct a single structured warning. `suggestion` is optional."""
    warning: dict[str, Any] = {"field": field, "reason": reason, "source": source}
    if suggestion is not None:
        warning["suggestion"] = suggestion
    return warning


def build_ok(
    data: Any,
    *,
    source: Source,
    data_completeness: Completeness = "complete",
    warnings: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    if data_completeness == "none":
        raise ValueError("data_completeness='none' is only valid on error envelopes")
    return {
        "status": "ok",
        "data_completeness": data_completeness,
        "data": data,
        "source": source,
        "timestamp": _now_iso(),
        "error_detail": None,
        "warnings": list(warnings or []),
    }


def build_error(
    *,
    status: Status,
    source: Source,
    detail: str,
    data: Any = None,
    data_completeness: Completeness = "none",
    warnings: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    if status == "ok":
        raise ValueError("build_error cannot produce an ok envelope; use build_ok")
    return {
        "status": status,
        "data_completeness": data_completeness,
        "data": data,
        "source": source,
        "timestamp": _now_iso(),
        "error_detail": detail,
        "warnings": list(warnings or []),
    }
