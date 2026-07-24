"""Shared UTC-timestamp formatting for the daemon's JSON/log wire surfaces.

Several modules independently formatted `unix -> "YYYY-MM-DDTHH:MM:SSZ"` with
byte-identical bodies. This is the single home so the emitted wire format stays
identical everywhere and there is one place to change it.
"""

from __future__ import annotations

from datetime import datetime, timezone


def iso_from_unix(ts: int) -> str:
    """Unix seconds -> ISO-8601 UTC, second resolution, trailing 'Z'."""
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


__all__ = ["iso_from_unix"]
