"""SQLite data-access layer.

This module owns:

  - Connection construction (always WAL + FK on).
  - Schema initialization from `schema/initial.sql`.
  - Migration tracking via the `schema_version` table — even at v1, the
    pattern is in place so a future v2 migration is a one-line addition
    to MIGRATIONS rather than a structural change.
  - The themes-registry sync used by `news-watch-daemon themes load` and
    queried by `news-watch-daemon themes list`.
  - Daemon-heartbeat helpers used by `news-watch-daemon status`.

Scrape / synthesis / alert layers will add their own helpers in
subsequent briefs. This module stays infrastructure-only; it does NOT
implement business logic.
"""

from __future__ import annotations

import json
import sqlite3
import time
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator

from .theme_config import ThemeConfig


# ---------- migration registry ------------------------------------------

# Each tuple: (version, description, sql_filename_or_None).
# `None` is reserved for migrations defined inline in code (none yet).
MIGRATIONS: tuple[tuple[int, str, str | None], ...] = (
    (1, "initial schema", "initial.sql"),
    (2, "headlines dedupe composite index", "v2_dedupe_composite_index.sql"),
)

# Resolves relative to the installed package; the schema dir sits beside
# the package root at deploy time (and the project root in dev).
_SCHEMA_DIR = Path(__file__).resolve().parent.parent.parent / "schema"


# ---------- timestamps --------------------------------------------------


def _now_pair() -> tuple[int, str]:
    """Return matched (unix_seconds, iso_8601_utc) for paired timestamps."""
    dt = datetime.now(timezone.utc)
    return int(dt.timestamp()), dt.isoformat(timespec="seconds").replace("+00:00", "Z")


# ---------- connection --------------------------------------------------


def connect(db_path: Path) -> sqlite3.Connection:
    """Open a SQLite connection with WAL + FK enforcement.

    Parent directory is created if missing. Caller owns the connection
    lifecycle. Use `transaction(conn)` for atomic writes.
    """
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path), isolation_level=None, timeout=10.0)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn


@contextmanager
def transaction(conn: sqlite3.Connection) -> Iterator[sqlite3.Connection]:
    """Wrap a block of writes in a single transaction. Commits on success."""
    conn.execute("BEGIN")
    try:
        yield conn
    except Exception:
        conn.execute("ROLLBACK")
        raise
    else:
        conn.execute("COMMIT")


# ---------- schema + migrations ----------------------------------------


def _schema_file(filename: str) -> Path:
    path = _SCHEMA_DIR / filename
    if not path.is_file():
        raise FileNotFoundError(f"schema file not found: {path}")
    return path


def _current_version(conn: sqlite3.Connection) -> int:
    """Highest applied migration version, or 0 if schema_version is absent."""
    row = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='schema_version'"
    ).fetchone()
    if row is None:
        return 0
    row = conn.execute("SELECT MAX(version) AS v FROM schema_version").fetchone()
    return row["v"] or 0


def init_db(conn: sqlite3.Connection) -> int:
    """Apply all migrations the database hasn't seen. Returns the new version.

    Safe to call repeatedly: idempotent. Each migration is recorded in
    schema_version atomically with its SQL.
    """
    current = _current_version(conn)
    target = MIGRATIONS[-1][0]
    if current >= target:
        return current

    for version, description, filename in MIGRATIONS:
        if version <= current:
            continue
        if filename is None:
            raise RuntimeError(f"migration {version} has no SQL file and no inline handler")
        sql = _schema_file(filename).read_text(encoding="utf-8")
        unix, iso = _now_pair()
        # executescript() finalizes any active transaction and runs the script
        # in its own implicit transaction; it cannot be nested in our wrapper.
        # The subsequent INSERT runs autocommit; the schema-apply / version-
        # stamp gap is microseconds and would be detected at the next run.
        conn.executescript(sql)
        conn.execute(
            "INSERT INTO schema_version (version, applied_at_unix, applied_at, description) "
            "VALUES (?, ?, ?, ?)",
            (version, unix, iso, description),
        )
    return MIGRATIONS[-1][0]


def schema_version(conn: sqlite3.Connection) -> int:
    """Public read-only accessor for the current schema version."""
    return _current_version(conn)


# ---------- themes registry --------------------------------------------


@dataclass(frozen=True)
class ThemeRegistryEntry:
    theme_id: str
    display_name: str
    status: str
    config_hash: str
    loaded_at_unix: int
    loaded_at: str


def upsert_themes(conn: sqlite3.Connection, themes: list[ThemeConfig]) -> dict[str, int]:
    """Insert or update each theme into the registry. Returns counts.

    Returned dict keys: `inserted`, `updated`, `unchanged`. A theme is
    considered `unchanged` if its config_hash matches the stored row.
    Themes present in the registry but absent from the input list are
    NOT removed — archiving is an explicit action handled elsewhere.
    """
    unix, iso = _now_pair()
    inserted = updated = unchanged = 0
    with transaction(conn):
        for theme in themes:
            existing = conn.execute(
                "SELECT config_hash FROM themes WHERE theme_id = ?",
                (theme.theme_id,),
            ).fetchone()
            new_hash = theme.config_hash()
            if existing is None:
                conn.execute(
                    "INSERT INTO themes "
                    "(theme_id, display_name, status, config_hash, loaded_at_unix, loaded_at) "
                    "VALUES (?, ?, ?, ?, ?, ?)",
                    (theme.theme_id, theme.display_name, theme.status, new_hash, unix, iso),
                )
                inserted += 1
            elif existing["config_hash"] != new_hash:
                conn.execute(
                    "UPDATE themes SET display_name=?, status=?, config_hash=?, "
                    "loaded_at_unix=?, loaded_at=? WHERE theme_id=?",
                    (theme.display_name, theme.status, new_hash, unix, iso, theme.theme_id),
                )
                updated += 1
            else:
                unchanged += 1
    return {"inserted": inserted, "updated": updated, "unchanged": unchanged}


def list_themes(conn: sqlite3.Connection) -> list[ThemeRegistryEntry]:
    """All themes in the registry, ordered by theme_id."""
    rows = conn.execute(
        "SELECT theme_id, display_name, status, config_hash, loaded_at_unix, loaded_at "
        "FROM themes ORDER BY theme_id"
    ).fetchall()
    return [
        ThemeRegistryEntry(
            theme_id=r["theme_id"],
            display_name=r["display_name"],
            status=r["status"],
            config_hash=r["config_hash"],
            loaded_at_unix=r["loaded_at_unix"],
            loaded_at=r["loaded_at"],
        )
        for r in rows
    ]


# ---------- heartbeat helpers ------------------------------------------


def record_heartbeat(
    conn: sqlite3.Connection,
    *,
    component: str,
    status: str,
    duration_ms: int | None = None,
    error_detail: str | None = None,
) -> None:
    """Upsert the most recent run of a daemon component into daemon_heartbeat."""
    unix, iso = _now_pair()
    with transaction(conn):
        conn.execute(
            "INSERT INTO daemon_heartbeat "
            "(component, last_run_unix, last_run, last_status, last_duration_ms, last_error_detail) "
            "VALUES (?, ?, ?, ?, ?, ?) "
            "ON CONFLICT(component) DO UPDATE SET "
            "  last_run_unix=excluded.last_run_unix, "
            "  last_run=excluded.last_run, "
            "  last_status=excluded.last_status, "
            "  last_duration_ms=excluded.last_duration_ms, "
            "  last_error_detail=excluded.last_error_detail",
            (component, unix, iso, status, duration_ms, error_detail),
        )


def read_heartbeats(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    """All daemon-component heartbeats, ordered by component name."""
    rows = conn.execute(
        "SELECT component, last_run_unix, last_run, last_status, "
        "last_duration_ms, last_error_detail "
        "FROM daemon_heartbeat ORDER BY component"
    ).fetchall()
    return [dict(r) for r in rows]


def read_source_health(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    """All source_health rows, ordered by source name."""
    rows = conn.execute(
        "SELECT source, last_successful_fetch_unix, last_successful_fetch, "
        "last_attempt_unix, last_attempt, last_status, last_error_detail, "
        "consecutive_failure_count "
        "FROM source_health ORDER BY source"
    ).fetchall()
    return [dict(r) for r in rows]


# ---------- JSON column helpers ----------------------------------------


def to_json_column(value: Any) -> str | None:
    """Serialize a JSON-compatible Python value for an `*_json` column."""
    if value is None:
        return None
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def from_json_column(value: str | None) -> Any:
    """Parse an `*_json` column back to a Python value."""
    if value is None:
        return None
    return json.loads(value)


__all__ = [
    "MIGRATIONS",
    "ThemeRegistryEntry",
    "connect",
    "from_json_column",
    "init_db",
    "list_themes",
    "read_heartbeats",
    "read_source_health",
    "record_heartbeat",
    "schema_version",
    "to_json_column",
    "transaction",
    "upsert_themes",
]
