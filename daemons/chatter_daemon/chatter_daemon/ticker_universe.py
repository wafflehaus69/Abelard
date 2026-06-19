"""Chatter's own US-equity universe — Finnhub /stock/symbol, SQLite 24h cache.

ATTENTION (Order 8) validates discovered ticker-shaped tokens against the real listed-
symbol set, so junk strings drop BEFORE counting. Chatter builds its OWN cache (biz's
`universe_cache` is biz-local, constraint #5) — a separate table in the same state DB
as the Order-7 baseline. Resolution order: fresh cache -> live Finnhub -> optional
static fallback. A live failure with no fallback fails loud; with a fallback it threads
a warning so the degradation is never silent.

The Finnhub key is env-only and never logged: the injected HttpClient redacts `token=`
in URLs, and `fetch_us_symbols_live` redacts the key defensively from any error string.
"""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .baseline import transaction
from .errors import ChatterDaemonError

FINNHUB_SYMBOL_ENDPOINT = "https://finnhub.io/api/v1/stock/symbol"

_SCHEMA = """
CREATE TABLE IF NOT EXISTS universe_cache (
    id           INTEGER PRIMARY KEY CHECK (id = 1),
    symbols_json TEXT NOT NULL,
    source       TEXT NOT NULL,
    fetched_at   INTEGER NOT NULL
);
"""


class UniverseError(ChatterDaemonError):
    def __init__(self, message: str) -> None:
        super().__init__(message, stage="ticker_universe")


@dataclass(frozen=True)
class UniverseResult:
    symbols: frozenset[str]
    source: str  # "cache" | "finnhub" | "static_fallback"
    warning: str | None = None


def init_universe_table(conn: sqlite3.Connection) -> None:
    """Create the universe_cache table if absent. Idempotent."""
    conn.executescript(_SCHEMA)


def read_cached_universe(
    conn: sqlite3.Connection, *, ttl_s: int, now: int
) -> set[str] | None:
    row = conn.execute(
        "SELECT symbols_json, fetched_at FROM universe_cache WHERE id = 1"
    ).fetchone()
    if row is None:
        return None
    if now - int(row["fetched_at"]) > ttl_s:
        return None
    return set(json.loads(row["symbols_json"]))


def write_cached_universe(
    conn: sqlite3.Connection, *, symbols: set[str], source: str, now: int
) -> None:
    symbols_json = json.dumps(sorted(symbols), separators=(",", ":"))
    with transaction(conn):
        conn.execute(
            "INSERT INTO universe_cache (id, symbols_json, source, fetched_at) "
            "VALUES (1, ?, ?, ?) "
            "ON CONFLICT(id) DO UPDATE SET symbols_json=excluded.symbols_json, "
            "source=excluded.source, fetched_at=excluded.fetched_at",
            (symbols_json, source, int(now)),
        )


def fetch_us_symbols_live(client: Any, api_key: str) -> set[str]:
    """Pull the US symbol set from Finnhub via the injected client. Raises
    `UniverseError` on failure; never lets the key reach the message."""
    if not api_key:
        raise UniverseError("FINNHUB_API_KEY is empty")
    try:
        data = client.get_json(
            FINNHUB_SYMBOL_ENDPOINT, params={"exchange": "US", "token": api_key}
        )
    except Exception as exc:  # transport / tier / decode — redact then fail loud
        msg = str(exc).replace(api_key, "***") if api_key else str(exc)
        raise UniverseError(f"Finnhub /stock/symbol request failed: {msg}") from None
    if not isinstance(data, list):
        raise UniverseError("Finnhub /stock/symbol did not return a list")
    symbols = {
        str(row["symbol"]).upper()
        for row in data
        if isinstance(row, dict) and row.get("symbol")
    }
    if not symbols:
        raise UniverseError("Finnhub /stock/symbol returned an empty list")
    return symbols


def load_static_fallback(path: Path) -> set[str]:
    if not path.exists():
        raise UniverseError(f"static symbol fallback not found: {path}")
    symbols: set[str] = set()
    for line in path.read_text(encoding="utf-8").splitlines():
        token = line.strip()
        if token and not token.startswith("#"):
            symbols.add(token.upper())
    if not symbols:
        raise UniverseError(f"static symbol fallback is empty: {path}")
    return symbols


def load_universe(
    conn: sqlite3.Connection,
    *,
    client: Any,
    api_key: str,
    ttl_s: int,
    now: int,
    fallback_path: Path | None = None,
) -> UniverseResult:
    """Return the validation universe: fresh cache, else live Finnhub, else the
    static fallback (if one is configured). A live failure with no fallback raises."""
    cached = read_cached_universe(conn, ttl_s=ttl_s, now=now)
    if cached is not None:
        return UniverseResult(frozenset(cached), "cache")

    try:
        symbols = fetch_us_symbols_live(client, api_key)
    except UniverseError as exc:
        if fallback_path is not None and Path(fallback_path).exists():
            symbols = load_static_fallback(Path(fallback_path))
            write_cached_universe(conn, symbols=symbols, source="static_fallback", now=now)
            return UniverseResult(
                frozenset(symbols),
                "static_fallback",
                warning=f"ticker_universe: {exc}; used static fallback",
            )
        raise  # no fallback -> fail loud

    write_cached_universe(conn, symbols=symbols, source="finnhub", now=now)
    return UniverseResult(frozenset(symbols), "finnhub")


__all__ = [
    "FINNHUB_SYMBOL_ENDPOINT",
    "UniverseError",
    "UniverseResult",
    "fetch_us_symbols_live",
    "init_universe_table",
    "load_static_fallback",
    "load_universe",
    "read_cached_universe",
    "write_cached_universe",
]
