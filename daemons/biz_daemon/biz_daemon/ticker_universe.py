"""US-equity ticker universe from Finnhub, SQLite-cached with a 24h TTL.

Source: Finnhub `GET /stock/symbol?exchange=US`, keyed by FINNHUB_API_KEY
(env only, never logged). One network pull per day max.

HYPOTHESIS (verify against a live free-tier key at build time): the free tier
exposes /stock/symbol. If it does not, the static fallback file under data/ is
used instead. The fallback is wired either way so the build never blocks on
the live endpoint.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path

import requests

from .config import BizDaemonError

FINNHUB_SYMBOL_ENDPOINT = "https://finnhub.io/api/v1/stock/symbol"

_log = logging.getLogger("biz_daemon.universe")


class UniverseError(BizDaemonError):
    def __init__(self, message: str) -> None:
        super().__init__(message, stage="ticker_universe")


@dataclass(frozen=True)
class UniverseResult:
    symbols: frozenset[str]
    source: str  # "cache" | "finnhub" | "static_fallback"
    warning: str | None = None


def fetch_us_symbols_live(
    api_key: str,
    *,
    session: requests.Session | None = None,
    timeout: float = 10.0,
) -> set[str]:
    """Pull the US symbol set from Finnhub. Raises UniverseError on failure.

    The API key is sent as a query param but is never placed into any raised
    message or log line.
    """
    if not api_key:
        raise UniverseError("FINNHUB_API_KEY is empty")
    sess = session or requests.Session()
    try:
        resp = sess.get(
            FINNHUB_SYMBOL_ENDPOINT,
            params={"exchange": "US", "token": api_key},
            timeout=timeout,
        )
    except requests.RequestException as exc:
        # str(exc) can echo the URL (with token) — redact defensively.
        raise UniverseError(
            f"Finnhub /stock/symbol request failed: {_redact(str(exc), api_key)}"
        ) from None

    if resp.status_code in (401, 403):
        raise UniverseError(
            f"Finnhub /stock/symbol returned {resp.status_code} "
            "(not available on this key/tier)"
        )
    if resp.status_code != 200:
        raise UniverseError(f"Finnhub /stock/symbol returned {resp.status_code}")

    try:
        data = resp.json()
    except ValueError as exc:
        raise UniverseError(f"Finnhub /stock/symbol returned malformed JSON: {exc}")
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
        if not token or token.startswith("#"):
            continue
        symbols.add(token.upper())
    if not symbols:
        raise UniverseError(f"static symbol fallback is empty: {path}")
    return symbols


def load_universe(
    conn,
    *,
    api_key: str,
    fallback_path: Path,
    ttl_s: int,
    now: int,
    session: requests.Session | None = None,
    timeout: float = 10.0,
) -> UniverseResult:
    """Return the universe: fresh cache, else live Finnhub, else static file.

    On a live-endpoint failure the static fallback is used and a warning is
    threaded back so the orchestrator can surface the degradation — we never
    silently pretend the live universe was loaded.
    """
    from . import storage

    cached = storage.read_cached_universe(conn, ttl_s=ttl_s, now=now)
    if cached is not None:
        return UniverseResult(symbols=frozenset(cached), source="cache")

    try:
        symbols = fetch_us_symbols_live(api_key, session=session, timeout=timeout)
    except UniverseError as exc:
        _log.warning("live universe unavailable, using static fallback: %s", exc)
        symbols = load_static_fallback(fallback_path)
        storage.write_cached_universe(
            conn, symbols=symbols, source="static_fallback", now=now
        )
        return UniverseResult(
            symbols=frozenset(symbols),
            source="static_fallback",
            warning=f"ticker_universe: {exc}; used static fallback",
        )

    storage.write_cached_universe(conn, symbols=symbols, source="finnhub", now=now)
    return UniverseResult(symbols=frozenset(symbols), source="finnhub")


def _redact(text: str, secret: str) -> str:
    return text.replace(secret, "***") if secret else text
