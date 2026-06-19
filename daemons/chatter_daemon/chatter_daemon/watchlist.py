"""Watchlist config primitive — Appendix A.

A watchlist is a named JSON file under `watchlists/`. Adding a list = dropping a
file; no code change. Each is loaded once and validated up front (Pydantic,
`extra="forbid"`) with fail-loud errors:

  - missing directory / missing named list / malformed JSON / non-object root /
    schema violation / empty ticker array  -> `WatchlistError` with a clear message.
  - domain check: `name` must match the file stem (filename without `.json`).

Per-ticker flags drive downstream behavior (honored from Order 2 on):
  - `name_match=False` -> ticker-only on free-text sources (collision-word names
    like CAT / DE / MU / APP / NOW, and ETFs whose name is noise).
  - `is_etf=True`      -> documents expected chatter/news silence (read-path hint).
  - `enabled=False`    -> excluded from scanning; a visible placeholder for an
                          unverified symbol (e.g. `P`, pending confirmation).
  - `names`            -> aliases, used TWO ways: free-text matching (only when
                          `name_match=True`) AND the Google Trends search query (for
                          ANY ticker that has one, even `name_match:false` — a
                          collision-word ticker's full company name is a fine search
                          term). May be empty for S&P names (resolved from the shared
                          alias map); filled by hand for non-S&P names (see `notes`).
  - `ambiguous_name`   -> the Trends search term is ambiguous (Apple / Oracle /
                          Caterpillar...); emits `noisy_query` (with REAL interest).
                          Independent of `name_match` (governs free-text, not queries).
  - `notes`            -> free-text annotation / scaffold TODO for the operator.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

from pydantic import BaseModel, ConfigDict, Field, field_validator

from .errors import ChatterDaemonError

WATCHLIST_NAME_RE = re.compile(r"^[a-z][a-z0-9_]*$")
SYMBOL_RE = re.compile(r"^[A-Z]{1,5}(?:\.[A-Z])?$")


class WatchlistError(ChatterDaemonError):
    def __init__(self, message: str) -> None:
        super().__init__(message, stage="watchlist")


class TickerSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")

    symbol: str
    names: list[str] = Field(default_factory=list)
    name_match: bool = True
    is_etf: bool = False
    enabled: bool = True
    # Trends-only: the company name is an ambiguous search term (Apple, Oracle,
    # Caterpillar...). Drives the `noisy_query` flag; independent of `name_match`,
    # which governs FREE-TEXT matching, not search queries.
    ambiguous_name: bool = False
    notes: str | None = None

    @field_validator("symbol")
    @classmethod
    def _symbol_format(cls, v: str) -> str:
        if not SYMBOL_RE.match(v):
            raise ValueError(
                f"symbol must match {SYMBOL_RE.pattern} (1-5 uppercase letters, "
                f"optional .CLASS suffix); got {v!r}"
            )
        return v

    @field_validator("names")
    @classmethod
    def _names_non_empty(cls, names: list[str]) -> list[str]:
        for n in names:
            if not isinstance(n, str) or not n.strip():
                raise ValueError("each name alias must be a non-empty string")
        return names


class WatchlistConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str
    tickers: list[TickerSpec] = Field(min_length=1)

    @field_validator("name")
    @classmethod
    def _name_format(cls, v: str) -> str:
        if not WATCHLIST_NAME_RE.match(v):
            raise ValueError(
                "watchlist name must be snake_case (lowercase, digits, underscores; "
                f"starts with a letter); got {v!r}"
            )
        return v

    @field_validator("tickers")
    @classmethod
    def _symbols_unique(cls, tickers: list[TickerSpec]) -> list[TickerSpec]:
        syms = [t.symbol for t in tickers]
        dupes = sorted({s for s in syms if syms.count(s) > 1})
        if dupes:
            raise ValueError(f"duplicate symbols in watchlist: {dupes}")
        return tickers

    @property
    def active_tickers(self) -> list[TickerSpec]:
        """Tickers eligible for scanning (placeholders with enabled=False excluded)."""
        return [t for t in self.tickers if t.enabled]


def load_watchlist(name: str, *, watchlists_dir: Path) -> WatchlistConfig:
    """Load and validate a single watchlist by name. Fail-loud on every bad path."""
    if not watchlists_dir.is_dir():
        raise WatchlistError(f"watchlists directory not found: {watchlists_dir}")
    path = watchlists_dir / f"{name}.json"
    if not path.is_file():
        raise WatchlistError(f"watchlist not found: {name!r} (looked for {path})")
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise WatchlistError(f"invalid JSON in {path}: {exc}") from exc
    if not isinstance(raw, dict):
        raise WatchlistError(f"watchlist JSON root must be an object in {path}")
    try:
        wl = WatchlistConfig.model_validate(raw)
    except Exception as exc:  # pydantic ValidationError -> loud, with the detail
        raise WatchlistError(f"watchlist validation failed for {path}: {exc}") from exc
    if wl.name != path.stem:
        raise WatchlistError(
            f"watchlist name {wl.name!r} does not match filename stem "
            f"{path.stem!r} (file: {path})"
        )
    return wl


def load_all_watchlists(watchlists_dir: Path) -> list[WatchlistConfig]:
    """Load every `*.json` watchlist in the directory, sorted by name."""
    if not watchlists_dir.is_dir():
        raise WatchlistError(f"watchlists directory not found: {watchlists_dir}")
    paths = sorted(watchlists_dir.glob("*.json"))
    if not paths:
        raise WatchlistError(f"no watchlist files (*.json) in {watchlists_dir}")
    lists = [load_watchlist(p.stem, watchlists_dir=watchlists_dir) for p in paths]
    names = [w.name for w in lists]
    dupes = sorted({n for n in names if names.count(n) > 1})
    if dupes:
        raise WatchlistError(f"duplicate watchlist names in {watchlists_dir}: {dupes}")
    return lists
