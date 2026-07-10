"""Watchlist config primitive — Appendix A.

A watchlist is a named JSON **or CSV** file under `watchlists/` (`{name}.json`, or the
human-editable `{name}.csv` — one format per name; both present is ambiguous and fails
loud). Adding a list = dropping a file; no code change. Each is loaded once and validated
up front (Pydantic, `extra="forbid"`) with fail-loud errors:

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

import csv
import io
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


# Portfolio CSV — the human-editable form (a spreadsheet round-trips it). `names` are
# pipe-separated within their cell; booleans are true/false (blank -> the field default);
# `notes` may contain commas (csv quoting handles them). Columns mirror TickerSpec.
_CSV_HEADER = ["symbol", "names", "name_match", "is_etf", "enabled", "ambiguous_name", "notes"]


def _watchlist_from_json(path: Path) -> WatchlistConfig:
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise WatchlistError(f"invalid JSON in {path}: {exc}") from exc
    if not isinstance(raw, dict):
        raise WatchlistError(f"watchlist JSON root must be an object in {path}")
    try:
        return WatchlistConfig.model_validate(raw)
    except Exception as exc:  # pydantic ValidationError -> loud, with the detail
        raise WatchlistError(f"watchlist validation failed for {path}: {exc}") from exc


def _csv_bool(row: dict, key: str, default: bool) -> bool:
    v = (row.get(key) or "").strip().lower()
    if not v:
        return default
    return v in ("1", "true", "yes", "y", "t")


def _ticker_spec_from_csv_row(row: dict) -> dict:
    names_cell = (row.get("names") or "").strip()
    names = [n.strip() for n in names_cell.split("|") if n.strip()] if names_cell else []
    return {
        "symbol": (row.get("symbol") or "").strip(),
        "names": names,
        "name_match": _csv_bool(row, "name_match", True),
        "is_etf": _csv_bool(row, "is_etf", False),
        "enabled": _csv_bool(row, "enabled", True),
        "ambiguous_name": _csv_bool(row, "ambiguous_name", False),
        "notes": (row.get("notes") or "").strip() or None,
    }


def _watchlist_from_csv(path: Path) -> WatchlistConfig:
    """Parse a portfolio CSV into a validated WatchlistConfig. A blank `symbol` row is skipped
    (trailing spreadsheet lines); every other row is a ticker. Fail-loud on a missing header or
    a schema violation, exactly like the JSON path."""
    try:
        text = path.read_text(encoding="utf-8-sig")  # tolerate a spreadsheet BOM
    except OSError as exc:
        raise WatchlistError(f"cannot read {path}: {exc}") from exc
    reader = csv.DictReader(io.StringIO(text))
    if not reader.fieldnames or "symbol" not in reader.fieldnames:
        raise WatchlistError(f"portfolio CSV {path} needs a header row with a 'symbol' column")
    specs = [
        _ticker_spec_from_csv_row(row) for row in reader if (row.get("symbol") or "").strip()
    ]
    try:
        return WatchlistConfig(name=path.stem, tickers=specs)
    except Exception as exc:  # pydantic ValidationError -> loud, with the detail
        raise WatchlistError(f"portfolio validation failed for {path}: {exc}") from exc


def write_watchlist_csv(wl: WatchlistConfig, path: Path) -> None:
    """Write a WatchlistConfig to the portfolio CSV format — the inverse of
    `_watchlist_from_csv`, so a JSON list can be exported for spreadsheet editing."""
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(_CSV_HEADER)
    for t in wl.tickers:
        writer.writerow([
            t.symbol,
            "|".join(t.names),
            str(t.name_match).lower(),
            str(t.is_etf).lower(),
            str(t.enabled).lower(),
            str(t.ambiguous_name).lower(),
            t.notes or "",
        ])
    path.write_text(buf.getvalue(), encoding="utf-8")


def load_watchlist(name: str, *, watchlists_dir: Path) -> WatchlistConfig:
    """Load and validate a single watchlist by name — from `{name}.csv` (the human-editable
    portfolio) or `{name}.json`. Fail-loud on every bad path; having BOTH formats is ambiguous."""
    if not watchlists_dir.is_dir():
        raise WatchlistError(f"watchlists directory not found: {watchlists_dir}")
    json_path = watchlists_dir / f"{name}.json"
    csv_path = watchlists_dir / f"{name}.csv"
    if json_path.is_file() and csv_path.is_file():
        raise WatchlistError(
            f"ambiguous watchlist {name!r}: both {csv_path.name} and {json_path.name} exist in "
            f"{watchlists_dir} — keep exactly one format"
        )
    if csv_path.is_file():
        wl, src = _watchlist_from_csv(csv_path), csv_path
    elif json_path.is_file():
        wl, src = _watchlist_from_json(json_path), json_path
    else:
        raise WatchlistError(
            f"watchlist not found: {name!r} (looked for {csv_path} / {json_path})"
        )
    if wl.name != src.stem:
        raise WatchlistError(
            f"watchlist name {wl.name!r} does not match filename stem {src.stem!r} (file: {src})"
        )
    return wl


def load_all_watchlists(watchlists_dir: Path) -> list[WatchlistConfig]:
    """Load every watchlist in the directory — `*.json` and/or `*.csv` — sorted by name. A name
    present as BOTH .json and .csv is ambiguous (fail loud)."""
    if not watchlists_dir.is_dir():
        raise WatchlistError(f"watchlists directory not found: {watchlists_dir}")
    paths = sorted([*watchlists_dir.glob("*.json"), *watchlists_dir.glob("*.csv")])
    if not paths:
        raise WatchlistError(f"no watchlist files (*.json / *.csv) in {watchlists_dir}")
    stems = [p.stem for p in paths]
    dupe_stems = sorted({s for s in stems if stems.count(s) > 1})
    if dupe_stems:
        raise WatchlistError(
            f"watchlist(s) present in BOTH .json and .csv in {watchlists_dir}: {dupe_stems} "
            f"— keep exactly one format each"
        )
    lists = [load_watchlist(p.stem, watchlists_dir=watchlists_dir) for p in paths]
    names = [w.name for w in lists]
    dupes = sorted({n for n in names if names.count(n) > 1})
    if dupes:
        raise WatchlistError(f"duplicate watchlist names in {watchlists_dir}: {dupes}")
    return lists
