"""PS-1 — reference series. VIX, WTI, and the benchmark ETFs.

**Ruled 2026-09-02:** Yahoo ``CL=F`` is the working daily WTI series with a roll
flag; FRED ``DCOILWTICO`` is kept as the lagging validator. The ``|return| > 4%``
roll heuristic is **dropped** — measured on the one roll in the sample it had a
0% hit rate (the 2026-08-21 roll moved −0.88%) and would instead have fired on
genuine oil moves, which is worse than having no flag at all.

**Roll detection is exact and free.** The chart response's
``meta.shortName`` names the front-month contract ("Crude Oil Oct 26"). A change
between sessions IS the roll; nothing is inferred from price action. Because it
rides on the request we already make, it costs nothing.

**Why FRED at all, if it lags.** It is the citable official series and it agrees
with Yahoo where they overlap (``^VIX`` and ``VIXCLS`` matched exactly on every
shared session at recon). So it is the cross-vendor check for the two series that
have one — and the direction of blame is fixed in advance: on a NON-roll session,
a divergence beyond the band indicts ``CL=F``, not FRED. FRED is slow, not wrong.
"""

from __future__ import annotations

import csv
import io
import re
import sqlite3
import time
from dataclasses import dataclass, field
from typing import Sequence

from ..http_client import HttpClient
from .schema import PriceStoreError
from .vendor import VendorError, YahooVendor

FRED_CSV = "https://fred.stlouisfed.org/graph/fredgraph.csv"

# (series_id, vendor symbol) for the daily series taken from the chart endpoint.
YAHOO_SERIES = (
    ("VIX", "^VIX"),
    ("WTI", "CL=F"),
    ("SPY", "SPY"),
    ("IVV", "IVV"),
    ("RSP", "RSP"),
    ("XLE", "XLE"),
)
# (series_id, FRED id) for the official validators.
FRED_SERIES = (("VIX", "VIXCLS"), ("WTI", "DCOILWTICO"))

# Non-roll divergence beyond this indicts CL=F. Wide enough that ordinary
# spot-vs-front-month basis passes; narrow enough that a broken series shows.
WTI_DIVERGENCE_USD = 3.0
VIX_DIVERGENCE = 0.75


class ReferenceError(PriceStoreError):
    def __init__(self, message: str) -> None:
        super().__init__(message, stage="reference")


@dataclass
class ReferenceReport:
    written: int = 0
    rolls: list[tuple[str, str]] = field(default_factory=list)
    divergences: list[tuple[str, str, float, float, float]] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)

    def render(self) -> str:
        out = ["[reference] rows written: {}".format(self.written)]
        out.append("[reference] contract rolls detected: {}".format(len(self.rolls)))
        for d, c in self.rolls[-5:]:
            out.append("     {} -> {}".format(d, c))
        out.append("[reference] validator divergences: {}".format(len(self.divergences)))
        for sid, d, a, b, gap in self.divergences[:10]:
            out.append("     {} {} yahoo={:.4f} fred={:.4f} gap={:+.4f}"
                       "  <-- indicts the Yahoo series, not FRED".format(sid, d, a, b, gap))
        for e in self.errors:
            out.append("[reference] ERROR {}".format(e))
        return "\n".join(out)


def _contract_of(short_name: str | None) -> str | None:
    """'Crude Oil Oct 26' -> 'Oct 26'. String parsing, so it is guarded: an
    unrecognised shape returns None rather than a wrong contract."""
    if not short_name:
        return None
    m = re.search(r"([A-Z][a-z]{2})\s+(\d{2})$", short_name.strip())
    return "{} {}".format(m.group(1), m.group(2)) if m else None


def parse_fred_csv(text: str) -> list[tuple[str, float]]:
    """FRED serves '.' for a missing observation. Those are skipped here rather
    than written as nulls: FRED's gaps are holidays, not vendor failures."""
    rows = []
    reader = csv.reader(io.StringIO(text))
    header = next(reader, None)
    if not header or "observation_date" not in header[0]:
        raise ReferenceError("unexpected FRED header: {!r}".format(header))
    for row in reader:
        if len(row) < 2 or row[1].strip() in (".", ""):
            continue
        try:
            rows.append((row[0].strip(), float(row[1])))
        except ValueError:
            continue
    return rows


def fetch_fred(client: HttpClient, fred_id: str) -> list[tuple[str, float]]:
    return parse_fred_csv(client.get_text(FRED_CSV, params={"id": fred_id}))


def _write(con: sqlite3.Connection, series_id: str, date: str, value: float | None,
           source: str, fetched_at: int, contract: str | None = None,
           roll: int = 0, status: str = "ok") -> None:
    con.execute(
        "INSERT OR REPLACE INTO reference_series (series_id, date, value, contract,"
        " roll_flag, status, source, fetched_at) VALUES (?,?,?,?,?,?,?,?)",
        (series_id, date, value, contract, roll, status, source, fetched_at))


def sync_yahoo(
    con: sqlite3.Connection,
    vendor: YahooVendor,
    start: str,
    end: str,
    series: Sequence[tuple[str, str]] = YAHOO_SERIES,
) -> ReferenceReport:
    """Daily reference series. WTI additionally carries its front-month contract
    and a roll flag on the session the contract changes."""
    rep = ReferenceReport()
    for series_id, symbol in series:
        try:
            s = vendor.fetch(symbol, start, end)
        except VendorError as exc:
            rep.errors.append("{}: {}".format(series_id, exc))
            continue
        contract = _contract_of(s.short_name)
        prev_contract = con.execute(
            "SELECT contract FROM reference_series WHERE series_id=? AND contract"
            " IS NOT NULL ORDER BY date DESC LIMIT 1", (series_id,)).fetchone()
        prev_contract = prev_contract[0] if prev_contract else None

        for i, b in enumerate(s.bars):
            is_last = i == len(s.bars) - 1
            # Only the newest bar can be attributed to the CURRENT front month;
            # meta.shortName describes the series now, not each historical bar.
            c = contract if (is_last and contract) else None
            roll = 1 if (c and prev_contract and c != prev_contract) else 0
            if roll:
                rep.rolls.append((b.date, c))
            _write(con, series_id, b.date, b.close, "yahoo_v8", s.fetched_at,
                   c, roll, "ok" if b.close is not None else "vendor_null")
            rep.written += 1
    con.commit()
    return rep


def sync_fred(
    con: sqlite3.Connection,
    client: HttpClient,
    series: Sequence[tuple[str, str]] = FRED_SERIES,
) -> ReferenceReport:
    rep = ReferenceReport()
    now = int(time.time())
    for series_id, fred_id in series:
        try:
            rows = fetch_fred(client, fred_id)
        except Exception as exc:  # noqa: BLE001 - one bad series is not fatal
            rep.errors.append("{}: {}".format(fred_id, exc))
            continue
        for date, value in rows:
            _write(con, series_id, date, value, "fred_" + fred_id.lower(), now)
            rep.written += 1
    con.commit()
    return rep


def reconcile_validators(con: sqlite3.Connection) -> ReferenceReport:
    """Weekly: compare each Yahoo series against its FRED counterpart wherever
    both hold the same session.

    Roll sessions are exempt for WTI — the front month legitimately steps there,
    and FRED's spot does not. On every other session a gap beyond the band
    indicts the Yahoo series. FRED is the slow one, not the wrong one.
    """
    rep = ReferenceReport()
    bands = {"WTI": WTI_DIVERGENCE_USD, "VIX": VIX_DIVERGENCE}
    for series_id, band in bands.items():
        for r in con.execute(
            "SELECT y.date, y.value AS yv, f.value AS fv, y.roll_flag"
            " FROM reference_series y JOIN reference_series f"
            "   ON f.series_id = y.series_id AND f.date = y.date"
            " WHERE y.series_id=? AND y.source='yahoo_v8'"
            "   AND f.source LIKE 'fred_%'"
            "   AND y.value IS NOT NULL AND f.value IS NOT NULL",
            (series_id,),
        ):
            if r["roll_flag"]:
                continue
            gap = r["yv"] - r["fv"]
            if abs(gap) > band:
                rep.divergences.append((series_id, r["date"], r["yv"], r["fv"], gap))
    rep.divergences.sort(key=lambda x: x[1], reverse=True)
    return rep
