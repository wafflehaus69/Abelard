"""PS-1B Phase 2V.1 — the Tiingo verification adapter.

**Verification only.** Mando ruled 2026-09-02 that Yahoo stays primary and
Tiingo verifies: a free unmetered source is the right thing to depend on
nightly, and the metered one is the right thing to check it with. Nothing in
this module writes a price. The one exception the order carves out is the hole
FILL — a first write into a session the primary never delivered — and that lives
in ``verify.py`` behind G3's rule, not here.

**Header auth, never a query string.** Both work; only one is safe. A token in a
URL reaches access logs, proxy logs, and `Referer` headers on any redirect.
``http_client.redact_url`` scrubs it from *our* logs and can do nothing about
anyone else's. There is a test asserting no module in this package builds a
`token=` query parameter.

**Why the shape is better than Yahoo's and it is still not primary.** Tiingo
returns raw ``close`` directly, plus per-row ``splitFactor`` and ``divCash`` —
prices and the corporate-action feed in one response, with no reconstruction
step to get wrong. That is strictly the better contract. It is metered, and a
personal licence is a single point of failure, so it verifies.

**The meters are real and the code enforces them, not memory.** Read off the
account page 2026-09-02:

    requests/hour   50
    requests/day    1,000
    bytes/month     2 GB

There is no unique-symbol cap; an earlier assumption that there was would have
guarded a limit Tiingo does not impose while leaving the ones it does impose
open. Every call is logged to ``vendor_calls`` and a sweep that would breach any
meter refuses to start.
"""

from __future__ import annotations

import datetime as dt
import json
import sqlite3
import time
from dataclasses import dataclass, field
from typing import Any, Sequence

from ..http_client import HttpClient, NotFound, RateLimited, TransportError
from .schema import PriceStoreError

VENDOR = "tiingo"
BASE_URL = "https://api.tiingo.com/tiingo/daily/{symbol}/prices"
TEST_URL = "https://api.tiingo.com/api/test"

# Read from the account page, 2026-09-02. Not machine-readable: the API exposes
# no usage endpoint and returns no rate-limit headers, so these are constants
# that must be re-read by a human when the plan changes (E33: the page that
# shows them is a Mando surface).
LIMIT_PER_HOUR = 50
LIMIT_PER_DAY = 1_000
LIMIT_BYTES_PER_MONTH = 2 * 1024 ** 3

# Mando's hard floor, 2026-09-02: >= 72 s between requests inside a sweep, so a
# pathological retry loop cannot reach 50/hour. 72 x 18 names = 21.6 minutes,
# which fits the 21:00 slot behind the nightly append.
PACE_SECONDS = 72.0

# Cross-vendor comparison tolerance. Deliberately looser than
# reconstruct.FACT_EPS, which compares a value against itself across two fetches
# of ONE vendor where equality is exact. Yahoo serves float32-precision closes
# widened to float64 (94.16000366210938); Tiingo quotes to the cent.
CROSS_VENDOR_EPS = 1e-6


class TiingoError(PriceStoreError):
    def __init__(self, message: str, *, stage: str = "tiingo") -> None:
        super().__init__(message, stage=stage)


class TiingoUnknownSymbol(TiingoError):
    """404 — delisted, renamed, or never covered. Counted, never fatal."""

    def __init__(self, message: str) -> None:
        super().__init__(message, stage="tiingo_unknown_symbol")


class QuotaExceeded(TiingoError):
    """A planned sweep would breach a meter. Refuse before the first request,
    not after the fiftieth."""

    def __init__(self, message: str) -> None:
        super().__init__(message, stage="tiingo_quota")


@dataclass(frozen=True)
class TiingoBar:
    date: str
    raw_close: float | None
    adj_close: float | None
    split_factor: float
    div_cash: float
    volume: int | None


@dataclass(frozen=True)
class QuotaState:
    last_hour: int
    last_day: int
    month_bytes: int

    def render(self) -> str:
        return ("tiingo quota: {}/{} hour · {}/{} day · {:.2f}/{:.0f} MB month"
                .format(self.last_hour, LIMIT_PER_HOUR, self.last_day,
                        LIMIT_PER_DAY, self.month_bytes / 1e6,
                        LIMIT_BYTES_PER_MONTH / 1e6))


# ------------------------------------------------------------------- quota --

def quota_state(con: sqlite3.Connection, now: float | None = None) -> QuotaState:
    """Rolling meters, counted from the call log rather than remembered."""
    now = now or time.time()
    hour = con.execute(
        "SELECT COUNT(*) FROM vendor_calls WHERE vendor=? AND called_at > ?",
        (VENDOR, now - 3600)).fetchone()[0]
    day = con.execute(
        "SELECT COUNT(*) FROM vendor_calls WHERE vendor=? AND called_at > ?",
        (VENDOR, now - 86400)).fetchone()[0]
    month_start = dt.datetime.fromtimestamp(now, dt.timezone.utc).replace(
        day=1, hour=0, minute=0, second=0, microsecond=0).timestamp()
    month = con.execute(
        "SELECT COALESCE(SUM(bytes),0) FROM vendor_calls WHERE vendor=? AND called_at >= ?",
        (VENDOR, month_start)).fetchone()[0]
    return QuotaState(hour, day, int(month))


def check_quota(
    con: sqlite3.Connection,
    planned_requests: int,
    now: float | None = None,
) -> QuotaState:
    """Refuse a sweep that would breach a meter. Raises ``QuotaExceeded``.

    Checked once, up front, against the whole plan — not per request. A sweep
    that dies at request 43 of 60 has already spent the quota and left the store
    half-verified, which is worse than not starting.
    """
    st = quota_state(con, now)
    if st.last_hour + planned_requests > LIMIT_PER_HOUR:
        raise QuotaExceeded(
            "{} requests planned would take the hour to {}/{}".format(
                planned_requests, st.last_hour + planned_requests, LIMIT_PER_HOUR))
    if st.last_day + planned_requests > LIMIT_PER_DAY:
        raise QuotaExceeded(
            "{} requests planned would take the day to {}/{}".format(
                planned_requests, st.last_day + planned_requests, LIMIT_PER_DAY))
    if st.month_bytes >= LIMIT_BYTES_PER_MONTH:
        raise QuotaExceeded(
            "month-to-date bandwidth {:.2f} GB is at the {:.0f} GB ceiling"
            .format(st.month_bytes / 1e9, LIMIT_BYTES_PER_MONTH / 1e9))
    return st


# ------------------------------------------------------------------ adapter --

@dataclass
class TiingoVendor:
    """Paced, metered, header-authenticated reader for Tiingo's daily endpoint.

    ``con`` is required: every call is logged, and a vendor that cannot record
    what it spent cannot be trusted to stay inside a meter.
    """

    token: str
    con: sqlite3.Connection
    client: HttpClient = field(default_factory=lambda: HttpClient(
        user_agent="Abelard PS-1 prices (verification)"))
    pace_seconds: float = PACE_SECONDS
    run_asof: int = 0
    _last_call: float = field(default=0.0, repr=False)

    def __post_init__(self) -> None:
        if not self.token or len(self.token) < 20:
            raise TiingoError(
                "no usable TIINGO_API_TOKEN — the CLI resolves it from the "
                "environment; the library never reads env itself")

    def _headers(self) -> dict[str, str]:
        # Header, never a query parameter. See the module docstring.
        return {"Authorization": "Token " + self.token,
                "Content-Type": "application/json"}

    def _pace(self) -> None:
        wait = self.pace_seconds - (time.monotonic() - self._last_call)
        if wait > 0:
            time.sleep(wait)
        self._last_call = time.monotonic()

    def _log(self, symbol: str, size: int, status: int | None) -> None:
        self.con.execute(
            "INSERT INTO vendor_calls (vendor, called_at, symbol, bytes, status,"
            " run_asof) VALUES (?,?,?,?,?,?)",
            (VENDOR, time.time(), symbol, size, status, self.run_asof or None))
        self.con.commit()

    def ping(self) -> bool:
        body = self.client.get_json(TEST_URL, headers=self._headers())
        self._log("_test", len(json.dumps(body or {})), 200)
        return bool(body)

    def daily(self, symbol: str, start: str, end: str) -> list[TiingoBar]:
        """Daily rows for ``[start, end]`` inclusive.

        Returns raw ``close`` alongside ``adjClose``, ``splitFactor`` and
        ``divCash`` — everything the comparison needs from one request.
        """
        if start > end:
            raise TiingoError("{}: start {} after end {}".format(symbol, start, end))
        self._pace()
        url = BASE_URL.format(symbol=symbol.lower())
        params = {"startDate": start, "endDate": end, "format": "json"}
        try:
            body = self.client.get_json(url, params=params, headers=self._headers())
        except NotFound as exc:
            self._log(symbol, 0, 404)
            raise TiingoUnknownSymbol(
                "{}: unknown to Tiingo (404)".format(symbol)) from exc
        except RateLimited as exc:
            self._log(symbol, 0, 429)
            raise TiingoError("{}: rate limited — {}".format(symbol, exc)) from exc
        except TransportError as exc:
            self._log(symbol, 0, None)
            raise TiingoError("{}: transport — {}".format(symbol, exc)) from exc

        payload = json.dumps(body or [])
        self._log(symbol, len(payload), 200)
        if not isinstance(body, list):
            raise TiingoError(
                "{}: expected a list of daily rows, got {}".format(
                    symbol, type(body).__name__), stage="tiingo_schema")
        return [_row(r) for r in body]


def _row(r: dict[str, Any]) -> TiingoBar:
    return TiingoBar(
        date=str(r.get("date", ""))[:10],
        raw_close=r.get("close"),
        adj_close=r.get("adjClose"),
        # Tiingo carries the split on the session it takes effect, 1.0 otherwise.
        split_factor=float(r.get("splitFactor") or 1.0),
        div_cash=float(r.get("divCash") or 0.0),
        volume=r.get("volume"),
    )


# ------------------------------------------------------------------ symbols --

def vendor_symbol(con: sqlite3.Connection, instrument_id: str) -> str | None:
    """The ticker to ask Tiingo for, from ``ticker_aliases`` — never derived.

    Four notations are in play (PS-1-P0 §P0.4(v)) and string surgery on them is
    how `BRKB` became a second instrument during the Phase 2 build. Tiingo takes
    the dashed form, which is the alias the store already holds as 'vendor'.
    """
    row = con.execute(
        "SELECT ticker FROM ticker_aliases WHERE instrument_id=?"
        " AND notation='vendor' AND valid_to IS NULL LIMIT 1",
        (instrument_id,)).fetchone()
    return row[0] if row else None
