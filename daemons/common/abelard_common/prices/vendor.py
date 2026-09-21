"""PS-1 Phase 2 — the Yahoo v8 chart adapter.

**Scope note, so a later reader does not inherit a stale premise.** This module
is *an* adapter, not *the* adapter. P0.1 concluded there was no reachable source
of raw closes and A2 built the un-splitting derivation below on that. Tiingo
supersedes it: `/tiingo/daily/<t>/prices` returns raw `close` directly alongside
per-row `splitFactor` and `divCash`. Mando ruled 2026-09-02 that **Yahoo stays
primary and Tiingo verifies** — a free unmetered source is the right thing to
depend on nightly — so the derivation here is still the production path. But it
is a ruling, not a fact about the world, and Phase 2V adds the second adapter
beside this one.

One endpoint, one request shape. The request always carries
``events=div,split``, because that is what turns the nightly append into a
corporate-action detector at zero marginal cost: the events block is returned
even for a one-day window (verified, PS-1-P0 §P0.1), so a split is caught on its
effective date without a rotation waiting to notice it.

**Fail loud on schema drift.** A missing ``timestamp``/``close``/``adjclose``
block raises rather than yielding an empty series — an empty result and a broken
parser must never look alike (E1). The raw body is dumped for post-mortem when a
dump directory is configured.

**Pacing.** ``PACE_SECONDS`` between request starts. Measured round-trip is
~0.9 s, which already exceeds the floor, so this is a guard for the case where
the vendor gets fast, not the binding constraint. Transport retry/backoff and
429 handling come from ``abelard_common.http_client.HttpClient`` — this module
does not reimplement them. (The house has logged the duplicate-HttpClient debt
three times; a fourth copy is not the contribution to make here.)
"""

from __future__ import annotations

import datetime as dt
import json
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from ..http_client import HttpClient, NotFound, RateLimited, TransportError
from .calendar import DEFAULT_TZ, session_date
from .reconstruct import Bar, Dividend, Split
from .schema import PriceStoreError

CHART_URL = "https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
SOURCE = "yahoo_v8"
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36 AbelardPrices/0.1"
)
PACE_SECONDS = 0.5


class VendorError(PriceStoreError):
    """Vendor unreachable, degraded, or serving a shape we do not understand."""

    def __init__(self, message: str, *, stage: str = "vendor") -> None:
        super().__init__(message, stage=stage)


class VendorUnknownSymbol(VendorError):
    """The vendor 404s this symbol — delisted, renamed, or never covered.

    A distinct class because it is expected at the edges of any real universe
    (HOLX is in the ETF's holdings and gone from the chart endpoint) and must be
    counted rather than treated as an outage.
    """

    def __init__(self, message: str) -> None:
        super().__init__(message, stage="vendor_unknown_symbol")


class VendorSchemaError(VendorError):
    """The response parsed as JSON but is not the shape the contract requires."""

    def __init__(self, message: str) -> None:
        super().__init__(message, stage="vendor_schema")


@dataclass(frozen=True)
class VendorSeries:
    """One symbol's history as the vendor served it, plus what it declared."""

    symbol: str
    bars: list[Bar]
    splits: list[Split]
    dividends: list[Dividend]
    vendor_adjclose: dict[str, float]
    short_name: str | None = None
    fetched_at: int = 0

    @property
    def last_date(self) -> str | None:
        """The newest date the vendor actually RETURNED.

        This is what ``freshness.last_date_held`` is set from — never the
        requested end. Recording the request as if it were the response is the
        bug that froze 343 tickers in the layer this replaces.
        """
        dated = [b.date for b in self.bars if b.close is not None]
        return max(dated) if dated else None


def _iso(ts: int, tz_name: str | None = None) -> str:
    """Epoch -> session date, in the EXCHANGE's timezone.

    Not UTC. A daily equity bar is stamped 13:30/14:30 UTC and dates correctly
    either way, but ``CL=F`` is stamped 04:00/05:00 UTC — midnight
    exchange-local — which lands on the right UTC date only because New York is
    behind UTC. See prices/calendar.py.
    """
    return session_date(ts, tz_name or DEFAULT_TZ)


def _epoch(iso: str) -> int:
    return int(dt.datetime.fromisoformat(iso).replace(tzinfo=dt.timezone.utc).timestamp())


@dataclass
class YahooVendor:
    """Paced, fail-loud reader for the v8 chart endpoint.

    ``client`` is injectable so tests drive a fake and never touch the network.
    """

    client: HttpClient = field(default_factory=lambda: HttpClient(user_agent=USER_AGENT))
    pace_seconds: float = PACE_SECONDS
    dump_dir: Path | None = None
    _last_call: float = field(default=0.0, repr=False)

    def _pace(self) -> None:
        wait = self.pace_seconds - (time.monotonic() - self._last_call)
        if wait > 0:
            time.sleep(wait)
        self._last_call = time.monotonic()

    def _dump(self, symbol: str, body: Any) -> str | None:
        if self.dump_dir is None:
            return None
        self.dump_dir.mkdir(parents=True, exist_ok=True)
        safe = "".join(c if (c.isalnum() or c in ".-=^") else "_" for c in symbol)
        path = self.dump_dir / "{}_{}.json".format(safe, int(time.time() * 1000))
        path.write_text(json.dumps(body)[:500_000], errors="replace")
        return str(path)

    def fetch(self, symbol: str, start: str, end: str) -> VendorSeries:
        """Daily bars and declared events for ``[start, end]`` inclusive."""
        if start > end:
            raise VendorError("{}: start {} after end {}".format(symbol, start, end))
        params = {
            "period1": _epoch(start),
            # +1 day so the endpoint includes `end` itself.
            "period2": _epoch(end) + 86400,
            "interval": "1d",
            "includeAdjustedClose": "true",
            "events": "div,split",
        }
        self._pace()
        try:
            body = self.client.get_json(CHART_URL.format(symbol=symbol), params=params)
        except NotFound as exc:
            # A delisted or renamed symbol 404s. That is a fact about the
            # vendor's coverage, not a reason to abort a 500-name run -- the old
            # layer learned this the hard way ("one weird ticker must not kill a
            # multi-thousand-ticker bulk run"). Surfaced as a per-name
            # vendor_error, counted, and visible in status.
            raise VendorUnknownSymbol(
                "{}: unknown to the vendor (404)".format(symbol)) from exc
        except RateLimited as exc:
            raise VendorError("{}: rate limited — {}".format(symbol, exc)) from exc
        except TransportError as exc:
            raise VendorError("{}: transport — {}".format(symbol, exc)) from exc
        return self.parse(symbol, body)

    def parse(self, symbol: str, body: Any) -> VendorSeries:
        """Response body -> VendorSeries. Separated from transport so the parser
        is testable against recorded fixtures with no network."""
        chart = (body or {}).get("chart") or {}
        if chart.get("error"):
            raise VendorError("{}: chart.error={}".format(symbol, chart["error"]))
        result = chart.get("result")
        if not result:
            raise VendorSchemaError(
                "{}: chart.result missing raw={}".format(symbol, self._dump(symbol, body))
            )
        res = result[0]
        ts = res.get("timestamp")
        indicators = res.get("indicators") or {}
        quote = (indicators.get("quote") or [{}])[0]
        adjblock = (indicators.get("adjclose") or [{}])[0]
        closes = quote.get("close")
        adjs = adjblock.get("adjclose")
        if ts is None or closes is None or adjs is None:
            raise VendorSchemaError(
                "{}: timestamp/close/adjclose absent raw={}".format(
                    symbol, self._dump(symbol, body)
                )
            )

        meta = res.get("meta") or {}
        tz_name = meta.get("exchangeTimezoneName") or DEFAULT_TZ
        opens, highs, lows = quote.get("open"), quote.get("high"), quote.get("low")
        volumes = quote.get("volume")

        def at(seq: Any, i: int) -> Any:
            return seq[i] if seq is not None and i < len(seq) else None

        bars: list[Bar] = []
        vendor_adj: dict[str, float] = {}
        for i, t in enumerate(ts):
            date = _iso(t, tz_name)
            bars.append(Bar(
                date=date,
                open=at(opens, i), high=at(highs, i), low=at(lows, i),
                close=at(closes, i), volume=at(volumes, i),
            ))
            a = at(adjs, i)
            if a is not None:
                vendor_adj[date] = a

        events = res.get("events") or {}
        splits = sorted(
            (
                Split(_iso(int(k), tz_name), float(v["numerator"]) / float(v["denominator"]))
                for k, v in (events.get("splits") or {}).items()
                if v.get("denominator")
            ),
            key=lambda s: s.effective_date,
        )
        dividends = sorted(
            (
                Dividend(_iso(int(k), tz_name), float(v["amount"]))
                for k, v in (events.get("dividends") or {}).items()
                if v.get("amount") is not None
            ),
            key=lambda d: d.ex_date,
        )
        return VendorSeries(
            symbol=symbol,
            bars=bars,
            splits=splits,
            dividends=dividends,
            vendor_adjclose=vendor_adj,
            # For CL=F this is the front-month contract ("Crude Oil Oct 26") --
            # the roll signal, free, in the response we already make.
            short_name=meta.get("shortName"),
            fetched_at=int(time.time()),
        )
