"""Price layer. Single wrapper around the certified Yahoo v8 chart endpoint.

DEGRADED-class source per SOURCE_VERDICTS.md G6 and ORDER SM-1 binding rules:
fail-loud on any schema drift with the raw body dumped to data/raw/price_errors/,
cache-first via the prices and price_spans tables, 0.5s pacing between live
calls, max 2 retries then PriceDegraded. Timestamps are never synthesized —
eod asof_unix comes verbatim from the API timestamp array, quote asof_unix is
regularMarketTime verbatim.

abelard_common hoist candidate for the future Price Daemon. Daemon-local until
convergence is ordered.
"""
import datetime as dt
import json
import pathlib
import time

import requests

CHART_URL = "https://query1.finance.yahoo.com/v8/finance/chart/{t}"
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AbelardSmartMoney/0.1"
ERR_DIR = pathlib.Path("data/raw/price_errors")
SOURCE = "yahoo_v8"
PACE_SECONDS = 0.5
MAX_ATTEMPTS = 3  # 1 try + 2 retries

_last_call = 0.0


class PriceError(RuntimeError):
    pass


class PriceSchemaError(PriceError):
    pass


class PriceDegraded(PriceError):
    pass


def _pace():
    global _last_call
    wait = PACE_SECONDS - (time.monotonic() - _last_call)
    if wait > 0:
        time.sleep(wait)
    _last_call = time.monotonic()


def _dump(ticker: str, body: str) -> pathlib.Path:
    ERR_DIR.mkdir(parents=True, exist_ok=True)
    path = ERR_DIR / "{}_{}.txt".format(ticker, int(time.time() * 1000))
    path.write_text(body[:500000], errors="replace")
    return path


def _fetch(ticker: str, params: dict):
    """One chart call with retry ceiling. Returns (result0, raw_text)."""
    last_err = None
    for attempt in range(MAX_ATTEMPTS):
        _pace()
        try:
            r = requests.get(
                CHART_URL.format(t=ticker),
                params=params,
                headers={"User-Agent": USER_AGENT},
                timeout=30,
            )
        except requests.RequestException as exc:
            last_err = repr(exc)
            continue
        if r.status_code >= 500 or r.status_code == 429:
            last_err = "HTTP {}".format(r.status_code)
            continue
        if r.status_code != 200:
            p = _dump(ticker, r.text)
            raise PriceDegraded("{} HTTP {} raw={}".format(ticker, r.status_code, p))
        try:
            body = r.json()
        except ValueError:
            p = _dump(ticker, r.text)
            raise PriceSchemaError("{} body is not JSON raw={}".format(ticker, p))
        chart = body.get("chart") or {}
        if chart.get("error"):
            p = _dump(ticker, r.text)
            raise PriceDegraded(
                "{} chart.error={} raw={}".format(ticker, chart["error"], p)
            )
        result = chart.get("result")
        if not result:
            p = _dump(ticker, r.text)
            raise PriceSchemaError("{} chart.result missing raw={}".format(ticker, p))
        return result[0], r.text
    raise PriceDegraded(
        "{} degraded after {} attempts last_err={}".format(
            ticker, MAX_ATTEMPTS, last_err
        )
    )


def _spans(con, ticker):
    rows = con.execute(
        "SELECT start_date, end_date FROM price_spans WHERE ticker=? "
        "ORDER BY start_date",
        (ticker,),
    ).fetchall()
    return rows


def _next_day(iso: str) -> str:
    return (dt.date.fromisoformat(iso) + dt.timedelta(days=1)).isoformat()


def _covered(con, ticker, start, end) -> bool:
    cursor = start
    for a, b in _spans(con, ticker):
        if a <= cursor <= b:
            if b >= end:
                return True
            cursor = _next_day(b)
        elif a > cursor:
            return False
    return False


def _add_span(con, ticker, start, end, fetched_at):
    spans = _spans(con, ticker) + [(start, end)]
    spans.sort()
    merged = []
    for a, b in spans:
        if merged and a <= _next_day(merged[-1][1]):
            merged[-1] = (merged[-1][0], max(merged[-1][1], b))
        else:
            merged.append((a, b))
    con.execute("DELETE FROM price_spans WHERE ticker=?", (ticker,))
    con.executemany(
        "INSERT INTO price_spans VALUES (?,?,?,?)",
        [(ticker, a, b, fetched_at) for a, b in merged],
    )


def eod(con, ticker: str, start: str, end: str):
    """Daily rows for ISO span [start, end] inclusive. Cache-first.

    Returns list of (date, close, adj_close, asof_unix)."""
    if start > end:
        raise PriceError("{} start {} after end {}".format(ticker, start, end))
    if not _covered(con, ticker, start, end):
        p1 = int(
            dt.datetime.fromisoformat(start)
            .replace(tzinfo=dt.timezone.utc)
            .timestamp()
        )
        p2 = int(
            (dt.datetime.fromisoformat(end) + dt.timedelta(days=1))
            .replace(tzinfo=dt.timezone.utc)
            .timestamp()
        )
        result, raw = _fetch(
            ticker,
            {
                "period1": p1,
                "period2": p2,
                "interval": "1d",
                "includeAdjustedClose": "true",
            },
        )
        ts = result.get("timestamp")
        ind = result.get("indicators") or {}
        quote = (ind.get("quote") or [{}])[0]
        closes = quote.get("close")
        adjblock = ind.get("adjclose") or [{}]
        adjs = adjblock[0].get("adjclose")
        if ts is None or closes is None or adjs is None:
            p = _dump(ticker, raw)
            raise PriceSchemaError(
                "{} eod schema drift, timestamp/close/adjclose absent raw={}".format(
                    ticker, p
                )
            )
        fetched_at = int(time.time())
        rows = []
        for t_, c, a in zip(ts, closes, adjs):
            if c is None or a is None:
                continue
            d = dt.datetime.fromtimestamp(t_, dt.timezone.utc).date().isoformat()
            rows.append((ticker, d, c, a, "eod", t_, fetched_at, SOURCE))
        con.executemany(
            "INSERT OR REPLACE INTO prices VALUES (?,?,?,?,?,?,?,?)", rows
        )
        _add_span(con, ticker, start, end, fetched_at)
        con.commit()
    return con.execute(
        "SELECT date, close, adj_close, asof_unix FROM prices "
        "WHERE ticker=? AND price_type='eod' AND date>=? AND date<=? ORDER BY date",
        (ticker, start, end),
    ).fetchall()


def latest(con, ticker: str):
    """Live quote. Returns (price, asof_unix) from meta, verbatim."""
    result, raw = _fetch(ticker, {"range": "1d", "interval": "1d"})
    meta = result.get("meta") or {}
    price = meta.get("regularMarketPrice")
    asof = meta.get("regularMarketTime")
    if price is None or asof is None:
        p = _dump(ticker, raw)
        raise PriceSchemaError(
            "{} quote schema drift, regularMarketPrice/Time absent raw={}".format(
                ticker, p
            )
        )
    d = dt.datetime.fromtimestamp(asof, dt.timezone.utc).date().isoformat()
    con.execute(
        "INSERT OR REPLACE INTO prices VALUES (?,?,?,?,?,?,?,?)",
        (ticker, d, price, price, "quote", asof, int(time.time()), SOURCE),
    )
    con.commit()
    return price, asof
