"""SM-O1 P2: nightly options-chain snapshots — the forensic clock.

DEGRADED-class source, and the most fragile one in the stack. Yahoo's v7 options endpoint
needs a crumb + cookie (a bare call returns HTTP 401), rate-limits under load, and its
schema is not contractual. So: fail loud, dump the raw body on drift, pace ~2s, at most
2 retries, and never a retry storm. Bad nights WILL happen. A missed snapshot is a
COUNTED GAP, never interpolated — options data cannot be reconstructed after the fact,
and a fabricated row would poison every metric built on it. If the structural miss rate
turns out to be material, that is the trigger to price a paid EOD tier (Tradier/Polygon),
never to fight the wall harder.

DEPTH RULE (ruled): every expiry <= 75 calendar days out, floored at 4 and capped at 6
per ticker. LEAPS are deliberately excluded — institutional long-dated positioning is
already visible through 13F put/call, so paying for it twice buys nothing. Revisit only
if a join actually demands it.

OI SEMANTICS — THE THING MOST LIKELY TO BE GOT WRONG. Open interest is OCC-settled T+1,
so a chain pulled today carries YESTERDAY's settled OI while `volume` is today's. Every
row therefore stores an explicit `oi_asof` distinct from `snapshot_date`, and any vol/OI
ratio must divide today's volume by PRIOR-DAY OI. The offset is asserted, not assumed:
`confirm_t1` measures it against two consecutive snapshots, and no ratio ships until it
has been confirmed on our own data.

INGEST-ONLY. This leg returns a source status and counts, emits ZERO events, and is
excluded from the scan's exit spine. It is structurally unable to alert.

Retention is NOT implemented here — raw chains grow ~4-5GB/yr at this scope and the
proposal belongs at the P5 gate.

  python -m smart_money.options_chain --once        # one snapshot pass
  python -m smart_money.options_chain --confirm-t1  # verify the OI T+1 offset
"""
import argparse
import datetime as dt
import json
import pathlib
import sys
import time

import requests

from . import db as dbmod

OPTIONS_URL = "https://query2.finance.yahoo.com/v7/finance/options/{t}"
CRUMB_URL = "https://query2.finance.yahoo.com/v1/test/getcrumb"
COOKIE_URL = "https://fc.yahoo.com"
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AbelardSmartMoney/0.1"
ERR_DIR = pathlib.Path("data/raw/options_errors")
SOURCE = "yahoo_v7_options"

PACE_SECONDS = 2.0            # the endpoint tarpits under load; this is not negotiable
MAX_ATTEMPTS = 3              # 1 try + 2 retries, then DEGRADED. No storms.
MAX_DAYS = 75                 # depth rule: nothing further out than this
FLOOR_EXPIRIES = 4
CAP_EXPIRIES = 6

_last_call = 0.0


class OptionsError(RuntimeError):
    pass


class OptionsSchemaError(OptionsError):
    pass


class OptionsDegraded(OptionsError):
    pass


def _pace():
    global _last_call
    wait = PACE_SECONDS - (time.monotonic() - _last_call)
    if wait > 0:
        time.sleep(wait)
    _last_call = time.monotonic()


def _dump(ticker, body):
    ERR_DIR.mkdir(parents=True, exist_ok=True)
    safe = "".join(c if (c.isalnum() or c in ".-") else "_" for c in (ticker or "_"))
    path = ERR_DIR / "{}_{}.txt".format(safe, int(time.time() * 1000))
    path.write_text((body or "")[:500000], errors="replace")
    return path


def normalize(ticker):
    """Yahoo writes share classes with a DASH. BRK.B returns HTTP 200 with an empty
    424-byte chain while BRK-B returns a real one — a silent empty, which is worse than
    an error because nothing looks wrong. Normalising is mandatory, not cosmetic."""
    return (ticker or "").strip().upper().replace(".", "-")


def session():
    """(requests.Session, crumb). A bare call without both is HTTP 401."""
    s = requests.Session()
    s.headers["User-Agent"] = USER_AGENT
    try:
        s.get(COOKIE_URL, timeout=20)          # 404 by design; it sets the cookie
    except requests.RequestException as exc:
        raise OptionsDegraded("cookie seed failed: {!r}".format(exc))
    try:
        r = s.get(CRUMB_URL, timeout=20)
    except requests.RequestException as exc:
        raise OptionsDegraded("crumb fetch failed: {!r}".format(exc))
    if r.status_code != 200 or not r.text.strip():
        p = _dump("_crumb", r.text)
        raise OptionsDegraded("crumb HTTP {} raw={}".format(r.status_code, p))
    return s, r.text.strip()


def _fetch(s, crumb, ticker, expiry=None):
    """One options call with a retry ceiling. Returns (result0, raw_text)."""
    params = {"crumb": crumb}
    if expiry is not None:
        params["date"] = int(expiry)
    last_err = None
    for _ in range(MAX_ATTEMPTS):
        _pace()
        try:
            r = s.get(OPTIONS_URL.format(t=ticker), params=params, timeout=30)
        except requests.RequestException as exc:
            last_err = repr(exc)
            continue
        if r.status_code >= 500 or r.status_code == 429:
            last_err = "HTTP {}".format(r.status_code)
            continue
        if r.status_code != 200:
            p = _dump(ticker, r.text)
            raise OptionsDegraded("{} HTTP {} raw={}".format(ticker, r.status_code, p))
        try:
            body = r.json()
        except ValueError:
            p = _dump(ticker, r.text)
            raise OptionsSchemaError("{} body is not JSON raw={}".format(ticker, p))
        chain = body.get("optionChain") or {}
        if chain.get("error"):
            p = _dump(ticker, r.text)
            raise OptionsDegraded("{} error={} raw={}".format(
                ticker, chain["error"], p))
        result = chain.get("result")
        if not result:
            p = _dump(ticker, r.text)
            raise OptionsSchemaError("{} optionChain.result missing raw={}".format(
                ticker, p))
        return result[0], r.text
    raise OptionsDegraded("{} degraded after {} attempts last_err={}".format(
        ticker, MAX_ATTEMPTS, last_err))


def _epoch_date(e):
    return dt.datetime.fromtimestamp(int(e), dt.timezone.utc).date()


def pick_expiries(expirations, today, max_days=MAX_DAYS, floor=FLOOR_EXPIRIES,
                  cap=CAP_EXPIRIES):
    """The depth rule, applied to a unix-epoch expiry list.

    SPEC CONFLICT, RESOLVED ONE WAY AND FLAGGED. The order says "all expiries <= 75
    calendar days out, floor 4, cap 6" AND "LEAPS deliberately excluded". Those two
    clauses disagree for a ticker with, say, three expiries inside 75 days and then a
    jump to next January: topping up to four would reach past the window for exactly the
    long-dated contract the order excludes on the record, and it gives its reason
    (institutional long-dated is already visible through 13F put/call).

    So the FLOOR is read as ANTI-STARVATION, not as a quota: it rescues a ticker whose
    near window is EMPTY, and never pads a non-empty one with LEAPS. A ticker with three
    near expiries captures three. RAISED FOR RULING — if Mando reads the floor as a
    quota instead, the change is `if len(near) < floor` in place of `if not near`, one
    line, and the two behaviours are pinned by separate tests."""
    day = dt.date.fromisoformat(today) if isinstance(today, str) else today
    out = []
    for e in sorted(int(x) for x in expirations or []):
        d = _epoch_date(e)
        if d < day:                                    # already expired
            continue
        out.append((e, (d - day).days))
    near = [e for e, days in out if days <= max_days]
    if not near:
        near = [e for e, _ in out[:floor]]
    return near[:cap]


def _rows(result, ticker, snapshot_date, oi_asof):
    """Flatten one chain response into per-contract rows. A contract missing its strike
    or expiry is DROPPED and counted, never written with a guessed key."""
    quote = result.get("quote") or {}
    under = quote.get("regularMarketPrice")
    rows, bad = [], 0
    for block in result.get("options") or []:
        exp = block.get("expirationDate")
        for kind in ("calls", "puts"):
            for c in block.get(kind) or []:
                strike = c.get("strike")
                e = c.get("expiration", exp)
                if strike is None or e is None:
                    bad += 1
                    continue
                rows.append((
                    ticker, snapshot_date,
                    _epoch_date(e).isoformat(),
                    float(strike), "C" if kind == "calls" else "P",
                    c.get("contractSymbol"),
                    c.get("volume") or 0, c.get("openInterest") or 0, oi_asof,
                    c.get("impliedVolatility"), c.get("lastPrice"), c.get("bid"),
                    c.get("ask"), under, int(time.time())))
    return rows, bad


def _prior_trading_day(day):
    """OI is OCC-settled T+1, so today's chain carries the PRIOR TRADING DAY's OI.
    Weekends are skipped; exchange holidays are NOT modelled here, which is why the
    offset is measured by `confirm_t1` rather than trusted."""
    d = dt.date.fromisoformat(day) if isinstance(day, str) else day
    d -= dt.timedelta(days=1)
    while d.weekday() >= 5:
        d -= dt.timedelta(days=1)
    return d.isoformat()


def snapshot_ticker(con, s, crumb, ticker, snapshot_date, oi_asof=None):
    """One ticker's chains for the ruled expiry depth. Returns {contracts, expiries,
    dropped}. Raises OptionsDegraded / OptionsSchemaError — the caller counts the gap."""
    sym = normalize(ticker)
    oi_asof = oi_asof or _prior_trading_day(snapshot_date)
    first, _raw = _fetch(s, crumb, sym)
    exps = pick_expiries(first.get("expirationDates"), snapshot_date)
    if not exps:
        return {"contracts": 0, "expiries": 0, "dropped": 0, "no_chain": True}
    total, dropped = 0, 0
    for i, e in enumerate(exps):
        result = first if i == 0 and _served(first, e) else _fetch(s, crumb, sym, e)[0]
        rows, bad = _rows(result, sym, snapshot_date, oi_asof)
        dropped += bad
        for r in rows:
            con.execute(
                "INSERT OR REPLACE INTO options_chain_snapshots(ticker, snapshot_date, "
                "expiry, strike, option_type, contract_symbol, volume, open_interest, "
                "oi_asof, implied_vol, last_price, bid, ask, underlying_close, "
                "ingested_at_unix) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", r)
        total += len(rows)
    con.commit()
    return {"contracts": total, "expiries": len(exps), "dropped": dropped,
            "no_chain": False}


def _served(result, expiry):
    """Did the default response already carry this expiry? Saves a call per ticker."""
    for b in result.get("options") or []:
        if b.get("expirationDate") == expiry:
            return True
    return False


def universe(con):
    """Scoped tickers: the overlay's conviction book, watchlist and network sets, plus
    the registry's tracked-issuer symbols.

    BOUNDED BY DESIGN (~60). This is the forensic clock, not a sweep — at 2s pacing and
    up to 6 expiries a ticker, a corpus-wide universe would not finish a night, and the
    joins in P4 only ever look at scoped issuers anyway."""
    from . import queries as qmod
    tickers = set(qmod._scoped_tickers())
    try:
        reg = json.loads(open(dbmod.find_artifact("registry.json", "analysis")).read())
        for e in reg.get("entries") or []:
            tk = (e.get("ticker") or "").strip().upper()
            if tk:
                tickers.add(tk)
    except (OSError, ValueError):
        pass                     # registry absent or malformed - the overlay still stands
    return sorted(t for t in tickers if t and "/" not in t and " " not in t)


def run(con, tickers=None, snapshot_date=None, out=sys.stdout, progress_every=10):
    """One snapshot pass. Per-ticker failures are COUNTED AS GAPS and never fatal — one
    bad symbol must not cost the night. Returns the counts the scan leg reports."""
    day = snapshot_date or dt.date.today().isoformat()
    tks = tickers if tickers is not None else universe(con)
    st = {"tickers": len(tks), "ok": 0, "gaps": 0, "contracts": 0, "no_chain": 0,
          "dropped": 0, "snapshot_date": day, "oi_asof": _prior_trading_day(day),
          "errors": []}
    if not tks:
        return st
    s, crumb = session()
    for i, tk in enumerate(tks, 1):
        try:
            r = snapshot_ticker(con, s, crumb, tk, day, st["oi_asof"])
            st["contracts"] += r["contracts"]
            st["dropped"] += r["dropped"]
            if r["no_chain"]:
                st["no_chain"] += 1
            else:
                st["ok"] += 1
        except OptionsError as exc:
            st["gaps"] += 1
            if len(st["errors"]) < 20:
                st["errors"].append("{}: {}".format(tk, str(exc)[:120]))
        if progress_every and i % progress_every == 0:
            print("[options] {}/{} ok={} gaps={} contracts={}".format(
                i, len(tks), st["ok"], st["gaps"], st["contracts"]), file=out)
    record_pass(con, st)
    return st


def record_pass(con, st):
    """A pass ledger, so a missed night is VISIBLE rather than inferred from absence."""
    con.execute(
        "INSERT OR REPLACE INTO options_snapshot_passes(snapshot_date, tickers, ok, "
        "gaps, no_chain, contracts, dropped, oi_asof, ran_at_unix) "
        "VALUES(?,?,?,?,?,?,?,?,?)",
        (st["snapshot_date"], st["tickers"], st["ok"], st["gaps"], st["no_chain"],
         st["contracts"], st["dropped"], st["oi_asof"], int(time.time())))
    con.commit()


def confirm_t1(con, ticker=None):
    """Measure the OI T+1 offset against our OWN two most recent consecutive snapshots.

    The claim under test: OI reported on day D equals OI reported on day D+1 for the same
    contract ONLY IF the offset is real, because both would then describe the same settled
    figure lagged differently. Concretely — if today's chain carries yesterday's settled
    OI, then a contract's `open_interest` on snapshot D+1 should differ from D by the
    volume traded on D, not by the volume traded on D+1.

    Returns a comparison, NOT a verdict. No vol/OI ratio may ship until a human has read
    this on real consecutive days."""
    days = [r[0] for r in con.execute(
        "SELECT DISTINCT snapshot_date FROM options_chain_snapshots "
        "ORDER BY snapshot_date DESC LIMIT 2")]
    if len(days) < 2:
        return {"ready": False, "reason": "need 2 snapshot days, have {}".format(
            len(days)), "days": days}
    new, old = days[0], days[1]
    sql = ("SELECT n.ticker, n.contract_symbol, n.open_interest, n.volume, "
           "o.open_interest, o.volume "
           "FROM options_chain_snapshots n "
           "JOIN options_chain_snapshots o "
           "  ON o.contract_symbol = n.contract_symbol AND o.snapshot_date = ? "
           "WHERE n.snapshot_date = ? AND n.contract_symbol IS NOT NULL")
    params = [old, new]
    if ticker:
        sql += " AND n.ticker = ?"
        params.append(normalize(ticker))
    rows = list(con.execute(sql, params))
    matched = agree_prior = agree_same = 0
    for _tk, _cs, oi_new, vol_new, oi_old, vol_old in rows:
        if oi_new is None or oi_old is None:
            continue
        matched += 1
        delta = oi_new - oi_old
        # If OI is lagged T+1, the change between the two reports reflects the OLDER
        # day's volume. If it were same-day, it would reflect the NEWER day's.
        if abs(delta) <= (vol_old or 0):
            agree_prior += 1
        if abs(delta) <= (vol_new or 0):
            agree_same += 1
    return {"ready": True, "older": old, "newer": new, "contracts": matched,
            "consistent_with_prior_day_oi": agree_prior,
            "consistent_with_same_day_oi": agree_same,
            "note": "a comparison, NOT a verdict - no vol/OI ratio ships until this is "
                    "read on real consecutive trading days"}


def render(st):
    lines = ["SM-O1 P2 OPTIONS CHAIN SNAPSHOT", "=" * 62,
             "snapshot_date {}   oi_asof {} (OCC-settled T+1)".format(
                 st["snapshot_date"], st["oi_asof"]),
             "tickers {}  ok {}  gaps {}  no_chain {}".format(
                 st["tickers"], st["ok"], st["gaps"], st["no_chain"]),
             "contracts {}  dropped {}".format(st["contracts"], st["dropped"])]
    if st["errors"]:
        lines += ["", "GAPS (counted, never interpolated):"]
        lines += ["  " + e for e in st["errors"]]
    lines += ["",
              "A missed snapshot is a COUNTED GAP. Options data cannot be rebuilt after",
              "the fact, so nothing here is ever back-filled or interpolated."]
    return "\n".join(lines)


def main(argv=None):
    ap = argparse.ArgumentParser(description="SM-O1 P2 options chain snapshots")
    ap.add_argument("--db", default=dbmod.DB_PATH_DEFAULT)
    ap.add_argument("--once", action="store_true", help="run one snapshot pass")
    ap.add_argument("--confirm-t1", action="store_true",
                    help="measure the OI T+1 offset on two consecutive snapshots")
    ap.add_argument("--tickers", help="comma-separated override of the universe")
    args = ap.parse_args(argv)
    con = dbmod.connect(args.db)
    con.execute("PRAGMA busy_timeout=30000")
    try:
        if args.confirm_t1:
            print(json.dumps(confirm_t1(con), indent=2))
            return 0
        tks = ([t.strip().upper() for t in args.tickers.split(",")]
               if args.tickers else None)
        print(render(run(con, tickers=tks)))
    finally:
        con.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
