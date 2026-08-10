"""SM-O1 P2: options chain snapshots. No network — the fetch layer is swapped out.

Two properties carry this module: a missed snapshot must stay a COUNTED GAP rather than
become an interpolation, and open interest must never be silently treated as same-day.
"""
import datetime as dt
import os
import tempfile

from smart_money import db as dbmod, options_chain as oc

TODAY = "2026-08-07"          # a Friday
EPOCH = dt.datetime(1970, 1, 1)


def _ts(datestr):
    return int((dt.datetime.fromisoformat(datestr) - EPOCH).total_seconds())


def _db():
    fd, p = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    return p, dbmod.connect(p)


def _contract(strike, vol, oi, ctype="C", expiry="2026-08-21"):
    return {"strike": strike, "volume": vol, "openInterest": oi,
            "impliedVolatility": 0.42, "lastPrice": 1.5, "bid": 1.4, "ask": 1.6,
            "expiration": _ts(expiry),
            "contractSymbol": "X{}{}{:08d}".format(
                expiry.replace("-", "")[2:], ctype, int(strike * 1000))}


def _result(expiries=("2026-08-21",), calls=(), puts=(), under=100.0):
    return {"expirationDates": [_ts(e) for e in expiries],
            "quote": {"regularMarketPrice": under},
            "options": [{"expirationDate": _ts(expiries[0]),
                         "calls": list(calls), "puts": list(puts)}]}


# ---------------------------------------------------------------- depth rule

def test_depth_rule_keeps_only_expiries_inside_the_window():
    exps = [_ts(d) for d in ("2026-08-14", "2026-08-21", "2026-09-18", "2027-01-15")]
    got = oc.pick_expiries(exps, TODAY)
    assert len(got) == 3, "the 2027 LEAP is past 75 days and excluded"
    assert _ts("2027-01-15") not in got


def test_depth_rule_caps_at_six():
    exps = [_ts("2026-08-%02d" % d) for d in (10, 11, 12, 13, 14, 15, 16, 17)]
    assert len(oc.pick_expiries(exps, TODAY)) == 6


def test_the_floor_rescues_an_EMPTY_near_window():
    """A ticker whose next expiries are ALL past 75 days must not capture as NOTHING —
    a silent zero is indistinguishable from a ticker with no options at all."""
    exps = [_ts(d) for d in ("2027-01-15", "2027-02-19", "2027-03-19", "2027-06-18",
                             "2027-09-17")]
    got = oc.pick_expiries(exps, TODAY)
    assert len(got) == 4, got


def test_the_floor_does_not_pad_a_partial_window_with_leaps():
    """SPEC CONFLICT, resolved and flagged: "floor 4" and "LEAPS excluded" disagree when
    a ticker has 3 near expiries and then a jump to next January. Topping up to four
    would reach for exactly the long-dated contract the order excludes on the record, so
    the floor is read as anti-starvation rather than as a quota."""
    exps = [_ts(d) for d in ("2026-08-14", "2026-08-21", "2026-09-18", "2027-01-15")]
    got = oc.pick_expiries(exps, TODAY)
    assert len(got) == 3, got
    assert _ts("2027-01-15") not in got


def test_depth_rule_drops_already_expired_dates():
    exps = [_ts(d) for d in ("2026-01-16", "2026-08-21")]
    assert oc.pick_expiries(exps, TODAY) == [_ts("2026-08-21")]


def test_leaps_are_excluded_when_near_dated_expiries_exist():
    exps = [_ts(d) for d in ("2026-08-14", "2026-08-21", "2026-09-18", "2026-10-16",
                             "2028-01-21")]
    assert _ts("2028-01-21") not in oc.pick_expiries(exps, TODAY)


# ---------------------------------------------------------------- ticker shape

def test_dot_tickers_are_normalised_to_dashes():
    """BRK.B returns HTTP 200 with an EMPTY 424-byte chain while BRK-B returns a real
    one. A silent empty is worse than an error because nothing looks wrong."""
    assert oc.normalize("BRK.B") == "BRK-B"
    assert oc.normalize("MOG.A") == "MOG-A"
    assert oc.normalize(" aapl ") == "AAPL"


# ---------------------------------------------------------------- OI semantics

def test_oi_asof_is_the_prior_trading_day_not_the_snapshot_day():
    """OI is OCC-settled T+1: today's chain carries yesterday's settled figure."""
    assert oc._prior_trading_day("2026-08-07") == "2026-08-06"


def test_oi_asof_skips_the_weekend():
    assert oc._prior_trading_day("2026-08-10") == "2026-08-07"   # Monday -> Friday


def test_a_row_stores_volume_and_oi_against_different_dates(monkeypatch):
    p, con = _db()
    try:
        res = _result(calls=[_contract(100, 250, 900)])
        monkeypatch.setattr(oc, "_fetch", lambda *a, **k: (res, "{}"))
        oc.snapshot_ticker(con, None, None, "AAPL", TODAY)
        row = con.execute("SELECT snapshot_date, oi_asof, volume, open_interest "
                          "FROM options_chain_snapshots").fetchone()
        assert row[0] == TODAY and row[1] == "2026-08-06", row
        assert row[2] == 250 and row[3] == 900
    finally:
        con.close()
        os.unlink(p)


def test_confirm_t1_refuses_a_verdict_without_two_sessions(monkeypatch):
    p, con = _db()
    try:
        res = _result(calls=[_contract(100, 10, 20)])
        monkeypatch.setattr(oc, "_fetch", lambda *a, **k: (res, "{}"))
        oc.snapshot_ticker(con, None, None, "AAPL", TODAY)
        r = oc.confirm_t1(con)
        assert r["ready"] is False and "need 2 trading sessions" in r["reason"]
    finally:
        con.close()
        os.unlink(p)


def test_confirm_t1_compares_and_does_not_conclude(monkeypatch):
    """It returns a MEASUREMENT. No vol/OI ratio may ship on an assumed offset."""
    p, con = _db()
    try:
        monkeypatch.setattr(oc, "_fetch",
                            lambda *a, **k: (_result(calls=[_contract(100, 50, 900)]),
                                             "{}"))
        oc.snapshot_ticker(con, None, None, "AAPL", "2026-08-06")
        monkeypatch.setattr(oc, "_fetch",
                            lambda *a, **k: (_result(calls=[_contract(100, 70, 940)]),
                                             "{}"))
        oc.snapshot_ticker(con, None, None, "AAPL", "2026-08-07")
        r = oc.confirm_t1(con)
        assert r["ready"] is True and r["contracts"] == 1
        # OI moved 40; the OLDER session volume was 50, so a T+1 lag is consistent.
        assert r["consistent_with_prior_day_oi"] == 1
        assert "NOT a verdict" in r["note"]
    finally:
        con.close()
        os.unlink(p)


# ---------------------------------------------------------------- idempotence

def test_two_runs_of_the_same_day_do_not_double_rows(monkeypatch):
    """The order's two-run idempotence proof."""
    p, con = _db()
    try:
        res = _result(calls=[_contract(100, 10, 20), _contract(105, 5, 7)],
                      puts=[_contract(95, 3, 11, "P")])
        monkeypatch.setattr(oc, "_fetch", lambda *a, **k: (res, "{}"))
        oc.snapshot_ticker(con, None, None, "AAPL", TODAY)
        n1 = con.execute("SELECT COUNT(*) FROM options_chain_snapshots").fetchone()[0]
        oc.snapshot_ticker(con, None, None, "AAPL", TODAY)
        n2 = con.execute("SELECT COUNT(*) FROM options_chain_snapshots").fetchone()[0]
        assert n1 == n2 == 3, (n1, n2)
    finally:
        con.close()
        os.unlink(p)


def test_a_rerun_refreshes_values_rather_than_appending(monkeypatch):
    p, con = _db()
    try:
        monkeypatch.setattr(oc, "_fetch",
                            lambda *a, **k: (_result(calls=[_contract(100, 10, 20)]),
                                             "{}"))
        oc.snapshot_ticker(con, None, None, "AAPL", TODAY)
        monkeypatch.setattr(oc, "_fetch",
                            lambda *a, **k: (_result(calls=[_contract(100, 99, 20)]),
                                             "{}"))
        oc.snapshot_ticker(con, None, None, "AAPL", TODAY)
        rows = con.execute("SELECT volume FROM options_chain_snapshots").fetchall()
        assert rows == [(99,)], rows
    finally:
        con.close()
        os.unlink(p)


# ---------------------------------------------------------------- gaps, never fakes

def test_a_failing_ticker_is_a_counted_gap_and_not_fatal(monkeypatch):
    p, con = _db()
    try:
        def boom(s, crumb, tk, expiry=None):
            if tk == "BAD":
                raise oc.OptionsDegraded("BAD HTTP 500")
            return _result(calls=[_contract(100, 1, 2)]), "{}"
        monkeypatch.setattr(oc, "_fetch", boom)
        monkeypatch.setattr(oc, "session", lambda: (None, "crumb"))
        st = oc.run(con, tickers=["AAPL", "BAD", "MSFT"], snapshot_date=TODAY,
                    out=open(os.devnull, "w"), progress_every=0)
        assert st["ok"] == 2 and st["gaps"] == 1
        assert any("BAD" in e for e in st["errors"])
        assert st["contracts"] == 2, "the good tickers still landed"
    finally:
        con.close()
        os.unlink(p)


def test_a_gap_writes_no_rows_for_that_ticker(monkeypatch):
    """Never interpolated. A missing night stays missing."""
    p, con = _db()
    try:
        monkeypatch.setattr(oc, "_fetch",
                            lambda *a, **k: (_ for _ in ()).throw(
                                oc.OptionsDegraded("down")))
        monkeypatch.setattr(oc, "session", lambda: (None, "crumb"))
        st = oc.run(con, tickers=["AAPL"], snapshot_date=TODAY,
                    out=open(os.devnull, "w"), progress_every=0)
        assert st["gaps"] == 1
        assert con.execute("SELECT COUNT(*) FROM options_chain_snapshots"
                           ).fetchone()[0] == 0
    finally:
        con.close()
        os.unlink(p)


def test_the_pass_ledger_records_the_night_including_its_gaps(monkeypatch):
    """Absence of rows reads identically to 'nothing traded'. The ledger is how a missed
    night stays visible."""
    p, con = _db()
    try:
        monkeypatch.setattr(oc, "_fetch",
                            lambda *a, **k: (_result(calls=[_contract(100, 1, 2)]),
                                             "{}"))
        monkeypatch.setattr(oc, "session", lambda: (None, "crumb"))
        oc.run(con, tickers=["AAPL"], snapshot_date=TODAY,
               out=open(os.devnull, "w"), progress_every=0)
        row = con.execute("SELECT snapshot_date, tickers, ok, gaps, contracts, oi_asof "
                          "FROM options_snapshot_passes").fetchone()
        assert row == (TODAY, 1, 1, 0, 1, "2026-08-06"), row
    finally:
        con.close()
        os.unlink(p)


def test_a_contract_without_a_strike_is_dropped_and_counted(monkeypatch):
    """Never written with a guessed key."""
    p, con = _db()
    try:
        bad = _contract(100, 1, 2)
        bad["strike"] = None
        monkeypatch.setattr(oc, "_fetch",
                            lambda *a, **k: (_result(calls=[bad, _contract(105, 2, 3)]),
                                             "{}"))
        r = oc.snapshot_ticker(con, None, None, "AAPL", TODAY)
        assert r["contracts"] == 1 and r["dropped"] == 1
    finally:
        con.close()
        os.unlink(p)


def test_a_ticker_with_no_expiries_is_no_chain_not_an_error(monkeypatch):
    p, con = _db()
    try:
        monkeypatch.setattr(oc, "_fetch",
                            lambda *a, **k: ({"expirationDates": [], "options": [],
                                              "quote": {}}, "{}"))
        r = oc.snapshot_ticker(con, None, None, "NOOPT", TODAY)
        assert r["no_chain"] is True and r["contracts"] == 0
    finally:
        con.close()
        os.unlink(p)


# ---------------------------------------------------------------- scan wiring

def test_the_leg_is_ingest_only_and_emits_no_events(monkeypatch):
    from smart_money import scan
    p, con = _db()
    try:
        monkeypatch.setattr(oc, "_fetch",
                            lambda *a, **k: (_result(calls=[_contract(100, 1, 2)]),
                                             "{}"))
        monkeypatch.setattr(oc, "session", lambda: (None, "crumb"))
        monkeypatch.setattr(oc, "universe", lambda c: ["AAPL"])
        src, st = scan.leg_options(con)
        assert src["source"] == "options_chains" and src["status"] == "OK"
        assert isinstance(st, dict) and st["contracts"] == 1
        # the leg returns (source, counts) - there is no event channel at all
        assert len(scan.leg_options(con)) == 2
    finally:
        con.close()
        os.unlink(p)


def test_a_pass_that_captured_nothing_degrades(monkeypatch):
    """'Ran and got zero' must never read like 'ran and there was nothing'."""
    from smart_money import scan
    p, con = _db()
    try:
        monkeypatch.setattr(oc, "session", lambda: (None, "crumb"))
        monkeypatch.setattr(oc, "universe", lambda c: [])
        src, _ = scan.leg_options(con)
        assert src["status"] == "DEGRADED"
    finally:
        con.close()
        os.unlink(p)


def test_the_options_source_is_excluded_from_the_exit_spine():
    """A collection leg must not be able to trip - or mask - the all-signals-down alarm."""
    import inspect
    from smart_money import scan
    src = inspect.getsource(scan.main)
    assert '"options_chains"' in src
    i = src.index("Exit spine")
    assert '"options_chains"' in src[i:], "must appear in the exclusion list"


# ---------------------------------------------------------------- no_chain vs gap

class _FakeResp:
    def __init__(self, payload, text):
        self._p, self.text, self.status_code = payload, text, 200

    def json(self):
        return self._p


class _FakeSession:
    def __init__(self, payload, text):
        self._p, self._t = payload, text

    def get(self, *a, **k):
        return _FakeResp(self._p, self._t)


def test_an_empty_result_is_no_chain_not_a_gap(monkeypatch):
    """Observed live on CTRA: {"optionChain":{"result":[],"error":null}} is Yahoo saying
    the symbol has NO OPTIONS, not a failure. The first cut raised on it and counted an
    optionless ticker as a collection gap - and the miss rate is the stated trigger for
    buying a paid EOD tier, so that fiction would have driven a purchase decision."""
    p, con = _db()
    try:
        payload = {"optionChain": {"result": [], "error": None}}
        monkeypatch.setattr(oc, "_pace", lambda: None)
        monkeypatch.setattr(oc, "session",
                            lambda: (_FakeSession(payload, '{"optionChain":{}}'), "cr"))
        st = oc.run(con, tickers=["CTRA"], snapshot_date=TODAY,
                    out=open(os.devnull, "w"), progress_every=0)
        assert st["gaps"] == 0, "an optionless symbol is not a collection gap"
        assert st["no_chain"] == 1 and st["errors"] == []
    finally:
        con.close()
        os.unlink(p)


def test_a_missing_result_KEY_is_still_schema_drift(monkeypatch):
    """The distinction that makes the above safe: an empty LIST is an answer; a missing
    KEY is drift and must fail loud with the body dumped."""
    monkeypatch.setattr(oc, "_pace", lambda: None)
    sess = _FakeSession({"optionChain": {"error": None}}, '{"optionChain":{}}')
    try:
        oc._fetch(sess, "crumb", "AAPL")
    except oc.OptionsSchemaError as exc:
        assert "result key missing" in str(exc)
    else:
        raise AssertionError("a missing result key must raise")


def test_the_leg_is_actually_called_by_the_scan():
    """The exit-spine exclusion lives in main() but the CALL lives in run_scan(). An
    ad-hoc probe of main() alone reported the leg as unwired when it was wired - so pin
    the call site itself, in the function that owns it."""
    import inspect
    from smart_money import scan
    src = inspect.getsource(scan.run_scan)
    assert "leg_options(con)" in src, "run_scan must invoke the options leg"
    # and its status must reach the envelope, or the leg runs and reports nothing
    assert "src_o]" in src.replace(" ", ""), "the leg's source must join the list"


# ---------------------------------------------------------------- weekend sessions

def test_a_weekend_pull_is_labelled_with_the_prior_trading_session():
    """The leg rides a nightly scan, so it fires on weekends - and a weekend pull returns
    the previous session verbatim. Measured on the first three live snapshots: the
    Saturday and Sunday pulls matched on 9,499 of 9,500 contracts by volume (99.99%)."""
    assert oc.session_date("2026-08-07") == "2026-08-07"   # Friday -> itself
    assert oc.session_date("2026-08-08") == "2026-08-07"   # Saturday -> Friday
    assert oc.session_date("2026-08-09") == "2026-08-07"   # Sunday   -> Friday
    assert oc.session_date("2026-08-10") == "2026-08-10"   # Monday   -> itself


def test_rows_carry_both_the_pull_date_and_the_session(monkeypatch):
    """The pull date stays for PROVENANCE; the session is what metrics must group on."""
    p, con = _db()
    try:
        monkeypatch.setattr(oc, "_fetch",
                            lambda *a, **k: (_result(calls=[_contract(100, 5, 9)]), "{}"))
        oc.snapshot_ticker(con, None, None, "AAPL", "2026-08-09")     # a Sunday
        row = con.execute("SELECT snapshot_date, session_date FROM "
                          "options_chain_snapshots").fetchone()
        assert row == ("2026-08-09", "2026-08-07"), row
    finally:
        con.close()
        os.unlink(p)


def test_confirm_t1_needs_two_SESSIONS_not_two_calendar_pulls(monkeypatch):
    """The bug this pins: the two most recent snapshots were Saturday and Sunday - the
    same Friday session compared against itself. That would have produced a
    confident-looking nothing rather than an honest 'not ready'."""
    p, con = _db()
    try:
        monkeypatch.setattr(oc, "_fetch",
                            lambda *a, **k: (_result(calls=[_contract(100, 5, 9)]), "{}"))
        oc.snapshot_ticker(con, None, None, "AAPL", "2026-08-08")     # Sat -> Fri
        oc.snapshot_ticker(con, None, None, "AAPL", "2026-08-09")     # Sun -> Fri
        r = oc.confirm_t1(con)
        assert r["ready"] is False, "two weekend pulls are ONE session"
        assert "two trading sessions" in r["reason"] or "2 trading sessions" in r["reason"]
    finally:
        con.close()
        os.unlink(p)


def test_confirm_t1_runs_once_two_real_sessions_exist(monkeypatch):
    p, con = _db()
    try:
        monkeypatch.setattr(oc, "_fetch",
                            lambda *a, **k: (_result(calls=[_contract(100, 50, 900)]),
                                             "{}"))
        oc.snapshot_ticker(con, None, None, "AAPL", "2026-08-06")     # Thursday
        monkeypatch.setattr(oc, "_fetch",
                            lambda *a, **k: (_result(calls=[_contract(100, 70, 940)]),
                                             "{}"))
        oc.snapshot_ticker(con, None, None, "AAPL", "2026-08-07")     # Friday
        r = oc.confirm_t1(con)
        assert r["ready"] is True
        assert (r["older_session"], r["newer_session"]) == ("2026-08-06", "2026-08-07")
    finally:
        con.close()
        os.unlink(p)


def test_oi_asof_derives_from_the_session_not_the_pull_date(monkeypatch):
    """A Sunday pull returns FRIDAY's chain, and Friday's chain carries THURSDAY's
    settled OI. Deriving oi_asof from the pull date gave oi_asof == session_date - the
    chain claiming to carry its own day's settled figure, which is exactly the
    off-by-one this column exists to prevent."""
    p, con = _db()
    try:
        monkeypatch.setattr(oc, "_fetch",
                            lambda *a, **k: (_result(calls=[_contract(100, 5, 9)]), "{}"))
        oc.snapshot_ticker(con, None, None, "AAPL", "2026-08-09")      # Sunday
        pull, sess, oi = con.execute(
            "SELECT snapshot_date, session_date, oi_asof FROM options_chain_snapshots"
        ).fetchone()
        assert (pull, sess) == ("2026-08-09", "2026-08-07")
        assert oi == "2026-08-06", "Thursday, one session before Friday"
        assert oi != sess, "OI can never be as-of its own session"
    finally:
        con.close()
        os.unlink(p)


def test_a_weekday_pull_keeps_the_ordinary_t1_offset(monkeypatch):
    p, con = _db()
    try:
        monkeypatch.setattr(oc, "_fetch",
                            lambda *a, **k: (_result(calls=[_contract(100, 5, 9)]), "{}"))
        oc.snapshot_ticker(con, None, None, "AAPL", "2026-08-10")      # Monday
        pull, sess, oi = con.execute(
            "SELECT snapshot_date, session_date, oi_asof FROM options_chain_snapshots"
        ).fetchone()
        assert (pull, sess, oi) == ("2026-08-10", "2026-08-10", "2026-08-07")
    finally:
        con.close()
        os.unlink(p)


def test_the_migration_repairs_a_weekend_row_written_with_the_old_rule():
    """Rows written before oi_asof derived from the session got oi_asof == session_date -
    the chain claiming its own session's settled OI. Self-healing and idempotent."""
    p, con = _db()
    try:
        con.execute(
            "INSERT INTO options_chain_snapshots(ticker, snapshot_date, session_date, "
            "expiry, strike, option_type, volume, open_interest, oi_asof, "
            "ingested_at_unix) VALUES('AAPL','2026-08-09','2026-08-07','2026-08-21',"
            "100,'C',5,9,'2026-08-07',0)")
        con.commit()
        con.close()
        con = dbmod.connect(p)                       # connect runs the migration
        assert con.execute("SELECT oi_asof FROM options_chain_snapshots"
                           ).fetchone()[0] == "2026-08-06"
        con.close()
        con = dbmod.connect(p)                       # second run must not shift it again
        assert con.execute("SELECT oi_asof FROM options_chain_snapshots"
                           ).fetchone()[0] == "2026-08-06"
    finally:
        con.close()
        os.unlink(p)


def test_the_migration_repairs_a_monday_session_back_across_the_weekend():
    p, con = _db()
    try:
        con.execute(
            "INSERT INTO options_chain_snapshots(ticker, snapshot_date, session_date, "
            "expiry, strike, option_type, volume, open_interest, oi_asof, "
            "ingested_at_unix) VALUES('AAPL','2026-08-10','2026-08-10','2026-08-21',"
            "100,'C',5,9,'2026-08-10',0)")
        con.commit()
        con.close()
        con = dbmod.connect(p)
        assert con.execute("SELECT oi_asof FROM options_chain_snapshots"
                           ).fetchone()[0] == "2026-08-07", "Monday -> Friday"
    finally:
        con.close()
        os.unlink(p)


def test_the_migration_backfills_the_pass_ledger_not_only_the_rows():
    """The ledger is what makes a missed night VISIBLE and what the P5 miss-rate is
    computed from, so a null session there is not cosmetic. Healed on the same rule."""
    p, con = _db()
    try:
        con.execute(
            "INSERT INTO options_snapshot_passes(snapshot_date, tickers, ok, gaps, "
            "no_chain, contracts, dropped, oi_asof, ran_at_unix) "
            "VALUES('2026-08-08',32,27,0,5,10568,0,'2026-08-07',0)")
        con.commit()
        con.close()
        con = dbmod.connect(p)                        # migration runs here
        row = con.execute("SELECT session_date, oi_asof FROM options_snapshot_passes"
                          ).fetchone()
        assert row == ("2026-08-07", "2026-08-06"), row
        con.close()
        con = dbmod.connect(p)                        # idempotent
        assert con.execute("SELECT session_date, oi_asof FROM options_snapshot_passes"
                           ).fetchone() == ("2026-08-07", "2026-08-06")
    finally:
        con.close()
        os.unlink(p)
