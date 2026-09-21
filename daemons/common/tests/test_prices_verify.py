"""PS-1B Phase 2V — cross-vendor verification tests. No network.

The order names seven explicitly: a fixture that agrees, one that disagrees on
price, one on a split date, one with a hole; a ceiling refusal; an unsigned
draft that cannot be applied; and a fill that never touches a held value.
"""

from __future__ import annotations

import json
import sqlite3
import time
from pathlib import Path

import pytest

from abelard_common.prices import corrections as CO
from abelard_common.prices import reconstruct as R
from abelard_common.prices import schema as S
from abelard_common.prices import verify as V
from abelard_common.prices import vendor_tiingo as T
from abelard_common.prices import writer as W

RUN = 1788000000
AS_OF = "2026-08-14"
SINCE = "2026-08-03"
DATES = ["2026-08-03", "2026-08-04", "2026-08-05", "2026-08-06", "2026-08-07",
         "2026-08-10", "2026-08-11", "2026-08-12", "2026-08-13", "2026-08-14"]


class FakeTiingo:
    """Serves prepared bars and records what it was asked for."""

    def __init__(self, bars_by_symbol, unknown=()):
        self._bars = bars_by_symbol
        self.unknown = set(unknown)
        self.calls: list[tuple[str, str, str]] = []

    def daily(self, symbol, start, end):
        self.calls.append((symbol, start, end))
        if symbol in self.unknown:
            raise T.TiingoUnknownSymbol("{}: unknown".format(symbol))
        return [b for b in self._bars.get(symbol, []) if start <= b.date <= end]


def bars(closes, splits=None, divs=None):
    splits, divs = splits or {}, divs or {}
    return [T.TiingoBar(d, c, c, splits.get(d, 1.0), divs.get(d, 0.0), 1_000_000)
            for d, c in closes.items() if c is not None]


@pytest.fixture()
def con(tmp_path):
    c = S.connect(tmp_path / "p.db")
    yield c
    c.close()


def register(con, iid, ticker):
    con.execute(
        "INSERT INTO instruments (instrument_id, cik, class_code, class_source,"
        " name, primary_ticker, source, provisional, first_seen, last_seen)"
        " VALUES (?,?,'0','single',?,?,'test',0,'2026-08-01','2026-08-01')",
        (iid, iid.split(".")[0], ticker, ticker))
    con.execute("INSERT INTO ticker_aliases VALUES (?,?,'vendor','2026-08-01',NULL,'t')",
                (iid, ticker))
    con.execute("INSERT INTO index_membership VALUES (?,'SPX','2026-08-01',1,'t')", (iid,))
    con.commit()
    return iid


def hold(con, iid, closes, status="ok"):
    for d, c in closes.items():
        con.execute(
            "INSERT INTO prices_raw (instrument_id, date, close, status, source,"
            " fetched_at, run_asof) VALUES (?,?,?,?,'yahoo_v8',?,?)",
            (iid, d, c, "vendor_null" if c is None else status, RUN, RUN))
    con.commit()


# =========================================================== the comparison ==

def test_a_vendor_that_agrees_verifies(con):
    iid = register(con, "0000000001.0", "AGREE")
    closes = {d: 100.0 + i for i, d in enumerate(DATES)}
    hold(con, iid, closes)
    v = FakeTiingo({"AGREE": bars(closes)})
    res = V.sweep(con, v, 5, AS_OF, SINCE, run_asof=RUN)
    assert res.counts() == {"VERIFIED": 1}
    row = con.execute("SELECT * FROM verification").fetchone()
    assert row["kind"] == "VERIFIED" and row["agreements"] == len(DATES)
    assert row["disagreements"] == 0
    assert res.drafts == [] and res.quarantined == 0


def test_a_price_disagreement_quarantines_and_drafts(con, tmp_path):
    iid = register(con, "0000000002.0", "PRICE")
    held = {d: 100.0 for d in DATES}
    hold(con, iid, held)
    theirs = dict(held)
    theirs["2026-08-06"] = 200.0                     # exactly 2x -- the MNST shape
    v = FakeTiingo({"PRICE": bars(theirs)})
    res = V.sweep(con, v, 5, AS_OF, SINCE, run_asof=RUN, drafts_root=tmp_path)

    assert res.counts() == {"DISAGREE_PRICE": 1}
    assert res.quarantined == 1
    assert con.execute(
        "SELECT COUNT(*) FROM quarantine WHERE instrument_id=? AND date='2026-08-06'",
        (iid,)).fetchone()[0] == 1
    # The fact is untouched.
    assert con.execute(
        "SELECT close, status FROM prices_raw WHERE instrument_id=? AND date='2026-08-06'",
        (iid,)).fetchone()["close"] == 100.0
    # ... and the quarantined session leaves the analytics view.
    assert "2026-08-06" not in W.held_raw_closes(con, iid)

    draft = json.loads(Path(res.drafts[0]).read_text())
    assert draft["authored_by"] == ""
    assert draft["rows"][0]["evidence"]["ratio_verifier_to_held"] == 2.0


def test_a_split_disagreement_is_ca_not_price(con):
    iid = register(con, "0000000003.0", "SPLT")
    held = {d: 100.0 for d in DATES}
    hold(con, iid, held)
    # The verifier carries a 2:1 the store has never been told about.
    v = FakeTiingo({"SPLT": bars(held, splits={"2026-08-10": 2.0})})
    V.sweep(con, v, 5, AS_OF, SINCE, run_asof=RUN)
    row = con.execute("SELECT kind, detail FROM verification").fetchone()
    assert row["kind"] == "DISAGREE_CA"
    assert "not declared" in row["detail"]
    # A CA disagreement must NOT quarantine or re-version anything by itself.
    assert con.execute("SELECT COUNT(*) FROM quarantine").fetchone()[0] == 0
    assert con.execute("SELECT COUNT(*) FROM adjustment_factors").fetchone()[0] == 0


def test_a_declared_split_the_verifier_lacks_is_also_flagged(con):
    iid = register(con, "0000000004.0", "SPL2")
    hold(con, iid, {d: 100.0 for d in DATES})
    con.execute("INSERT INTO corporate_actions VALUES (?,?,'split',2.0,NULL,?,?,?)",
                (iid, "2026-08-10", RUN, "yahoo_v8_events", RUN))
    con.commit()
    v = FakeTiingo({"SPL2": bars({d: 100.0 for d in DATES})})
    V.sweep(con, v, 5, AS_OF, SINCE, run_asof=RUN)
    assert "does not carry" in con.execute(
        "SELECT detail FROM verification").fetchone()[0]


def test_a_dividend_difference_beyond_a_cent_is_flagged(con):
    iid = register(con, "0000000005.0", "DIV")
    hold(con, iid, {d: 100.0 for d in DATES})
    con.execute("INSERT INTO corporate_actions VALUES (?,?,'dividend',NULL,1.30,?,?,?)",
                (iid, "2026-08-10", RUN, "yahoo_v8_events", RUN))
    con.commit()
    v = FakeTiingo({"DIV": bars({d: 100.0 for d in DATES}, divs={"2026-08-10": 1.31})})
    V.sweep(con, v, 5, AS_OF, SINCE, run_asof=RUN)
    assert "dividend on 2026-08-10 differs" in con.execute(
        "SELECT detail FROM verification").fetchone()[0]
    # ... and a sub-cent difference is not.
    con.execute("DELETE FROM verification")
    v2 = FakeTiingo({"DIV": bars({d: 100.0 for d in DATES}, divs={"2026-08-10": 1.3009})})
    V.sweep(con, v2, 5, AS_OF, SINCE, run_asof=RUN + 1)
    assert con.execute("SELECT kind FROM verification").fetchone()[0] == "VERIFIED"


def test_cross_vendor_float_precision_is_not_a_disagreement(con):
    """Yahoo serves float32-precision closes widened to float64; Tiingo quotes
    to the cent. At FACT_EPS every real agreement would read as a break."""
    iid = register(con, "0000000006.0", "PREC")
    hold(con, iid, {DATES[0]: 94.16000366210938})
    v = FakeTiingo({"PREC": bars({DATES[0]: 94.16})})
    V.sweep(con, v, 5, AS_OF, SINCE, run_asof=RUN)
    assert con.execute("SELECT kind FROM verification").fetchone()[0] == "VERIFIED"


def test_an_unknown_symbol_is_counted_not_fatal(con):
    register(con, "0000000007.0", "GONE")
    register(con, "0000000008.0", "FINE")
    hold(con, "0000000008.0", {d: 100.0 for d in DATES})
    v = FakeTiingo({"FINE": bars({d: 100.0 for d in DATES})}, unknown={"GONE"})
    res = V.sweep(con, v, 5, AS_OF, SINCE, run_asof=RUN)
    assert res.counts() == {"TIINGO_UNKNOWN": 1, "VERIFIED": 1}


def test_a_name_with_nothing_comparable_is_insufficient(con):
    iid = register(con, "0000000009.0", "EMPTY")
    v = FakeTiingo({"EMPTY": []})
    V.sweep(con, v, 5, AS_OF, SINCE, run_asof=RUN)
    assert con.execute("SELECT kind FROM verification").fetchone()[0] == "INSUFFICIENT"


# ============================================================ unsigned drafts ==

def test_an_auto_drafted_correction_cannot_be_applied_unsigned(con, tmp_path):
    """The machine may propose; only a human may sign."""
    iid = register(con, "0000000010.0", "DRAFT")
    hold(con, iid, {d: 100.0 for d in DATES})
    theirs = {d: 100.0 for d in DATES}
    theirs["2026-08-06"] = 50.0
    v = FakeTiingo({"DRAFT": bars(theirs)})
    res = V.sweep(con, v, 5, AS_OF, SINCE, run_asof=RUN, drafts_root=tmp_path)
    path = Path(res.drafts[0])

    with pytest.raises(CO.CorrectionError) as e:
        CO.load(path)
    assert "authored_by" in str(e.value)

    # Signing it makes it loadable -- the ONLY thing that changes is a name.
    payload = json.loads(path.read_text())
    payload["authored_by"] = "mando"
    path.write_text(json.dumps(payload))
    assert CO.load(path)["authored_by"] == "mando"


def test_the_draft_carries_both_vendors_values(con, tmp_path):
    iid = register(con, "0000000011.0", "EVID")
    hold(con, iid, {DATES[0]: 100.0})
    v = FakeTiingo({"EVID": bars({DATES[0]: 200.0})})
    res = V.sweep(con, v, 5, AS_OF, SINCE, run_asof=RUN, drafts_root=tmp_path)
    ev = json.loads(Path(res.drafts[0]).read_text())["rows"][0]["evidence"]
    assert ev["held_primary"] == 100.0 and ev["verifier"] == 200.0
    assert ev["verifier_name"] == "tiingo"


# ================================================================== ceiling ==

def _log_calls(con, n, at=None):
    at = at or time.time()
    con.executemany(
        "INSERT INTO vendor_calls (vendor, called_at, symbol, bytes, status)"
        " VALUES ('tiingo',?,?,?,200)",
        [(at, "X", 1000) for _ in range(n)])
    con.commit()


def test_a_sweep_that_would_breach_the_hourly_meter_refuses(con):
    _log_calls(con, 45)
    with pytest.raises(T.QuotaExceeded) as e:
        T.check_quota(con, 10)                       # 45 + 10 > 50
    assert "hour" in str(e.value)
    T.check_quota(con, 5)                            # 45 + 5 == 50 is allowed


def test_the_daily_meter_is_enforced_too(con):
    _log_calls(con, 995, at=time.time() - 7200)      # outside the hour, inside the day
    T.check_quota(con, 5)
    with pytest.raises(T.QuotaExceeded) as e:
        T.check_quota(con, 6)
    assert "day" in str(e.value)


def test_the_bandwidth_meter_is_enforced(con):
    con.execute("INSERT INTO vendor_calls (vendor, called_at, symbol, bytes, status)"
                " VALUES ('tiingo',?, 'X', ?, 200)",
                (time.time(), T.LIMIT_BYTES_PER_MONTH + 1))
    con.commit()
    with pytest.raises(T.QuotaExceeded) as e:
        T.check_quota(con, 1)
    assert "bandwidth" in str(e.value)


def test_the_sweep_refuses_before_the_first_request_not_after_the_last(con):
    """A sweep that dies at request 43 of 60 has spent the quota and left the
    store half-verified."""
    iid = register(con, "0000000012.0", "QUOTA")
    hold(con, iid, {d: 100.0 for d in DATES})
    _log_calls(con, 50)
    v = FakeTiingo({"QUOTA": bars({d: 100.0 for d in DATES})})
    with pytest.raises(T.QuotaExceeded):
        V.sweep(con, v, 1, AS_OF, SINCE, run_asof=RUN)
    assert v.calls == [], "not one request may be made"


def test_quota_state_reports_all_three_meters(con):
    _log_calls(con, 3)
    st = T.quota_state(con)
    assert st.last_hour == 3 and st.last_day == 3 and st.month_bytes == 3000
    assert "3/50 hour" in st.render()


# ================================================================ hole fill ==

def test_a_hole_is_filled_and_a_held_value_is_never_touched(con):
    iid = register(con, "0000000020.0", "HOLE")
    held = {d: 100.0 for d in DATES}
    held["2026-08-06"] = None                        # the primary had nothing
    hold(con, iid, held)
    theirs = {d: 100.0 for d in DATES}
    theirs["2026-08-06"] = 91.43
    theirs["2026-08-07"] = 999.0                     # a HELD date the verifier differs on
    v = FakeTiingo({"HOLE": bars(theirs)})

    res = V.fill_holes(con, v, AS_OF, run_asof=RUN)
    assert [f[1] for f in res.filled] == ["2026-08-06"]

    # The hole is filled from the verifier ...
    assert con.execute(
        "SELECT filled_close, source FROM fills WHERE instrument_id=?",
        (iid,)).fetchone()["filled_close"] == 91.43
    # ... the vendor_null row stays as the record that the primary had nothing ...
    assert con.execute(
        "SELECT close, status FROM prices_raw WHERE instrument_id=? AND date='2026-08-06'",
        (iid,)).fetchone()["status"] == "vendor_null"
    # ... and the held 2026-08-07 value is untouched despite the verifier
    # disagreeing about it. Filling and correcting are different acts (G3).
    assert con.execute(
        "SELECT close FROM prices_raw WHERE instrument_id=? AND date='2026-08-07'",
        (iid,)).fetchone()["close"] == 100.0
    assert con.execute(
        "SELECT COUNT(*) FROM fills WHERE date='2026-08-07'").fetchone()[0] == 0


def test_a_hole_neither_vendor_has_stays_a_hole(con):
    iid = register(con, "0000000021.0", "GAP")
    held = {d: 100.0 for d in DATES}
    held["2026-08-06"] = None
    hold(con, iid, held)
    v = FakeTiingo({"GAP": bars({d: 100.0 for d in DATES if d != "2026-08-06"})})
    res = V.fill_holes(con, v, AS_OF, run_asof=RUN)
    assert res.filled == []
    assert (iid, "2026-08-06") in res.unfillable
    assert con.execute("SELECT COUNT(*) FROM fills").fetchone()[0] == 0


def test_a_fresh_hole_is_left_alone_until_it_ages(con):
    """Today's absence is usually an unsettled session, not a hole."""
    iid = register(con, "0000000022.0", "FRESH")
    hold(con, iid, {"2026-08-13": 100.0, "2026-08-14": None})
    assert V.fillable_holes(con, "2026-08-14") == {}
    assert iid in V.fillable_holes(con, "2026-08-20")


def test_a_filled_session_enters_the_analytics_view(con):
    iid = register(con, "0000000023.0", "VIEW")
    held = {d: 100.0 for d in DATES}
    held["2026-08-06"] = None
    hold(con, iid, held)
    theirs = dict.fromkeys(DATES, 100.0)
    theirs["2026-08-06"] = 91.43
    V.fill_holes(con, FakeTiingo({"VIEW": bars(theirs)}), AS_OF, run_asof=RUN)
    W._rebuild_view(con, iid, [], [], 1, RUN)
    con.commit()
    got = con.execute(
        "SELECT adj_close FROM adjusted_view WHERE instrument_id=? AND date='2026-08-06'",
        (iid,)).fetchone()
    assert got is not None and got[0] == pytest.approx(91.43)


def test_a_correction_outranks_a_fill(con):
    """A human adjudication beats an automatic first write."""
    iid = register(con, "0000000024.0", "RANK")
    hold(con, iid, {"2026-08-05": 100.0, "2026-08-06": None})
    V.fill_holes(con, FakeTiingo({"RANK": bars(
        {"2026-08-05": 100.0, "2026-08-06": 91.43})}), AS_OF, run_asof=RUN)
    con.execute("INSERT INTO corrections (instrument_id, date, corrected_close,"
                " reason, authored_by, authored_at) VALUES (?,?,?,?,?,?)",
                (iid, "2026-08-06", 92.00, "checked the tape", "mando", RUN + 1))
    con.commit()
    W._rebuild_view(con, iid, [], [], 1, RUN)
    con.commit()
    assert con.execute(
        "SELECT adj_close FROM adjusted_view WHERE instrument_id=? AND date='2026-08-06'",
        (iid,)).fetchone()[0] == pytest.approx(92.00)


# ================================================================ hygiene ==

def test_no_module_puts_a_token_in_a_query_string():
    """A token in a URL reaches access logs, proxy logs and Referer headers on
    any redirect. redact_url scrubs ours and can do nothing about anyone else's."""
    import ast
    pkg = Path(V.__file__).parent
    offenders = []
    for f in sorted(pkg.glob("*.py")):
        tree = ast.parse(f.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            # A params / query dict carrying a 'token' key. Checked by AST
            # rather than by grep: a line-based check flags `TiingoVendor(
            # token=...)`, which is a Python keyword argument and perfectly
            # safe, and a test that cries wolf gets disabled.
            if isinstance(node, ast.Dict):
                for k in node.keys:
                    if isinstance(k, ast.Constant) and k.value == "token":
                        offenders.append("{}:{} params dict with a token key"
                                         .format(f.name, node.lineno))
            # A string literal embedding the parameter directly in a URL.
            if isinstance(node, ast.Constant) and isinstance(node.value, str):
                v = node.value
                if ("token=" in v or "&token" in v or "?token" in v) \
                        and "query" not in v.lower():
                    offenders.append("{}:{} literal {!r}"
                                     .format(f.name, node.lineno, v[:60]))
    assert not offenders, offenders


def test_the_adapter_authenticates_by_header(con):
    v = T.TiingoVendor(token="x" * 40, con=con)
    h = v._headers()
    assert h["Authorization"] == "Token " + "x" * 40
    assert "token" not in T.BASE_URL


def test_a_missing_token_fails_loud(con):
    for bad in ("", "short"):
        with pytest.raises(T.TiingoError):
            T.TiingoVendor(token=bad, con=con)


def test_the_vendor_symbol_comes_from_aliases_never_surgery(con):
    iid = register(con, "0001067983.1", "BRK-B")
    assert T.vendor_symbol(con, iid) == "BRK-B"
    assert T.vendor_symbol(con, "nope.0") is None
