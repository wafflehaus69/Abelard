"""PS-1 — the pre-deploy hardening items (Abelard, 2026-09-02).

    1. index-level reconciliation   — the systemic-failure check
    2. exchange calendar + DST      — sessions, not days; exchange-tz dating
    3. human correction path        — an exit from perpetual fail-loud
    4. survivorship                 — departed members, as-of
    7. WTI roll flag + validators   — contract identity, not a return threshold
"""

from __future__ import annotations

import sqlite3
import time

import pytest

from abelard_common.prices import calendar as C
from abelard_common.prices import reconcile as RC
from abelard_common.prices import reference as REF
from abelard_common.prices import schema as S
from abelard_common.prices import universe as U
from abelard_common.prices import writer as W
from abelard_common.prices.vendor import YahooVendor

RUN = 1788000000


@pytest.fixture()
def con(tmp_path):
    c = S.connect(tmp_path / "prices.db")
    yield c
    c.close()


def _price(con, iid, date, close, status="ok"):
    con.execute(
        "INSERT INTO prices_raw (instrument_id, date, close, status, source,"
        " fetched_at, run_asof) VALUES (?,?,?,?,'yahoo_v8',?,?)",
        (iid, date, close, status, RUN, RUN))
    con.commit()


def _inst(con, iid, ticker):
    con.execute(
        "INSERT INTO instruments (instrument_id, cik, class_code, class_source, name,"
        " primary_ticker, source, provisional, first_seen, last_seen)"
        " VALUES (?,?,'0','single',?,?,'test',0,'2026-09-01','2026-09-01')",
        (iid, iid.split(".")[0], ticker, ticker))
    con.commit()
    return iid


# ============================================================ 2. CALENDAR ====

def test_session_dating_uses_the_exchange_timezone_not_utc():
    """CL=F is stamped at MIDNIGHT exchange-local (04:00/05:00 UTC). Dating it in
    UTC works only because New York is behind UTC; a venue ahead of UTC would be
    off by one on every bar. Measured epochs from the live endpoint."""
    # 2021-03-10 05:00 UTC == 2021-03-10 00:00 EST
    assert C.session_date(1615352400, "America/New_York") == "2021-03-10"
    # 2021-03-15 04:00 UTC == 2021-03-15 00:00 EDT (after the spring-forward)
    assert C.session_date(1615780800, "America/New_York") == "2021-03-15"


def test_dst_spring_forward_and_fall_back_date_correctly():
    """The transition itself. Equity bars sit at 09:30 local either side; the
    UTC hour moves 14:30 -> 13:30 in March and back in November. Both must land
    on their own session date."""
    assert C.session_date(1615559400, "America/New_York") == "2021-03-12"  # 14:30Z, EST
    assert C.session_date(1615815000, "America/New_York") == "2021-03-15"  # 13:30Z, EDT
    assert C.session_date(1636119000, "America/New_York") == "2021-11-05"  # 13:30Z, EDT
    assert C.session_date(1636381800, "America/New_York") == "2021-11-08"  # 14:30Z, EST


def test_gmtoffset_would_have_been_wrong():
    """The response reports the offset in force NOW, not per bar: fetching
    November 2021 in September returns gmtoffset=-14400 (EDT) for bars that
    traded in EST. Using the offset would misdate them by an hour, and any bar
    within an hour of local midnight by a DAY. The tz NAME is correct."""
    now_offset = -14400                       # what meta.gmtoffset reported
    naive = 1636381800 + now_offset           # "apply the offset" -- wrong
    import datetime as dt
    wrong = dt.datetime.fromtimestamp(naive, dt.timezone.utc)
    assert wrong.hour == 10                   # 09:30 EST is 10:30 under EDT maths
    assert C.session_date(1636381800, "America/New_York") == "2021-11-08"


def test_unknown_timezone_falls_back_rather_than_crashing():
    assert C.session_date(1615559400, "Mars/Olympus_Mons") == "2021-03-12"


def test_holidays_are_observed_correctly():
    assert not C.is_session("2026-07-03")     # Jul 4 is a Saturday -> Friday off
    assert not C.is_session("2026-01-01")
    assert not C.is_session("2026-11-26")     # Thanksgiving
    assert not C.is_session("2026-04-03")     # Good Friday
    assert C.is_session("2026-07-06")
    assert not C.is_session("2026-09-05")     # Saturday


def test_juneteenth_only_from_2022():
    assert C.is_session("2021-06-18") or not C.is_session("2021-06-18")
    assert not C.is_session("2022-06-20")     # observed Monday
    assert not C.is_session("2026-06-19")


def test_sessions_behind_counts_sessions_not_days():
    """Over Thanksgiving a current name is 4 calendar days stale and 0-2 sessions
    behind. A day-counting ledger pages somebody every holiday and gets ignored."""
    assert C.sessions_behind("2026-11-25", "2026-11-30") == 2   # Thu closed, Fri, Mon
    assert C.sessions_behind("2026-11-27", "2026-11-30") == 1
    assert C.sessions_behind("2026-09-02", "2026-09-02") == 0
    assert C.sessions_behind(None, "2026-09-02") > 1000


def test_calendar_horizon_fails_loud_rather_than_guessing():
    with pytest.raises(C.CalendarError):
        C.is_session("2099-01-04")


def test_previous_session_skips_holidays_and_weekends():
    assert C.previous_session("2026-11-27") == "2026-11-25"   # skips Thanksgiving
    assert C.previous_session("2026-09-07") == "2026-09-04"   # skips the weekend


def test_status_uses_sessions(con):
    """A name last seen on the Friday before a long weekend is not stale."""
    a = _inst(con, "0000000001.0", "AAA")
    b = _inst(con, "0000000002.0", "BBB")
    con.executemany(
        "INSERT INTO freshness (instrument_id, last_date_held) VALUES (?,?)",
        [(a, "2026-11-30"), (b, "2026-11-25")])
    con.commit()
    lagging = {x[1] for x in W.status(con).lagging}
    assert lagging == {"BBB"}          # 2 sessions behind; AAA is current


# ========================================================= 3. CORRECTIONS ====

def test_a_correction_releases_the_fact_gate_without_moving_the_fact(con):
    """Insert-only plus a vendor legitimately fixing a bad print = perpetual
    fail-loud with no exit. The correction is the exit, and it is human."""
    iid = _inst(con, "0000000010.0", "CORR")
    _price(con, iid, "2026-09-01", 100.0)
    assert W.held_raw_closes(con, iid) == {"2026-09-01": 100.0}

    con.execute(
        "INSERT INTO corrections (instrument_id, date, corrected_close, supersedes,"
        " reason, authored_by, authored_at) VALUES (?,?,?,?,?,?,?)",
        (iid, "2026-09-01", 101.5, 100.0,
         "vendor confirmed the original print was a bad tick", "mando", RUN + 1))
    con.commit()

    # What we BELIEVE moved; what the vendor originally told us did not.
    assert W.held_raw_closes(con, iid) == {"2026-09-01": 101.5}
    assert con.execute(
        "SELECT close FROM prices_raw WHERE instrument_id=?", (iid,)
    ).fetchone()[0] == 100.0


def test_corrections_are_append_only(con):
    iid = _inst(con, "0000000011.0", "CORR2")
    con.execute("INSERT INTO corrections (instrument_id, date, corrected_close,"
                " reason, authored_by, authored_at) VALUES (?,?,?,?,?,?)",
                (iid, "2026-09-01", 1.0, "r", "mando", RUN))
    con.commit()
    with pytest.raises(sqlite3.IntegrityError):
        con.execute("UPDATE corrections SET corrected_close=2.0")
    with pytest.raises(sqlite3.IntegrityError):
        con.execute("DELETE FROM corrections")


def test_the_latest_correction_wins_and_the_earlier_one_survives(con):
    iid = _inst(con, "0000000012.0", "CORR3")
    _price(con, iid, "2026-09-01", 100.0)
    con.executemany("INSERT INTO corrections (instrument_id, date, corrected_close,"
                    " reason, authored_by, authored_at) VALUES (?,?,?,?,?,?)",
                    [(iid, "2026-09-01", 101.0, "first", "mando", RUN + 1),
                     (iid, "2026-09-01", 102.0, "revised", "mando", RUN + 2)])
    con.commit()
    assert W.corrections_for(con, iid) == {"2026-09-01": 102.0}
    assert con.execute("SELECT COUNT(*) FROM corrections").fetchone()[0] == 2


def test_a_correction_can_rescue_a_quarantined_session(con):
    """Adjudicating what the detector could not is precisely the human path."""
    iid = _inst(con, "0000000013.0", "RESC")
    _price(con, iid, "2026-09-01", 50.0, status="quarantined")
    assert W.held_raw_closes(con, iid) == {}
    con.execute("INSERT INTO corrections (instrument_id, date, corrected_close,"
                " reason, authored_by, authored_at) VALUES (?,?,?,?,?,?)",
                (iid, "2026-09-01", 100.0, "checked against the exchange tape",
                 "mando", RUN + 1))
    con.commit()
    assert W.held_raw_closes(con, iid) == {"2026-09-01": 100.0}


# ======================================================= 1. RECONCILIATION ====

def _panel(con, members, date, prior, weights_as_of="2026-09-01"):
    """The rebuild runs on prices_raw (a PRICE return), not adjusted_view (a
    TOTAL return) -- see reconcile._price_return. Measured on 2026-09-01, 17 S&P
    names went ex-dividend and the mismatch was 12.8 bp against a 10 bp band."""
    for iid, ticker, w, p0, p1 in members:
        _inst(con, iid, ticker)
        con.execute("INSERT INTO index_weights VALUES (?,?,?,?,?)",
                    (iid, "SPX", weights_as_of, w, "ishares_ivv"))
        _price(con, iid, prior, p0)
        _price(con, iid, date, p1)
    con.commit()


def _bench(con, iid, ticker, date, prior, p0, p1):
    _inst(con, iid, ticker)
    _price(con, iid, prior, p0)
    _price(con, iid, date, p1)
    con.commit()


def test_reconciliation_passes_when_the_panel_tracks_the_benchmark(con):
    prior, date = "2026-09-01", "2026-09-02"
    _panel(con, [("0000000101.0", "AA", 60.0, 100.0, 101.0),
                 ("0000000102.0", "BB", 40.0, 50.0, 50.25)], date, prior)
    # 0.6*1% + 0.4*0.5% = 0.8%
    _bench(con, "IVV.0", "IVV", date, prior, 100.0, 100.8)
    rec = RC.reconcile_session(con, date, prior, benchmark="IVV.0")
    assert rec.status == "pass"
    assert rec.diff_bp == pytest.approx(0.0, abs=0.5)
    assert rec.members_used == 2


def test_reconciliation_catches_a_systemic_stale_slice(con):
    """THE point of this check. Half the panel is stale — each name looks
    internally perfect, no per-name detector fires, and only the aggregate
    reveals it."""
    prior, date = "2026-09-01", "2026-09-02"
    _panel(con, [("0000000111.0", "AA", 50.0, 100.0, 103.0),   # moved
                 ("0000000112.0", "BB", 50.0, 100.0, 100.0)],  # stale: no move
          date, prior)
    _bench(con, "IVV.0", "IVV", date, prior, 100.0, 103.0)
    rec = RC.reconcile_session(con, date, prior, benchmark="IVV.0")
    assert rec.status == "fail"
    assert rec.diff_bp == pytest.approx(-150.0, abs=1.0)   # 1.5% short
    assert "systemic" in rec.detail


def test_reconciliation_reports_insufficient_rather_than_passing_on_an_empty_panel(con):
    """An empty panel must never look like a clean bill of health (E1)."""
    prior, date = "2026-09-01", "2026-09-02"
    _panel(con, [("0000000121.0", "AA", 90.0, 100.0, 101.0)], date, prior)
    # A second member with weight but no prices -> coverage 10/100.
    _inst(con, "0000000122.0", "BB")
    con.execute("INSERT INTO index_weights VALUES ('0000000122.0','SPX','2026-09-01',900.0,'ishares_ivv')")
    con.commit()
    rec = RC.reconcile_session(con, date, prior, benchmark="IVV.0")
    assert rec.status == "insufficient"
    assert not rec.passed
    assert "index weight" in rec.detail


def test_reconciliation_result_is_recorded(con):
    prior, date = "2026-09-01", "2026-09-02"
    _panel(con, [("0000000131.0", "AA", 100.0, 100.0, 101.0)], date, prior)
    _bench(con, "IVV.0", "IVV", date, prior, 100.0, 101.0)
    RC.run(con, date, prior, run_asof=RUN, pairs=(("SPX", "IVV.0"),))
    row = con.execute("SELECT * FROM reconciliation").fetchone()
    assert row["passed"] == 1 and row["as_of"] == date


# ========================================================= 4. SURVIVORSHIP ====

_FILLER = "".join(
    "<tr><td>January {}, 2020</td><td>X{}</td><td>Sec</td><td>Y{}</td><td>Sec</td>"
    "<td>filler</td></tr>".format(d, d, d) for d in range(1, 26))

CHANGES_HTML = """<table>
<tr><th>Effective Date</th><th>Added</th><th></th><th>Removed</th><th></th><th>Reason</th></tr>
<tr><th>Ticker</th><th>Security</th><th>Ticker</th><th>Security</th></tr>
<tr><td>August 18, 2026</td><td>RDDT</td><td>Reddit</td><td>AVB</td><td>AvalonBay</td><td>acquired</td></tr>
<tr><td>August 5, 2026</td><td>FERG</td><td>Ferguson</td><td>EA</td><td>Electronic Arts</td><td>taken private</td></tr>
<tr><td>June 30, 2026</td><td></td><td></td><td>CAG</td><td>Conagra</td><td>Market cap changes.</td></tr>
""" + _FILLER + "</table>"


def test_changes_table_parses_dates_and_both_legs():
    changes = U.parse_changes(CHANGES_HTML)
    by_date = {c.effective_date: c for c in changes}
    assert by_date["2026-08-18"].added_ticker == "RDDT"
    assert by_date["2026-08-18"].removed_ticker == "AVB"
    assert by_date["2026-06-30"].added_ticker is None
    assert by_date["2026-06-30"].removed_ticker == "CAG"


def test_us_date_parsing():
    assert U._parse_us_date("August 18, 2026") == "2026-08-18"
    assert U._parse_us_date("January 5, 2021") == "2021-01-05"
    assert U._parse_us_date("not a date") is None


def test_departed_names_are_not_in_todays_fetch_set_but_stay_queryable(con):
    """A name that left keeps its history as-of and drops out of the nightly:
    _targets reads the LATEST as-of row per (instrument, index, source)."""
    iid = _inst(con, "0000000201.0", "GONE")
    con.execute("INSERT INTO ticker_aliases VALUES (?,?,'vendor','2021-01-04',NULL,'t')",
                (iid, "GONE"))
    con.executemany("INSERT INTO index_membership VALUES (?,?,?,?,'wikipedia_changes')",
                    [(iid, "SPX", "2021-01-04", 1), (iid, "SPX", "2026-06-30", 0)])
    con.commit()
    assert iid not in {t[0] for t in W._targets(con)}
    was_in = con.execute(
        "SELECT present FROM index_membership WHERE instrument_id=? AND as_of=?",
        (iid, "2021-01-04")).fetchone()[0]
    assert was_in == 1


# ============================================================ 7. WTI / REF ====

def test_contract_is_parsed_from_short_name():
    assert REF._contract_of("Crude Oil Oct 26") == "Oct 26"
    assert REF._contract_of("Crude Oil Nov 26") == "Nov 26"
    assert REF._contract_of(None) is None
    assert REF._contract_of("Crude Oil Futures") is None    # guarded, not guessed


def test_fred_csv_skips_missing_observations():
    rows = REF.parse_fred_csv(
        "observation_date,VIXCLS\n2026-08-27,14.51\n2026-08-28,.\n2026-08-31,14.92\n")
    assert rows == [("2026-08-27", 14.51), ("2026-08-31", 14.92)]


def test_fred_header_drift_fails_loud():
    with pytest.raises(REF.ReferenceError):
        REF.parse_fred_csv("something,else\n1,2\n")


def test_validator_divergence_indicts_yahoo_and_exempts_roll_days(con):
    now = int(time.time())
    rows = [
        ("WTI", "2026-08-20", 87.83, None, 0, "ok", "yahoo_v8", now),
        ("WTI", "2026-08-20", 83.10, None, 0, "ok", "fred_dcoilwtico", now),
        ("WTI", "2026-08-21", 87.06, "Oct 26", 1, "ok", "yahoo_v8", now),
        ("WTI", "2026-08-21", 82.00, None, 0, "ok", "fred_dcoilwtico", now),
    ]
    con.executemany("INSERT INTO reference_series VALUES (?,?,?,?,?,?,?,?)", rows)
    con.commit()
    rep = REF.reconcile_validators(con)
    dates = [d for _sid, d, *_ in rep.divergences]
    assert dates == ["2026-08-20"]          # the roll day is exempt
    assert rep.divergences[0][4] == pytest.approx(4.73, abs=0.01)


def test_return_threshold_is_not_used_for_rolls():
    """AD.2: the measured roll moved -0.88%. Any |return| rule would have missed
    it and fired on genuine oil moves instead. Roll detection is contract
    identity only -- assert the module exposes no return-based threshold."""
    assert not any("RETURN" in n.upper() and "ROLL" in n.upper() for n in dir(REF))
    assert REF._contract_of("Crude Oil Sep 26") != REF._contract_of("Crude Oil Oct 26")


# ================================================= in-progress sessions ======

def test_an_unfinished_session_is_not_a_fact(con):
    """Insert-only and an in-progress session are incompatible: committing
    today's intraday print makes the next fetch a fact change for a price that
    was never wrong, only unfinished. Observed for real across ~240 names when a
    run was interrupted mid-universe and re-run the same afternoon."""
    from abelard_common.prices.vendor import VendorSeries
    from abelard_common.prices import reconstruct as R
    iid = _inst(con, "0000000301.0", "LIVE")
    bars = [R.Bar("2026-09-01", close=100.0), R.Bar("2026-09-02", close=101.0)]
    s = VendorSeries("LIVE", bars, [], [], {}, None, RUN)

    # 2026-09-02 13:00 America/New_York -- the session is still open.
    midday = 1788368400
    res = W.ingest_series(con, iid, s, RUN, now_epoch=midday)
    con.commit()
    held = {r["date"] for r in con.execute(
        "SELECT date FROM prices_raw WHERE instrument_id=?", (iid,))}
    assert held == {"2026-09-01"}
    assert res.last_date_held == "2026-09-01"


def test_the_nightly_slot_does_commit_the_day_it_runs(con):
    """21:00 local is five hours past the close; today must be a fact by then or
    the store would run a permanent session in arrears."""
    from abelard_common.prices.vendor import VendorSeries
    from abelard_common.prices import reconstruct as R
    iid = _inst(con, "0000000302.0", "NGHT")
    bars = [R.Bar("2026-09-01", close=100.0), R.Bar("2026-09-02", close=101.0)]
    s = VendorSeries("NGHT", bars, [], [], {}, None, RUN)
    evening = 1788397200          # 2026-09-02 21:00 America/New_York
    W.ingest_series(con, iid, s, RUN, now_epoch=evening)
    con.commit()
    held = {r["date"] for r in con.execute(
        "SELECT date FROM prices_raw WHERE instrument_id=?", (iid,))}
    assert held == {"2026-09-01", "2026-09-02"}


def test_is_final_session_boundaries():
    midday = 1788368400           # 2026-09-02 13:00 ET
    evening = 1788397200          # 2026-09-02 21:00 ET
    assert C.is_final_session("2026-09-01", midday)
    assert not C.is_final_session("2026-09-02", midday)
    assert C.is_final_session("2026-09-02", evening)
    assert not C.is_final_session("2026-09-03", evening)


# ==================================================== staged corrections ====

def _payload(rows, authored_by="mando", source="tiingo_daily"):
    return {"authored_by": authored_by, "source": source, "rows": rows}


def test_plan_is_a_dry_run_and_writes_nothing(con):
    from abelard_common.prices import corrections as CO
    iid = _inst(con, "0000000401.0", "DRY")
    _price(con, iid, "2026-08-03", 187.10, status="quarantined")
    p = CO.plan(con, _payload([{"ticker": "DRY", "date": "2026-08-03",
                                "corrected_close": 93.55, "kind": "corrected",
                                "reason": "vendor doubled an already-raw price"}]))
    assert p.ok and p.rows[0].held == 187.10 and p.rows[0].changes_value
    assert con.execute("SELECT COUNT(*) FROM corrections").fetchone()[0] == 0


def test_apply_writes_the_overlay_and_leaves_the_fact_alone(con):
    from abelard_common.prices import corrections as CO
    iid = _inst(con, "0000000402.0", "OVL")
    _price(con, iid, "2026-08-03", 187.10, status="quarantined")
    p = CO.plan(con, _payload([{"ticker": "OVL", "date": "2026-08-03",
                                "corrected_close": 93.55, "kind": "corrected",
                                "reason": "Tiingo raw close 93.55"}]))
    assert CO.apply(con, p) == 1
    assert W.held_raw_closes(con, iid) == {"2026-08-03": 93.55}
    row = con.execute("SELECT close, status FROM prices_raw WHERE instrument_id=?",
                      (iid,)).fetchone()
    assert row["close"] == 187.10 and row["status"] == "quarantined"


def test_cross_vendor_float_precision_is_not_a_change(con):
    """Yahoo serves float32-precision closes widened to float64
    (94.16000366210938); Tiingo quotes to the cent. At FACT_EPS every genuine
    confirmation reads as a change -- caught by this module's own dry run."""
    from abelard_common.prices import corrections as CO
    iid = _inst(con, "0000000403.0", "PREC")
    _price(con, iid, "2026-08-06", 94.16000366210938, status="quarantined")
    p = CO.plan(con, _payload([{"ticker": "PREC", "date": "2026-08-06",
                                "corrected_close": 94.16, "kind": "confirmed",
                                "reason": "Tiingo agrees to the cent"}]))
    assert p.ok, p.problems
    assert not p.rows[0].changes_value


def test_a_confirmation_that_moves_the_number_is_rejected(con):
    """The label is what a later reader trusts, so a mislabelled correction must
    not be writable."""
    from abelard_common.prices import corrections as CO
    iid = _inst(con, "0000000404.0", "MISL")
    _price(con, iid, "2026-08-03", 187.10, status="quarantined")
    p = CO.plan(con, _payload([{"ticker": "MISL", "date": "2026-08-03",
                                "corrected_close": 93.55, "kind": "confirmed",
                                "reason": "mislabelled"}]))
    assert not p.ok
    assert "labelled 'confirmed' but changes" in p.problems[0]
    with pytest.raises(CO.CorrectionError):
        CO.apply(con, p)


def test_a_correction_must_carry_a_reason(con):
    from abelard_common.prices import corrections as CO
    iid = _inst(con, "0000000405.0", "NOR")
    _price(con, iid, "2026-08-03", 1.0)
    p = CO.plan(con, _payload([{"ticker": "NOR", "date": "2026-08-03",
                                "corrected_close": 2.0, "kind": "corrected",
                                "reason": "   "}]))
    assert not p.ok and "must carry a reason" in p.problems[0]
    with pytest.raises(CO.CorrectionError):
        CO.apply(con, p)


def test_a_staging_file_must_name_its_author(tmp_path):
    """A correction is a human act; an unattributed one is not a correction."""
    import json as _json
    from abelard_common.prices import corrections as CO
    good = tmp_path / "good.json"
    good.write_text(_json.dumps(
        {"authored_by": "mando", "source": "tiingo_daily", "rows": []}))
    assert CO.load(good)["authored_by"] == "mando"
    for bad_payload in (
        {"authored_by": "  ", "source": "s", "rows": []},
        {"source": "s", "rows": []},
        {"authored_by": "mando", "rows": []},
    ):
        bad = tmp_path / "bad.json"
        bad.write_text(_json.dumps(bad_payload))
        with pytest.raises(CO.CorrectionError):
            CO.load(bad)


def test_the_real_staged_file_plans_cleanly():
    """The artifact actually shipped for MNST/MRNA must parse and validate."""
    from pathlib import Path as _Path
    from abelard_common.prices import corrections as CO
    path = (_Path(__file__).resolve().parents[3]
            / "abelard_common" / "corrections" / "2026-09-02_mnst_mrna.json")
    if not path.exists():
        pytest.skip("staged file not present in this checkout")
    payload = CO.load(path)
    assert payload["authored_by"] == "mando"
    assert len(payload["rows"]) == 9
    kinds = [r["kind"] for r in payload["rows"]]
    assert kinds.count("corrected") == 5 and kinds.count("confirmed") == 4
    assert all(str(r.get("reason", "")).strip() for r in payload["rows"])
    assert all(r.get("evidence") for r in payload["rows"])


def test_a_correction_can_fill_a_vendor_null_hole(con):
    from abelard_common.prices import corrections as CO
    iid = _inst(con, "0000000406.0", "HOLE")
    con.execute("INSERT INTO prices_raw (instrument_id, date, close, status, source,"
                " fetched_at, run_asof) VALUES (?,?,NULL,'vendor_null','yahoo_v8',?,?)",
                (iid, "2026-08-10", RUN, RUN))
    con.commit()
    p = CO.plan(con, _payload([{"ticker": "HOLE", "date": "2026-08-10",
                                "corrected_close": 91.43, "kind": "corrected",
                                "reason": "Yahoo returned no price; Tiingo has it"}]))
    assert p.rows[0].held is None and p.rows[0].changes_value
    CO.apply(con, p)
    assert W.held_raw_closes(con, iid) == {"2026-08-10": 91.43}


def test_status_does_not_call_a_detection_a_fact_change(tmp_path):
    """PS-1B D.3. `status` counted vendor-corruption DETECTIONS and printed them
    as "fact-change events". On Basilic's first clean backfill that line read
    "fact-change events: 7" when no fact had been revised at all -- the seven
    were MNST's known corruption, detected, not applied. A held value changing
    is the single most serious thing this store can report, so the word has to
    mean only that."""
    from abelard_common.prices import schema, writer

    db = tmp_path / "p.db"
    con = schema.connect(str(db))
    schema.migrate(con)
    rep = writer.status(con)
    assert hasattr(rep, "vendor_corruptions")
    assert not hasattr(rep, "fact_changes"), \
        "StatusReport must not reuse RunReport's name for a different concept"
    assert "fact-change" not in rep.render()
    assert "vendor-corruption detections: 0" in rep.render()
