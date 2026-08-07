"""price_backfill orchestration tests. prices.eod is swapped for a recorder so no
network is touched; only the target selection + run accounting are exercised."""
import datetime as dt
import os
import tempfile

from smart_money import db as dbmod
from smart_money import prices
from smart_money import price_backfill as pb

_FI = ("INSERT INTO form4_transactions(accession, tx_index, reporting_person, "
       "reporting_cik, issuer, issuer_cik, ticker, code, plan_flag, shares, price, "
       "value, ownership_after, tx_date, filed_date, role, ingest_regime) "
       "VALUES(?,0,'P','1','Co','9',?,?,0,1,1.0,1,NULL,?,?,NULL,'watchlist')")


def _db(rows):
    path = tempfile.mktemp(suffix=".db")
    con = dbmod.connect(path)
    for acc, tk, code, tx in rows:
        con.execute(_FI, (acc, tk, code, tx, tx))
    con.commit()
    return path, con


def test_targets_selects_ps_earliest_excludes_nonticker_and_since():
    path, con = _db([
        ("a", "AAA", "P", "2026-06-01"),
        ("b", "AAA", "S", "2026-05-01"),   # earlier sell -> AAA earliest = 2026-05-01
        ("c", "BBB", "P", "2026-04-01"),
        ("d", "NONE", "P", "2026-03-01"),  # non-security excluded
        ("e", "CCC", "M", "2026-02-01"),   # not P/S excluded
    ])
    assert dict(pb.targets(con)) == {"AAA": "2026-05-01", "BBB": "2026-04-01"}
    # since filters the ROWS first, so AAA's earliest becomes its post-cutoff min
    assert dict(pb.targets(con, since="2026-05-15")) == {"AAA": "2026-06-01"}
    con.close()
    os.unlink(path)


def test_targets_skip_invalid_symbols():
    # slashes / spaces are not valid Yahoo symbols and would crash the fetch/dump path
    path, con = _db([("a", "AAA", "P", "2026-06-01"),
                     ("b", "UONE/UONEK", "P", "2026-05-01"),
                     ("c", "BAD SYM", "P", "2026-04-01")])
    t = dict(pb.targets(con))
    assert "AAA" in t and "UONE/UONEK" not in t and "BAD SYM" not in t, t
    con.close()
    os.unlink(path)


_HI = ("INSERT INTO thirteenf_holdings(cik, accession, period, filed_date, cusip, "
       "ticker, issuer, put_call, value, shares, ingested_at_unix) "
       "VALUES(?,?,?,?,?,?,'Co','long',1,1,0)")


def test_targets_include_13f_holdings_so_the_unit_anchor_can_be_priced():
    """G1 residue: _filer_unit_scale decides dollars-vs-thousands against an EOD close,
    so a 13F name that never appears in a Form 4 must still be priced. Affinity holds
    exactly one position (QXO) and no insider ever traded it, which left its unit
    UNDETERMINED. A filer's unit may not depend on that coincidence."""
    path, con = _db([("a", "AAA", "P", "2026-06-01")])
    con.execute(_HI, ("1", "acc1", "2025-12-31", "2026-02-14", "c1", "QXO"))
    con.execute(_HI, ("1", "acc2", "2026-03-31", "2026-05-15", "c1", "QXO"))
    con.commit()
    t = dict(pb.targets(con))
    assert "AAA" in t and "QXO" in t, t
    assert t["QXO"] == "2025-12-31", "earliest period held, not the latest"
    con.close()
    os.unlink(path)


def test_13f_only_ticker_already_priced_is_skipped_by_only_missing():
    path, con = _db([("a", "AAA", "P", "2026-06-01")])
    con.execute(_HI, ("1", "acc1", "2026-03-31", "2026-05-15", "c1", "QXO"))
    con.execute(_HI, ("1", "acc1", "2026-03-31", "2026-05-15", "c2", "ZZZ"))
    con.execute("INSERT INTO prices VALUES('QXO','2026-03-31',19,19,'eod',0,0,'y')")
    con.commit()
    t = dict(pb.targets(con, only_missing=True))
    assert "QXO" not in t and "ZZZ" in t, t
    con.close()
    os.unlink(path)


def test_a_ticker_in_both_populations_is_fetched_once_from_the_earlier_date():
    """AAA is both insider-traded (2026) and 13F-held (2025). One target — and it must
    start at the EARLIER date, or the 2025 period keeps the gap that broke the anchor."""
    path, con = _db([("a", "AAA", "P", "2026-06-01")])
    con.execute(_HI, ("1", "acc1", "2025-03-31", "2025-05-15", "c1", "AAA"))
    con.commit()
    tg = pb.targets(con)
    assert [t for t, _ in tg].count("AAA") == 1, tg
    assert dict(tg)["AAA"] == "2025-03-31", "earlier of the two populations wins"
    con.close()
    os.unlink(path)


def test_run_survives_non_price_exception():
    path, con = _db([("a", "AAA", "P", "2026-06-01"), ("b", "BBB", "P", "2026-05-01")])
    orig = prices.eod

    def boom(c, tk, s, e):
        if tk == "BBB":
            raise OSError("disk gremlin")     # NOT a PriceError
        return []
    prices.eod = boom
    try:
        res = pb.run(con, out=open(os.devnull, "w"))
        assert res == {"total": 2, "ok": 1, "fail": 1}, res   # OSError counted, not raised
    finally:
        prices.eod = orig
    con.close()
    os.unlink(path)


def test_only_missing_skips_already_priced():
    path, con = _db([("a", "AAA", "P", "2026-06-01"), ("b", "BBB", "P", "2026-05-01")])
    con.execute("INSERT INTO prices VALUES('AAA','2026-06-01',10,10,'eod',0,0,'y')")
    con.commit()
    t = dict(pb.targets(con, only_missing=True))
    assert "AAA" not in t and "BBB" in t, t
    con.close()
    os.unlink(path)


def test_run_counts_ok_fail_and_respects_limit():
    path, con = _db([("a", "AAA", "P", "2026-06-01"), ("b", "BBB", "P", "2026-05-01"),
                     ("c", "FAILT", "P", "2026-04-01")])
    calls = []
    orig = prices.eod

    def fake_eod(c, tk, start, end):
        calls.append((tk, start, end))
        if tk == "FAILT":
            raise prices.PriceDegraded("boom")
        return []
    prices.eod = fake_eod
    try:
        res = pb.run(con, out=open(os.devnull, "w"))
        assert res == {"total": 3, "ok": 2, "fail": 1}, res
        # earliest-date order -> FAILT(2026-04-01) first; start is the earliest date
        assert calls[0][0] == "FAILT" and calls[0][1] == "2026-04-01", calls[0]
        assert calls[-1][2] == dt.date.today().isoformat(), "end is today"
        calls.clear()
        res2 = pb.run(con, limit=1, out=open(os.devnull, "w"))
        assert res2["total"] == 1 and len(calls) == 1, (res2, calls)
    finally:
        prices.eod = orig
    con.close()
    os.unlink(path)


def test_floor_days_bounds_the_start():
    path, con = _db([("a", "AAA", "P", "2020-01-01")])   # very old trade
    captured = []
    orig = prices.eod
    prices.eod = lambda c, tk, s, e: captured.append((tk, s, e)) or []
    try:
        pb.run(con, floor_days=30, out=open(os.devnull, "w"))
    finally:
        prices.eod = orig
    floor = (dt.date.today() - dt.timedelta(days=30)).isoformat()
    assert captured[0][1] == floor, "start floored to N days ago, not the 2020 trade"
    con.close()
    os.unlink(path)
