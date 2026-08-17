"""CUSIP -> ticker must resolve to the US composite listing.

OpenFIGI returns every listing of an instrument worldwide, unordered. The old code
took data[0]. Insmed's CUSIP 457669307 returns 109 records with a Frankfurt line
first and exactly one exchCode='US' — so it stored as IM8N. Somnigroup stored as
TPD, Baidu as BIDUN. Form 13F covers section 13(f) securities, which are
US-exchange-traded, so a non-US result is definitionally a resolver error.
"""
import os
import tempfile

from smart_money import db as dbmod, repair_cusips, thirteenf_ingest


# The real shape, trimmed: Frankfurt first, one US record. Verified live.
INSMED = [
    {"ticker": "IM8N", "exchCode": "GR", "marketSector": "Equity",
     "securityType": "Common Stock", "name": "INSMED INC"},
    {"ticker": "IM8N", "exchCode": "XE", "marketSector": "Equity",
     "securityType": "Common Stock", "name": "INSMED INC"},
    {"ticker": "INSM", "exchCode": "US", "marketSector": "Equity",
     "securityType": "Common Stock", "name": "INSMED INC"},
]


def _db():
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    return path


# ------------------------------------------------------------------ picking ---

def test_us_listing_wins_over_a_foreign_data_zero():
    rec, how = thirteenf_ingest.pick_listing(INSMED)
    assert rec["ticker"] == "INSM", "data[0] was IM8N, a Frankfurt line"
    assert how == "openfigi_us"


def test_equity_wins_over_corp_among_us_records():
    """A convertible's CUSIP can return both an Equity and a Corp US record; Corp
    is how Bloomberg bond descriptors like 'GOOGL 6.25 05/15/29 A' won the field."""
    data = [
        {"ticker": "GOOGL 6.25 05/15/29 A", "exchCode": "US",
         "marketSector": "Corp", "securityType": "PRIV PLACEMENT"},
        {"ticker": "GOOGL", "exchCode": "US", "marketSector": "Equity",
         "securityType": "Common Stock"},
    ]
    rec, how = thirteenf_ingest.pick_listing(data)
    assert rec["ticker"] == "GOOGL"
    assert how == "openfigi_us"


def test_no_us_listing_still_yields_a_ticker_but_says_it_is_suspect():
    """Mark, never drop — the row keeps a symbol so nothing goes blank, but names
    itself so a repair pass can find it."""
    data = [{"ticker": "XYZ1", "exchCode": "GR", "marketSector": "Equity"}]
    rec, how = thirteenf_ingest.pick_listing(data)
    assert rec["ticker"] == "XYZ1"
    assert how == "openfigi_foreign"


def test_empty_response_is_a_miss_not_a_crash():
    rec, how = thirteenf_ingest.pick_listing([])
    assert rec is None and how == "openfigi_miss"
    rec, how = thirteenf_ingest.pick_listing(None)
    assert rec is None and how == "openfigi_miss"


# -------------------------------------------------------------- persistence ---

class _Resp:
    def __init__(self, payload, status=200):
        self._p, self.status_code = payload, status

    def json(self):
        return self._p


def test_mapping_records_the_venue_it_chose(monkeypatch):
    path = _db()
    try:
        con = dbmod.connect(path)
        monkeypatch.setattr(thirteenf_ingest.time, "sleep", lambda *_a: None)
        monkeypatch.setattr(thirteenf_ingest.requests, "post",
                            lambda *a, **k: _Resp([{"data": INSMED}]))
        out = thirteenf_ingest.map_cusips(con, ["457669307"], "test")
        assert out["457669307"] == "INSM"
        row = con.execute(
            "SELECT ticker, mapped_via, exch_code, market_sector, ticker_raw "
            "FROM cusip_ticker WHERE cusip='457669307'").fetchone()
        assert row[0] == "INSM"
        assert row[1] == "openfigi_us"
        assert row[2] == "US"
        assert row[3] == "Equity"
        assert row[4] == "IM8N", "the rejected data[0] pick is kept for audit"
    finally:
        os.remove(path)


def test_a_failed_request_writes_nothing_and_is_retried(monkeypatch):
    """THE regression. The old code wrote a NULL-ticker row tagged 'openfigi' on
    failure — identical to a genuine miss — and map_cusips only ever queries CUSIPs
    it has not seen, so it was never retried. 136 of 1300 cached rows are NULL and
    which of them are poisoned is unrecoverable."""
    path = _db()
    try:
        con = dbmod.connect(path)
        monkeypatch.setattr(thirteenf_ingest.time, "sleep", lambda *_a: None)
        monkeypatch.setattr(thirteenf_ingest.requests, "post",
                            lambda *a, **k: _Resp(None, status=503))
        report = {}
        out = thirteenf_ingest.map_cusips(con, ["457669307"], "test", report)
        assert out["457669307"] is None
        assert con.execute("SELECT COUNT(*) FROM cusip_ticker").fetchone()[0] == 0, (
            "a network failure must leave no cache row, so the next run retries")
        assert report["openfigi_unreachable"] == 1, "and it must be reported"

        # next run succeeds and the cusip resolves
        monkeypatch.setattr(thirteenf_ingest.requests, "post",
                            lambda *a, **k: _Resp([{"data": INSMED}]))
        out = thirteenf_ingest.map_cusips(con, ["457669307"], "test")
        assert out["457669307"] == "INSM"
    finally:
        os.remove(path)


def test_a_genuine_miss_is_recorded_as_a_miss(monkeypatch):
    path = _db()
    try:
        con = dbmod.connect(path)
        monkeypatch.setattr(thirteenf_ingest.time, "sleep", lambda *_a: None)
        monkeypatch.setattr(thirteenf_ingest.requests, "post",
                            lambda *a, **k: _Resp([{"warning": "No identifier found"}]))
        thirteenf_ingest.map_cusips(con, ["000000000"], "test")
        row = con.execute("SELECT ticker, mapped_via FROM cusip_ticker").fetchone()
        assert row == (None, "openfigi_miss"), (
            "distinguishable from a network failure, which writes nothing at all")
    finally:
        os.remove(path)


# ------------------------------------------------------------------ repair ---

def test_repair_targets_legacy_foreign_and_null_rows():
    path = _db()
    try:
        con = dbmod.connect(path)
        for cusip, tk, via in (("457669307", "IM8N", "openfigi"),
                               ("056752108", "BIDUN", "openfigi_foreign"),
                               ("000000001", None, "openfigi_miss"),
                               ("15675D103", "CBRS", "openfigi_us")):
            con.execute("INSERT INTO cusip_ticker(cusip, ticker, name, mapped_via,"
                        " mapped_at_unix) VALUES (?,?,?,?,0)",
                        (cusip, tk, None, via))
        con.commit()
        t = repair_cusips.targets(con)
        assert "457669307" in t, "legacy provenance must be re-asked"
        assert "056752108" in t, "known-foreign must be re-asked"
        assert "000000001" in t, "NULL could be an error-poisoned row"
        assert "15675D103" not in t, "an already-US pick is left alone"
    finally:
        os.remove(path)


def test_repair_propagates_to_the_holdings_not_just_the_cache():
    """Repairing cusip_ticker alone changes nothing a reader sees —
    thirteenf_holdings.ticker was stamped from the cache at ingest."""
    path = _db()
    try:
        con = dbmod.connect(path)
        con.execute("INSERT INTO cusip_ticker(cusip, ticker, name, mapped_via,"
                    " mapped_at_unix) VALUES ('457669307','IM8N',NULL,'openfigi',0)")
        con.execute(
            "INSERT INTO thirteenf_holdings(cik, accession, period, filed_date, "
            "cusip, ticker, issuer, put_call, value, shares, ingested_at_unix) "
            "VALUES ('1263508','a','2026-06-30','2026-08-14','457669307','IM8N',"
            "'Insmed Incorporated','long',937841355,8796111,0)")
        con.commit()
        got = {"457669307": ("INSM", "INSMED INC", "US", "Equity",
                             "Common Stock", "IM8N", "openfigi_us")}
        cache, holdings, _flagged = repair_cusips.apply(con, got)
        assert cache == 1 and holdings == 1
        assert con.execute(
            "SELECT ticker FROM thirteenf_holdings").fetchone()[0] == "INSM"
        assert con.execute(
            "SELECT ticker, mapped_via FROM cusip_ticker").fetchone() == (
                "INSM", "openfigi_us")
    finally:
        os.remove(path)


def test_repair_is_idempotent():
    path = _db()
    try:
        con = dbmod.connect(path)
        con.execute("INSERT INTO cusip_ticker(cusip, ticker, name, mapped_via,"
                    " mapped_at_unix) VALUES ('457669307','INSM',NULL,'openfigi_us',0)")
        con.execute(
            "INSERT INTO thirteenf_holdings(cik, accession, period, filed_date, "
            "cusip, ticker, issuer, put_call, value, shares, ingested_at_unix) "
            "VALUES ('1','a','2026-06-30','2026-08-14','457669307','INSM',"
            "'Insmed','long',1,1,0)")
        con.commit()
        got = {"457669307": ("INSM", "INSMED INC", "US", "Equity",
                             "Common Stock", "IM8N", "openfigi_us")}
        _c, holdings, _f = repair_cusips.apply(con, got)
        assert holdings == 0, "nothing to reticker when it is already correct"
    finally:
        os.remove(path)


def test_a_miss_never_overwrites_an_existing_symbol():
    """AIR LEASE CORP's CUSIP was cached as AL, which is CORRECT, and OpenFIGI
    returned no US record for it. Applying the miss would have blanked a right
    answer on a $78M position."""
    assert repair_cusips.is_safe("AL", None, "openfigi_miss") is False


def test_a_foreign_pick_never_overwrites_an_existing_symbol():
    """Electronic Arts is cached as EA. A non-US pick offered EA*, a Bloomberg
    delisted-line marker."""
    assert repair_cusips.is_safe("EA", "EA*", "openfigi_foreign") is False


def test_a_us_pick_does_overwrite():
    assert repair_cusips.is_safe("IM8N", "INSM", "openfigi_us") is True
    assert repair_cusips.is_safe("BAC", "VZ", "openfigi_us") is True


def test_a_us_pick_that_agrees_is_not_a_change():
    assert repair_cusips.is_safe("INSM", "INSM", "openfigi_us") is False


def test_withheld_rows_keep_their_symbol_and_are_flagged():
    path = _db()
    try:
        con = dbmod.connect(path)
        con.execute("INSERT INTO cusip_ticker(cusip, ticker, name, mapped_via,"
                    " mapped_at_unix) VALUES ('00912X302','AL',NULL,'openfigi',0)")
        con.execute(
            "INSERT INTO thirteenf_holdings(cik, accession, period, filed_date, "
            "cusip, ticker, issuer, put_call, value, shares, ingested_at_unix) "
            "VALUES ('1','a','2026-06-30','2026-08-14','00912X302','AL',"
            "'AIR LEASE CORP','long',78460034,1,0)")
        con.commit()
        got = {"00912X302": (None, None, None, None, None, None, "openfigi_miss")}
        cache, holdings, flagged = repair_cusips.apply(con, got)
        assert cache == 0 and holdings == 0
        assert con.execute("SELECT ticker FROM cusip_ticker").fetchone()[0] == "AL"
        assert con.execute(
            "SELECT ticker FROM thirteenf_holdings").fetchone()[0] == "AL"
        assert flagged and flagged[0][0] == "00912X302"
        # and it is marked checked, so it is not re-queried forever
        assert con.execute(
            "SELECT mapped_via FROM cusip_ticker").fetchone()[0] == \
            "openfigi_checked_no_us"
    finally:
        os.remove(path)


def test_manual_override_pins_a_ticker_and_propagates():
    """OpenFIGI has no US record for Centessa Pharmaceuticals, so the automatic
    rule correctly withheld it and left a meaningless '260'. CNTA is the real
    Nasdaq symbol and the filer names the issuer outright."""
    path = _db()
    try:
        con = dbmod.connect(path)
        con.execute("INSERT INTO cusip_ticker(cusip, ticker, name, mapped_via,"
                    " mapped_at_unix) VALUES "
                    "('152309100','260',NULL,'openfigi_checked_no_us',0)")
        con.execute(
            "INSERT INTO thirteenf_holdings(cik, accession, period, filed_date, "
            "cusip, ticker, issuer, put_call, value, shares, ingested_at_unix) "
            "VALUES ('1','a','2026-06-30','2026-08-14','152309100','260',"
            "'Centessa Pharmaceuticals plc','long',106201725,1,0)")
        con.commit()
        n = repair_cusips.set_manual(con, "152309100", "CNTA", "Mando ruling")
        assert n == 1
        assert con.execute(
            "SELECT ticker FROM thirteenf_holdings").fetchone()[0] == "CNTA"
        assert con.execute(
            "SELECT ticker, mapped_via FROM cusip_ticker").fetchone() == (
                "CNTA", "manual")
    finally:
        os.remove(path)


def test_a_manual_pin_is_not_re_targeted_by_the_automatic_pass():
    """A human ruling must not be silently undone by the next repair run."""
    path = _db()
    try:
        con = dbmod.connect(path)
        con.execute("INSERT INTO cusip_ticker(cusip, ticker, name, mapped_via,"
                    " mapped_at_unix) VALUES ('152309100','CNTA',NULL,'manual',0)")
        con.commit()
        assert "152309100" not in repair_cusips.targets(con)
    finally:
        os.remove(path)
