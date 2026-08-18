"""D3 — scan idempotency and watermark preservation."""
import sqlite3

import pytest

from capex_daemon import identity, scan, storage, universe


@pytest.fixture()
def con(tmp_path):
    c = sqlite3.connect(str(tmp_path / "s.db"))
    c.executescript(storage.SCHEMA)
    return c


def subs(form="10-Q", filed="2026-08-04", period="2026-06-30", cik=789019,
         name="MICROSOFT CORPORATION", tickers=("MSFT",)):
    return {"cik": cik, "name": name, "tickers": list(tickers), "exchanges": ["Nasdaq"],
            "sic": "7372", "sicDescription": "Services", "fiscalYearEnd": "0630",
            "entityType": "operating", "formerNames": [],
            "filings": {"recent": {
                "form": [form], "filingDate": [filed], "reportDate": [period],
                "accessionNumber": ["acc-1"], "primaryDocument": ["d.htm"]}}}


def one_entity():
    return universe.Entity("789019", "MSFT", "hyperscaler", "", "")


# --- watermark discipline (E12) ------------------------------------------

def test_watermark_advances_only_forward(con):
    assert scan.write_watermark(con, "0000789019", "2026-08-04") is True
    assert scan.read_watermark(con, "0000789019") == "2026-08-04"
    # backwards is refused
    assert scan.write_watermark(con, "0000789019", "2026-05-01") is False
    assert scan.read_watermark(con, "0000789019") == "2026-08-04"
    # same value is not an advance
    assert scan.write_watermark(con, "0000789019", "2026-08-04") is False


def test_watermark_never_set_to_empty(con):
    assert scan.write_watermark(con, "0000789019", None) is False
    assert scan.write_watermark(con, "0000789019", "") is False
    assert scan.read_watermark(con, "0000789019") is None


# --- affected detection ---------------------------------------------------

def test_first_sight_is_a_new_filing(con):
    c = scan.check_issuer(con, one_entity(), submissions_doc=subs())
    assert c.is_affected
    assert c.newest_filing == "2026-08-04"
    assert c.watermark is None


def test_filing_older_than_watermark_is_current(con):
    scan.write_watermark(con, "0000789019", "2026-08-04")
    c = scan.check_issuer(con, one_entity(), submissions_doc=subs(filed="2026-05-01"))
    assert not c.is_affected
    assert c.status == "current"


def test_an_fpi_with_no_periodic_filing_is_not_an_error(con):
    c = scan.check_issuer(con, one_entity(), submissions_doc=subs(form="6-K"))
    assert c.status == "no-periodic"
    assert not c.is_affected


def test_rename_surfaces_in_the_scan_detail(con):
    scan.check_issuer(con, one_entity(), submissions_doc=subs(name="Bitfarms Ltd"))
    c = scan.check_issuer(con, one_entity(),
                          submissions_doc=subs(name="Keel Infrastructure Corp.",
                                               filed="2026-08-05"))
    assert "identity events" in c.detail


# --- the whole cycle ------------------------------------------------------

def test_zero_new_filings_is_a_noop_that_says_so(con):
    roster = {"0000789019": one_entity()}
    scan.write_watermark(con, "0000789019", "2026-08-04")
    r = scan.run(con=con, roster=roster, render=False,
                 submissions_by_cik={"0000789019": subs()})
    assert r["outcome"] == scan.OUTCOME_NOOP
    assert r["affected"] == 0
    assert r["artifacts_written"] is False
    assert "none with a new filing" in r["summary"]


def test_noop_run_preserves_the_watermark_exactly(con):
    roster = {"0000789019": one_entity()}
    scan.write_watermark(con, "0000789019", "2026-08-04")
    before = scan.read_watermark(con, "0000789019")
    scan.run(con=con, roster=roster, render=False,
             submissions_by_cik={"0000789019": subs()})
    assert scan.read_watermark(con, "0000789019") == before


def test_a_failed_refresh_does_not_advance_the_watermark(con):
    """The watermark is a claim that data was ingested. A refresh that raised
    ingested nothing, so moving it would silently skip that filing forever."""
    roster = {"0000789019": one_entity()}
    r = scan.run(con=con, roster=roster, render=False,
                 submissions_by_cik={"0000789019": subs()},
                 facts_by_cik={"0000789019": "not-a-document"})   # raises in index_facts
    assert scan.read_watermark(con, "0000789019") is None
    assert r["errors"]


def test_scan_is_idempotent_across_two_runs(con):
    """Second run the same night finds the same newest filing and does nothing."""
    roster = {"0000789019": one_entity()}
    facts = {"0000789019": {"facts": {"us-gaap": {}}}}
    first = scan.run(con=con, roster=roster, render=False,
                     submissions_by_cik={"0000789019": subs()}, facts_by_cik=facts)
    assert first["outcome"] == scan.OUTCOME_UPDATED
    assert first["watermarks_advanced"] == ["MSFT"]
    second = scan.run(con=con, roster=roster, render=False,
                      submissions_by_cik={"0000789019": subs()}, facts_by_cik=facts)
    assert second["outcome"] == scan.OUTCOME_NOOP
    assert second["affected"] == 0


def test_summary_line_is_loud_on_error_and_quiet_otherwise():
    quiet = scan.format_summary({"outcome": "no-op", "summary": "22 issuers checked", "errors": []})
    loud = scan.format_summary({"outcome": "updated", "summary": "1 of 22",
                                "errors": [("WULF", "fetch failed")]})
    assert "ERRORS" not in quiet
    assert "ERRORS" in loud and "WULF" in loud


# --- A5, display resolution ----------------------------------------------

@pytest.mark.parametrize("tickers,expected", [
    (["CLSK", "CLSKW"], "CLSK"),
    (["CLSKW", "CLSK"], "CLSK"),          # order-independent
    (["SLNH", "SLNHP"], "SLNH"),
    (["PLD", "PLDGP"], "PLD"),
    (["GPUS", "GPUS-PD"], "GPUS"),
    (["GDS", "GDHLF"], "GDS"),            # not a prefix pair; shortest wins
    (["MSFT"], "MSFT"),
    ([], None),
])
def test_common_share_ticker_is_preferred(tickers, expected):
    assert identity.preferred_display_ticker(tickers) == expected


def test_non_traded_filer_displays_cik_and_name():
    s = identity.Snapshot("0001868516", "StratCap Digital Infrastructure", [], [],
                          "6798", "REIT", "1231", "operating")
    assert s.ticker_display is None
    assert s.display_label == "CIK 0001868516 (StratCap Digital Infrastructure)"
