"""B4 acceptance tests for the entity registry (E10, R1, R3)."""
import sqlite3

import pytest

from capex_daemon import identity, storage, universe


@pytest.fixture()
def con(tmp_path):
    c = sqlite3.connect(str(tmp_path / "t.db"))
    c.executescript(storage.SCHEMA)
    return c


def snap(cik="0001144879", name="Applied Digital Corp.", tickers=("APLD",),
         sic="7374", fye="0531", former=()):
    return identity.Snapshot(cik, name, tickers, ("Nasdaq",), sic,
                             "Services", fye, "operating", former)


# --- the APLD artifact hazard --------------------------------------------

def test_formernames_entry_equal_to_current_name_is_not_a_rename():
    """APLD's formerNames carries its own current name with an end date and no
    8-K item 5.03. Diffing name against formerNames reports a rename that never
    happened; detection must compare across scans instead."""
    s = snap(former=("Applied Digital Corp.", "Applied Blockchain, Inc."))
    stored = {"name_current": "Applied Digital Corp.", "ticker_display": "APLD",
              "sic": "7374", "fiscal_year_end": "0531"}
    assert identity.diff(stored, s, 0) == []


def test_real_rename_across_scans_is_detected():
    """KEEL <- Bitfarms is a genuine discontinuity."""
    stored = {"name_current": "Bitfarms Ltd", "ticker_display": "KEEL",
              "sic": "6199", "fiscal_year_end": "1231"}
    s = snap(cik="0001812477", name="Keel Infrastructure Corp.", tickers=("KEEL",),
             sic="6199", fye="1231")
    events = identity.diff(stored, s, 99)
    assert [e.field for e in events] == [identity.FIELD_NAME]
    assert events[0].old_value == "Bitfarms Ltd"
    assert events[0].new_value == "Keel Infrastructure Corp."


def test_legal_suffix_change_is_not_a_rename():
    stored = {"name_current": "Applied Digital Corporation", "ticker_display": "APLD",
              "sic": "7374", "fiscal_year_end": "0531"}
    assert identity.diff(stored, snap(), 0) == []


def test_first_sight_is_not_a_rename():
    assert identity.diff(None, snap(), 0) == []


@pytest.mark.parametrize("a,b", [
    ("Applied Digital Corp.", "APPLIED DIGITAL CORP"),
    ("Meta Platforms, Inc.", "Meta Platforms Inc"),
    ("IREN Ltd", "IREN"),
])
def test_normalize_name_collapses_cosmetic_difference(a, b):
    assert identity.normalize_name(a) == identity.normalize_name(b)


def test_normalize_name_keeps_real_difference():
    assert identity.normalize_name("Bitfarms Ltd") != identity.normalize_name("Keel Infrastructure Corp.")


# --- registry persistence -------------------------------------------------

def test_record_is_cik_keyed_and_writes_discontinuities(con):
    identity.record(con, snap(cik="0001812477", name="Bitfarms Ltd", tickers=("KEEL",)),
                    "builder", now_unix=1)
    events = identity.record(con, snap(cik="0001812477", name="Keel Infrastructure Corp.",
                                       tickers=("KEEL",)), "builder", now_unix=2)
    assert [e.field for e in events] == [identity.FIELD_NAME]
    rows = con.execute("SELECT field, old_value, new_value FROM identity_events").fetchall()
    assert rows == [("name", "Bitfarms Ltd", "Keel Infrastructure Corp.")]
    # One entity row, keyed on CIK, name updated in place.
    assert con.execute("SELECT COUNT(*) FROM entities").fetchone()[0] == 1
    assert con.execute("SELECT name_current FROM entities").fetchone()[0] == "Keel Infrastructure Corp."


def test_rescanning_unchanged_entity_writes_no_events(con):
    identity.record(con, snap(), "builder", now_unix=1)
    assert identity.record(con, snap(), "builder", now_unix=2) == []
    assert con.execute("SELECT COUNT(*) FROM identity_events").fetchone()[0] == 0


def test_from_submissions_reads_the_documented_fields():
    doc = {"cik": 1144879, "name": "Applied Digital Corp.", "tickers": ["APLD"],
           "exchanges": ["Nasdaq"], "sic": "7374", "sicDescription": "Services",
           "fiscalYearEnd": "0531", "entityType": "operating",
           "formerNames": [{"name": "Applied Blockchain, Inc."}]}
    s = identity.from_submissions(doc)
    assert s.cik == "0001144879"
    assert s.ticker_display == "APLD"
    assert s.former_names == ("Applied Blockchain, Inc.",)


# --- tier transitions (R1) ------------------------------------------------

def test_graduation_at_four_quarters_is_logged_not_ruled(con):
    con.execute("INSERT INTO coverage(cik, series_kind, status, tier) "
                "VALUES ('0001181412','capex','ok',?)", (universe.TIER_THIN,))
    ev = identity.record_tier(con, "0001181412", universe.TIER_UNRULED_BAND, 4,
                              "reached 4 consecutive quarters", now_unix=10)
    assert ev["direction"] == identity.DIRECTION_UP
    assert ev["old_tier"] == universe.TIER_THIN
    row = con.execute("SELECT direction, consecutive_quarters FROM tier_events").fetchone()
    assert row == ("graduation", 4)


def test_downgrade_is_logged_as_a_regression(con):
    con.execute("INSERT INTO coverage(cik, series_kind, status, tier) "
                "VALUES ('0000789019','capex','ok',?)", (universe.TIER_CORE,))
    ev = identity.record_tier(con, "0000789019", universe.TIER_THIN, 2,
                              "coverage regressed", now_unix=11)
    assert ev["direction"] == identity.DIRECTION_DOWN


def test_unchanged_tier_logs_nothing(con):
    con.execute("INSERT INTO coverage(cik, series_kind, status, tier) "
                "VALUES ('0000789019','capex','ok',?)", (universe.TIER_CORE,))
    assert identity.record_tier(con, "0000789019", universe.TIER_CORE, 72, "same") is None
    assert con.execute("SELECT COUNT(*) FROM tier_events").fetchone()[0] == 0
