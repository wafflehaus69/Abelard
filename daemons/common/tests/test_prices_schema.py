"""PS-1 Phase 1 — schema contract tests.

These are not "does the table exist" tests. Each one pins a rule the store
exists to enforce, and each maps to a defect CR-R0 found in the layer this
replaces:

  * insert-only prices          <- the vendor rewrote history and the cache kept
                                   the old vintage (92% of names, CR-R0 §R8.2)
  * INSERT OR REPLACE blocked   <- the old writer's upsert path
  * as-of append-only           <- membership and classification are facts
  * versioned factors immutable <- past statistics must stay reproducible
  * dual-class collision        <- A3: CIK alone would fail-loud every night
  * adjusted_view IS writable   <- the negative test; over-locking the derived
                                   table would break the normal rebuild path
"""

from __future__ import annotations

import sqlite3

import pytest

from abelard_common.prices import schema as S


# ---------------------------------------------------------------- fixtures --

@pytest.fixture()
def con(tmp_path):
    c = S.connect(tmp_path / "prices" / "prices.db")
    yield c
    c.close()


def _instrument(con, iid="0000320193.0", cik="0000320193", ticker="AAPL",
                class_code="0", class_source="single", provisional=0):
    con.execute(
        "INSERT INTO instruments (instrument_id, cik, class_code, class_source,"
        " name, primary_ticker, source, provisional, first_seen, last_seen)"
        " VALUES (?,?,?,?,?,?,?,?,?,?)",
        (iid, cik, class_code, class_source, "Apple Inc.", ticker, "sec",
         provisional, "2026-09-02", "2026-09-02"),
    )
    con.commit()
    return iid


def _price(con, iid, date="2026-09-01", close=100.0, status="ok"):
    con.execute(
        "INSERT INTO prices_raw (instrument_id, date, open, high, low, close,"
        " volume, status, source, fetched_at, run_asof)"
        " VALUES (?,?,?,?,?,?,?,?,?,?,?)",
        (iid, date, close, close, close, close, 1_000_000, status,
         "yahoo_v8", 1788000000, 1788000000),
    )
    con.commit()


# ------------------------------------------------------------- versioning --

def test_migrate_stamps_version_and_is_idempotent(tmp_path):
    p = tmp_path / "prices.db"
    c = S.connect(p)
    assert S.schema_version(c) == S.SCHEMA_VERSION
    # Re-opening an existing store must be a no-op, not an error.
    assert S.migrate(c) == S.SCHEMA_VERSION
    c.close()
    c2 = S.connect(p)
    assert S.schema_version(c2) == S.SCHEMA_VERSION
    c2.close()


def test_schema_version_is_none_before_creation(tmp_path):
    raw = sqlite3.connect(tmp_path / "empty.db")
    assert S.schema_version(raw) is None
    raw.close()


def test_migrate_refuses_a_newer_store(con):
    """An old binary must not write into a shape it does not understand."""
    con.execute("UPDATE price_meta SET value='99' WHERE key='schema_version'")
    con.commit()
    with pytest.raises(S.PriceStoreError) as e:
        S.migrate(con)
    assert "refusing to run against a newer shape" in str(e.value)
    assert e.value.stage == "migrate"
    assert e.value.to_error().startswith("migrate: ")


def test_connect_creates_missing_parent_directory(tmp_path):
    c = S.connect(tmp_path / "deep" / "nested" / "prices.db")
    assert S.schema_version(c) == S.SCHEMA_VERSION
    c.close()


# ----------------------------------------------------- prices_raw: the fact --

def test_prices_raw_unique_instrument_date(con):
    iid = _instrument(con)
    _price(con, iid, "2026-09-01", 100.0)
    with pytest.raises(sqlite3.IntegrityError):
        _price(con, iid, "2026-09-01", 101.0)


def test_prices_raw_update_is_blocked(con):
    iid = _instrument(con)
    _price(con, iid, "2026-09-01", 100.0)
    with pytest.raises(sqlite3.IntegrityError) as e:
        con.execute("UPDATE prices_raw SET close=999 WHERE instrument_id=?", (iid,))
    assert "insert-only" in str(e.value)


def test_prices_raw_delete_is_blocked(con):
    iid = _instrument(con)
    _price(con, iid, "2026-09-01", 100.0)
    with pytest.raises(sqlite3.IntegrityError):
        con.execute("DELETE FROM prices_raw WHERE instrument_id=?", (iid,))


def test_prices_raw_insert_or_replace_is_blocked(con):
    """The one that matters: SQLite implements REPLACE as DELETE + INSERT, so
    without the delete trigger an upsert would bypass the update trigger and
    silently restate a fact."""
    iid = _instrument(con)
    _price(con, iid, "2026-09-01", 100.0)
    with pytest.raises(sqlite3.IntegrityError):
        con.execute(
            "INSERT OR REPLACE INTO prices_raw (instrument_id, date, open, high,"
            " low, close, volume, status, source, fetched_at, run_asof)"
            " VALUES (?,?,?,?,?,?,?,?,?,?,?)",
            (iid, "2026-09-01", 1, 1, 1, 999.0, 1, "ok", "yahoo_v8", 1, 1),
        )
    assert con.execute(
        "SELECT close FROM prices_raw WHERE instrument_id=? AND date='2026-09-01'",
        (iid,)).fetchone()[0] == 100.0


def test_replace_is_blocked_on_a_bare_connection_too(tmp_path):
    """The guarantee must not depend on how the file was opened. SQLite fires
    delete triggers during REPLACE only when recursive_triggers is ON -- a
    per-connection pragma -- so a caller using a bare sqlite3.connect() would
    otherwise restate a fact silently. The BEFORE INSERT trigger closes that."""
    path = tmp_path / "prices.db"
    c = S.connect(path)
    iid = _instrument(c)
    _price(c, iid, "2026-09-01", 100.0)
    c.close()

    bare = sqlite3.connect(path)          # no pragmas, no migrate()
    with pytest.raises(sqlite3.IntegrityError) as e:
        bare.execute(
            "INSERT OR REPLACE INTO prices_raw (instrument_id, date, close,"
            " status, source, fetched_at, run_asof) VALUES (?,?,?,?,?,?,?)",
            (iid, "2026-09-01", 999.0, "ok", "yahoo_v8", 1, 1),
        )
    assert "insert-only" in str(e.value)
    assert bare.execute(
        "SELECT close FROM prices_raw WHERE date='2026-09-01'").fetchone()[0] == 100.0
    bare.close()


def test_prices_raw_rejects_unknown_instrument(con):
    """foreign_keys=ON: an orphan price row fails at insert rather than becoming
    a silent gap nobody attributes to anything."""
    with pytest.raises(sqlite3.IntegrityError):
        _price(con, "9999999999.0", "2026-09-01", 100.0)


def test_prices_raw_status_is_constrained(con):
    iid = _instrument(con)
    for ok in ("ok", "vendor_null", "quarantined"):
        _price(con, iid, "2026-09-0" + str(1 + ("ok", "vendor_null",
                                                "quarantined").index(ok)), 1.0, ok)
    with pytest.raises(sqlite3.IntegrityError):
        _price(con, iid, "2026-09-09", 1.0, "fine-probably")


def test_vendor_null_row_is_recorded_not_dropped(con):
    """A5. A null close is a row with status='vendor_null', so a gap is visible
    rather than inferred from absence."""
    iid = _instrument(con)
    con.execute(
        "INSERT INTO prices_raw (instrument_id, date, close, status, source,"
        " fetched_at, run_asof) VALUES (?,?,?,?,?,?,?)",
        (iid, "2026-09-01", None, "vendor_null", "yahoo_v8", 1, 1),
    )
    con.commit()
    row = con.execute(
        "SELECT close, status FROM prices_raw WHERE instrument_id=?", (iid,)
    ).fetchone()
    assert row["close"] is None and row["status"] == "vendor_null"


# --------------------------------------------------------- as-of append-only --

def test_index_membership_is_append_only(con):
    iid = _instrument(con)
    con.execute("INSERT INTO index_membership VALUES (?,?,?,?,?)",
                (iid, "SPX", "2026-09-01", 1, "wikipedia"))
    con.commit()
    with pytest.raises(sqlite3.IntegrityError) as e:
        con.execute("UPDATE index_membership SET present=0")
    assert "append-only" in str(e.value)
    with pytest.raises(sqlite3.IntegrityError):
        con.execute("DELETE FROM index_membership")


def test_membership_exit_is_a_new_row_not_an_edit(con):
    iid = _instrument(con)
    con.executemany("INSERT INTO index_membership VALUES (?,?,?,?,?)", [
        (iid, "SPX", "2026-09-01", 1, "wikipedia"),
        (iid, "SPX", "2026-10-01", 0, "wikipedia"),
    ])
    con.commit()
    rows = con.execute(
        "SELECT as_of, present FROM index_membership ORDER BY as_of").fetchall()
    assert [(r["as_of"], r["present"]) for r in rows] == [
        ("2026-09-01", 1), ("2026-10-01", 0)]


def test_membership_index_code_is_constrained(con):
    iid = _instrument(con)
    with pytest.raises(sqlite3.IntegrityError):
        con.execute("INSERT INTO index_membership VALUES (?,?,?,?,?)",
                    (iid, "DJIA", "2026-09-01", 1, "wikipedia"))


def test_classification_is_append_only(con):
    iid = _instrument(con)
    con.execute("INSERT INTO classification VALUES (?,?,?,?,?,?)",
                (iid, "GICS", "Information Technology",
                 "Technology Hardware, Storage & Peripherals",
                 "2026-09-01", "wikipedia"))
    con.commit()
    with pytest.raises(sqlite3.IntegrityError):
        con.execute("UPDATE classification SET sector='Financials'")
    with pytest.raises(sqlite3.IntegrityError):
        con.execute("DELETE FROM classification")


def test_two_sources_may_disagree_and_both_are_kept(con):
    """A4: Wikipedia-vs-IVV disagreement is logged, not resolved here. Both rows
    coexist because source is part of the key."""
    iid = _instrument(con)
    con.executemany("INSERT INTO classification VALUES (?,?,?,?,?,?)", [
        (iid, "GICS", "Information Technology", "Tech Hardware", "2026-09-01",
         "wikipedia"),
        (iid, "GICS", "Consumer Discretionary", None, "2026-09-01", "ishares_ivv"),
    ])
    con.commit()
    assert con.execute("SELECT COUNT(*) FROM classification").fetchone()[0] == 2


def test_ndx_only_name_carries_no_gics_row(con):
    """A6: no ICB->GICS hand map. Absence is the correct representation."""
    iid = _instrument(con, iid="0001018724.0", cik="0001018724", ticker="AMZN")
    con.execute("INSERT INTO index_membership VALUES (?,?,?,?,?)",
                (iid, "NDX", "2026-09-01", 1, "wikipedia_ndx"))
    con.commit()
    assert con.execute(
        "SELECT COUNT(*) FROM classification WHERE instrument_id=?",
        (iid,)).fetchone()[0] == 0


# ------------------------------------------------------- corporate actions --

def test_corporate_actions_append_only(con):
    iid = _instrument(con)
    con.execute("INSERT INTO corporate_actions VALUES (?,?,?,?,?,?,?,?)",
                (iid, "2026-08-11", "split", 2.0, None, 1788000000,
                 "yahoo_v8_events", 1788000000))
    con.commit()
    with pytest.raises(sqlite3.IntegrityError):
        con.execute("UPDATE corporate_actions SET ratio=3.0")
    with pytest.raises(sqlite3.IntegrityError):
        con.execute("DELETE FROM corporate_actions")


def test_split_requires_a_ratio_and_dividend_an_amount(con):
    iid = _instrument(con)
    with pytest.raises(sqlite3.IntegrityError):
        con.execute("INSERT INTO corporate_actions VALUES (?,?,?,?,?,?,?,?)",
                    (iid, "2026-08-11", "split", None, 1.30, 1, "yahoo", 1))
    with pytest.raises(sqlite3.IntegrityError):
        con.execute("INSERT INTO corporate_actions VALUES (?,?,?,?,?,?,?,?)",
                    (iid, "2026-05-26", "dividend", 2.0, None, 1, "yahoo", 1))
    con.execute("INSERT INTO corporate_actions VALUES (?,?,?,?,?,?,?,?)",
                (iid, "2026-05-26", "dividend", None, 1.30, 1, "yahoo", 1))
    con.commit()


def test_a_nonpositive_split_ratio_is_rejected(con):
    iid = _instrument(con)
    with pytest.raises(sqlite3.IntegrityError):
        con.execute("INSERT INTO corporate_actions VALUES (?,?,?,?,?,?,?,?)",
                    (iid, "2026-08-11", "split", 0.0, None, 1, "yahoo", 1))


# ------------------------------------------------------ versioned factors --

def test_factor_versions_are_immutable_and_coexist(con):
    iid = _instrument(con)
    con.executemany("INSERT INTO adjustment_factors VALUES (?,?,?,?,?)", [
        (iid, "2026-08-10", 1.0, 1, 1788000000),
        (iid, "2026-08-10", 0.5, 2, 1788600000),
    ])
    con.commit()
    assert con.execute(
        "SELECT COUNT(*) FROM adjustment_factors WHERE date='2026-08-10'"
    ).fetchone()[0] == 2
    with pytest.raises(sqlite3.IntegrityError) as e:
        con.execute("UPDATE adjustment_factors SET factor=9 WHERE version=1")
    assert "immutable" in str(e.value)
    with pytest.raises(sqlite3.IntegrityError):
        con.execute("DELETE FROM adjustment_factors WHERE version=1")


def test_adjustment_events_are_append_only(con):
    iid = _instrument(con)
    con.execute("INSERT INTO adjustment_events VALUES (?,?,?,?,?,?,?)",
                (iid, "2026-07-23", 1.956, "vendor_corruption", 1788000000,
                 '{"prev": 47.83, "next": 93.56}', 1))
    con.commit()
    with pytest.raises(sqlite3.IntegrityError):
        con.execute("UPDATE adjustment_events SET implied_ratio=1.0")
    with pytest.raises(sqlite3.IntegrityError):
        con.execute("DELETE FROM adjustment_events")


def test_adjustment_event_kind_is_constrained(con):
    iid = _instrument(con)
    with pytest.raises(sqlite3.IntegrityError):
        con.execute("INSERT INTO adjustment_events VALUES (?,?,?,?,?,?,?)",
                    (iid, "2026-07-23", 2.0, "probably_a_split", 1, None, 1))


# ------------------------------------------------- derived tables stay open --

def test_adjusted_view_is_rebuildable(con):
    """The negative test. adjusted_view is DERIVED: locking it the way
    prices_raw is locked would break the normal re-version rebuild."""
    iid = _instrument(con)
    con.execute("INSERT INTO adjusted_view VALUES (?,?,?,?)",
                (iid, "2026-08-10", 97.07, 1))
    con.commit()
    con.execute("UPDATE adjusted_view SET adj_close=48.54, factor_version=2")
    con.execute("DELETE FROM adjusted_view WHERE instrument_id=?", (iid,))
    con.commit()
    assert con.execute("SELECT COUNT(*) FROM adjusted_view").fetchone()[0] == 0


def test_vendor_adjclose_lives_outside_prices_raw(con):
    """It is a vendor VIEW kept for comparison only. Separate table so it cannot
    be joined into a return calculation by accident."""
    cols = {r[1] for r in con.execute("PRAGMA table_info(prices_raw)")}
    assert "vendor_adjclose" not in cols and "adj_close" not in cols
    iid = _instrument(con)
    con.execute("INSERT INTO vendor_adjusted VALUES (?,?,?,?,?)",
                (iid, "2026-09-01", 99.5, "yahoo_v8", 1))
    con.commit()
    assert con.execute("SELECT vendor_adjclose FROM vendor_adjusted").fetchone()[0] == 99.5


# ------------------------------------------------------------------- A3 id --

def test_dual_class_securities_do_not_collide(con):
    """A3. Under CIK alone GOOG and GOOGL share 0001652044 and would collide on
    UNIQUE(instrument_id, date) -- raising a fact-change event every night."""
    a = _instrument(con, iid="0001652044.A", cik="0001652044", ticker="GOOGL",
                    class_code="A", class_source="wikipedia")
    c = _instrument(con, iid="0001652044.C", cik="0001652044", ticker="GOOG",
                    class_code="C", class_source="wikipedia")
    _price(con, a, "2026-09-01", 250.0)
    _price(con, c, "2026-09-01", 248.0)
    rows = con.execute(
        "SELECT instrument_id, close FROM prices_raw WHERE date='2026-09-01'"
        " ORDER BY instrument_id").fetchall()
    assert [(r["instrument_id"], r["close"]) for r in rows] == [
        ("0001652044.A", 250.0), ("0001652044.C", 248.0)]


def test_class_source_records_the_ordinal_fallback(con):
    """The Berkshire case: Wikipedia does not disambiguate BRK.B because BRK-A
    is not an index member. The fallback must be visibly a fallback."""
    iid = _instrument(con, iid="0001067983.1", cik="0001067983", ticker="BRK-B",
                      class_code="1", class_source="ordinal")
    assert con.execute(
        "SELECT class_source FROM instruments WHERE instrument_id=?", (iid,)
    ).fetchone()[0] == "ordinal"
    with pytest.raises(sqlite3.IntegrityError):
        _instrument(con, iid="X.9", cik="1", ticker="X", class_source="vibes")


def test_provisional_id_for_a_name_with_no_cik(con):
    """A3: the 23 RUT names with no CIK get NOCIK.<ticker> and provisional=1,
    never a silent drop."""
    iid = _instrument(con, iid="NOCIK.SKYT", cik=None, ticker="SKYT",
                      provisional=1)
    row = con.execute(
        "SELECT cik, provisional FROM instruments WHERE instrument_id=?", (iid,)
    ).fetchone()
    assert row["cik"] is None and row["provisional"] == 1


# ------------------------------------------------------- ops / reference --

def test_freshness_ledger_roundtrips(con):
    iid = _instrument(con)
    con.execute("INSERT INTO freshness VALUES (?,?,?,?,?)",
                (iid, "2026-09-01", 1788000000, "ok", 1788000000))
    con.commit()
    row = con.execute("SELECT * FROM freshness WHERE instrument_id=?", (iid,)).fetchone()
    assert row["last_date_held"] == "2026-09-01" and row["last_fetch_status"] == "ok"


def test_reference_series_carries_contract_and_roll_flag(con):
    """Nullable, so the schema serves either WTI ruling without a migration."""
    con.executemany("INSERT INTO reference_series VALUES (?,?,?,?,?,?,?,?)", [
        ("CL=F", "2026-08-20", 87.83, "Crude Oil Sep 26", 0, "ok", "yahoo_v8", 1),
        ("CL=F", "2026-08-21", 87.06, "Crude Oil Oct 26", 1, "ok", "yahoo_v8", 1),
        ("VIXCLS", "2026-08-31", 14.92, None, 0, "ok", "fred", 1),
    ])
    con.commit()
    assert con.execute(
        "SELECT date FROM reference_series WHERE roll_flag=1").fetchone()[0] == "2026-08-21"
    assert con.execute(
        "SELECT contract FROM reference_series WHERE series_id='VIXCLS'"
    ).fetchone()[0] is None


def test_same_series_from_two_sources_coexists(con):
    """Yahoo ^VIX and FRED VIXCLS agree exactly (P0.6); keeping both is how that
    stays checkable rather than assumed."""
    con.executemany("INSERT INTO reference_series VALUES (?,?,?,?,?,?,?,?)", [
        ("VIX", "2026-08-31", 14.92, None, 0, "ok", "yahoo_v8", 1),
        ("VIX", "2026-08-31", 14.92, None, 0, "ok", "fred", 1),
    ])
    con.commit()
    assert con.execute("SELECT COUNT(*) FROM reference_series").fetchone()[0] == 2


def test_run_telemetry_is_writable_before_the_run_commits(con):
    con.execute("INSERT INTO run_telemetry (run_asof, started_at) VALUES (?,?)",
                (1788000000, 1788000000))
    con.commit()
    con.execute("UPDATE run_telemetry SET finished_at=?, requests_made=534,"
                " status='ok' WHERE run_asof=?", (1788002000, 1788000000))
    con.commit()
    assert con.execute(
        "SELECT requests_made FROM run_telemetry").fetchone()[0] == 534


def test_every_expected_table_exists(con):
    names = {r[0] for r in con.execute(
        "SELECT name FROM sqlite_master WHERE type='table'")}
    assert {
        "instruments", "ticker_aliases", "index_membership", "classification",
        "prices_raw", "corporate_actions", "adjustment_events",
        "adjustment_factors", "adjusted_view", "vendor_adjusted", "freshness",
        "reference_series", "price_meta", "run_telemetry",
    } <= names
