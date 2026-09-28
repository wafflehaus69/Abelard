"""Ruling R6 (Mando, 2026-09-28) — a series met for the first time is not news.

A newly captured series brings its whole history with it. NVIDIA's supply and
capacity arrives with $119B -> $279B already in it; Microsoft's leases arrive
with $196.6B -> $329.1B. None of that is the issuer doing something tonight — it
is the daemon learning to read something. So:

  * a move in a first-seen series never reaches the queue;
  * it is published ONCE, on the Brief, under a "Newly captured" header;
  * from the next scan on, the series is known and its next genuine move alerts.

Decided per SERIES, not per table. C4's first-run flag was table-wide, which is
right exactly once: every capture rule added afterwards would have met a
non-empty table and announced its history as tonight's news.
"""
import sqlite3

import pytest

from capex_daemon import commitment_basis as cb
from capex_daemon import commitment_capture as cc
from capex_daemon import commitments, report, snapshot, storage
from tests.test_brief import _flat_text


def _rule(ticker, key):
    return next(r for r in cc.rules_for(ticker) if r.key == key)


def _snap(captures=(), tagged=None):
    issuers = {}
    for ticker, key, points in captures:
        rows = [{"rule": _rule(ticker, key), "points": points}]
        iss = issuers.setdefault(ticker, {
            "ticker": ticker, "bucket": "supplier", "observations": [],
            "commitments": {"status": "COVERED", "concept": "x", "points_cq": [],
                            "points": [], "basis": cb.basis_for(ticker).json(),
                            "buildout_row": None, "captured": []}})
        iss["commitments"]["captured"] += snapshot._captured_rows(ticker, rows)
    for ticker, pts in (tagged or {}).items():
        issuers[ticker] = {"ticker": ticker, "bucket": "builder", "observations": [],
                           "commitments": {"status": "COVERED",
                                           "concept": "PurchaseObligation",
                                           "points_cq": pts}}
    return {"issuers": issuers, "buckets": {}, "transitions": [],
            "total": {"latest_quarter": "2026Q2", "observations": []}}


NVDA = ("NVDA", "supply-and-capacity", {"2026-04-26": 119e9, "2026-07-26": 279e9})
MARA = {"MARA": [{"q": "2026Q1", "value": 10e9}, {"q": "2026Q2", "value": 40e9}]}


@pytest.fixture()
def con():
    c = sqlite3.connect(":memory:")
    c.executescript(storage.SCHEMA)
    storage._migrate(c)
    return c


def test_the_series_key_is_the_rule_for_a_capture_and_the_ticker_otherwise():
    d = [x for x in snapshot.commitment_deltas(_snap([NVDA], MARA))]
    keys = {commitments.series_key(x) for x in d}
    assert keys == {"NVDA:supply-and-capacity", "MARA"}


def test_a_first_seen_series_is_withheld_from_the_alerts():
    snap = _snap([NVDA])
    alertable, _ = snapshot.commitment_alerts_and_quarantine(snap)
    assert len(alertable) == 1                        # it WOULD alert
    kept, first = snapshot.first_sight(alertable, seen_series=set())
    assert kept == [] and len(first) == 1


def test_the_since_record_carries_the_newly_captured_moves_not_the_alerts():
    snap = _snap([NVDA])
    since = snapshot.since_last_scan(snap, commitment_prior_keys=set(),
                                     commitment_seen_series=set())
    assert since["commitment_alerts"] == []
    assert [d["rule_key"] for d in since["commitment_newly_captured"]] == \
        ["NVDA:supply-and-capacity"]


def test_first_seen_tagged_series_are_counted_not_listed():
    """On the table's first run the tagged series are first-seen too. They have
    been on the dashboard for weeks; they are history, reported as a count."""
    since = snapshot.since_last_scan(_snap([NVDA], MARA), commitment_prior_keys=set(),
                                     commitment_seen_series=set())
    assert len(since["commitment_newly_captured"]) == 1
    assert since["commitment_backfilled_count"] == 1


def test_the_next_genuine_move_in_a_known_series_alerts(con):
    snap = _snap([NVDA])
    commitments.record_commitment_events(con, snapshot.commitment_deltas(snap))
    later = _snap([("NVDA", "supply-and-capacity",
                    {"2026-07-26": 279e9, "2026-10-25": 400e9})])
    prior = {r[0] for r in con.execute("SELECT event_key FROM commitment_events")}
    alertable, _ = snapshot.commitment_alerts_and_quarantine(later, prior_keys=prior)
    kept, first = snapshot.first_sight(alertable, commitments.seen_series(con))
    assert len(kept) == 1 and first == []
    assert kept[0]["to_q"] == "2026Q4"


def test_a_capture_added_later_does_not_announce_its_history(con):
    """The hole a table-wide first run leaves. MARA's tagged series is known;
    NVIDIA's capture arrives a week later into a NON-EMPTY table."""
    commitments.record_commitment_events(con, snapshot.commitment_deltas(_snap(tagged=MARA)))
    assert commitments.seen_series(con) == {"MARA"}
    snap = _snap([NVDA], MARA)
    prior = {r[0] for r in con.execute("SELECT event_key FROM commitment_events")}
    alertable, _ = snapshot.commitment_alerts_and_quarantine(snap, prior_keys=prior)
    kept, first = snapshot.first_sight(alertable, commitments.seen_series(con))
    assert kept == []
    assert [d["rule_key"] for d in first] == ["NVDA:supply-and-capacity"]


def test_an_old_table_gains_the_series_column_with_its_rows_keyed_by_ticker():
    c = sqlite3.connect(":memory:")
    c.execute("CREATE TABLE commitment_events(event_key TEXT PRIMARY KEY, "
              "ticker TEXT NOT NULL, from_q TEXT NOT NULL, to_q TEXT NOT NULL, "
              "from_value REAL, to_value REAL, delta REAL, multiple REAL, "
              "basis_class TEXT, concept TEXT, observed_unix INTEGER)")
    c.execute("INSERT INTO commitment_events(event_key, ticker, from_q, to_q) "
              "VALUES ('commit:SMCI:2026Q1:2026Q2:1', 'SMCI', '2026Q1', '2026Q2')")
    storage._migrate(c)
    storage._migrate(c)                                # idempotent
    assert commitments.seen_series(c) == {"SMCI"}


def test_the_brief_prints_the_header_and_the_moves():
    snap = _snap([NVDA], MARA)
    snap[snapshot.SINCE_KEY] = snapshot.since_last_scan(
        snap, commitment_prior_keys=set(), commitment_seen_series=set(),
        scan_unix=1790000000)
    text = _flat_text(report.sec_since(snap, report._styles()))
    assert "Newly captured" in text
    assert "NVDA · supply-and-capacity" in text
    assert "None was sent to the queue" in text
    assert "1 moves in tagged series were recorded as history" in text
