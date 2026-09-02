"""CD-GAP2A A1 — the ingest gap: a filing seen, its period never held.

Measured live 2026-09-02: DLR filed a 10-Q for 2026-06-30 on 2026-07-31 and
companyfacts still did not carry the period 33 days later. AMT was in the same
state, and RIOT a third. All three were rendered as "behind on filing", which is
a statement about the issuer, when the truth was a statement about us.

Two separate defects produced that, and both are pinned here:

  1. `freshness` — the module written to cover exactly this API lag — was called
     by nothing outside its own tests. `assess`, `needs_fallback` and
     `fetch_fallback_facts` had never run in production.
  2. The watermark advanced on a filing being SEEN rather than its period being
     INGESTED, so the one attempt that missed was also the last. The gate closed
     and no later scan ever retried.

The second is the one that turned a temporary lag into a permanent hole, and it
is why "behind on filing" must distinguish ISSUER-LATE from INGEST-GAP: absence
has two causes and only one of them is the issuer's doing.
"""
import sqlite3

import pytest

from capex_daemon import freshness, ixbrl, scan, storage, universe

from .test_scan import one_entity, subs


@pytest.fixture()
def con(tmp_path):
    c = sqlite3.connect(str(tmp_path / "s.db"))
    c.executescript(storage.SCHEMA)
    return c


def _filing(period="2026-06-30", filed="2026-07-31"):
    return freshness.Filing("10-Q", filed, period, "0001104659-26-089296",
                            "dlr-20260630x10q.htm")


def _fact(concept, start, end, value, dims=None, basis=ixbrl.SCALE_BASIS_DECLARED
          if hasattr(ixbrl, "SCALE_BASIS_DECLARED") else "declared"):
    f = ixbrl.Fact(taxonomy="us-gaap", concept=concept, value=value, unit="USD",
                   scale=None, scale_basis=basis, period_start=start,
                   period_end=end, dims=dims or {}, context_ref="c1")
    return f


# --- the calendar-quarter comparison the retry rests on --------------------

def test_period_end_maps_to_its_calendar_quarter():
    assert scan._cq_of("2026-06-30") == "2026Q2"
    assert scan._cq_of("2026-01-31") == "2026Q1"
    assert scan._cq_of("2025-12-31") == "2025Q4"
    assert scan._cq_of(None) is None
    assert scan._cq_of("garbage") is None


def test_quarters_compare_correctly_as_strings():
    """The retry gate compares them lexicographically; YYYYQN must sort right."""
    assert "2026Q1" < "2026Q2" < "2026Q3" < "2026Q4"
    assert "2025Q4" < "2026Q1"


# --- the gate that stranded DLR -------------------------------------------

def test_a_closed_watermark_reopens_when_the_panel_is_behind(con):
    """DLR exactly: watermark 2026-07-31, filing 2026-07-31, panel at 2026Q1.

    Before the fix this returned `current` forever and the 2026Q2 period was
    unreachable until the Q3 filing in late October.
    """
    scan.write_watermark(con, "0000789019", "2026-07-31")
    covered = {"0000789019": "2026Q1"}
    c = scan.check_issuer(con, one_entity(),
                          submissions_doc=subs(filed="2026-07-31", period="2026-06-30"),
                          covered=covered)
    assert c.is_affected
    assert "INGEST-GAP retry" in c.detail


def test_a_closed_watermark_stays_closed_when_the_panel_holds_the_period(con):
    """The retry must not fire on every issuer every night — only on real gaps."""
    scan.write_watermark(con, "0000789019", "2026-07-31")
    covered = {"0000789019": "2026Q2"}
    c = scan.check_issuer(con, one_entity(),
                          submissions_doc=subs(filed="2026-07-31", period="2026-06-30"),
                          covered=covered)
    assert not c.is_affected
    assert c.status == "current"


def test_an_empty_panel_does_not_look_like_thirty_five_ingest_gaps(con):
    """A database with no snapshot yet has covered={}; the watermark keeps its
    original meaning rather than declaring every issuer stranded."""
    scan.write_watermark(con, "0000789019", "2026-07-31")
    c = scan.check_issuer(con, one_entity(),
                          submissions_doc=subs(filed="2026-07-31", period="2026-06-30"),
                          covered={})
    assert not c.is_affected
    assert scan._covered_quarters(con, {"0000789019": one_entity()}) == {}


# --- the crossing from filing facts into the API's shape -------------------

def test_dimensioned_facts_never_cross_into_the_undimensioned_series():
    """An instance carries the total AND its segment breakdown under one concept.
    Merging both adds a company's parts to its whole."""
    total = _fact("PaymentsToDevelopRealEstateAssets", "2026-04-01", "2026-06-30", 900.0)
    segment = _fact("PaymentsToDevelopRealEstateAssets", "2026-04-01", "2026-06-30",
                    400.0, dims={"srt:StatementGeographicalAxis": "us-gaap:US"})
    out = freshness.to_api_facts([total, segment], _filing())
    assert len(out) == 1
    assert out[0].value == 900.0
    assert out[0].duration_days == 90


def test_instant_facts_do_not_cross():
    """A capex series is durations. A point-in-time fact has no period to fill."""
    instant = _fact("PaymentsToDevelopRealEstateAssets", None, "2026-06-30", 900.0)
    assert freshness.to_api_facts([instant], _filing()) == []


def test_crossed_facts_carry_the_filing_as_provenance():
    f = _fact("PaymentsToDevelopRealEstateAssets", "2026-04-01", "2026-06-30", 900.0)
    out = freshness.to_api_facts([f], _filing())[0]
    assert out.form == "10-Q" and out.filed == "2026-07-31"
    assert out.frame is None          # a filled fact was never in a frame


# --- filling holes, and only holes ----------------------------------------

class _Stub:
    """Stands in for the fetch so the test stays hermetic."""

    def __init__(self, facts):
        self.facts = facts
        self.calls = 0

    def __call__(self, cik, filing, http=None, prefer_instance=True):
        self.calls += 1
        return self.facts, freshness.PROVENANCE_INSTANCE, "instance"


def test_a_served_period_is_never_displaced_by_a_filled_one(monkeypatch):
    """The API is LATE here, not wrong. If it has the period, it wins."""
    from capex_daemon.facts_api import ApiFact
    served = ApiFact(concept="PaymentsToDevelopRealEstateAssets", taxonomy="us-gaap",
                     unit="USD", value=111.0, period_start="2026-04-01",
                     period_end="2026-06-30", duration_days=90, form="10-Q",
                     filed="2026-07-31", frame=None)
    indexed = {"PaymentsToDevelopRealEstateAssets": [served]}
    stub = _Stub([_fact("PaymentsToDevelopRealEstateAssets", "2026-04-01",
                        "2026-06-30", 999.0)])
    monkeypatch.setattr(freshness, "fetch_fallback_facts", stub)
    fill = freshness.fill_from_filing(
        "0000789019", subs(filed="2026-07-31", period="2026-06-30"), indexed,
        concept="PaymentsToDevelopRealEstateAssets")
    # api_latest already covers the filed period, so no fetch should even happen
    assert fill.status == freshness.FILL_NOT_NEEDED
    assert stub.calls == 0
    assert indexed["PaymentsToDevelopRealEstateAssets"][0].value == 111.0


def test_a_missing_period_is_filled_from_the_filing(monkeypatch):
    indexed = {}
    stub = _Stub([_fact("PaymentsToDevelopRealEstateAssets", "2026-04-01",
                        "2026-06-30", 900.0)])
    monkeypatch.setattr(freshness, "fetch_fallback_facts", stub)
    fill = freshness.fill_from_filing(
        "0000789019", subs(filed="2026-07-31", period="2026-06-30"), indexed,
        concept="PaymentsToDevelopRealEstateAssets")
    assert fill.filled and fill.added == 1
    assert fill.period == "2026-06-30"
    assert indexed["PaymentsToDevelopRealEstateAssets"][0].value == 900.0


def test_a_filing_carrying_nothing_tracked_is_reported_not_silently_ok(monkeypatch):
    """FILL_EMPTY is what keeps the watermark shut and the gap loud."""
    indexed = {}
    stub = _Stub([_fact("SomeConceptWeDoNotTrack", "2026-04-01", "2026-06-30", 5.0)])
    monkeypatch.setattr(freshness, "fetch_fallback_facts", stub)
    fill = freshness.fill_from_filing(
        "0000789019", subs(filed="2026-07-31", period="2026-06-30"), indexed,
        concept="PaymentsToDevelopRealEstateAssets")
    assert fill.status == freshness.FILL_EMPTY
    assert not fill.filled


def test_a_failed_fetch_is_a_reported_gap_not_an_exception(monkeypatch):
    def boom(cik, filing, http=None, prefer_instance=True):
        raise OSError("EDGAR timed out")
    monkeypatch.setattr(freshness, "fetch_fallback_facts", boom)
    fill = freshness.fill_from_filing(
        "0000789019", subs(filed="2026-07-31", period="2026-06-30"), {},
        concept="PaymentsToDevelopRealEstateAssets")
    assert fill.status == freshness.FILL_FAILED
    assert "EDGAR timed out" in fill.detail
