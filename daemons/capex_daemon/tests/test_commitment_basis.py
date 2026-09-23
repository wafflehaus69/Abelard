"""GAP2 P2 C1 — the class map, and what a class is allowed to reach.

The classes exist because verification found six published "forward
commitments" that are not commitments at all: CORZ's $3.1B is lease payments it
RECEIVES as a lessor, WULF's $90M is prepaid rent received, KEEL's $1.08B is
~97.5% note principal. Each of those flowed through the A4 deltas and the
Brief's since-page as a forward-demand move.

These tests pin the gate in both directions: a buildout class reaches Leg 3, the
since-page and the alert path; nothing else does, and the exclusion is visible
rather than silent.
"""
import pytest

from capex_daemon import commitment_basis as cb
from capex_daemon import dashboard, phases, report, snapshot


def _snap(points, ticker, concept="PurchaseObligation", bucket="builder"):
    obs = [{"quarter": p["q"], "state": phases.STATE_PLATEAU} for p in points]
    return {"issuers": {ticker: {
        "ticker": ticker, "bucket": bucket, "state": phases.STATE_PLATEAU,
        "observations": obs,
        "commitments": {
            "status": "COVERED", "concept": concept, "detail": "",
            "latest": points[-1]["value"],
            "points": [{"end": "2026-06-30", "value": p["value"]} for p in points],
            "points_cq": points,
            "basis": cb.basis_for(ticker).json(),
            "buildout_row": (cb.buildout_row(ticker).json()
                             if cb.buildout_row(ticker) else None)}}},
        "buckets": {}, "total": {"observations": obs}, "transitions": [],
        "panel": {}}


BIG = [{"q": "2026Q1", "value": 10.0e9}, {"q": "2026Q2", "value": 40.0e9}]


# --- the map itself ---------------------------------------------------------

def test_every_ratified_class_is_a_known_class():
    for b in cb.TAGGED_BASIS.values():
        assert b.cls in cb.CLASS_ORDER
    for b in cb.BUILDOUT_ROWS.values():
        assert b.cls in cb.BUILDOUT_CLASSES


def test_only_three_classes_are_buildout():
    """R1: only these feed Leg 3, the since-page and any alert."""
    assert cb.BUILDOUT_CLASSES == (cb.SUPPLY, cb.SUPPLY_CAPEX,
                                   cb.LEASES_NOT_COMMENCED)


def test_the_six_non_commitments_are_classified_as_such():
    """Verified one at a time in the filing, not inferred from the concept."""
    for t in ("CORZ", "WULF", "KEEL", "CCOI", "RIOT", "WYFI"):
        assert cb.class_of(t) == cb.NOT_A_COMMITMENT, t
        assert not cb.is_buildout(t)


def test_one_concept_spans_several_classes():
    """The finding in one line: the XBRL concept predicts nothing. Meta's supply
    total and a 2016 royalty from a predecessor business share a concept."""
    assert cb.class_of("META") == cb.SUPPLY_CAPEX
    assert cb.class_of("RIOT") == cb.NOT_A_COMMITMENT


def test_amzn_and_irm_carry_two_rows_because_the_tag_is_not_the_buildout():
    """R1(a). The class travels with the ROW, not with the issuer."""
    assert cb.class_of("AMZN") == cb.CONTENT_ENERGY_SOFTWARE
    assert cb.buildout_row("AMZN").cls == cb.LEASES_NOT_COMMENCED
    assert cb.class_of("IRM") == cb.CONTENT_ENERGY_SOFTWARE
    assert cb.buildout_row("IRM").cls == cb.SUPPLY_CAPEX


def test_spcx_is_mixed_unseparable_and_not_buildout():
    """R1(b): it mixes a spectrum-license acquisition into the same total."""
    assert cb.class_of("SPCX") == cb.MIXED_UNSEPARABLE
    assert not cb.is_buildout("SPCX")


def test_an_unknown_issuer_is_unclassified_and_not_buildout():
    """A new name does not enter the buildout read by existing."""
    assert cb.class_of("ZZZZ") == cb.UNCLASSIFIED
    assert not cb.is_buildout("ZZZZ")


def test_every_ratified_row_says_where_it_was_read():
    """E23: a class assigned without an accession is an opinion."""
    for b in cb.TAGGED_BASIS.values():
        assert b.accession and b.as_of and b.contains, b.ticker


# --- the gate ---------------------------------------------------------------

def test_a_not_a_commitment_row_is_absent_from_the_deltas():
    """Removed, not styled differently: a delta on prepaid rent received is a
    move in nothing."""
    assert snapshot.commitment_deltas(_snap(BIG, "WULF")) == []


def test_a_buildout_row_still_produces_a_delta_and_an_alert():
    snap = _snap(BIG, "MARA")
    d = snapshot.commitment_deltas(snap)
    assert len(d) == 1 and d[0]["basis_class"] == cb.SUPPLY_CAPEX
    assert d[0]["buildout"] is True
    assert len(snapshot.commitment_alert_lines(snap)) == 1


def test_an_annotative_row_publishes_a_delta_but_cannot_alert():
    """GOOGL's $7.7B is content licensing, confirmed at the tag. It is a real
    commitment and it is not the buildout."""
    snap = _snap(BIG, "GOOGL")
    d = snapshot.commitment_deltas(snap)
    assert len(d) == 1 and d[0]["buildout"] is False
    assert snapshot.commitment_alert_lines(snap) == []


def test_mixed_unseparable_cannot_alert():
    assert snapshot.commitment_alert_lines(_snap(BIG, "SPCX")) == []


def test_an_unclassified_move_is_published_as_work_not_as_silence():
    """The edge the class gate creates: a genuine jump at a name nobody has read
    cannot alert. It must not vanish — it becomes a basis check owed."""
    snap = _snap(BIG, "ZZZZ")
    assert snapshot.commitment_alert_lines(snap) == []
    owed = snapshot.commitment_basis_checks_owed(snap)
    assert len(owed) == 1 and owed[0]["ticker"] == "ZZZZ"
    assert owed[0]["basis_class"] == cb.UNCLASSIFIED


def test_a_ruled_non_buildout_class_is_not_a_check_owed():
    """GOOGL has been read. Its absence from the alerts is a verdict, not a
    backlog item, and re-queueing it every night would be noise."""
    assert snapshot.commitment_basis_checks_owed(_snap(BIG, "GOOGL")) == []


def test_the_since_page_carries_the_owed_section_and_reports_its_emptiness():
    keys = [k for k, _, _ in snapshot.SINCE_SECTIONS]
    assert "commitment_basis_owed" in keys
    since = snapshot.since_last_scan(_snap(BIG, "MARA"))
    assert since["commitment_basis_owed"] == []


# --- the renderers ----------------------------------------------------------

def test_leg_three_plots_buildout_classes_only():
    """A chart of "forward commitments" that plots lessor receipts beside supply
    commitments is a chart of two different things."""
    snap = _snap(BIG, "CORZ")
    assert "CORZ" not in dashboard._commitments_chart(snap, "t")
    assert "CORZ" not in str(report._commitments_series(snap, 10))
    assert "MARA" in str(report._commitments_series(_snap(BIG, "MARA"), 10))


def test_every_published_row_carries_its_class_and_its_source():
    rows = dashboard.commitment_rows(_snap(BIG, "CORZ"))
    assert len(rows) == 1
    assert rows[0]["class"] == cb.NOT_A_COMMITMENT
    assert rows[0]["accession"] == "0001839341-26-000014"
    assert "RECEIVE" in rows[0]["contains"]


def test_the_excluded_rows_are_listed_so_the_exclusion_is_visible():
    """Silence about an exclusion is indistinguishable from an oversight."""
    html = dashboard.view_commitments(_snap(BIG, "CORZ"))
    assert "NOT-A-COMMITMENT" in html and "CORZ" in html
    assert "excluded everywhere" in html


def test_a_second_row_renders_for_an_issuer_whose_tag_is_not_its_buildout():
    rows = dashboard.commitment_rows(_snap(BIG, "AMZN"))
    assert {r["kind"] for r in rows} == {"tagged", "buildout"}
    assert {r["class"] for r in rows} == {cb.CONTENT_ENERGY_SOFTWARE,
                                          cb.LEASES_NOT_COMMENCED}
