"""Diff-engine tests: vocabulary, gap handling, and schema-drift safety."""

from __future__ import annotations

from fdu_daemon import lineage, transitions
from fdu_daemon.archive import ArchiveFile, _is_exempt, _parse_date, coverage_gaps
from fdu_daemon.transitions import EVENT_TYPES, diff_snapshots, summarize


class _Snap:
    def __init__(self, date, rows, absent=(), source="f.zip"):
        self.snapshot_date = date
        self.rows = rows
        self.absent_fields = tuple(absent)
        self.source_file = source


def _row(crd, name="A LLC", status="Approved", emp=10, aum=1_000_000):
    return {"crd": crd, "legal_name": name, "sec_status": status,
            "total_employees": emp, "aum_total": aum}


# -- vocabulary ----------------------------------------------------------


def test_every_emitted_type_is_in_the_closed_vocabulary():
    a = _Snap("2022-01-01", {"1": _row("1"), "2": _row("2")})
    b = _Snap("2022-02-01", {"2": _row("2", name="B LLC"), "3": _row("3")})
    for e in diff_snapshots(a, b):
        assert e.event_type in EVENT_TYPES, f"unknown event type {e.event_type}"


def test_disappearance_and_appearance():
    a = _Snap("2022-01-01", {"1": _row("1")})
    b = _Snap("2022-02-01", {"2": _row("2")})
    types = {e.event_type for e in diff_snapshots(a, b)}
    assert types == {"disappearance", "appearance"}


def test_rename_detected_on_persisting_crd():
    a = _Snap("2022-01-01", {"1": _row("1", name="JANUS CAPITAL MANAGEMENT LLC")})
    b = _Snap("2022-02-01", {"1": _row("1", name="JANUS HENDERSON INVESTORS US LLC")})
    evs = [e for e in diff_snapshots(a, b) if e.event_type == "rename"]
    assert len(evs) == 1
    assert evs[0].old_value.startswith("JANUS CAPITAL")
    assert evs[0].new_value.startswith("JANUS HENDERSON")


def test_no_event_when_nothing_moved():
    a = _Snap("2022-01-01", {"1": _row("1")})
    b = _Snap("2022-02-01", {"1": _row("1")})
    assert diff_snapshots(a, b) == []


# -- I-13: nothing here claims an acquisition ---------------------------


def test_no_event_type_asserts_an_acquisition():
    """The unit has zero verified acquisitions. The vocabulary must not imply any."""
    for t in EVENT_TYPES:
        assert "acqui" not in t and "sale" not in t and "success" not in t, t


# -- gap handling: the rate-inflation trap ------------------------------


def test_interval_recorded_and_gap_flagged():
    """Diffing across a coverage hole must not read as a one-month move.

    The archive is missing 49 of 243 months. Counting a 13-month interval as one
    step inflates every rate computed from it.
    """
    a = _Snap("2023-01-01", {"1": _row("1")})
    b = _Snap("2024-02-01", {})
    e = diff_snapshots(a, b)[0]
    assert e.interval_months == 13
    assert e.spans_gap is True


def test_adjacent_months_are_not_flagged():
    a = _Snap("2022-01-01", {"1": _row("1")})
    b = _Snap("2022-02-01", {})
    e = diff_snapshots(a, b)[0]
    assert e.interval_months == 1
    assert e.spans_gap is False


def test_summary_counts_gap_spanning_separately():
    a = _Snap("2020-01-01", {"1": _row("1"), "2": _row("2")})
    b = _Snap("2021-06-01", {"1": _row("1")})
    s = summarize(diff_snapshots(a, b))
    assert s["total"] == s["gap_spanning"], "all events from a gap-spanning pair are flagged"


# -- schema drift: an absent column is not a change ---------------------


def test_field_absent_in_one_era_is_never_a_transition():
    """2012-era files have no headcount column; 2016-era files do.

    Comparing across that boundary must not manufacture a headcount event out of
    a schema difference.
    """
    a = _Snap("2012-01-01", {"1": _row("1", emp=None)}, absent=("total_employees",))
    b = _Snap("2016-01-01", {"1": _row("1", emp=42)})
    assert not [e for e in diff_snapshots(a, b) if e.event_type == "headcount_delta"]


def test_none_to_value_is_not_a_change_within_an_era():
    a = _Snap("2022-01-01", {"1": _row("1", aum=None)})
    b = _Snap("2022-02-01", {"1": _row("1", aum=5)})
    assert not [e for e in diff_snapshots(a, b) if e.event_type == "aum_delta"]


def test_real_value_change_is_a_change():
    a = _Snap("2022-01-01", {"1": _row("1", aum=100)})
    b = _Snap("2022-02-01", {"1": _row("1", aum=50)})
    evs = [e for e in diff_snapshots(a, b) if e.event_type == "aum_delta"]
    assert len(evs) == 1 and evs[0].old_value == "100" and evs[0].new_value == "50"


# -- archive filename handling (I-9 / I-10) -----------------------------


def test_date_parses_every_observed_naming_variant():
    assert _parse_date("ia060506.zip") == (2006, 6, 5)
    assert _parse_date("ia08032026_0.zip") == (2026, 8, 3)
    assert _parse_date("010118-exempt.zip") == (2018, 1, 1)
    assert _parse_date("ia020119-2-exempt.zip") == (2019, 2, 1)
    assert _parse_date("ia020226-exemptzip.zip") == (2026, 2, 2)
    assert _parse_date("ia051023exempt.zip") is None      # the one that will not parse


def test_population_classified_on_filename():
    assert _is_exempt("ia08032026-exempt_0.zip") is True
    assert _is_exempt("ia08032026_0.zip") is False


def test_coverage_gaps_enumerated():
    files = [
        ArchiveFile("/p/ia010122.zip", "ia010122.zip", "f", 2022, 1, 1, False),
        ArchiveFile("/p/ia010422.zip", "ia010422.zip", "f", 2022, 4, 1, False),
    ]
    assert coverage_gaps(files) == ["2022-02", "2022-03"]


# -- lineage resolution --------------------------------------------------


def test_resolver_finds_fields_across_era_headers():
    old = ["SEC Region Name", "Organization CRD #", "SEC #", "Legal Name"]
    new = ["SEC Region", "Organization CRD#", "SEC#", "Legal Name", "5A", "5F(2)(c)", "Acquired Firm"]
    r_old, r_new = lineage.resolve(old), lineage.resolve(new)
    assert "crd" in r_old.index and "crd" in r_new.index
    assert "total_employees" in r_new.index and "total_employees" not in r_old.index
    assert lineage.era_label(r_old) == "roster-only"
    assert lineage.era_label(r_new) == "2026-wide"


def test_5a_pattern_does_not_match_wider_columns():
    """Unanchored '5A' also matches '5A(1)' in wider eras."""
    r = lineage.resolve(["Organization CRD#", "5A(1)", "5A"])
    assert r.index["total_employees"] == 2


def test_normalize_int_never_invents_a_zero():
    assert lineage.normalize_int(None) is None
    assert lineage.normalize_int("") is None
    assert lineage.normalize_int("n/a") is None
    assert lineage.normalize_int("8.0") == 8
    assert lineage.normalize_int("1,234") == 1234
    assert lineage.normalize_int("0") == 0


# -- L2 worksheet: firm-level only, and no machine-suggested label -------


def test_worksheet_offers_no_machine_label():
    """A suggested label is the thing a human then agrees with.

    L2's entire value is an independent judgement, so the worksheet must carry
    observed facts and lookup URLs and nothing resembling a guess.

    Checked against the emitted COLUMNS, not the source text -- a first cut
    grepped the module and failed on its own docstring explaining that it does
    not score anything.
    """
    import csv
    import tempfile
    from pathlib import Path

    from fdu_daemon.adjudicate import Candidate, write_worksheet

    c = Candidate(crd="1", last_name_seen="A LLC", snapshot_from="2020-01-01",
                  snapshot_to="2020-02-01", interval_months=1, spans_gap=False,
                  last_aum=1, last_employees=1, era="x")
    with tempfile.TemporaryDirectory() as d:
        p = write_worksheet([c], Path(d) / "w.csv")
        header = next(csv.reader(p.open(encoding="utf-8")))
    for banned in ("likely", "predicted", "score", "probability", "confidence", "rank"):
        offenders = [h for h in header if banned in h.lower()]
        assert not offenders, f"worksheet column suggests a label: {offenders}"
    # the decision columns must be present and empty for the human to fill
    for needed in ("OUTCOME", "SUCCESSOR_FIRM", "EVIDENCE_URL", "ADJUDICATOR"):
        assert needed in header


def test_outcome_vocabulary_includes_the_boring_answers():
    """A worksheet offering only interesting outcomes manufactures them."""
    from fdu_daemon.adjudicate import OUTCOMES

    for needed in ("wound_down", "moved_to_state", "reorganized_same_owner", "undetermined"):
        assert needed in OUTCOMES


def test_worksheet_has_no_person_columns():
    """I-11: person-level artifacts stay forbidden until the approval file lands."""
    import csv, io, re

    from fdu_daemon.adjudicate import Candidate, write_worksheet
    from pathlib import Path
    import tempfile

    c = Candidate(crd="1", last_name_seen="A LLC", snapshot_from="2020-01-01",
                  snapshot_to="2020-02-01", interval_months=1, spans_gap=False,
                  last_aum=1, last_employees=1, era="x")
    with tempfile.TemporaryDirectory() as d:
        p = write_worksheet([c], Path(d) / "w.csv")
        header = next(csv.reader(p.open(encoding="utf-8")))
    person = re.compile(r"first_?name|last_?name$|email|phone|principal|owner|contact|advisor_name", re.I)
    bad = [h for h in header if person.search(h)]
    assert not bad, f"worksheet exposes person-shaped columns: {bad}"


def test_gap_spanning_disappearances_excluded_from_sample():
    from fdu_daemon.adjudicate import Candidate, stratified_sample

    good = [Candidate(crd=str(i), last_name_seen="A", snapshot_from="2020-01-01",
                      snapshot_to="2020-02-01", interval_months=1, spans_gap=False,
                      last_aum=None, last_employees=None, era="e", aum_band="unknown")
            for i in range(10)]
    bad = [Candidate(crd="x", last_name_seen="B", snapshot_from="2023-01-01",
                     snapshot_to="2024-02-01", interval_months=13, spans_gap=True,
                     last_aum=None, last_employees=None, era="e", aum_band="unknown")]
    picked = stratified_sample(good + bad, n=20)
    assert all(not c.spans_gap for c in picked)


def test_sample_is_deterministic():
    from fdu_daemon.adjudicate import Candidate, stratified_sample

    cands = [Candidate(crd=str(i), last_name_seen="A", snapshot_from="2020-01-01",
                       snapshot_to="2020-02-01", interval_months=1, spans_gap=False,
                       last_aum=i * 1_000_000, last_employees=None, era="e")
             for i in range(60)]
    for c in cands:
        from fdu_daemon.adjudicate import _band
        c.aum_band = _band(c.last_aum)
    a = [c.crd for c in stratified_sample(cands, n=20)]
    b = [c.crd for c in stratified_sample(cands, n=20)]
    assert a == b, "an adjudicator must be able to be handed the same 50 twice"
