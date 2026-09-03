"""CD-BRIEF1 B1-B3 — from state dump to daily read.

The daemon published everything it knew, every night, with no way to see what
was NEW without diffing two renders by hand. That is how SMCI's commitments
tripled unremarked and how DLR sat stranded for a month.
"""
import pytest

from capex_daemon import phases, report, snapshot

from .test_charts import _fake_snapshot


def _flat_text(flowables):
    """All text in a story, including inside Tables.

    `_warn` wraps its Paragraph in a Table for the tinted border, so a flat
    `getattr(f, "text", "")` sweep misses exactly the flowables that carry the
    refusals and warnings — which are the ones worth asserting on.
    """
    out = []
    stack = list(flowables)
    while stack:
        f = stack.pop()
        t = getattr(f, "text", None)
        if isinstance(t, str):
            out.append(t)
        for attr in ("_cellvalues", "_content"):
            rows = getattr(f, attr, None)
            if rows:
                for row in rows:
                    stack.extend(row if isinstance(row, (list, tuple)) else [row])
    return " ".join(out)


# --- B1: since last scan ---------------------------------------------------

def test_every_section_is_named_even_when_empty():
    """Silence must be explicit. A section that vanishes when empty is
    indistinguishable from one that failed to run."""
    since = snapshot.since_last_scan(_fake_snapshot(), prior_keys=set())
    for key, _title, _empty in snapshot.SINCE_SECTIONS:
        assert key in since, key


def test_every_section_has_an_emptiness_sentence():
    for _key, title, empty in snapshot.SINCE_SECTIONS:
        assert title and empty
        assert not empty.endswith(".")        # rendered inside a sentence


def test_the_block_is_assembled_by_the_scan_not_a_renderer():
    """'New' is a fact about the RUN. A renderer reading only the snapshot
    cannot know it, so the scan writes it and both renderers read it."""
    import inspect
    src = inspect.getsource(snapshot.since_last_scan)
    assert "prior_keys" in inspect.signature(snapshot.since_last_scan).parameters
    assert "scan_unix" in src


def test_a_snapshot_without_the_record_says_so_rather_than_rendering_empty():
    """An old snapshot has no record; that is different from a quiet night."""
    snap = _fake_snapshot()
    assert snapshot.SINCE_KEY not in snap
    body = report.sec_since(snap, report._styles())
    assert "predates" in _flat_text(body)


def test_composition_events_are_frontier_gated():
    """A membership change from 2013 is not news tonight."""
    snap = _fake_snapshot()
    snap["buckets"]["hyperscaler"]["composition_events"] = [
        {"quarter": "2013Q2", "ticker": "OLD", "change": "entered"},
        {"quarter": "2026Q2", "ticker": "NEW", "change": "entered"}]
    snap["total"]["observations"] = [
        {"quarter": q, "state": phases.STATE_PLATEAU} for q in ("2026Q1", "2026Q2")]
    ev = snapshot.since_last_scan(snap, prior_keys=set())["composition_events"]
    assert [e["ticker"] for e in ev] == ["NEW"]


# --- B2: the thesis line ---------------------------------------------------

def test_the_thesis_line_is_one_paragraph_with_a_fixed_shape():
    line = snapshot.thesis_line(_fake_snapshot())
    assert "\n" not in line
    for clause in ("Panel capex TTM", "credit issuance is",
                   "forward commitments are", "Hyperscalers:"):
        assert clause in line, clause


def test_the_thesis_line_carries_no_adjectives_of_judgement():
    """The value of a fixed sentence is that a changed clause is visible without
    reading. A word chosen for emphasis destroys that."""
    line = snapshot.thesis_line(_fake_snapshot()).lower()
    for word in ("strong", "sharp", "dramatic", "concerning", "healthy", "weak",
                 "surge", "collapse", "worrying", "impressive"):
        assert word not in line, word


def test_no_band_is_registered_and_the_clause_does_not_pretend_one_is():
    """The first cut of this shipped CROSSCHECK_BAND = (0.44, 0.48) with a
    comment claiming it came from CD-3b. CD-3b measured the dcrev:supplier
    DEAD-BAND (9pp) — a band on quarter-to-quarter moves in the ladder, not a
    registered range for the ratio's LEVEL. Two invented numbers were about to
    be labelled "pre-registered" on the front page of a daily read."""
    assert snapshot.CROSSCHECK_BAND is None
    snap = _fake_snapshot()
    snap["suppliers"]["crosscheck"]["latest_ratio"] = 0.538
    line = snapshot.thesis_line(snap)
    assert "53.8%" in line
    assert "against no pre-registered band" in line
    assert "above" not in line and "inside" not in line


def test_the_clause_shows_change_by_eye_without_a_band():
    """A band is not the only way to make a move visible; the prior quarter is."""
    snap = _fake_snapshot()
    snap["suppliers"]["crosscheck"]["latest_ratio"] = 0.538
    snap["suppliers"]["crosscheck"]["series"] = [
        {"q": "2026Q1", "ratio": 0.507}, {"q": "2026Q2", "ratio": 0.538}]
    assert "from 50.7% a quarter earlier" in snapshot.thesis_line(snap)


def test_a_band_once_registered_is_stated_as_a_position():
    """The machinery stays, so registering a range later needs no new code —
    which is what "pre-registered" has to mean to be worth anything."""
    snap = _fake_snapshot()
    snap["suppliers"]["crosscheck"]["latest_ratio"] = 0.538
    assert "above its pre-registered" in snapshot.thesis_line(snap, band=(0.44, 0.48))
    snap["suppliers"]["crosscheck"]["latest_ratio"] = 0.46
    assert "inside its pre-registered" in snapshot.thesis_line(snap, band=(0.44, 0.48))
    snap["suppliers"]["crosscheck"]["latest_ratio"] = 0.20
    assert "below its pre-registered" in snapshot.thesis_line(snap, band=(0.44, 0.48))


def test_the_thesis_line_honours_the_b5_refusal():
    """B5 refused the cross-basis commitments total. A Brief that says
    "forward commitments are rising" one screen from its own refusal of that
    exact sum is worse than one that says nothing."""
    snap = _fake_snapshot()
    snap["panel"]["commitments"] = [{"q": "2026Q1", "value": 10.0e9, "members": 2},
                                    {"q": "2026Q2", "value": 90.0e9, "members": 3}]
    snap["panel"]["commitments_panel"] = {"status": "REFUSED-MIXED-BASIS",
                                          "detail": "three concepts"}
    line = snapshot.thesis_line(snap)
    assert "refused, so it has no direction" in line
    assert "forward commitments are rising" not in line


def test_a_missing_cross_check_says_so_rather_than_inventing_a_position():
    snap = _fake_snapshot()
    snap["suppliers"]["crosscheck"]["latest_ratio"] = None
    assert "no current reading" in snapshot.thesis_line(snap)


def test_the_same_snapshot_always_yields_the_same_sentence():
    snap = _fake_snapshot()
    assert snapshot.thesis_line(snap) == snapshot.thesis_line(snap)


# --- B3: the split ---------------------------------------------------------

def test_the_brief_is_three_sections():
    assert len(report.BRIEF_SECTIONS) == 3
    assert [n for n, _r, _f in report.BRIEF_SECTIONS] == [
        "Since the last scan", "The thesis line", "Phase board"]


def test_the_brief_leads_with_what_is_new():
    assert report.BRIEF_SECTIONS[0][2] is report.sec_since
    assert report.SECTIONS[0][2] is report.sec_since


def test_the_brief_and_the_reference_share_their_section_functions():
    """One snapshot, two renders. The Brief cannot say anything the Reference
    does not, because they call the same functions."""
    ref = {f for _n, _r, f in report.SECTIONS}
    for _n, _r, f in report.BRIEF_SECTIONS:
        assert f in ref or f is report.sec_phase_board


def test_the_brief_renders_and_is_shorter_than_the_reference(tmp_path):
    snap = _fake_snapshot()
    b = report.build(snap, tmp_path / "b.pdf", title="Brief",
                     sections=report.BRIEF_SECTIONS)
    r = report.build(snap, tmp_path / "r.pdf", title="Reference")
    assert b.is_file() and r.is_file()
    assert b.read_bytes().count(b"/Type /Page\n") < r.read_bytes().count(b"/Type /Page\n")


def test_the_two_renders_have_distinct_default_filenames():
    """They are emitted side by side every night; one must not overwrite the
    other."""
    assert report.BRIEF_NAME != report.REFERENCE_NAME
