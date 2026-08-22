"""Order-invariance tests.

These encode the measurement that shaped the design: raw-byte change detection
reports 25.92% weekly churn against a true 1.95%, because the publisher shuffles
``<States>`` children. 92.5% of raw hits are that artifact.

If these tests fail, the pipeline is about to fire thousands of spurious
document fetches a week.
"""

from __future__ import annotations

from fdu_daemon.feed import FirmRecord
from fdu_daemon.normalize import (
    CHANGE_FIELDS,
    ENRICH_TRIGGER_FIELDS,
    canonical_form,
    change_key,
    diff_fields,
    should_enrich,
)


def _rec(**kw) -> FirmRecord:
    base = dict(crd="123", source_feed="sec", legal_name="EXAMPLE ADVISORS LLC",
                filing_date="2026-03-04", total_employees=8, aum_total=1_000_000)
    base.update(kw)
    return FirmRecord(**base)


def test_shuffled_states_are_not_a_change():
    """The whole point of the module."""
    a = _rec(notice_states=("GA", "HI", "MT", "NH"))
    b = _rec(notice_states=("NH", "GA", "MT", "HI"))
    assert change_key(a) == change_key(b)
    assert diff_fields(a, b) == {}


def test_duplicate_state_entries_collapse():
    a = _rec(notice_states=("GA", "HI"))
    b = _rec(notice_states=("HI", "GA", "GA"))
    assert change_key(a) == change_key(b)


def test_a_real_state_addition_is_a_change():
    a = _rec(notice_states=("GA", "HI"))
    b = _rec(notice_states=("GA", "HI", "NY"))
    assert change_key(a) != change_key(b)
    assert "notice_states" in diff_fields(a, b)


def test_filing_date_movement_is_a_change_and_triggers_enrich():
    a = _rec(filing_date="2026-03-04")
    b = _rec(filing_date="2026-08-19")
    moved = diff_fields(a, b)
    assert moved["filing_date"] == ("2026-03-04", "2026-08-19")
    assert should_enrich(moved)


def test_address_churn_alone_does_not_trigger_enrich():
    a = _rec(city="CHICAGO")
    b = _rec(city="EVANSTON")
    moved = diff_fields(a, b)
    assert "city" in moved
    assert not should_enrich(moved), "address churn must not cost a document fetch"


def test_none_is_distinct_from_zero():
    """A missing field must never compare equal to a reported zero.

    An ERA that does not complete Item 5A reports nothing; a firm that reports
    zero employees has said something. Collapsing them would make an absence
    look like a measured value.
    """
    a = _rec(total_employees=None)
    b = _rec(total_employees=0)
    assert change_key(a) != change_key(b)
    assert "total_employees" in diff_fields(a, b)


def test_canonical_form_is_stable_across_calls():
    r = _rec(notice_states=("MT", "GA"))
    assert canonical_form(r) == canonical_form(r)


def test_change_fields_all_exist_on_the_record():
    """A renamed field would otherwise silently stop being watched."""
    r = _rec()
    for name in CHANGE_FIELDS:
        assert hasattr(r, name), f"CHANGE_FIELDS names {name}, which FirmRecord does not have"


def test_enrich_triggers_are_a_subset_of_watched_fields():
    assert ENRICH_TRIGGER_FIELDS <= set(CHANGE_FIELDS)


def test_unrelated_records_differ():
    assert change_key(_rec(crd="1", legal_name="A LLC")) != change_key(_rec(crd="1", legal_name="B LLC"))
