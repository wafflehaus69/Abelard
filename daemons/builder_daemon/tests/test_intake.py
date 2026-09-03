"""The input contract: admitted rows, code-PR shape, read-only on scout's ledger."""

from __future__ import annotations

import sqlite3

import pytest

from builder_daemon import intake
from builder_daemon.errors import IntakeError

_DDL = """
CREATE TABLE opportunities (
    opportunity_id TEXT PRIMARY KEY,
    source TEXT, title TEXT, url TEXT,
    payout_usd_low REAL, payout_basis TEXT,
    rank_segment TEXT, status TEXT, raw_json TEXT
);
"""


def make_ledger(tmp_path, rows):
    path = tmp_path / "scout.sqlite3"
    conn = sqlite3.connect(path)
    conn.executescript(_DDL)
    conn.executemany(
        "INSERT INTO opportunities VALUES(?,?,?,?,?,?,?,?,?)", rows
    )
    conn.commit()
    conn.close()
    return path


def row(oid, *, url="https://github.com/o/r/issues/1", basis="per_task",
        segment="GREEN", status="admitted", raw="{}", payout=100.0):
    return (oid, "opire", f"t-{oid[:6]}", url, payout, basis, segment, status, raw)


# ---------------------------------------------------------------------------
# Shape, not name
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("url,ok", [
    ("https://github.com/godotengine/godot/issues/70796", True),
    ("https://gitlab.com/o/r/-/issues/12", True),
    ("https://codeberg.org/o/r/issues/3", True),
    ("https://github.com/o/r/pull/5", False),          # a PR is not an issue
    ("https://github.com/o/r", False),                 # a repo is not an issue
    ("https://github.com/o/r/discussions/1", False),
    ("https://earn.superteam.fun/listing/x", False),   # not a forge
    ("https://example.com/o/r/issues/1", False),       # unknown host
    ("", False),
    (None, False),
])
def test_issue_shape_is_structural(url, ok) -> None:
    assert (intake.issue_shape(url) is not None) is ok


def test_shape_survives_the_absence_of_a_work_type_column() -> None:
    """Scout's `category` holds the repo's LANGUAGE for these rows -- 'Rust',
    'C++, C, GLSL'. Measured 2026-09-02: twelve distinct language strings across
    23 rows. Nothing in the input contract may depend on it."""
    import inspect
    src = inspect.getsource(intake)
    assert "category" not in src.split('"""')[-1], "intake must not read `category`"


def test_all_three_conditions_are_required() -> None:
    class R(dict):
        __getitem__ = dict.__getitem__

    base = {"url": "https://github.com/o/r/issues/1",
            "payout_basis": "per_task", "rank_segment": "GREEN"}
    assert intake.is_code_pr(R(base))
    assert not intake.is_code_pr(R({**base, "payout_basis": "pool"}))
    assert not intake.is_code_pr(R({**base, "rank_segment": "HUMAN_ONLY"}))
    assert not intake.is_code_pr(R({**base, "url": "https://github.com/o/r"}))


# ---------------------------------------------------------------------------
# Admitted only
# ---------------------------------------------------------------------------

def test_select_work_returns_only_admitted_code_pr_rows(tmp_path) -> None:
    path = make_ledger(tmp_path, [
        row("a" * 64, status="admitted"),
        row("b" * 64, status="proposed"),
        row("c" * 64, status="discovered"),
        row("d" * 64, status="dismissed"),
        row("e" * 64, status="admitted", segment="POOL"),
        row("f" * 64, status="admitted", url="https://earn.superteam.fun/x"),
    ])
    items = intake.select_work(intake.connect_scout(path))
    assert [i.opportunity_id for i in items] == ["a" * 64]


def test_an_empty_input_is_the_expected_state_not_an_error(tmp_path) -> None:
    """Zero admitted code rows is where the Tribe actually is today."""
    path = make_ledger(tmp_path, [row("a" * 64, status="proposed")])
    assert intake.select_work(intake.connect_scout(path)) == []


def test_the_ledger_cannot_be_written_through_this_handle(tmp_path) -> None:
    path = make_ledger(tmp_path, [row("a" * 64)])
    conn = intake.connect_scout(path)
    with pytest.raises(sqlite3.OperationalError, match="readonly"):
        conn.execute("UPDATE opportunities SET status='dismissed'")


# ---------------------------------------------------------------------------
# The rehearsal carve-out
# ---------------------------------------------------------------------------

def test_load_one_reaches_a_non_admitted_row_by_explicit_id(tmp_path) -> None:
    path = make_ledger(tmp_path, [row("a" * 64, status="proposed")])
    item = intake.load_one(intake.connect_scout(path), "a" * 64)
    assert item.status == "proposed"
    assert not item.is_admitted


def test_load_one_accepts_the_pasteable_short_id(tmp_path) -> None:
    path = make_ledger(tmp_path, [row("a" * 64, status="discovered")])
    item = intake.load_one(intake.connect_scout(path), "a" * 12)
    assert item.opportunity_id == "a" * 64


def test_load_one_refuses_an_empty_id(tmp_path) -> None:
    path = make_ledger(tmp_path, [row("a" * 64)])
    conn = intake.connect_scout(path)
    for bad in ("", "   ", None):
        with pytest.raises(IntakeError):
            intake.load_one(conn, bad)


def test_load_one_refuses_a_row_that_is_not_code_pr_shaped(tmp_path) -> None:
    """ZNS -- the only row actually admitted today -- is a content listing."""
    path = make_ledger(tmp_path, [
        row("5cbe29d07047" + "0" * 52, url="https://earn.superteam.fun/listing/zns"),
    ])
    with pytest.raises(IntakeError, match="not code-PR shaped"):
        intake.load_one(intake.connect_scout(path), "5cbe29d07047")


def test_load_one_reports_a_missing_row_rather_than_returning_none(tmp_path) -> None:
    path = make_ledger(tmp_path, [row("a" * 64)])
    with pytest.raises(IntakeError, match="no ledger row"):
        intake.load_one(intake.connect_scout(path), "f" * 12)


def test_a_missing_ledger_is_loud(tmp_path) -> None:
    with pytest.raises(IntakeError, match="not found"):
        intake.connect_scout(tmp_path / "nope.sqlite3")
