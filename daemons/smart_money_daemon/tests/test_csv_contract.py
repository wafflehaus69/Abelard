"""Every CSV export declares a completeness contract.

Each DictWriter used extrasaction="ignore", which drops a row key absent from the
column list silently. Measured against production, that was losing `value_flag`
from /trades.csv — the parse-time Form 4 corruption tag — so a known-suspect value
exported looking clean, while the weaker derived `value_quality` DID export and is
easy to mistake for it. The row survived; the mark did not.

Six exports were affected: trades (value_flag, security_class, security_title),
breadth_yoy (6 fields), disagreements (n_managers, which is also its sort key),
oge (filed_date, report_type), portfolios (unmapped), insiders (asset_type).
"""
import pytest

from smart_money import dashboard as dash


def test_an_unlisted_field_raises_rather_than_vanishing():
    """THE regression. A field added to a query row must not be able to disappear
    from every export with no error anywhere."""
    with pytest.raises(ValueError) as e:
        dash._csv_bytes(["a"], [{"a": 1, "surprise": 2}])
    assert "surprise" in str(e.value)


def test_a_deliberately_omitted_field_is_allowed():
    out = dash._csv_bytes(["a"], [{"a": 1, "b": 2}], omit=["b"])
    assert "a" in out and "b" not in out.splitlines()[0]


def test_a_complete_row_writes_normally():
    out = dash._csv_bytes(["a", "b"], [{"a": 1, "b": 2}])
    lines = out.strip().splitlines()
    assert lines[0] == "a,b"
    assert lines[1] == "1,2"


def test_missing_keys_are_still_fine():
    """A row that lacks a column is blank, not an error — only EXTRA keys are a
    contract violation, because only those vanish silently."""
    out = dash._csv_bytes(["a", "b"], [{"a": 1}])
    assert out.strip().splitlines()[1] == "1,"


def test_value_flag_is_exported_from_trades():
    """The corruption tag leaves with the data. 217 rows in the live corpus carry
    one (price_vs_close, price_over_max, value_denominated)."""
    assert "value_flag" in dash._CSV_COLS
    # the weaker derived heuristic must not be mistaken for it
    assert "value_quality" in dash._CSV_COLS


def test_disagreements_exports_the_key_it_sorts_by():
    """The file was ordered by n_managers and did not emit it, so the ordering was
    driven by an invisible column."""
    assert "n_managers" in dash._DIS_CSV_COLS


def test_sentinel_export_carries_identity_and_units():
    """cusip and issuer are the durable identity; without them a mis-resolved
    ticker reads as an unidentifiable symbol. value_scale says what the number
    next to it means."""
    for col in ("shares", "cusip", "issuer", "value_scale", "shares_type"):
        assert col in dash._SENTINEL_CSV_COLS, col


def test_previously_dropped_fields_are_now_exported():
    for cols, want in (
        (dash._PORT_CSV_COLS, "unmapped"),
        (dash._INSIDER_CSV_COLS, "asset_type"),
        (dash._OGE_CSV_COLS, "filed_date"),
        (dash._OGE_CSV_COLS, "report_type"),
        (dash._YOY_CSV_COLS, "asset_types"),
        (dash._YOY_CSV_COLS, "confidence_why"),
        (dash._CSV_COLS, "security_title"),
    ):
        assert want in cols, want


def test_no_column_list_has_duplicates():
    """A duplicate name makes DictWriter emit two identically-headed columns, which
    is how a spreadsheet ends up with an 'action.1'."""
    for name in dir(dash):
        if not name.endswith("_CSV_COLS"):
            continue
        cols = getattr(dash, name)
        if not isinstance(cols, list):
            continue
        assert len(cols) == len(set(cols)), (name, cols)


def test_every_column_name_is_non_empty():
    for name in dir(dash):
        if not name.endswith("_CSV_COLS"):
            continue
        cols = getattr(dash, name)
        if not isinstance(cols, list):
            continue
        assert all(isinstance(c, str) and c.strip() for c in cols), name
