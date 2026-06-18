"""Window derivation — single-anchor invariant + purity."""

from __future__ import annotations

from chatter_daemon.windows import DAY_S, MONTH_S, WEEK_S, derive_windows, iso_z

FIXED = 1_718_733_600  # arbitrary fixed unix timestamp


def test_iso_z_format():
    s = iso_z(FIXED)
    assert s.endswith("Z")
    assert "T" in s
    assert "+00:00" not in s


def test_all_windows_share_one_anchor():
    w = derive_windows(FIXED)
    assert set(w) == {"24h", "7d", "monthly"}
    end = iso_z(FIXED)
    # every window ends at the single canonical timestamp
    assert w["24h"].end == end
    assert w["7d"].end == end
    assert w["monthly"].end == end
    # starts are the canonical timestamp minus the span
    assert w["24h"].start == iso_z(FIXED - DAY_S)
    assert w["7d"].start == iso_z(FIXED - WEEK_S)
    assert w["monthly"].start == iso_z(FIXED - MONTH_S)
    assert [w[k].label for k in ("24h", "7d", "monthly")] == ["24h", "7d", "monthly"]


def test_determinism():
    # Pure: same input -> same output (no hidden clock read).
    assert derive_windows(FIXED) == derive_windows(FIXED)
