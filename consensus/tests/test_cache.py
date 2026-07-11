"""Raw-response cache: append-only store, canonical keying, no-lookahead reads."""

from __future__ import annotations

from consensus.cache import RawCache, canonical_params


def test_canonical_params_order_independent():
    assert canonical_params({"a": 1, "b": 2}) == canonical_params({"b": 2, "a": 1})
    assert canonical_params(None) == "{}"


def test_store_and_latest_roundtrip(cache: RawCache):
    body = [{"x": 1}, {"x": 2}]
    cache.store(source="s", endpoint="/e", params={"user": "u"}, body=body, http_status=200)
    got = cache.latest(source="s", endpoint="/e", params={"user": "u"})
    assert got is not None
    assert got.body == body
    assert got.row_count == 2
    assert got.http_status == 200


def test_latest_key_order_independent(cache: RawCache):
    cache.store(source="s", endpoint="/e", params={"a": 1, "b": 2}, body={"ok": True}, http_status=200)
    got = cache.latest(source="s", endpoint="/e", params={"b": 2, "a": 1})
    assert got is not None and got.body == {"ok": True}


def test_miss_returns_none(cache: RawCache):
    assert cache.latest(source="s", endpoint="/e", params={"user": "nope"}) is None


def test_append_only_latest_wins(cache: RawCache):
    p = {"user": "u"}
    cache.store(source="s", endpoint="/e", params=p, body=[1], http_status=200, fetch_ts="2026-01-01T00:00:00.000000Z")
    cache.store(source="s", endpoint="/e", params=p, body=[1, 2], http_status=200, fetch_ts="2026-02-01T00:00:00.000000Z")
    got = cache.latest(source="s", endpoint="/e", params=p)
    assert got is not None and got.body == [1, 2]
    assert cache.count() == 2  # both preserved


def test_as_of_enforces_no_lookahead(cache: RawCache):
    p = {"user": "u"}
    cache.store(source="s", endpoint="/e", params=p, body="early", http_status=200, fetch_ts="2026-01-01T00:00:00.000000Z")
    cache.store(source="s", endpoint="/e", params=p, body="late", http_status=200, fetch_ts="2026-03-01T00:00:00.000000Z")

    # As of Feb, only the January fetch is visible — the March one is the future.
    got = cache.latest(source="s", endpoint="/e", params=p, as_of="2026-02-01T00:00:00.000000Z")
    assert got is not None and got.body == "early"

    # As of April, the latest (March) is visible.
    got2 = cache.latest(source="s", endpoint="/e", params=p, as_of="2026-04-01T00:00:00.000000Z")
    assert got2 is not None and got2.body == "late"

    # As of before anything, nothing is visible.
    assert cache.latest(source="s", endpoint="/e", params=p, as_of="2025-01-01T00:00:00.000000Z") is None


def test_as_of_boundary_is_inclusive(cache: RawCache):
    """as_of == fetch_ts must be visible (fetched AT the as-of instant)."""
    ts = "2026-01-15T12:00:00.000000Z"
    p = {"user": "u"}
    cache.store(source="s", endpoint="/e", params=p, body="exact", http_status=200, fetch_ts=ts)
    got = cache.latest(source="s", endpoint="/e", params=p, as_of=ts)
    assert got is not None and got.body == "exact"


def test_stats_reports_totals_and_per_source_ranges(cache: RawCache):
    cache.store(source="a", endpoint="/x", params={}, body=[1], http_status=200,
                fetch_ts="2026-01-01T00:00:00.000000Z")
    cache.store(source="a", endpoint="/x", params={}, body=[2], http_status=200,
                fetch_ts="2026-02-01T00:00:00.000000Z")
    cache.store(source="b", endpoint="/y", params={}, body=[3], http_status=200,
                fetch_ts="2026-03-01T00:00:00.000000Z")
    s = cache.stats()
    assert s["total_rows"] == 3
    assert isinstance(s["size_bytes"], int) and s["size_bytes"] > 0
    by_source = {row["source"]: row for row in s["sources"]}
    assert by_source["a"]["rows"] == 2
    assert by_source["a"]["oldest_fetch_ts"] == "2026-01-01T00:00:00.000000Z"
    assert by_source["a"]["newest_fetch_ts"] == "2026-02-01T00:00:00.000000Z"
    assert by_source["b"]["rows"] == 1


def test_equal_fetch_ts_tiebreak_is_deterministic(cache: RawCache):
    """Two rows with identical fetch_ts: the later insert (higher id) wins,
    every time — determinism requires a total order."""
    ts = "2026-01-15T12:00:00.000000Z"
    p = {"user": "u"}
    cache.store(source="s", endpoint="/e", params=p, body="first", http_status=200, fetch_ts=ts)
    cache.store(source="s", endpoint="/e", params=p, body="second", http_status=200, fetch_ts=ts)
    for _ in range(3):
        got = cache.latest(source="s", endpoint="/e", params=p)
        assert got is not None and got.body == "second"
