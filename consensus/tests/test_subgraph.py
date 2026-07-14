"""L1 subgraph fetchers: GraphQL cache/replay discipline, cursor walks,
provenance, and the deep-slice reproducibility gate (build order #2)."""

from __future__ import annotations

import json

import pytest

from consensus.errors import DataLayerError
from consensus.sources_subgraph import (
    get_order_filled_events,
    get_subgraph_meta,
    paginate_order_filled,
)
from tests.conftest import SUBGRAPH_URL, subgraph_event, subgraph_meta_body


def _events_body(events):
    return {"data": {"orderFilledEvents": events}}


def _meta_response():
    return {"json": {"data": subgraph_meta_body()}}


# -- fetch_graphql plumbing ----------------------------------------------------


def test_graphql_errors_array_is_loud_and_not_cached(dl, requests_mock):
    requests_mock.post(SUBGRAPH_URL, json={"errors": [{"message": "rate limited"}]})
    with pytest.raises(DataLayerError) as ei:
        get_subgraph_meta(dl)
    assert "server returned errors" in str(ei.value)
    # A failed query must not be replayable as a good response.
    dl.replay = True
    with pytest.raises(DataLayerError, match="replay cache miss"):
        get_subgraph_meta(dl)


def test_graphql_non_object_body_is_loud(dl, requests_mock):
    requests_mock.post(SUBGRAPH_URL, json=[1, 2, 3])
    with pytest.raises(DataLayerError, match="expected an object"):
        get_subgraph_meta(dl)


def test_graphql_missing_data_is_loud(dl, requests_mock):
    requests_mock.post(SUBGRAPH_URL, json={"something": 1})
    with pytest.raises(DataLayerError, match="no data object"):
        get_subgraph_meta(dl)


def test_graphql_transport_error_mapped(dl, requests_mock):
    requests_mock.post(SUBGRAPH_URL, status_code=500)
    with pytest.raises(DataLayerError, match="transport error"):
        get_subgraph_meta(dl)


# -- page fetch + filters --------------------------------------------------------


def test_get_events_parses_and_types(dl, requests_mock):
    requests_mock.post(SUBGRAPH_URL, json=_events_body([subgraph_event(1), {"id": None}]))
    events = get_order_filled_events(dl, asset_ids=["111000111"])
    assert len(events) == 1  # malformed record dropped as a counted gap
    e = events[0]
    assert e.timestamp == 1001 and e.maker == "0xmaker1"
    assert e.maker_amount_filled == 220_001  # raw int, no interpretation
    assert e.transaction_hash == "0x0001tx"


def test_where_clause_carries_filters(dl, requests_mock):
    m = requests_mock.post(SUBGRAPH_URL, json=_events_body([]))
    get_order_filled_events(dl, asset_ids=["A1"], ts_gte=100, ts_lt=200, first=500)
    query = m.last_request.json()["query"]
    # Graph-node forbids column filters beside `or` — every branch must carry
    # the shared filters (verified against the live server).
    branch = 'timestamp_gte: "100", timestamp_lt: "200"'
    assert f'{{ makerAssetId: "A1", {branch} }}' in query
    assert f'{{ takerAssetId: "A1", {branch} }}' in query
    assert query.count(branch) == 2  # filters only inside branches
    assert "or: [" in query
    assert "first: 500" in query
    # Ordered by timestamp (not id): orderBy id times the server out on
    # high-volume markets (verified live 2026-07-13).
    assert "orderBy: timestamp" in query


# -- cursor walk + provenance ------------------------------------------------------


def test_paginate_walks_ts_cursor_and_reports_provenance(dl, requests_mock):
    # distinct timestamps so no boundary overlap
    page1 = [subgraph_event(i, ts=1000 + i) for i in range(3)]
    page2 = [subgraph_event(7, ts=1010)]
    m = requests_mock.post(SUBGRAPH_URL, [
        _meta_response(),
        {"json": _events_body(page1)},
        {"json": _events_body(page2)},
    ])
    events, prov = paginate_order_filled(dl, asset_ids=["111000111"], page_size=3)
    assert len(events) == 4
    assert m.call_count == 3
    # Cursor continuation used the last TIMESTAMP of page 1 (gte, for boundary
    # safety).
    q3 = m.request_history[2].json()["query"]
    assert f'timestamp_gte: "{page1[-1]["timestamp"]}"' in q3
    assert prov["layer"] == "L1" and prov["head_block"] == 87_814_766
    assert prov["pages"] == 2 and prov["events"] == 4
    assert prov["first_ts"] == 1000 and prov["last_ts"] == 1010
    assert prov["truncated_by_max_records"] is False
    assert prov["forced_skips_ts"] == []


def test_paginate_dedupes_second_boundary_overlap(dl, requests_mock):
    """A fill at the page-boundary second reappears when the next page re-queries
    timestamp_gte — it must be counted once, never lost or doubled."""
    shared = subgraph_event(2, ts=1002)      # lands at the tail of page 1...
    page1 = [subgraph_event(0, ts=1000), subgraph_event(1, ts=1001), shared]
    page2 = [shared, subgraph_event(3, ts=1003)]   # ...and the head of page 2
    requests_mock.post(SUBGRAPH_URL, [
        _meta_response(),
        {"json": _events_body(page1)},
        {"json": _events_body(page2)},
    ])
    events, prov = paginate_order_filled(dl, asset_ids=["A"], page_size=3)
    ids = [e.event_id for e in events]
    assert len(ids) == len(set(ids)) == 4  # shared counted exactly once
    assert prov["events"] == 4


def test_paginate_livelock_guard_on_overfull_second(dl, requests_mock):
    """>page_size fills in one second: the walk must advance past it (declared
    in forced_skips_ts) instead of re-querying the same second forever."""
    same = [subgraph_event(i, ts=1000) for i in range(3)]     # full page, one ts
    after = [subgraph_event(9, ts=1001)]
    requests_mock.post(SUBGRAPH_URL, [
        _meta_response(),
        {"json": _events_body(same)},
        {"json": _events_body(same)},   # gte 1000 re-serves the same second...
        {"json": _events_body(after)},  # ...then the guard steps to gte 1001
    ])
    events, prov = paginate_order_filled(dl, asset_ids=["A"], page_size=3)
    assert prov["forced_skips_ts"] == [1000]
    assert 1001 in [e.timestamp for e in events]


def test_paginate_max_records_is_explicit_in_provenance(dl, requests_mock):
    requests_mock.post(SUBGRAPH_URL, [
        _meta_response(),
        {"json": _events_body([subgraph_event(i) for i in range(3)])},
    ])
    events, prov = paginate_order_filled(dl, asset_ids=["A"], page_size=3, max_records=2)
    assert len(events) == 2
    assert prov["truncated_by_max_records"] is True  # never a silent cap


# -- the build-order gate: deep-slice reproducibility -------------------------------


def test_deep_slice_replay_reproduces_walk_exactly(dl, requests_mock):
    """Live walk populates the cache; a replay DataLayer re-serves the exact
    same slice with zero network calls — the M0 substrate contract."""
    page1 = [subgraph_event(i, ts=1_733_100_000 + i) for i in range(3)]
    page2 = [subgraph_event(9, ts=1_733_100_009)]
    m = requests_mock.post(SUBGRAPH_URL, [
        _meta_response(),
        {"json": _events_body(page1)},
        {"json": _events_body(page2)},
    ])
    live_events, live_prov = paginate_order_filled(
        dl, asset_ids=["111000111"], ts_gte=1_733_000_000, ts_lt=1_734_000_000, page_size=3
    )
    calls_after_live = m.call_count

    dl.replay = True
    replay_events, replay_prov = paginate_order_filled(
        dl, asset_ids=["111000111"], ts_gte=1_733_000_000, ts_lt=1_734_000_000, page_size=3
    )
    assert m.call_count == calls_after_live  # zero network in replay
    assert replay_events == live_events
    assert replay_prov == live_prov


def test_replay_miss_on_different_slice_is_loud(dl, requests_mock):
    requests_mock.post(SUBGRAPH_URL, [
        _meta_response(),
        {"json": _events_body([subgraph_event(1)])},
    ])
    paginate_order_filled(dl, asset_ids=["A"], page_size=3)
    dl.replay = True
    with pytest.raises(DataLayerError, match="replay cache miss"):
        # Different filter -> different query -> not in cache. Loud, not empty.
        paginate_order_filled(dl, asset_ids=["B"], page_size=3)


# -- CLI ---------------------------------------------------------------------------


def test_cli_subgraph_by_market_resolves_tokens(config_file, requests_mock, capsys):
    from consensus.cli import main
    from tests.conftest import GAMMA_MARKETS

    requests_mock.get("https://gamma-api.polymarket.com/markets", json=GAMMA_MARKETS)
    requests_mock.post(SUBGRAPH_URL, [
        _meta_response(),
        {"json": _events_body([subgraph_event(1), subgraph_event(2)])},
    ])
    rc = main(["--config", str(config_file), "--json", "data", "subgraph", "--market", "0xCID"])
    assert rc == 0
    out = json.loads(capsys.readouterr().out)
    assert out["kind"] == "data.subgraph"
    assert out["asset_ids"] == ["111000111", "222000222"]  # from clobTokenIds
    assert out["count"] == 2
    assert out["provenance"]["layer"] == "L1"
    assert out["events"][0]["maker_amt"] == 220_001
