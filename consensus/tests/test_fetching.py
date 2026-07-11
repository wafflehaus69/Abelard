"""DataLayer: cache-through on live fetch, replay from cache, loud on failure."""

from __future__ import annotations

import pytest

from consensus.errors import DataLayerError
from consensus.models import Trade
from tests.conftest import TRADES

_URL = "https://data-api.polymarket.com/trades"


def test_live_fetch_stores_and_returns(dl, requests_mock):
    m = requests_mock.get(_URL, json=[{"a": 1}])
    body = dl.fetch(
        source="polymarket_data",
        base_url="https://data-api.polymarket.com",
        endpoint="/trades",
        request_params={"user": "u"},
    )
    assert body == [{"a": 1}]
    assert m.call_count == 1
    # It was written to the cache verbatim.
    cached = dl.cache.latest(source="polymarket_data", endpoint="/trades", params={"user": "u"})
    assert cached is not None and cached.body == [{"a": 1}]


def test_replay_reads_cache_without_network(dl, requests_mock):
    m = requests_mock.get(_URL, json=[{"a": 1}])
    kwargs = dict(
        source="polymarket_data",
        base_url="https://data-api.polymarket.com",
        endpoint="/trades",
        request_params={"user": "u"},
    )
    dl.fetch(**kwargs)          # live populate
    assert m.call_count == 1

    dl.replay = True
    body = dl.fetch(**kwargs)   # replay
    assert body == [{"a": 1}]
    assert m.call_count == 1    # no additional network call


def test_replay_miss_is_loud(dl):
    dl.replay = True
    with pytest.raises(DataLayerError):
        dl.fetch(
            source="polymarket_data",
            base_url="https://data-api.polymarket.com",
            endpoint="/trades",
            request_params={"user": "never-cached"},
        )


def test_parse_records_drops_bad_and_counts(dl):
    parsed = dl.parse_records(TRADES, parser=Trade.from_api, source="polymarket_data", endpoint="/trades")
    # 3 raw records, 1 unusable (missing price) -> 2 parsed, none fabricated.
    assert len(parsed) == 2


def test_parse_records_non_list_raises(dl):
    with pytest.raises(DataLayerError):
        dl.parse_records({"not": "a list"}, parser=Trade.from_api, source="s", endpoint="/e")


def test_transport_error_mapped_to_datalayererror(dl, requests_mock):
    requests_mock.get(_URL, status_code=500)
    with pytest.raises(DataLayerError):
        dl.fetch(
            source="polymarket_data",
            base_url="https://data-api.polymarket.com",
            endpoint="/trades",
            request_params={"user": "u"},
        )


def test_not_found_mapped_to_datalayererror(dl, requests_mock):
    requests_mock.get(_URL, status_code=404)
    with pytest.raises(DataLayerError):
        dl.fetch(
            source="polymarket_data",
            base_url="https://data-api.polymarket.com",
            endpoint="/trades",
            request_params={"user": "u"},
        )


def test_rate_limited_mapped_to_datalayererror(dl, requests_mock):
    requests_mock.get(_URL, status_code=429)
    with pytest.raises(DataLayerError):
        dl.fetch(
            source="polymarket_data",
            base_url="https://data-api.polymarket.com",
            endpoint="/trades",
            request_params={"user": "u"},
        )


def test_invalid_json_2xx_mapped_to_datalayererror(dl, requests_mock):
    """A 200 with a non-JSON body must surface as a structured per-source gap,
    not escape as an unstructured ValueError that aborts the whole run."""
    requests_mock.get(_URL, text="<html>definitely not json</html>", status_code=200)
    with pytest.raises(DataLayerError) as ei:
        dl.fetch(
            source="polymarket_data",
            base_url="https://data-api.polymarket.com",
            endpoint="/trades",
            request_params={"user": "u"},
        )
    assert "invalid JSON" in str(ei.value)


def test_error_messages_scrub_secrets(tmp_path, requests_mock):
    """A requests exception embedding the wire URL (apikey included) must never
    surface the secret in the DataLayerError text — that text reaches stdout."""
    import requests as _requests
    from tests.conftest import make_loaded
    from consensus.fetching import build_data_layer

    loaded = make_loaded(tmp_path, etherscan_key="SUPERSEKRET")
    dl2 = build_data_layer(loaded)
    try:
        requests_mock.get(
            "https://api.etherscan.io/v2/api",
            exc=_requests.exceptions.ConnectionError(
                "Max retries exceeded with url: /api?module=account&apikey=SUPERSEKRET"
            ),
        )
        with pytest.raises(DataLayerError) as ei:
            dl2.fetch(
                source="etherscan_polygon",
                base_url="https://api.etherscan.io/v2/api",
                endpoint="",
                request_params={"module": "account", "apikey": "SUPERSEKRET"},
                cache_params={"module": "account"},
            )
        msg = str(ei.value) + ei.value.to_error()
        assert "SUPERSEKRET" not in msg
    finally:
        dl2.cache.close()


def test_scrub_helper_covers_both_layers(dl):
    """_scrub removes query-pattern secrets (any value) and known secret values
    (anywhere in the text)."""
    dl.loaded.secrets.etherscan_api_key = "KNOWNSECRET"
    scrubbed = dl._scrub(
        "boom url: /api?module=a&apikey=whatever123 and raw KNOWNSECRET elsewhere"
    )
    assert "whatever123" not in scrubbed
    assert "KNOWNSECRET" not in scrubbed


def test_parse_records_logs_gap_and_drops_non_dict(dl, caplog):
    import logging

    with caplog.at_level(logging.WARNING, logger="consensus.data"):
        parsed = dl.parse_records(
            [{"proxyWallet": "0xa", "side": "BUY", "conditionId": "0xC",
              "size": 1.0, "price": 0.5, "timestamp": 1},
             "not-a-dict",
             42],
            parser=Trade.from_api, source="polymarket_data", endpoint="/trades",
        )
    assert len(parsed) == 1
    assert any("dropped 2/3" in r.getMessage() for r in caplog.records)
