"""M5 funded->bet latency: CEX classifier, funding-latency, latency score."""

from __future__ import annotations

import pytest

from consensus.errors import DataLayerError
from consensus.fetching import build_data_layer
from consensus.m5 import (classify_funder, latency_score, wallet_funding_latency,
                          _KNOWN_NONPERSONAL)
from tests.conftest import make_loaded


ETHERSCAN_URL = "https://api.etherscan.io/v2/api"


def _transfers(rows):
    return {"status": "1", "message": "OK", "result": rows}


def _row(**kw):
    d = {"blockNumber": "1", "timeStamp": "1000", "hash": "0xh",
         "from": "0xFROM", "to": "0xTO", "value": "7198000000",  # 7,198 USDC (above dust floor)
         "tokenSymbol": "USDC", "tokenDecimal": "6", "contractAddress": "0xC"}
    d.update(kw)
    return d


@pytest.fixture
def dl(tmp_path):
    d = build_data_layer(make_loaded(tmp_path, etherscan_key="K"))
    yield d
    d.cache.close()


# -- funding latency -----------------------------------------------------------


def test_latency_is_last_stable_inbound_before_first_bet(dl, requests_mock):
    w = "0xwallet"
    requests_mock.get(ETHERSCAN_URL, json=_transfers([
        _row(to=w, timeStamp="900", **{"from": "0xfunder"}),   # funding 100s before bet
        _row(to=w, timeStamp="500", **{"from": "0xfunder"}),   # older funding
        _row(to=w, timeStamp="1500", **{"from": "0xfunder"}),  # AFTER first bet -> ignored
    ]))
    wf = wallet_funding_latency(dl, w, first_bet_ts=1000)
    assert wf.funded_ts == 900 and wf.latency_s == 100
    assert wf.funder == "0xfunder" and wf.inbound_count == 2 and wf.error is None


def test_latency_pull_is_newest_first(dl, requests_mock):
    """sort=desc so the funding transfer nearest the bet survives the record cap
    (asc would keep only the oldest and truncate the real funding away)."""
    m = requests_mock.get(ETHERSCAN_URL, json=_transfers([]))
    wallet_funding_latency(dl, "0xw", first_bet_ts=1000)
    assert m.last_request.qs["sort"] == ["desc"]


def test_non_stable_and_seeded_settlement_excluded(dl, requests_mock):
    w = "0xwallet"
    cex = next(iter(_KNOWN_NONPERSONAL))
    requests_mock.get(ETHERSCAN_URL, json=_transfers([
        _row(to=w, timeStamp="800", tokenSymbol="WMATIC", **{"from": "0xf"}),  # not a stable
        _row(to=w, timeStamp="850", **{"from": cex}),   # seeded settlement route, excluded
    ]))
    wf = wallet_funding_latency(dl, w, first_bet_ts=1000)
    assert wf.latency_s is None and wf.funder is None  # declared, not imputed


def test_bidirectional_counterparty_is_not_funding(dl, requests_mock):
    """An UNSEEDED settlement route: the wallet both sent to and received from it
    -> a trading counterparty, not a deposit source. Behavioural exclusion."""
    w = "0xwallet"
    exch = "0xunseeded_negrisk"
    requests_mock.get(ETHERSCAN_URL, json=_transfers([
        _row(**{"from": w}, to=exch, timeStamp="700", tokenSymbol="POSITION"),  # sent tokens to it
        _row(to=w, timeStamp="900", **{"from": exch}),   # got USDC back = settlement, not funding
    ]))
    wf = wallet_funding_latency(dl, w, first_bet_ts=1000)
    assert wf.latency_s is None  # bidirectional -> excluded even though unseeded


def test_dust_inbound_is_not_funding(dl, requests_mock):
    w = "0xwallet"
    requests_mock.get(ETHERSCAN_URL, json=_transfers([
        _row(to=w, timeStamp="990", value="5000000", **{"from": "0xf"}),  # 5 USDC dust (6-dec)
    ]))
    wf = wallet_funding_latency(dl, w, first_bet_ts=1000)
    assert wf.latency_s is None  # below the dust floor


def test_fetch_failure_sets_error_distinct_from_no_funding(dl, requests_mock):
    requests_mock.get(ETHERSCAN_URL, status_code=500)
    wf = wallet_funding_latency(dl, "0xw", first_bet_ts=1000)
    assert wf.latency_s is None and wf.error is not None   # a fetch failure, NOT no-funding


def test_no_funding_found_is_declared_none(dl, requests_mock):
    requests_mock.get(ETHERSCAN_URL, json=_transfers([]))
    wf = wallet_funding_latency(dl, "0xw", first_bet_ts=1000)
    assert wf.latency_s is None and wf.inbound_count == 0 and wf.error is None


# -- CEX classifier ------------------------------------------------------------


def test_classify_cex_by_fanout(dl, requests_mock):
    funder = "0xhot"
    rows = [_row(**{"from": funder}, to=f"0x{i:040x}") for i in range(50)]
    requests_mock.get(ETHERSCAN_URL, json=_transfers(rows))
    c = classify_funder(dl, funder, cex_fanout_threshold=40)
    assert c.kind == "cex" and c.fanout == 50


def test_classify_dedicated_below_threshold(dl, requests_mock):
    funder = "0xpal"
    rows = [_row(**{"from": funder}, to=f"0x{i:040x}") for i in range(3)]
    requests_mock.get(ETHERSCAN_URL, json=_transfers(rows))
    c = classify_funder(dl, funder, cex_fanout_threshold=40)
    assert c.kind == "dedicated" and c.fanout == 3


def test_known_nonpersonal_short_circuits(dl):
    addr = next(iter(_KNOWN_NONPERSONAL))
    c = classify_funder(dl, addr)  # no HTTP mock needed -> must not fetch
    assert c.kind == "nonpersonal"


# -- latency score -------------------------------------------------------------


def test_latency_score_piecewise():
    bps, sc = [5, 60, 1440], [1.0, 0.6, 0.2, 0.02]
    assert latency_score(30, bps, sc) == 1.0        # 0.5 min < 5
    assert latency_score(600, bps, sc) == 0.6       # 10 min < 60
    assert latency_score(7200, bps, sc) == 0.2      # 2 h < 24h
    assert latency_score(10 * 86400, bps, sc) == 0.02
    assert latency_score(None, bps, sc) == 0.0      # no funding -> 0, never imputed
