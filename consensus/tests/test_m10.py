"""M10 live UNUSUAL_ACTIVITY scan (Detector B): scan pipeline, latency elevator,
and the hard discipline rules (no EV, gaps declared, enrichment gated)."""

from __future__ import annotations

import json

from consensus.fetching import build_data_layer
from consensus.m10 import run_scan
from consensus.models import Trade
from consensus.tape import TapeStore
from tests.conftest import make_loaded

ETHERSCAN_URL = "https://api.etherscan.io/v2/api"
_BET_TS = 1_700_000_000


def _seed_tape(loaded, *, wallet="0xWALLET", cid="0xM", size=60_000, price=0.5, ts=_BET_TS):
    tape = TapeStore(loaded.tape_path)
    tape.upsert_market(cid, slug="m", question="q", tags="geopolitics",
                       source="enumeration", now_ts=1)
    tape.store_page([{
        "proxyWallet": wallet, "side": "BUY", "asset": "tok1", "conditionId": cid,
        "size": size, "price": price, "timestamp": ts,
        "transactionHash": "0xtx", "slug": "m",
    }], lane="market", poll_id=1, parsed_by=Trade.from_api)
    tape.close()


def test_m10_scan_surfaces_dossier_and_has_no_ev(tmp_path):
    loaded = make_loaded(tmp_path)
    _seed_tape(loaded)
    dl = build_data_layer(loaded)
    try:
        s = run_scan(dl, loaded, max_wallets=0)  # skip enrichment -> no chain calls
    finally:
        dl.cache.close()
    assert s["daemon"] == "consensus_m10" and s["schema"] == 1
    r = s["result"]
    assert r["candidates_scored"] >= 1 and r["enriched"] == 0
    assert r["dossiers"], "a big directional footprint should surface"
    d = r["dossiers"][0]
    assert d["market"] == "0xM" and d["wallet"] == "0xwallet"
    assert d["tier"] in ("WATCH", "ELEVATED", "CRITICAL")
    # HARD RULE: no EV anywhere in the dossier or envelope.
    blob = json.dumps(s).lower()
    assert "expected_value" not in blob and '"ev"' not in blob
    assert "not a validated trade signal" in d["caveat"].lower()


def test_m10_latency_elevator_boosts_a_past_bar_wallet(tmp_path, requests_mock):
    loaded = make_loaded(tmp_path, etherscan_key="K")
    _seed_tape(loaded)
    # Funding 60s before the bet from a low-fanout (dedicated) funder -> tight
    # latency, full elevator boost.
    requests_mock.get(ETHERSCAN_URL, json={"status": "1", "message": "OK", "result": [
        {"blockNumber": "1", "timeStamp": str(_BET_TS - 60), "hash": "0xh",
         "from": "0xfunder", "to": "0xwallet", "value": "50000000000",
         "tokenSymbol": "USDC", "tokenDecimal": "6", "contractAddress": "0xc"},
    ]})
    dl = build_data_layer(loaded)
    try:
        s = run_scan(dl, loaded)
    finally:
        dl.cache.close()
    enriched = [d for d in s["result"]["dossiers"] if d["enriched"]]
    assert enriched, "a strong candidate must clear the fill-factor bar and be enriched"
    d = enriched[0]
    assert d["funding"]["latency_s"] == 60
    assert d["funding"]["funder_kind"] == "dedicated"
    assert d["latency_boost"] == 1.5
    assert d["composite"] > d["composite_pre_elevator"]  # elevated, never suppressed


def test_m10_enrichment_error_declares_not_imputes(tmp_path):
    """No Etherscan key -> the enrichment fetch fails loudly; latency stays
    unknown (never imputed) and the wallet keeps its fill-factor tier."""
    loaded = make_loaded(tmp_path)  # no etherscan key
    _seed_tape(loaded)
    dl = build_data_layer(loaded)
    try:
        s = run_scan(dl, loaded)
    finally:
        dl.cache.close()
    enriched = [d for d in s["result"]["dossiers"] if d["enriched"]]
    if enriched:
        d = enriched[0]
        assert d["funding"]["latency_s"] is None
        assert d["funding"]["enrichment_error"]  # declared, not imputed
        assert d["latency_boost"] == 1.0          # no lift on a failed pull
    assert s["status"] == "degraded"  # errors present


def test_m10_empty_window_is_clean(tmp_path):
    loaded = make_loaded(tmp_path)
    TapeStore(loaded.tape_path).close()  # empty tape
    dl = build_data_layer(loaded)
    try:
        s = run_scan(dl, loaded)
    finally:
        dl.cache.close()
    assert s["result"]["dossiers"] == [] and s["result"]["fills_scanned"] == 0
    assert s["status"] in ("ok", "degraded")
