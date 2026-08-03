"""Unit tests for the live first-seen resolver (M10-D §3.1). Deterministic — the two
live sources are monkeypatched; no network."""

from dataclasses import dataclass

import consensus.firstseen as fs
from consensus.errors import DataLayerError


@dataclass
class _Act:
    timestamp: int


@dataclass
class _Tx:
    timestamp: int


def _activity(pages):
    """Build a fake get_wallet_activity that serves ``pages`` (list of lists) by offset."""
    def _f(dl, wallet, *, limit, offset):
        i = offset // limit
        return pages[i] if i < len(pages) else []
    return _f


def test_activity_reaches_oldest_is_exact(monkeypatch):
    # two pages, second short -> oldest reached; earliest ts is 1000
    pages = [[_Act(5000), _Act(1000)] + [_Act(9000)] * 498, [_Act(1000), _Act(1200)]]
    monkeypatch.setattr(fs, "get_wallet_activity", _activity(pages))
    r = fs.resolve_first_seen(object(), "0xabc", now=10_000_000)
    assert r.source == "activity" and r.exact and r.ts == 1000


def test_short_page_from_a_dropped_record_does_not_truncate_the_walk(monkeypatch):
    """get_wallet_activity returns PARSED records, so a page with one unparseable
    record comes back SHORT while still being a full page of history. Terminating
    there would report a LATER first-seen -> established wallet looks fresh -> F
    inflated -> false CRITICAL. The walk must continue to a genuinely empty page."""
    short_but_full = [_Act(9000)] * (fs._ACTIVITY_PAGE - 1)   # one record dropped
    true_oldest = [_Act(1000)]
    pages = [short_but_full, true_oldest]
    monkeypatch.setattr(fs, "get_wallet_activity", _activity(pages))
    r = fs.resolve_first_seen(object(), "0xabc", now=10_000_000)
    assert r.exact and r.ts == 1000, "walk stopped on a short page and missed the true first-seen"


def test_capped_falls_back_to_etherscan(monkeypatch):
    # every page full (never short) -> capped; etherscan supplies the exact birth
    full = [_Act(50_000)] * fs._ACTIVITY_PAGE
    monkeypatch.setattr(fs, "get_wallet_activity", _activity([full] * fs._ACTIVITY_MAX_PAGES))
    monkeypatch.setattr(fs, "get_erc20_transfers", lambda dl, w, **k: [_Tx(1234), _Tx(9999)])
    r = fs.resolve_first_seen(object(), "0xabc", now=10_000_000)
    assert r.source == "etherscan" and r.exact and r.ts == 1234


def test_capped_activity_ts_is_never_returned(monkeypatch):
    # capped + no etherscan data -> established, NOT exact, and the (later) capped
    # oldest is NOT surfaced as a first-seen (would invert F). min_age is a lower bound.
    full = [_Act(50_000)] * fs._ACTIVITY_PAGE
    monkeypatch.setattr(fs, "get_wallet_activity", _activity([full] * fs._ACTIVITY_MAX_PAGES))
    monkeypatch.setattr(fs, "get_erc20_transfers", lambda dl, w, **k: [])
    r = fs.resolve_first_seen(object(), "0xabc", now=1_000_000)
    assert r.source == "activity_capped" and not r.exact and r.ts is None
    assert not r.available and r.min_age_days is not None


def test_both_sources_unavailable_is_declared(monkeypatch):
    def _boom(*a, **k):
        raise DataLayerError("down", source="x")
    monkeypatch.setattr(fs, "get_wallet_activity", _boom)
    monkeypatch.setattr(fs, "get_erc20_transfers", _boom)
    r = fs.resolve_first_seen(object(), "0xabc", now=1_000_000)
    assert r.source == "unavailable" and not r.available and r.ts is None
