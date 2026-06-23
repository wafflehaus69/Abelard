"""/smg/ plugin — dual-scan, boundary discipline, rarity, decode, audit, raises."""

from __future__ import annotations

import json as _json

import pytest
import requests

from abelard_common import fourchan_fetch
from abelard_common.company_aliases import load_name_map
from abelard_common.fourchan_fetch import FourchanError, NoSmgThreadError
from chatter_daemon.config import (
    _default_common_words_path,
    _default_company_names_path,
    _default_slang_blacklist_path,
    _default_watchlists_dir,
)
from chatter_daemon.orchestrator import run_scan
from chatter_daemon.sources.base import ScanContext
from chatter_daemon.sources.smg import SmgSource, audit_name_match
from chatter_daemon.watchlist import WatchlistConfig, load_watchlist
from chatter_daemon.windows import derive_windows, iso_z

FIXED = 1_718_733_600

WL = WatchlistConfig(
    name="t",
    tickers=[
        {"symbol": "NVDA"},  # name_match:true -> "nvidia" from the shared map
        {"symbol": "DE", "name_match": False},  # ticker-only (Deere)
        {"symbol": "CAT", "name_match": False},  # ticker-only (Caterpillar)
        {"symbol": "MU", "name_match": False},  # ticker-only (Micron)
    ],
)


def _ctx():
    return ScanContext(
        scan_mode="watchlist",
        canonical_unix=FIXED,
        canonical_ts=iso_z(FIXED),
        windows=derive_windows(FIXED),
    )


class _FakeResp:
    def __init__(self, status_code, payload=None, headers=None):
        self.status_code = status_code
        self._payload = payload
        self.headers = headers or {}
        self.encoding = None

    def json(self):
        return self._payload


class _FakeSession:
    def __init__(self, responses):
        self._responses = list(responses)

    def get(self, url, headers=None, timeout=None):
        return self._responses.pop(0)


def _fetcher(responses):
    return fourchan_fetch.Fetcher(
        user_agent="t",
        session=_FakeSession(responses),
        sleep=lambda _s: None,
        clock=lambda: 0.0,
    )


def _smg(fetcher):
    return SmgSource(
        company_names_path=_default_company_names_path(),
        common_words_path=_default_common_words_path(),
        slang_blacklist_path=_default_slang_blacklist_path(),
        fetcher=fetcher,
    )


_CATALOG = [{"page": 1, "threads": [{"no": 100, "sub": "/smg/ - Stock Market General"}]}]


def _thread(coms):
    posts = [{"no": 100, "sub": "/smg/ - Stock Market General", "com": coms[0]}]
    posts += [{"no": 101 + i, "com": c} for i, c in enumerate(coms[1:])]
    return {"posts": posts}


def _records(thread):
    res = _smg(_fetcher([_FakeResp(200, _CATALOG), _FakeResp(200, thread)])).fetch(
        WL, context=_ctx()
    )
    return {r.ticker: r for r in res.records}


def test_dual_scan_cashtag_and_name_deduped():
    by = _records(_thread(["Nvidia is mooning $NVDA"]))  # name + cashtag, one post
    assert by["NVDA"].metrics.mention_count == 1
    assert set(by["NVDA"].matched_by) == {"cashtag", "name"}
    assert "rarity_hit" in by["NVDA"].flags


def test_boundary_no_false_match():
    # \b discipline: these words must NOT match DE / CAT / MU.
    by = _records(_thread(["I will DECIDE the CATEGORY of every CHEMICAL stock"]))
    for sym in ("DE", "CAT", "MU"):
        assert by[sym].metrics.mention_count == 0
        assert by[sym].matched_by == []


def test_collision_name_is_ticker_only():
    # "Caterpillar" (the name) must NOT match CAT (name_match:false); $CAT must.
    by_name = _records(_thread(["Caterpillar earnings looked strong"]))
    assert by_name["CAT"].metrics.mention_count == 0
    by_tag = _records(_thread(["$CAT to the moon"]))
    assert by_tag["CAT"].metrics.mention_count == 1
    assert by_tag["CAT"].matched_by == ["cashtag"]


def test_rarity_hit_only_on_appearance():
    by = _records(_thread(["$NVDA looking good"]))
    assert "rarity_hit" in by["NVDA"].flags
    assert by["DE"].metrics.mention_count == 0
    assert by["DE"].flags == []  # absent ticker -> no rarity_hit


def test_distinct_post_count():
    by = _records(_thread(["$NVDA", "$NVDA again", "no ticker here"]))
    assert by["NVDA"].metrics.mention_count == 2  # two distinct posts


def test_nonascii_decode_through_real_fetcher():
    def _resp(obj):
        r = requests.Response()
        r.status_code = 200
        r._content = _json.dumps(obj, ensure_ascii=False).encode("utf-8")
        r.encoding = "ISO-8859-1"  # mis-set; the fetcher forces utf-8
        return r

    thread = {
        "posts": [
            {"no": 100, "sub": "/smg/ - Stock Market General", "com": "café season — $NVDA ripping"}
        ]
    }
    res = _smg(_fetcher([_resp(_CATALOG), _resp(thread)])).fetch(WL, context=_ctx())
    by = {r.ticker: r for r in res.records}
    assert by["NVDA"].metrics.mention_count == 1  # ticker adjacent to non-ASCII still extracts


def test_no_live_smg_thread_raises():
    catalog = [{"page": 1, "threads": [{"no": 1, "sub": "crypto general"}]}]
    with pytest.raises(NoSmgThreadError):
        _smg(_fetcher([_FakeResp(200, catalog)])).fetch(WL, context=_ctx())


def test_malformed_thread_raises():
    bad_thread = {"posts": "not a list"}
    with pytest.raises(FourchanError):
        _smg(_fetcher([_FakeResp(200, _CATALOG), _FakeResp(200, bad_thread)])).fetch(
            WL, context=_ctx()
        )


def test_audit_clean_on_barber_growth():
    wl = load_watchlist("barber_growth", watchlists_dir=_default_watchlists_dir())
    shared = load_name_map(_default_company_names_path())
    audit = audit_name_match(wl, shared)
    empty = [sym for sym, names in audit.items() if not names]
    assert empty == []  # no name_match:true ticker resolves nothing


def test_end_to_end_via_run_scan():
    src = _smg(_fetcher([_FakeResp(200, _CATALOG), _FakeResp(200, _thread(["$NVDA"]))]))
    env = run_scan([WL], sources=[src], now=FIXED)
    assert env.degraded is False
    assert env.sources[0].source == "smg"
    assert env.sources[0].ok is True
    nvda = next(r for r in env.records if r.ticker == "NVDA")
    assert nvda.metrics.mention_count == 1
    assert nvda.sentiment.method == "none"  # 1 mention < floor (and no key) -> no stance


# --- Haiku stance over /smg/ post text (Order 9) ---------------------------


class _Block:
    def __init__(self, text):
        self.type = "text"
        self.text = text


class _Usage:
    def __init__(self):
        self.input_tokens = 10
        self.output_tokens = 5
        self.cache_read_input_tokens = 0
        self.cache_creation_input_tokens = 0


class _Resp:
    def __init__(self, text, stop):
        self.content = [_Block(text)]
        self.usage = _Usage()
        self.stop_reason = stop


class _FakeMessages:
    def __init__(self, text, stop):
        self._text, self._stop = text, stop

    def create(self, **kwargs):
        return _Resp(self._text, self._stop)


class _FakeAnthropic:
    def __init__(self, text='{"classifications":[]}', stop="end_turn"):
        self.messages = _FakeMessages(text, stop)


def _smg_haiku(fetcher, anthropic_client, *, floor=3):
    return SmgSource(
        company_names_path=_default_company_names_path(),
        common_words_path=_default_common_words_path(),
        slang_blacklist_path=_default_slang_blacklist_path(),
        anthropic_client=anthropic_client,
        sentiment_min_mentions=floor,
        fetcher=fetcher,
    )


def test_smg_haiku_stance_above_floor():
    thread = _thread(["$NVDA breaking out", "$NVDA loading calls", "$NVDA puts look juicy"])
    classifications = {"classifications": [
        {"post_id": "100", "ticker": "NVDA", "stance": "bullish"},
        {"post_id": "101", "ticker": "NVDA", "stance": "bullish"},
        {"post_id": "102", "ticker": "NVDA", "stance": "bearish"},
    ]}
    src = _smg_haiku(
        _fetcher([_FakeResp(200, _CATALOG), _FakeResp(200, thread)]),
        _FakeAnthropic(text=_json.dumps(classifications)),
        floor=3,
    )
    res = src.fetch(WL, context=_ctx())
    by = {r.ticker: r for r in res.records}
    nvda = by["NVDA"]
    assert nvda.metrics.mention_count == 3
    assert nvda.sentiment.method == "haiku"
    assert (nvda.sentiment.bullish, nvda.sentiment.bearish, nvda.sentiment.neutral) == (2, 1, 0)
    assert res.cost.haiku_calls == 1  # one per-ticker call
    assert by["DE"].sentiment.method == "none"  # 0 mentions -> below floor, no stance


def test_smg_below_floor_no_haiku():
    thread = _thread(["$NVDA solo mention"])  # 1 distinct post < floor 3
    src = _smg_haiku(
        _fetcher([_FakeResp(200, _CATALOG), _FakeResp(200, thread)]), _FakeAnthropic(), floor=3
    )
    res = src.fetch(WL, context=_ctx())
    nvda = {r.ticker: r for r in res.records}["NVDA"]
    assert nvda.metrics.mention_count == 1 and nvda.sentiment.method == "none"
    assert res.cost.haiku_calls == 0  # gate held


def test_smg_no_anthropic_key_no_haiku():
    # above floor (3 posts) but NO key -> provider returns None -> method stays none
    thread = _thread(["$NVDA a", "$NVDA b", "$NVDA c"])
    src = _smg(_fetcher([_FakeResp(200, _CATALOG), _FakeResp(200, thread)]))
    res = src.fetch(WL, context=_ctx())
    nvda = {r.ticker: r for r in res.records}["NVDA"]
    assert nvda.metrics.mention_count == 3 and nvda.sentiment.method == "none"


def test_smg_haiku_failure_degrades_to_none():
    thread = _thread(["$NVDA a", "$NVDA b", "$NVDA c"])
    src = _smg_haiku(
        _fetcher([_FakeResp(200, _CATALOG), _FakeResp(200, thread)]),
        _FakeAnthropic(text="not json at all"),  # parse error -> SentimentError
        floor=3,
    )
    res = src.fetch(WL, context=_ctx())
    nvda = {r.ticker: r for r in res.records}["NVDA"]
    assert nvda.sentiment.method == "none"  # degraded, but the count still ships
    assert nvda.metrics.mention_count == 3
    assert any("NVDA" in w and "Haiku failed" in w for w in res.warnings)


def test_clean_com_strips_html_and_unescapes():
    from chatter_daemon.sources.smg import _clean_com

    out = _clean_com('<span class="quote">&gt;buy $NVDA</span><br>great &amp; cheap')
    assert "<" not in out and "&gt;" not in out and "&amp;" not in out
    assert ">buy $NVDA" in out and "great & cheap" in out
