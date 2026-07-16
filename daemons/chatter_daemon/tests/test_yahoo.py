"""Yahoo per-ticker RSS source (CH-SRC-1) — relevance filter (title+desc, incl. name for a
name_match:false ticker), Finnhub net-new dedup, silent-deprecation freshness assertion, and
per-ticker degrade. Hermetic — a fake get_text client, no network."""

from __future__ import annotations

from datetime import datetime, timezone
from email.utils import format_datetime

from abelard_common.http_client import TransportError

from chatter_daemon.schema import Headline, Metrics, NormalizedRecord, Sentiment
from chatter_daemon.sources.base import ScanContext
from chatter_daemon.sources.yahoo_rss import YahooRssSource
from chatter_daemon.watchlist import WatchlistConfig
from chatter_daemon.windows import derive_windows, iso_z

FIXED = 1_718_733_600
WL = WatchlistConfig(name="w", tickers=[
    {"symbol": "NVDA", "names": ["Nvidia"]},
    {"symbol": "MU", "name_match": False, "names": ["Micron"]},
])


def _ctx():
    return ScanContext(scan_mode="watchlist", canonical_unix=FIXED, canonical_ts=iso_z(FIXED),
                       windows=derive_windows(FIXED))


def _item(title, link, desc="", ago_h=1):
    pub = format_datetime(datetime.fromtimestamp(FIXED - int(ago_h * 3600), tz=timezone.utc))
    return f"<item><title>{title}</title><link>{link}</link><description>{desc}</description><pubDate>{pub}</pubDate></item>"


def _rss(*items):
    return '<?xml version="1.0" encoding="UTF-8"?><rss version="2.0"><channel>' + "".join(items) + "</channel></rss>"


class _FakeClient:
    """get_text keyed on params['s']; a mapped Exception is raised (transport failure)."""

    def __init__(self, by_symbol):
        self._by_symbol = by_symbol
        self.calls: list = []

    def get_text(self, url, *, params=None, headers=None, timeout=None):
        self.calls.append(params)
        v = self._by_symbol.get(params["s"])
        if isinstance(v, Exception):
            raise v
        return v if v is not None else _rss()


def _fin(sym, *titles):
    return NormalizedRecord(
        watchlist="w", scan_mode="watchlist", canonical_ts=iso_z(FIXED),
        window=derive_windows(FIXED)["24h"], source="finnhub_news", ticker=sym,
        matched_by=["symbol"], sentiment=Sentiment(method="none"),
        metrics=Metrics(mention_count=len(titles),
                        headlines=[Headline(title=t, url="http://x") for t in titles]),
    )


def test_relevance_filter_keeps_on_ticker_drops_market_noise():
    xml = _rss(
        _item("Nvidia earnings beat expectations", "http://a"),  # name -> relevant
        _item("Why Micron Stock Dropped Again", "http://b"),     # not NVDA
        _item("Sector Update: Tech Stocks Fall", "http://c"),    # not NVDA
        _item("NVDA H200 chips ship to China", "http://d"),      # symbol -> relevant
    )
    res = YahooRssSource(client=_FakeClient({"NVDA": xml})).fetch(WL, context=_ctx())
    by = {r.ticker: r for r in res.records}
    titles = [h.title for h in by["NVDA"].metrics.headlines]
    assert by["NVDA"].metrics.mention_count == 2  # only the 2 on-ticker items
    assert "Nvidia earnings beat expectations" in titles and "NVDA H200 chips ship to China" in titles
    assert by["NVDA"].source == "yahoo_rss" and by["NVDA"].sentiment.method == "none"
    assert by["MU"].metrics.mention_count == 0  # empty feed -> honest zero


def test_name_match_false_ticker_matches_own_name():
    # MU is name_match:false, but Yahoo's ?s=MU feed is ticker-scoped, so 'Micron' is safe.
    xml = _rss(_item("Micron guides HBM revenue higher", "http://m"))
    res = YahooRssSource(client=_FakeClient({"MU": xml})).fetch(WL, context=_ctx())
    assert {r.ticker: r for r in res.records}["MU"].metrics.mention_count == 1


def test_title_only_ignores_description_mentions():
    # A headline whose TITLE doesn't name the ticker (only the blurb does) is NOT attributed —
    # this kills the cross-ticker dup where a single-name article's blurb lists many tickers.
    xml = _rss(_item("Market Wrap: Stocks Rise Broadly", "http://a", desc="Micron gained 3% today"))
    res = YahooRssSource(client=_FakeClient({"MU": xml})).fetch(WL, context=_ctx())
    assert {r.ticker: r for r in res.records}["MU"].metrics.mention_count == 0  # title didn't name MU


def test_roundup_title_naming_many_tickers_dropped():
    syms = ["NVDA", "MSFT", "AMD", "META", "AAPL"]
    wl = WatchlistConfig(name="w", tickers=[{"symbol": s} for s in syms])
    xml = _rss(
        _item("Dow movers: NVDA, MSFT, AMD, META and AAPL all rally", "http://a"),  # 5 -> roundup
        _item("NVDA ships new chip", "http://b"),                                    # 1 -> keep
    )
    fake = _FakeClient({s: (xml if s == "NVDA" else _rss()) for s in syms})
    res = YahooRssSource(client=fake, roundup_max=4).fetch(wl, context=_ctx())
    nvda = {r.ticker: r for r in res.records}["NVDA"]
    assert [h.title for h in nvda.metrics.headlines] == ["NVDA ships new chip"]  # roundup dropped


def test_roundup_disabled_keeps_multi_ticker_headline():
    syms = ["NVDA", "MSFT", "AMD", "META"]
    wl = WatchlistConfig(name="w", tickers=[{"symbol": s} for s in syms])
    xml = _rss(_item("Movers: NVDA, MSFT, AMD, META all up", "http://a"))
    fake = _FakeClient({s: (xml if s == "NVDA" else _rss()) for s in syms})
    res = YahooRssSource(client=fake, roundup_max=0).fetch(wl, context=_ctx())  # 0 = off
    assert {r.ticker: r for r in res.records}["NVDA"].metrics.mention_count == 1


def test_dedup_vs_finnhub_keeps_only_net_new():
    xml = _rss(
        _item("Nvidia ships new GPU", "http://a"),     # already in Finnhub -> drop
        _item("Nvidia CEO on AI demand", "http://b"),  # net-new -> keep
    )
    prior = [_fin("NVDA", "Nvidia ships new GPU")]
    res = YahooRssSource(client=_FakeClient({"NVDA": xml})).fetch(WL, context=_ctx(), prior_records=prior)
    nvda = {r.ticker: r for r in res.records}["NVDA"]
    assert [h.title for h in nvda.metrics.headlines] == ["Nvidia CEO on AI demand"]


def test_within_feed_dedup():
    xml = _rss(_item("Nvidia rallies today", "http://a"), _item("Nvidia rallies today", "http://b"))
    res = YahooRssSource(client=_FakeClient({"NVDA": xml})).fetch(WL, context=_ctx())
    assert {r.ticker: r for r in res.records}["NVDA"].metrics.mention_count == 1


def test_zero_items_scanwide_is_source_error():
    res = YahooRssSource(client=_FakeClient({"NVDA": _rss(), "MU": _rss()})).fetch(WL, context=_ctx())
    assert res.error and "zero items" in res.error


def test_stale_feed_warns():
    xml = _rss(_item("Nvidia old news", "http://a", ago_h=100))  # 100h > 48h default
    res = YahooRssSource(client=_FakeClient({"NVDA": xml})).fetch(WL, context=_ctx())
    assert any("stale" in w for w in res.warnings)


def test_per_ticker_error_isolated():
    xml = _rss(_item("NVDA rallies", "http://a"))
    res = YahooRssSource(client=_FakeClient({"NVDA": xml, "MU": TransportError("boom")})).fetch(WL, context=_ctx())
    tickers = {r.ticker for r in res.records}
    assert "NVDA" in tickers and "MU" not in tickers   # MU blocked, NVDA fine
    assert res.error is None and any("MU" in w for w in res.warnings)  # partial block -> warning, not error


def test_all_blocked_is_source_error():
    fake = _FakeClient({"NVDA": TransportError("x"), "MU": TransportError("y")})
    res = YahooRssSource(client=fake).fetch(WL, context=_ctx())
    assert res.error and "unavailable" in res.error


def test_malformed_xml_blocks_that_ticker():
    fake = _FakeClient({"NVDA": "<not valid xml", "MU": _rss(_item("Micron news out", "http://m"))})
    res = YahooRssSource(client=fake).fetch(WL, context=_ctx())
    by = {r.ticker: r for r in res.records}
    assert "NVDA" not in by                          # parse error -> blocked
    assert by["MU"].metrics.mention_count == 1        # the other ticker still ships


def test_raw_items_carry_yahoo_heads():
    xml = _rss(_item("Nvidia AI update", "http://a"))
    res = YahooRssSource(client=_FakeClient({"NVDA": xml})).fetch(WL, context=_ctx())
    assert "NVDA\tNvidia AI update" in res.raw_items
