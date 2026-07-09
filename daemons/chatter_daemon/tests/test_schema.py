"""Normalized-record schema + Source protocol — compile, validate, reject drift."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from chatter_daemon.schema import NormalizedRecord, ScanEnvelope
from chatter_daemon.sources.base import ChatterPost, Source, SourceResult

_WINDOW = {"start": "2026-06-17T18:00:00Z", "end": "2026-06-18T18:00:00Z", "label": "24h"}


def _record(**over):
    base = dict(
        watchlist="barber_growth",
        scan_mode="watchlist",
        canonical_ts="2026-06-18T18:00:00Z",
        window=_WINDOW,
        source="stocktwits",
        ticker="NVDA",
        matched_by=["symbol"],
        metrics={"mention_count": 412},
        sentiment={"method": "native", "bullish": 220, "bearish": 90, "neutral": 102},
        flags=[],
    )
    base.update(over)
    return base


def test_normalized_record_roundtrip():
    rec = NormalizedRecord.model_validate(_record())
    assert rec.schema_version == "1"
    assert rec.ticker == "NVDA"
    assert rec.metrics.mention_count == 412
    assert rec.sentiment is not None and rec.sentiment.method == "native"
    dumped = rec.model_dump(mode="json")
    assert NormalizedRecord.model_validate(dumped) == rec


def test_record_rejects_unknown_field():
    with pytest.raises(ValidationError):
        NormalizedRecord.model_validate(_record(bogus=1))


def test_record_rejects_unknown_source():
    with pytest.raises(ValidationError):
        NormalizedRecord.model_validate(_record(source="facebook"))  # not a SourceName


def test_record_rejects_unknown_matched_by():
    with pytest.raises(ValidationError):
        NormalizedRecord.model_validate(_record(matched_by=["ticker"]))  # not a MatchedBy


def test_metrics_rejects_negative_mentions():
    with pytest.raises(ValidationError):
        NormalizedRecord.model_validate(_record(metrics={"mention_count": -1}))


def test_scan_envelope_validates():
    env = ScanEnvelope.model_validate(
        {
            "scan_mode": "watchlist",
            "canonical_ts": "2026-06-18T18:00:00Z",
            "windows": [_WINDOW],
            "watchlists": [{"name": "barber_growth", "tickers": 46, "active": 45}],
            "records": [_record()],
            "errors": [],
        }
    )
    assert env.schema_version == "1"
    assert env.watchlists[0].active == 45
    assert len(env.records) == 1


def test_source_protocol_runtime_checkable():
    class Dummy:
        name = "stocktwits"

        def fetch(self, watchlist, *, context):
            return SourceResult(source="stocktwits")

    assert isinstance(Dummy(), Source)
    assert not isinstance(object(), Source)


def test_chatterpost_and_sourceresult_construct():
    post = ChatterPost(source="stocktwits", post_id="t3_abc", text="GME to the moon")
    assert post.explicit_symbols == ()
    assert post.meta == {}
    res = SourceResult(source="stocktwits", warnings=["degraded"], error=None)
    assert res.records == []
    assert res.warnings == ["degraded"]


def test_record_requires_sentiment():
    base = _record()
    del base["sentiment"]
    with pytest.raises(ValidationError):
        NormalizedRecord.model_validate(base)  # sentiment is non-optional


def test_no_stance_source_uses_method_none():
    rec = NormalizedRecord.model_validate(_record(sentiment={"method": "none"}))
    assert rec.sentiment.method == "none"


def test_envelope_carries_sources_and_degraded():
    env = ScanEnvelope.model_validate(
        {
            "scan_mode": "watchlist",
            "canonical_ts": "2026-06-18T18:00:00Z",
            "windows": [_WINDOW],
            "watchlists": [],
            "sources": [
                {"source": "finnhub_news", "ok": True, "record_count": 45},
                {"source": "stocktwits", "ok": False, "record_count": 0, "error": "no key"},
            ],
            "records": [],
            "degraded": True,
            "errors": ["stocktwits: no key"],
        }
    )
    assert env.degraded is True
    assert env.sources[0].source == "finnhub_news"
    assert env.sources[1].ok is False
