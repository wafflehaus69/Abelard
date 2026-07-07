"""Unit tests for the stated-magnitude extractor (synthesize/magnitude.py)."""

from __future__ import annotations

import pytest

from news_watch_daemon.synthesize.magnitude import Magnitude, extract_magnitudes


def _one(text: str) -> Magnitude:
    mags = extract_magnitudes(text)
    assert len(mags) == 1, f"expected exactly 1 magnitude in {text!r}, got {mags}"
    return mags[0]


# ---------- currency ----------

@pytest.mark.parametrize("text,value", [
    ("$2B", 2e9),
    ("$2 billion", 2e9),
    ("$2bn", 2e9),
    ("$2.3M", 2.3e6),
    ("USD 2 billion", 2e9),
    ("$5", 5.0),
    ("$2t", 2e12),
    ("$2,500", 2500.0),
])
def test_currency_usd_value(text, value):
    m = _one(text)
    assert m.kind == "currency"
    assert m.unit == "USD"
    assert m.value == pytest.approx(value)
    assert m.raw_span == text


@pytest.mark.parametrize("text,unit,value", [
    ("€500 million", "EUR", 5e8),
    ("£1.2bn", "GBP", 1.2e9),
    ("¥3 trillion", "JPY", 3e12),
])
def test_currency_symbols(text, unit, value):
    m = _one(text)
    assert m.kind == "currency"
    assert m.unit == unit
    assert m.value == pytest.approx(value)


# ---------- percent ----------

@pytest.mark.parametrize("text,value,raw", [
    ("20%", 20.0, "20%"),
    ("20 percent", 20.0, "20 percent"),
    ("300bps", 3.0, "300bps"),
    ("300 basis points", 3.0, "300 basis points"),
])
def test_percent(text, value, raw):
    m = _one(text)
    assert m.kind == "percent"
    assert m.unit == "percent"
    assert m.value == pytest.approx(value)
    assert m.raw_span == raw


# ---------- volume / physical ----------

@pytest.mark.parametrize("text,value,unit", [
    ("40M barrels", 4e7, "barrels"),
    ("500 tonnes", 500.0, "tonnes"),
    ("500 tons", 500.0, "tonnes"),
    ("2.5GW", 2.5, "GW"),
    ("800 MW", 800.0, "MW"),
    ("10 TWh", 10.0, "TWh"),
    ("2 million barrels", 2e6, "barrels"),
])
def test_volume(text, value, unit):
    m = _one(text)
    assert m.kind == "volume"
    assert m.unit == unit
    assert m.value == pytest.approx(value)


# ---------- noise-guard rejects (must yield ()) ----------

@pytest.mark.parametrize("text", [
    "2026",
    "In 2026 the summit convened",
    "v4.6",
    "Series 7",
    "3 senators voted no",
    "two ships collided",
    "call 555-1234",
    "40 people gathered",
    "the number 40",
    "chapter 12 opens",
])
def test_noise_rejects(text):
    assert extract_magnitudes(text) == ()


# ---------- totality + in-context ----------

def test_empty_and_totality():
    assert extract_magnitudes("") == ()
    assert extract_magnitudes("no numbers here at all") == ()


def test_in_context_sentence():
    m = _one("Company loses $2B in chips lost at sea off Taiwan")
    assert m.value == pytest.approx(2e9)
    assert m.raw_span == "$2B"


def test_multiple_distinct_magnitudes_in_order():
    mags = extract_magnitudes("Oil exports of 40M barrels rose 20% after the $2B deal")
    kinds = [(m.kind, m.raw_span) for m in mags]
    assert ("volume", "40M barrels") in kinds
    assert ("percent", "20%") in kinds
    assert ("currency", "$2B") in kinds
    assert len(mags) == 3


def test_non_str_raises():
    with pytest.raises(TypeError):
        extract_magnitudes(None)  # type: ignore[arg-type]


# ---------- integration: enrichment + render ----------

from news_watch_daemon.synthesize.cluster import Cluster, ClusterInput  # noqa: E402
from news_watch_daemon.synthesize.synthesize import (  # noqa: E402
    enrich_clusters_with_magnitudes,
)
from news_watch_daemon.synthesize.prompt import _format_cluster  # noqa: E402


def _ci(headline, hid="h1", ts=1000):
    return ClusterInput(
        headline_id=hid, headline=headline, url="http://x",
        publisher="Reuters", published_at_unix=ts,
    )


def test_enrich_populates_member_and_deduped_aggregate():
    c = Cluster(headline_ids=("h1", "h2"), members=(
        _ci("Company loses $2B in chips at sea", "h1", ts=2000),
        _ci("$2B loss confirmed as 40M barrels also lost", "h2", ts=1000),
    ))
    [e] = enrich_clusters_with_magnitudes([c])
    # per-headline populated
    assert any(m.raw_span == "$2B" for m in e.members[0].stated_magnitudes)
    # aggregate: $2B in both members -> once (deduped); 40M barrels once
    raws = [m.raw_span for m in e.stated_magnitudes]
    assert raws.count("$2B") == 1
    assert "40M barrels" in raws


def test_enrich_empty_for_magnitude_free_cluster():
    c = Cluster(headline_ids=("h1",), members=(_ci("Diplomats meet in Doha"),))
    [e] = enrich_clusters_with_magnitudes([c])
    assert e.stated_magnitudes == ()
    assert e.members[0].stated_magnitudes == ()


def test_format_cluster_renders_magnitude_line():
    c = Cluster(headline_ids=("h1",), members=(_ci("Company loses $2B"),))
    [e] = enrich_clusters_with_magnitudes([c])
    assert "[stated magnitudes: $2B]" in _format_cluster(e, 1)


def test_format_cluster_omits_line_when_none():
    c = Cluster(headline_ids=("h1",), members=(_ci("Diplomats meet in Doha"),))
    [e] = enrich_clusters_with_magnitudes([c])
    assert "stated magnitudes" not in _format_cluster(e, 1)


def test_dollar_to_dollar_does_not_misparse_to_as_trillion():
    # "$2 to $3" must yield two plain USD values, not a 't'(trillion) scale.
    mags = extract_magnitudes("priced between $2 to $3 per share")
    vals = sorted(m.value for m in mags)
    assert vals == [2.0, 3.0]
