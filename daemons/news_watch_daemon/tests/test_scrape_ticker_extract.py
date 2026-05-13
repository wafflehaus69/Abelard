"""Ticker extraction tests — hermetic, no I/O beyond tmp files."""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from news_watch_daemon.scrape.ticker_extract import (
    TickerExtractError,
    TrackedTickers,
    load_tracked_tickers,
)


# ---------- helpers ----------


def _write_yaml(tmp_path: Path, payload: dict, name: str = "tt.yaml") -> Path:
    path = tmp_path / name
    path.write_text(yaml.safe_dump(payload), encoding="utf-8")
    return path


@pytest.fixture
def seed_tickers() -> TrackedTickers:
    """The canonical seed set Mando shipped in config/tracked_tickers.yaml."""
    return TrackedTickers(
        conviction=("MSTR", "META", "COIN", "CRCL", "CRWV", "NOW", "ONDS", "TSLA", "LINK", "ETH", "BTC"),
        watchlist=("NTR", "NEU", "GLD", "MOG.A", "LHX", "CORN", "WEAT", "LIN", "CTRA", "GFS"),
        _regex=None,
    )


@pytest.fixture
def seed_with_regex(seed_tickers) -> TrackedTickers:
    """Same as seed_tickers but with the regex actually compiled."""
    return load_tracked_tickers(
        Path(__file__).resolve().parent.parent / "config" / "tracked_tickers.yaml"
    )


# ---------- conviction / watchlist match ----------


def test_conviction_ticker_match(seed_with_regex):
    assert seed_with_regex.extract("MSTR ramped up Bitcoin purchases") == ["MSTR"]


def test_watchlist_ticker_match(seed_with_regex):
    assert seed_with_regex.extract("Nutrien NTR posts earnings beat") == ["NTR"]


def test_multiple_tickers_in_one_headline(seed_with_regex):
    result = seed_with_regex.extract("META and MSTR rally on AI capex commentary")
    assert result == ["META", "MSTR"]


def test_no_match_returns_empty(seed_with_regex):
    assert seed_with_regex.extract("Iran tests new ballistic missile") == []


def test_empty_input(seed_with_regex):
    assert seed_with_regex.extract("") == []
    assert seed_with_regex.extract(None) == []


# ---------- word boundary correctness ----------


def test_eth_inside_ether_does_not_match(seed_with_regex):
    """ETH should not fire on 'ETHER' / 'ETHICS' etc."""
    assert seed_with_regex.extract("ETHER price action and ETHICS panel") == []


def test_now_inside_knowingly_does_not_match(seed_with_regex):
    """NOW is a common 3-letter sequence; must not match inside other words."""
    assert seed_with_regex.extract("knowingly accepted the proposal") == []


def test_btc_at_word_boundary_matches(seed_with_regex):
    assert seed_with_regex.extract("BTC closes flat") == ["BTC"]


def test_dotted_share_class_preserved(seed_with_regex):
    """MOG.A with the dot must match intact."""
    assert seed_with_regex.extract("MOG.A wins defense contract") == ["MOG.A"]


def test_dotted_ticker_dot_not_treated_as_regex(seed_with_regex):
    """The dot in MOG.A must be a literal dot, not regex wildcard.

    If the dot were unescaped, 'MOGXA' would match. Verify it doesn't.
    """
    assert "MOG.A" not in seed_with_regex.extract("MOGXA misprint here")


# ---------- cashtag pattern ----------


def test_cashtag_match_outside_tracked_list(seed_with_regex):
    """$-prefix pattern catches tickers even when not in the tracked list."""
    # AAPL is not in the seed list; cashtag should still find it.
    result = seed_with_regex.extract("Buying $AAPL on the dip")
    assert "AAPL" in result


def test_cashtag_with_share_class():
    """$BRK.B and similar share-class cashtags."""
    empty = TrackedTickers(conviction=(), watchlist=(), _regex=None)
    assert empty.extract("Long $BRK.B forever") == ["BRK.B"]


def test_cashtag_lowercase_ignored():
    """Cashtag pattern is uppercase-only by design — lowercase is noise."""
    empty = TrackedTickers(conviction=(), watchlist=(), _regex=None)
    assert empty.extract("posted $aapl by mistake") == []


def test_cashtag_must_be_at_least_one_letter():
    empty = TrackedTickers(conviction=(), watchlist=(), _regex=None)
    assert empty.extract("price is $1000 today") == []


# ---------- combined sources ----------


def test_cashtag_and_tracked_both_captured(seed_with_regex):
    """Headline mentions a tracked ticker AND a cashtag of another."""
    result = seed_with_regex.extract("MSTR raises stake; trader posts $AAPL idea")
    assert result == ["AAPL", "MSTR"]


def test_dedup_when_same_ticker_via_both_sources(seed_with_regex):
    """If MSTR appears both unprefixed and as $MSTR, return once."""
    result = seed_with_regex.extract("MSTR rallies; $MSTR conviction grows")
    assert result == ["MSTR"]


# ---------- load_tracked_tickers ----------


def test_load_valid_yaml(tmp_path):
    p = _write_yaml(tmp_path, {"conviction": ["MSTR", "META"], "watchlist": ["NTR"]})
    tt = load_tracked_tickers(p)
    assert tt.conviction == ("MSTR", "META")
    assert tt.watchlist == ("NTR",)
    assert tt.all == frozenset({"MSTR", "META", "NTR"})


def test_load_missing_file_fails_loud(tmp_path):
    with pytest.raises(TickerExtractError, match="not found"):
        load_tracked_tickers(tmp_path / "missing.yaml")


def test_load_invalid_yaml_fails_loud(tmp_path):
    bad = tmp_path / "bad.yaml"
    bad.write_text("not: valid: yaml: :", encoding="utf-8")
    with pytest.raises(TickerExtractError, match="invalid YAML"):
        load_tracked_tickers(bad)


def test_load_root_not_a_mapping_fails_loud(tmp_path):
    bad = tmp_path / "list.yaml"
    bad.write_text("- just\n- a list\n", encoding="utf-8")
    with pytest.raises(TickerExtractError, match="must be a mapping"):
        load_tracked_tickers(bad)


def test_load_empty_yaml_is_empty_config(tmp_path):
    empty = tmp_path / "empty.yaml"
    empty.write_text("", encoding="utf-8")
    tt = load_tracked_tickers(empty)
    assert tt.conviction == ()
    assert tt.watchlist == ()
    # No regex compiled when both lists empty.
    assert tt.extract("MSTR rallies") == []


def test_load_non_string_entry_rejected(tmp_path):
    p = _write_yaml(tmp_path, {"conviction": ["MSTR", 12345]})
    with pytest.raises(TickerExtractError, match="non-empty strings"):
        load_tracked_tickers(p)


def test_seed_config_file_loads_cleanly():
    """The bundled config/tracked_tickers.yaml must load without error."""
    path = Path(__file__).resolve().parent.parent / "config" / "tracked_tickers.yaml"
    tt = load_tracked_tickers(path)
    # Conviction must include the 11 Mando-supplied tickers
    assert {"MSTR", "META", "COIN", "CRCL", "CRWV", "NOW", "ONDS",
            "TSLA", "LINK", "ETH", "BTC"}.issubset(set(tt.conviction))
    # Watchlist must include the 10 Mando-supplied tickers
    assert {"NTR", "NEU", "GLD", "MOG.A", "LHX", "CORN", "WEAT",
            "LIN", "CTRA", "GFS"}.issubset(set(tt.watchlist))
