"""Ticker extraction tests — hermetic, no I/O beyond tmp files."""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from news_watch_daemon.scrape.ticker_extract import (
    TickerExtractError,
    TrackedTickers,
    load_tracked_tickers,
    log_tracked_ticker_match,
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


# ---------- Pass D follow-on: tracked-ticker false-positive instrumentation ----------
#
# `NOW` (ServiceNow, held conviction) collides with the channel-prefix word
# "NOW -" in @chainlinkbreadcrumbs. Removing NOW would lose all natural-
# language ServiceNow visibility; the daemon has no company-name fallback yet.
# Decision (2026-05-24): accept the noise, instrument it, scope Option E
# (entity-aware extraction reading tracked_entities.companies) post-Mac-mini.


def test_find_tracked_matches_returns_positions(seed_with_regex):
    """find_tracked_matches returns (ticker, start_pos) for each tracked-list match,
    in source order, including duplicates."""
    matches = seed_with_regex.find_tracked_matches(
        "MSTR rallied; later META also moved; MSTR closed up"
    )
    tickers = [t for t, _ in matches]
    positions = [p for _, p in matches]
    assert tickers == ["MSTR", "META", "MSTR"]   # order preserved, duplicates included
    assert positions == [0, 20, 37]
    # Position correctness sanity:
    text = "MSTR rallied; later META also moved; MSTR closed up"
    for ticker, pos in matches:
        assert text[pos : pos + len(ticker)] == ticker


def test_find_tracked_matches_excludes_cashtags(seed_with_regex):
    """Cashtag matches ($AAPL) are NOT in find_tracked_matches output —
    instrumentation should only fire on tracked-list path."""
    text = "MSTR rallied; trader posts $AAPL idea"
    matches = seed_with_regex.find_tracked_matches(text)
    tickers = [t for t, _ in matches]
    assert tickers == ["MSTR"]   # AAPL is cashtag-only, excluded
    # But extract() still returns AAPL — it pulls from both paths:
    assert "AAPL" in seed_with_regex.extract(text)


def test_find_tracked_matches_empty_inputs(seed_with_regex):
    """None / empty text / no tracked tickers configured → empty list."""
    assert seed_with_regex.find_tracked_matches(None) == []
    assert seed_with_regex.find_tracked_matches("") == []
    empty = TrackedTickers(conviction=(), watchlist=(), _regex=None)
    assert empty.find_tracked_matches("MSTR rallied") == []


def test_find_tracked_matches_now_in_breadcrumbs_prefix(seed_with_regex):
    """Regression: the breadcrumbs 'NOW -' prefix DOES match the tracked NOW.
    This is the known false-positive that the instrumentation is meant to
    measure — if this stops matching, the instrumentation is dead. The
    semantic fix (Option E company-name extraction) is deferred."""
    text = (
        "NOW - Blackrock's Larry Fink says he has \"relooked at his assumptions\""
        " about crypto"
    )
    matches = seed_with_regex.find_tracked_matches(text)
    tickers = [t for t, _ in matches]
    assert "NOW" in tickers


def test_log_tracked_ticker_match_emits_debug_with_context(seed_with_regex, caplog):
    """Emits DEBUG log with source_channel, headline_id, ticker, position,
    and surrounding 50-char context."""
    import logging
    text = (
        "NOW - Blackrock's Larry Fink says he has relooked at his assumptions"
        " about crypto"
    )
    # find the NOW match position from the real extractor (consistency)
    matches = seed_with_regex.find_tracked_matches(text)
    assert matches, "fixture: NOW must match"
    ticker, pos = matches[0]
    with caplog.at_level(logging.DEBUG, logger="news_watch_daemon.scrape.ticker_extract"):
        log_tracked_ticker_match(
            source_channel="telegram:chainlinkbreadcrumbs",
            headline_id="abc123",
            ticker=ticker,
            headline=text,
            match_position=pos,
        )
    matched = [
        r for r in caplog.records
        if "tracked_ticker_match" in r.getMessage()
    ]
    assert len(matched) == 1
    msg = matched[0].getMessage()
    assert "channel=telegram:chainlinkbreadcrumbs" in msg
    assert "headline_id=abc123" in msg
    assert "ticker=NOW" in msg
    assert f"match_pos={pos}" in msg
    # Context should include the channel-prefix "NOW -" giving operators
    # visible evidence of the false-positive pattern.
    assert "NOW -" in msg


def test_log_tracked_ticker_match_context_truncated_at_text_boundaries(caplog):
    """When the match is near the start or end of the text, the 50-char
    context clamps to the available range without raising."""
    import logging
    short_text = "NOW"
    with caplog.at_level(logging.DEBUG, logger="news_watch_daemon.scrape.ticker_extract"):
        log_tracked_ticker_match(
            source_channel="test_channel",
            headline_id="test_id",
            ticker="NOW",
            headline=short_text,
            match_position=0,
        )
    matched = [
        r for r in caplog.records
        if "tracked_ticker_match" in r.getMessage()
    ]
    assert len(matched) == 1
    msg = matched[0].getMessage()
    assert "context='NOW'" in msg   # full text is the context, no over-slice


def test_log_tracked_ticker_match_debug_silent_at_info_level(caplog):
    """DEBUG level: at default INFO logging, the message must NOT appear."""
    import logging
    with caplog.at_level(logging.INFO, logger="news_watch_daemon.scrape.ticker_extract"):
        log_tracked_ticker_match(
            source_channel="test",
            headline_id="test",
            ticker="MSTR",
            headline="MSTR rallied",
            match_position=0,
        )
    matched = [
        r for r in caplog.records
        if "tracked_ticker_match" in r.getMessage()
    ]
    assert len(matched) == 0  # DEBUG suppressed at INFO threshold
