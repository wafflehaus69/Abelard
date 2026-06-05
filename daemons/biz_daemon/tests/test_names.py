"""S&P 500 name resolution: name->ticker, case/word boundaries, pruning."""

from __future__ import annotations

import pytest

from biz_daemon import extractor
from biz_daemon.config import _default_sp500_names_path

NAME_MAP = extractor.load_name_map(_default_sp500_names_path())
RESOLVER = extractor.build_name_resolver(NAME_MAP)

UNIVERSE = frozenset(
    {"NVDA", "AMZN", "MSFT", "HD", "JNJ", "KO", "GM", "GPS", "TGT", "AAPL"}
)


def _names(text, universe=UNIVERSE):
    return RESOLVER.tickers_in(text, universe)


def _extract_one(com, universe=UNIVERSE):
    return extractor.extract(
        [{"no": 1, "com": com}], universe=universe, blacklist=frozenset(),
        name_resolver=RESOLVER,
    )


# --- resolution --------------------------------------------------------------


def test_name_resolves_to_ticker():
    assert _names("I just loaded up on Nvidia") == {"NVDA"}


def test_case_insensitive():
    for variant in ("nvidia", "NVIDIA", "NvIdIa"):
        assert _names(f"buying {variant} today") == {"NVDA"}


def test_multiword_name():
    assert _names("went to home depot") == {"HD"}
    assert _names("homedepot") == set()  # not a real phrase boundary


def test_punctuated_names():
    assert _names("johnson & johnson is defensive") == {"JNJ"}
    assert _names("coca-cola dividend") == {"KO"}


# --- whole-word boundary -----------------------------------------------------


def test_whole_word_boundary():
    assert _names("amazonian rainforest") == set()  # not Amazon
    assert _names("amazon prime") == {"AMZN"}


# --- universe gating ---------------------------------------------------------


def test_name_requires_universe_membership():
    assert _names("Nvidia", universe=frozenset({"AMZN"})) == set()  # NVDA absent


# --- pruned collisions -------------------------------------------------------


def test_pruned_collision_words_absent_from_map():
    for word in [
        "gap", "target", "block", "match", "host", "union", "public",
        "service", "best", "first", "general", "american", "apple",
        "oracle", "intel", "visa", "ford", "uber", "delta",
    ]:
        assert word not in NAME_MAP, f"{word!r} must be pruned from the name map"


def test_pruned_words_do_not_resolve_in_prose():
    # GPS=Gap, TGT=Target, AAPL=Apple are in the universe but the NAMES are
    # pruned, so prose mentions must not resolve.
    assert _names("mind the gap on the platform") == set()
    assert _names("hit the target price") == set()
    assert _names("an apple a day") == set()


# --- fold into the same mention count ---------------------------------------


def test_name_and_symbol_count_once():
    table = _extract_one("Nvidia is mooning, $NVDA NVDA NVDA")
    assert table["NVDA"].mention_count == 1  # one post, counted once
    assert table["NVDA"].post_ids == {1}


def test_name_alone_creates_a_mention():
    table = _extract_one("microsoft earnings tonight", universe=frozenset({"MSFT"}))
    assert "MSFT" in table
    assert table["MSFT"].mention_count == 1
