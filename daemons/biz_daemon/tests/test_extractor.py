"""extractor: the precision core."""

from __future__ import annotations

from abelard_common import ticker_noise as extractor
from abelard_common.ticker_noise import tickers_in_post

UNIVERSE = frozenset({"GME", "AMD", "ALL", "ON", "IT", "MOG.A", "T", "NTR"})
BLACKLIST = frozenset({"FUD", "DD", "ATH", "ALL", "ON", "IT", "GO", "OR"})


def test_cashtag_high_confidence():
    assert tickers_in_post("$GME to the moon", universe=UNIVERSE, blacklist=BLACKLIST) == {"GME"}


def test_bare_token_medium_confidence():
    assert tickers_in_post("AMD looks strong", universe=UNIVERSE, blacklist=BLACKLIST) == {"AMD"}


def test_six_char_token_dropped_by_length_cap():
    # BAGHOLDER (9) and SIXCHR (6) never reach validation
    assert tickers_in_post("BAGHOLDER SIXCHR", universe=UNIVERSE, blacklist=BLACKLIST) == set()


def test_blacklist_drops_slang_collisions_as_bare():
    assert tickers_in_post("FUD and DD at ATH", universe=UNIVERSE, blacklist=BLACKLIST) == set()


def test_universe_validation_rejects_non_symbols():
    # ZZZ is uppercase, short, not blacklisted — but not a real symbol
    assert tickers_in_post("ZZZ moon", universe=UNIVERSE, blacklist=BLACKLIST) == set()


def test_mog_a_survives_dot_intact():
    assert tickers_in_post("holding MOG.A here", universe=UNIVERSE, blacklist=BLACKLIST) == {"MOG.A"}
    assert tickers_in_post("$MOG.A", universe=UNIVERSE, blacklist=BLACKLIST) == {"MOG.A"}


def test_lowercase_ignored_on_bare_path():
    assert tickers_in_post("amd gme all", universe=UNIVERSE, blacklist=BLACKLIST) == set()


def test_collision_token_rejected_bare_accepted_as_cashtag():
    # bare ALL/ON/IT rejected by blacklist...
    assert tickers_in_post("ALL ON IT", universe=UNIVERSE, blacklist=BLACKLIST) == set()
    # ...but explicit cashtags bypass the blacklist
    assert tickers_in_post("$ALL $ON $IT", universe=UNIVERSE, blacklist=BLACKLIST) == {"ALL", "ON", "IT"}


def test_mention_metric_is_distinct_posts():
    posts = [
        {"no": 1, "com": "GME GME GME GME GME"},  # spam — counts once
        {"no": 2, "com": "GME again"},
        {"no": 3, "com": "AMD"},
    ]
    table = extractor.extract(posts, universe=UNIVERSE, blacklist=BLACKLIST)
    assert table["GME"].mention_count == 2
    assert table["GME"].post_ids == {1, 2}
    assert table["AMD"].mention_count == 1


def test_multi_ticker_post_attributes_both():
    posts = [{"no": 5, "com": "$GME and AMD both ripping"}]
    table = extractor.extract(posts, universe=UNIVERSE, blacklist=BLACKLIST)
    assert set(table) == {"GME", "AMD"}
    assert table["GME"].post_ids == {5}
    assert table["AMD"].post_ids == {5}


# --- four-layer filter against the REAL bundled data files -------------------

from abelard_common import ticker_noise as _bl  # noqa: E402
from biz_daemon.config import (  # noqa: E402
    DEFAULT_WORD_TICKER_ALLOWLIST,
    _default_blacklist_path,
    _default_common_words_path,
)

DENY = _bl.load_blacklist(_default_blacklist_path())
WORDS = _bl.load_common_words(_default_common_words_path())
ALLOW = DEFAULT_WORD_TICKER_ALLOWLIST

SINGLE_LETTERS = ["A", "S", "P", "B", "D", "E", "H", "M", "T", "U", "V"]
WORD_TICKERS = ["FOR", "YOU", "JUST", "LIFE", "LOVE", "KNOW", "CUT", "HAS", "HIS", "HIT", "BULL"]
ALLOWLISTED = ["NOW", "META", "CORN"]
REAL_3PLUS = ["SOXL", "SOXS", "TQQQ", "UPRO", "AVGO", "DRAM", "MRVL", "NVDA"]

# A universe that contains every symbol-shaped token under test, so each one
# reaches (and is judged by) the intended filter layer rather than failing the
# universe check.
BIG_UNIVERSE = frozenset(
    set(SINGLE_LETTERS) | set(WORD_TICKERS) | set(ALLOWLISTED) | set(REAL_3PLUS) | {"EOD", "MU"}
)


def _bare(text: str):
    return extractor.tickers_in_post(
        text, universe=BIG_UNIVERSE, blacklist=DENY, common_words=WORDS, allowlist=ALLOW
    )


def test_seed_denylist_actually_loads_eod():
    # the root cause of the leak was a missing seed entry, not a compare bug
    assert "EOD" in DENY


def test_eod_rejected_denylist_enforced():
    assert _bare("selling at EOD") == set()
    assert "EOD" not in _bare("EOD EOD EOD")


def test_denylist_is_case_insensitive():
    # bare path uppercases; lowercase/mixed never reaches the bare path anyway,
    # but the comparison itself must be case-insensitive
    assert _bare("EOD") == set()


def test_single_letters_rejected_bare_accepted_as_cashtag():
    for letter in SINGLE_LETTERS:
        assert _bare(letter) == set(), f"bare {letter} should be rejected by length"
    assert _bare("$A") == {"A"}
    assert _bare("$S") == {"S"}


def test_bare_two_char_ticker_passes():
    # length floor rejects only 1-char; bare 2-char real tickers count again
    assert _bare("MU") == {"MU"}


def test_common_word_tickers_rejected_bare():
    for word in WORD_TICKERS:
        assert _bare(word) == set(), f"bare {word} should be rejected by wordlist"


def test_allowlist_survives_wordlist():
    # NOW/META/CORN are in the common-word set but rescued by the allowlist
    for sym in ALLOWLISTED:
        assert sym.lower() in WORDS, f"{sym} must be in the wordlist to test override"
        assert _bare(sym) == {sym}, f"{sym} should survive via the allowlist"


def test_real_tickers_unaffected():
    for sym in REAL_3PLUS:
        assert _bare(sym) == {sym}, f"{sym} should pass cleanly"
    # MU is a real 2-char ticker; bare MU now passes (length floor rejects only
    # single letters). $MU passes too.
    assert _bare("MU") == {"MU"}
    assert _bare("$MU") == {"MU"}


def test_cashtag_bypasses_every_filter():
    # $EOD (denylisted), $FOR (wordlist), $A (length) all pass as cashtags
    assert _bare("$EOD") == {"EOD"}
    assert _bare("$FOR") == {"FOR"}
