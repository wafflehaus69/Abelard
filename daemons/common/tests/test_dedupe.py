"""Shared dedupe — normalization, hashing, and HASH INVARIANCE across the hoist.

The invariance tests are the point. This module's character class determines
every stored dedupe_hash in every consuming daemon; if the hoist changed even
one byte of it, old rows would stop colliding with new ones, duplicates would
reappear days later, and nothing would raise. The vectors below are pinned
against the pre-hoist implementations.
"""

from __future__ import annotations

import hashlib

import pytest

from abelard_common.dedupe import compute_dedupe_hash, normalize_text


# ---------------------------------------------------------------------------
# HASH INVARIANCE -- pinned to the pre-hoist output
# ---------------------------------------------------------------------------
# Produced by the SINGLE pre-hoist implementation --
# news_watch_daemon/scrape/dedup.py at commit ded7ff3, the file this hoist
# deletes. Scout never had its own copy (it has identity.py, deliberately not
# hoisted), so there was no second implementation to agree with; an earlier
# version of this comment claimed one and was wrong.
#
# Cross-checked 2026-08-12 against 2,578 live rows in Basilic's production
# news-watch.db: every stored headlines.dedupe_hash reproduces from the hoisted
# module under Python 3.14.6, 1,302 of them non-ASCII, zero mismatches.
#
# If any of these change, the hoist has altered stored-hash semantics and every
# consuming ledger is wrong.
_PINNED_PRE_HOIST: tuple[tuple[str, str], ...] = (
    ("", "e3b0c44298fc1c149afbf4c8996fb924"),
    ("Write a Blog Post!", "7fa1dc6b761a8fcaa2ba229c5261d506"),
    ("  MIXED   Case  ", "938c14299c2810673b77d4a3a05ef9c7"),
    ("SEC Chairman Eyes 'Gun-Jumping' Rule Changes",
     "2b7c75ea37dde3b4e28e042c6220dcf2"),
    ("Россия подарила Казахстану тигров", "6eeabcb040393062380db9410a0acd92"),
    ("日本語のタイトル", "348774f35fa91a21ee2d20e7d78f48f1"),
    ("한국어 제목", "031137b46e45f724e22306c48468063d"),
    ("مرحبا بالعالم", "9262a0a791605071a500c1a15bef2d5e"),
    ("Ελληνικά", "8ab1661503fc902d0457fd921164d9ed"),
    ("Flávio Türkiye Erdoğan", "fa19bec9b2eed68f9831f0437a0a0cbc"),
    ("HENKAKU community-growth Event", "4c73638ccc692cf97bf03d95cee91906"),
    ("a" * 200, "0f45e858fbc4176cdf4e411f88281ede"),
    ("tab\tand\nnewline", "825a91a47c8402c90b0f52f618681f56"),
    ("emoji 🌴 title", "014f0dc28f8e1879482a24c597e25f65"),
)


@pytest.mark.parametrize("text,digest", _PINNED_PRE_HOIST)
def test_hash_is_pinned_to_the_pre_hoist_digest(text, digest) -> None:
    """The external anchor. These literals were produced by the DELETED
    news_watch scrape/dedup.py as it stands at git HEAD -- not by the module
    under test -- so a search-and-replace that edits the character class in
    both dedupe.py and _reference() below still fails here.

    The empty-string vector is independently checkable: SHA256("")[:32].
    """
    assert compute_dedupe_hash(text) == digest


def _reference(text: str) -> str:
    """The frozen algorithm, written out independently of the module."""
    import re

    ws = re.compile(r"\s+")
    drop = re.compile(
        r"[^a-z0-9 Ѐ-ӿ一-鿿぀-ゟ゠-ヿ"
        r"가-힯؀-ۿ֐-׿Ͱ-Ͽ]"
    )
    lowered = text.lower() if text is not None else ""
    collapsed = ws.sub(" ", drop.sub("", ws.sub(" ", lowered))).strip()[:80]
    return hashlib.sha256(collapsed.encode("utf-8")).hexdigest()[:32]


@pytest.mark.parametrize("text", [
    "",
    "Write a Blog Post!",
    "  MIXED   Case  ",
    "SEC Chairman Eyes 'Gun-Jumping' Rule Changes",
    "Россия подарила Казахстану тигров",
    "日本語のタイトル",
    "한국어 제목",
    "مرحبا بالعالم",
    "Ελληνικά",
    "Flávio Türkiye Erdoğan",
    "HENKAKU community-growth Event",
    "a" * 200,
    "tab\tand\nnewline",
    "emoji 🌴 title",
])
def test_hash_matches_the_frozen_algorithm(text) -> None:
    assert compute_dedupe_hash(text) == _reference(text)


def test_empty_and_none_agree() -> None:
    """Both pre-hoist implementations returned '' for None."""
    assert normalize_text(None) == ""
    assert compute_dedupe_hash(None) == compute_dedupe_hash("")
    assert compute_dedupe_hash(None) == "e3b0c44298fc1c149afbf4c8996fb924"


# ---------------------------------------------------------------------------
# Normalization behaviour
# ---------------------------------------------------------------------------

def test_cosmetic_variants_collapse() -> None:
    assert compute_dedupe_hash("Write a Blog Post!") == compute_dedupe_hash(
        "  write a   blog post  "
    )


def test_distinct_text_does_not_collide() -> None:
    assert compute_dedupe_hash("alpha") != compute_dedupe_hash("beta")


def test_truncates_at_eighty_characters() -> None:
    base = "x" * 80
    assert normalize_text(base + "differing tail") == base


def test_latin_extended_is_dropped_not_folded() -> None:
    """Intentional: preserving prior behaviour beats handling accents well."""
    assert normalize_text("Flávio") == "flvio"
    assert normalize_text("Türkiye") == "trkiye"


@pytest.mark.parametrize("text,expected_nonempty", [
    ("Россия", True),      # Cyrillic
    ("日本語", True),        # CJK
    ("ひらがな", True),       # Hiragana
    ("한국어", True),         # Hangul
    ("مرحبا", True),        # Arabic
    ("שלום", True),         # Hebrew
    ("Ελληνικά", True),     # Greek
    ("🌴🌴🌴", False),        # emoji dropped entirely
])
def test_script_blocks_survive_normalization(text, expected_nonempty) -> None:
    assert bool(normalize_text(text)) is expected_nonempty


def test_hash_is_32_hex_chars() -> None:
    value = compute_dedupe_hash("anything")
    assert len(value) == 32
    assert all(c in "0123456789abcdef" for c in value)


def test_normalization_is_idempotent() -> None:
    once = normalize_text("  Mixed CASE and   spacing!  ")
    assert normalize_text(once) == once
