"""Dedup hash and normalization tests."""

from __future__ import annotations

import re

from abelard_common.dedupe import compute_dedupe_hash, normalize_text


# ---------- normalization ----------


def test_normalize_lowercases():
    assert normalize_text("Iran Tests Missile") == "iran tests missile"


def test_normalize_collapses_whitespace():
    assert normalize_text("Iran   tests\n\tmissile") == "iran tests missile"


def test_normalize_strips_punctuation():
    assert normalize_text("Iran's missile, fired today!") == "irans missile fired today"


def test_normalize_drops_special_characters():
    assert normalize_text("U.S.–Iran tensions ↑") == "usiran tensions"


def test_normalize_strips_leading_trailing_whitespace():
    assert normalize_text("   hi   ") == "hi"


def test_normalize_truncates_to_80_chars():
    long = "a" * 200
    assert len(normalize_text(long)) == 80


def test_normalize_handles_empty():
    assert normalize_text("") == ""


def test_normalize_handles_only_punctuation():
    assert normalize_text("!@#$%^&*()") == ""


# ---------- hash ----------


def test_hash_is_32_hex_chars():
    h = compute_dedupe_hash("Iran tests missile")
    assert re.fullmatch(r"[0-9a-f]{32}", h)


def test_hash_is_stable():
    a = compute_dedupe_hash("Iran tests missile")
    b = compute_dedupe_hash("Iran tests missile")
    assert a == b


def test_hash_collides_on_case_difference():
    a = compute_dedupe_hash("Iran tests missile")
    b = compute_dedupe_hash("IRAN tests missile")
    assert a == b


def test_hash_collides_on_punctuation_difference():
    a = compute_dedupe_hash("Iran tests missile.")
    b = compute_dedupe_hash("Iran tests missile!")
    assert a == b


def test_hash_collides_on_whitespace_difference():
    a = compute_dedupe_hash("Iran tests missile")
    b = compute_dedupe_hash("Iran   tests\tmissile")
    assert a == b


def test_hash_distinguishes_different_stories():
    a = compute_dedupe_hash("Iran tests missile")
    b = compute_dedupe_hash("Saudi Arabia holds talks")
    assert a != b


def test_hash_empty_string_does_not_crash():
    h = compute_dedupe_hash("")
    assert re.fullmatch(r"[0-9a-f]{32}", h)
