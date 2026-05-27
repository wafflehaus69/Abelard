"""Language classifier tests (Task 2 — 2026-05-27).

The classifier is pure / deterministic / no I/O; tests are trivially
hermetic. Coverage:

  - Pure-script cases (Russian / English / mixed) at realistic message
    sizes
  - Boundary semantics at the 0.50 / 0.20 thresholds (exact / just-above
    / just-below) — locked behavior that Pass F's translation gate
    depends on
  - Edge cases yielding `en` by design (empty / whitespace / emoji-only
    / URL-only / punctuation-only / digits-only)
  - Ukrainian / Belarusian Cyrillic letters classify as `ru` (the
    Cyrillic block detection covers the full Slavic alphabet range, not
    just modern Russian А-Я / а-я)
  - Non-Cyrillic / non-Latin scripts (CJK, Greek) currently classify
    as `en` because they fall into the no-alphabetic-chars branch —
    this is locked behavior and the test documents WHY a future
    "other" class would change it
"""

from __future__ import annotations

import pytest

from news_watch_daemon.lang import LANGUAGES, classify_language


# ---------- pure script ----------


def test_pure_russian_classifies_as_ru():
    """Real Ateobreaking-shape Russian headline → ru."""
    text = (
        "Американские военные сейчас не сопровождают коммерческие суда "
        "через Ормузский пролив, заявили в Пентагоне."
    )
    assert classify_language(text) == "ru"


def test_pure_english_classifies_as_en():
    """Real CIG_telegram-shape English headline → en."""
    text = (
        "The U.S. tried to re-colonize part of the Philippines under the "
        "so-called Pax Silica initiative, the brainchild of an ex-Palantir "
        "executive now running State Department economic diplomacy."
    )
    assert classify_language(text) == "en"


def test_mixed_script_classifies_as_mixed():
    """Roughly-equal Cyrillic + Latin (e.g. Russian quoting an English brand)."""
    # 30 Cyrillic chars + 30 Latin chars = cyr_ratio 0.50... wait, 0.50
    # exactly is `ru`. Tune to mid-band: 25 cyr + 75 lat → ratio 0.25 → mixed.
    text = "Россия Россия Россия Россия Россия " + ("a" * 75)
    # Count: "Россия " is 6 Cyrillic + 1 space = 6 cyr per copy × 5 = 30 cyr;
    # but I want 25 cyr / 75 lat. Use explicit construction:
    text = ("Р" * 25) + ("a" * 75)
    assert classify_language(text) == "mixed"


# ---------- boundary semantics ----------


def test_boundary_cyr_ratio_exactly_50pct_classifies_as_ru():
    """cyr_ratio == 0.50 falls in the `ru` bucket (>= 0.50, inclusive)."""
    text = ("Р" * 5) + ("a" * 5)  # ratio = 0.5000
    assert classify_language(text) == "ru"


def test_boundary_cyr_ratio_just_below_50pct_classifies_as_mixed():
    """cyr_ratio just below 0.50 → mixed."""
    text = ("Р" * 49) + ("a" * 51)  # ratio = 0.49
    assert classify_language(text) == "mixed"


def test_boundary_cyr_ratio_exactly_20pct_classifies_as_en():
    """cyr_ratio == 0.20 falls in the `en` bucket (<= 0.20, inclusive,
    equivalent to lat_ratio >= 0.80)."""
    text = ("Р" * 1) + ("a" * 4)  # ratio = 0.20
    assert classify_language(text) == "en"


def test_boundary_cyr_ratio_just_above_20pct_classifies_as_mixed():
    """cyr_ratio just above 0.20 → mixed (the (0.20, 0.50) open interval)."""
    text = ("Р" * 21) + ("a" * 79)  # ratio = 0.21
    assert classify_language(text) == "mixed"


# ---------- edge cases yielding `en` by design ----------


@pytest.mark.parametrize("text", [
    None,                                  # None input
    "",                                    # empty string
    "   ",                                 # whitespace only
    "🦄🦄🦄",                              # emoji only
    "https://t.me/Ateobreaking/170758",    # URL only
    "!!!???...",                           # punctuation only
    "12345 67890",                         # digits only
    "🇬🇧🇩🇪🇪🇸🇫🇷🇮🇹",                          # flag emoji only
])
def test_zero_alphabetic_chars_classifies_as_en(text):
    """All zero-alphabetic-char inputs → `en` (operational default: the
    Pass F gate is `WHERE language != 'en'`, so `en` correctly skips
    translation — no translatable content lost)."""
    assert classify_language(text) == "en"


def test_no_special_branch_for_short_russian_text():
    """A 2-char Russian message classifies as `ru`, not `en`.

    Locks the design decision against a `<10 chars` short-circuit branch
    — Pass F's translation logic decides whether tiny text is worth a
    translation call. The classifier just honestly reports the script
    composition.
    """
    assert classify_language("Да") == "ru"


def test_no_special_branch_for_short_english_text():
    """Two Latin chars → en (already covered by ratio, but documents the
    parallel to the short-Russian test)."""
    assert classify_language("Hi") == "en"


# ---------- script-block coverage ----------


def test_ukrainian_letters_classify_as_ru():
    """Ukrainian-specific Cyrillic letters (Ї, І, Є, Ґ) live in the same
    U+0400-U+04FF block, so Ukrainian content also flags `ru`. Pass F
    behaviour is the same — non-English content goes to translation."""
    text = "Київ підтримує оборону, повідомили в Генштабі"  # Ukrainian
    assert classify_language(text) == "ru"


def test_belarusian_letter_classifies_as_ru():
    """Belarusian Ў is in the Cyrillic block; classified as `ru`."""
    text = "Беларускі ўрад абвясціў"  # Belarusian (note Ў)
    assert classify_language(text) == "ru"


def test_cjk_currently_classifies_as_en_documented_limitation():
    """CJK characters fall outside both Cyrillic and Latin counts, so
    currently classify as `en`. This is a known limitation: when CJK
    corpora become operationally relevant, the classifier learns a
    third script class and the `Language` Literal expands to include
    `other`. Until then, Chinese / Japanese / Korean headlines would
    incorrectly skip Pass F translation. The Ateobreaking + tracked-
    source corpus has zero CJK content as of 2026-05-27.
    """
    text = "中国新闻报道"  # "Chinese news report" in Chinese
    assert classify_language(text) == "en"


def test_greek_currently_classifies_as_en_documented_limitation():
    """Greek letters (U+0370-U+03FF) are also outside Cyrillic and Latin
    counts. Same `other` reservation as CJK applies."""
    text = "Ελληνικά νέα"  # "Greek news"
    assert classify_language(text) == "en"


# ---------- public-API surface ----------


def test_languages_tuple_matches_literal_members():
    """LANGUAGES tuple is the canonical set of values the classifier can
    return. If the Literal expands (e.g. adding `other`), this constant
    must expand too."""
    assert set(LANGUAGES) == {"ru", "en", "mixed"}


@pytest.mark.parametrize("text", [
    "Hello world",
    "Привет мир",
    ("Р" * 30) + ("a" * 70),
    "",
])
def test_classifier_return_value_always_in_languages_tuple(text):
    """The classifier never returns a value outside LANGUAGES — locks
    the closed-Literal invariant."""
    assert classify_language(text) in LANGUAGES
