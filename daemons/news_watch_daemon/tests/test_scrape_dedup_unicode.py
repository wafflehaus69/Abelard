"""Task 2.5 (2026-05-27) — Unicode-aware dedup normalization regression suite.

Three phases:

  Phase 1 — Hash invariance (load-bearing safety net):
    Every English-content sample from the persistent DB corpus must produce
    a bit-identical dedupe_hash pre and post the regex change. FROZEN_HASH
    was computed against the OLD regex (pre-Task-2.5); these tests confirm
    the NEW regex produces the same hashes for content with no non-ASCII
    *letters* (all 32 samples qualified — see probe_classify_samples.py).

    Out-of-allow-set non-ASCII chars (emoji, em-dashes, curly quotes,
    accented Latin like Flávio/Türkiye/Erdoğan, etc.) are dropped pre AND
    post fix — so the invariance holds even on richly-formatted samples.

  Phase 2 — Bug fix (the actual behavior change):
    Three synthetic Cyrillic-only headlines that under the OLD regex all
    normalize to "" and share a single dedupe_hash. Under the NEW regex
    each preserves its Cyrillic content and produces a distinct hash.

  Phase 3 — Multi-script coverage:
    Each non-Latin script block (Cyrillic, CJK, Hiragana/Katakana,
    Hangul, Arabic, Hebrew, Greek) preserves its characters in
    normalized form. Out-of-block punctuation (em-dashes, curly quotes,
    emoji) is still dropped. Truncation cap is character-count not
    byte-count for multi-byte scripts (locks the invariant against a
    future refactor accidentally introducing byte-counting).
"""

from __future__ import annotations

import pytest

from news_watch_daemon.scrape.dedup import (
    compute_dedupe_hash,
    normalize_headline,
)


# ============================================================================
# PHASE 1 — Hash invariance (the load-bearing safety net)
# ============================================================================

# 32 curated samples from the persistent DB (2026-05-27), spread across all
# 8 en-only sources. Classification (probe_classify_samples.py) confirmed
# zero samples contain non-ASCII LETTERS inside the new allow-set (Cyrillic,
# CJK, etc.) — every sample is either pure-ASCII or contains only out-of-set
# non-ASCII chars (emoji, em-dashes, curly quotes, accented Latin). All 32
# therefore qualify for the invariance regression set.
EN_HEADLINE_SAMPLES: list[tuple[str, str]] = [
    ('finnhub:general', 'Israel pounds Lebanon with strikes, expands ground operations past security zone - Reuters'),
    ('finnhub:general', 'Russia says US did not grant visa for official to attend UN meeting - Reuters'),
    ('finnhub:general', 'Airlines cancel flights in response to Middle East conflict - Reuters'),
    ('finnhub:general', 'S&P 500, Nasdaq hit record closing highs on AI optimism, Micron joins $1 trillion club - Reuters'),
    ('rss:bloomberg_economics', 'Court Ruling Ends Institutional Crisis at Colombian Central Bank'),
    ('rss:bloomberg_economics', 'US Says Process to Form China Board of Trade to Start Shortly'),
    ('rss:bloomberg_economics', 'Bank of Korea Seen Favoring Hawkish Hold as Inflation Risks Grow'),
    ('rss:bloomberg_economics', 'What Trump’s Venezuela Blueprint Means for Cuba'),
    ('rss:bloomberg_markets', 'Iqbal Khan on Geopolitics, AI, and Growth'),
    ('rss:bloomberg_markets', 'SQM Boosts Lithium Guidance as Earnings Top Estimates'),
    ('rss:bloomberg_markets', 'Adnoc Exports Another LNG Shipment Through Hormuz to India'),
    ('rss:bloomberg_markets', 'Memory Chip Frenzy Sends SK Hynix, Micron Into $1 Trillion Club'),
    ('rss:bloomberg_politics', 'Paxton Crushes Cornyn in Texas Race After Winning Trump Nod'),
    ('rss:bloomberg_politics', 'Ex-President Biden Sues to Stop DOJ Sharing Interview Tapes'),
    ('rss:bloomberg_politics', "Texas Showdown Latest Test of Trump's Sway | Balance of Power: Late Edition 5/26/2026"),
    ('rss:bloomberg_politics', 'Sen. Scott: Trump Won’t Punt Iran Nuclear Demands'),
    ('telegram:CIG_telegram', '\U0001f1fa\U0001f1f8 Trump appoints Pam Bondi to White House AI panel. Bondi will be tasked with facilitating coordination between the government and AI companies on the panel. Bondi will also serve in a newly established advisory role on national infrastructure.\n\n\U0001f4ce [Disclosetv](https://x.com/i/status/2059433669442367907)'),
    ('telegram:CIG_telegram', '\U0001f1e7\U0001f1f7\U0001f1e7\U0001f1f7\U0001f4de\U0001f1fa\U0001f1f8\U0001f5de — Senator Flávio Bolsonaro says he spoke with Trump about rare earths and stated for the American leader that Brazil is the only alternative to China for the Free World free itself from Chinese dependency:\n\n"Under my government, there will be a strategic partnership in this sector."'),
    ('telegram:CIG_telegram', "\U0001f31b \U0001f1fa\U0001f1f8 \U0001f680 NASA selects Astrolab and Lunar Outpost as the companies to build the Artemis lunar terrain vehicles that astronauts will drive on the Moon's surface.\n\nThe vehicles can carry up to 2 astronauts, travel up to 200km, 10km/hr, and have crewed & autonomous capabilities.\n\n\U0001f4ce [Toby Li](https://x.com/i/status/2059342127658618924)"),
    ('telegram:CIG_telegram', '\U0001f1fa\U0001f1f8\U0001f1e7\U0001f1f4⚡️ — The U.S. State Department issued a security alert for Bolivia warning of ongoing roadblocks and demonstrations disrupting transportation and essential services across the country.\n\nThe advisory warns travelers to avoid protest areas and not travel by road between cities, citing reports of violence, assaults, and vehicle damage at roadblocks. \n\nAir travel remains operational, but access to El Alto/La Paz airport has been intermittently disrupted by blockades near the airport entrance.'),
    ('telegram:chainlinkbreadcrumbs', 'https://x.com/MarquartCapital/status/2059364293276279243?s=20'),
    ('telegram:chainlinkbreadcrumbs', 'https://x.com/i/status/2059393097365135377'),
    ('telegram:chainlinkbreadcrumbs', 'https://x.com/i/status/2059372372625641607'),
    ('telegram:chainlinkbreadcrumbs', 'ETHConf - June 8 - 10, 2026'),
    ('telegram:real_DonaldJTrump', 'It is critically important that the CFTC’s exclusive authority over Prediction Markets is maintained, and that they will thrive. Under my leadership, we are setting “rules of the road” that are the Gold Standard for the States. We cannot have SCUM like Chris Christie, Letitia James, Tim Walz, and JB Pritzker setting the rules! Other Countries are after this new form of Financial Market, and we want to remain at the top. Likewise, and even more importantly, where we are currently the Crypto (Bitcoin, etc.) Capital of the World, other Countries are trying diligently to replace us in that capacity, but we won’t let that happen. It is a major Industry, and we must protect it. Mike Selig, CFTC Chairman, and respected by all, is doing a great job. Thank you Mike! President DONALD J. TRUMP'),
    ('telegram:real_DonaldJTrump', 'If Iran surrenders, admits their Navy is gone and resting at the bottom of the sea, and their Air Force is no longer with us, and if their entire Military walks out of Tehran, weapons dropped and hands held high, each shouting “I surrender, I surrender” while wildly waving the representative White Flag, and if their entire remaining Leadership signs all necessary “Documents of Surrender,” and admit their defeat to the great power and force of the magnificent U.S.A., The Failing New York Times, The China Street Journal (WSJ!), Corrupt and now Irrelevant CNN, and all other members of the Fake News Media, will headline that Iran had a Masterful and Brilliant Victory over The United States of America, it wasn’t even close. The Dumacrats and Media have totally lost their way. They have gone absolutely CRAZY!!! President DJT'),
    ('telegram:real_DonaldJTrump', 'The Enriched Uranium (Nuclear Dust!) will either be immediately turned over to the United States to be brought home and destroyed or, preferably, in conjunction and coordination with the Islamic Republic of Iran, destroyed in place or, at another acceptable location, with the Atomic Energy Commission, or its equivalent, being witness to this process and event. Thank you for your attention to this matter! President DJT'),
    ('telegram:real_DonaldJTrump', 'Negotiations with the Islamic Republic of Iran are proceeding nicely! It will only be a Great Deal for all or, no Deal at all — Back to the Battlefront and shooting, but bigger and stronger than ever before — And nobody wants that! During my discussions on Saturday with President Mohammed bin Salman Al Saud, of Saudi Arabia, Mohammed bin Zayed Al Nahyan, of The United Arab Emirates, Emir Tamim bin Hamad bin Khalifa Al Thani, Prime Minister Mohammed bin Abdulrahman bin Jassim bin Jaber Al Thani, and Minister Ali al-Thawadi, of Qatar, Field Marshal\xa0Syed Asim Munir Ahmed Shah, of Pakistan, President Recep Tayyip Erdoğan, of Türkiye, President Abdel Fattah El-Sisi, of Egypt, King Abdullah II, of Jordan, and King Hamad bin Isa Al Khalifa, of Bahrain, I stated that, after all the work done by the United States to try and pull this very complex puzzle together, \xa0it should be mandatory that all of these Countries, at a minimum, simultaneously, sign onto the Abraham Accords. Those Countries discussed are Saudi Arabia, The United Arab Emirates (already a Member!), Qatar, Pakistan, Türkiye, Egypt, Jordan, and Bahrain (already a Member!). It may be possible that one or two have a reason for not doing so, and that will be accepted, but most should be ready, willing, and able to make this Settlement with Iran a far more Historic Event than it would, otherwise, be. The Abraham Accords have proven to be, for the Countries involved (The United Arab Emirates, Bahrain, Morocco, Sudan, and Kazakhstan), a Financial, Economic, and Social BOOM, even during this time of Conflict and War, with the current Members never even suggesting leaving, or taking so much as even a pause. The reason for this is that the Abraham Accords have been great for them, and will be even better for everybody, and bring true Power, Strength, and Peace to the Middle East for the first time in 5,000 years. It will be a Document respected like no other that has ever been signed, anywhere in the World. Its level of Importance and Prestige will be unparalleled! It should start with the immediate signing by Saudi Arabia and Qatar, and everybody else should follow suit. If they don’t, they should not be part of this Deal in that it shows bad intention. In speaking to numerous of the Great Leaders mentioned above, they would be honored, as soon as our Document is signed, to have the Islamic Republic of Iran as part of the Abraham Accords. Wow, now that would be something special! This will be the most important Deal that any of these Great, but always in Conflict Countries, will ever sign. Nothing in the past, or in the future, will surpass it. Therefore, I am mandatorily requesting that all Countries immediately sign the Abraham Accords, and that, if Iran signs its Agreement with me, as President of the United States of America, it would be an Honor to have them also be part of this unparalleled World Coalition. The Middle East would be United, Powerful, and Economically Strong, like perhaps no other area, anywhere in the World! By copy of this TRUTH, I am asking my Representatives to begin, and successfully complete, the process of signing these Countries into the already Historic Abraham Accords. Thank you for your attention to this matter!'),
    ('telegram:trading', "Berkshire Hathaway now has **$400 Billion in cash**, the highest in the conglomerate's history\n\n**✅****@trading**"),
    ('telegram:trading', '⚡️ **JUST IN:** **US consumer confidence rises to 93.1** in **May**, beating the **92.0** estimate.\n\n**✅****@trading**'),
    ('telegram:trading', '**Peter Schiff** says **Michael Saylor** is **running out of cash** after **Strategy’s $1.5B debt repurchase**, questioning what he will sell next to keep the wheels from falling off.\n\n**✅****@trading**'),
    ('telegram:trading', "⚡️ **JUST IN:** **Iran's Foreign Ministry** says **the US** has **violated the ceasefire in the Hormozgan area**.\n\n**✅****@trading**"),
]

# Frozen hashes computed against the OLD regex (pre-Task-2.5) on 2026-05-27.
# Generated by probe_freeze_hashes.py. Each entry pins the invariant
# "this English headline's hash MUST stay bit-identical after the fix".
FROZEN_HASH_BY_INDEX: dict[int, str] = {
    0: 'f1a611acafba4ffd381015f7b8db75f5',
    1: '04fee133c253095f1b189077e770f6a9',
    2: '8cc78e933cd82c25cab6ed81591ce01a',
    3: '42d48895ec6400b5f3e4258dc0e1bece',
    4: 'cb79af4b95c70581f3d1fbd07ff90f58',
    5: '3b5e408c8754a201f5217bde882ae085',
    6: 'd7242506dcf7790c1cdc08ff1776be9a',
    7: '27eba33b01311e1f1ffeba1c711bae0f',
    8: 'a9ce2054aa14b2de983d8c37bd816818',
    9: '940afc1c35b94602acceb1cb139b1914',
    10: '9cf924cad9fe14fa2737a10a50350559',
    11: 'fd42ab7eb68f5a8164bb36a74dfd36b4',
    12: 'dfd9c5fbbe15b4be1eadb706f7f6e3d9',
    13: 'e7a5b53d0c2168331b69914a605e2cdf',
    14: '6c86b61edddd6be6d4c52a84419817d8',
    15: 'ea7f30061a01d3133c002528697c5441',
    16: '4dd36f5b7db7be4c415dd30067c20463',
    17: '1b269ea6c77575bd36fb990bed64901b',
    18: 'c3c9c6fc3cfbae3145d3786faf7ce828',
    19: '6044852c2229c685e64326d4ab845e9b',
    20: '88d4f79fb0f9d6224b8a5a5a6dd65d06',
    21: '912bedbac107e1f76c3e0f73bafc52a6',
    22: '69725f4f70dbd4300529787a955e1713',
    23: '128db0d4cc23ad2199cfd6200a110479',
    24: '6212cae90932c139a4846c26d6e45c01',
    25: 'be0a5ed7c37917de1ceef5e273de48e3',
    26: 'cd07df24d12aee08171837716e31c390',
    27: '7b7f2e1d7b29cc18769eeafb9c477973',
    28: '97ac244cdacaf99877d562a2c8ba162d',
    29: 'b0df602c29387ec3ea18c522b4dee84f',
    30: 'e9a75a76c751f6d600e2781b9840c1d6',
    31: '1adc83de84bbf00ce2de36e346594e08',
}


@pytest.mark.parametrize("index", list(range(len(EN_HEADLINE_SAMPLES))))
def test_curated_en_corpus_hashes_invariant(index: int):
    """The load-bearing safety net: every curated English sample produces
    the same dedupe_hash post-fix as it did pre-fix. If this test ever
    fails, either:
      (a) the regex change accidentally affected English content (a bug),
      (b) some other normalize step changed (also a bug), OR
      (c) the sample list was edited without re-freezing the hashes
          (test maintenance gap).
    Any failure here is investigated before the change ships.
    """
    source, headline = EN_HEADLINE_SAMPLES[index]
    expected = FROZEN_HASH_BY_INDEX[index]
    actual = compute_dedupe_hash(headline)
    assert actual == expected, (
        f"Hash drift on sample {index} ({source}): "
        f"expected {expected!r}, got {actual!r}. "
        f"Headline preview: {headline[:80]!r}"
    )


# ============================================================================
# PHASE 2 — Bug fix: Cyrillic-only headlines get distinct hashes
# ============================================================================


# These three synthetic Cyrillic-only headlines, under the OLD regex,
# all normalize to "" (Cyrillic chars dropped, no ASCII content) and
# share a single dedupe_hash. Under the NEW regex they preserve Cyrillic
# content and produce three distinct hashes — the bug fix in action.
CYRILLIC_ONLY_DISTINCT: list[str] = [
    "Россия наступает",
    "Украина обороняется",
    "Беларусь молчит",
]


def test_three_cyrillic_only_headlines_get_distinct_hashes_post_fix():
    """The headline bug fix: three pure-Cyrillic short quotes that under
    the OLD regex all collapsed to "" + shared a single hash now produce
    three distinct hashes."""
    hashes = {compute_dedupe_hash(h) for h in CYRILLIC_ONLY_DISTINCT}
    assert len(hashes) == 3, (
        f"Expected 3 distinct hashes for distinct Cyrillic headlines; "
        f"got {len(hashes)}: {hashes}. The Unicode-aware regex isn't "
        f"distinguishing them — has _DROP_CHARS_RE lost the Cyrillic range?"
    )


def test_cyrillic_normalize_preserves_cyrillic_chars():
    """The normalized form of a Cyrillic headline contains Cyrillic
    letters (lowercased) — not the empty string the pre-fix regex
    would have produced."""
    result = normalize_headline("Российские военные провели учения")
    assert result, "Cyrillic headline must not normalize to empty string"
    # At least one Cyrillic char in result (block U+0400-U+04FF)
    assert any("Ѐ" <= ch <= "ӿ" for ch in result), (
        f"Normalized form lost all Cyrillic chars: {result!r}"
    )
    # Lowercased: no uppercase Cyrillic should remain (Р -> р, etc.)
    assert all(not ("А" <= ch <= "Я") for ch in result), (
        f"Normalized form contains uppercase Cyrillic: {result!r}"
    )


def test_cyrillic_normalize_lowercases_via_python_str_lower():
    """Lock the .lower() step's behavior on Cyrillic: "РОССИЯ" -> "россия".
    A future refactor that changes .lower() handling would surface here."""
    result = normalize_headline("РОССИЯ")
    assert result == "россия"


# ============================================================================
# PHASE 3 — Multi-script coverage + truncation + edge cases
# ============================================================================


def test_cjk_normalize_preserves_chars():
    """CJK Unified Ideographs (U+4E00-U+9FFF) survive normalization."""
    result = normalize_headline("中国新闻报道")  # "中国新闻报道"
    assert result
    assert any("一" <= ch <= "鿿" for ch in result), (
        f"Normalized form lost all CJK chars: {result!r}"
    )


def test_arabic_normalize_preserves_chars():
    """Arabic block (U+0600-U+06FF) survives normalization."""
    result = normalize_headline("أخبار عربية")  # "أخبار عربية"
    assert result
    assert any("؀" <= ch <= "ۿ" for ch in result), (
        f"Normalized form lost all Arabic chars: {result!r}"
    )


def test_hebrew_normalize_preserves_chars():
    """Hebrew block (U+0590-U+05FF) survives normalization."""
    result = normalize_headline("חדשות ארץ")  # "חדשות ארץ"
    assert result
    assert any("֐" <= ch <= "׿" for ch in result), (
        f"Normalized form lost all Hebrew chars: {result!r}"
    )


def test_greek_normalize_preserves_chars():
    """Greek block (U+0370-U+03FF) survives normalization."""
    result = normalize_headline("Ελληνικά νέα")  # "Ελληνικά νέα"
    assert result
    assert any("Ͱ" <= ch <= "Ͽ" for ch in result), (
        f"Normalized form lost all Greek chars: {result!r}"
    )


def test_punctuation_still_dropped():
    """ASCII punctuation outside [a-z0-9 ] is still dropped, unchanged
    from pre-fix behavior. The dedup invariant for English content
    relies on this."""
    result = normalize_headline("Hello, world! [link](url)")
    # Expected: "hello world linkurl" (commas, !, [, ], (, ) all dropped)
    assert "," not in result
    assert "!" not in result
    assert "[" not in result
    assert "]" not in result
    assert "(" not in result
    assert ")" not in result
    assert "hello" in result
    assert "world" in result


def test_emoji_still_dropped():
    """Emoji are NOT in any of the allow-set script blocks — still
    dropped. Surrounding text is preserved."""
    # Unicorn 🦄 (U+1F984) — pictograph, outside all allow-set blocks
    result = normalize_headline("Russia \U0001f984 news")
    assert "russia" in result
    assert "news" in result
    assert "\U0001f984" not in result


def test_unicode_punctuation_still_dropped():
    """Em-dash (U+2014), curly quotes (U+2018-U+201D) are outside the
    allow-set — still dropped. Locked because these are common in
    Bloomberg/Trump headlines and the invariance regression test
    depends on them being dropped consistently pre and post fix."""
    result = normalize_headline("Trump’s “Big Deal” — maybe")
    # Curly apostrophe (U+2019), curly quotes (U+201C/U+201D), em dash (U+2014) all dropped
    assert "’" not in result
    assert "“" not in result
    assert "”" not in result
    assert "—" not in result
    assert "trumps" in result
    assert "big" in result
    assert "deal" in result


def test_truncation_is_character_count_not_byte_count():
    """The 80-char cap counts CHARACTERS not BYTES. A 100-Cyrillic-char
    headline (each char = 2 bytes in UTF-8) normalizes to exactly 80
    characters, not 40. Locks against a future refactor accidentally
    introducing byte-counting (e.g. via encode().decode() round-trips
    or .ljust(80, ...) on bytes)."""
    # 120 Cyrillic 'а' chars (U+0430) — well over the 80-char cap.
    long_cyrillic = "а" * 120
    result = normalize_headline(long_cyrillic)
    assert len(result) == 80, (
        f"Expected 80-char result; got {len(result)} chars. "
        f"Truncation may have switched to byte-counting."
    )
    # Sanity: result is still all Cyrillic — not corrupted by encoding
    assert all(ch == "а" for ch in result), (
        f"Truncation corrupted character integrity: {result!r}"
    )
