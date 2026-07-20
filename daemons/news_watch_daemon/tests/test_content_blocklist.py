"""Editorial content-blocklist tests — unconditional drop of forbidden terms,
without over-matching adjective/lookalike usage."""

from __future__ import annotations

import pytest

from news_watch_daemon.scrape.content_blocklist import classify_blocklist


# ---------- forbidden terms → dropped (unconditional) ----------


@pytest.mark.parametrize("headline", [
    "The Kalergi Plan explains mass migration, channel claims",
    "Talmudic law cited in viral post",                      # inflection: Talmud -> Talmudic
    "Protocols of the Elders of Zion resurface in meme",
    "White genocide narrative spreads on the channel",
    "ZOG controls the banks, poster says",
])
def test_forbidden_terms_are_dropped(headline):
    assert classify_blocklist(headline) is not None


def test_blocklist_is_unconditional_even_with_real_signal():
    # Unlike the sports filter, there is NO geo guard: a real markets/geo token
    # in the same headline does NOT rescue it.
    assert classify_blocklist(
        "Iran sanctions and the Kalergi Plan drive the channel's narrative"
    ) is not None


def test_real_geopolitical_terms_pass_through():
    # Mando 2026-07-15: real named frameworks / geopolitical concepts are NOT
    # blocked even when they sound loaded — only fabricated conspiracies are.
    assert classify_blocklist(
        "Netanyahu ally invokes Greater Israel map at settlement rally"
    ) is None
    assert classify_blocklist(
        "Isaac Accords framework floated for Saudi-Israel normalization"
    ) is None
    assert classify_blocklist("Greater Judea land policy debated in Knesset") is None
    assert classify_blocklist("Abraham Accords expansion talks resume") is None


# ---------- must NOT drop (lookalikes / legit) ----------


@pytest.mark.parametrize("headline", [
    "A greater Israeli role in Gaza reconstruction expected",   # adjective, not ideology
    "Greater Manchester economy grows 3%",                      # 'Greater' + other place
    "Abraham Accords expansion talks resume with Saudi Arabia",  # NOT 'Isaac Accords'
    "Zogby poll shows Trump approval steady",                   # NOT 'ZOG'
    "Fed holds rates as inflation cools",
    "General Dynamics wins submarine production contract",
])
def test_lookalikes_and_legit_are_kept(headline):
    assert classify_blocklist(headline) is None


# ---------- edges ----------


def test_empty_and_none():
    assert classify_blocklist(None) is None
    assert classify_blocklist("") is None


def test_returns_matched_term():
    assert classify_blocklist("Discussion of the Kalergi Plan trends") == "Kalergi Plan"
    # case-insensitive
    assert classify_blocklist("the kalergi agenda talking point") == "kalergi"
