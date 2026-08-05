"""The unresolved chokepoint (consensus.resolution). Four fail-open defects shipped
because each consumer re-derived 'unresolved' for itself; these lock the single
definition and its fail-CLOSED direction."""

import consensus.resolution as r


def test_only_complete_scores_are_trusted():
    for t in ("NONE", "WATCH", "ELEVATED", "CRITICAL"):
        assert r.is_complete_score(t)
    # the detector's own refusal to tier must not read as a complete score
    assert not r.is_complete_score("INSUFFICIENT_DATA")
    assert r.is_complete_score(None)          # absent tier == NONE, a real (low) tier


def test_row_is_complete_requires_both_current_and_peak():
    assert r.row_is_complete({"tier": "WATCH", "tier_peak": "CRITICAL"})
    # the alert reads the PEAK, so an incomplete peak must disqualify the row
    assert not r.row_is_complete({"tier": "WATCH", "tier_peak": "INSUFFICIENT_DATA"})
    assert not r.row_is_complete({"tier": "INSUFFICIENT_DATA", "tier_peak": "NONE"})


def test_actor_count_never_substitutes_a_number():
    assert r.actor_count({"actor_count_post_collapse": 3}) == 3
    assert r.actor_count({"actor_count_post_collapse": None}) is None
    assert r.actor_count({}) is None          # absent is unresolved, not 1


def test_collapse_state_fails_closed_on_unknown():
    twenty = [f"0x{i}" for i in range(20)]
    # the Mojtaba shape: 20 wallets, one actor
    assert r.collapse_state({"actor_count_post_collapse": 1}, twenty) == "collapsed"
    assert r.collapse_state({"actor_count_post_collapse": 20}, twenty) == "independent"
    # unknown must NOT read as independent (that would assert 20 actors)
    assert r.collapse_state({"actor_count_post_collapse": None}, twenty) == "unresolved"
    # ...and not as solo either, even for a single wallet
    assert r.collapse_state({"actor_count_post_collapse": None}, ["0xa"]) == "unresolved"
    assert r.collapse_state({"actor_count_post_collapse": 1}, ["0xa"]) == "solo"


def test_fmt_never_emits_a_bare_none_or_a_guess():
    assert r.fmt(None) == r.UNRESOLVED
    assert r.fmt(4) == "4"
    assert "None" not in r.fmt(None)
