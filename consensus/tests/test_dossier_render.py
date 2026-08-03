"""Renderer tests (M10-D §3.3 / §5.3): a rendered dossier is correct AND carries the
non-negotiable honesty constraints from §4."""

import json

import consensus.dossier_render as dr


def _row(**over):
    r = dict(
        dossier_id="abc123", wallet="0xW1", condition_id="0xM", token_id="0xT", side="BUY",
        market_question="Will X happen?", market_category="Politics",
        first_seen_ts=1_700_000_000, first_seen_source="activity",
        detection_ts=1_700_500_000, entry_vwap=0.42, price_at_detection=0.44,
        contested_notional=25_000.0, headline_notional=900_000.0,
        f_factor=1.0, s_factor=0.6, d_factor=0.58, c_factor=0.7, latency_factor=None,
        composite=0.71, tier="ELEVATED", tier_peak="ELEVATED", tier_peak_ts=1_700_500_000,
        cluster_wallets=json.dumps(["0xW1"]), actor_count_post_collapse=1,
        cross_market_cluster=None, funding_summary=json.dumps({"funder": "0xF"}),
        cex_class="dedicated", cex_confidence=0.9, resolved=0, winning_token=None,
        resolution_ts=None, outcome_for_side=None,
        provenance=json.dumps({"scan_id": "s1"}), label=None,
    )
    r.update(over)
    return r


def test_footer_is_mandatory_and_no_ev_language():
    md = dr.render_markdown(_row())
    assert "not a validated trade signal" in md
    assert "not an allegation" in md
    assert "no expected value is implied or computed" in md
    low = md.lower()
    for banned in ("expected value:", "ev =", "recommend", "you should buy", "take the bet"):
        assert banned not in low


def test_low_confidence_cex_renders_unclassified():
    # §4.5: a provisional class must never harden into an implied allegation
    md = dr.render_markdown(_row(cex_class="dedicated", cex_confidence=0.2))
    assert "**unclassified**" in md
    assert "CEX class: **dedicated**" not in md
    # and high confidence is allowed through
    assert "**dedicated**" in dr.render_markdown(_row(cex_class="dedicated", cex_confidence=0.9))


def test_render_cex_helper_boundary():
    assert dr.render_cex("dedicated", 0.9) == "dedicated"
    assert dr.render_cex("dedicated", 0.49) == "unclassified"
    assert dr.render_cex("dedicated", None) == "unclassified"
    assert dr.render_cex(None, 1.0) == "unclassified"


def test_mesh_collapse_is_stated_as_n_equals_one():
    md = dr.render_markdown(_row(
        cluster_wallets=json.dumps([f"0x{i}" for i in range(20)]),
        actor_count_post_collapse=1))
    assert "Raw wallets: **20**" in md
    assert "post-collapse actors: **1**" in md
    assert "n=1" in md            # the inversion stated explicitly


def test_contested_and_headline_notional_both_shown():
    md = dr.render_markdown(_row())
    assert "$25,000" in md and "$900,000" in md
    assert "Contested notional" in md and "Headline notional" in md


def test_missing_data_is_declared_never_imputed():
    md = dr.render_markdown(_row(first_seen_ts=None, first_seen_source="unavailable",
                                 latency_factor=None))
    assert "unavailable" in md and "Rule 1" in md
    assert "| —" in md or "— |" in md          # latency renders as em-dash, not 0


def test_tier_trajectory_not_retraction():
    md = dr.render_markdown(_row(tier="WATCH", tier_peak="CRITICAL"))
    assert "peaked at **CRITICAL**" in md and "now WATCH" in md


def test_resolution_rendered_when_stamped():
    md = dr.render_markdown(_row(resolved=1, winning_token="0xT", outcome_for_side=1,
                                 resolution_ts=1_701_000_000))
    assert "WON" in md
    assert "LOST" in dr.render_markdown(_row(resolved=1, outcome_for_side=0,
                                             resolution_ts=1_701_000_000))


def test_unresolved_cluster_is_never_rendered_as_n_actors():
    """The prohibited artifact: an uncollapsed 20-wallet mesh must NOT read as
    "20 coordinated actors" — it is indistinguishable from a verified 20-actor
    cluster, which is the Mojtaba overstatement the invariant exists to prevent."""
    md = dr.render_markdown(_row(cluster_wallets=json.dumps([f"0x{i}" for i in range(20)]),
                                 actor_count_post_collapse=None))
    assert "post-collapse actors: **UNRESOLVED**" in md
    assert "post-collapse actors: **20**" not in md
    assert "NOT n=20" in md


def test_unresolved_solo_still_reads_as_solo():
    md = dr.render_markdown(_row(cluster_wallets=json.dumps(["0xW1"]),
                                 actor_count_post_collapse=None))
    assert "solo footprint" in md and "UNRESOLVED" in md
