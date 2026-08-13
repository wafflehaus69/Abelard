"""Dashboard export (M10-Dash §4.1). The dashboard is the FIFTH consumer of
"unresolved"; §3.6 requires it route through the chokepoint rather than re-deriving it."""
import json
import sqlite3

import consensus.dossier_export as dx
import consensus.dossier_store as ds


def _rec(**over):
    r = dict(wallet="0xW", condition_id="0xM", token_id="0xT", side="BUY",
             market_question="Q?", market_category="politics", event_slug="e",
             first_seen_ts=1_000_000, first_seen_source="activity", detection_ts=1_100_000,
             entry_vwap=0.42, price_at_detection=None, contested_notional=None,
             headline_notional=25_000.0, f_factor=1.0, s_factor=0.6, d_factor=0.6,
             c_factor=0.7, latency_factor=None, composite=0.9, tier="CRITICAL",
             cluster_id=None, cluster_wallets=["0xW"], actor_count_post_collapse=1,
             cross_market_cluster=None, funding_summary=None, cex_class=None,
             cex_confidence=None, provenance={"s": 1})
    r.update(over)
    return r


def test_blocks_math_matches_the_m0b_power_table():
    # M0-B: 155 blocks detects a 10pp effect, 618 detects 5pp (k=2.486, sigma=0.50)
    assert dx.blocks_for_mde(0.10) == 155
    assert dx.blocks_for_mde(0.05) == 618


def test_projection_counts_market_blocks_not_rows():
    """Three footprints in ONE market is one block, not three — the M0-C error the
    projection must not repeat, or the dashboard would overstate progress 3x."""
    con = ds.connect(":memory:")
    for w in ("0xA", "0xB", "0xC"):
        ds.upsert(con, _rec(wallet=w, condition_id="0xSAME"), scan_ts=1)
    ds.backfill_resolutions(con, lambda cid: ("0xT", 1_500_000))
    out = dx.build_export(con, now_ts=2_000_000)
    assert out["totals"]["resolved_dossiers"] == 3
    assert out["totals"]["resolved_blocks"] == 1


def test_unresolved_actor_count_survives_export_uncollapsed():
    """§3.6: unresolved must reach the page as unresolved, never as the raw count."""
    con = ds.connect(":memory:")
    ds.upsert(con, _rec(cluster_wallets=[f"0x{i}" for i in range(20)],
                        actor_count_post_collapse=None), scan_ts=1)
    card = dx.build_export(con, now_ts=2)["recent"][0]
    assert card["actor_count"] is None
    assert card["collapse_state"] == "unresolved"
    assert card["raw_wallets"] == 20          # raw is shown, but never AS the actor count


def test_export_routes_through_the_chokepoint_not_a_local_copy():
    """The four prior fail-open defects each came from a consumer re-deriving this."""
    src = open(dx.__file__, encoding="utf-8").read()
    assert "from . import resolution as _res" in src
    for banned in ("or n_raw", "or 1)", 'actor_count_post_collapse") or'):
        assert banned not in src, f"export re-derives unresolved via {banned!r}"


def test_contested_and_headline_stay_separate():
    con = ds.connect(":memory:")
    ds.upsert(con, _rec(contested_notional=None, headline_notional=50_000.0), scan_ts=1)
    card = dx.build_export(con, now_ts=2)["recent"][0]
    assert card["headline_notional"] == 50_000.0
    assert card["contested_notional"] is None   # unknown, never copied from headline


def test_a_not_measured_factor_is_never_rendered_as_zero():
    """85% of live cards have F unmeasured (the wallet was not gated for enrichment).
    An empty bar is visually identical to a zero bar, so the template must render a
    null factor as an explicit not-measured placeholder — otherwise the page repeats,
    in pixels, the imputed-freshness error the scoring path was fixed to stop making."""
    tpl = open(dx._TEMPLATE, encoding="utf-8").read()
    assert "not measured" in tpl
    assert "v == null" in tpl, "template does not branch on an unmeasured factor"
    # the contested column must say why it is empty rather than showing a bare dash
    assert "c.contested_notional == null" in tpl


def test_outcome_column_cannot_read_as_a_hit_rate():
    """Live review finding: Panel A showed 3 wins / 0 losses — the only outcome evidence
    on the page — from ONE correlated event, all at entry ~0.998 (the carry band, where
    winning is the base rate), all tiered NONE, with no price column to reveal it. That
    is the flattering-GO in visual form. Every verdict must carry its entry price, and
    the column must state it is not a hit rate."""
    tpl = open(dx._TEMPLATE, encoding="utf-8").read()
    assert "not a hit rate" in tpl
    assert "carry band" in tpl, "a win at 0.998 must be labelled as carry, not skill"
    assert "c.entry_vwap" in tpl, "verdicts render without their entry price"
    assert "recency sample" in tpl


def test_entry_vwap_is_exported_so_the_page_can_show_it():
    con = ds.connect(":memory:")
    ds.upsert(con, _rec(entry_vwap=0.998), scan_ts=1)
    assert dx.build_export(con, now_ts=2)["recent"][0]["entry_vwap"] == 0.998
