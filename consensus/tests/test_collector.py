"""M1.5 collector: lanes, tiers, overlap walks, gap declaration, envelope."""

from __future__ import annotations

import json

import pytest

from consensus.collector import Collector, _GLOBAL_WATERMARK_KEY
from consensus.tape import TapeStore
from tests.conftest import make_loaded
from consensus.fetching import build_data_layer

_TRADES_URL = "https://data-api.polymarket.com/trades"
_EVENTS_URL = "https://gamma-api.polymarket.com/events"
_MARKETS_URL = "https://gamma-api.polymarket.com/markets"


@pytest.fixture
def collector(tmp_path):
    loaded = make_loaded(tmp_path)
    dl = build_data_layer(loaded)
    tape = TapeStore(tmp_path / "tape.db")
    c = Collector(dl, tape)
    yield c
    tape.close()
    dl.cache.close()


def _fill(i: int, *, cid: str = "0xCID", ts: int | None = None):
    return {
        "proxyWallet": f"0x{i:040x}", "side": "BUY", "asset": f"a{i}",
        "conditionId": cid, "size": 1.0, "price": 0.5,
        "timestamp": ts if ts is not None else 1000 + i,
        "transactionHash": f"0xtx{i}", "slug": "m",
    }


def _event(cid: str = "0xCID", *, closed: bool = False):
    return {"title": "E?", "markets": [
        {"conditionId": cid, "slug": "m", "question": "Q?", "closed": closed,
         "endDate": "2026-12-31T00:00:00Z"},
    ]}


# -- enumeration --------------------------------------------------------------


def test_enumeration_adopts_markets(collector, requests_mock):
    requests_mock.get(_EVENTS_URL, json=[_event("0xNEW")])
    r = collector.run_enumeration(now_ts=1_000_000)
    assert r["markets_added"] == 1
    assert collector.tape.markets()[0]["condition_id"] == "0xNEW"
    assert collector.enumeration_due(1_000_000 + 60) is False
    assert collector.enumeration_due(1_000_000 + 31 * 60) is True


def test_enumeration_adjudicates_strays(collector, requests_mock):
    collector.tape.record_stray("0xSTRAY", now_ts=1, fills=5)
    requests_mock.get(_EVENTS_URL, json=[])
    requests_mock.get(_MARKETS_URL, json=[
        {"conditionId": "0xSTRAY", "slug": "s", "question": "Q?",
         "category": "Geopolitics", "closed": False},
    ])
    r = collector.run_enumeration(now_ts=1_000_000)
    assert r["strays_adopted"] == 1
    assert collector.tape.unresolved_strays() == []
    assert any(m["condition_id"] == "0xSTRAY" for m in collector.tape.markets())


# -- market lane ----------------------------------------------------------------


def _track(collector, cid="0xCID"):
    collector.tape.upsert_market(cid, slug="m", question="Q?", tags="geopolitics",
                                 source="enumeration", now_ts=1)
    return collector.tape.markets()[0]


def test_market_poll_bootstrap_short_history(collector, requests_mock):
    mkt = _track(collector)
    requests_mock.get(_TRADES_URL, json=[_fill(0), _fill(1)])
    r = collector.poll_market(mkt, now_ts=2_000)
    assert r["new"] == 2 and r["gap"] is False and r["pages"] == 1
    assert collector.tape.stats()["gaps_declared"] == 0


def test_market_poll_stops_on_overlap(collector, requests_mock):
    mkt = _track(collector)
    page_a = [_fill(0), _fill(1)]
    requests_mock.get(_TRADES_URL, json=page_a)
    collector.poll_market(mkt, now_ts=2_000)
    # Next poll: one new fill on top; page contains overlap -> stops at page 1.
    m2 = requests_mock.get(_TRADES_URL, json=[_fill(2, ts=1005)] + page_a)
    mkt = collector.tape.markets()[0]
    r = collector.poll_market(mkt, now_ts=2_120)
    assert r["new"] == 1 and r["pages"] == 1 and r["gap"] is False
    assert m2.call_count == 1


# -- dedup skip fast-path (Basilic 2026-07-19 throughput fix) ------------------


def test_store_page_skips_below_threshold_keeps_band(collector):
    """store_page contract: a record strictly below skip_below_ts is elided
    (presumed already stored — pure redundant work), while a record at/above it
    still INSERT-OR-IGNOREs so a late-indexed fill in the band is captured."""
    from consensus.models import Trade
    tape = collector.tape
    poll = tape.open_poll(invoked_ts=1, lane="market", condition_id="0xCID")
    base = 1_000_000_000
    deep = _fill(1, cid="0xCID", ts=base - 10_000)   # below -> skipped
    band = _fill(2, cid="0xCID", ts=base + 10)        # at/above -> stored
    counts = tape.store_page([deep, band], lane="market", poll_id=poll,
                             parsed_by=Trade.from_api, skip_below_ts=base)
    assert counts["presumed_stored"] == 1 and counts["new"] == 1
    stored = {r[0] for r in tape._conn.execute(
        "SELECT timestamp FROM l2_trades WHERE parse_ok=1").fetchall()}
    assert (base - 10_000) not in stored and (base + 10) in stored


def test_poll_market_skips_stored_deep_dupes(collector, requests_mock):
    """A caught-up market re-fetches its newest page each poll. Already-stored
    fills older than (frontier − margin) are elided instead of re-IGNORE'd;
    the genuinely-new top fill is still captured and continuity holds."""
    mkt = _track(collector)
    now = 2_000_000_000
    margin_s = collector.cfg.late_arrival_margin_minutes * 60
    first = [_fill(0, ts=now), _fill(1, ts=now - 1000),
             _fill(2, ts=now - margin_s - 500)]  # last is below (frontier − margin)
    requests_mock.get(_TRADES_URL, json=first)
    collector.poll_market(mkt, now_ts=now + 1)
    assert collector.tape.stats()["fills"] == 3
    mkt = collector.tape.markets()[0]
    requests_mock.get(_TRADES_URL, json=[_fill(9, ts=now + 5)] + first)
    r = collector.poll_market(mkt, now_ts=now + 120)
    assert r["presumed_stored"] == 1  # the deep, already-stored fill elided
    assert r["new"] == 1              # only the new top fill inserted
    assert r["gap"] is False          # continuity intact despite the skip
    assert collector.tape.stats()["fills"] == 4


def test_poll_market_captures_late_arrival_in_margin_band(collector, requests_mock):
    """The non-negotiable: a fill data-api surfaces LATE (timestamp just under
    the frontier, within the margin) must still be captured — the skip elides
    only records OLDER than frontier − margin, never the near-frontier band."""
    mkt = _track(collector)
    now = 1_000_000_000
    margin_s = collector.cfg.late_arrival_margin_minutes * 60
    requests_mock.get(_TRADES_URL, json=[_fill(0, ts=now - 5), _fill(1, ts=now)])
    collector.poll_market(mkt, now_ts=now + 1)
    fills_before = collector.tape.stats()["fills"]
    mkt = collector.tape.markets()[0]
    late = _fill(99, cid="0xCID", ts=now - margin_s // 2)  # within the margin band
    requests_mock.get(_TRADES_URL, json=[_fill(2, ts=now + 10), late, _fill(1, ts=now)])
    r = collector.poll_market(mkt, now_ts=now + 120)
    # Both the above-frontier fill and the late in-band fill are captured.
    assert collector.tape.stats()["fills"] == fills_before + 2
    assert r["gap"] is False


def test_errored_poll_does_not_advance_frontier_onto_global_fill(collector, requests_mock):
    """Adversarial-audit regression (2026-07-19, CONFIRMED data-loss): a pages==0
    errored market poll must NOT advance the frontier onto a fill the GLOBAL lane
    stored above it. The old code advanced newest_fill_ts to a lane-agnostic
    MAX(timestamp); the frontier jumped across an un-walked interval with no gap,
    and the next poll's dedup skip then silently elided that hole."""
    mkt = _track(collector)
    F = 1_772_000_000
    requests_mock.get(_TRADES_URL, json=[_fill(0, ts=F)])
    collector.poll_market(mkt, now_ts=F + 10)
    assert collector.tape.markets()[0]["newest_fill_ts"] == F
    # Global lane deposits a fresh fill for this market well above the frontier.
    requests_mock.get(_TRADES_URL, json=[_fill(9, cid="0xCID", ts=F + 100_000)])
    collector.poll_global(F + 20, {"0xCID"})
    assert collector.tape.newest_fill_ts("0xCID") == F + 100_000  # it IS on the tape
    # Market-lane poll errors on the first fetch: nothing walked (pages==0).
    requests_mock.get(_TRADES_URL, status_code=500)
    mkt = collector.tape.markets()[0]
    r = collector.poll_market(mkt, now_ts=F + 30)
    assert r["error"] is not None
    # Frontier must NOT have jumped to the global fill; it stays at F so the next
    # poll re-walks (F, now] rather than skipping it.
    assert collector.tape.markets()[0]["newest_fill_ts"] == F


def test_corrupt_future_timestamp_does_not_poison_frontier(collector, requests_mock):
    """Re-audit 2026-07-19 (CONFIRMED): a corrupt far-future timestamp (e.g. a
    millisecond value) must NOT advance the MAX-monotonic frontier. If it did,
    every later walk would trip reached_frontier on page 0 (truncating bursts)
    and the skip band (clamped to now) would elide real fills during a poll gap."""
    mkt = _track(collector)
    now = 1_772_000_000
    # A real fill plus one corrupt ms-scale record in the same page.
    requests_mock.get(_TRADES_URL, json=[_fill(1, ts=now), _fill(99, ts=now * 1000)])
    collector.poll_market(mkt, now_ts=now + 5)
    fr = collector.tape.markets()[0]["newest_fill_ts"]
    assert fr == now, f"frontier must track the real fill, not the corrupt future ts (got {fr})"


def test_corrupt_low_timestamp_does_not_trip_false_continuity(collector, requests_mock):
    """Re-audit 2026-07-19 (CONFIRMED, data-loss): a corrupt LOW ts (e.g. a
    zeroed field) on page 0 must NOT pull page_oldest below the frontier and
    falsely trip reached_frontier — that would stop the walk mid-burst and
    silently drop the deeper pages with no gap declared."""
    mkt = _track(collector)
    requests_mock.get(_TRADES_URL, json=[_fill(0, ts=1000)])
    collector.poll_market(mkt, now_ts=1_772_000_000)
    assert collector.tape.markets()[0]["newest_fill_ts"] == 1000
    P = collector.cfg.page_size
    # Full page-0 burst all ABOVE the frontier, plus one corrupt ts=0 record.
    page0 = [_fill(1000 + i, ts=5000 + i) for i in range(P - 1)] + [_fill(9999, ts=0)]
    # page-1 carries a fill that exists ONLY here; a false stop at page 0 loses it.
    page1 = [_fill(20000, ts=1500), _fill(20001, ts=999)]
    requests_mock.get(_TRADES_URL, [{"json": page0}, {"json": page1}])
    mkt = collector.tape.markets()[0]
    r = collector.poll_market(mkt, now_ts=1_772_000_100)
    assert r["pages"] == 2, "walk must not stop at page 0 on a corrupt low ts"
    stored = collector.tape._conn.execute(
        "SELECT COUNT(*) FROM l2_trades WHERE timestamp = 1500").fetchone()[0]
    assert stored == 1, "page-1 fill must be captured, not silently dropped"


def test_presumed_stored_persisted_in_l2_polls(collector, requests_mock):
    """The dedup skip count is persisted per-poll (observability parity with the
    other record dispositions), not only aggregated in the run envelope."""
    mkt = _track(collector)
    now = 2_000_000_000
    margin_s = collector.cfg.late_arrival_margin_minutes * 60
    requests_mock.get(_TRADES_URL, json=[_fill(0, ts=now), _fill(1, ts=now - margin_s - 500)])
    collector.poll_market(mkt, now_ts=now + 1)
    mkt = collector.tape.markets()[0]
    requests_mock.get(_TRADES_URL, json=[_fill(9, ts=now + 5), _fill(1, ts=now - margin_s - 500)])
    collector.poll_market(mkt, now_ts=now + 120)
    row = collector.tape._conn.execute(
        "SELECT presumed_records FROM l2_polls WHERE lane='market' ORDER BY id DESC LIMIT 1"
    ).fetchone()
    assert row[0] == 1  # the one deep already-stored fill was skipped and recorded


def test_market_poll_declares_gap_when_window_rolled(collector, requests_mock):
    """Prior tape exists, but a full max-page walk finds no stored fill ->
    the market's window rolled past us. The interval is DECLARED, not bridged."""
    mkt = _track(collector)
    requests_mock.get(_TRADES_URL, json=[_fill(0), _fill(1)])
    collector.poll_market(mkt, now_ts=2_000)

    # Simulate a huge burst: 4 full pages of fills we've never seen.
    cfg = collector.cfg
    pages = [
        {"json": [_fill(10_000 + p * cfg.page_size + i, ts=50_000 + p * cfg.page_size + i)
                  for i in range(cfg.page_size)]}
        for p in range(cfg.max_pages)
    ]
    requests_mock.get(_TRADES_URL, pages)
    mkt = collector.tape.markets()[0]
    r = collector.poll_market(mkt, now_ts=3_000)
    assert r["gap"] is True and r["pages"] == cfg.max_pages
    gaps = collector.tape._conn.execute(
        "SELECT lane, condition_id, reason FROM l2_gaps"
    ).fetchall()
    assert gaps and gaps[0][0] == "market" and gaps[0][1] == "0xCID"
    assert "rolled" in gaps[0][2]


def test_market_poll_bootstrap_truncation_declares_gap(collector, requests_mock):
    """A market adopted with >4k existing fills: its pre-window history is
    unreachable from data-api -> declared bootstrap gap (belongs to L1/L3)."""
    mkt = _track(collector)
    cfg = collector.cfg
    pages = [
        {"json": [_fill(p * cfg.page_size + i) for i in range(cfg.page_size)]}
        for p in range(cfg.max_pages)
    ]
    requests_mock.get(_TRADES_URL, pages)
    r = collector.poll_market(mkt, now_ts=2_000)
    assert r["gap"] is True
    reason = collector.tape._conn.execute("SELECT reason FROM l2_gaps").fetchone()[0]
    assert "bootstrap" in reason


def test_market_poll_error_is_recorded_not_raised(collector, requests_mock):
    mkt = _track(collector)
    requests_mock.get(_TRADES_URL, status_code=500)
    r = collector.poll_market(mkt, now_ts=2_000)
    assert r["error"] is not None and r["new"] == 0


# -- tiers -----------------------------------------------------------------------


def test_tier_transitions(collector, requests_mock):
    mkt = _track(collector)
    t = collector.cfg.tiers
    # Big page of new fills -> hot.
    requests_mock.get(_TRADES_URL, json=[_fill(i, ts=999_000 + i) for i in range(60)])
    r = collector.poll_market(mkt, now_ts=1_000_000)
    assert r["tier"] == "hot"
    # Hot TTL expired, newest fill still recent (<24h) -> quiet.
    mkt = collector.tape.markets()[0]
    requests_mock.get(_TRADES_URL, json=[_fill(0, ts=999_000)])
    r = collector.poll_market(mkt, now_ts=1_000_000 + t.hot_ttl_minutes * 60 + 1)
    assert r["tier"] == "quiet"
    # Once the newest fill is older than the quiet window -> dormant.
    mkt = collector.tape.markets()[0]
    requests_mock.get(_TRADES_URL, json=[_fill(0, ts=999_000)])
    r = collector.poll_market(mkt, now_ts=999_059 + (t.quiet_if_fill_within_hours + 1) * 3600)
    assert r["tier"] == "dormant"


def test_market_due_respects_tier_intervals(collector):
    t = collector.cfg.tiers
    base = {"condition_id": "0xC", "last_polled_ts": 10_000}
    assert collector.market_due({**base, "tier": "hot"}, 10_000 + t.hot_interval_minutes * 60) is True
    assert collector.market_due({**base, "tier": "hot"}, 10_000 + 30) is False
    assert collector.market_due({**base, "tier": "dormant"},
                                10_000 + t.quiet_interval_minutes * 60) is False


# -- global lane -------------------------------------------------------------------


def test_global_lane_promotes_and_records_strays(collector, requests_mock):
    _track(collector, "0xCID")
    requests_mock.get(_TRADES_URL, json=[_fill(0, cid="0xCID"), _fill(1, cid="0xELSE")])
    r = collector.poll_global(1_000_000, {"0xCID"})
    assert r["promoted"] == 1 and r["strays_seen"] == 1
    assert r["first_run"] is True and r["gap"] is False
    assert collector.tape.markets()[0]["tier"] == "hot"
    assert collector.tape.unresolved_strays() == ["0xELSE"]
    # Tracked fill persisted; stray fill skipped.
    assert collector.tape.stats()["fills"] == 1


def test_global_lane_overlap_via_watermark(collector, requests_mock):
    requests_mock.get(_TRADES_URL, json=[_fill(0), _fill(1)])
    collector.poll_global(1_000, set())
    assert collector.tape.get_meta(_GLOBAL_WATERMARK_KEY) is not None
    # Next poll sees one new fill + a watermarked one -> continuity, no gap.
    requests_mock.get(_TRADES_URL, json=[_fill(2, ts=1005), _fill(0)])
    r = collector.poll_global(1_120, set())
    assert r["gap"] is False and r["pages"] == 1


def test_global_lane_declares_gap_when_window_rolled(collector, requests_mock):
    requests_mock.get(_TRADES_URL, json=[_fill(0), _fill(1)])
    collector.poll_global(1_000, set())
    cfg = collector.cfg
    pages = [
        {"json": [_fill(20_000 + p * cfg.page_size + i, ts=60_000 + i)
                  for i in range(cfg.page_size)]}
        for p in range(cfg.max_pages)
    ]
    requests_mock.get(_TRADES_URL, pages)
    r = collector.poll_global(2_000, set())
    assert r["gap"] is True
    reason = collector.tape._conn.execute("SELECT reason FROM l2_gaps").fetchone()[0]
    assert "global window rolled" in reason


# -- review-hardened behaviors (adversarial findings, 2026-07-13) --------------------


def test_global_lane_prestored_fills_do_not_fake_continuity(collector, requests_mock):
    """THE reproduced high-severity finding: fills stored by the global lane
    moments earlier must not terminate the market walk — continuity means
    reaching the market's pre-walk FRONTIER, not hitting any stored fill."""
    mkt = _track(collector)
    # Market-lane poll establishes frontier at ts=1001.
    requests_mock.get(_TRADES_URL, json=[_fill(0, ts=1000), _fill(1, ts=1001)])
    collector.poll_market(mkt, now_ts=2_000)
    # Burst: global lane stores the 5 newest fills of this market.
    requests_mock.get(_TRADES_URL, json=[_fill(100 + i, ts=5_000 - i) for i in range(5)])
    collector.poll_global(2_060, {"0xCID"})
    # Market walk: page 0 = those same 5 fills (collisions!) + more new ones,
    # down to ts 3000 — frontier (1001) NOT reached, page full -> must keep
    # walking to page 1, which bridges down to the frontier.
    cfg = collector.cfg
    page0 = [_fill(100 + i, ts=5_000 - i) for i in range(5)] + [
        _fill(200 + i, ts=4_900 - i) for i in range(cfg.page_size - 5)
    ]
    page1 = [_fill(300, ts=1001), _fill(301, ts=1000)]
    m = requests_mock.get(_TRADES_URL, [{"json": page0}, {"json": page1}])
    mkt = collector.tape.markets()[0]
    r = collector.poll_market(mkt, now_ts=2_120)
    assert m.call_count == 2, "walk stopped at page 0 on a global-lane collision"
    assert r["gap"] is False
    # Everything between page 0 and the frontier was captured.
    assert collector.tape.stats()["fills"] >= 2 + 5 + (cfg.page_size - 5) + 1


def test_midwalk_error_declares_gap_with_interval(collector, requests_mock):
    """An errored walk that stored newer fills must declare (frontier, oldest]
    — otherwise those fills become a false continuity anchor next poll."""
    mkt = _track(collector)
    requests_mock.get(_TRADES_URL, json=[_fill(0, ts=1000), _fill(1, ts=1001)])
    collector.poll_market(mkt, now_ts=2_000)

    cfg = collector.cfg
    page0 = [_fill(1000 + i, ts=60_999 - i) for i in range(cfg.page_size)]
    requests_mock.get(_TRADES_URL, [{"json": page0}, {"status_code": 500}])
    mkt = collector.tape.markets()[0]
    r = collector.poll_market(mkt, now_ts=3_000)
    assert r["error"] is not None
    assert r["gap"] is True
    lane, cid, lo, hi, reason = collector.tape._conn.execute(
        "SELECT lane, condition_id, lo_ts, hi_ts, reason FROM l2_gaps ORDER BY id DESC LIMIT 1"
    ).fetchone()
    # Interval values pinned (review finding): lo = old frontier, hi = oldest fetched.
    assert (lane, cid) == ("market", "0xCID")
    assert lo == 1001
    assert hi == 60_999 - cfg.page_size + 1
    assert "errored" in reason


def test_error_on_first_fetch_no_gap_no_anchor(collector, requests_mock):
    mkt = _track(collector)
    requests_mock.get(_TRADES_URL, json=[_fill(0, ts=1000)])
    collector.poll_market(mkt, now_ts=2_000)
    requests_mock.get(_TRADES_URL, status_code=500)
    mkt = collector.tape.markets()[0]
    r = collector.poll_market(mkt, now_ts=3_000)
    assert r["error"] is not None and r["gap"] is False
    # Frontier unchanged: next poll retries the same interval.
    assert collector.tape.markets()[0]["newest_fill_ts"] == 1000


def test_global_watermark_not_advanced_on_errored_walk(collector, requests_mock):
    requests_mock.get(_TRADES_URL, json=[_fill(0), _fill(1)])
    collector.poll_global(1_000, set())
    w1 = collector.tape.get_meta(_GLOBAL_WATERMARK_KEY)
    cfg = collector.cfg
    page0 = [_fill(5000 + i, ts=90_000 - i) for i in range(cfg.page_size)]
    requests_mock.get(_TRADES_URL, [{"json": page0}, {"status_code": 500}])
    r = collector.poll_global(2_000, set())
    assert r["error"] is not None
    assert collector.tape.get_meta(_GLOBAL_WATERMARK_KEY) == w1, \
        "errored walk must not hide the un-walked interval behind a new watermark"


def test_global_watermark_persists_across_collector_instances(tmp_path, requests_mock):
    loaded = make_loaded(tmp_path)
    dl = build_data_layer(loaded)
    tape = TapeStore(tmp_path / "tape.db")
    try:
        requests_mock.get(_TRADES_URL, json=[_fill(0), _fill(1)])
        Collector(dl, tape).poll_global(1_000, set())
        # Fresh Collector (new invocation): continuity via l2_meta, no gap.
        requests_mock.get(_TRADES_URL, json=[_fill(2, ts=1005), _fill(0)])
        r = Collector(dl, tape).poll_global(1_120, set())
        assert r["first_run"] is False and r["gap"] is False
    finally:
        tape.close()
        dl.cache.close()


def test_closed_market_gets_drain_window_then_deactivates(collector, requests_mock):
    """Enumeration seeing closed must NOT drop the market from rotation —
    the fill tail around resolution is captured through the drain window."""
    _track(collector)
    requests_mock.get(_EVENTS_URL, json=[_event("0xCID", closed=True)])
    collector.run_enumeration(now_ts=10_000)
    m = collector.tape.markets()[0]
    assert m["close_seen_ts"] == 10_000 and m["active"] == 1  # still polled
    # Inside the drain window: not deactivated.
    drain_s = collector.cfg.drain_minutes * 60
    assert collector.tape.deactivate_drained(now_ts=10_000 + drain_s - 1,
                                             drain_seconds=drain_s) == []
    # After the window: retired.
    assert collector.tape.deactivate_drained(now_ts=10_000 + drain_s,
                                             drain_seconds=drain_s) == ["0xCID"]
    assert collector.tape.markets(active_only=True) == []
    # Re-listing (seen open again) reactivates and clears the stamp.
    requests_mock.get(_EVENTS_URL, json=[_event("0xCID", closed=False)])
    collector.run_enumeration(now_ts=20_000)
    m = collector.tape.markets()[0]
    assert m["active"] == 1 and m["close_seen_ts"] == 0


def test_stray_closed_market_is_still_adopted(collector, requests_mock):
    """A target-category stray that closed before adjudication must be adopted
    (with a drain window), not silently resolved out of existence."""
    collector.tape.record_stray("0xSTRAY", now_ts=1, fills=5)
    requests_mock.get(_EVENTS_URL, json=[])
    # Open lookup empty; closed=true lookup finds it.
    requests_mock.get(_MARKETS_URL, [
        {"json": []},
        {"json": [{"conditionId": "0xSTRAY", "slug": "s", "question": "Q?",
                   "category": "Geopolitics", "closed": True}]},
    ])
    r = collector.run_enumeration(now_ts=50_000)
    assert r["strays_adopted"] == 1
    m = [x for x in collector.tape.markets() if x["condition_id"] == "0xSTRAY"][0]
    assert m["active"] == 1 and m["close_seen_ts"] == 50_000  # drain window


def test_stray_unknown_to_gamma_stays_unresolved(collector, requests_mock):
    collector.tape.record_stray("0xGHOST", now_ts=1, fills=1)
    requests_mock.get(_EVENTS_URL, json=[])
    requests_mock.get(_MARKETS_URL, json=[])  # empty for both lookups
    collector.run_enumeration(now_ts=50_000)
    assert collector.tape.unresolved_strays() == ["0xGHOST"]  # retried later


def test_enumeration_nonlist_body_is_loud(collector, requests_mock):
    requests_mock.get(_EVENTS_URL, json={"error": "service degraded"})
    r = collector.run_enumeration(now_ts=1_000)
    assert r["errors"], "non-list gamma body must surface as an error, not end-of-pages"
    assert r["stamped"] is False
    # Enumeration stays due: it will retry next invocation.
    assert collector.enumeration_due(1_001) is True


def test_enumeration_paginates_tag_pages(collector, requests_mock):
    limit = collector.cfg.gamma_page_limit
    page1 = [_event(f"0xM{i}") for i in range(limit)]  # full page -> keep walking
    page2 = [_event("0xLAST")]
    m = requests_mock.get(_EVENTS_URL, [{"json": page1}, {"json": page2}])
    r = collector.run_enumeration(now_ts=1_000)
    assert m.call_count == 2
    assert m.request_history[1].qs["offset"] == [str(limit)]
    assert r["markets_added"] == limit + 1


def test_first_poll_fresh_fills_classify_quiet_not_dormant(collector, requests_mock):
    """Tier must come from the just-completed poll's newest fill, not the
    stale row value (review finding: fresh market misclassified dormant)."""
    mkt = _track(collector)
    now = 1_000_000
    # 30 fills (< hot threshold) all within the last half hour.
    requests_mock.get(_TRADES_URL, json=[_fill(i, ts=now - 1800 + i) for i in range(30)])
    r = collector.poll_market(mkt, now_ts=now)
    assert r["tier"] == "quiet"


def test_run_once_budget_and_oldest_first(collector, requests_mock):
    for i in range(5):
        collector.tape.upsert_market(f"0xM{i}", slug="m", question="Q?",
                                     tags="geopolitics", source="enumeration", now_ts=1)
        collector.tape.update_market_poll_state(
            f"0xM{i}", tier="hot", hot_until_ts=2**31, last_polled_ts=100 + i,
            newest_fill_ts=0, last_new_fills=0,
        )
    object.__setattr__(collector.cfg, "max_markets_per_run", 2)
    requests_mock.get(_EVENTS_URL, json=[])
    requests_mock.get(_TRADES_URL, json=[])
    env = collector.run_once()
    r = env["result"]
    assert r["markets_polled"] == 2 and r["due_backlog"] == 3
    polled = [row[0] for row in collector.tape._conn.execute(
        "SELECT condition_id FROM l2_polls WHERE lane='market'"
    ).fetchall()]
    assert polled == ["0xM0", "0xM1"], "budget must take the longest-unpolled first"


def test_run_once_prioritizes_hot_over_older_dormant_backlog(collector, requests_mock):
    """Priority-inversion regression (Basilic 2026-07-19): a large dormant
    backlog that was polled long ago must NOT crowd hot markets out of the
    per-run budget. Selection is by overdue-ratio (age / tier interval), so a
    hot market polled 5 min ago (2.5x its 2-min interval) beats a dormant
    market polled 10 h ago (only ~1.6x its 360-min interval), even though the
    dormant market is absolutely older."""
    import time as _time
    now = int(_time.time())
    # Pin intervals to production values so the scenario is self-contained
    # (the shared test config uses shorter intervals).
    object.__setattr__(collector.cfg.tiers, "hot_interval_minutes", 2)
    object.__setattr__(collector.cfg.tiers, "dormant_interval_minutes", 360)
    # 3 dormant markets polled 10 h ago: absolutely oldest, but barely overdue
    # (~1.7x a 360-min interval).
    for i in range(3):
        collector.tape.upsert_market(f"0xD{i}", slug="m", question="Q?",
                                     tags="geopolitics", source="enumeration", now_ts=1)
        collector.tape.update_market_poll_state(
            f"0xD{i}", tier="dormant", hot_until_ts=0,
            last_polled_ts=now - 10 * 3600, newest_fill_ts=0, last_new_fills=0)
    # 2 hot markets polled 5 min ago: absolutely newer, but well past their
    # 2-min interval.
    for i in range(2):
        collector.tape.upsert_market(f"0xH{i}", slug="m", question="Q?",
                                     tags="geopolitics", source="enumeration", now_ts=1)
        collector.tape.update_market_poll_state(
            f"0xH{i}", tier="hot", hot_until_ts=now + 3600,
            last_polled_ts=now - 5 * 60, newest_fill_ts=now, last_new_fills=0)
    object.__setattr__(collector.cfg, "max_markets_per_run", 2)
    requests_mock.get(_EVENTS_URL, json=[])
    requests_mock.get(_TRADES_URL, json=[])
    env = collector.run_once()
    assert env["result"]["markets_polled"] == 2
    polled = {row[0] for row in collector.tape._conn.execute(
        "SELECT condition_id FROM l2_polls WHERE lane='market'").fetchall()}
    assert polled == {"0xH0", "0xH1"}, \
        "hot markets must win the budget over an absolutely-older dormant backlog"


def test_run_once_polls_promoted_market_same_invocation(collector, requests_mock):
    """Rule 5 close: a dormant, not-yet-due market seen in global fills gets
    hot-promoted and polled within the SAME invocation."""
    import time as _time
    now = int(_time.time())
    collector.tape.upsert_market("0xCID", slug="m", question="Q?",
                                 tags="geopolitics", source="enumeration", now_ts=1)
    hot_min = collector.cfg.tiers.hot_interval_minutes
    collector.tape.update_market_poll_state(
        "0xCID", tier="dormant", hot_until_ts=0,
        last_polled_ts=now - hot_min * 60 - 30,  # due as hot, NOT due as dormant
        newest_fill_ts=0, last_new_fills=0,
    )
    requests_mock.get(_EVENTS_URL, json=[])
    requests_mock.get(_TRADES_URL, json=[_fill(0, cid="0xCID", ts=now)])
    env = collector.run_once()
    assert env["result"]["markets_polled"] == 1
    assert collector.tape.markets()[0]["tier"] == "hot"


# -- end to end ----------------------------------------------------------------------


def test_run_once_envelope_shape(collector, requests_mock):
    requests_mock.get(_EVENTS_URL, json=[_event("0xCID")])
    requests_mock.get(_TRADES_URL, json=[_fill(0), _fill(1)])
    env = collector.run_once()
    assert env["daemon"] == "consensus_collector"
    assert env["status"] in ("ok", "degraded")
    r = env["result"]
    assert r["markets_tracked"] == 1
    assert r["markets_polled"] == 1
    assert r["new_fills"] >= 2
    assert r["gaps_declared"] == 0
    assert "tape" in r and r["tape"]["fills"] >= 2
    assert json.dumps(env, default=str)  # envelope is JSON-serializable


def test_run_once_degraded_on_market_error(collector, requests_mock):
    requests_mock.get(_EVENTS_URL, json=[_event("0xCID")])
    # Global lane page ok (empty), market lane 500s.
    requests_mock.get(_TRADES_URL, [{"json": []}, {"status_code": 500}])
    env = collector.run_once()
    assert env["status"] == "degraded"
    assert env["errors"]
