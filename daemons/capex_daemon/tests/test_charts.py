"""CD-PH2 acceptance tests: constant-membership levels, the jaws, SVG marks."""
import math
import xml.etree.ElementTree as ET

import pytest

from capex_daemon import brief, dashboard, phases, svgcharts, trend

SVG = "{http://www.w3.org/2000/svg}"


def qs(start_y, start_q, n):
    out, (y, q) = [], (start_y, start_q)
    for _ in range(n):
        out.append("{}Q{}".format(y, q))
        q += 1
        if q > 4:
            y, q = y + 1, 1
    return out


def flat(quarters, v):
    return {q: v for q in quarters}


# --- constant membership: the guard that makes a LEVEL plottable -----------

def test_a_level_is_not_drawn_over_changing_membership():
    """The live defect: matched membership ran 1 -> 12 names across 66 quarters,
    so a plotted level showed arrivals as growth. The invariant that fixes it is
    that membership is CONSTANT across every quarter of the plotted window."""
    old = qs(2015, 1, 40)
    members = {"A": flat(old, 100.0), "B": flat(old, 100.0),
               "LATE": flat(old[-6:], 900.0)}          # a big arrival at the end
    cp = trend.constant_membership_panel(members)
    assert cp
    for t in cp.members:                               # every member spans the window
        assert all(q in members[t] for q in cp.quarters)
    assert len(set(cp.level.values())) == 1            # a flat panel plots flat


def test_a_large_late_arrival_shortens_the_window_rather_than_being_blended_in():
    """LATE is 82% of current dollars, so a long window excluding it would cover
    18%. The rule takes the short window with everyone over the long window with
    a fifth of the money — and never the long window with LATE spliced in."""
    old = qs(2015, 1, 40)
    members = {"A": flat(old, 100.0), "B": flat(old, 100.0),
               "LATE": flat(old[-6:], 900.0)}
    cp = trend.constant_membership_panel(members)
    assert "LATE" in cp.members and len(cp.quarters) == 6
    assert cp.coverage >= trend.COVERAGE_FLOOR


def test_coverage_beats_history_when_they_conflict():
    """Maximising members x quarters selected a 44-quarter window that dropped
    Amazon and Meta. Coverage of today's dollars comes first."""
    long_q = qs(2010, 1, 60)
    short_q = long_q[-26:]                    # co-terminal: WHALE is not lagging
    members = {"SMALL1": flat(long_q, 1.0), "SMALL2": flat(long_q, 1.0),
               "WHALE": flat(short_q, 1000.0)}
    cp = trend.constant_membership_panel(members)
    assert "WHALE" in cp.members
    assert cp.coverage >= trend.COVERAGE_FLOOR
    assert len(cp.quarters) == len(short_q)   # history traded away for the dollars


def test_the_chosen_window_is_the_longest_that_clears_the_floor():
    q = qs(2012, 1, 50)
    members = {"A": flat(q, 50.0), "B": flat(q, 50.0), "C": flat(q[-8:], 1.0)}
    cp = trend.constant_membership_panel(members)
    assert cp.members == ["A", "B"]          # C is 1% of dollars; history wins
    assert len(cp.quarters) == 50


def test_names_behind_on_filing_are_named_not_dropped_silently():
    q = qs(2018, 1, 30)
    members = {"A": flat(q, 10.0), "B": flat(q, 10.0), "LAGGARD": flat(q[:-1], 99.0)}
    cp = trend.constant_membership_panel(members)
    assert [t for t, _lq, _v in cp.lagging] == ["LAGGARD"]
    assert cp.lagging[0][1] == q[-2]


def test_no_qualifying_window_returns_a_falsy_panel_not_a_guess():
    cp = trend.constant_membership_panel({"A": {"2026Q1": 1.0}})
    assert not cp and cp.members is None


# --- the jaws must be a matched pair --------------------------------------

def test_jaws_are_the_same_names_on_both_legs():
    """Capex coverage and issuance coverage select different names — 5 against 2
    live. Rebasing one onto the other would compare different companies."""
    snap = _fake_snapshot()
    m = svgcharts.composite_model(snap)
    assert m["jaws_capex_panel"]["members"] == m["issuance_panel"]["members"]
    assert m["jaws_capex_panel"]["members"] == ["AA", "BB"]


def test_jaws_are_true_dollars_with_no_rescaling():
    """Rebasing onto a linear axis rescaled credit 21x and pinned every real
    capex line to the floor. Log axis, true dollars, divergence read by slope."""
    snap = _fake_snapshot()
    m = svgcharts.composite_model(snap)
    assert m["log"] is True
    assert "rebase_k" not in m
    ji = {r["q"]: r["value"] for r in snap["panel"]["constant"]["jaws_issuance"]["series"]}
    for q, v in m["issuance"]:
        assert v == ji[q]                     # plotted value IS the reported value


def test_growth_multiples_are_published_for_the_caption():
    m = svgcharts.composite_model(_fake_snapshot())
    assert m["growth"]["capex_x"] == pytest.approx(2.0)
    assert m["growth"]["credit_x"] == pytest.approx(10.0)


# --- log scale -------------------------------------------------------------

def test_log_scale_places_a_decade_at_a_constant_offset():
    f = svgcharts.Frame(qs(2020, 1, 4), 1e6, 1e10, log=True)
    d1 = f.y(1e7) - f.y(1e8)
    d2 = f.y(1e8) - f.y(1e9)
    assert d1 == pytest.approx(d2)


def test_log_scale_never_takes_the_log_of_zero():
    f = svgcharts.Frame(qs(2020, 1, 4), 1e6, 1e10, log=True)
    assert math.isfinite(f.y(0.0)) and math.isfinite(f.y(-5.0))


def test_both_renderers_place_a_point_identically():
    """The SVG dashboard and the PDF brief share `norm`; if they diverge, the
    front page and the brief disagree about where a line is."""
    for v in (1e6, 5e8, 3.2e10, 1e12):
        assert svgcharts.norm(v, 1e6, 1e12, log=True) == pytest.approx(
            (math.log10(v) - 6) / 6)


# --- marks -----------------------------------------------------------------

def test_labels_never_overlap():
    """TOTAL PANEL and the hyperscaler sum landed 0.4px apart, live."""
    items = [(100.0, 500, "a", "#000"), (100.4, 500, "b", "#111"),
             (101.0, 500, "c", "#222")]
    out = svgcharts.place_labels(items, min_gap=12.0)
    ys = sorted(float(t.get("y")) for t in ET.fromstring(
        "<g xmlns='http://www.w3.org/2000/svg'>" + out + "</g>").iter(SVG + "text"))
    assert all(b - a >= 11.9 for a, b in zip(ys, ys[1:]))


def test_every_chart_is_well_formed_svg_and_in_bounds():
    snap = _fake_snapshot()
    rows, quarters = svgcharts.issuer_rows_for_grid(snap)
    for name, svg in (("composite", svgcharts.composite(snap)),
                      ("grid", svgcharts.state_grid(rows, quarters)),
                      ("yoy", svgcharts.yoy_chart(snap["total"]["yoy_series"],
                                               snap["total"]["observations"], "t", "#111"))):
        root = ET.fromstring(svg)
        w, h = (float(x) for x in root.get("viewBox").split()[2:])
        for c in root.iter(SVG + "circle"):
            assert 0 <= float(c.get("cx")) <= w and 0 <= float(c.get("cy")) <= h, name


def test_insufficient_history_gets_no_colour_on_the_grid():
    """A blank cell must read as absence, not as a fifth state."""
    assert phases.STATE_INSUFFICIENT not in svgcharts.BAND_FILLS
    assert set(svgcharts.BAND_FILLS) == set(phases.REAL_STATES) - {phases.STATE_INSUFFICIENT}


def test_a_gap_in_a_series_is_dashed_not_joined_flat():
    solid = svgcharts.multi_line_chart({"X": [("2024Q1", 1.0), ("2024Q2", 2.0)]}, "t")
    gapped = svgcharts.multi_line_chart({"X": [("2024Q1", 1.0), ("2025Q3", 2.0)]}, "t")
    assert "stroke-dasharray" not in solid
    assert "stroke-dasharray" in gapped


# --- refusals are stated, not omitted --------------------------------------

def test_a_refused_leg_prints_its_refusal_on_the_chart():
    snap = _fake_snapshot()
    snap["panel"]["commitments_panel"] = {
        "status": "REFUSED-NO-CONSTANT-MEMBERSHIP", "detail": "ragged disclosure"}
    assert "ragged disclosure" in svgcharts.composite(snap)


def test_a_bucket_below_the_floor_shows_no_number_anywhere():
    """The floor set the STATE but the TTM/YoY cells kept printing the number."""
    bk = {"state": trend.STATE_INSUFFICIENT_MEMBERSHIP, "ttm": 5.4e9,
          "latest_yoy": 0.567, "member_count": 1, "min_members": 2,
          "membership": ["EQIX"]}
    assert "5.4" not in dashboard._bucket_num(bk, "ttm", dashboard._money)
    assert "56.7" not in dashboard._bucket_num(bk, "latest_yoy", dashboard._pct)
    assert "EQIX" in dashboard._bucket_num(bk, "ttm", dashboard._money)


# --- the views and the brief ----------------------------------------------

def test_every_route_renders_and_since_last_scan_is_the_front_page():
    """B1 moved the front page: what is NEW comes before what is known. The
    aggregate is still the front page of the state view, one click away."""
    snap = _fake_snapshot()
    assert dashboard.VIEWS[0][0] == "/since"
    assert dashboard.ROUTES["/since"] is dashboard.view_since
    assert dashboard.ROUTES["/"] is dashboard.view_aggregate
    for path, _name in dashboard.VIEWS:
        html = dashboard.render(path, snap)
        assert html
        # /since is a table page by design — it reports events, not series.
        assert "<svg" in html or path == "/since"


def test_the_brief_draws_from_the_same_model_as_the_dashboard(tmp_path):
    snap = _fake_snapshot()
    out = brief.phase_page(snap, tmp_path / "p.pdf")
    assert out.is_file() and out.stat().st_size > 1000


def test_the_brief_refuses_rather_than_writing_an_empty_pdf(tmp_path):
    from abelard_common.render.pdf import PdfRenderError
    with pytest.raises((PdfRenderError, KeyError, TypeError)):
        brief.phase_page({}, tmp_path / "empty.pdf")


# --- fixture ---------------------------------------------------------------

def _series(quarters, first, mult):
    """Geometric from `first` to `first*mult` across `quarters`."""
    n = len(quarters) - 1
    return [{"q": q, "value": first * (mult ** (i / n))}
            for i, q in enumerate(quarters)]


def _fake_snapshot():
    q = qs(2019, 1, 20)
    obs = [{"quarter": x, "yoy": 0.3, "delta": 1.0, "direction": "up",
            "state": phases.STATE_ACCELERATING, "flags": [],
            "quarters_in_state": 2, "entered": q[0]} for x in q]
    return {
        "bands_measured_on": "2026-08-18",
        "generated_unix": 1755648000,
        "total": {"state": phases.STATE_ACCELERATING, "band": 5.0, "ttm": 4.0e11,
                  "latest_yoy": 0.8, "member_count": 5, "membership": ["AA", "BB"],
                  "latest_quarter": q[-1], "observations": obs,
                  "yoy_series": [{"q": x, "yoy": 0.3} for x in q],
                  "ttm_series": [{"q": r["q"], "ttm": r["value"], "members": 5}
                                 for r in _series(q, 1e11, 4.0)]},
        "buckets": {"hyperscaler": {
            "bucket": "hyperscaler", "state": phases.STATE_ACCELERATING, "band": 4.0,
            "ttm": 3.9e11, "latest_yoy": 0.83, "member_count": 4, "min_members": 2,
            "membership": ["AA", "BB"], "latest_quarter": q[-1], "top2_share": 0.7,
            "observations": obs, "breadth": {"net_direction": 1},
            "yoy_series": [{"q": x, "yoy": 0.3, "members": ["AA", "BB"]} for x in q],
            "ttm_series": [dict(r, ttm=r["value"], members=4) for r in _series(q, 9e10, 4.0)],
            "composition_events": []}},
        "issuers": {"AA": _issuer("AA", "hyperscaler", q, obs)},
        "panel": {
            "constant": {
                "capex": {"members": ["AA", "BB"], "member_count": 2, "coverage": 0.98,
                          "first_quarter": q[0], "last_quarter": q[-1],
                          "series": _series(q, 1e11, 4.0), "lagging": []},
                "issuance": {"members": ["AA", "BB"], "member_count": 2, "coverage": 0.97,
                             "first_quarter": q[0], "last_quarter": q[-1],
                             "series": _series(q, 1e10, 10.0), "lagging": []},
                "commitments": {"members": [], "member_count": 0, "coverage": 0.0,
                                "series": [], "lagging": []},
                "jaws_capex": {"members": ["AA", "BB"], "member_count": 2, "coverage": 0.98,
                               "first_quarter": q[0], "last_quarter": q[-1],
                               "series": _series(q, 5e10, 2.0), "lagging": []},
                "jaws_issuance": {"members": ["AA", "BB"], "member_count": 2,
                                  "coverage": 0.97, "first_quarter": q[0],
                                  "last_quarter": q[-1],
                                  "series": _series(q, 1e10, 10.0), "lagging": []},
                "buckets": {"hyperscaler": {
                    "members": ["AA", "BB"], "member_count": 2, "coverage": 1.0,
                    "first_quarter": q[0], "last_quarter": q[-1],
                    "series": _series(q, 9e10, 4.0), "lagging": []}}},
            "issuance_ttm": [dict(r, members=2) for r in _series(q, 1e10, 10.0)],
            "issuance_membership_latest": ["AA", "BB"],
            "commitments": [], "commitments_membership_latest": [],
            "credit_ratio_series": [{"q": x, "ratio": 0.3, "members": 2} for x in q],
            "breadth_series": [{"q": x, phases.STATE_ACCELERATING: 3,
                                phases.STATE_PLATEAU: 1, phases.STATE_DECELERATING: 1,
                                phases.STATE_CONTRACTING: 0, "net_direction": 2}
                               for x in q],
            "commitments_panel": {"status": "OK", "detail": "", "disclosing_issuers": 0}},
        "suppliers": {
            "covered": ["NVDA"],
            "legs": {"NVDA": {"ticker": "NVDA", "status": "COVERED", "detail": "resolved",
                              "axes": ["ProductOrServiceAxis"], "concept": "Revenues",
                              "instances": 14, "ttm": 2.3e11, "latest_quarter": q[-1],
                              "quarters": [{"q": x, "value": 5e10} for x in q],
                              "ttm_series": [{"q": x, "value": 2e11} for x in q],
                              "restatements": [], "restatement_count": 0, "dropped": 0}},
            "combined": {"members": ["NVDA"], "ttm": 2.3e11, "latest_quarter": q[-1],
                         "ttm_series": [{"q": x, "value": 2e11, "members": 1} for x in q]},
            "crosscheck": {"against": "hyperscaler", "latest_ratio": 0.45,
                           "latest_quarter": q[-1], "warning": None,
                           "series": [{"q": x, "ratio": 0.45, "dc": 2e11, "capex": 4.4e11,
                                       "dc_members": 1, "capex_members": 5} for x in q]}},
        "transitions": [],
    }


def _issuer(tick, bucket, q, obs):
    return {"cik": "0000000001", "ticker": tick, "bucket": bucket, "notes": "",
            "state": phases.STATE_ACCELERATING, "flags": [], "quarters_in_state": 2,
            "entered": q[0], "direction": "up", "latest_yoy": 0.8, "latest_delta": 9.0,
            "band": 4.0, "ttm_capex": 2.0e11, "ttm_issuance": 8.0e10,
            "credit_ratio": 0.4, "coverage": ["OK"],
            "quarters": [{"q": x, "value": 5e10} for x in q],
            "yoy_series": [{"q": x, "yoy": 0.3} for x in q],
            "observations": obs,
            "commitments": {"status": "OK", "detail": "", "latest": 3.2e10,
                            "concept": "PurchaseObligation", "points": [],
                            "points_cq": [{"q": x, "value": 3.0e10} for x in q[-6:]]}}


# --- alerts are for news, not for freshly-derived history -------------------

def test_a_classifier_change_does_not_alert_thirteen_year_old_transitions():
    """Event keys are content-derived, so changing the classifier mints new keys
    for OLD quarters and every one looks unseen. Measured live: the
    CONTRACTING-exit fix made a rebuild announce `bucket:builder 2013Q3
    CONTRACTING->PLATEAU` as news in 2026."""
    from capex_daemon import snapshot
    obs = [{"quarter": q, "state": "PLATEAU"} for q in ("2026Q1", "2026Q2")]
    snap = {"issuers": {}, "buckets": {"builder": {"observations": obs}},
            "total": {"observations": obs},
            "transitions": [
                {"series_key": "bucket:builder", "quarter": "2013Q3",
                 "from_state": "CONTRACTING", "to_state": "PLATEAU",
                 "yoy": .1, "delta": 9.0, "event_key": "old"},
                {"series_key": "bucket:builder", "quarter": "2026Q2",
                 "from_state": "PLATEAU", "to_state": "ACCELERATING",
                 "yoy": .5, "delta": 30.0, "event_key": "now"}]}
    keys = [a["event_key"] for a in snapshot.alert_lines(snap)]
    assert keys == ["now"]


def test_the_frontier_allows_one_quarter_of_slack():
    """Issuers file weeks apart; a transition on the quarter just before the
    frontier is still current news, not history."""
    from capex_daemon import snapshot
    obs = [{"quarter": q, "state": "PLATEAU"} for q in ("2026Q1", "2026Q2")]
    snap = {"issuers": {}, "buckets": {"builder": {"observations": obs}},
            "total": {"observations": obs},
            "transitions": [{"series_key": "bucket:builder", "quarter": "2026Q1",
                             "from_state": "PLATEAU", "to_state": "CONTRACTING",
                             "yoy": -.1, "delta": -30.0, "event_key": "k"}]}
    assert len(snapshot.alert_lines(snap)) == 1


# --- dashboard binding and snapshot provenance ------------------------------

def test_the_dashboard_defaults_to_loopback():
    """A read-only dashboard is still a listening socket. Exposure is an
    explicit act by the launcher, never the default."""
    assert dashboard.HOST_DEFAULT == "127.0.0.1"
    assert dashboard.HOST_DEFAULT != "0.0.0.0"


def test_a_fresh_snapshot_shows_its_stamp_and_no_warning():
    import time as _t
    snap = _fake_snapshot()
    snap["generated_unix"] = int(_t.time())
    banner = dashboard._stale_banner(snap, last_scan_unix=snap["generated_unix"])
    assert "scan is current" in banner and "HAS NOT RUN" not in banner


def test_staleness_measures_the_SCAN_not_the_snapshot():
    """Most nights are no-ops by design, so a healthy daemon serves a snapshot
    days old. Measured live: two clean consecutive no-ops left the snapshot 54h
    old and the banner claiming the nightly had not completed, when it had run
    twice. Stale means THE SCAN stopped, which is the actual failure."""
    import time as _t
    now = int(_t.time())
    old_snap = _fake_snapshot()
    old_snap["generated_unix"] = now - int(3600 * 200)     # panel unchanged for days
    # ...but the scan ran an hour ago: healthy, no warning.
    stale, _age = dashboard._staleness(old_snap, last_scan_unix=now - 3600)
    assert not stale
    assert "scan is current" in dashboard._stale_banner(old_snap, last_scan_unix=now - 3600)
    # ...and when the SCAN stops, it warns.
    dead = now - int(3600 * (dashboard.STALE_AFTER_HOURS + 5))
    stale, age = dashboard._staleness(old_snap, last_scan_unix=dead)
    assert stale and age > dashboard.STALE_AFTER_HOURS
    assert "SCAN HAS NOT RUN" in dashboard._stale_banner(old_snap, last_scan_unix=dead)
    # ...and the view still renders rather than 503-ing
    assert dashboard.render("/", old_snap, last_scan_unix=dead)


def test_staleness_falls_back_to_the_snapshot_stamp_when_never_scanned():
    """A database written before last_scan_unix existed still reports honestly."""
    import time as _t
    snap = _fake_snapshot()
    snap["generated_unix"] = int(_t.time()) - int(3600 * (dashboard.STALE_AFTER_HOURS + 5))
    stale, _age = dashboard._staleness(snap, last_scan_unix=None)
    assert stale


def test_every_view_carries_the_provenance_banner():
    """Injected in render(), so a new view cannot be added without it.

    Both banner forms name the generation time — the fresh one as a stamp, the
    stale one inside its warning. What must never happen is a view rendering
    figures with no indication of when they were true.
    """
    import time as _t
    fresh = _fake_snapshot()
    fresh["generated_unix"] = int(_t.time())
    stale = _fake_snapshot()          # fixture stamp is deliberately old
    for path, _name in dashboard.VIEWS:
        import time as _t
        assert "scan is current" in dashboard.render(
            path, fresh, last_scan_unix=int(_t.time()))
        out = dashboard.render(path, stale, last_scan_unix=1)
        assert "SCAN HAS NOT RUN" in out and "panel itself last changed" in out


# --- CD-GAP1 P1/P5: graduated disclosure -----------------------------------

class _E:
    def __init__(self, bucket, cik="0000000001", ticker="TEST"):
        self.bucket, self.cik, self.ticker_display = bucket, cik, ticker


def _qs(n, start=(2024, 1), value=100.0):
    from capex_daemon import disclosure  # noqa: F401
    out, (y, q) = {}, start
    for i in range(n):
        out["{}Q{}".format(y, q)] = value * (1 + i)
        q += 1
        if q > 4:
            y, q = y + 1, 1
    return out


def test_ten_quarters_is_the_classification_threshold():
    """Measured, not assumed: TTM needs 4, a TTM YoY needs 8, and the ladder
    needs N_CONFIRM+1 = 3 YoY points."""
    from capex_daemon import disclosure
    assert disclosure.QUARTERS_TO_CLASSIFY == 10
    _d, short = disclosure.first_eligible(_qs(6))
    assert short == 4
    _d, short = disclosure.first_eligible(_qs(10))
    assert short == 0


def test_a_pre_eligible_name_publishes_its_level_not_a_dash():
    """FRMI carried $1.16B of TTM capex behind a dash. What was unknown was its
    YoY, not its spending."""
    from capex_daemon import disclosure
    d = disclosure.classify(_E("builder"), _qs(6))
    assert d["cause"] == disclosure.CAUSE_THIN_MATURING
    assert d["ttm"] is not None and d["ttm"] > 0
    assert d["quarters_held"] == 6 and d["quarters_short"] == 4
    assert d["first_eligible"]


def test_the_interim_read_is_never_a_phase_state():
    """It has no dead-band and no confirmation window, so it must never be
    mistakable for a ladder verdict."""
    from capex_daemon import disclosure, phases
    d = disclosure.classify(_E("builder"), _qs(6))
    assert d["interim_growth"] is not None
    assert "non-ladder" in d["interim_basis"]
    assert d["cause"] not in phases.REAL_STATES


def test_the_cause_is_split_not_lumped():
    from capex_daemon import disclosure
    assert disclosure.classify(_E("sidecar"), _qs(10))["cause"] == disclosure.CAUSE_SIDECAR
    assert disclosure.classify(_E("fpi"), {})["cause"] == disclosure.CAUSE_FPI_ANNUAL
    assert disclosure.classify(
        _E("builder"), {}, coverage=("CAPEX-UNRESOLVED",)
    )["cause"] == disclosure.CAUSE_REFUSED


def test_an_fpi_that_stopped_tagging_is_not_an_annual_basis_issuer():
    """Alibaba's capex concept carries data through 2020 and nothing after.
    'Reports annually' and 'stopped reporting' look identical from an empty
    quarterly series and are not the same fact."""
    from capex_daemon import disclosure
    import datetime
    d = disclosure.classify(_E("fpi"), {}, last_tagged="2020-09-30",
                            today=datetime.date(2026, 8, 26))
    assert d["cause"] == disclosure.CAUSE_TAGGING_CEASED
    fresh = disclosure.classify(_E("fpi"), {}, last_tagged="2026-03-31",
                                today=datetime.date(2026, 8, 26))
    assert fresh["cause"] == disclosure.CAUSE_FPI_ANNUAL


def test_a_classified_name_is_owed_no_explanation():
    from capex_daemon import disclosure, phases
    assert disclosure.classify(
        _E("builder"), _qs(12), state=phases.STATE_ACCELERATING) is None


def test_lateness_is_measured_against_the_calendar_not_the_panel():
    """The first cut compared each issuer to the most advanced filer and marked
    seven current names late, because one off-calendar filer had already reached
    the next calendar quarter."""
    from capex_daemon import disclosure
    import datetime
    q = _qs(6, start=(2025, 1))                    # ends 2026Q2
    due, overdue = disclosure.expected_by(q, today=datetime.date(2026, 8, 26))
    assert due.isoformat() == "2026-11-14"         # 2026Q3 end + 45 days
    assert overdue is False
    _due, overdue = disclosure.expected_by(q, today=datetime.date(2027, 1, 1))
    assert overdue is True


# --- CD-GAP2A A6: two live rendering defects -------------------------------

def test_a_fractional_axis_gets_more_than_one_gridline():
    """Every YoY and ratio chart is fractional — +84.7% is 0.847. The old
    magnitude floored at 1.0, so a -0.2..+0.9 panel produced ticks at 0 and 1
    and every such chart on the live dashboard rendered a single '+0%' line."""
    ticks = svgcharts._nice_ticks(-0.2, 0.9, 5)
    inside = [t for t in ticks if -0.2 <= t <= 0.9]
    assert len(inside) >= 4, ticks
    # and the dollar axes it always handled must be UNCHANGED: Leg 1 spans
    # 0..$600B and prints $200B/$400B/$600B, before and after.
    assert svgcharts._nice_ticks(0.0, 600e9, 5) == [0.0, 200e9, 400e9, 600e9]


def test_tiny_and_degenerate_ranges_do_not_explode():
    assert svgcharts._nice_ticks(0.5, 0.5) == [0.5]
    assert svgcharts._nice_ticks(0.0, 0.004, 4)
    assert svgcharts._nice_ticks(-0.03, 0.03, 5)


def test_the_last_quarter_label_is_never_overprinted():
    """66 quarters with every=4 drew index 64 and index 65 on top of each other:
    '2026Q1' and '2026Q2' rendered as '20226Q2' on Leg 1."""
    q = qs(2010, 1, 66)
    f = svgcharts.Frame(q, 0.0, 1.0, width=1160, height=250)
    body = svgcharts.quarter_axis(f)
    root = ET.fromstring("<svg xmlns='http://www.w3.org/2000/svg'>" + body + "</svg>")
    xs = sorted(float(t.get("x")) for t in root.iter(SVG + "text"))
    labels = [t.text for t in root.iter(SVG + "text")]
    assert q[-1] in labels
    gaps = [b - a for a, b in zip(xs, xs[1:])]
    assert gaps and min(gaps) >= svgcharts.MIN_LABEL_GAP - 1e-6, min(gaps)


def test_the_phase_grid_labels_do_not_overprint_either():
    """The grid had the same defect as the time axis in its own label loop:
    '2026Q2' and '2026Q3' landed on top of each other at the right edge."""
    rows = [("A", {q: phases.STATE_PLATEAU for q in qs(2010, 1, 67)}, "")]
    svg = svgcharts.state_grid(rows, qs(2010, 1, 67))
    root = ET.fromstring(svg)
    xs = sorted(float(t.get("x")) for t in root.iter(SVG + "text")
                if (t.text or "").endswith(("Q1", "Q2", "Q3", "Q4")))
    gaps = [b - a for a, b in zip(xs, xs[1:])]
    assert gaps and min(gaps) > 1.0, min(gaps)
