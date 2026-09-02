"""Acceptance tests for the dashboard-as-PDF renderer.

The thing worth testing is not that a PDF appears — it is that the PDF and the
dashboard cannot disagree. Two renderers over one snapshot is fine; two
renderers each deciding for themselves what a number is, or which numbers may be
shown, is the defect this module exists to prevent.
"""
import pytest

from capex_daemon import dashboard, report, svgcharts, trend

from .test_charts import _fake_snapshot, qs


def _texts(flowables):
    """Every string a story would print, flattened out of the flowables."""
    out = []
    for f in flowables:
        t = getattr(f, "text", None)
        if isinstance(t, str):
            out.append(t)
        for attr in ("_cellvalues", "_content"):
            cells = getattr(f, attr, None)
            if cells:
                for row in cells:
                    out.extend(_texts(row if isinstance(row, (list, tuple)) else [row]))
    return out


def _story(snap):
    styles = report._styles()
    out = []
    for _name, _route, fn in report.SECTIONS:
        out.extend(fn(snap, styles))
    return out


# --- the two renderers cannot disagree about geometry ----------------------

def test_both_renderers_place_a_point_identically():
    """The PDF frame is the SVG frame with one subtraction. If that stops being
    true the brief and the dashboard start drawing the same value in different
    places, which is the same class of defect as printing different values."""
    q = qs(2020, 1, 24)
    kw = dict(width=720, height=200, left=62, right=26, top=22, bottom=26)
    sf = svgcharts.Frame(q, 1e9, 5e11, log=True, **kw)
    pf = report.PdfFrame(q, 1e9, 5e11, log=True, **kw)
    for v in (1e9, 7.3e10, 5e11):
        assert pf.y(v) == pytest.approx(200 - sf.y(v), abs=1e-9)
    for x in q[::5]:
        assert pf.x(x) == pytest.approx(sf.x(x), abs=1e-9)


def test_the_last_quarter_label_is_never_overprinted():
    """`svgcharts.quarter_axis` draws every Nth label AND the last one, so when
    the length is not a multiple of N the final two collide — live, Leg 1 renders
    '2026Q1' and '2026Q2' on top of each other. The last quarter wins and its
    neighbour is dropped."""
    from reportlab.graphics.shapes import Drawing, String
    q = qs(2010, 1, 66)                                # 66 % (66//14=4) == 2
    d = Drawing(720, 150)
    pf = report.PdfFrame(q, 0.0, 1.0, 720, 150, left=62, right=26, top=22, bottom=26)
    report._quarter_axis(d, pf, size=6.0)
    xs = sorted(s.x for s in d.contents if isinstance(s, String))
    assert q[-1] in [s.text for s in d.contents if isinstance(s, String)]
    gaps = [b - a for a, b in zip(xs, xs[1:])]
    assert gaps and min(gaps) >= 6.0 * 4.2 - 1e-6


# --- the two renderers cannot disagree about numbers -----------------------

def test_the_pdf_formats_money_and_percent_with_the_dashboard_s_own_functions():
    """Not a stylistic preference. A second implementation of _money is a second
    set of thresholds and a second rounding rule, and the first divergence shows
    up as the brief and the page quoting different figures for one fact."""
    assert report._money is dashboard._money
    assert report._pct is dashboard._pct
    assert report._axis_money is svgcharts._money


def test_headline_figures_match_the_dashboard_view_exactly():
    snap = _fake_snapshot()
    html = dashboard.render("/", snap)
    text = " ".join(_texts(report.sec_aggregate(snap, report._styles())))
    t = snap["total"]
    for figure in (dashboard._money(t["ttm"]), dashboard._pct(t["latest_yoy"])):
        assert figure in html
        assert figure in text


def test_a_bucket_below_the_floor_shows_no_number_on_paper_either():
    """The withholding is a rule about what may be published, not a rendering
    detail of one surface. A figure that is not a bucket figure must not appear
    as one in the PDF any more than in the HTML."""
    bk = {"state": trend.STATE_INSUFFICIENT_MEMBERSHIP, "ttm": 5.4e9,
          "latest_yoy": 0.567, "member_count": 1, "min_members": 2,
          "membership": ["EQIX"]}
    cell = report._bucket_cell(bk, "ttm", dashboard._money)
    assert "5.4" not in cell and "withheld" in cell
    assert "56.7" not in report._bucket_cell(bk, "latest_yoy", dashboard._pct)


# --- refusals and withholdings survive the crossing ------------------------

def test_a_refused_panel_total_prints_its_refusal():
    snap = _fake_snapshot()
    snap["panel"]["commitments_panel"] = {
        "status": "REFUSED-SPARSE", "detail": "3 of 35 issuers disclose"}
    text = " ".join(_texts(report.sec_commitments(snap, report._styles())))
    assert "REFUSED-SPARSE" in text and "3 of 35 issuers disclose" in text


def test_hover_only_provenance_is_carried_rather_than_dropped():
    """Matched membership, per-issuer band and coverage live in `title=`
    attributes on the page. Paper has no hover, so they have to land somewhere
    visible or they are silently unpublished."""
    snap = _fake_snapshot()
    styles = report._styles()
    agg = " ".join(_texts(report.sec_aggregate(snap, styles)))
    assert "matched" in agg and "AA" in agg
    prov = " ".join(_texts(report.sec_provenance(snap, styles)))
    assert "AA" in prov and "OK" in prov


def test_sparklines_are_drawn_not_typed():
    """The unicode block characters are absent from the bundled Vera face and
    print as empty boxes. Nothing in the story may contain one."""
    from reportlab.graphics.shapes import Rect
    d = report.spark_drawing([1.0, 5.0, 2.0, 9.0])
    assert [c for c in d.contents if isinstance(c, Rect)]
    blocks = set("▁▂▃▄▅▆▇█")
    joined = " ".join(_texts(_story(_fake_snapshot())))
    assert not (blocks & set(joined))


def test_an_empty_sparkline_is_blank_rather_than_a_crash():
    assert report.spark_drawing([]).contents == []
    assert report.spark_drawing(None) is not None


# --- coverage and the fail-loud contract ----------------------------------

def test_every_dashboard_route_has_a_section():
    routed = {r for _n, r, _f in report.SECTIONS if r}
    assert routed == set(dashboard.ROUTES)


def test_the_whole_dashboard_renders_to_one_pdf(tmp_path):
    snap = _fake_snapshot()
    out = report.build(snap, tmp_path / "dash.pdf")
    assert out.is_file() and out.stat().st_size > 20000
    body = out.read_bytes()
    assert body.startswith(b"%PDF")
    # One page per view at least, plus the cover and the appendix.
    assert body.count(b"/Type /Page\n") >= len(report.SECTIONS)


def test_it_refuses_rather_than_writing_an_empty_pdf(tmp_path):
    from abelard_common.render.pdf import PdfRenderError
    with pytest.raises(PdfRenderError):
        report.build(None, tmp_path / "empty.pdf")


def test_the_staleness_banner_is_the_dashboard_s_own_verdict():
    """Computed by `dashboard._staleness`, not re-derived — the banner is the
    difference between trusting a number as current and knowing when it was
    true, and two implementations of that is one too many."""
    snap = _fake_snapshot()
    fresh_stale, fresh = report._banner_text(snap, last_scan_unix=None)
    assert fresh_stale is True and "SCAN HAS NOT RUN" in fresh
    import time
    ok_stale, ok = report._banner_text(snap, last_scan_unix=int(time.time()))
    assert ok_stale is False and "scan is current" in ok
