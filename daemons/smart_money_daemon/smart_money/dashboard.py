"""SM-R1 L2 dashboard. Stdlib only: http.server + read-only sqlite + string
templates. No JS framework, no build step.

SAFE BY CONSTRUCTION:
  * the DB is opened mode=ro per request, so there are ZERO write endpoints;
  * only GET is served (anything else is 405);
  * it binds to a LAN/Tailscale host only and REFUSES 0.0.0.0 (fail-loud), so it
    is never exposed on a public interface;
  * every value rendered is html-escaped and every query is parameterized, so a
    filter param cannot inject markup or SQL.

Five views map 1:1 onto the L1 query layer; filter state lives in URL query
params on every view; the print button is GET /brief.pdf?<current params>.
"""
from __future__ import annotations

import argparse
import datetime as dt
import html
import io
import os
import sys
import tempfile
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

from . import db as dbmod
from . import queries as q

NAV = [("/", "Front"), ("/portfolios", "Portfolios"), ("/congress", "Congress"),
       ("/trades", "Trades"), ("/flows", "Net flows"), ("/clusters", "Clusters"),
       ("/sentinels", "Sentinels"), ("/ticker", "Ticker")]

# Theme palettes. Default is light; dark applies automatically when the viewer's
# system prefers dark, and can be forced either way via ?theme=dark|light (which
# threads through the URL like the other filters). Pure CSS custom properties, no
# JS — fits the stdlib, no-build ethos.
_LIGHT = ("--bg:#ffffff;--fg:#1a1a1a;--border:#cccccc;--th-bg:#eeeeff;"
          "--muted:#666666;--hot-bg:#fdecea;--hot-fg:#1a1a1a;--link:#0a58ca;"
          "--pos:#1a8a3a;--neg:#c0392b;--reported:#c0392b;"
          "--prov-book:#dcefdc;--prov-watch:#dce8fb;--prov-trump:#fbe4d6;--prov-thiel:#ecdcfb")
_DARK = ("--bg:#0f1216;--fg:#d7dce3;--border:#2b313b;--th-bg:#1a2130;"
         "--muted:#8b93a1;--hot-bg:#3a2323;--hot-fg:#f3d9d6;--link:#6ea8fe;"
         "--pos:#4ade80;--neg:#ff6b6b;--reported:#ff6b6b;"
         "--prov-book:#1c3324;--prov-watch:#1b2a44;--prov-trump:#3a2a1c;--prov-thiel:#2c1f3d")
_CSS = (
    ":root{" + _LIGHT + "}"
    "@media(prefers-color-scheme:dark){:root:not([data-theme]){" + _DARK + "}}"
    ":root[data-theme='dark']{" + _DARK + "}"
    ":root[data-theme='light']{" + _LIGHT + "}"
    "body{font:14px system-ui,sans-serif;margin:1.5rem;max-width:1100px;"
    "background:var(--bg);color:var(--fg)}"
    "a{color:var(--link)}"
    "h1{font-size:1.3rem}h2{font-size:1.05rem;margin-top:1.4rem}"
    "table{border-collapse:collapse;width:100%;margin:.4rem 0}"
    "table.wide{width:auto;min-width:100%}"
    "table.wide td,table.wide th{white-space:nowrap}"
    "th a{color:inherit;text-decoration:none} th a:hover{text-decoration:underline}"
    "td,th{border:1px solid var(--border);padding:3px 7px;text-align:left;font-size:13px}"
    "th{background:var(--th-bg)}"
    "nav{margin-bottom:1rem}.print{float:right}"
    ".muted{color:var(--muted);font-size:12px}"
    ".hot{background:var(--hot-bg);color:var(--hot-fg)}"
    "form{margin:.5rem 0;font-size:13px}"
    "input,button,select{background:var(--bg);color:var(--fg);"
    "border:1px solid var(--border);border-radius:3px;padding:2px 6px}"
    ".badge{display:inline-block;font-size:10px;padding:0 4px;border-radius:3px;"
    "margin-left:3px;border:1px solid var(--border)}"
    ".plan{background:var(--hot-bg);color:var(--hot-fg)}"
    ".smid{background:var(--th-bg)}"
    ".prov-book{background:var(--prov-book)} .prov-watch{background:var(--prov-watch)}"
    ".prov-trump{background:var(--prov-trump)} .prov-thiel{background:var(--prov-thiel)}"
    ".qoq-new{background:var(--prov-book)} .qoq-added{background:var(--prov-watch)}"
    ".qoq-trimmed{background:var(--hot-bg);color:var(--hot-fg)}"
    ".qoq-exited{background:var(--th-bg);color:var(--muted)}"
    ".strip{display:flex;flex-wrap:wrap;gap:.5rem;margin:.5rem 0}"
    ".book{border:1px solid var(--border);border-radius:5px;padding:.4rem .6rem;font-size:12px;min-width:150px}"
    ".book b{font-size:13px}"
    ".reported{color:var(--reported);font-weight:bold}"
    ".pos{color:var(--pos)} .neg{color:var(--neg)}"
    ".expand a{margin-right:8px} .expand{margin:.3rem 0;font-size:12px}"
)


# ---------------------------------------------------------------- html helpers
def _page(title, body, params):
    qs = _qs(params)
    # Cross-view nav must NOT carry per-view cursor/sort state: page/spage are per-view
    # and sort keys are view-specific (a flows column key means nothing to trades and
    # would suppress the destination's default-sort arrow). Reset them so every view
    # opens on page 1 in its own default sort (per_page, a display preference, carries).
    nav_qs = _qs(params, page=None, spage=None, sort=None, dir=None,
                 ssort=None, sdir=None, cticker=None, cinstr=None, cowner=None)
    nav = " &middot; ".join(
        '<a href="{}{}">{}</a>'.format(href, nav_qs, html.escape(label))
        for href, label in NAV)
    printbtn = '<a class="print" href="/brief.pdf{}">Print brief (PDF)</a>'.format(qs)
    dark = params.get("theme") == "dark"
    toggle = '<a href="{}">{} mode</a>'.format(
        _qs(params, theme=("light" if dark else "dark")), "Light" if dark else "Dark")
    attr = ' data-theme="{}"'.format(params["theme"]) if params.get("theme") else ""
    return (
        "<!doctype html><html{attr}><head><meta charset='utf-8'>"
        "<meta name='color-scheme' content='light dark'>"
        "<title>{t}</title><style>{css}</style></head><body>"
        "<nav>{nav} &middot; {toggle} {print}</nav><h1>{t}</h1>{filt}{body}</body></html>"
    ).format(attr=attr, t=html.escape(title), css=_CSS, nav=nav, toggle=toggle,
             print=printbtn, filt=_filter_form(params), body=body)


def _qs(params, **override):
    base = {k: params.get(k) for k in ("window", "anchor", "floor", "symbol",
                                       "theme", "per_page", "page", "side", "plan",
                                       "scope")}
    base["smid"] = "1" if params.get("smid") else None
    base["capit"] = "1" if params.get("capit") else None
    # src/spage carry only when non-default, so Trades/other URLs stay clean but
    # the Clusters sell-table page survives paging the buy table (and vice versa).
    base["src"] = params.get("src") if params.get("src") not in (None, "", "all") else None
    base["metric"] = params.get("metric") if params.get("metric") not in (None, "", "value") else None
    base["spage"] = params.get("spage") if params.get("spage", 1) not in (None, 1) else None
    # Sort state: the column carries when set; direction only when non-default (asc),
    # since desc is the default. ssort/sdir are the second-table sort on Clusters.
    base["sort"] = params.get("sort") or None
    base["dir"] = params.get("dir") if params.get("sort") and params.get("dir") == "asc" else None
    base["ssort"] = params.get("ssort") or None
    base["sdir"] = params.get("sdir") if params.get("ssort") and params.get("sdir") == "asc" else None
    base["filer"] = params.get("filer") or None
    base["period"] = params.get("period") or None
    base["cticker"] = params.get("cticker") or None
    base["cinstr"] = params.get("cinstr") if params.get("cinstr") not in (None, "", "SH") else None
    base["cowner"] = params.get("cowner") if params.get("cowner") not in (None, "", "all") else None
    base.update(override)
    keep = {k: v for k, v in base.items() if v}
    if not keep:
        return ""
    return "?" + "&".join("{}={}".format(k, html.escape(str(v))) for k, v in keep.items())


def _filter_form(params):
    return (
        "<form method='get'>window <input name='window' value='{w}' size='4'> "
        "anchor <input name='anchor' value='{a}' size='10'> "
        "floor <input name='floor' value='{f}' size='2'> "
        "<button>apply</button></form>"
    ).format(w=html.escape(str(params.get("window", 90))),
             a=html.escape(str(params.get("anchor", ""))),
             f=html.escape(str(params.get("floor", 3))))


def _table(cols, rows, hot=None):
    head = "".join("<th>{}</th>".format(html.escape(c)) for c in cols)
    out = ["<table><tr>{}</tr>".format(head)]
    for r in rows:
        cls = ' class="hot"' if hot and hot(r) else ""
        cells = "".join("<td>{}</td>".format(html.escape(_fmt(r.get(c))))
                        for c in cols)
        out.append("<tr{}>{}</tr>".format(cls, cells))
    out.append("</table>")
    if not rows:
        out.append("<p class='muted'>Nothing to report this window.</p>")
    return "".join(out)


def _sorted(rows, key, direction):
    """Sort row-dicts by `key`, missing values (None/"") always last regardless of
    direction. Empty key -> rows unchanged (caller applies its own default order)."""
    if not key or not rows:
        return rows
    present = [r for r in rows if r.get(key) is not None and r.get(key) != ""]
    missing = [r for r in rows if r.get(key) is None or r.get(key) == ""]
    present.sort(key=lambda r: r.get(key), reverse=(direction != "asc"))
    return present + missing


def _sort_headers(params, cols, qs_fn, active, direction,
                  sort_param="sort", dir_param="dir", page_reset=("page",)):
    """A <tr> of clickable column headers. cols is [(key, label)]; a None key is a
    plain (non-sortable) header. Clicking a column sorts it descending; clicking the
    already-active column flips direction. `active`/`direction` are the EFFECTIVE sort
    (including a view's default) so the active column shows its arrow. Each link resets
    the page cursor(s) in `page_reset`."""
    resets = {k: 1 for k in page_reset}
    ths = []
    for key, label in cols:
        if not key:
            ths.append("<th>{}</th>".format(html.escape(label)))
            continue
        if key == active:
            newdir = "asc" if direction == "desc" else "desc"
            arrow = " &#9660;" if direction == "desc" else " &#9650;"
        else:
            newdir, arrow = "desc", ""
        href = qs_fn(**{sort_param: key, dir_param: newdir, **resets})
        ths.append('<th><a href="{}">{}{}</a></th>'.format(
            href, html.escape(label), arrow))
    return "<tr>{}</tr>".format("".join(ths))


def _sortable_table(params, cols, rows, qs_fn, active, direction, hot=None,
                    sort_param="sort", dir_param="dir", page_reset=("page",)):
    """A table whose headers are click-to-sort links (cols = [(key, label)]; a None
    key is a plain header). Cells render row[key] via _fmt. `hot` tints a row."""
    header = _sort_headers(params, cols, qs_fn, active, direction,
                           sort_param, dir_param, page_reset)
    out = ["<table>" + header]
    for r in rows:
        c = ' class="hot"' if hot and hot(r) else ""
        cells = "".join("<td>{}</td>".format(html.escape(_fmt(r.get(k))))
                        for k, _ in cols)
        out.append("<tr{}>{}</tr>".format(c, cells))
    out.append("</table>")
    if not rows:
        out.append("<p class='muted'>Nothing to report this window.</p>")
    return "".join(out)


def _fmt(v):
    if v is None:
        return "-"
    if isinstance(v, float):
        return "{:,.0f}".format(v) if abs(v) >= 1000 else "{:.2f}".format(v)
    if isinstance(v, list):
        return ",".join(str(x) for x in v)
    return str(v)


def _money(v):
    """Signed compact dollars: +$1.2M, -$340.0K, +$820."""
    a, sign = abs(v), ("-" if v < 0 else "+")
    for div, suf in ((1e9, "B"), (1e6, "M"), (1e3, "K")):
        if a >= div:
            return "{}${:.1f}{}".format(sign, a / div, suf)
    return "{}${:.0f}".format(sign, a)


def _money0(v):
    """Plain unsigned dollars with thousands separators; '-' for None."""
    return "-" if v is None else "${:,.0f}".format(v)


def _flow_cell(metric, v):
    """A net-flow table cell: 0 is neutral; positive (net bought) tints green,
    negative (net sold) red. Dollars are compacted; shares/insiders are counts."""
    if not v:
        return "0"
    cls = "pos" if v > 0 else "neg"
    if metric == "value":
        txt = _money(v)
    elif metric == "shares":
        txt = "{:+,.0f}".format(v)
    else:
        txt = "{:+d}".format(int(v))
    return '<span class="{}">{}</span>'.format(cls, txt)


# ---------------------------------------------------------------- views
def _params(qsd):
    def one(k, default=None):
        return qsd.get(k, [default])[0]
    theme = (one("theme") or "").lower().strip()
    pp = one("per_page")
    pg = one("page")
    sp = one("spage")
    side = (one("side") or "").lower()
    plan = (one("plan") or "").lower()
    scope = (one("scope") or "").lower()
    src = (one("src") or "").lower()
    metric = (one("metric") or "").lower()

    def _sortkey(v):   # column keys are [a-z0-9_]; anything else -> no sort
        v = (v or "").lower().strip()
        return v if v and all(c.isalnum() or c == "_" for c in v) else ""

    def _dir(v):
        v = (v or "").lower().strip()
        return v if v in ("asc", "desc") else "desc"

    filer = "".join(ch for ch in (one("filer") or "") if ch.isdigit())
    per = (one("period") or "").strip()
    period = per if (len(per) == 10 and per[4] == "-" and per[7] == "-"
                     and per.replace("-", "").isdigit()) else ""
    cticker = "".join(c for c in (one("cticker") or "").upper()
                      if c.isalnum() or c in ".-")[:10]
    cinstr = (one("cinstr") or "").upper()
    cowner = (one("cowner") or "").lower()
    p = {"window": _int(one("window"), 90), "floor": _int(one("floor"), 3),
         "anchor": one("anchor") or dt.date.today().isoformat(),
         "symbol": (one("symbol") or "").upper().strip(),
         "theme": theme if theme in ("dark", "light") else "",
         "per_page": int(pp) if pp in ("25", "50", "100", "250", "500") else 100,
         "page": int(pg) if (pg or "").isdigit() and int(pg) >= 1 else 1,
         "spage": int(sp) if (sp or "").isdigit() and int(sp) >= 1 else 1,
         "side": side if side in ("buy", "sell", "all") else "buy",
         "plan": plan if plan in ("all", "discretionary", "planned") else "all",
         "scope": scope if scope in ("all", "scoped") else "scoped",
         "src": src if src in ("congress", "13f", "form4") else "all",
         "metric": metric if metric in ("value", "shares") else "value",
         "sort": _sortkey(one("sort")), "dir": _dir(one("dir")),
         "ssort": _sortkey(one("ssort")), "sdir": _dir(one("sdir")),
         "filer": filer, "period": period,
         "cticker": cticker, "cinstr": cinstr if cinstr in ("SH", "OP") else "SH",
         "cowner": cowner if cowner in ("self", "spouse", "dependent", "joint") else "all",
         "smid": one("smid") == "1",
         "capit": one("capit") == "1"}
    return p


def _int(v, d):
    try:
        return max(1, min(3650, int(v)))
    except (TypeError, ValueError):
        return d


def _tracked_books_strip(books):
    cards = []
    for b in books["filers"]:
        if not b["period"]:
            cards.append("<div class='book'><b>{}</b><br><span class='muted'>no "
                         "filings</span></div>".format(html.escape(b["name"] or "?")))
            continue
        top = ", ".join("{} {}%".format(
            html.escape(t["ticker"]), "?" if t["pct"] is None else t["pct"])
            for t in b["top3"]) or "-"
        d2f = b["days_to_filing"]
        d2f_txt = ("filing window passed" if d2f is not None and d2f < 0 else
                   "{}d to next filing".format(d2f) if d2f is not None else "-")
        cards.append(
            "<div class='book'><a href='/portfolios?filer={cik}'><b>{name}</b></a><br>"
            "<span class='muted'>{per} &middot; {book}</span><br>top3 {top}<br>"
            "<span class='muted'>{d2f}</span></div>".format(
                cik=q.cik_int(b["cik"]), name=html.escape(b["name"] or "?"),
                per=html.escape(b["period"]), book=_money0(b["book_value"]),
                top=top, d2f=d2f_txt))
    return ("<h2>Tracked books (reported 13F) &middot; "
            "<a href='/portfolios'>open</a></h2><div class='strip'>{}</div>".format(
                "".join(cards)))


def view_front(con, p):
    sent = q.q_sentinel_log(con, window=p["window"] * 2, anchor=p["anchor"])
    conv = q.q_principal_convergence(con)
    press = q.q_ownership_pressure(con, "all", p["window"], p["anchor"])
    ev = q.q_positioning_events(since=q._win(p["anchor"], p["window"]), overlay_only=True)
    body = [
        "<p class='muted'>as-of {}. Front page = tracked books, sentinels, principal "
        "convergence, ownership pressure, overlay-flagged events. Clusters are "
        "context.</p>".format(html.escape(press["as_of"])),
        _tracked_books_strip(q.q_tracked_books(con, anchor=p["anchor"])),
        "<h2>Sentinel activity ({} events)</h2>".format(sent["count"]),
        _table(["event_date", "src", "seed", "ticker", "action"], sent["rows"][:15]),
        "<h2>Principal convergence — {} same-side, {} QoQ disagreements</h2>".format(
            len(conv["convergences"]), len(conv["qoq_accumulate_distribute_disagreements"])),
        _table(["ticker", "period", "converge_dir", "long_filers"], conv["convergences"][:12]),
        "<h2>Ownership pressure — flow (top movers)</h2>",
        _table(["ticker", "net_shares", "distinct_buyers", "distinct_sellers", "direction"],
               press["rows"][:15]),
        "<h2>Overlay-flagged positioning events ({})</h2>".format(ev["count"]),
        _table(["disclosure_date", "leg", "ticker", "side", "person"], ev["rows"][:15]),
    ]
    return _page("Smart Money — front page", "".join(body), p)


def _sentinel_filter_form(params):
    opts = ("all", "congress", "13f", "form4")
    o = "".join("<option value='{}'{}>{}</option>".format(
        v, " selected" if v == params["src"] else "", v) for v in opts)
    return (
        "<form method='get'>source <select name='src'>{o}</select> "
        "<input type='hidden' name='window' value='{w}'>"
        "<input type='hidden' name='anchor' value='{a}'>"
        "<input type='hidden' name='per_page' value='{pp}'>"
        "<input type='hidden' name='page' value='1'>"
        "<input type='hidden' name='theme' value='{t}'>"
        "<button>apply</button></form>"
        "<div class='muted'><a href='/sentinels{clr}'>clear filters</a> "
        "&middot; resets source, window, and paging to defaults</div>"
    ).format(o=o, w=params["window"], a=html.escape(params["anchor"]),
             pp=params["per_page"], t=html.escape(params.get("theme") or ""),
             clr=("?theme=" + params["theme"]) if params.get("theme") else "")


def _cluster_filter_form(params):
    return (
        "<form method='get'>"
        "<label><input type='checkbox' name='capit' value='1'{c}> "
        "capitulation only</label> "
        "<input type='hidden' name='window' value='{w}'>"
        "<input type='hidden' name='anchor' value='{a}'>"
        "<input type='hidden' name='floor' value='{f}'>"
        "<input type='hidden' name='per_page' value='{pp}'>"
        "<input type='hidden' name='page' value='1'>"
        "<input type='hidden' name='spage' value='1'>"
        "<input type='hidden' name='theme' value='{t}'>"
        "<button>apply</button></form>"
        "<div class='muted'><a href='/clusters{clr}'>clear filters</a> "
        "&middot; resets floor, capitulation, window, and paging to defaults</div>"
    ).format(c=" checked" if params["capit"] else "", w=params["window"],
             a=html.escape(params["anchor"]), f=params["floor"],
             pp=params["per_page"], t=html.escape(params.get("theme") or ""),
             clr=("?theme=" + params["theme"]) if params.get("theme") else "")


def _cluster_data(con, p):
    """The two cluster feeds AND their post-filter row lists, from one place so the
    on-screen tables and the CSV export never drift. Buy clusters honor the
    capitulation-only toggle; the sell feed keeps its >=3-seller baseline gate."""
    cc = q.q_cluster_context(con, window=p["window"] * 2, floor=p["floor"], anchor=p["anchor"])
    buy = cc["rows"]
    if p["capit"]:
        buy = [r for r in buy if r.get("capitulation")]
    sd = q.q_sell_anomaly(con, window=p["window"], anchor=p["anchor"])
    sell = [r for r in sd["rows"] if r["distinct_sellers_12mo"] >= 3]
    return cc, buy, sd, sell


def view_clusters(con, p):
    cc, buy, sd, sell = _cluster_data(con, p)
    buy_active = p["sort"] or "n_buyers"          # buy table sorts on sort/dir
    sell_active = p["ssort"] or "rate_ratio"      # sell table sorts on ssort/sdir
    buy = _sorted(buy, buy_active, p["dir"])
    sell = _sorted(sell, sell_active, p["sdir"])
    buy_rows, buy_meta = _page_slice(buy, p["per_page"], p["page"])
    buy_meta["csv_extra"] = {"which": "buy"}
    sell_rows, sell_meta = _page_slice(sell, p["per_page"], p["spage"])
    sell_meta["csv_extra"] = {"which": "sell"}
    qs_fn = lambda **kw: _qs(p, **kw)
    buy_cols = [(c, c) for c in ("ticker", "n_buyers", "n_buys", "span_days",
                                 "calendar_months", "capitulation")]
    sell_cols = [(c, c) for c in ("ticker", "rate_ratio", "distinct_sellers_window",
                                  "distinct_sellers_12mo", "window_sell_value", "elevated")]
    body = [
        "<p class='muted'>CONTEXT, never alerts. Buy clusters with the capitulation "
        "timeline; sell feed ranked by rate ratio (elevated tint at {}). Click any "
        "column header to sort that table.</p>".format(sd["elevated_ratio"]),
        _cluster_filter_form(p),
        "<div class='expand'>{}</div>".format(
            _per_page_selector(p, ("page", "spage"))),
        "<h2>Buy clusters (floor {}{}, {} matching)</h2>".format(
            p["floor"], ", capitulation only" if p["capit"] else "", buy_meta["total"]),
        "<div class='expand'>{}</div>".format(
            _page_nav(p, buy_meta, "/clusters.csv", "page")),
        _sortable_table(p, buy_cols, buy_rows, qs_fn, buy_active, p["dir"],
                        hot=lambda r: r.get("capitulation"),
                        sort_param="sort", dir_param="dir", page_reset=("page",)),
        "<h2>Sell context feed (ranked by rate ratio, {} matching)</h2>".format(
            sell_meta["total"]),
        "<div class='expand'>{}</div>".format(
            _page_nav(p, sell_meta, "/clusters.csv", "spage")),
        _sortable_table(p, sell_cols, sell_rows, qs_fn, sell_active, p["sdir"],
                        hot=lambda r: r.get("elevated"),
                        sort_param="ssort", dir_param="sdir", page_reset=("spage",)),
    ]
    return _page("Smart Money — cluster context", "".join(body), p)


def _sentinel_data(con, p):
    """Sentinel log plus its source-filtered rows — shared by the view and CSV."""
    sent = q.q_sentinel_log(con, window=p["window"] * 4, anchor=p["anchor"])
    rows = sent["rows"]
    if p["src"] != "all":
        rows = [r for r in rows if r.get("src") == p["src"]]
    return sent, rows


def view_sentinels(con, p):
    sent, rows = _sentinel_data(con, p)
    active = p["sort"] or "event_date"            # default: newest event first
    rows = _sorted(rows, active, p["dir"])
    page_rows, meta = _page_slice(rows, p["per_page"], p["page"])
    cols = [(c, c) for c in ("event_date", "src", "seed", "role", "ticker",
                             "action", "value")]
    body = ["<p class='muted'>Registry as-of {}. {} events{}. Click a column header "
            "to sort.</p>".format(
                html.escape(str(sent["registry_as_of"])), meta["total"],
                "" if p["src"] == "all" else " in source " + html.escape(p["src"])),
        _sentinel_filter_form(p),
        _pager(p, meta, "/sentinels.csv"),
        _sortable_table(p, cols, page_rows, lambda **kw: _qs(p, **kw), active, p["dir"]),
        _pager(p, meta, "/sentinels.csv")]
    return _page("Smart Money — sentinel log", "".join(body), p)


def view_ticker(con, p):
    sym = p["symbol"]
    if not sym:
        return _page("Smart Money — ticker", "<p>Add <code>?symbol=TICKER</code> "
                     "to the URL or the box below.</p><form method='get'>"
                     "symbol <input name='symbol'><button>go</button></form>", p)
    t = q.q_ticker_panel(con, sym)
    ov = t["overlay"]
    body = [
        "<p class='muted'>{} — overlay conviction={} watchlist={}. "
        "13F net = long+call-put; congress amounts are bands.</p>".format(
            html.escape(sym), ov["conviction"], ov["watchlist"]),
        "<h2>Insider (Form 4) by code</h2>",
        _table(["code", "plan_flag", "n", "shares", "value", "distinct_filers"],
               t["insider_by_code"]),
        "<h2>Ownership pressure (flow)</h2>",
        _table(["net_shares", "distinct_buyers", "distinct_sellers", "direction"],
               t["ownership_pressure"]),
        "<h2>Congressional</h2>",
        _table(["name", "side", "amt_low", "amt_high", "tx_date", "disclosure_date", "owner"],
               t["congress"][:25]),
        "<h2>13F principal positions (direction-netted)</h2>",
        _table(["cik", "period", "net_value"], t["thirteenf_net"][:25]),
        "<p class='muted'>Price sparkline points {} (direct read-only SELECT).</p>".format(
            len(t["price_sparkline"])),
    ]
    return _page("Smart Money — {}".format(sym), "".join(body), p)


def _page_slice(rows, per_page, page):
    """Slice a full row list to one page. Returns (page_rows, meta) where meta has
    per_page / page (clamped into range) / pages / total. View-level pagination —
    the cluster and sentinel queries return the whole list cheaply (no per-row
    price enrichment, unlike trades which paginates inside the query)."""
    total = len(rows)
    pages = max(1, (total + per_page - 1) // per_page)
    page = min(max(1, page), pages)
    off = (page - 1) * per_page
    return rows[off:off + per_page], {"per_page": per_page, "page": page,
                                      "pages": pages, "total": total}


def _per_page_selector(params, reset_keys=("page",)):
    """The 25/50/100/250/500 row-count selector. reset_keys lets a size change
    reset every page cursor on the view (Clusters has two independent tables)."""
    pp = params["per_page"]
    resets = {k: 1 for k in reset_keys}
    sizes = []
    for n in (25, 50, 100, 250, 500):
        sizes.append("<b>{}</b>".format(n) if n == pp
                     else '<a href="{}">{}</a>'.format(_qs(params, per_page=n, **resets), n))
    return "per page: " + " ".join(sizes)


def _page_nav(params, meta, csv_base, page_key="page"):
    """prev / page X of Y / next plus this-page and whole-dataset CSV links for one
    table. page_key names this table's cursor so several tables can share a page."""
    pg, pages, total = meta["page"], meta["pages"], meta["total"]
    nav = []
    if pg > 1:
        nav.append('<a href="{}">&laquo; prev</a>'.format(_qs(params, **{page_key: pg - 1})))
    nav.append("page {} of {}".format(pg, pages))
    if pg < pages:
        nav.append('<a href="{}">next &raquo;</a>'.format(_qs(params, **{page_key: pg + 1})))
    extra = dict(meta.get("csv_extra") or {})
    csv_page = '<a href="{}{}">this page CSV</a>'.format(csv_base, _qs(params, **extra))
    csv_all = '<a href="{}{}">whole dataset CSV</a>'.format(
        csv_base, _qs(params, full="1", **extra))
    return ("{nav} &nbsp;&middot;&nbsp; {total} total &nbsp;&middot;&nbsp; export: "
            "{cp} | {ca}".format(nav=" ".join(nav), total=total, cp=csv_page, ca=csv_all))


def _pager(params, meta, csv_base="/trades.csv", page_key="page"):
    """Combined selector + nav for a single-table view (Trades, Sentinels)."""
    if "total" not in meta:                       # trades query uses total_matching
        meta = dict(meta, total=meta["total_matching"])
    return "<div class='expand'>{} &nbsp;&middot;&nbsp; {}</div>".format(
        _per_page_selector(params, (page_key,)),
        _page_nav(params, meta, csv_base, page_key))


def _trade_filter_form(params):
    def sel(name, cur, opts):
        o = "".join("<option value='{}'{}>{}</option>".format(
            v, " selected" if v == cur else "", v) for v in opts)
        return "{} <select name='{}'>{}</select>".format(name, name, o)
    return (
        "<form method='get'>{side} {plan} "
        "<label><input type='checkbox' name='smid' value='1'{smid}> SMID only</label> "
        "window <input name='window' value='{w}' size='4'> "
        "<input type='hidden' name='per_page' value='{pp}'>"
        "<input type='hidden' name='page' value='1'>"
        "<input type='hidden' name='anchor' value='{a}'>"
        "<input type='hidden' name='theme' value='{t}'>"
        "<input type='hidden' name='scope' value='{sc}'>"
        "<button>apply</button></form>"
        "<div class='muted'><a href='/trades{clr}'>clear filters</a> "
        "&middot; clears every filter and overlay (all sides, all plans, all "
        "issuers, 90d, 100/page)</div>"
    ).format(side=sel("side", params["side"], ("buy", "sell", "all")),
             plan=sel("plan", params["plan"], ("all", "discretionary", "planned")),
             smid=" checked" if params["smid"] else "",
             w=params["window"], pp=params["per_page"],
             a=html.escape(params["anchor"]), t=html.escape(params.get("theme") or ""),
             sc=html.escape(params["scope"]),
             clr="?side=all&plan=all&scope=all" + (
                 "&theme=" + params["theme"] if params.get("theme") else ""))


def view_trades(con, p):
    active = p["sort"] or "trade_date"            # default: newest trade first
    res = q.q_insider_trades(con, side=p["side"], window=p["window"],
                             anchor=p["anchor"], plan=p["plan"], smid_only=p["smid"],
                             per_page=p["per_page"], page=p["page"], scope=p["scope"],
                             sort=p["sort"], direction=p["dir"])
    if p["scope"] == "all":
        scope_line = ('scope: <b>all issuers</b> &middot; <a href="{}">overlay + '
                      'network issuers only</a>'.format(_qs(p, scope="scoped")))
    else:
        scope_line = ('scope: <b>overlay + network issuers</b> (book, watch, '
                      'trump, thiel) &middot; <a href="{}">show all '
                      'issuers</a>'.format(_qs(p, scope="all")))
    # entry / latest / % since are enriched only for the current page, so they are not
    # server-sortable (None key -> plain header); the raw columns are click-to-sort.
    tcols = [("person", "person"), ("ticker", "ticker"), ("side", "side"),
             ("trade_date", "trade date"), ("reported_date", "reported"),
             ("value", "value"), (None, "entry"), (None, "latest"), (None, "% since")]
    header = _sort_headers(p, tcols, lambda **kw: _qs(p, **kw), active, p["dir"])
    body = ["<p class='muted'>Amendment-deduped Form 4 open-market trades. Trade Date "
            "is the transaction date; a red Reported Date means the trade date was "
            "unavailable and the filing date is shown instead. % since = entry close "
            "on the trade date vs latest close. Click a column header to sort (entry / "
            "latest / % since sort the current page only).</p>",
            "<p class='muted'>{}</p>".format(scope_line),
            _trade_filter_form(p), _pager(p, res),
            "<table>" + header]
    for t in res["rows"]:
        dcell = ('<span class="reported">{} (Reported)</span>'.format(
                    html.escape(str(t["trade_date"] or "-")))
                 if t["date_is_reported"] else html.escape(str(t["trade_date"] or "-")))
        badges = ""
        if t.get("provenance"):
            badges += '<span class="badge prov-{p}">{p}</span>'.format(p=t["provenance"])
        if t["plan_10b5_1"]:
            badges += '<span class="badge plan">10b5-1</span>'
        if t["smid_band"] in ("micro", "small", "mid"):
            badges += '<span class="badge smid">{}</span>'.format(t["smid_band"])
        if t["pct_since_trade"] is None:
            pcell = "-"
        else:
            pcell = '<span class="{}">{:+.1%}</span>'.format(
                "pos" if t["pct_since_trade"] >= 0 else "neg", t["pct_since_trade"])
        lag = "" if t["lag_days"] is None else " (+{}d)".format(t["lag_days"])
        body.append(
            "<tr><td>{p}</td><td>{tk}{b}</td><td>{s}</td><td>{d}</td><td>{r}{lag}</td>"
            "<td>{v}</td><td>{e}</td><td>{l}</td><td>{pct}</td></tr>".format(
                p=html.escape(str(t["person"] or "-")),
                tk=html.escape(str(t["ticker"] or "-")), b=badges,
                s=html.escape(t["side"]), d=dcell,
                r=html.escape(str(t["reported_date"] or "-")), lag=lag,
                v=_fmt(t["value"]), e=_fmt(t["entry_close"]),
                l=_fmt(t["latest_close"]), pct=pcell))
    body.append("</table>")
    if not res["rows"]:
        body.append("<p class='muted'>No trades match this window and filter.</p>")
    else:
        body.append(_pager(p, res))
    return _page("Insider trades — {} / {}".format(p["side"], p["plan"]),
                 "".join(body), p)


_FLOW_TF = [("7", "7d"), ("30", "30d"), ("90", "90d"), ("180", "180d"),
            ("365", "365d"), ("all", "all-time")]
_SEC_LABEL = {"value": "$", "shares": "sh"}
_SEC_NAME = {"value": "net $", "shares": "net shares"}


def _flow_filter_form(params):
    def sel(name, cur, opts):
        o = "".join("<option value='{}'{}>{}</option>".format(
            v, " selected" if v == cur else "", lbl) for v, lbl in opts)
        return "{} <select name='{}'>{}</select>".format(name, name, o)
    return (
        "<form method='get'>secondary column {metric} "
        "<input type='hidden' name='anchor' value='{a}'>"
        "<input type='hidden' name='per_page' value='{pp}'>"
        "<input type='hidden' name='page' value='1'>"
        "<input type='hidden' name='scope' value='{sc}'>"
        "<input type='hidden' name='theme' value='{t}'>"
        "<button>apply</button></form>"
        "<div class='muted'><a href='/flows{clr}'>clear filters</a> "
        "&middot; resets the secondary column, scope, sort, and paging to "
        "defaults</div>"
    ).format(metric=sel("metric", params["metric"],
                        [("value", "$ value"), ("shares", "shares")]),
             a=html.escape(params["anchor"]), pp=params["per_page"],
             sc=html.escape(params["scope"]), t=html.escape(params.get("theme") or ""),
             clr=("?theme=" + params["theme"]) if params.get("theme") else "")


def view_flows(con, p):
    res = q.q_net_flows(con, anchor=p["anchor"], scope=p["scope"])
    sec = p["metric"]                    # value | shares -> column shown beside insiders
    # Default sort follows the displayed secondary metric (net $ by default, since
    # p["metric"] defaults to value) so the board opens on all-time net dollars,
    # most net-bought first. The parse-time sanity guard quarantines corrupt Form 4
    # dollar values, so net $ is now trustworthy enough to lead; net insiders stays
    # one click away. Keying off the displayed metric keeps the sort column visible.
    active = p["sort"] or (sec + "_all")
    rows = _sorted(res["rows"], active, p["dir"])
    page_rows, meta = _page_slice(rows, p["per_page"], p["page"])
    if p["scope"] == "all":
        scope_line = ('scope: <b>all scraped securities</b> &middot; <a href="{}">'
                      'overlay only</a>'.format(_qs(p, scope="scoped")))
    else:
        scope_line = ('scope: <b>overlay securities only</b> &middot; <a href="{}">'
                      'all scraped</a>'.format(_qs(p, scope="all")))
    cols = [("ticker", "ticker")]
    for tf, lbl in _FLOW_TF:
        cols.append(("persons_" + tf, lbl + " ins"))
        cols.append((sec + "_" + tf, lbl + " " + _SEC_LABEL[sec]))
    header = _sort_headers(p, cols, lambda **kw: _qs(p, **kw), active, p["dir"])
    trs = ["<table class='wide'>" + header]
    for r in page_rows:
        cells = ["<td>{}</td>".format(html.escape(r["ticker"]))]
        for tf, _ in _FLOW_TF:
            cells.append("<td>{}</td>".format(_flow_cell("persons", r["persons_" + tf])))
            cells.append("<td>{}</td>".format(_flow_cell(sec, r[sec + "_" + tf])))
        trs.append("<tr>{}</tr>".format("".join(cells)))
    trs.append("</table>")
    if not page_rows:
        trs.append("<p class='muted'>No scraped securities match this scope.</p>")
    excl = res.get("rows_excluded") or 0
    guard = ("" if not excl else
             " {} trade(s) with corrupt Form 4 price/value/share data were dropped "
             "from net $ and net shares; insider counts are unaffected.".format(excl))
    body = [
        "<p class='muted'>Net insider Form 4 flow per scraped security — buys "
        "(code P) minus sells (code S) over nested lookbacks, anchored at {}. Each "
        "timeframe shows net <b>insiders</b> (distinct buyers minus sellers) and net "
        "<b>{}</b> side by side; green = net bought, red = net sold. Click any column "
        "header to sort. <b>Note:</b> net $/shares exclude rows with corrupt Form 4 "
        "price or value data quarantined at ingest; a rare unit-mismatch residual may "
        "remain, so treat extreme outliers with care.{}"
        "</p>".format(html.escape(res["anchor"]), _SEC_NAME[sec], guard),
        "<p class='muted'>{}</p>".format(scope_line),
        _flow_filter_form(p),
        _pager(p, meta, "/flows.csv"),
        "<div style='overflow-x:auto'>" + "".join(trs) + "</div>",
        _pager(p, meta, "/flows.csv"),
    ]
    return _page("Smart Money — net flows", "".join(body), p)


_PORT_CAVEAT = (
    "<p class='muted'><b>Reported book only</b> — long US-listed 13F positions; no "
    "shorts, cash, or private holdings. Marks are quarter-end and up to 45 days stale. "
    "Unmapped CUSIPs are kept and counted, never dropped; their % of book is shown. "
    "Single-filing filers get no QoQ deltas. put-heavy is option notional, never a "
    "short position.</p>")


def _portfolio_filter_form(params, res):
    def sel(name, cur, opts):
        o = "".join("<option value='{}'{}>{}</option>".format(
            html.escape(str(v)), " selected" if str(v) == str(cur) else "",
            html.escape(str(lbl))) for v, lbl in opts)
        return "{} <select name='{}'>{}</select>".format(name, name, o)
    filers = [(str(q.cik_int(c)), n) for c, n in res.get("filers", [])]
    cur_filer = str(q.cik_int(res["filer_cik"])) if res.get("filer_cik") else ""
    periods = [(pp, pp) for pp in res.get("periods", [])]
    return (
        "<form method='get'>filer {filer} period {period} "
        "<input type='hidden' name='per_page' value='{pp}'>"
        "<input type='hidden' name='page' value='1'>"
        "<input type='hidden' name='theme' value='{t}'>"
        "<button>view</button></form>"
        "<div class='muted'><a href='/portfolios{clr}'>reset</a> "
        "&middot; defaults to the first filer, latest period</div>"
    ).format(filer=sel("filer", cur_filer, filers),
             period=(sel("period", res.get("period") or "", periods)
                     if periods else "<i>none</i>"),
             pp=params["per_page"], t=html.escape(params.get("theme") or ""),
             clr=("?theme=" + params["theme"]) if params.get("theme") else "")


def view_portfolios(con, p):
    res = q.q_portfolio(con, filer_cik=p["filer"] or None, period=p["period"] or None)
    active = p["sort"] or "value"                 # default: largest position first
    rows = _sorted(res["rows"], active, p["dir"])
    page_rows, meta = _page_slice(rows, p["per_page"], p["page"])
    cols = [("ticker", "ticker"), ("issuer", "issuer"), ("instrument", "instr"),
            ("value", "value"), ("shares", "shares"), ("pct_of_book", "% book"),
            ("badge", "QoQ")]
    header = _sort_headers(p, cols, lambda **kw: _qs(p, **kw), active, p["dir"])
    trs = ["<table>" + header]
    for r in page_rows:
        tk = (html.escape(r["ticker"]) if r["ticker"] else
              "<span class='muted'>unmapped {}</span>".format(html.escape(r["cusip"])))
        badge = ("" if not r["badge"] else
                 "<span class='badge qoq-{b}'>{b}</span>".format(b=r["badge"]))
        pct = "-" if r["pct_of_book"] is None else "{:.2f}%".format(r["pct_of_book"])
        trs.append(
            "<tr><td>{tk}</td><td>{iss}</td><td>{ins}</td><td>{val}</td><td>{sh}</td>"
            "<td>{pct}</td><td>{b}</td></tr>".format(
                tk=tk, iss=html.escape(str(r["issuer"] or "-")),
                ins=html.escape(r["instrument"]), val=_money0(r["value"]),
                sh=_fmt(r["shares"]), pct=pct, b=badge))
    trs.append("</table>")
    if not page_rows:
        trs.append("<p class='muted'>No holdings for this filer/period.</p>")
    if not res.get("periods"):
        head = ("<p class='muted'><b>{}</b> — no reported holdings in "
                "thirteenf_holdings.</p>".format(html.escape(res.get("filer_name") or "?")))
    else:
        dn = "" if res["has_deltas"] else " &middot; <b>single filing</b> — no QoQ deltas"
        head = (
            "<p class='muted'><b>{name}</b> &middot; period <b>{per}</b>{prior} "
            "&middot; reported book <b>{book}</b>{dn}</p>"
            "<p class='muted'>direction-netted: long <b>{lng}</b> &middot; put-notional "
            "<b>{put}</b> &middot; call-notional <b>{call}</b> &middot; {unm} unmapped "
            "CUSIP(s) = {unmv}. Click a column header to sort.</p>".format(
                name=html.escape(res["filer_name"] or "?"),
                per=html.escape(res["period"]),
                prior=(" (QoQ vs " + html.escape(res["prior_period"]) + ")"
                       if res["prior_period"] else ""),
                book=_money0(res["book_value"]), dn=dn,
                lng=_money0(res["long_value"]), put=_money0(res["put_notional"]),
                call=_money0(res["call_notional"]), unm=res["unmapped_count"],
                unmv=_money0(res["unmapped_value"])))
    body = [head, _PORT_CAVEAT, _portfolio_filter_form(p, res),
            _pager(p, meta, "/portfolios.csv"), "".join(trs),
            _pager(p, meta, "/portfolios.csv")]
    return _page("Reported portfolio", "".join(body), p)


_PORT_CSV_COLS = ["cusip", "ticker", "issuer", "instrument", "value", "shares",
                  "pct_of_book", "badge", "prior_value"]


def _build_portfolio_csv(con, p, full):
    """CSV of the reported-portfolio holdings — current page or whole set — same
    filer/period/sort as the on-screen view."""
    import csv
    import io
    res = q.q_portfolio(con, filer_cik=p["filer"] or None, period=p["period"] or None)
    rows = _sorted(res["rows"], p["sort"] or "value", p["dir"])
    if not full:
        rows = _page_slice(rows, p["per_page"], p["page"])[0]
    buf = io.StringIO()
    w = csv.DictWriter(buf, fieldnames=_PORT_CSV_COLS, extrasaction="ignore")
    w.writeheader()
    for r in rows:
        w.writerow(r)
    return buf.getvalue()


_CONGRESS_CAVEAT = (
    "<p class='muted'><b>Annual snapshot, not continuous</b> — one FD report per member "
    "per year (filed ~May), so it lags live trading; PTRs are the flow between snapshots. "
    "<b>Band-valued</b>: exposure is a coarse band-midpoint proxy, not a mark. Non-security "
    "assets (funds without a ticker, real property) are excluded from ticker breadth; parse "
    "gaps are shown per row. Options count as distinct positions from the same-ticker stock. "
    "Owner = whose position it is (self / spouse / dependent / joint).</p>")


def _congress_owner_form(params):
    o = "".join("<option value='{}'{}>{}</option>".format(
        v, " selected" if v == params["cowner"] else "", v)
        for v in ("all", "self", "spouse", "joint", "dependent"))
    return (
        "<form method='get'>owner <select name='cowner'>{o}</select> "
        "<input type='hidden' name='per_page' value='{pp}'>"
        "<input type='hidden' name='page' value='1'>"
        "<input type='hidden' name='theme' value='{t}'>"
        "<button>apply</button></form>"
        "<div class='muted'><a href='/congress{clr}'>clear</a></div>"
    ).format(o=o, pp=params["per_page"], t=html.escape(params.get("theme") or ""),
             clr=("?theme=" + params["theme"]) if params.get("theme") else "")


def view_congress(con, p):
    qs_fn = lambda **kw: _qs(p, **kw)
    if p["cticker"]:                                  # holder drill-down = the matrix, made human
        res = q.q_congress_holders(con, p["cticker"], p["cinstr"])
        active = p["sort"] or "value_lo"
        rows = _sorted(res["rows"], active, p["dir"])
        page_rows, meta = _page_slice(rows, p["per_page"], p["page"])
        cols = [("member", "member"), ("state", "state"), ("owner", "owner"),
                ("value_lo", "band lo"), ("value_hi", "band hi")]
        trs = ["<table>" + _sort_headers(p, cols, qs_fn, active, p["dir"])]
        for r in page_rows:
            trs.append("<tr><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td></tr>".format(
                html.escape(r["member"]), html.escape(str(r["state"] or "-")),
                html.escape(r["owner"]), _money0(r["value_lo"]),
                _money0(r["value_hi"]) if r["value_hi"] else "open"))
        trs.append("</table>")
        body = ["<p class='muted'><a href='/congress'>&laquo; breadth board</a> &middot; "
                "<b>{} {}</b> — {} positions across members (latest filing each). Click a "
                "column to sort.</p>".format(html.escape(res["ticker"]), res["instrument"],
                                             res["count"]),
                _CONGRESS_CAVEAT, _pager(p, meta, "/congress.csv"), "".join(trs)]
        return _page("Congress holders — {}".format(res["ticker"]), "".join(body), p)
    res = q.q_congress_breadth(con, min_holders=2, owner_filter=p["cowner"])
    active = p["sort"] or "holder_count"
    rows = _sorted(res["rows"], active, p["dir"])
    page_rows, meta = _page_slice(rows, p["per_page"], p["page"])
    dist = {}
    for r in res["rows"]:
        b = ("20+" if r["holder_count"] >= 20 else "10-19" if r["holder_count"] >= 10
             else "5-9" if r["holder_count"] >= 5 else "2-4")
        dist[b] = dist.get(b, 0) + 1
    cols = [("ticker", "ticker"), ("instrument", "instr"), ("holder_count", "holders"),
            ("self", "self"), ("spouse", "spouse"), ("joint", "joint"),
            ("dependent", "dep"), ("midpoint_exposure", "~exposure"),
            ("yoy_change", "YoY"), ("first_year", "first")]
    trs = ["<table>" + _sort_headers(p, cols, qs_fn, active, p["dir"])]
    for r in page_rows:
        tlink = "<a href=\"{}\">{}</a>".format(
            _qs(p, cticker=r["ticker"], cinstr=r["instrument"], sort=None, dir=None, page=1),
            html.escape(r["ticker"]))
        yoy = "-" if r["yoy_change"] is None else "{:+d}".format(r["yoy_change"])
        trs.append(
            "<tr><td>{tk}</td><td>{ins}</td><td>{hc}</td><td>{s}</td><td>{sp}</td><td>{jt}"
            "</td><td>{dp}</td><td>{ex}</td><td>{yy}</td><td>{fy}</td></tr>".format(
                tk=tlink, ins=r["instrument"], hc=r["holder_count"], s=r["self"],
                sp=r["spouse"], jt=r["joint"], dp=r["dependent"],
                ex=_money0(r["midpoint_exposure"]), yy=yoy, fy=r["first_year"] or "-"))
    trs.append("</table>")
    diststr = " &middot; ".join("{}: {}".format(k, dist[k])
                                for k in ("20+", "10-19", "5-9", "2-4") if k in dist)
    body = ["<p class='muted'>Who holds what across Congress — one row per ticker+instrument; "
            "holder_count = distinct members in their latest annual FD. <b>Distribution-first</b>: "
            "mega-caps and index funds top raw breadth mechanically, so read the distribution, "
            "not the raw top — the signal is SMID names with outsized breadth and YoY change. "
            "Holder-count distribution ({} tickers held by ≥2): {}. Click a ticker for the "
            "holder list; click a column to sort.</p>".format(res["count"], diststr),
            _CONGRESS_CAVEAT, _congress_owner_form(p), _pager(p, meta, "/congress.csv"),
            "".join(trs), _pager(p, meta, "/congress.csv")]
    return _page("Congress breadth — who holds what", "".join(body), p)


def _build_congress_csv(con, p, full):
    """CSV of the congress surface — the breadth board or (with cticker) the holder list,
    honoring the active sort and owner filter, matching the on-screen page."""
    import csv
    import io
    if p["cticker"]:
        res = q.q_congress_holders(con, p["cticker"], p["cinstr"])
        rows = _sorted(res["rows"], p["sort"] or "value_lo", p["dir"])
        cols = ["member", "state", "owner", "value_lo", "value_hi"]
    else:
        res = q.q_congress_breadth(con, min_holders=2, owner_filter=p["cowner"])
        rows = _sorted(res["rows"], p["sort"] or "holder_count", p["dir"])
        cols = ["ticker", "instrument", "holder_count", "self", "spouse", "joint",
                "dependent", "midpoint_exposure", "yoy_change", "first_year"]
    if not full:
        rows = _page_slice(rows, p["per_page"], p["page"])[0]
    buf = io.StringIO()
    w = csv.DictWriter(buf, fieldnames=cols, extrasaction="ignore")
    w.writeheader()
    for r in rows:
        w.writerow(r)
    return buf.getvalue()


_CSV_COLS = ["person", "ticker", "side", "trade_date", "date_is_reported",
             "reported_date", "lag_days", "shares", "value", "plan_10b5_1",
             "entry_close", "latest_close", "pct_since_trade", "smid_band",
             "provenance"]


def _build_trades_csv(con, p, full):
    """CSV of the trades feed — the current page, or the whole dataset when
    full=True. Same query, same filters/scope as the on-screen view."""
    import csv
    import io
    res = q.q_insider_trades(con, side=p["side"], window=p["window"],
                             anchor=p["anchor"], plan=p["plan"], smid_only=p["smid"],
                             scope=p["scope"], per_page=p["per_page"], page=p["page"],
                             full=full, sort=p["sort"], direction=p["dir"])
    buf = io.StringIO()
    w = csv.DictWriter(buf, fieldnames=_CSV_COLS, extrasaction="ignore")
    w.writeheader()
    for row in res["rows"]:
        w.writerow(row)
    return buf.getvalue()


_SENTINEL_CSV_COLS = ["event_date", "src", "seed", "role", "ticker", "action",
                      "value", "tx_date", "period", "amt_low", "amt_high",
                      "lag_days", "owner"]


def _build_sentinels_csv(con, p, full):
    """CSV of the sentinel log — current page or whole dataset — same source filter
    and window as the on-screen view."""
    import csv
    import io
    _, rows = _sentinel_data(con, p)
    rows = _sorted(rows, p["sort"] or "event_date", p["dir"])   # match the view's sort
    if not full:
        rows, _ = _page_slice(rows, p["per_page"], p["page"])
    buf = io.StringIO()
    w = csv.DictWriter(buf, fieldnames=_SENTINEL_CSV_COLS, extrasaction="ignore")
    w.writeheader()
    for row in rows:
        w.writerow(row)
    return buf.getvalue()


_CLUSTER_BUY_COLS = ["ticker", "issuer_cik", "n_buyers", "n_buys", "window_start",
                     "event_filed", "span_days", "calendar_months", "capitulation",
                     "total_value"]
_CLUSTER_SELL_COLS = ["ticker", "issuer_cik", "distinct_sellers_window",
                      "distinct_sellers_12mo", "window_sell_value",
                      "expected_window_rate", "rate_ratio", "baseline_sufficient",
                      "elevated"]


def _build_clusters_csv(con, p, full, which):
    """CSV of one cluster table (which = buy | sell), current page or whole set.
    Same filters/gates as the on-screen view; each table pages independently."""
    import csv
    import io
    _, buy, _, sell = _cluster_data(con, p)
    if which == "sell":                            # match view_clusters' per-table sort
        rows = _sorted(sell, p["ssort"] or "rate_ratio", p["sdir"])
        cols, pg = _CLUSTER_SELL_COLS, p["spage"]
    else:
        rows = _sorted(buy, p["sort"] or "n_buyers", p["dir"])
        cols, pg = _CLUSTER_BUY_COLS, p["page"]
    if not full:
        rows, _ = _page_slice(rows, p["per_page"], pg)
    buf = io.StringIO()
    w = csv.DictWriter(buf, fieldnames=cols, extrasaction="ignore")
    w.writeheader()
    for row in rows:
        r = dict(row)
        if isinstance(r.get("calendar_months"), list):
            r["calendar_months"] = " ".join(r["calendar_months"])
        w.writerow(r)
    return buf.getvalue()


def _build_flows_csv(con, p, full):
    """CSV of the net-flow board — current page or whole set — all three metrics
    (insiders, $, shares) for every timeframe, in the same sort order as the view."""
    import csv
    import io
    res = q.q_net_flows(con, anchor=p["anchor"], scope=p["scope"])
    rows = _sorted(res["rows"], p["sort"] or (p["metric"] + "_all"), p["dir"])
    if not full:
        rows = _page_slice(rows, p["per_page"], p["page"])[0]
    hdr = ["ticker"]
    for tf, _ in _FLOW_TF:
        hdr += ["ins_" + tf, "val_" + tf, "sh_" + tf]
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(hdr)
    for r in rows:
        line = [r["ticker"]]
        for tf, _ in _FLOW_TF:
            line += [r["persons_" + tf], r["value_" + tf], r["shares_" + tf]]
        w.writerow(line)
    return buf.getvalue()


ROUTES = {"/": view_front, "/portfolios": view_portfolios, "/congress": view_congress,
          "/trades": view_trades, "/flows": view_flows, "/clusters": view_clusters,
          "/sentinels": view_sentinels, "/ticker": view_ticker}


# ---------------------------------------------------------------- server
class Handler(BaseHTTPRequestHandler):
    db_path = None

    def log_message(self, fmt, *args):  # quiet default logging to stderr line
        sys.stderr.write("[dash] {} {}\n".format(self.address_string(), fmt % args))

    def _send(self, code, body, ctype="text/html; charset=utf-8", headers=None):
        data = body if isinstance(body, bytes) else body.encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        for k, v in (headers or {}).items():
            self.send_header(k, v)
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):
        u = urlparse(self.path)
        p = _params(parse_qs(u.query))
        con = q.connect_ro(self.db_path)  # read-only, per request
        try:
            if u.path == "/brief.pdf":
                return self._brief(con, p)
            if u.path == "/trades.csv":
                return self._trades_csv(con, p, parse_qs(u.query))
            if u.path == "/sentinels.csv":
                return self._sentinels_csv(con, p, parse_qs(u.query))
            if u.path == "/clusters.csv":
                return self._clusters_csv(con, p, parse_qs(u.query))
            if u.path == "/flows.csv":
                return self._flows_csv(con, p, parse_qs(u.query))
            if u.path == "/portfolios.csv":
                return self._portfolios_csv(con, p, parse_qs(u.query))
            if u.path == "/congress.csv":
                return self._congress_csv(con, p, parse_qs(u.query))
            view = ROUTES.get(u.path)
            if not view:
                return self._send(404, _page("Not found", "<p>No such view.</p>", p))
            self._send(200, view(con, p))
        except Exception as exc:  # noqa: BLE001 - surface, never write
            self._send(500, _page("Error", "<pre>{}</pre>".format(
                html.escape(type(exc).__name__ + ": " + str(exc))), p))
        finally:
            con.close()

    def _trades_csv(self, con, p, qsd):
        full = qsd.get("full", ["0"])[0] == "1"
        data = _build_trades_csv(con, p, full)
        fname = "insider_trades_{}_{}_{}.csv".format(
            p["side"], p["scope"], "all" if full else "page{}".format(p["page"]))
        self._send(200, data, ctype="text/csv; charset=utf-8",
                   headers={"Content-Disposition":
                            'attachment; filename="{}"'.format(fname)})

    def _sentinels_csv(self, con, p, qsd):
        full = qsd.get("full", ["0"])[0] == "1"
        data = _build_sentinels_csv(con, p, full)
        fname = "sentinel_log_{}_{}.csv".format(
            p["src"], "all" if full else "page{}".format(p["page"]))
        self._send(200, data, ctype="text/csv; charset=utf-8",
                   headers={"Content-Disposition":
                            'attachment; filename="{}"'.format(fname)})

    def _clusters_csv(self, con, p, qsd):
        full = qsd.get("full", ["0"])[0] == "1"
        which = qsd.get("which", ["buy"])[0]
        which = which if which in ("buy", "sell") else "buy"
        data = _build_clusters_csv(con, p, full, which)
        pg = p["spage"] if which == "sell" else p["page"]
        fname = "clusters_{}_{}.csv".format(
            which, "all" if full else "page{}".format(pg))
        self._send(200, data, ctype="text/csv; charset=utf-8",
                   headers={"Content-Disposition":
                            'attachment; filename="{}"'.format(fname)})

    def _flows_csv(self, con, p, qsd):
        full = qsd.get("full", ["0"])[0] == "1"
        data = _build_flows_csv(con, p, full)
        fname = "net_flows_{}_{}_{}.csv".format(
            p["metric"], p["scope"], "all" if full else "page{}".format(p["page"]))
        self._send(200, data, ctype="text/csv; charset=utf-8",
                   headers={"Content-Disposition":
                            'attachment; filename="{}"'.format(fname)})

    def _portfolios_csv(self, con, p, qsd):
        full = qsd.get("full", ["0"])[0] == "1"
        data = _build_portfolio_csv(con, p, full)
        fname = "portfolio_{}_{}_{}.csv".format(
            p["filer"] or "default", p["period"] or "latest",
            "all" if full else "page{}".format(p["page"]))
        self._send(200, data, ctype="text/csv; charset=utf-8",
                   headers={"Content-Disposition":
                            'attachment; filename="{}"'.format(fname)})

    def _congress_csv(self, con, p, qsd):
        full = qsd.get("full", ["0"])[0] == "1"
        data = _build_congress_csv(con, p, full)
        what = "holders_{}".format(p["cticker"]) if p["cticker"] else "breadth_{}".format(p["cowner"])
        fname = "congress_{}_{}.csv".format(what, "all" if full else "page{}".format(p["page"]))
        self._send(200, data, ctype="text/csv; charset=utf-8",
                   headers={"Content-Disposition":
                            'attachment; filename="{}"'.format(fname)})

    def _brief(self, con, p):
        from . import brief
        tmp = os.path.join(tempfile.mkdtemp(), "brief.pdf")
        brief.render_brief(con, tmp, window=p["window"], anchor=p["anchor"],
                           floor=p["floor"])
        with open(tmp, "rb") as fh:
            data = fh.read()
        self._send(200, data, ctype="application/pdf")

    def do_POST(self):    # no writes, ever
        self._send(405, "method not allowed")
    do_PUT = do_DELETE = do_PATCH = do_POST


class _Dashboard(ThreadingHTTPServer):
    """HTTPServer.server_bind() calls socket.getfqdn(host), a reverse-DNS lookup
    that blocks for ~35s on this host when the resolver is slow — hanging startup.
    We never use the FQDN, so skip it and bind instantly regardless of DNS."""

    def server_bind(self):
        from socketserver import TCPServer
        TCPServer.server_bind(self)
        self.server_name = self.server_address[0]
        self.server_port = self.server_address[1]


def serve(db_path, host, port):
    if host in ("0.0.0.0", "::", ""):
        raise SystemExit("refusing to bind {} - LAN/Tailscale host only, never "
                         "a public interface".format(host or "<empty>"))
    Handler.db_path = os.path.expanduser(db_path)
    httpd = _Dashboard((host, port), Handler)
    sys.stderr.write("[dash] serving read-only on http://{}:{}\n".format(host, port))
    httpd.serve_forever()


def main(argv=None):
    ap = argparse.ArgumentParser(description="SM-R1 read-only dashboard")
    ap.add_argument("--db", default=dbmod.DB_PATH_DEFAULT)
    ap.add_argument("--host", default="127.0.0.1", help="LAN/Tailscale bind host, never 0.0.0.0")
    ap.add_argument("--port", type=int, default=8787)
    args = ap.parse_args(argv)
    serve(args.db, args.host, args.port)
    return 0


if __name__ == "__main__":
    sys.exit(main())
