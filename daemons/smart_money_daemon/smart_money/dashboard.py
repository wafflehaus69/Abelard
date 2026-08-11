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
from urllib.parse import parse_qs, unquote, urlparse

from . import db as dbmod
from . import queries as q

MEMBER_PREFIX = "/congress/member/"

NAV = [("/", "Front"), ("/portfolios", "Portfolios"), ("/insiders", "Insider books"),
       ("/disagreements", "Disagreements"), ("/congress", "Congress"),
       ("/committees", "Committees"), ("/oge", "OGE 278e"),
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
    # Restricted-source styling. The per-row tag is small but always visible, and the
    # page banner is unmissable — this source carries a statutory use restriction.
    ".restrict{font-size:10px;letter-spacing:.02em;color:var(--reported);"
    "white-space:nowrap}"
    ".restrict-banner{border:2px solid var(--reported);border-left-width:6px;"
    "padding:.7rem .9rem;margin:.6rem 0;background:var(--hot-bg);color:var(--hot-fg);"
    "line-height:1.45}"
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
                 ssort=None, sdir=None, cticker=None, cinstr=None, cowner=None,
                 member=None, myear=None)
    nav = " &middot; ".join(
        '<a href="{}{}">{}</a>'.format(href, nav_qs, html.escape(label))
        for href, label in NAV)
    # The print button renders THIS page, not always the front-page brief. `view` rides
    # on the querystring so the handler knows which surface the reader is looking at.
    _vpath = params.get("_path") or "/"
    _sep = "&" if qs else "?"
    printbtn = '<a class="print" href="/brief.pdf{}{}view={}">Print {} (PDF)</a>'.format(
        qs, _sep, html.escape(_vpath.lstrip("/") or "front"),
        "page" if _vpath not in ("/", "") else "brief")
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
    base["ctf"] = params.get("ctf")
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
    base["member"] = params.get("member") or None
    base["myear"] = params.get("myear") or None
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


# Column NAMES that hold dollars. Formatting is decided centrally by name rather than at
# each call site, because "value" appears on eight surfaces and they had drifted into
# three different renderings of the same quantity.
_MONEY_COLS = frozenset((
    "value", "total_value", "book_value", "midpoint_exposure", "floor_exposure",
    "net_value", "window_sell_value", "value_all", "prior_value",
    "exec_price", "implied_price", "entry_close", "latest_close", "close",
    "price", "underlying_close", "deduplicated_economic_value",
))
# Per-share prices want cents; totals do not. $18.00 and $1,000,984,274 are both money,
# but rounding the first to $18 loses the number that matters.
_PRICE_COLS = frozenset((
    "exec_price", "implied_price", "entry_close", "latest_close", "close", "price",
    "underlying_close",
))


def _money_cell(key, v):
    """Dollar-format a cell when its COLUMN is a money column, else fall back to _fmt."""
    if key not in _MONEY_COLS or v is None or isinstance(v, (str, list, bool)):
        return _fmt(v)
    try:
        n = float(v)
    except (TypeError, ValueError):
        return _fmt(v)
    if key in _PRICE_COLS:
        return "${:,.2f}".format(n)
    return "${:,.0f}".format(n)


def _table(cols, rows, hot=None):
    head = "".join("<th>{}</th>".format(html.escape(c)) for c in cols)
    out = ["<table><tr>{}</tr>".format(head)]
    for r in rows:
        cls = ' class="hot"' if hot and hot(r) else ""
        cells = "".join("<td>{}</td>".format(html.escape(_money_cell(c, r.get(c))))
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
        cells = "".join("<td>{}</td>".format(html.escape(_money_cell(k, r.get(k))))
                        for k, _ in cols)
        out.append("<tr{}>{}</tr>".format(c, cells))
    out.append("</table>")
    if not rows:
        out.append("<p class='muted'>Nothing to report this window.</p>")
    return "".join(out)


def _fmt(v):
    if v is None:
        return "-"
    if isinstance(v, bool):
        return str(v)
    if isinstance(v, float):
        return "{:,.0f}".format(v) if abs(v) >= 1000 else "{:.2f}".format(v)
    # Integers were falling through to str() and rendering as 2572732 while the same
    # quantity stored as a float rendered 2,572,732 — the separator depended on the
    # column's storage type rather than on the number.
    if isinstance(v, int):
        return "{:,}".format(v)
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
    # Committee ids are thomas_ids (HSAG, HSAG15) - alnum only, so a hostile value can
    # never reach the query as anything but a miss.
    cmte = "".join(c for c in (one("cmte") or "").upper() if c.isalnum())[:16]
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
         "filer": filer, "period": period, "cmte": cmte,
         "cticker": cticker, "cinstr": cinstr if cinstr in ("SH", "OP") else "SH",
         "cowner": cowner if cowner in ("self", "spouse", "dependent", "joint") else "all",
         "member": (one("member") or "")[:120],
         "myear": _int(one("myear"), None),
         "smid": one("smid") == "1",
         "capit": one("capit") == "1",
         "ctf": (one("ctf") if one("ctf") in _CLUSTER_TF_VALUES else "180")}
    return p


def _int(v, d):
    try:
        return max(1, min(3650, int(v)))
    except (TypeError, ValueError):
        return d


def _tracked_books_strip(books):
    cards = []
    ordered = sorted(books["filers"], key=lambda b: -(b.get("book_value") or 0))
    for b in ordered:
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
    # SM-P2: at 16 filers the strip swamps the front page. Show the biggest books,
    # collapse the rest behind a native <details> (no JS).
    TOP_N = 6
    head, rest = cards[:TOP_N], cards[TOP_N:]
    more = ("<details><summary>{} more tracked book(s)</summary>"
            "<div class='strip'>{}</div></details>".format(len(rest), "".join(rest))
            if rest else "")
    return ("<h2>Tracked books (reported 13F) &middot; "
            "<a href='/portfolios'>open</a></h2><div class='strip'>{}</div>{}".format(
                "".join(head), more))


def _disagreements_section(con, limit=8):
    """ORDER SM-P2 flagship: same ticker, opposite QoQ direction, across tracked filers."""
    res = q.q_opposed_pairs(con)
    if not res["rows"]:
        return ("<h2>Disagreements (cross-manager)</h2><p class='muted'>No opposed pairs "
                "across {} tracked filers with two or more reported periods. A filer with "
                "a single filing cannot express a direction and is excluded rather than "
                "guessed at.</p>".format(res["filers_compared"]))
    trs = ["<table><tr><th>ticker</th><th>instr</th><th>accumulating</th>"
           "<th>distributing</th><th>split</th></tr>"]
    for r in res["rows"][:limit]:
        x = ("<span class='badge qoq-new'>cross-thesis</span>" if r["cross_thesis"] else "")
        trs.append(
            "<tr><td><a href='/ticker?symbol={t}'>{t}</a> {x}</td><td>{ins}</td>"
            "<td>{acc}</td><td>{dis}</td><td>{na}v{nd}</td></tr>".format(
                t=html.escape(r["ticker"]), x=x, ins=r["instrument"],
                acc=html.escape(r["acc_names"])[:90],
                dis=html.escape(r["dis_names"])[:90],
                na=r["n_accumulating"], nd=r["n_distributing"]))
    trs.append("</table>")
    return ("<h2>Disagreements (cross-manager) &middot; "
            "<a href='/disagreements'>all {n}</a></h2>"
            "<p class='muted'>Same ticker, <b>opposite QoQ direction</b>, across the "
            "tracked 13F shelf — one manager accumulating while another distributes. "
            "Ranked by disagreement breadth, <b>no verdict on who is right</b>. 13F is "
            "long-only US-listed and 45 days stale, and each side is that filer's own "
            "newest period, so sides can be slightly off-phase.</p>{tbl}".format(
                n=res["count"], tbl="".join(trs)))


def view_disagreements(con, p):
    """Full cross-manager disagreement board."""
    res = q.q_opposed_pairs(con)
    active = p["sort"] or "n_managers"
    rows = _sorted(res["rows"], active, p["dir"])
    page_rows, meta = _page_slice(rows, p["per_page"], p["page"])
    cols = [("ticker", "ticker"), ("instrument", "instr"),
            ("n_accumulating", "#acc"), ("n_distributing", "#dis"),
            ("acc_names", "accumulating"), ("dis_names", "distributing"),
            ("acc_value", "acc $"), ("dis_value", "dis $")]
    trs = ["<table>" + _sort_headers(p, cols, lambda **kw: _qs(p, **kw), active, p["dir"])]
    for r in page_rows:
        trs.append(
            "<tr><td><a href='/ticker?symbol={t}'>{t}</a></td><td>{ins}</td><td>{na}</td>"
            "<td>{nd}</td><td>{acc}</td><td>{dis}</td><td>{av}</td><td>{dv}</td></tr>"
            .format(t=html.escape(r["ticker"]), ins=r["instrument"],
                    na=r["n_accumulating"], nd=r["n_distributing"],
                    acc=html.escape(r["acc_names"]), dis=html.escape(r["dis_names"]),
                    av=_money0(r["acc_value"]), dv=_money0(r["dis_value"])))
    trs.append("</table>")
    body = ["<p class='muted'>{n} tickers where tracked managers moved in OPPOSITE "
            "directions in their newest reported period, across {f} filers with enough "
            "history to express a direction. {note}. Click a column to sort.</p>".format(
                n=res["count"], f=res["filers_compared"], note=html.escape(res["note"])),
            _pager(p, meta, "/disagreements.csv"), "".join(trs),
            _pager(p, meta, "/disagreements.csv")]
    return _page("Cross-manager disagreements", "".join(body), p)


_DIS_CSV_COLS = ["ticker", "instrument", "n_accumulating", "n_distributing",
                 "acc_names", "dis_names", "acc_value", "dis_value", "cross_thesis"]


def _build_disagreements_csv(con, p, full):
    import csv
    import io
    res = q.q_opposed_pairs(con)
    rows = _sorted(res["rows"], p["sort"] or "n_managers", p["dir"])
    if not full:
        rows = _page_slice(rows, p["per_page"], p["page"])[0]
    buf = io.StringIO()
    w = csv.DictWriter(buf, fieldnames=_DIS_CSV_COLS, extrasaction="ignore")
    w.writeheader()
    for r in rows:
        w.writerow(r)
    return buf.getvalue()


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
        _disagreements_section(con),
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


# Cluster lookback choices. "all" is genuinely unbounded rather than a large number, so
# an old filing is never silently outside the search.
_CLUSTER_TF = [("7", "7d"), ("30", "30d"), ("90", "3mo"), ("180", "6mo"),
               ("365", "1y"), ("all", "all")]
_CLUSTER_TF_VALUES = {v for v, _ in _CLUSTER_TF}


def _cluster_tf_form(params):
    opts = "".join(
        "<option value='{v}'{sel}>{l}</option>".format(
            v=v, l=l, sel=" selected" if v == params["ctf"] else "")
        for v, l in _CLUSTER_TF)
    hidden = "".join(
        "<input type='hidden' name='{k}' value='{v}'>".format(k=k, v=html.escape(str(v)))
        for k, v in (("floor", params["floor"]), ("anchor", params["anchor"]),
                     ("per_page", params["per_page"]),
                     ("capit", "1" if params["capit"] else ""),
                     ("theme", params.get("theme") or "")) if v not in (None, ""))
    return ("<form method='get' style='margin:.4rem 0'>cluster lookback "
            "<select name='ctf' onchange='this.form.submit()'>{o}</select> "
            "<button>apply</button>{h}</form>".format(o=opts, h=hidden))


def _cluster_data(con, p):
    """The two cluster feeds AND their post-filter row lists, from one place so the
    on-screen tables and the CSV export never drift. Buy clusters honor the
    capitulation-only toggle; the sell feed keeps its >=3-seller baseline gate."""
    cc = q.q_cluster_context(con, floor=p["floor"], anchor=p["anchor"],
                             lookback=p["ctf"])
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
    # total_value was computed by the query and never displayed; a cluster's size in
    # dollars is the first thing you want next to its buyer count.
    buy_cols = [(c, c) for c in ("ticker", "n_buyers", "n_buys", "total_value",
                                 "span_days", "calendar_months", "capitulation")]
    sell_cols = [(c, c) for c in ("ticker", "rate_ratio", "distinct_sellers_window",
                                  "distinct_sellers_12mo", "window_sell_value", "elevated")]
    body = [
        "<p class='muted'>CONTEXT, never alerts. Buy clusters with the capitulation "
        "timeline; sell feed ranked by rate ratio (elevated tint at {}). Click any "
        "column header to sort that table.</p>".format(sd["elevated_ratio"]),
        _cluster_filter_form(p),
        _cluster_tf_form(p),
        "<div class='expand'>{}</div>".format(
            _per_page_selector(p, ("page", "spage"))),
        # The two periods are different things and both belong in the heading: how far
        # back we LOOKED, and how close together buys must fall to count as one cluster.
        "<h2>Buy clusters &mdash; {look} lookback (floor {f}{c}, {n} matching)</h2>".format(
            look=("all time" if cc["lookback"] == "all"
                  else "{}d".format(cc["lookback"])),
            f=p["floor"], c=", capitulation only" if p["capit"] else "",
            n=buy_meta["total"]),
        "<p class='muted'>A cluster is {sp} or fewer days apart with at least {f} "
        "distinct discretionary buyers on one issuer. The <b>lookback</b> above sets how "
        "far back that search runs; it does not change what counts as a cluster. "
        "Searching from {st}.<br><b>capitulation</b> = every buy in the cluster landed "
        "in ONE calendar month, i.e. the insiders moved together rather than "
        "accumulating over time. It is a TIMING test only &mdash; nothing here looks at "
        "the share price, so it does not claim the stock had fallen, and two buys either "
        "side of a month boundary read as separate even when they are days apart. "
        "<b>span_days</b> is the plainer version of the same question.</p>".format(
            sp=cc["cluster_span_days"], f=p["floor"],
            st=("the start of the corpus" if cc["lookback"] == "all"
                else cc["lookback_start"])),
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


def _tension_block(t, level):
    """SM-C3 Phase X: do the surfaces disagree, and is the disagreement even legible?"""
    if not t:
        return ""
    verdict = ("<b>TENSION</b> &mdash; the surfaces disagree" if t["tension"]
               else "<b>Agreement</b> &mdash; both read {}".format(t["consensus"])
               if t["agreement"]
               else "<b>No read</b> &mdash; fewer than two surfaces express a direction")
    rows = []
    for k in ("insider", "managers", "congress"):
        leg = t["legs"][k]
        votes = k in t["surfaces_with_direction"]
        rows.append(
            "<tr><td>{k}</td><td>{d}</td><td>{b}</td><td>{s}</td><td>{a}</td>"
            "<td>{v}</td><td class='muted'>{u}</td></tr>".format(
                k=k, d=html.escape(leg["direction"]), b=_fmt(leg["buy"]),
                s=_fmt(leg["sell"]), a=html.escape(str(leg["as_of"] or "-")),
                v="votes" if votes else "&mdash;", u=html.escape(leg["unit"])))
    spread = ("<b>{} days</b> between the freshest and stalest voting surface. ".format(
        t["as_of_spread_days"]) if t["as_of_spread_days"] is not None else "")
    return (
        "<h2>Three-surface tension ({}d window)</h2>"
        "<p>{v}.</p>"
        "<table><tr><th>surface</th><th>direction</th><th>buy</th><th>sell</th>"
        "<th>as of</th><th>counts?</th><th>unit</th></tr>{rows}</table>"
        "<p class='muted'><b>Congressional breadth does not vote here.</b> {n} members "
        "disclosed this in their latest annual (house {h}, senate {s}; FD years {y}) "
        "&mdash; but that is a <b>level</b>, and a holder count falls identically when a "
        "member sells and when a member simply has not filed. Direction comes from PTR "
        "flows only; the level rides as context.</p>"
        "<p class='muted'>{sp}Form 4 lands ~2 days after the trade, a PTR is due within "
        "45 and often later, a 13F is 45 days stale and marked at a quarter end. A "
        "disagreement across those clocks may be the calendar rather than the "
        "parties.</p>".format(
            t["window_days"], v=verdict, rows="".join(rows),
            n=level["holder_count"], h=level["house"], s=level["senate"],
            y=", ".join(str(x) for x in level["anchor_years"]) or "-", sp=spread))


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
        "<h2>Insider activity by transaction type &mdash; all time</h2>",
        "<p class='muted'>Form 4 uses single-letter codes; the plain-English meaning is "
        "in <b>what</b>. <b>cash</b> = no means the shares moved without a purchase at a "
        "market price (an option exercise, a grant, a gift), so a value of 0.00 on those "
        "rows is expected, not missing data. Only <b>Open-market buy</b> is a "
        "discretionary cash purchase. <b>This table covers the issuer's WHOLE filing "
        "history</b> &mdash; the pressure and flow figures below are windowed, so a large "
        "buy count here alongside zero buyers below is a difference of period, not a "
        "contradiction.</p>",
        _table(["code", "what", "cash", "10b5-1", "filings", "shares", "value",
                "filers"], t["insider_by_code"]),
        "<h2>Ownership pressure &mdash; last {}d</h2>".format(
            t.get("pressure_window_days") or 180),
        _table(["net_shares", "distinct_buyers", "distinct_sellers", "direction"],
               t["ownership_pressure"]),
        "<h2>Congressional</h2>",
        _table(["name", "side", "amt_low", "amt_high", "tx_date", "disclosure_date", "owner"],
               t["congress"][:25]),
        "<h2>13F principal positions (direction-netted)</h2>",
        _table(["cik", "period", "net_value"], t["thirteenf_net"][:25]),
        _tension_block(t["tension"], t["congress_holdings"]),
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

    def grouped_filer_sel(cur):
        parts = []
        for tag in sorted(groups):
            opts = "".join("<option value='{}'{}>{}</option>".format(
                html.escape(v), " selected" if v == str(cur) else "", html.escape(lbl))
                for v, lbl, _sz in sorted(groups[tag], key=lambda x: -x[2]))
            parts.append("<optgroup label='{}'>{}</optgroup>".format(
                html.escape(tag), opts))
        return "filer <select name='filer'>{}</select>".format("".join(parts))
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
    # "paid" is the EXECUTION price from the filing; "close" is the market close on the
    # trade date. They are different numbers and the old single "entry" column showed only
    # the second one, which read as the first.
    tcols = [("person", "person"), ("ticker", "ticker"), ("side", "side"),
             ("trade_date", "trade date"), ("reported_date", "reported"),
             ("value", "value"), ("exec_price", "paid"), (None, "close"),
             (None, "latest"), (None, "insider %")]
    header = _sort_headers(p, tcols, lambda **kw: _qs(p, **kw), active, p["dir"])
    body = ["<p class='muted'>Amendment-deduped Form 4 open-market trades. Trade Date "
            "is the transaction date; a red Reported Date means the trade date was "
            "unavailable and the filing date is shown instead. <b>paid</b> is the "
            "execution price from the filing; <b>close</b> is the market close on the "
            "trade date - they are different numbers, and a bold <b>paid</b> means the "
            "two are 10%+ apart. <b>insider %</b> is the return from what the insider "
            "PAID, not from the close; it is greyed with a warning when the share basis "
            "is inconsistent (splits, ADRs) and must not be ranked on. Click a column "
            "header to sort (close / latest / insider % sort the current page only).</p>",
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
        # The INSIDER's return, from what they actually paid - not the stock's return
        # from the trade-date close. When the share basis is inconsistent the number is
        # shown struck-through with its reason rather than hidden, so the reader can see
        # both that something moved and that it cannot be ranked on.
        ir = t["insider_return"]
        if ir is None:
            pcell = "-"
        elif not t["return_rankable"]:
            pcell = ('<span class="muted" title="{w}">{v:+.0%} &#9888;</span>'.format(
                w=html.escape(str(t["return_basis_warning"] or "")), v=ir))
        else:
            pcell = '<span class="{}">{:+.1%}</span>'.format(
                "pos" if ir >= 0 else "neg", ir)
        qflag = ""
        if t.get("value_quality"):
            qflag = ' <span class="badge warn" title="{q}">REVIEW</span>'.format(
                q=html.escape(t["value_quality"]))
        if t.get("cofiling_suspected"):
            qflag += (' <span class="badge" title="same block reported by {n} filers">'
                      'CO-FILED x{n}</span>'.format(n=t["cofiler_count"]))
        if t.get("issuer_class") != "operating":
            qflag += ' <span class="badge">{}</span>'.format(
                html.escape((t.get("issuer_class") or "").replace("_", " ")))
        # The execution price, highlighted when it is materially away from the close -
        # a below-market allocation (BRVE at $18 into a $30 close) is a real signal and
        # was invisible while the table showed only the close.
        px = t["exec_price"]
        if px is None:
            pxcell = "-"
        else:
            vsc = t["price_vs_close_pct"]
            pxcell = _money_cell("exec_price", px)
            if vsc is not None and abs(vsc) >= 0.10:
                pxcell = ('<b title="{d:+.0%} vs the trade-date close">{v}</b>'
                          .format(d=vsc, v=_money_cell("exec_price", px)))
        lag = "" if t["lag_days"] is None else " (+{}d)".format(t["lag_days"])
        body.append(
            "<tr><td>{p}</td><td>{tk}{b}</td><td>{s}</td><td>{d}</td><td>{r}{lag}</td>"
            "<td>{v}</td><td>{px}</td><td>{e}</td><td>{l}</td><td>{pct}</td></tr>".format(
                p=html.escape(str(t["person"] or "-")),
                tk=html.escape(str(t["ticker"] or "-")), b=badges + qflag,
                s=html.escape(t["side"]), d=dcell,
                r=html.escape(str(t["reported_date"] or "-")), lag=lag,
                v=_money_cell("value", t["value"]), px=pxcell,
                e=_money_cell("entry_close", t["entry_close"]),
                l=_money_cell("latest_close", t["latest_close"]), pct=pcell))
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

    def grouped_filer_sel(cur):
        parts = []
        for tag in sorted(groups):
            opts = "".join("<option value='{}'{}>{}</option>".format(
                html.escape(v), " selected" if v == str(cur) else "", html.escape(lbl))
                for v, lbl, _sz in sorted(groups[tag], key=lambda x: -x[2]))
            parts.append("<optgroup label='{}'>{}</optgroup>".format(
                html.escape(tag), opts))
        return "filer <select name='filer'>{}</select>".format("".join(parts))
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

    def grouped_filer_sel(cur):
        parts = []
        for tag in sorted(groups):
            opts = "".join("<option value='{}'{}>{}</option>".format(
                html.escape(v), " selected" if v == str(cur) else "", html.escape(lbl))
                for v, lbl, _sz in sorted(groups[tag], key=lambda x: -x[2]))
            parts.append("<optgroup label='{}'>{}</optgroup>".format(
                html.escape(tag), opts))
        return "filer <select name='filer'>{}</select>".format("".join(parts))
    # SM-P2: at 16 filers a flat list is unreadable — group by thesis tag, and inside
    # each group order by book size so the biggest books lead.
    thesis = q._filer_thesis()
    sizes = res.get("filer_book_sizes") or {}
    groups = {}
    for c, n in res.get("filers", []):
        groups.setdefault(thesis.get(c) or "unclassified", []).append(
            (str(q.cik_int(c)), n, sizes.get(str(q.cik_int(c))) or 0))
    filers = [(str(q.cik_int(c)), n) for c, n in res.get("filers", [])]
    cur_filer = str(q.cik_int(res["filer_cik"])) if res.get("filer_cik") else ""
    periods = [(pp, pp) for pp in res.get("periods", [])]
    return (
        "<form method='get'>{filer} period {period} "
        "<input type='hidden' name='per_page' value='{pp}'>"
        "<input type='hidden' name='page' value='1'>"
        "<input type='hidden' name='theme' value='{t}'>"
        "<button>view</button></form>"
        "<div class='muted'><a href='/portfolios{clr}'>reset</a> "
        "&middot; defaults to the first filer, latest period</div>"
    ).format(filer=grouped_filer_sel(cur_filer),
             period=(sel("period", res.get("period") or "", periods)
                     if periods else "<i>none</i>"),
             pp=params["per_page"], t=html.escape(params.get("theme") or ""),
             clr=("?theme=" + params["theme"]) if params.get("theme") else "")


def view_portfolios(con, p):
    res = q.q_portfolio(con, filer_cik=p["filer"] or None, period=p["period"] or None)
    # book sizes for the thesis-grouped selector (SM-P2: order each group by book size)
    try:
        res["filer_book_sizes"] = {
            str(q.cik_int(b["cik"])): (b.get("book_value") or 0)
            for b in q.q_tracked_books(con, anchor=p["anchor"])["filers"]}
    except Exception:
        res["filer_book_sizes"] = {}
    active = p["sort"] or "value"                 # default: largest position first
    rows = _sorted(res["rows"], active, p["dir"])
    page_rows, meta = _page_slice(rows, p["per_page"], p["page"])
    cols = [("ticker", "ticker"), ("issuer", "issuer"), ("instrument", "instr"),
            ("value", "value"), ("shares", "shares"), ("pct_of_book", "% book"),
            ("badge", "QoQ"), ("reported_period", "as of"), ("filed_date", "filed")]
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
            "<td>{pct}</td><td>{b}</td><td>{per}</td><td>{fil}</td></tr>".format(
                tk=tk, iss=html.escape(str(r["issuer"] or "-")),
                ins=html.escape(r["instrument"]), val=_money0(r["value"]),
                sh=_fmt(r["shares"]), pct=pct, b=badge,
                per=html.escape(str(r["reported_period"] or "-")),
                fil=html.escape(str(r["filed_date"] or "-"))))
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
            "CUSIP(s) = {unmv}. Per position, <b>as of</b> = the quarter-end the holding "
            "is reported for and <b>filed</b> = when that filing reached EDGAR; the gap "
            "between them is the disclosure lag, and an exited row carries the dates of "
            "the last filing it appeared in. Click a column header to sort.</p>".format(
                name=html.escape(res["filer_name"] or "?"),
                per=html.escape(res["period"]),
                prior=(" (QoQ vs " + html.escape(res["prior_period"]) + ")"
                       if res["prior_period"] else ""),
                book=_money0(res["book_value"]), dn=dn,
                lng=_money0(res["long_value"]), put=_money0(res["put_notional"]),
                call=_money0(res["call_notional"]), unm=res["unmapped_count"],
                unmv=_money0(res["unmapped_value"])))
    # SM-P2 G1 gate: an unanchored unit scale or an implausible book means these figures
    # may be 1000x wrong. Say so loudly rather than letting them read as trustworthy.
    warn = res.get("magnitude_warning")
    banner = ("<p class='restrict-banner'><b>UNIT SCALE NOT VERIFIED</b><br>{}</p>".format(
        html.escape(warn)) if warn else "")
    body = [banner, head, _PORT_CAVEAT, _portfolio_filter_form(p, res),
            _pager(p, meta, "/portfolios.csv"), "".join(trs),
            _pager(p, meta, "/portfolios.csv")]
    return _page("Reported portfolio", "".join(body), p)


_PORT_CSV_COLS = ["cusip", "ticker", "issuer", "instrument", "value", "shares",
                  "pct_of_book", "badge", "prior_value", "reported_period", "filed_date"]


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
        cols = [("member", "member"), ("chamber", "chamber"), ("state", "state"),
                ("owner", "owner"), ("value_lo", "band lo"), ("value_hi", "band hi")]
        trs = ["<table>" + _sort_headers(p, cols, qs_fn, active, p["dir"])]
        for r in page_rows:
            trs.append(
                "<tr><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td></tr>".format(
                    html.escape(r["member"]), html.escape(r["chamber"]),
                    html.escape(str(r["state"] or "-")), html.escape(r["owner"]),
                    _money0(r["value_lo"]),
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
            ("house", "House"), ("senate", "Senate"),
            ("dem", "D"), ("rep", "R"), ("ind", "I"), ("party_unknown", "party?"),
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
            "<tr><td>{tk}</td><td>{ins}</td><td>{hc}</td><td>{ho}</td><td>{se}</td>"
            "<td>{d}</td><td>{rp}</td><td>{iv}</td><td>{pu}</td><td>{s}</td>"
            "<td>{sp}</td><td>{jt}</td><td>{dp}</td><td>{ex}</td><td>{yy}</td><td>{fy}</td></tr>"
            .format(tk=tlink, ins=r["instrument"], hc=r["holder_count"], ho=r["house"],
                    se=r["senate"], d=r["dem"], rp=r["rep"], iv=r["ind"],
                    pu=r["party_unknown"],
                    s=r["self"], sp=r["spouse"], jt=r["joint"],
                    dp=r["dependent"], ex=_money0(r["midpoint_exposure"]), yy=yoy,
                    fy=r["first_year"] or "-"))
    trs.append("</table>")
    diststr = " &middot; ".join("{}: {}".format(k, dist[k])
                                for k in ("20+", "10-19", "5-9", "2-4") if k in dist)
    body = ["<p class='muted'>Who holds what across Congress — one row per ticker+instrument; "
            "holder_count = distinct members in their latest annual FD. <b>Distribution-first</b>: "
            "mega-caps and index funds top raw breadth mechanically, so read the distribution, "
            "not the raw top — the signal is SMID names with outsized breadth and YoY change. "
            "Holder-count distribution ({} tickers held by ≥2): {}. D/R/I = party of the "
            "holding members (<b>party?</b> = not resolvable to a sitting member, mostly "
            "candidates who filed but never served — never guessed). Click a ticker for "
            "the holder list; click a column to sort. "
            "<a href='/congress_gaps'>Coverage roll &raquo;</a> — who is behind these "
            "counts, and why they are floors. "
            "<a href='/breadth_yoy'>Breadth change &raquo;</a> — what moved between "
            "annual cycles, on the members who filed both.</p>".format(
                res["count"], diststr),
            _CONGRESS_CAVEAT, _congress_owner_form(p), _pager(p, meta, "/congress.csv"),
            "".join(trs), _pager(p, meta, "/congress.csv")]
    return _page("Congress breadth — who holds what", "".join(body), p)


_INSIDER_CAVEAT = (
    "<p class='muted'><b>Annual snapshot, band-valued</b> — one FD per member per year "
    "(filed ~May), so it lags live trading; every figure is a COARSE band midpoint, never "
    "a mark. <b>Owner is the point, not a footnote</b>: several members do little trading "
    "in their own name and the positions sit with a spouse, so a book that is mostly "
    "spouse-owned is the same disclosure surface — read the owner split in the header. "
    "Only roster-CONFIRMED members appear here; candidates who filed a disclosure but "
    "never served are excluded.</p>")


def _insider_filter_form(p, res):
    ms = "".join("<option value='{}'{}>{}</option>".format(
        html.escape(m["key"]), " selected" if m["key"] == res.get("member_key") else "",
        html.escape(m["label"])) for m in res.get("members") or [])
    ys = "".join("<option value='{}'{}>{}</option>".format(
        y, " selected" if y == res.get("year") else "", y)
        for y in res.get("years") or [])
    return ("<form method='get'>member <select name='member'>{ms}</select> "
            "year <select name='myear'>{ys}</select> "
            "<input type='hidden' name='per_page' value='{pp}'>"
            "<input type='hidden' name='page' value='1'>"
            "<input type='hidden' name='theme' value='{t}'>"
            "<button>apply</button></form>").format(
                ms=ms, ys=ys, pp=p["per_page"],
                t=html.escape(p.get("theme") or ""))


def view_insiders(con, p):
    """Political-insider reported book — the /portfolios layout, for a member of Congress."""
    res = q.q_member_book(con, member_key=p["member"] or None, year=p["myear"])
    active = p["sort"] or "midpoint"
    rows = _sorted(res["rows"], active, p["dir"])
    page_rows, meta = _page_slice(rows, p["per_page"], p["page"])
    cols = [("ticker", "ticker"), ("asset_name", "asset"), ("instrument", "instr"),
            ("owner", "owner"), ("midpoint", "~value"), ("value_lo", "band lo"),
            ("value_hi", "band hi"), ("pct_of_book", "% book")]
    trs = ["<table>" + _sort_headers(p, cols, lambda **kw: _qs(p, **kw), active, p["dir"])]
    for r in page_rows:
        tk = (html.escape(r["ticker"]) if r["ticker"] else "<span class='muted'>-</span>")
        own = ("<b>{}</b>".format(html.escape(r["owner"])) if r["owner"] != "self"
               else html.escape(r["owner"]))
        pct = "-" if r["pct_of_book"] is None else "{:.2f}%".format(r["pct_of_book"])
        trs.append(
            "<tr><td>{tk}</td><td>{an}</td><td>{ins}</td><td>{ow}</td><td>{mid}</td>"
            "<td>{lo}</td><td>{hi}</td><td>{pct}</td></tr>".format(
                tk=tk, an=html.escape(str(r["asset_name"] or "-"))[:70],
                ins=html.escape(r["instrument"]), ow=own, mid=_money0(r["midpoint"]),
                lo=_money0(r["value_lo"]),
                hi=_money0(r["value_hi"]) if r["value_hi"] else "open", pct=pct))
    trs.append("</table>")
    if not res.get("member"):
        head = ("<p class='muted'>No roster-confirmed members with holdings yet — sync the "
                "roster (python -m smart_money.roster) after ingesting FD holdings.</p>")
    else:
        split = res["owner_split"]
        sp = res.get("spouse_share")
        proxy = res.get("proxy_share")
        flag = ""
        if sp is not None and sp >= 50:
            flag = (" &middot; <b>{}% of this book is SPOUSE-owned</b> — the disclosure "
                    "surface here is largely not the member's own name".format(sp))
        head = (
            "<p class='muted'><b>{m}</b> &middot; {ch}{pt}{st} &middot; FD year <b>{yr}</b> "
            "&middot; {n} positions &middot; reported book <b>{bk}</b> (band midpoints)</p>"
            "<p class='muted'>owner split: {osp}{flag}. Click a column header to sort.</p>"
        ).format(m=html.escape(res["member"]), ch=html.escape(res["chamber"] or "?"),
                 pt=(" &middot; " + html.escape(res["party"])) if res.get("party") else "",
                 st=(" &middot; " + html.escape(res["state"])) if res.get("state") else "",
                 yr=res["year"], n=res["count"], bk=_money0(res["book_value"]),
                 osp=" &middot; ".join(
                     "{} {}".format(k, _money0(v))
                     for k, v in sorted(split.items(), key=lambda x: -x[1])) or "-",
                 flag=flag)
    body = [head, _INSIDER_CAVEAT, _insider_filter_form(p, res),
            _pager(p, meta, "/insiders.csv"), "".join(trs),
            _pager(p, meta, "/insiders.csv")]
    return _page("Political insider book", "".join(body), p)


_INSIDER_CSV_COLS = ["ticker", "asset_name", "instrument", "owner", "midpoint",
                     "value_lo", "value_hi", "pct_of_book", "income_type"]


def _build_insiders_csv(con, p, full):
    """CSV of the political-insider book, same member/year/sort as the screen."""
    import csv
    import io
    res = q.q_member_book(con, member_key=p["member"] or None, year=p["myear"])
    rows = _sorted(res["rows"], p["sort"] or "midpoint", p["dir"])
    if not full:
        rows = _page_slice(rows, p["per_page"], p["page"])[0]
    buf = io.StringIO()
    w = csv.DictWriter(buf, fieldnames=_INSIDER_CSV_COLS, extrasaction="ignore")
    w.writeheader()
    for r in rows:
        w.writerow(r)
    return buf.getvalue()


def view_oge(con, p):
    """OGE 278e executive-branch disclosure. RESTRICTED SOURCE — the use restriction is
    rendered on EVERY row, not just as a page banner, and travels into the CSV."""
    res = q.q_oge_holdings(con, filer=p["member"] or None)
    active = p["sort"] or "midpoint"
    rows = _sorted(res["rows"], active, p["dir"])
    page_rows, meta = _page_slice(rows, p["per_page"], p["page"])
    cols = [("line_no", "line"), ("description", "asset"), ("ticker", "ticker"),
            ("owner", "owner"), ("eif", "EIF"), ("midpoint", "~value"),
            ("value_lo", "band lo"), ("value_hi", "band hi"),
            ("use_restriction", "use restriction")]
    trs = ["<table>" + _sort_headers(p, cols, lambda **kw: _qs(p, **kw), active, p["dir"])]
    for r in page_rows:
        own = ("<b>{}</b>".format(html.escape(r["owner"])) if r["owner"] == "spouse"
               else html.escape(r["owner"]))
        trs.append(
            "<tr><td>{ln}</td><td>{d}</td><td>{tk}</td><td>{ow}</td><td>{eif}</td>"
            "<td>{mid}</td><td>{lo}</td><td>{hi}</td>"
            "<td><b class='restrict'>{res}</b></td></tr>".format(
                ln=html.escape(r["line_no"]),
                d=html.escape(str(r["description"] or "-"))[:80],
                tk=html.escape(r["ticker"] or "-"), ow=own,
                eif=html.escape(str(r["eif"] or "-")),
                mid=_money0(r["midpoint"]) if r["midpoint"] is not None else "-",
                lo=_money0(r["value_lo"]) if r["value_lo"] is not None else "-",
                hi=(_money0(r["value_hi"]) if r["value_hi"] is not None
                    else ("open" if r["value_lo"] is not None else "-")),
                res=html.escape(r["use_restriction"] or "")))
    trs.append("</table>")
    if not res["filers"]:
        body = ["<p class='muted'>No OGE 278e reports ingested. "
                "<code>python -m smart_money.oge_ingest --filer 'Warsh, Kevin'</code></p>"]
        return _page("OGE 278e disclosure", "".join(body), p)
    sel = "".join("<option value='{}'{}>{}</option>".format(
        html.escape(f), " selected" if f == res["filer"] else "", html.escape(f))
        for f in res["filers"])
    banner = (
        "<p class='restrict-banner'><b>{r}</b><br>"
        "Source: OGE Form 278e. Unlike every other source in this dashboard "
        "(SEC Form 4/13F, STOCK Act congressional filings — all unrestricted), these "
        "reports are restricted by the Ethics in Government Act, 5 U.S.C. app. "
        "&sect;&nbsp;105(c): unlawful to obtain or use for any commercial purpose (other "
        "than by news media for public dissemination), to set a credit rating, or to "
        "solicit money. Civil penalty up to $11,000. The tag on each row is provenance, "
        "not a permission — what keeps this lawful is the use staying non-commercial. "
        "This source is deliberately excluded from the scan, alert and enqueue path.</p>"
    ).format(r=html.escape(res["restriction"] or "RESTRICTED"))
    head = ("<p class='muted'><b>{f}</b> &middot; {n} disclosure lines &middot; {b} with a "
            "value band &middot; band midpoints are COARSE, never a mark. "
            "<b>FILER vs SPOUSE</b> is the report's own marking — a spouse-held book is "
            "the same disclosure surface.</p>".format(
                f=html.escape(res["filer"]), n=res["count"], b=res["banded"]))
    form = ("<form method='get'>filer <select name='member'>{s}</select> "
            "<input type='hidden' name='per_page' value='{pp}'>"
            "<input type='hidden' name='page' value='1'>"
            "<input type='hidden' name='theme' value='{t}'>"
            "<button>apply</button></form>").format(
                s=sel, pp=p["per_page"], t=html.escape(p.get("theme") or ""))
    body = [banner, head, form, _pager(p, meta, "/oge.csv"), "".join(trs),
            _pager(p, meta, "/oge.csv")]
    return _page("OGE 278e disclosure", "".join(body), p)


_OGE_CSV_COLS = ["line_no", "description", "ticker", "owner", "eif", "midpoint",
                 "value_lo", "value_hi", "income_type", "use_restriction"]


def _build_oge_csv(con, p, full):
    """CSV export. use_restriction is a REQUIRED column — the tag leaves with the data."""
    import csv
    import io
    res = q.q_oge_holdings(con, filer=p["member"] or None)
    rows = _sorted(res["rows"], p["sort"] or "midpoint", p["dir"])
    if not full:
        rows = _page_slice(rows, p["per_page"], p["page"])[0]
    buf = io.StringIO()
    w = csv.DictWriter(buf, fieldnames=_OGE_CSV_COLS, extrasaction="ignore")
    w.writeheader()
    for r in rows:
        w.writerow(r)
    return buf.getvalue()


_TIER_HELP = {
    "anchored": "as-reported in the annual and STALE — no PTR since",
    "anchored+flows": "annual position adjusted by later PTRs — an ESTIMATE",
    "flows-only": "PTR activity with no annual position — new since the annual",
    "flows>anchor": "sales exceed what the annual band could hold — FLAGGED, cause not "
                    "interpreted (band coarseness, owner mismatch, or a paper/unparsed "
                    "anchor year)",
}
_TIER_CLASS = {"anchored": "qoq-exited", "anchored+flows": "qoq-added",
               "flows-only": "qoq-new", "flows>anchor": "qoq-trimmed"}
_FUSION_CSV_COLS = ["ticker", "asset_name", "owner", "instrument", "tier", "anchor_lo",
                    "anchor_hi", "buy_flow", "sell_flow", "n_buy", "n_sell", "last_tx",
                    "estimate_lo", "estimate_hi", "unfusable"]


def _range(lo, hi):
    """A band is a RANGE. An open top stays open; nothing is collapsed to a point."""
    if lo is None and hi is None:
        return "<span class='muted'>-</span>"
    if hi is None:
        return "&ge; {}".format(_money0(lo))
    return "{} &ndash; {}".format(_money0(lo), _money0(hi))


def view_member(con, p):
    """SM-C3 Phase F — one member's book: annual anchor fused with later PTR flows."""
    res = q.q_member_fusion(con, member_key=p["member"] or None)
    if not res.get("member"):
        return _page("Member book", "<p class='muted'>No roster-confirmed members with "
                                    "holdings yet.</p>", p)
    active = p["sort"] or "estimate_lo"
    rows = _sorted(res["rows"], active, p["dir"])
    page_rows, meta = _page_slice(rows, p["per_page"], p["page"])
    cols = [("ticker", "ticker"), ("asset_name", "asset"), ("owner", "owner"),
            ("tier", "basis"), ("anchor_lo", "annual (as reported)"),
            ("buy_flow", "buys since"), ("sell_flow", "sells since"),
            ("estimate_lo", "estimate (range)"), ("last_tx", "last PTR")]
    trs = ["<table>" + _sort_headers(p, cols, lambda **kw: _qs(p, **kw), active, p["dir"])]
    for r in page_rows:
        tier = r["tier"]
        badge = "<span class='badge {}'>{}</span>".format(
            _TIER_CLASS.get(tier, ""), html.escape(tier))
        if r["unfusable"]:
            badge += " <b class='restrict'>UNFUSABLE</b>"
        est = ("<span class='muted'>n/a</span>" if r["estimate_lo"] is None
               else _range(r["estimate_lo"], r["estimate_hi"]))
        trs.append(
            "<tr><td>{tk}</td><td>{an}</td><td>{ow}</td><td>{b}</td><td>{anc}</td>"
            "<td>{buy}</td><td>{sell}</td><td>{est}</td><td>{tx}</td></tr>".format(
                tk=(html.escape(r["ticker"]) if r["ticker"]
                    else "<span class='muted'>(no ticker)</span>"),
                an=html.escape(str(r["asset_name"] or "-"))[:56],
                ow=("<b>{}</b>".format(html.escape(r["owner"])) if r["owner"] != "self"
                    else html.escape(r["owner"])),
                b=badge, anc=_range(r["anchor_lo"], r["anchor_hi"]),
                buy=(_money0(r["buy_flow"]) if r["buy_flow"] else "-"),
                sell=(_money0(r["sell_flow"]) if r["sell_flow"] else "-"),
                est=est, tx=html.escape(r["last_tx"] or "-")))
    trs.append("</table>")

    sel = "".join("<option value='{}'{}>{}</option>".format(
        html.escape(m["key"]), " selected" if m["key"] == res.get("member_key") else "",
        html.escape(m["label"])) for m in res.get("members") or [])
    form = ("<form method='get'>member <select name='member'>{s}</select> "
            "<input type='hidden' name='per_page' value='{pp}'>"
            "<input type='hidden' name='page' value='1'>"
            "<input type='hidden' name='theme' value='{t}'>"
            "<button>view</button></form>").format(
                s=sel, pp=p["per_page"], t=html.escape(p.get("theme") or ""))
    tier_line = " &middot; ".join(
        "<b>{}</b> {}".format(k, v) for k, v in sorted(res["tiers"].items()))
    ptr_note = ("linked to PTR filer <b>{}</b>".format(html.escape(res["ptr_person"]))
                if res.get("ptr_linked") else
                "<b>no PTR filer matched</b> — every row is annual-only, which may mean "
                "they file no PTRs we hold, not that they did not trade")
    head = (
        "<p class='muted'><b>{m}</b> &middot; {ch}{pt}{st} &middot; anchor <b>CY{yr}</b> "
        "filed {fd} &middot; {n} positions &middot; {ptr}</p>"
        "<p class='muted'>basis: {tiers}</p>"
    ).format(m=html.escape(res["member"]), ch=html.escape(res["chamber"] or "?"),
             pt=(" &middot; " + html.escape(res["party"])) if res.get("party") else "",
             st=(" &middot; " + html.escape(res["state"])) if res.get("state") else "",
             yr=res["anchor_year"], fd=html.escape(res["anchor_filed"] or "?"),
             n=res["count"], ptr=ptr_note, tiers=tier_line or "-")
    caveat = (
        "<p class='restrict-banner'><b>THIS PAGE DERIVES, IT DOES NOT REPORT.</b><br>"
        "The annual column is as-filed. Everything right of it is COMPUTED: an estimate "
        "is the annual band shifted by PTR band MIDPOINTS, rendered as a range and never "
        "a mark. Annuals run up to ~18 months stale, so an <b>anchored</b> row is what was "
        "reported then, not a current holding. Full-vs-partial sale is NEVER inferred — a "
        "sale is only what the PTR states. {u} holding(s) here carry no ticker and are "
        "marked <b class='restrict'>UNFUSABLE</b>: they can never join a flow, so "
        "&ldquo;no flows matched&rdquo; must not be read as &ldquo;no flows "
        "occurred&rdquo;. Counts are FLOORS where a filing year is paper or unparsed.</p>"
        "<p class='muted'>{help}</p>"
    ).format(u=res["unfusable"],
             help=" &middot; ".join("<b>{}</b>: {}".format(k, html.escape(v))
                                    for k, v in _TIER_HELP.items()))
    links = ("<p class='muted'><a href='/trades'>PTR history</a> &middot; "
             "<a href='/sentinels'>sentinel log</a> &middot; "
             "<a href='/congress'>breadth board</a> &middot; "
             "<a href='/congress_gaps'>coverage roll</a></p>")
    body = [head, caveat, form, links, _pager(p, meta, "/member.csv"), "".join(trs),
            _pager(p, meta, "/member.csv")]
    return _page("Member book — {}".format(res["member"]), "".join(body), p)


def _build_member_csv(con, p, full):
    import csv
    import io
    res = q.q_member_fusion(con, member_key=p["member"] or None)
    rows = _sorted(res["rows"], p["sort"] or "estimate_lo", p["dir"])
    if not full:
        rows = _page_slice(rows, p["per_page"], p["page"])[0]
    buf = io.StringIO()
    w = csv.DictWriter(buf, fieldnames=_FUSION_CSV_COLS, extrasaction="ignore")
    w.writeheader()
    for r in rows:
        w.writerow(r)
    return buf.getvalue()


def view_congress_gaps(con, p):
    """SM-C2 P3: the coverage roll — who is behind the breadth counts, and who isn't."""
    res = q.q_congress_gaps(con)
    active = p["sort"] or "rows"
    rows = _sorted(res["rows"], active, p["dir"])
    page_rows, meta = _page_slice(rows, p["per_page"], p["page"])
    cols = [("member", "member"), ("chamber", "chamber"), ("party", "party"),
            ("state", "state"), ("match_kind", "resolved"), ("years", "FD years"),
            ("year_count", "#yrs"), ("rows", "rows")]
    trs = ["<table>" + _sort_headers(p, cols, lambda **kw: _qs(p, **kw), active, p["dir"])]
    for r in page_rows:
        trs.append(
            "<tr><td>{m}</td><td>{c}</td><td>{p}</td><td>{s}</td><td>{k}</td><td>{y}</td>"
            "<td>{n}</td><td>{rw}</td></tr>".format(
                m=html.escape(r["member"]), c=html.escape(r["chamber"]),
                p=html.escape(r["party"] or "-"), s=html.escape(str(r["state"] or "-")),
                k=html.escape(r["match_kind"]), y=html.escape(r["years"] or "-"),
                n=r["year_count"], rw=_fmt(r["rows"])))
    trs.append("</table>")
    body = ["<p class='muted'><a href='/congress'>&laquo; breadth board</a> &middot; "
            "<b>Coverage roll</b> — every filer identity in the holdings corpus. "
            "<b>Breadth counts are FLOORS</b>: a member absent here (paper-only filing, "
            "blocked fetch, unparsed layout) depresses every ticker they hold. "
            "{res} of {tot} identities resolved to a party from the public roster; the "
            "{unres} <b>unmatched</b> are dominated by CANDIDATES who filed a disclosure "
            "but never served — not a parse gap. Party is assigned only on a deterministic "
            "key (House surname+state, Senate surname against recent senators) and is "
            "left blank otherwise, never guessed.</p>".format(
                res=res["resolved"], tot=res["count"], unres=res["unresolved"]),
            _pager(p, meta, "/congress_gaps.csv"), "".join(trs),
            _pager(p, meta, "/congress_gaps.csv")]
    return _page("Congress coverage roll", "".join(body), p)


def view_breadth_yoy(con, p):
    """SM-C3 Phase Y: what changed between two annual cycles. CONTEXT, never an alert."""
    res = q.q_congress_breadth_watch(con)
    if not res["year"] or not res["prior_year"]:
        return _page("Breadth change", "<p class='muted'>Not enough coverage years in "
                     "the holdings corpus to compare.</p>", p)
    rows = _sorted(res["rows"], p["sort"] or "delta_comparable", p["dir"])
    page_rows, meta = _page_slice(rows, p["per_page"], p["page"])
    cols = [("ticker", "ticker"), ("holders_both_prior", "CY{}".format(res["prior_year"])),
            ("holders_both_year", "CY{}".format(res["year"])),
            ("delta_comparable", "&Delta;"), ("new_members", "new"),
            ("exited_members", "exited"), ("first_seen_year", "1st seen"),
            ("floor_exposure", "floor exposure"), ("confidence", "confidence")]
    trs = ["<table>" + _sort_headers(p, cols, lambda **kw: _qs(p, **kw),
                                     p["sort"] or "delta_comparable", p["dir"])]
    for r in page_rows:
        mark = (" <span class='badge'>NEW TO CORPUS</span>" if r["new_to_corpus"] else "")
        if r["identity_discontinuity"]:
            mark += (" <span class='badge' title='also filed as {t}'>ALSO FILED AS "
                     "{t}</span>".format(t=html.escape(",".join(r["symbol_twins"]))))
        conf = ("<span class='warn' title='{}'>low</span>".format(
            html.escape("; ".join(r["confidence_why"]))) if r["confidence"] == "low"
            else "ok")
        trs.append(
            "<tr><td><a href='/congress?cticker={t}'>{t}</a>{mk}</td><td>{a}</td>"
            "<td>{b}</td><td><b>{d:+d}</b></td><td>{n}</td><td>{x}</td><td>{fs}</td>"
            "<td>{ex}</td><td>{c}</td></tr>".format(
                t=html.escape(r["ticker"]), mk=mark, a=r["holders_both_prior"],
                b=r["holders_both_year"], d=r["delta_comparable"], n=r["new_members"],
                x=r["exited_members"], fs=r["first_seen_year"] or "-",
                ex=_money_cell("floor_exposure", r["floor_exposure"]), c=conf))
    trs.append("</table>")
    pop = res["population"]
    head = (
        "<p class='muted'><a href='/congress'>&laquo; breadth board</a> &middot; "
        "<b>CY{y} vs CY{p}</b> &mdash; {n} single-name positions that {b} or more "
        "additional members held at the end of CY{y} than CY{p}.</p>".format(
            y=res["year"], p=res["prior_year"], n=res["count"], b=res["min_delta"]))
    context = (
        "<p class='muted'><b>This is context, not an alert.</b> An annual FD says what a "
        "member held on 31 Dec &mdash; not that they bought it, and not when. The filing "
        "lands months later. Nothing on this page emits an event or pages anyone.</p>")
    denom = (
        "<p class='muted'><b>Counted on the {both} members who filed BOTH years.</b> "
        "Members file on extensions, so CY{y} has {fy} filers against CY{p}'s {fp}. "
        "Differencing raw counts would report {left} members' holdings as sales they "
        "never made. {ent} members appear only in CY{y} and {left} only in CY{p}; "
        "neither group can evidence a change, so neither is counted. "
        "House both-years {hb}, Senate both-years {sb}.</p>".format(
            both=pop["both"], y=res["year"], p=res["prior_year"],
            fy=pop["filed_year"], fp=pop["filed_prior"], ent=pop["entered"],
            left=pop["left"], hb=pop["house"]["both"], sb=pop["senate"]["both"]))
    cut = (
        "<p class='muted'><b>Cut: {cut}.</b> Single names only &mdash; the unfiltered "
        "head is 71% index products, and a board that surfaces IVV every cycle says "
        "nothing. A ticker one member reports as stock and another as a fund is left out "
        "rather than half-counted. Exposure is a coarse band FLOOR (an open top band "
        "contributes its floor, never an invented ceiling).</p>".format(
            cut=html.escape(res["cut"])))
    twins = (
        "<p class='muted'><b>ALSO FILED AS</b> means another symbol carries the same "
        "company name in these two years, so this row's cohort may span a notation "
        "change. Fiserv is filed as both FISV and FI; union the two and FISV's +2 is +1. "
        "The number here is <b>left exactly as computed</b> &mdash; the flag states the "
        "fact and nothing is re-ranked or merged on it. {n} of {c} rows carry it.</p>"
        .format(n=res["identity_discontinuity_rows"], c=res["count"]))
    marks = (
        "<p class='muted'><b>NEW TO CORPUS</b> means the ticker appears in no prior "
        "year's filings at all &mdash; look before reading it as accumulation. It does "
        "not say why: Q (Qnity Electronics) is new because the company did not exist "
        "before CY{y}, while SMCI has been listed since 2007 and simply was not held by "
        "anyone tracked here. We hold no corporate-actions feed and do not guess. "
        "{ca} of {n} rows carry it.</p>".format(
            y=res["year"], ca=res["corporate_action_rows"], n=res["count"]))
    conf_note = (
        "<p class='muted'><b>Confidence</b> is per member, not per chamber. A member "
        "whose CY{p} filing fell under the {bar}% ticker-capture bar may have held a "
        "position we could not read, so their <i>new</i> badge is marked low. "
        "{lm} of the {both} both-years members are in that state. Standing caveat: "
        "{cells}.</p>".format(
            p=res["prior_year"], bar=res["bar"], lm=res["sub_bar_members"],
            both=pop["both"],
            cells=html.escape("; ".join(res["sub_bar_cells"]) or "none")))
    body = [head, context, denom, cut, marks, twins, conf_note,
            _pager(p, meta, "/breadth_yoy.csv"), "".join(trs),
            _pager(p, meta, "/breadth_yoy.csv")]
    return _page("Breadth change CY{} vs CY{}".format(res["year"], res["prior_year"]),
                 "".join(body), p)


_CMTE_CSV_COLS = ["ticker", "instrument", "holder_count", "floor_exposure",
                  "anchor_years"]
_ROLL_CSV_COLS = ["committee_id", "committee_name", "seats", "filers_we_hold"]


def _cmte_caveats(res):
    cov = res.get("coverage") or {}
    per = " ".join("{} {}%".format(
        c, round(100.0 * v["with_committee"] / v["rows"], 1) if v["rows"] else "-")
        for c, v in sorted(cov.items()))
    return (
        "<p class='muted'><b>Partial by construction &mdash; {pct}% of anchor holdings "
        "rows carry a committee</b> ({per}). A seat attaches only where a filer identity "
        "resolved to one person who sits in the CURRENT Congress, so this is strictly "
        "narrower than the roster join: a member who resolved cleanly but has since left "
        "Congress carries no seat. Everything outside that is invisible here, not "
        "absent from the corpus.</p>"
        "<p class='muted'><b>Present-tense seats, dated holdings.</b> "
        "congress-legislators publishes no historical membership, so this pairs a "
        "member's seat TODAY with their latest annual FD &mdash; which may cover an "
        "earlier year. It says <i>this member, who now sits on X, disclosed Y</i>; it "
        "does not say they sat on X when they held Y. The anchor years are shown per "
        "row.</p>"
        "<p class='muted'><b>Counts only.</b> Holding a ticker inside a committee's "
        "jurisdiction is not evidence of anything by itself &mdash; assignment and "
        "portfolio both track a member's background. Nothing here is an inference, and "
        "mega-caps and index funds top these counts mechanically.</p>".format(
            pct=res.get("coverage_pct"), per=per))


def view_committees(con, p):
    """SM-C3 Phase R: COMMITTEE x HOLDINGS."""
    cid = (p.get("cmte") or "").strip() or None
    res = q.q_committee_holdings(con, committee_id=cid, min_holders=2 if cid else 1)
    if not res["rows"] and not cid:
        return _page("Committees", "<p class='muted'>No committee membership synced yet "
                     "&mdash; run <code>python -m smart_money.committees</code>.</p>", p)
    if cid:
        rows = _sorted(res["rows"], p["sort"] or "holder_count", p["dir"])
        page_rows, meta = _page_slice(rows, p["per_page"], p["page"])
        cols = [("ticker", "ticker"), ("instrument", "instr"),
                ("holder_count", "members holding"),
                ("floor_exposure", "floor exposure"), ("anchor_years", "FD years")]
        trs = ["<table>" + _sort_headers(p, cols, lambda **kw: _qs(p, **kw),
                                         p["sort"] or "holder_count", p["dir"])]
        for r in page_rows:
            trs.append("<tr><td><a href='/congress?cticker={t}'>{t}</a></td><td>{i}</td>"
                       "<td>{h}</td><td>{e}</td><td>{y}</td></tr>".format(
                           t=html.escape(r["ticker"]), i=html.escape(r["instrument"]),
                           h=r["holder_count"],
                           e=_money_cell("floor_exposure", r["floor_exposure"]),
                           y=html.escape(r["anchor_years"])))
        trs.append("</table>")
        head = ("<p class='muted'><a href='/committees'>&laquo; all committees</a> "
                "&middot; <b>{n}</b> &mdash; {s} seats, {f} of them filers whose annual "
                "FD we hold. {c} tickers held by 2+ of them.</p>".format(
                    n=html.escape(res["committee_name"] or cid), s=res["seats"],
                    f=res["filers_we_hold"], c=res["count"]))
        body = [head, _cmte_caveats(res), _pager(p, meta, "/committees.csv"),
                "".join(trs), _pager(p, meta, "/committees.csv")]
        return _page("Committee holdings - {}".format(res["committee_name"] or cid),
                     "".join(body), p)
    rows = _sorted(res["rows"], p["sort"] or "filers_we_hold", p["dir"])
    page_rows, meta = _page_slice(rows, p["per_page"], p["page"])
    cols = [("committee_name", "committee"), ("seats", "seats"),
            ("filers_we_hold", "filers we hold")]
    trs = ["<table>" + _sort_headers(p, cols, lambda **kw: _qs(p, **kw),
                                     p["sort"] or "filers_we_hold", p["dir"])]
    for r in page_rows:
        trs.append("<tr><td><a href='/committees?cmte={i}'>{n}</a></td><td>{s}</td>"
                   "<td>{f}</td></tr>".format(
                       i=html.escape(r["committee_id"]),
                       n=html.escape(r["committee_name"] or r["committee_id"]),
                       s=r["seats"], f=r["filers_we_hold"]))
    trs.append("</table>")
    head = ("<p class='muted'><b>Committee roll</b> &mdash; {n} committees and "
            "subcommittees. <b>filers we hold</b> is how many of a committee's members "
            "have an annual FD in this corpus; that, not the seat count, is what a cut "
            "on it can actually see.</p>".format(n=res["count"]))
    body = [head, _cmte_caveats(res), _pager(p, meta, "/committees.csv"),
            "".join(trs), _pager(p, meta, "/committees.csv")]
    return _page("Committees", "".join(body), p)


def _build_committees_csv(con, p, full):
    import csv
    import io
    cid = (p.get("cmte") or "").strip() or None
    res = q.q_committee_holdings(con, committee_id=cid, min_holders=2 if cid else 1)
    cols = _CMTE_CSV_COLS if cid else _ROLL_CSV_COLS
    rows = _sorted(res["rows"], p["sort"] or ("holder_count" if cid
                                              else "filers_we_hold"), p["dir"])
    if not full:
        rows = _page_slice(rows, p["per_page"], p["page"])[0]
    buf = io.StringIO()
    w = csv.DictWriter(buf, fieldnames=cols, extrasaction="ignore")
    w.writeheader()
    for r in rows:
        w.writerow(r)
    return buf.getvalue()


_YOY_CSV_COLS = ["ticker", "instrument", "holders_both_prior", "holders_both_year",
                 "delta_comparable", "new_members", "exited_members", "delta_total",
                 "holders_prior", "holders_year", "first_seen_year", "new_to_corpus",
                 "identity_discontinuity", "symbol_twins", "floor_exposure",
                 "confidence"]


def _build_breadth_yoy_csv(con, p, full):
    """CSV of the Phase Y cut, honoring the active sort."""
    import csv
    import io
    res = q.q_congress_breadth_watch(con)
    rows = _sorted(res["rows"], p["sort"] or "delta_comparable", p["dir"])
    if not full:
        rows = _page_slice(rows, p["per_page"], p["page"])[0]
    buf = io.StringIO()
    w = csv.DictWriter(buf, fieldnames=_YOY_CSV_COLS, extrasaction="ignore")
    w.writeheader()
    for r in rows:
        w.writerow(r)
    return buf.getvalue()


_GAP_CSV_COLS = ["member", "chamber", "party", "state", "match_kind", "years",
                 "year_count", "rows"]


def _build_congress_gaps_csv(con, p, full):
    """CSV of the coverage roll, honoring the active sort."""
    import csv
    import io
    res = q.q_congress_gaps(con)
    rows = _sorted(res["rows"], p["sort"] or "rows", p["dir"])
    if not full:
        rows = _page_slice(rows, p["per_page"], p["page"])[0]
    buf = io.StringIO()
    w = csv.DictWriter(buf, fieldnames=_GAP_CSV_COLS, extrasaction="ignore")
    w.writeheader()
    for r in rows:
        w.writerow(r)
    return buf.getvalue()


def _build_congress_csv(con, p, full):
    """CSV of the congress surface — the breadth board or (with cticker) the holder list,
    honoring the active sort and owner filter, matching the on-screen page."""
    import csv
    import io
    if p["cticker"]:
        res = q.q_congress_holders(con, p["cticker"], p["cinstr"])
        rows = _sorted(res["rows"], p["sort"] or "value_lo", p["dir"])
        cols = ["member", "chamber", "state", "owner", "value_lo", "value_hi"]
    else:
        res = q.q_congress_breadth(con, min_holders=2, owner_filter=p["cowner"])
        rows = _sorted(res["rows"], p["sort"] or "holder_count", p["dir"])
        cols = ["ticker", "instrument", "holder_count", "house", "senate", "dem", "rep",
                "ind", "party_unknown", "self", "spouse", "joint", "dependent",
                "midpoint_exposure", "yoy_change", "first_year"]
    if not full:
        rows = _page_slice(rows, p["per_page"], p["page"])[0]
    buf = io.StringIO()
    w = csv.DictWriter(buf, fieldnames=cols, extrasaction="ignore")
    w.writeheader()
    for r in rows:
        w.writerow(r)
    return buf.getvalue()


# RENAMED: pct_since_trade -> market_return_since_trade, and exec_price /
# insider_return added. The old name did not say whose return it was and was read as the
# insider's when it is the stock's. This is a deliberate CSV schema change - a consumer
# parsing by name is fine, one parsing by position is not.
_CSV_COLS = ["person", "ticker", "side", "trade_date", "date_is_reported",
             "reported_date", "lag_days", "shares", "exec_price", "implied_price",
             "value", "plan_10b5_1", "entry_close", "latest_close",
             "market_return_since_trade", "insider_return", "return_rankable",
             "return_basis_warning", "price_vs_close_pct", "value_quality",
             "issuer_class", "cofiler_count", "cofiling_suspected", "smid_band",
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


def _page_brief_spec(con, p, path):
    """(title, subtitle, columns, rows, notes) for the CURRENTLY VIEWED page, or None to
    fall back to the front-page brief. Uses the SAME query + sort the page used, and the
    SAME column constants the CSV export uses, so the PDF, the screen and the CSV cannot
    drift apart."""
    def srt(rows, default):
        return _sorted(rows, p["sort"] or default, p["dir"])

    if path == "/portfolios":
        res = q.q_portfolio(con, filer_cik=p["filer"] or None, period=p["period"] or None)
        notes = ["Reported 13F book. Band-valued, 45 days stale, long US-listed only."]
        if res.get("magnitude_warning"):
            notes.insert(0, "UNIT SCALE NOT VERIFIED - " + res["magnitude_warning"])
        return ("Reported portfolio - {}".format(res.get("filer_name") or "?"),
                "period {} - {} positions - book {}".format(
                    res.get("period") or "-", res.get("count") or 0,
                    _money0(res.get("book_value"))),
                _PORT_CSV_COLS, srt(res["rows"], "value"), notes)
    if path == "/insiders":
        res = q.q_member_book(con, member_key=p["member"] or None, year=p["myear"])
        notes = ["Annual FD snapshot, band midpoints, never a mark."]
        if res.get("spouse_share") is not None and res["spouse_share"] >= 50:
            notes.insert(0, "{}% of this book is SPOUSE-owned - the disclosure surface "
                            "is largely not the member's own name".format(
                                res["spouse_share"]))
        return ("Political insider book - {}".format(res.get("member") or "?"),
                "{} - FD year {} - {} positions - book {}".format(
                    res.get("chamber") or "-", res.get("year") or "-",
                    res.get("count") or 0, _money0(res.get("book_value"))),
                _INSIDER_CSV_COLS, srt(res["rows"], "midpoint"), notes)
    if path == "/oge":
        res = q.q_oge_holdings(con, filer=p["member"] or None)
        return ("OGE 278e disclosure - {}".format(res.get("filer") or "?"),
                "{} disclosure lines - {} with a value band".format(
                    res.get("count") or 0, res.get("banded") or 0),
                _OGE_CSV_COLS, srt(res["rows"], "midpoint"),
                [res.get("restriction") or "RESTRICTED",
                 "Ethics in Government Act 5 USC app. 105(c) - not for commercial use."])
    if path == "/congress_gaps":
        res = q.q_congress_gaps(con)
        return ("Congress coverage roll",
                "{} filer identities - {} resolved to a party - {} unmatched".format(
                    res["count"], res["resolved"], res["unresolved"]),
                _GAP_CSV_COLS, srt(res["rows"], "rows"),
                ["Breadth counts are FLOORS. Unmatched identities are dominated by "
                 "candidates who filed a disclosure but never served."])
    if path == "/breadth_yoy":
        res = q.q_congress_breadth_watch(con)
        return ("Breadth change CY{} vs CY{}".format(res["year"], res["prior_year"]),
                "{} single names - {} or more added holders - {} members filed both "
                "years".format(res["count"], res["min_delta"],
                               (res["population"] or {}).get("both", 0)),
                _YOY_CSV_COLS, srt(res["rows"], "delta_comparable"),
                ["CONTEXT, NEVER AN ALERT. An annual FD says what was held on 31 Dec, "
                 "not that it was bought and not when.",
                 "Counted only on members who filed BOTH years - {} entered and {} left "
                 "the filing population and cannot evidence a change.".format(
                     (res["population"] or {}).get("entered", 0),
                     (res["population"] or {}).get("left", 0)),
                 "NEW TO CORPUS marks a ticker absent from every prior year. It does not "
                 "say why - there is no corporate-actions feed here.",
                 "ALSO FILED AS marks a symbol whose company name is shared with another "
                 "symbol in these years (FISV/FI). The count is NOT adjusted for it.",
                 "Standing caveat: " + ("; ".join(res["sub_bar_cells"]) or "none")])
    if path == "/disagreements":
        res = q.q_opposed_pairs(con)
        return ("Cross-manager disagreements",
                "{} tickers - {} filers with a determinable direction".format(
                    res["count"], res["filers_compared"]),
                _DIS_CSV_COLS, srt(res["rows"], "n_managers"), [res["note"]])
    if path == "/congress":
        if p["cticker"]:
            res = q.q_congress_holders(con, p["cticker"], p["cinstr"])
            return ("Congress holders - {} {}".format(res["ticker"], res["instrument"]),
                    "{} positions across members, latest filing each".format(res["count"]),
                    ["member", "chamber", "state", "owner", "value_lo", "value_hi"],
                    srt(res["rows"], "value_lo"), [])
        res = q.q_congress_breadth(con, min_holders=2, owner_filter=p["cowner"])
        return ("Congress breadth - who holds what",
                "{} tickers held by 2 or more members".format(res["count"]),
                ["ticker", "instrument", "holder_count", "house", "senate", "dem", "rep",
                 "ind", "party_unknown", "midpoint_exposure", "yoy_change", "first_year"],
                srt(res["rows"], "holder_count"), [res["note"]])
    return None


ROUTES = {"/": view_front, "/portfolios": view_portfolios, "/congress": view_congress,
          "/insiders": view_insiders, "/oge": view_oge, "/member": view_member,
          "/disagreements": view_disagreements,
          "/congress_gaps": view_congress_gaps,
          "/breadth_yoy": view_breadth_yoy,
          "/committees": view_committees,
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
        p["_path"] = u.path          # so the Print button can target THIS page
        # /congress/member/<id> is the one PATH-parameterised route (the order specifies
        # that URL shape). The id is the url-quoted member key "chamber|last|first|dist";
        # everything else in the dashboard is exact-match + query params.
        if u.path.startswith(MEMBER_PREFIX):
            ident = unquote(u.path[len(MEMBER_PREFIX):]).strip("/")
            if ident:
                p["member"] = ident[:120]
        con = q.connect_ro(self.db_path)  # read-only, per request
        try:
            if u.path == "/brief.pdf":
                return self._brief(con, p, parse_qs(u.query))
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
            if u.path == "/disagreements.csv":
                return self._disagreements_csv(con, p, parse_qs(u.query))
            if u.path == "/member.csv":
                return self._member_csv(con, p, parse_qs(u.query))
            if u.path == "/oge.csv":
                return self._oge_csv(con, p, parse_qs(u.query))
            if u.path == "/insiders.csv":
                return self._insiders_csv(con, p, parse_qs(u.query))
            if u.path == "/congress_gaps.csv":
                return self._congress_gaps_csv(con, p, parse_qs(u.query))
            if u.path == "/breadth_yoy.csv":
                return self._breadth_yoy_csv(con, p, parse_qs(u.query))
            if u.path == "/committees.csv":
                return self._committees_csv(con, p, parse_qs(u.query))
            if u.path == "/congress.csv":
                return self._congress_csv(con, p, parse_qs(u.query))
            view = view_member if u.path.startswith(MEMBER_PREFIX) else ROUTES.get(u.path)
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

    def _disagreements_csv(self, con, p, qsd):
        full = qsd.get("full", ["0"])[0] == "1"
        data = _build_disagreements_csv(con, p, full)
        self._send(200, data, ctype="text/csv; charset=utf-8",
                   headers={"Content-Disposition":
                            'attachment; filename="disagreements.csv"'})

    def _member_csv(self, con, p, qsd):
        full = qsd.get("full", ["0"])[0] == "1"
        data = _build_member_csv(con, p, full)
        self._send(200, data, ctype="text/csv; charset=utf-8",
                   headers={"Content-Disposition":
                            'attachment; filename="member_book.csv"'})

    def _oge_csv(self, con, p, qsd):
        full = qsd.get("full", ["0"])[0] == "1"
        data = _build_oge_csv(con, p, full)
        self._send(200, data, ctype="text/csv; charset=utf-8",
                   headers={"Content-Disposition":
                            'attachment; filename="oge_278e_RESTRICTED.csv"'})

    def _insiders_csv(self, con, p, qsd):
        full = qsd.get("full", ["0"])[0] == "1"
        data = _build_insiders_csv(con, p, full)
        fname = "insider_book_{}_{}.csv".format(
            (p["member"] or "default").replace("|", "_"),
            "all" if full else "page{}".format(p["page"]))
        self._send(200, data, ctype="text/csv; charset=utf-8",
                   headers={"Content-Disposition":
                            'attachment; filename="{}"'.format(fname)})

    def _congress_gaps_csv(self, con, p, qsd):
        full = qsd.get("full", ["0"])[0] == "1"
        data = _build_congress_gaps_csv(con, p, full)
        fname = "congress_coverage_{}.csv".format(
            "all" if full else "page{}".format(p["page"]))
        self._send(200, data, ctype="text/csv; charset=utf-8",
                   headers={"Content-Disposition":
                            'attachment; filename="{}"'.format(fname)})

    def _breadth_yoy_csv(self, con, p, qsd):
        full = qsd.get("full", ["0"])[0] == "1"
        data = _build_breadth_yoy_csv(con, p, full)
        fname = "breadth_change_{}.csv".format(
            "all" if full else "page{}".format(p["page"]))
        self._send(200, data, ctype="text/csv; charset=utf-8",
                   headers={"Content-Disposition":
                            'attachment; filename="{}"'.format(fname)})

    def _committees_csv(self, con, p, qsd):
        full = qsd.get("full", ["0"])[0] == "1"
        data = _build_committees_csv(con, p, full)
        fname = "committees_{}.csv".format(
            "all" if full else "page{}".format(p["page"]))
        self._send(200, data, ctype="text/csv; charset=utf-8",
                   headers={"Content-Disposition":
                            'attachment; filename="{}"'.format(fname)})

    def _brief(self, con, p, qsd=None):
        from . import brief
        tmp = os.path.join(tempfile.mkdtemp(), "brief.pdf")
        view = ((qsd or {}).get("view", [""])[0] or "").strip("/")
        spec = _page_brief_spec(con, p, "/" + view) if view and view != "front" else None
        if spec:
            title, subtitle, cols, rows, notes = spec
            brief.render_page_brief(
                tmp, title=title,
                subtitle="{}  -  generated {}".format(subtitle, q._as_of()),
                columns=cols, rows=rows, notes=notes)
        else:
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
