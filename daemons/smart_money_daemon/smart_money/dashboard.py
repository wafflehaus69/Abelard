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

NAV = [("/", "Front"), ("/trades", "Trades"), ("/clusters", "Clusters"),
       ("/sentinels", "Sentinels"), ("/ticker", "Ticker")]

# Theme palettes. Default is light; dark applies automatically when the viewer's
# system prefers dark, and can be forced either way via ?theme=dark|light (which
# threads through the URL like the other filters). Pure CSS custom properties, no
# JS — fits the stdlib, no-build ethos.
_LIGHT = ("--bg:#ffffff;--fg:#1a1a1a;--border:#cccccc;--th-bg:#eeeeff;"
          "--muted:#666666;--hot-bg:#fdecea;--hot-fg:#1a1a1a;--link:#0a58ca;"
          "--pos:#1a8a3a;--neg:#c0392b;--reported:#c0392b")
_DARK = ("--bg:#0f1216;--fg:#d7dce3;--border:#2b313b;--th-bg:#1a2130;"
         "--muted:#8b93a1;--hot-bg:#3a2323;--hot-fg:#f3d9d6;--link:#6ea8fe;"
         "--pos:#4ade80;--neg:#ff6b6b;--reported:#ff6b6b")
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
    ".reported{color:var(--reported);font-weight:bold}"
    ".pos{color:var(--pos)} .neg{color:var(--neg)}"
    ".expand a{margin-right:8px} .expand{margin:.3rem 0;font-size:12px}"
)


# ---------------------------------------------------------------- html helpers
def _page(title, body, params):
    qs = _qs(params)
    nav = " &middot; ".join(
        '<a href="{}{}">{}</a>'.format(href, qs, html.escape(label))
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


def _fmt(v):
    if v is None:
        return "-"
    if isinstance(v, float):
        return "{:,.0f}".format(v) if abs(v) >= 1000 else "{:.2f}".format(v)
    if isinstance(v, list):
        return ",".join(str(x) for x in v)
    return str(v)


# ---------------------------------------------------------------- views
def _params(qsd):
    def one(k, default=None):
        return qsd.get(k, [default])[0]
    theme = (one("theme") or "").lower().strip()
    pp = one("per_page")
    pg = one("page")
    side = (one("side") or "").lower()
    plan = (one("plan") or "").lower()
    scope = (one("scope") or "").lower()
    p = {"window": _int(one("window"), 90), "floor": _int(one("floor"), 3),
         "anchor": one("anchor") or dt.date.today().isoformat(),
         "symbol": (one("symbol") or "").upper().strip(),
         "theme": theme if theme in ("dark", "light") else "",
         "per_page": int(pp) if pp in ("25", "50", "100", "250", "500") else 100,
         "page": int(pg) if (pg or "").isdigit() and int(pg) >= 1 else 1,
         "side": side if side in ("buy", "sell", "all") else "buy",
         "plan": plan if plan in ("all", "discretionary", "planned") else "all",
         "scope": scope if scope in ("all", "scoped") else "scoped",
         "smid": one("smid") == "1"}
    return p


def _int(v, d):
    try:
        return max(1, min(3650, int(v)))
    except (TypeError, ValueError):
        return d


def view_front(con, p):
    sent = q.q_sentinel_log(con, window=p["window"] * 2, anchor=p["anchor"])
    conv = q.q_principal_convergence(con)
    press = q.q_ownership_pressure(con, "all", p["window"], p["anchor"])
    ev = q.q_positioning_events(since=q._win(p["anchor"], p["window"]), overlay_only=True)
    body = [
        "<p class='muted'>as-of {}. Front page = sentinels, principal convergence, "
        "ownership pressure, overlay-flagged events. Clusters are context.</p>".format(
            html.escape(press["as_of"])),
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


def view_clusters(con, p):
    cc = q.q_cluster_context(con, window=p["window"] * 2, floor=p["floor"], anchor=p["anchor"])
    sd = q.q_sell_anomaly(con, window=p["window"], anchor=p["anchor"])
    body = [
        "<p class='muted'>CONTEXT, never alerts. Buy clusters with the capitulation "
        "timeline; sell feed ranked by rate ratio (elevated tint at {}).</p>".format(
            sd["elevated_ratio"]),
        "<h2>Buy clusters (floor {}, {} found)</h2>".format(p["floor"], cc["count"]),
        _table(["ticker", "n_buyers", "n_buys", "span_days", "calendar_months", "capitulation"],
               cc["rows"][:25], hot=lambda r: r.get("capitulation")),
        "<h2>Sell context feed (ranked by rate ratio)</h2>",
        _table(["ticker", "rate_ratio", "distinct_sellers_window", "distinct_sellers_12mo",
                "window_sell_value", "elevated"],
               [r for r in sd["rows"] if r["distinct_sellers_12mo"] >= 3][:25],
               hot=lambda r: r.get("elevated")),
    ]
    return _page("Smart Money — cluster context", "".join(body), p)


def view_sentinels(con, p):
    sent = q.q_sentinel_log(con, window=p["window"] * 4, anchor=p["anchor"])
    body = ["<p class='muted'>Registry as-of {}. {} events, newest first.</p>".format(
        html.escape(str(sent["registry_as_of"])), sent["count"]),
        _table(["event_date", "src", "seed", "role", "ticker", "action", "value"],
               sent["rows"][:100])]
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


def _pager(params, res):
    pp, pg, pages, total = (res["per_page"], res["page"], res["pages"],
                            res["total_matching"])
    sizes = []
    for n in (25, 50, 100, 250, 500):
        sizes.append("<b>{}</b>".format(n) if n == pp
                     else '<a href="{}">{}</a>'.format(_qs(params, per_page=n, page=1), n))
    nav = []
    if pg > 1:
        nav.append('<a href="{}">&laquo; prev</a>'.format(_qs(params, page=pg - 1)))
    nav.append("page {} of {}".format(pg, pages))
    if pg < pages:
        nav.append('<a href="{}">next &raquo;</a>'.format(_qs(params, page=pg + 1)))
    csv_page = '<a href="/trades.csv{}">this page CSV</a>'.format(_qs(params))
    csv_all = '<a href="/trades.csv{}">whole dataset CSV</a>'.format(
        _qs(params, full="1"))
    return ("<div class='expand'>per page: {sizes} &nbsp;&middot;&nbsp; {nav} "
            "&nbsp;&middot;&nbsp; {total} total &nbsp;&middot;&nbsp; export: {cp} "
            "| {ca}</div>".format(sizes=" ".join(sizes), nav=" ".join(nav),
                                  total=total, cp=csv_page, ca=csv_all))


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
        "&middot; back to defaults (buy, all plans, scoped, 90d, 100/page)</div>"
    ).format(side=sel("side", params["side"], ("buy", "sell", "all")),
             plan=sel("plan", params["plan"], ("all", "discretionary", "planned")),
             smid=" checked" if params["smid"] else "",
             w=params["window"], pp=params["per_page"],
             a=html.escape(params["anchor"]), t=html.escape(params.get("theme") or ""),
             sc=html.escape(params["scope"]),
             clr=("?theme=" + params["theme"]) if params.get("theme") else "")


def view_trades(con, p):
    res = q.q_insider_trades(con, side=p["side"], window=p["window"],
                             anchor=p["anchor"], plan=p["plan"], smid_only=p["smid"],
                             per_page=p["per_page"], page=p["page"], scope=p["scope"])
    if p["scope"] == "all":
        scope_line = ('scope: <b>all issuers</b> &middot; <a href="{}">overlay + '
                      'trump_network only</a>'.format(_qs(p, scope="scoped")))
    else:
        scope_line = ('scope: <b>overlay + trump_network issuers</b> &middot; '
                      '<a href="{}">show all issuers</a>'.format(_qs(p, scope="all")))
    body = ["<p class='muted'>Amendment-deduped Form 4 open-market trades, newest "
            "first. Trade Date is the transaction date; a red Reported Date means "
            "the trade date was unavailable and the filing date is shown instead. "
            "% since = entry close on the trade date vs latest close.</p>",
            "<p class='muted'>{}</p>".format(scope_line),
            _trade_filter_form(p), _pager(p, res),
            "<table><tr><th>person</th><th>ticker</th><th>side</th><th>trade date</th>"
            "<th>reported</th><th>value</th><th>entry</th><th>latest</th><th>% since</th></tr>"]
    for t in res["rows"]:
        dcell = ('<span class="reported">{} (Reported)</span>'.format(
                    html.escape(str(t["trade_date"] or "-")))
                 if t["date_is_reported"] else html.escape(str(t["trade_date"] or "-")))
        badges = ""
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


_CSV_COLS = ["person", "ticker", "side", "trade_date", "date_is_reported",
             "reported_date", "lag_days", "shares", "value", "plan_10b5_1",
             "entry_close", "latest_close", "pct_since_trade", "smid_band"]


def _build_trades_csv(con, p, full):
    """CSV of the trades feed — the current page, or the whole dataset when
    full=True. Same query, same filters/scope as the on-screen view."""
    import csv
    import io
    res = q.q_insider_trades(con, side=p["side"], window=p["window"],
                             anchor=p["anchor"], plan=p["plan"], smid_only=p["smid"],
                             scope=p["scope"], per_page=p["per_page"], page=p["page"],
                             full=full)
    buf = io.StringIO()
    w = csv.DictWriter(buf, fieldnames=_CSV_COLS, extrasaction="ignore")
    w.writeheader()
    for row in res["rows"]:
        w.writerow(row)
    return buf.getvalue()


ROUTES = {"/": view_front, "/trades": view_trades, "/clusters": view_clusters,
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
