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

NAV = [("/", "Front"), ("/clusters", "Clusters"), ("/sentinels", "Sentinels"),
       ("/ticker", "Ticker")]


# ---------------------------------------------------------------- html helpers
def _page(title, body, params):
    qs = _qs(params)
    nav = " &middot; ".join(
        '<a href="{}{}">{}</a>'.format(href, qs, html.escape(label))
        for href, label in NAV)
    printbtn = '<a class="print" href="/brief.pdf{}">Print brief (PDF)</a>'.format(qs)
    return (
        "<!doctype html><html><head><meta charset='utf-8'>"
        "<title>{t}</title><style>"
        "body{{font:14px system-ui,sans-serif;margin:1.5rem;max-width:1100px}}"
        "h1{{font-size:1.3rem}} h2{{font-size:1.05rem;margin-top:1.4rem}}"
        "table{{border-collapse:collapse;width:100%;margin:.4rem 0}}"
        "td,th{{border:1px solid #ccc;padding:3px 7px;text-align:left;font-size:13px}}"
        "th{{background:#eef}} nav{{margin-bottom:1rem}} .print{{float:right}}"
        ".muted{{color:#666;font-size:12px}} .hot{{background:#fdecea}}"
        "form{{margin:.5rem 0;font-size:13px}}</style></head><body>"
        "<nav>{nav} {print}</nav><h1>{t}</h1>{filt}{body}</body></html>"
    ).format(t=html.escape(title), nav=nav, print=printbtn,
             filt=_filter_form(params), body=body)


def _qs(params):
    keep = {k: params[k] for k in ("window", "anchor", "floor", "symbol")
            if params.get(k)}
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
    p = {"window": _int(one("window"), 90), "floor": _int(one("floor"), 3),
         "anchor": one("anchor") or dt.date.today().isoformat(),
         "symbol": (one("symbol") or "").upper().strip()}
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


ROUTES = {"/": view_front, "/clusters": view_clusters,
          "/sentinels": view_sentinels, "/ticker": view_ticker}


# ---------------------------------------------------------------- server
class Handler(BaseHTTPRequestHandler):
    db_path = None

    def log_message(self, fmt, *args):  # quiet default logging to stderr line
        sys.stderr.write("[dash] {} {}\n".format(self.address_string(), fmt % args))

    def _send(self, code, body, ctype="text/html; charset=utf-8"):
        data = body if isinstance(body, bytes) else body.encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", ctype)
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
            view = ROUTES.get(u.path)
            if not view:
                return self._send(404, _page("Not found", "<p>No such view.</p>", p))
            self._send(200, view(con, p))
        except Exception as exc:  # noqa: BLE001 - surface, never write
            self._send(500, _page("Error", "<pre>{}</pre>".format(
                html.escape(type(exc).__name__ + ": " + str(exc))), p))
        finally:
            con.close()

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


def serve(db_path, host, port):
    if host in ("0.0.0.0", "::", ""):
        raise SystemExit("refusing to bind {} - LAN/Tailscale host only, never "
                         "a public interface".format(host or "<empty>"))
    Handler.db_path = os.path.expanduser(db_path)
    httpd = ThreadingHTTPServer((host, port), Handler)
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
