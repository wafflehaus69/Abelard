"""SM-O1 P1 rerun-style deliverable: per-issuer insider derivative (Table II)
activity. Who at scoped issuers trades options of their own stock, and at what
tempo. Read-only over form4_derivatives; deterministic, no LLM, no network.
"""
import argparse
import datetime as dt
import sys
from collections import defaultdict

from . import db as dbmod


def _iso10(s):
    return (s or "")[:10]


def _tempo(dates):
    """Derivative transactions per calendar month over the active span, plus the
    first/last dates."""
    dates = sorted(d for d in dates if d)
    if not dates:
        return 0.0, None, None
    first, last = dates[0], dates[-1]
    try:
        span = (dt.date.fromisoformat(last) - dt.date.fromisoformat(first)).days
    except ValueError:
        return 0.0, first, last
    months = max(span / 30.44, 1.0)
    return round(len(dates) / months, 2), first, last


def summarize(con, regime="all"):
    q = ("SELECT issuer_cik, issuer, ticker, reporting_cik, reporting_person, "
         "security_title, code, tx_date FROM form4_derivatives")
    params = []
    if regime and regime != "all":
        q += " WHERE ingest_regime=?"
        params.append(regime)
    per_issuer = defaultdict(lambda: {"txns": 0, "insiders": set(), "sectitles": set(),
                                      "codes": defaultdict(int), "dates": [],
                                      "ticker": None})
    per_person = defaultdict(lambda: {"txns": 0, "dates": [], "sectitles": set(),
                                      "ticker": None, "person": None})
    total = 0
    for icik, iss, tk, rcik, rp, st, code, txd in con.execute(q, params):
        total += 1
        key = icik or ("TK:" + (tk or "?"))
        a = per_issuer[key]
        a["txns"] += 1
        a["ticker"] = a["ticker"] or tk
        a["insiders"].add(rcik or rp)
        if st:
            a["sectitles"].add(st)
        a["codes"][code or "?"] += 1
        d = _iso10(txd)
        if d:
            a["dates"].append(d)
        pk = (key, rcik or rp)
        p = per_person[pk]
        p["txns"] += 1
        p["ticker"] = p["ticker"] or tk
        p["person"] = p["person"] or rp
        if st:
            p["sectitles"].add(st)
        if d:
            p["dates"].append(d)
    return per_issuer, per_person, total


def _render(out, per_issuer, per_person, total, regime):
    m = ["# DERIVATIVE_ACTIVITY — SM-O1 insider Table II summary", "",
         "Who at scoped issuers trades options of their own stock, and at what "
         "tempo. Regime {}. {} derivative transactions across {} issuers, {} "
         "distinct insider-issuer pairs. Read-only over form4_derivatives.".format(
             regime, total, len(per_issuer), len(per_person)), ""]
    m.append("## Per-issuer insider derivative activity (by transaction count)")
    m.append("")
    m.append("| ticker | issuer_cik | deriv_txns | insiders | security_types | "
             "code_mix | first | last | tempo/mo |")
    m.append("|---|---|---|---|---|---|---|---|---|")
    for key, a in sorted(per_issuer.items(), key=lambda kv: -kv[1]["txns"])[:60]:
        tempo, first, last = _tempo(a["dates"])
        codemix = " ".join("{}:{}".format(c, n)
                           for c, n in sorted(a["codes"].items(), key=lambda x: -x[1]))
        st = ",".join(sorted(a["sectitles"]))[:60]
        m.append("| {} | {} | {} | {} | {} | {} | {} | {} | {} |".format(
            a["ticker"] or "-", key, a["txns"], len(a["insiders"]), st, codemix,
            first, last, tempo))
    m.append("")
    m.append("## Most active insiders trading their own stock's options (by tempo)")
    m.append("")
    m.append("| person | ticker | deriv_txns | first | last | tempo/mo | security_types |")
    m.append("|---|---|---|---|---|---|---|")
    ranked = sorted(per_person.items(),
                    key=lambda kv: (-_tempo(kv[1]["dates"])[0], -kv[1]["txns"]))
    for (key, who), p in ranked[:40]:
        tempo, first, last = _tempo(p["dates"])
        if p["txns"] < 2:
            continue  # tempo needs more than one point to mean anything
        st = ",".join(sorted(p["sectitles"]))[:50]
        m.append("| {} | {} | {} | {} | {} | {} | {} |".format(
            p["person"] or who, p["ticker"] or "-", p["txns"], first, last, tempo, st))
    m.append("")
    m.append("Tempo = derivative transactions per calendar month over the active "
             "span. Codes: M option exercise, A grant/award, F tax withholding, "
             "G gift, C conversion. This is descriptive corpus activity, not a "
             "signal — SM-O1 P4 is where options flow gets joined to it.")
    open(out, "w", encoding="utf-8").write("\n".join(m) + "\n")


def main(argv=None):
    ap = argparse.ArgumentParser(description="SM-O1 insider derivative activity summary")
    ap.add_argument("--db", default=dbmod.DB_PATH_DEFAULT)
    ap.add_argument("--regime", default="all")
    ap.add_argument("--out", default=None)
    args = ap.parse_args(argv)
    con = dbmod.connect(args.db)
    pi, pp, total = summarize(con, args.regime)
    out = args.out or dbmod.artifact_path("DERIVATIVE_ACTIVITY.md", "analysis")
    _render(out, pi, pp, total, args.regime)
    print("[deriv-summary] regime={} txns={} issuers={} persons={} -> {}".format(
        args.regime, total, len(pi), len(pp), out))
    return 0


if __name__ == "__main__":
    sys.exit(main())
