"""Phase 2 politician scorecard per ORDER SM-0/1 method + ORDER SM-1 amendments.

Method encoded exactly:
- Purchases scored, sells tallied descriptively only.
- Filters: asset_type Stock, resolvable ticker, person floor >= 20 lifetime
  stock purchases, price coverage required — missing price = excluded trade,
  counted, never fabricated.
- Two clocks per purchase: A = tx_date (skill), B = disclosure_date
  (copyability). Horizons 21/63/126 trading days on the SPY calendar.
  Excess = ticker return - SPY return over the identical span, adj_close.
- DETERMINISM AMENDMENT: RANKING columns computed only from completed EOD
  horizons — deterministic given the prices table. SNAPSHOT columns mark
  open purchases (126d not elapsed) to market against latest() quotes,
  labeled with asof timestamps, never feeding the ranking metric.
- Weights: band midpoint capped at p90 of scored midpoints, times recency
  weight, full inside 24 months, linear decay to floor 0.25 at 60 months.
- Ranking metric: t-stat = weighted mean clock-A 63d excess divided by
  weighted std over sqrt of effective n.
No LLM calls. No randomness. Reruns over the same DB are identical except
the labeled MTM snapshot block.
"""
import argparse
import bisect
import datetime as dt
import json
import math
import sys

import pandas as pd

from . import db as dbmod
from . import prices
from .clustering import cluster_purchases, WINDOW_DAYS as CLUSTER_WINDOW_DAYS
from .mdfmt import md_table

HORIZONS = (21, 63, 126)
PERSON_FLOOR = 20
PRICE_START_PAD_DAYS = 10


def load_purchases(con):
    q = """
    SELECT ct.trade_id, ct.person_id, p.name, p.cik_or_chamber AS chamber,
           ct.ticker, ct.amt_low, ct.amt_high,
           ct.tx_date, ct.disclosure_date, ct.lag_days
    FROM congress_trades ct JOIN persons p USING(person_id)
    WHERE ct.side = 'purchase' AND ct.asset_type = 'Stock' AND ct.ticker IS NOT NULL
      AND ct.superseded = 0
    """
    return pd.read_sql_query(q, con)


def load_sells(con):
    q = """
    SELECT person_id, COUNT(*) AS n_sells
    FROM congress_trades
    WHERE side IN ('sale','sale_full','sale_partial') AND asset_type = 'Stock'
    GROUP BY person_id
    """
    return pd.read_sql_query(q, con)


def build_series(con, ticker, start, end):
    rows = prices.eod(con, ticker, start, end)
    return {d: adj for d, _, adj, _ in rows}


def recency_weight(months: float) -> float:
    if months <= 24:
        return 1.0
    if months >= 60:
        return 0.25
    return 1.0 - 0.75 * (months - 24) / 36.0


def wstats(values, weights):
    """Weighted mean, weighted std, effective n."""
    sw = sum(weights)
    if sw <= 0:
        return None, None, 0.0
    mean = sum(v * w for v, w in zip(values, weights)) / sw
    var = sum(w * (v - mean) ** 2 for v, w in zip(values, weights)) / sw
    n_eff = sw * sw / sum(w * w for w in weights)
    return mean, math.sqrt(var), n_eff


def main(argv=None):
    ap = argparse.ArgumentParser(description="Phase 2 politician scorecard")
    ap.add_argument("--db", default=dbmod.DB_PATH_DEFAULT)
    ap.add_argument("--csv", default="analysis/POLITICIAN_SCORECARD.csv")
    ap.add_argument("--md", default="analysis/POLITICIAN_SCORECARD.md")
    ap.add_argument("--registry", default="analysis/registry.json")
    ap.add_argument("--no-mtm", action="store_true", help="skip quote snapshot")
    args = ap.parse_args(argv)

    con = dbmod.connect(args.db)
    purchases = load_purchases(con)
    if purchases.empty:
        print("FATAL no stock purchases in DB, run Phase 1 first", file=sys.stderr)
        return 2

    # Guard against filer-typo dates (e.g. a House PTR with tx_date 3031-04-30).
    # Both clocks need sane dates; a bad one poisons horizon arithmetic. Drop and
    # count — never silently coerce.
    today = dt.date.today().isoformat()
    floor_date = "2004-01-01"

    def _date_ok(s):
        return isinstance(s, str) and floor_date <= s <= today

    # F4 integrity: negative disclosure lag (disclosure before the trade) is
    # impossible and excluded from BOTH clocks, same as out-of-range dates.
    lag_ok = purchases.disclosure_date >= purchases.tx_date
    good = (
        purchases.tx_date.map(_date_ok)
        & purchases.disclosure_date.map(_date_ok)
        & lag_ok
    )
    n_bad = int((~good).sum())
    n_neg_lag = int((~lag_ok).sum())
    if n_bad:
        bad = purchases[~good]
        bad[["person_id", "name", "ticker", "tx_date", "disclosure_date"]].to_csv(
            "analysis/scorecard_bad_dates.csv", index=False
        )
        print(
            "[score] dropped {} purchases with out-of-range dates -> "
            "analysis/scorecard_bad_dates.csv".format(n_bad)
        )
    purchases = purchases[good].copy()
    # F4 regression guard: nothing malformed survives into scoring, both clocks.
    assert (purchases.disclosure_date >= purchases.tx_date).all(), \
        "negative-lag row leaked into scoring"
    assert purchases.tx_date.map(_date_ok).all() and \
        purchases.disclosure_date.map(_date_ok).all(), \
        "out-of-range date leaked into scoring"
    print("[score] excluded before scoring: {} bad-date/neg-lag rows "
          "({} negative-lag)".format(n_bad, n_neg_lag))

    counts = purchases.groupby("person_id").size()
    eligible_ids = set(counts[counts >= PERSON_FLOOR].index)
    scored = purchases[purchases.person_id.isin(eligible_ids)].copy()
    # F1 cluster correction: floor gates on raw fills (data-sufficiency), but
    # scoring runs on clustered accumulation episodes so DCA into one conviction
    # is not counted as N independent correct calls.
    fills_total = scored.groupby("person_id").size().to_dict()
    events = cluster_purchases(scored, CLUSTER_WINDOW_DAYS)
    print(
        "[score] persons total={} floor_pass={} raw_fills={} clustered_events={}".format(
            purchases.person_id.nunique(), len(eligible_ids), len(scored), len(events)
        )
    )
    if events.empty:
        print("FATAL nobody passes the {}-purchase floor".format(PERSON_FLOOR), file=sys.stderr)
        return 2

    px_start = (
        dt.date.fromisoformat(events.tx_date.min())
        - dt.timedelta(days=PRICE_START_PAD_DAYS)
    ).isoformat()

    spy = build_series(con, "SPY", px_start, today)
    # F6 QQQ benchmark for style-tilt visibility. Informational and never ranked,
    # so a QQQ fetch failure must NOT abort a ranking run (SPY stays fail-hard).
    try:
        qqq = build_series(con, "QQQ", px_start, today)
    except prices.PriceError as exc:
        qqq = {}
        print("[score] WARN QQQ benchmark unavailable, F6 column blank: {}".format(
            str(exc).split(" raw=")[0]))
    # Exclude today's date: its session may be incomplete, ranking is EOD-only.
    spy_dates = sorted(d for d in spy if d < today)
    if len(spy_dates) < 200:
        print("FATAL SPY calendar too thin", file=sys.stderr)
        return 2
    anchor = spy_dates[-1]
    spy_idx = {d: i for i, d in enumerate(spy_dates)}

    tickers = sorted(events.ticker.unique())
    # F3: skip tickers already classified no-series so we do not slow-retry dead
    # names every run (this was the coverage-loop hang under flaky DNS).
    dead = {
        r[0]
        for r in con.execute(
            "SELECT ticker FROM ticker_status WHERE verdict IN "
            "('delisted_presumed','data_gap')"
        )
    }
    series = {}
    price_failures = {}
    for i, t in enumerate(tickers):
        if t in dead:
            price_failures[t] = "cached no-series (ticker_status)"
            continue
        t_start = (
            dt.date.fromisoformat(events[events.ticker == t].tx_date.min())
            - dt.timedelta(days=PRICE_START_PAD_DAYS)
        ).isoformat()
        try:
            series[t] = build_series(con, t, t_start, today)
        except prices.PriceError as exc:
            price_failures[t] = str(exc).split(" raw=")[0]
        if (i + 1) % 50 == 0:
            print("[score] price coverage {}/{}".format(i + 1, len(tickers)), flush=True)
    print(
        "[score] tickers={} covered={} failed={}".format(
            len(tickers), len(series), len(price_failures)
        )
    )

    def entry_on_calendar(date_iso):
        """First trading date >= date_iso on SPY calendar, else None."""
        i = bisect.bisect_left(spy_dates, date_iso)
        return (spy_dates[i], i) if i < len(spy_dates) else (None, None)

    trade_rows = []
    excluded_price = 0
    for r in events.itertuples():
        ser = series.get(r.ticker)
        if ser is None:
            excluded_price += 1
            continue
        rec = {
            "person_id": r.person_id,
            "name": r.name,
            "chamber": r.chamber,
            "ticker": r.ticker,
            "mid": r.mid,  # F1: already the summed cluster midpoint
            "n_fills": r.n_fills,
            "tx_date": r.tx_date,
            "lag_days": r.lag_days,
        }
        usable = False
        for clock, start_date in (("A", r.tx_date), ("B", r.disclosure_date)):
            entry_date, ei = entry_on_calendar(start_date)
            if entry_date is None or entry_date not in ser or entry_date not in spy:
                continue
            pe, se = ser[entry_date], spy[entry_date]
            rec["entry_px_" + clock] = pe
            rec["entry_spy_" + clock] = se
            rec["entry_date_" + clock] = entry_date
            for h in HORIZONS:
                xi = ei + h
                if xi >= len(spy_dates):
                    rec["open_{}_{}".format(clock, h)] = True
                    continue
                exit_date = spy_dates[xi]
                if exit_date not in ser:
                    rec["miss_{}_{}".format(clock, h)] = True
                    continue
                excess = (ser[exit_date] / pe - 1.0) - (spy[exit_date] / se - 1.0)
                rec["x_{}_{}".format(clock, h)] = excess
                usable = True
                # F6 QQQ-relative excess, clock A 63d only (informational).
                if clock == "A" and h == 63 and entry_date in qqq and exit_date in qqq:
                    rec["xq_A_63"] = (ser[exit_date] / pe - 1.0) - (
                        qqq[exit_date] / qqq[entry_date] - 1.0
                    )
        if usable or any(rec.get("open_A_{}".format(h)) for h in HORIZONS):
            trade_rows.append(rec)
        else:
            excluded_price += 1
    print(
        "[score] scored trade events={} excluded_missing_price={}".format(
            len(trade_rows), excluded_price
        )
    )
    if not trade_rows:
        print("FATAL zero scoreable trades", file=sys.stderr)
        return 2

    mids = [t["mid"] for t in trade_rows]
    p90 = sorted(mids)[int(0.9 * (len(mids) - 1))]
    anchor_date = dt.date.fromisoformat(anchor)
    for t in trade_rows:
        months = (anchor_date - dt.date.fromisoformat(t["tx_date"])).days / 30.44
        t["w"] = min(t["mid"], p90) * recency_weight(months)

    # MTM snapshot for open cohort, clock A 126d not complete.
    mtm_quotes = {}
    mtm_asofs = []
    open_tickers = sorted(
        {t["ticker"] for t in trade_rows if t.get("open_A_126") and "entry_px_A" in t}
    )
    spy_quote = None
    if open_tickers and not args.no_mtm:
        try:
            spy_quote, spy_asof = prices.latest(con, "SPY")
            mtm_asofs.append(spy_asof)
        except prices.PriceError as exc:
            print("[score] WARN SPY quote failed, MTM skipped: {}".format(exc))
        if spy_quote is not None:
            for t in open_tickers:
                try:
                    q, asof = prices.latest(con, t)
                    mtm_quotes[t] = q
                    mtm_asofs.append(asof)
                except prices.PriceError as exc:
                    print("[score] WARN quote {} failed: {}".format(t, str(exc).split(" raw=")[0]))

    sells = load_sells(con).set_index("person_id").n_sells.to_dict()
    rows_out = []
    for pid in sorted({t["person_id"] for t in trade_rows}):
        pts = [t for t in trade_rows if t["person_id"] == pid]
        name = pts[0]["name"]
        out = {
            "person_id": int(pid),
            "person": name,
            "chamber": pts[0]["chamber"],
            "n_purchases_lifetime": int(counts[pid]),
            "n_fills_total": int(fills_total.get(pid, 0)),
            "n_sells_lifetime": int(sells.get(pid, 0)),
            "n_scored_events": len(pts),
        }
        for clock in ("A", "B"):
            for h in HORIZONS:
                key = "x_{}_{}".format(clock, h)
                have = [(t[key], t["w"]) for t in pts if key in t]
                out["n_completed_{}{}".format(clock, h)] = len(have)
                if have:
                    m, s, ne = wstats([v for v, _ in have], [w for _, w in have])
                    out["wavg_excess_{}{}".format(clock, h)] = m
                else:
                    out["wavg_excess_{}{}".format(clock, h)] = None
        a63 = [(t["x_A_63"], t["w"]) for t in pts if "x_A_63" in t]
        if len(a63) >= 2:
            m, s, ne = wstats([v for v, _ in a63], [w for _, w in a63])
            out["t_stat"] = (m / (s / math.sqrt(ne))) if s and s > 0 else None
            out["hit_rate_63A"] = sum(1 for v, _ in a63 if v > 0) / len(a63)
        else:
            out["t_stat"] = None
            out["hit_rate_63A"] = None
        ga = out["wavg_excess_A63"]
        gb = out["wavg_excess_B63"]
        out["copyability_gap_63"] = (ga - gb) if ga is not None and gb is not None else None
        # F6 style-tilt: QQQ-relative A63 excess alongside the SPY-based number.
        qhave = [(t["xq_A_63"], t["w"]) for t in pts if "xq_A_63" in t]
        if qhave:
            mq, _, _ = wstats([v for v, _ in qhave], [w for _, w in qhave])
            out["wavg_excess_A63_vs_QQQ"] = mq
        else:
            out["wavg_excess_A63_vs_QQQ"] = None
        out["median_lag_days"] = float(pd.Series([t["lag_days"] for t in pts]).median())
        byt = {}
        for t in pts:
            byt[t["ticker"]] = byt.get(t["ticker"], 0.0) + t["w"]
        out["top5_tickers"] = " ".join(
            k for k, _ in sorted(byt.items(), key=lambda kv: -kv[1])[:5]
        )
        cutoff = (anchor_date - dt.timedelta(days=365)).isoformat()
        out["active_last_12mo"] = any(t["tx_date"] >= cutoff for t in pts)

        opens = [
            t
            for t in pts
            if t.get("open_A_126") and "entry_px_A" in t and t["ticker"] in mtm_quotes
        ]
        out["n_open"] = sum(1 for t in pts if t.get("open_A_126"))
        if opens and spy_quote is not None:
            vals, ws = [], []
            for t in opens:
                x = (mtm_quotes[t["ticker"]] / t["entry_px_A"] - 1.0) - (
                    spy_quote / t["entry_spy_A"] - 1.0
                )
                vals.append(x)
                ws.append(t["w"])
            m, _, _ = wstats(vals, ws)
            out["mtm_open_excess"] = m
            out["mtm_n_marked"] = len(opens)
        else:
            out["mtm_open_excess"] = None
            out["mtm_n_marked"] = 0
        rows_out.append(out)

    df = pd.DataFrame(rows_out).sort_values("t_stat", ascending=False, na_position="last")
    df.to_csv(args.csv, index=False)

    ranked = df[df.t_stat.notna() & (df.n_completed_A63 >= 5)]
    top_t = ranked.head(15)
    top_b = df[df.wavg_excess_B63.notna() & (df.n_completed_B63 >= 5)].sort_values(
        "wavg_excess_B63", ascending=False
    ).head(15)
    both = sorted(set(top_t.person) & set(top_b.person))

    chambers = sorted({t["chamber"] for t in trade_rows})
    horizons = []
    for cham in chambers:
        # earliest tx_date actually present for the chamber = coverage floor
        floor = min(
            (t["tx_date"] for t in trade_rows if t["chamber"] == cham),
            default="n/a",
        )
        horizons.append("{} coverage from {}".format(cham, floor))
    scope = " + ".join(c.upper() for c in chambers)
    horizon = "; ".join(horizons) if horizons else "no horizon recorded"
    mtm_block = (
        "MTM quotes: {} tickers, asof_unix min={} max={}".format(
            len(mtm_quotes), min(mtm_asofs), max(mtm_asofs)
        )
        if mtm_asofs
        else "MTM snapshot skipped or empty"
    )

    def fmt_table(sub, cols):
        return md_table(sub[cols])

    md = []
    md.append("# POLITICIAN_SCORECARD — smart_money_daemon Phase 2 (SM-2 processing layer)")
    md.append("")
    md.append(
        "**Chamber scope: {}.** {}. "
        "Ranking anchor date (last completed EOD): {}.".format(scope, horizon, anchor)
    )
    md.append("")
    md.append(
        "SM-2 corrections applied: F1 cluster correction (accumulation episodes "
        "scored as one event, {}-day window); F2 registry sectioning + "
        "registry.json; F5 amendment supersede filter; F6 QQQ style-tilt column. "
        "See analysis/archive/POLITICIAN_SCORECARD_sm1.md for the pre-SM-2 "
        "baseline.".format(CLUSTER_WINDOW_DAYS)
    )
    md.append("")
    md.append("MTM snapshot block: {}".format(mtm_block))
    md.append("")
    md.append("## Top 15 by t-stat (clock A 63d, completed clustered events only)")
    md.append("")
    md.append(
        fmt_table(
            top_t,
            [
                "person", "t_stat", "wavg_excess_A63", "wavg_excess_A63_vs_QQQ",
                "hit_rate_63A", "n_completed_A63", "n_fills_total", "n_open",
                "copyability_gap_63", "median_lag_days", "active_last_12mo",
                "top5_tickers",
            ],
        )
    )
    md.append("")
    md.append("## Top 15 by clock-B 63d weighted excess (the follow-able list)")
    md.append("")
    md.append(
        fmt_table(
            top_b,
            [
                "person", "wavg_excess_B63", "wavg_excess_A63", "n_completed_B63",
                "n_open", "median_lag_days", "active_last_12mo", "top5_tickers",
            ],
        )
    )
    md.append("")
    # F2 registry sectioning: strong-on-both-lists split by forward-signal.
    # Qualitative-seed names are emitted via the seed path (not as ranked
    # performers), so exclude them from the performer registry lists.
    by_name = {r["person"]: r for r in rows_out}
    seed_names = {s["name"] for s in QUALITATIVE_SEEDS}
    both_perf = [nm for nm in both if nm not in seed_names]
    active = [nm for nm in both_perf if by_name[nm].get("active_last_12mo")]
    validation = [nm for nm in both_perf if not by_name[nm].get("active_last_12mo")]

    md.append("## RECOMMENDED REGISTRY — PROPOSAL ONLY, final selection is Mando's")
    md.append("")
    md.append("Active names (traded in the last 12 months, forward-signal candidates):")
    md.append("")
    if active:
        for nm in active:
            md.append("- {}".format(nm))
    else:
        md.append("- (none)")
    md.append("")
    md.append("## VALIDATION COHORT — strong record, NO forward signal")
    md.append("")
    md.append("Strong on both lists but inactive in the last 12 months. Use to "
              "validate methodology, NOT to follow — they are not currently trading.")
    md.append("")
    if validation:
        for nm in validation:
            md.append("- {}".format(nm))
    else:
        md.append("- (none)")
    md.append("")
    md.append("## NON-PERFORMER / QUALITATIVE WATCH — not a skill claim")
    md.append("")
    md.append("Included in registry.json regardless of t-stat. Their signal value "
              "is composition or flow, not stock-picking skill; scores are shown "
              "honestly but they are NOT ranked performers.")
    md.append("")
    for seed in QUALITATIVE_SEEDS:
        r = by_name.get(seed["name"])
        if r:
            md.append("- {} ({}), cluster-corrected t-stat {} — {}".format(
                seed["name"], seed["role"],
                "n/a" if r.get("t_stat") is None else round(r["t_stat"], 2),
                seed["rationale"]))
    md.append("")
    md.append("## Methodology notes")
    md.append("")
    md.append(
        "- Purchases scored, sells descriptive. Stock assets only, resolvable "
        "ticker, floor >= {} lifetime stock purchases.".format(PERSON_FLOOR)
    )
    md.append(
        "- Two clocks: A from tx_date (skill), B from disclosure_date "
        "(copyability). Horizons 21/63/126 trading days on the SPY calendar, "
        "excess vs SPY, adjusted close."
    )
    md.append(
        "- Ranking uses completed EOD horizons only, deterministic given the "
        "prices table. Open purchases marked to market against labeled quotes, "
        "never feeding the ranking."
    )
    md.append(
        "- Weights: band midpoint capped at p90 (${:,.0f}) times recency decay "
        "(full <= 24mo, linear to 0.25 at 60mo).".format(p90)
    )
    md.append("")
    md.append("## Data-quality caveats")
    md.append("")
    md.append(
        "- {} trade events excluded for missing price coverage, {} tickers had "
        "no usable series. Full list in DATA_QUALITY.md.".format(
            excluded_price, len(price_failures)
        )
    )
    md.append(
        "- Universe: {}. Paper and unparsed-layout filings skipped (never OCRed), "
        "counted in DATA_QUALITY.md. Open-ended top amount band stored with NULL "
        "high, midpoint uses its low bound.".format(scope)
    )
    md.append(
        "- Amount ranges are disclosure bands, not exact sizes. Band midpoints "
        "are a coarse size proxy."
    )
    md.append(
        "- Person rows canonicalized (honorific/whitespace splits merged) before "
        "scoring; see merge_persons."
    )
    pathlib_md = args.md
    with open(pathlib_md, "w") as f:
        f.write("\n".join(md) + "\n")

    with open("analysis/scorecard_price_failures.json", "w") as f:
        json.dump(price_failures, f, indent=1, sort_keys=True)

    _write_registry(active, validation, by_name, anchor, args.registry)

    print("[score] wrote {} ({} persons), {}, and {}".format(
        args.csv, len(df), args.md, args.registry))
    return 0


# Data-driven qualitative seed list — the ONE place to add a non-performer whose
# value is not stock-picking skill. Each entry is included in registry.json
# regardless of the t-stat ranking, with its scores emitted honestly from the
# clustered stats (only ranking-based inclusion is bypassed, never the numbers).
# Next qualitative add is a new dict entry here, not a code change elsewhere.
QUALITATIVE_SEEDS = [
    {
        "name": "McCormick, David H.",
        "role": "btc_flow_sentinel",
        "rationale": "cluster corrected picking t stat near zero tracked as "
                     "crypto flow sentinel not a picker",
    },
    {
        "name": "Foxx, Virginia",
        "role": "qualitative_watch",
        "rationale": "non index book energy bdc shipping composition watch not "
                     "skill claim",
    },
    {
        "name": "Guest, Michael Patrick",
        "role": "performer_seeded",
        "rationale": "fastest discloser 16d median lag dual benchmark positive "
                     "t 2.01 seeded on abelard rec mando approved",
    },
]
SEED_ROLES = {s["name"]: s["role"] for s in QUALITATIVE_SEEDS}


def _reg_scores(r):
    keys = ("t_stat", "wavg_excess_A63", "wavg_excess_B63", "hit_rate_63A")
    out = {k: r.get(k) for k in keys}
    out["n_events_completed"] = r.get("n_completed_A63")
    out["n_open"] = r.get("n_open")
    out["median_lag_days"] = r.get("median_lag_days")
    return out


def _write_registry(active, validation, by_name, anchor, path):
    entries = []
    placed = set()
    for status, names in (("active", active), ("validation", validation)):
        for nm in names:
            r = by_name[nm]
            entries.append({
                "person_id": r["person_id"],
                "name": nm,
                "chamber": r["chamber"],
                "status": status,
                "role": SEED_ROLES.get(nm, "performer"),
                "scores": _reg_scores(r),
                "as_of": anchor,
            })
            placed.add(nm)
    # Qualitative seeds included regardless of ranking (signal is not skill).
    # Scores emitted honestly from clustered stats; only ranking inclusion is
    # bypassed.
    for seed in QUALITATIVE_SEEDS:
        nm = seed["name"]
        if nm in placed or nm not in by_name:
            continue
        r = by_name[nm]
        entries.append({
            "person_id": r["person_id"],
            "name": nm,
            "chamber": r["chamber"],
            "status": "active" if r.get("active_last_12mo") else "validation",
            "role": seed["role"],
            "rationale": seed["rationale"],
            "scores": _reg_scores(r),
            "as_of": anchor,
        })
        placed.add(nm)
    # 13F leg — one registry across all legs. Scores null, future work.
    entries.append({
        "person_id": None,
        "name": "Situational Awareness LP (Aschenbrenner)",
        "cik": "0002045724",
        "chamber": None,
        "status": "active",
        "role": "manager_13f",
        "type": "manager_13f",
        "scores": None,
        "as_of": anchor,
    })
    with open(path, "w") as f:
        json.dump({"as_of": anchor, "entries": entries}, f, indent=2)


if __name__ == "__main__":
    sys.exit(main())
