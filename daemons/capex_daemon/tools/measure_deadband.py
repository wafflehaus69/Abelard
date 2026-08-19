"""P1 — dead-band measurement. Report-only; publishes no state and sets no constant.

Measures the distribution of QUARTER-OVER-QUARTER CHANGE IN TTM YoY — the
quantity a dead-band has to suppress — across three series classes.

  TTM YoY(t) = TTM(t) / TTM(t-4) - 1        needs 8 quarters
  delta(t)   = TTM YoY(t) - TTM YoY(t-1)    needs 9, in percentage points

Two measurement decisions, both forced by the data and both load-bearing for P3:

**Aggregates key on CALENDAR QUARTER, not raw period_end.** Microsoft closes
Jun/Sep/Dec/Mar and Oracle Feb/May/Aug/Nov, so a bucket sum keyed on raw end
dates has an empty member intersection and produces one usable observation. The
calendar label is what makes members addable at all.

**The window is split.** Builder history reaches back to when these issuers were
micro-cap miners: MARA's full-history |delta| tops 98,000pp, because a base of a
few hundred thousand dollars growing to a few million is a five-figure growth
rate. Those observations are arithmetically correct and describe a company that
no longer exists. A band calibrated on them would suppress everything. The
recent window is the regime the classifier will actually run in.
"""
import json
import os
import statistics as st
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from capex_daemon import facts_api, normalize, tagmap, universe  # noqa: E402

WINDOW = 4
RECENT_FROM = "2023-01-01"
AGGREGATED = ("hyperscaler", "builder", "reit")


def calendar_map(indexed):
    """{calendar_quarter: value} for one issuer, or None if capex is unresolved."""
    r = tagmap.resolve(indexed, tagmap.CAPEX)
    if r.is_multi_line or r.is_unresolved:
        return None
    out = {}
    for row in normalize.discrete_quarters(tagmap.series_facts(indexed, r)):
        out[row.calendar_quarter] = (row.value, row.period_end)
    return out


def _cq_sort(q):
    y, n = q.split("Q")
    return (int(y), int(n))


def yoy_from(qmap):
    """{calendar_quarter: (TTM YoY, period_end)} over a per-issuer calendar map."""
    qs = sorted(qmap, key=_cq_sort)
    ttm = {}
    for i in range(WINDOW - 1, len(qs)):
        win = qs[i - WINDOW + 1: i + 1]
        if not _contiguous(win):
            continue
        ttm[qs[i]] = sum(qmap[q][0] for q in win)
    te = sorted(ttm, key=_cq_sort)
    out = {}
    for i in range(WINDOW, len(te)):
        if not _contiguous([te[i - WINDOW], te[i]], span=WINDOW):
            continue
        prior = ttm[te[i - WINDOW]]
        if prior and prior > 0:
            out[te[i]] = (ttm[te[i]] / prior - 1.0, qmap[te[i]][1])
    return out


def _contiguous(quarters, span=None):
    """True when the quarters are consecutive calendar quarters."""
    idx = [_cq_sort(q)[0] * 4 + _cq_sort(q)[1] for q in quarters]
    if span is not None:
        return idx[-1] - idx[0] == span
    return idx[-1] - idx[0] == len(quarters) - 1


def deltas(yoy, since=None):
    qs = sorted(yoy, key=_cq_sort)
    out = []
    for i in range(1, len(qs)):
        if _cq_sort(qs[i])[0] * 4 + _cq_sort(qs[i])[1] - (
                _cq_sort(qs[i - 1])[0] * 4 + _cq_sort(qs[i - 1])[1]) != 1:
            continue
        end = yoy[qs[i]][1]
        if since and end < since:
            continue
        out.append((qs[i], (yoy[qs[i]][0] - yoy[qs[i - 1]][0]) * 100.0))
    return out


def pct(vals, p):
    s = sorted(vals)
    return s[min(len(s) - 1, int(p * len(s)))] if s else None


def line(label, vals):
    if not vals:
        return "  {:<24} n=0".format(label)
    a = sorted(abs(v) for v in vals)
    return ("  {:<24} n={:<4} p10={:>6.1f} p25={:>6.1f} p50={:>6.1f} "
            "p75={:>7.1f} p90={:>8.1f} max={:>9.1f}".format(
                label, len(a), pct(a, .10), pct(a, .25), st.median(a),
                pct(a, .75), pct(a, .90), a[-1]))


def bucket_sum_yoy(members, since=None):
    """Matched-membership bucket sum on calendar quarters.

    Both sides of every YoY are computed over the INTERSECTION of members that
    have a complete window on each side, so an arriving or departing name can
    never read as growth.
    """
    quarters = sorted({q for m in members.values() for q in m}, key=_cq_sort)
    yoy = {}
    for i in range(WINDOW * 2 - 1, len(quarters)):
        cw = quarters[i - WINDOW + 1: i + 1]
        pw = quarters[i - WINDOW * 2 + 1: i - WINDOW + 1]
        if not (_contiguous(cw) and _contiguous(pw)):
            continue
        common = [t for t, m in members.items()
                  if all(q in m for q in cw) and all(q in m for q in pw)]
        if not common:
            continue
        cur = sum(sum(members[t][q][0] for q in cw) for t in common)
        pri = sum(sum(members[t][q][0] for q in pw) for t in common)
        if pri > 0:
            end = max(members[t][quarters[i]][1] for t in common)
            yoy[quarters[i]] = (cur / pri - 1.0, end, len(common))
    return yoy


def main():
    scratch = os.environ["SCRATCH"]
    roster = universe.load()
    per_issuer, by_bucket_members = {}, {}
    for cik, e in roster.items():
        p = os.path.join(scratch, "cf_%s.json" % e.ticker_display)
        if not os.path.exists(p):
            continue
        qmap = calendar_map(facts_api.index_facts(json.load(open(p, encoding="utf-8"))))
        if not qmap:
            continue
        per_issuer[e.ticker_display] = (e.bucket, qmap)
        by_bucket_members.setdefault(e.bucket, {})[e.ticker_display] = qmap

    for tag, since in (("FULL HISTORY", None), ("RECENT (period_end >= %s)" % RECENT_FROM, RECENT_FROM)):
        print("=" * 100)
        print("WINDOW: {}".format(tag))
        print("-" * 100)
        print("CLASS 1 — PER ISSUER")
        pooled, bucket_pool = [], {}
        for tick, (bucket, qmap) in sorted(per_issuer.items()):
            d = [v for _, v in deltas(yoy_from(qmap), since)]
            if not d:
                continue
            bucket_pool.setdefault(bucket, []).extend(d)
            pooled.extend(d)
        for b in ("hyperscaler", "builder", "reit", "host", "mirror"):
            if b in bucket_pool:
                print(line(b + " pooled", bucket_pool[b]))
        print(line("ALL POOLED", pooled))

        print("CLASS 2 — BUCKET SUM (calendar-aligned, matched membership)")
        bsum = {}
        for b in AGGREGATED:
            if b not in by_bucket_members:
                continue
            y = bucket_sum_yoy(by_bucket_members[b], since)
            d = [v for _, v in deltas({k: (v[0], v[1]) for k, v in y.items()}, since)]
            bsum[b] = d
            print(line(b + " sum", d))

        print("CLASS 3 — TOTAL PANEL")
        allm = {}
        for b in AGGREGATED:
            allm.update(by_bucket_members.get(b, {}))
        y = bucket_sum_yoy(allm, since)
        td = [v for _, v in deltas({k: (v[0], v[1]) for k, v in y.items()}, since)]
        print(line("total panel", td))
        print()

        if since:
            print("-" * 100)
            print("PROPOSED DEAD-BANDS (percentile logic stated; NOT applied)")
            for label, vals in (("per-issuer hyperscaler", bucket_pool.get("hyperscaler", [])),
                                ("per-issuer builder", bucket_pool.get("builder", [])),
                                ("per-issuer reit", bucket_pool.get("reit", [])),
                                ("per-issuer host", bucket_pool.get("host", [])),
                                ("bucket-sum hyperscaler", bsum.get("hyperscaler", [])),
                                ("bucket-sum builder", bsum.get("builder", [])),
                                ("bucket-sum reit", bsum.get("reit", [])),
                                ("total panel", td)):
                a = sorted(abs(v) for v in vals)
                if not a:
                    print("  {:<24} n=0 — INSUFFICIENT DATA TO PROPOSE".format(label))
                    continue
                print("  {:<24} n={:<4} p25={:>7.1f}pp  ->  proposed band {:>6.1f}pp".format(
                    label, len(a), pct(a, .25), round(pct(a, .25))))
            print()
            print("  Logic: band = p25 of |Δ| in the recent window. The smallest quarter of")
            print("  observed moves is treated as noise and cannot trigger a direction change;")
            print("  three quarters of real moves still register. Stated for ratification only.")


if __name__ == "__main__":
    main()
