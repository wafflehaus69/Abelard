"""P3 — aggregate trend: bucket-sum series, matched membership, breadth.

**Aggregates key on CALENDAR QUARTER, permanently (ratified 2026-08-18).**
Microsoft closes Jun/Sep/Dec/Mar and Oracle Feb/May/Aug/Nov, so a bucket sum
keyed on raw period-end dates has a near-empty member intersection — the first
P1 measurement produced ONE usable observation for the hyperscaler sum before
this was fixed. The calendar label is what makes members addable at all. This is
the third appearance of the fiscal-misalignment trap: first in normalization,
then in the SEC `frame` labels, now in aggregation.

**Matched membership.** Both sides of every YoY are computed over the
INTERSECTION of members holding a complete window on each side. A name arriving
or departing therefore cannot read as growth. Membership changes are published
as composition events BESIDE the trend, never blended into it.

**Companion series.** Capex alone, and capex + finance-lease additions. Microsoft
deploys up to $9.15B a quarter through leases, so the two series answer
different questions and are published together rather than one standing in for
the other.
"""
from . import commitments, divergence, facts_api, normalize, phases, tagmap

WINDOW = 4
AGGREGATED_BUCKETS = ("hyperscaler", "builder", "landlord")

LEASE_CONCEPT = "RightOfUseAssetObtainedInExchangeForFinanceLeaseLiability"

# A bucket "sum" over one member is not a sum — it is that member's own number
# wearing a bucket label. Measured live: DLR's series ends 2026Q1 while EQIX
# reaches 2026Q2, so matched membership correctly refused to mix them and then
# published a one-name REIT bucket at +56.7%, which was simply EQIX. Matched
# membership guards the COMPARISON; this guards the AGGREGATE.
MIN_BUCKET_MEMBERS = 2
STATE_INSUFFICIENT_MEMBERSHIP = "INSUFFICIENT-MEMBERSHIP"

CHANGE_ENTERED = "entered"
CHANGE_LEFT = "left"


def _cq_sort(q):
    y, n = q.split("Q")
    return (int(y), int(n))


def _cq_index(q):
    y, n = _cq_sort(q)
    return y * 4 + n


def _contiguous(quarters):
    idx = [_cq_index(q) for q in quarters]
    return idx[-1] - idx[0] == len(quarters) - 1


def issuer_calendar_series(indexed, include_leases=False, cik=None):
    """{calendar_quarter: value} for one issuer, or None when capex is unresolved."""
    r = tagmap.resolve(indexed, tagmap.CAPEX, cik=cik)
    if r.is_multi_line or r.is_unresolved:
        return None
    rows = normalize.discrete_quarters(tagmap.series_facts(indexed, r))
    out = {x.calendar_quarter: x.value for x in rows}
    if include_leases:
        lf = [f for f in indexed.get(LEASE_CONCEPT, []) if f.unit == "USD" and f.period_start]
        if lf:
            pairs = [(f, LEASE_CONCEPT) for f in facts_api.dedupe_latest_filed(lf)]
            for x in normalize.discrete_quarters(pairs):
                if x.calendar_quarter in out:
                    out[x.calendar_quarter] += x.value
    return out


def ttm_by_quarter(qmap):
    """{quarter: TTM} over contiguous 4-quarter windows only."""
    qs = sorted(qmap, key=_cq_sort)
    out = {}
    for i in range(WINDOW - 1, len(qs)):
        win = qs[i - WINDOW + 1: i + 1]
        if _contiguous(win):
            out[qs[i]] = sum(qmap[q] for q in win)
    return out


def issuer_yoy(qmap):
    """{quarter: TTM YoY} for one issuer."""
    ttm = ttm_by_quarter(qmap)
    qs = sorted(ttm, key=_cq_sort)
    out = {}
    for i in range(WINDOW, len(qs)):
        if _cq_index(qs[i]) - _cq_index(qs[i - WINDOW]) != WINDOW:
            continue
        prior = ttm[qs[i - WINDOW]]
        if prior and prior > 0:
            out[qs[i]] = ttm[qs[i]] / prior - 1.0
    return out


def _commitments_calendar(cik, indexed):
    """{calendar_quarter: forward commitment stock} for one issuer, else None.

    Only a covered series contributes. UNCOVERED-UNTAGGED issuers are absent
    from the panel area rather than depressing it with a zero — the same rule
    the commitments view already publishes per issuer.
    """
    cs = commitments.forward_commitments(cik, indexed)
    if not cs or not cs.points:
        return None
    out = {}
    for p in cs.points:
        q, _off = normalize.calendar_align(p.period_end)
        out[q] = p.value               # latest observation wins within a quarter
    return out or None


def matched_ttm_series(members):
    """({quarter: ttm}, {quarter: [members]}) over a complete contiguous window.

    The level companion to `bucket_trend`, which publishes only YoY. View 0 plots
    LEVELS, so it needs the same matched-membership discipline applied one rung
    lower: at each quarter, sum only those members holding a full contiguous
    four-quarter window ending there. Membership is returned beside the number
    for the same reason it is everywhere else — a level that moved because a name
    arrived is not a level that moved.
    """
    quarters = sorted({q for m in members.values() for q in m}, key=_cq_sort)
    ttm, membership = {}, {}
    for i in range(WINDOW - 1, len(quarters)):
        win = quarters[i - WINDOW + 1: i + 1]
        if not _contiguous(win):
            continue
        common = sorted(t for t, m in members.items() if all(q in m for q in win))
        if not common:
            continue
        ttm[quarters[i]] = sum(sum(members[t][q] for q in win) for t in common)
        membership[quarters[i]] = common
    return ttm, membership


MIN_PANEL_QUARTERS = 6

# A plotted level must cover essentially all of the dollars it claims to be
# about. 0.95 is not a tuned parameter — the live frontier has a sharp knee:
# reaching back past 2018Q2 drops Amazon and costs 35pp of coverage at once,
# while stopping short of it buys 1.4pp for fifteen fewer quarters. Any floor
# between roughly 0.66 and 0.98 selects the same window.
COVERAGE_FLOOR = 0.95


class ConstantPanel:
    """A member set, a window, the summed level, and what it leaves out."""

    def __init__(self, members, quarters, level, coverage, lagging):
        self.members = members
        self.quarters = quarters
        self.level = level
        self.coverage = coverage
        self.lagging = lagging          # [(ticker, last_quarter, last_value)]

    def __bool__(self):
        return bool(self.members)

    __nonzero__ = __bool__

    def __repr__(self):
        return "ConstantPanel({} names, {} quarters, {:.1%})".format(
            len(self.members or []), len(self.quarters or []), self.coverage)


def constant_membership_panel(member_maps, min_members=MIN_BUCKET_MEMBERS,
                              min_quarters=MIN_PANEL_QUARTERS,
                              coverage_floor=COVERAGE_FLOOR):
    """Pick the (member set, trailing window) a LEVEL may honestly be drawn over.

    Matched membership makes a *comparison* safe: both sides of a YoY are taken
    over names present on both sides. It does not make a *level series* safe.
    Measured on the live panel, total-panel matched membership runs 1 → 12
    members across 66 quarters, so a plotted level would show a rise that is
    substantially name arrival. The YoY charts are immune to this; the front
    page plots levels, so it needs its own guard.

    The guard: hold membership CONSTANT across the plotted window, and take the
    LONGEST window whose members still cover `coverage_floor` of the dollars
    reported at the window's end. Coverage first, history second — the panel
    exists to measure capex dollars, so a long window that has lost a third of
    them is the worse chart. Maximising members × quarters instead was tried and
    selected a 44-quarter window that dropped Amazon and Meta.

    `lagging` names — issuers reporting at some point but not through the window
    end, i.e. behind on filing — are returned rather than silently dropped, so
    the chart can say who is missing instead of implying nobody is.
    """
    quarters = sorted({q for m in member_maps.values() for q in m}, key=_cq_sort)
    if not quarters:
        return ConstantPanel(None, None, None, 0.0, [])
    end = quarters[-1]
    at_end = {t: m[end] for t, m in member_maps.items() if end in m}
    denom = sum(abs(v) for v in at_end.values())

    best = None
    for i in range(len(quarters)):
        win = quarters[i:]
        if len(win) < min_quarters or not _contiguous(win):
            continue
        common = sorted(t for t, m in member_maps.items() if all(q in m for q in win))
        if len(common) < min_members:
            continue
        cov = (sum(abs(at_end[t]) for t in common) / denom) if denom else 0.0
        if cov + 1e-12 < coverage_floor:
            continue
        if best is None or len(win) > len(best[1]):        # longest qualifying
            best = (common, win, cov)
    if best is None:
        return ConstantPanel(None, None, None, 0.0, [])
    common, win, cov = best
    lagging = sorted((t, max(m, key=_cq_sort), m[max(m, key=_cq_sort)])
                     for t, m in member_maps.items()
                     if t not in common and m and end not in m)
    return ConstantPanel(common, win,
                         {q: sum(member_maps[t][q] for t in common) for q in win},
                         cov, lagging)


def matched_stock_series(members):
    """({quarter: stock}, {quarter: [members]}) for INSTANT quantities.

    Forward commitments are a stock, not a flow, so no window is summed — the
    value at a quarter is the sum over members observed at that quarter.
    """
    quarters = sorted({q for m in members.values() for q in m}, key=_cq_sort)
    stock, membership = {}, {}
    for q in quarters:
        common = sorted(t for t, m in members.items() if q in m)
        if common:
            stock[q] = sum(members[t][q] for t in common)
            membership[q] = common
    return stock, membership


def breadth_series(issuer_obs, tickers=None):
    """{quarter: breadth} — the state census per quarter, not just the latest.

    The breadth strip on View 0 needs history. `phases.breadth` counts a single
    snapshot of states; this walks each issuer's observation history and takes
    the census at every quarter any of them classify.
    """
    per_q, dir_q = {}, {}
    for tick, obs in issuer_obs.items():
        if tickers is not None and tick not in tickers:
            continue
        for o in obs:
            per_q.setdefault(o.quarter, {})[tick] = o.state
            dir_q.setdefault(o.quarter, {})[tick] = o.direction
    # B6 — both censuses, side by side. A state is a run and a direction is this
    # quarter; publishing only the first made a quarter in which most names
    # turned look identical to a quiet one.
    out = {}
    for q, states in per_q.items():
        row = dict(phases.breadth(states))
        row.update(phases.breadth_by_direction(dir_q.get(q, {})))
        out[q] = row
    return out


class BucketTrend:
    def __init__(self, bucket, yoy, membership, ttm, composition_events):
        self.bucket = bucket
        self.yoy = yoy
        self.membership = membership
        self.ttm = ttm
        self.composition_events = composition_events

    def __repr__(self):
        return "BucketTrend({} n_quarters={})".format(self.bucket, len(self.yoy))


def bucket_trend(bucket, members):
    """Matched-membership bucket-sum YoY plus composition events.

    `members` is {ticker: {quarter: value}}.
    """
    quarters = sorted({q for m in members.values() for q in m}, key=_cq_sort)
    yoy, membership, ttm, events = {}, {}, {}, []
    prev_common = None
    for i in range(WINDOW * 2 - 1, len(quarters)):
        cw = quarters[i - WINDOW + 1: i + 1]
        pw = quarters[i - WINDOW * 2 + 1: i - WINDOW + 1]
        if not (_contiguous(cw) and _contiguous(pw)):
            continue
        common = {t for t, m in members.items()
                  if all(q in m for q in cw) and all(q in m for q in pw)}
        if not common:
            continue
        cur = sum(sum(members[t][q] for q in cw) for t in common)
        pri = sum(sum(members[t][q] for q in pw) for t in common)
        if pri <= 0:
            continue
        q = quarters[i]
        yoy[q] = cur / pri - 1.0
        membership[q] = sorted(common)
        ttm[q] = cur
        if prev_common is not None:
            for t in sorted(common - prev_common):
                events.append((bucket, q, t, CHANGE_ENTERED))
            for t in sorted(prev_common - common):
                events.append((bucket, q, t, CHANGE_LEFT))
        prev_common = common
    return BucketTrend(bucket, yoy, membership, ttm, events)


def full_panel_trend(all_members):
    """Total panel across the aggregated buckets, same matched-membership rule."""
    return bucket_trend("total", all_members)


def build(roster, indexed_by_cik, include_leases=False):
    """Everything P3 owns, for one scan.

    Returns per-issuer series and states, bucket trends and their states, the
    total-panel trend and state, and breadth per bucket.
    """
    issuer_series, issuer_states, issuer_obs = {}, {}, {}
    bucket_members = {}
    issuance_members, commitment_members = {}, {}

    for cik, entity in roster.items():
        indexed = indexed_by_cik.get(cik)
        if indexed is None:
            continue
        qmap = issuer_calendar_series(indexed, include_leases=include_leases,
                                      cik=entity.cik)
        if not qmap:
            continue
        issuer_series[entity.ticker_display] = qmap
        if entity.bucket in AGGREGATED_BUCKETS:
            bucket_members.setdefault(entity.bucket, {})[entity.ticker_display] = qmap
            imap = divergence.issuer_issuance_calendar_series(indexed)
            if imap:
                issuance_members[entity.ticker_display] = imap
            cmap = _commitments_calendar(cik, indexed)
            if cmap:
                commitment_members[entity.ticker_display] = cmap

        yoy = issuer_yoy(qmap)
        band_class = "issuer:{}".format(entity.bucket)
        if phases.band_for(band_class) is None:
            issuer_states[entity.ticker_display] = phases.STATE_INSUFFICIENT
            continue
        obs = phases.classify(yoy, band_class, series_key=entity.ticker_display)
        issuer_obs[entity.ticker_display] = obs
        cur = phases.current(obs)
        issuer_states[entity.ticker_display] = cur.state if cur else phases.STATE_INSUFFICIENT

    bucket_trends, bucket_states, bucket_obs = {}, {}, {}
    for bucket, members in bucket_members.items():
        bt = bucket_trend(bucket, members)
        bucket_trends[bucket] = bt
        cls = "bucketsum:{}".format(bucket)
        if phases.band_for(cls) is None or len(bt.yoy) < phases.N_CONFIRM + 1:
            bucket_states[bucket] = phases.STATE_INSUFFICIENT
            continue
        latest = max(bt.membership, key=_cq_sort) if bt.membership else None
        if latest and len(bt.membership[latest]) < MIN_BUCKET_MEMBERS:
            bucket_states[bucket] = STATE_INSUFFICIENT_MEMBERSHIP
            continue
        obs = phases.classify(bt.yoy, cls, series_key="bucket:{}".format(bucket))
        bucket_obs[bucket] = obs
        cur = phases.current(obs)
        bucket_states[bucket] = cur.state if cur else phases.STATE_INSUFFICIENT

    all_members = {}
    for b in AGGREGATED_BUCKETS:
        all_members.update(bucket_members.get(b, {}))
    total = full_panel_trend(all_members)
    total_obs, total_state = [], phases.STATE_INSUFFICIENT
    if len(total.yoy) >= phases.N_CONFIRM + 1:
        total_obs = phases.classify(total.yoy, "total:panel", series_key="total:panel")
        cur = phases.current(total_obs)
        total_state = cur.state if cur else phases.STATE_INSUFFICIENT

    breadth_by_bucket = {}
    for bucket in AGGREGATED_BUCKETS:
        names = {t: s for t, s in issuer_states.items()
                 if t in bucket_members.get(bucket, {})}
        breadth_by_bucket[bucket] = phases.breadth(names)

    # --- View 0 companions: levels, credit, commitments, breadth history ---
    issuance_ttm, issuance_membership = matched_ttm_series(issuance_members)
    commitments_stock, commitments_membership = matched_stock_series(commitment_members)
    panel_breadth = breadth_series(issuer_obs, tickers=set(all_members))

    # Constant-membership panels — the only basis on which a LEVEL is plotted.
    const_capex = constant_membership_panel(
        {t: ttm_by_quarter(m) for t, m in all_members.items()})
    const_issuance = constant_membership_panel(
        {t: ttm_by_quarter(m) for t, m in issuance_members.items()})
    const_commitments = constant_membership_panel(commitment_members)

    # THE JAWS must be a matched pair. The capex panel is chosen for capex
    # coverage and the issuance panel for issuance coverage, and they do not
    # land on the same names — 5 against 2, live. Rebasing one onto the other
    # would draw a gap between DIFFERENT COMPANIES and call it a divergence.
    # So the jaws are computed over the intersection: same names, same window,
    # both legs, and the reader is told which names those are.
    jaws_names = sorted(set(all_members) & set(issuance_members))
    jaws_capex = jaws_issuance = None
    if len(jaws_names) >= MIN_BUCKET_MEMBERS:
        jc = {t: ttm_by_quarter(all_members[t]) for t in jaws_names}
        ji = {t: ttm_by_quarter(issuance_members[t]) for t in jaws_names}
        shared = {t: {q: v for q, v in jc[t].items() if q in ji[t]} for t in jaws_names}
        cp = constant_membership_panel(shared)
        if cp:
            jaws_capex = cp
            jaws_issuance = ConstantPanel(
                cp.members, cp.quarters,
                {q: sum(ji[t][q] for t in cp.members) for q in cp.quarters},
                cp.coverage, [])
    const_buckets = {}
    for b, mem in bucket_members.items():
        cp = constant_membership_panel({t: ttm_by_quarter(m) for t, m in mem.items()})
        if cp:
            const_buckets[b] = cp

    return {
        "issuer_series": issuer_series,
        "issuer_states": issuer_states,
        "issuer_obs": issuer_obs,
        "bucket_trends": bucket_trends,
        "bucket_states": bucket_states,
        "bucket_obs": bucket_obs,
        "total_trend": total,
        "total_obs": total_obs,
        "total_state": total_state,
        "breadth": breadth_by_bucket,
        "issuance_ttm": issuance_ttm,
        "issuance_membership": issuance_membership,
        "commitments_stock": commitments_stock,
        "commitments_membership": commitments_membership,
        "panel_breadth_series": panel_breadth,
        "const_capex": const_capex,
        "const_issuance": const_issuance,
        "const_commitments": const_commitments,
        "const_buckets": const_buckets,
        "jaws_capex": jaws_capex,
        "jaws_issuance": jaws_issuance,
    }
