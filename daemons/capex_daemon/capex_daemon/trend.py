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


def _cq_from_index(i):
    return "{}Q{}".format((i - 1) // 4, (i - 1) % 4 + 1)


def _quarter_end(q):
    from datetime import date
    y, n = _cq_sort(q)
    return date(y, n * 3, {1: 31, 2: 30, 3: 30, 4: 31}[n])


# --- the aggregate frontier ------------------------------------------------
#
# An aggregate's newest point is the newest quarter its MEMBERSHIP has reached,
# not the newest quarter ANY member has reached.
#
# Measured live 2026-09-21. Oracle's fiscal quarter ended 2026-08-31, so its
# 10-Q aligns to calendar 2026Q3 about six weeks before the calendar-year
# filers report Q3. Every matched-membership sum published 2026Q3 as its
# latest point anyway, with ONE member:
#
#     total panel   2026Q2 $614.18B  22 members   ->  2026Q3 $75.66B  1 member
#     hyperscaler   INSUFFICIENT-MEMBERSHIP (5 names reduced to ORCL)
#     cross-check   2026Q3 367.2%  ($277.8B of NVDA over ORCL alone)
#     composition   "2026Q3 AMZN, GOOGL, META, MSFT left" — four phantom exits
#     A3            demand frontier moved to 2026Q3, silencing NVDA's early read
#
# and the thesis line read "Panel capex TTM $75.66B is falling". It stood for
# nine days. It recurs every quarter ORCL files, since its quarters end in
# Aug, Nov, Feb and May.
#
# THE GATE. A trailing quarter joins the published series only once the members
# who have reported it account for at least `FRONTIER_COVERAGE_FLOOR` of the
# PRIOR quarter's dollars. Measured over the same prior quarter so growth cannot
# masquerade as coverage, and arrivals cannot lower it.
#
# Ruled by Mando 2026-09-21: the floor is COVERAGE_FLOOR, the 0.95 already
# ratified for choosing constant-membership windows, reused here. The live knee
# is sharp — every published quarter pair since 2025 covers 100%, the partial
# frontier covers 9.1% — so any floor from roughly 0.10 to 0.99 trims the same
# single quarter.
# (FRONTIER_COVERAGE_FLOOR is bound to COVERAGE_FLOOR where that is defined,
# below — an alias, deliberately never a second literal.)

# THE ESCAPE, so a real departure cannot freeze a frontier forever. Coverage
# also dips when a member genuinely LEAVES — measured, hyperscaler 2017Q1 at 78%
# was Amazon dropping out of the concept — and a gate with no end would hold
# that aggregate on its last complete quarter permanently.
#
# 90 days is not fitted. It is the latest regular deadline for any periodic
# report: Form 10-K for a non-accelerated filer is due 90 days after fiscal year
# end (10-Q deadlines are 40-45 days; large-accelerated 10-Ks, 60). Past it, a
# missing member is not "not yet filed" but behind on filing, and the quarter
# publishes without it, the missing names listed.
FRONTIER_MAX_WAIT_DAYS = 90


def member_ttm_at(series, q, window=WINDOW):
    """One member's trailing sum over the `window` quarters ending at `q`, or None."""
    i = _cq_index(q)
    win = [_cq_from_index(k) for k in range(i - window + 1, i + 1)]
    if not all(w in series for w in win):
        return None
    return sum(series[w] for w in win)


def frontier_split(membership, members, as_of=None,
                   floor=None, max_wait_days=FRONTIER_MAX_WAIT_DAYS, value_at=None):
    """({published quarters}, [partial rows]) for a matched-membership series.

    A quarter is PARTIAL while the members who reported it cover less than
    `floor` of the dollars of the last ACCEPTED quarter AND it is still inside
    `max_wait_days` of its calendar end.

    **Measured against the last accepted quarter, never the one before it.** The
    first cut walked back comparing each quarter with its immediate predecessor,
    and a test caught it: with ORCL alone at both 2026Q3 and 2026Q4, Q4 was
    judged against Q3 — itself partial — and ORCL covers 100% of ORCL, so the
    one-member tail published anyway. A held quarter can never be the reference
    that legitimises its successor.

    **Once one quarter is held, the whole tail is held**, so the published
    series is always a prefix and never has a hole in it.

    Only recent quarters can ever be held: anything more than `max_wait_days`
    past its end is accepted outright. That is what keeps a genuine historical
    departure — Amazon's 2017Q1 exit covered 78% of the prior quarter — in
    history, where it is already published as a composition change.
    """
    from datetime import date, timedelta
    floor = FRONTIER_COVERAGE_FLOOR if floor is None else floor
    as_of = as_of or date.today()
    # How much a member weighs at a quarter. Discrete quarterly flows (the
    # default) are summed over the trailing window; a caller whose maps are
    # already TTMs or stocks passes a direct lookup instead.
    if value_at is None:
        def value_at(t, q):
            return member_ttm_at(members.get(t) or {}, q)
    qs = sorted(membership, key=_cq_sort)
    if len(qs) < 2:
        return set(qs), []
    accepted, partial = [qs[0]], []
    for q in qs[1:]:
        ref = accepted[-1]
        base = {t: value_at(t, ref) for t in membership[ref]}
        base = {t: v for t, v in base.items() if v}
        denom = sum(base.values())
        reported = set(membership[q])
        num = sum(v for t, v in base.items() if t in reported)
        coverage = (num / denom) if denom > 0 else 1.0
        waited = (as_of - _quarter_end(q)).days
        if not partial and (coverage >= floor or waited >= max_wait_days):
            accepted.append(q)
            continue
        missing = sorted(t for t in base if t not in reported)
        partial.append({
            "q": q,
            "prior_q": ref,
            "members": sorted(reported),
            "member_count": len(reported),
            "prior_member_count": len(membership[ref]),
            "coverage": coverage,
            "missing": missing,
            "missing_share": {t: base[t] / denom for t in missing} if denom else {},
            "publishes_by": (_quarter_end(q)
                             + timedelta(days=max_wait_days)).isoformat(),
        })
    return set(accepted), partial


def membership_floor(membership, floor=None):
    """The membership-floor verdict for a matched sum's LATEST point.

    A separate guard from the frontier gate, deliberately. The gate is about
    TIMING: it holds a quarter not yet fully reported and releases it when the
    filings land. The floor is about IDENTITY: a one-member "sum" is that
    member's own number wearing an aggregate's label, however complete its
    reporting. So the floor REFUSES the figure rather than trimming it — a bucket
    genuinely reduced to one name says so, instead of freezing on its last good
    quarter.

    Latest point only, the same rule buckets have always used: history keeps its
    one-name early quarters, each labelled with its member count.
    """
    floor = MIN_BUCKET_MEMBERS if floor is None else floor
    if not membership:
        return {"status": STATE_INSUFFICIENT_MEMBERSHIP, "members": 0,
                "min_members": floor, "quarter": None}
    q = max(membership, key=_cq_sort)
    n = len(membership[q])
    return {"status": "OK" if n >= floor else STATE_INSUFFICIENT_MEMBERSHIP,
            "members": n, "min_members": floor, "quarter": q}


def _trim(values, membership, published):
    return ({q: v for q, v in values.items() if q in published},
            {q: m for q, m in membership.items() if q in published})


def matched_ttm_published(members, as_of=None):
    """`matched_ttm_series`, gated at the aggregate frontier.

    Returns (ttm, membership, partial). Every matched SUM the snapshot publishes
    goes through the gate; the raw function stays for callers that genuinely
    want every quarter any member reached.
    """
    ttm, membership = matched_ttm_series(members)
    published, partial = frontier_split(membership, members, as_of=as_of)
    for row in partial:
        row["ttm"] = ttm.get(row["q"])
    ttm, membership = _trim(ttm, membership, published)
    return ttm, membership, partial


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

# Ruled by Mando 2026-09-21: the aggregate-frontier gate reuses this floor.
# An alias rather than a copy, so the two can never drift apart.
FRONTIER_COVERAGE_FLOOR = COVERAGE_FLOOR


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
                              coverage_floor=COVERAGE_FLOOR, as_of=None):
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
    # The window may only END where the membership has actually reached.
    #
    # This used `quarters[-1]` — the newest quarter ANY member reached — and
    # every candidate window had to end there. On 2026-09-12 Oracle alone reached
    # calendar 2026Q3, so every window's common membership was ORCL, below the
    # two-member floor, and the panel came back empty: the front-page composite
    # read "no constant-membership panel" for nine days, and the hyperscaler
    # bucket's level line vanished with it. Same defect as the matched sums, in a
    # function that gate did not reach — found by rendering the Brief after the
    # first fix deployed.
    membership = {q: sorted(t for t, m in member_maps.items() if q in m) for q in quarters}
    published, _held = frontier_split(
        membership, member_maps, as_of=as_of,
        value_at=lambda t, q: abs(member_maps[t][q]) if q in member_maps[t] else None)
    quarters = [q for q in quarters if q in published]
    member_maps = {t: {q: v for q, v in m.items() if q in published}
                   for t, m in member_maps.items()}
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
    def __init__(self, bucket, yoy, membership, ttm, composition_events, partial=None):
        self.bucket = bucket
        self.yoy = yoy
        self.membership = membership
        self.ttm = ttm
        self.composition_events = composition_events
        # Trailing quarters held OUT of the series above because too few members
        # have reported them yet. Published beside it, never inside it.
        self.partial = partial or []

    def __repr__(self):
        return "BucketTrend({} n_quarters={})".format(self.bucket, len(self.yoy))


def bucket_trend(bucket, members, as_of=None):
    """Matched-membership bucket-sum YoY plus composition events.

    `members` is {ticker: {quarter: value}}. Gated at the aggregate frontier:
    see `frontier_split`. Trailing quarters too few members have reached are
    returned in `.partial` and are absent from `.yoy`, `.ttm`, `.membership`
    and the composition events, so the ladder never classifies them and no
    reader mistakes them for the aggregate's latest point.
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

    published, partial = frontier_split(membership, members, as_of=as_of)
    for row in partial:
        row["ttm"] = ttm.get(row["q"])
        row["yoy"] = yoy.get(row["q"])
    held = {row["q"] for row in partial}
    # The composition events at a partial quarter are not events. Every member
    # that has not reported YET reads as having LEFT — live, that published
    # "2026Q3 AMZN, GOOGL, META, MSFT left" when all four were simply calendar
    # filers who report Q3 in late October.
    events = [e for e in events if e[1] not in held]
    yoy = {q: v for q, v in yoy.items() if q in published}
    ttm, membership = _trim(ttm, membership, published)
    return BucketTrend(bucket, yoy, membership, ttm, events, partial)


def full_panel_trend(all_members, as_of=None):
    """Total panel across the aggregated buckets, same matched-membership rule."""
    return bucket_trend("total", all_members, as_of=as_of)


def build(roster, indexed_by_cik, include_leases=False, as_of=None):
    """Everything P3 owns, for one scan.

    `as_of` is the date the frontier gate measures its wait against. The
    snapshot passes the scan's own timestamp so a rebuild is reproducible;
    None means today.

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
        bt = bucket_trend(bucket, members, as_of=as_of)
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
    total = full_panel_trend(all_members, as_of=as_of)
    total_obs, total_state = [], phases.STATE_INSUFFICIENT
    # CD-FRONTIER-CLOSE F3. Buckets have always refused a one-member latest
    # point; the TOTAL never did, and that asymmetry is how a one-name "panel"
    # reached the front page on 2026-09-12. The same floor now applies to both.
    if membership_floor(total.membership)["status"] != "OK":
        total_state = STATE_INSUFFICIENT_MEMBERSHIP
    elif len(total.yoy) >= phases.N_CONFIRM + 1:
        total_obs = phases.classify(total.yoy, "total:panel", series_key="total:panel")
        cur = phases.current(total_obs)
        total_state = cur.state if cur else phases.STATE_INSUFFICIENT

    breadth_by_bucket = {}
    for bucket in AGGREGATED_BUCKETS:
        names = {t: s for t, s in issuer_states.items()
                 if t in bucket_members.get(bucket, {})}
        breadth_by_bucket[bucket] = phases.breadth(names)

    # --- View 0 companions: levels, credit, commitments, breadth history ---
    # The credit leg is a matched sum too, so it takes the same gate.
    issuance_ttm, issuance_membership, issuance_partial = matched_ttm_published(
        issuance_members, as_of=as_of)
    commitments_stock, commitments_membership = matched_stock_series(commitment_members)
    panel_breadth = breadth_series(issuer_obs, tickers=set(all_members))

    # Constant-membership panels — the only basis on which a LEVEL is plotted.
    const_capex = constant_membership_panel(
        {t: ttm_by_quarter(m) for t, m in all_members.items()}, as_of=as_of)
    const_issuance = constant_membership_panel(
        {t: ttm_by_quarter(m) for t, m in issuance_members.items()}, as_of=as_of)
    const_commitments = constant_membership_panel(commitment_members, as_of=as_of)

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
        cp = constant_membership_panel(shared, as_of=as_of)
        if cp:
            jaws_capex = cp
            jaws_issuance = ConstantPanel(
                cp.members, cp.quarters,
                {q: sum(ji[t][q] for t in cp.members) for q in cp.quarters},
                cp.coverage, [])
    const_buckets = {}
    for b, mem in bucket_members.items():
        cp = constant_membership_panel({t: ttm_by_quarter(m) for t, m in mem.items()},
                                       as_of=as_of)
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
        "issuance_partial": issuance_partial,
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
