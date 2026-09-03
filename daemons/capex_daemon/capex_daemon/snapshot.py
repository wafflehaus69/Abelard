"""The published view-model. One computation, many renderers.

The dashboard, the PDF section and the alert path all read THIS object and never
recompute. That is the rule the order states as "no parallel computation paths",
and it is the difference between a dashboard that agrees with the brief and one
that disagrees with it three months from now for reasons nobody can reconstruct.

The scan builds a snapshot and persists it as JSON in SQLite. Readers open the
DB read-only and render. Every published number carries its provenance —
resolved concept, derivation, source leg, coverage status — because a figure a
reader cannot audit is a figure they have to trust blindly.
"""
import json
import time

from . import commitments, config, disclosure, divergence, normalize, phases, trend

SNAPSHOT_KEY = "panel_snapshot"

# How far back from the panel's newest classified quarter a transition may still
# alert. One quarter of slack, because issuers file weeks apart and a name that
# transitions on the quarter just before the frontier is still current news.
ALERT_LOOKBACK_QUARTERS = 1


def _last_tagged(indexed, entity):
    """Newest period end the capex concepts carry, regardless of derivability.

    Distinguishes an annual-basis filer from one that stopped tagging. Reads the
    raw index rather than the derived series precisely because the derived
    series is empty in both cases.
    """
    from . import tagmap
    newest = None
    for c in tagmap.CANDIDATES[tagmap.CAPEX]:
        for f in indexed.get(c, []):
            if f.period_end and (newest is None or f.period_end > newest):
                newest = f.period_end
    return newest


def _obs_json(o):
    return {"quarter": o.quarter, "yoy": o.yoy, "delta": o.delta,
            "direction": o.direction, "state": o.state, "flags": list(o.flags),
            "quarters_in_state": o.quarters_in_state, "entered": o.entered}


# CD-BRIEF1 B6 (was GAP2 P1). How many quarters the frontier pair spans.
FRONTIER_PAIR_QUARTERS = 8


def _frontier_pair(capex_ttm, capex_membership, iss_ttm, iss_membership,
                   quarters=FRONTIER_PAIR_QUARTERS):
    """The jaws over FULL CURRENT membership for the trailing quarters. B6/P1.

    The constant-membership jaws are the honest LEVEL: five names held fixed
    across the whole window, so a rise is spending rather than arrivals. That
    correctness costs currency — the five are chosen for coverage across sixty
    quarters, so the newest and largest credit issuers are not in them.

    So a second, shorter pair on everyone who currently contributes. It is
    explicitly NOT a level to be compared with the long one: over eight quarters
    membership still changes, and every entry inside the window is published as
    a composition event so a step caused by an arrival cannot be read as
    spending. Two legs, both labelled, neither pretending to be the other.
    """
    qs = sorted(set(capex_ttm) & set(iss_ttm), key=trend._cq_sort)[-quarters:]
    if len(qs) < 2:
        return {}
    first = set(iss_membership.get(qs[0], []))
    entries = []
    for q in qs[1:]:
        now = set(iss_membership.get(q, []))
        for tick in sorted(now - first):
            entries.append({"q": q, "ticker": tick, "change": "entered"})
        first |= now
    return {
        "quarters": qs,
        "capex": [{"q": q, "value": capex_ttm[q],
                   "members": len(capex_membership.get(q, []))} for q in qs],
        "issuance": [{"q": q, "value": iss_ttm[q],
                      "members": len(iss_membership.get(q, []))} for q in qs],
        "composition_events": entries,
        "basis": ("full current membership over the trailing {} quarters — a "
                  "CURRENCY read, not a level. Entries inside the window are "
                  "published beside it.".format(len(qs))),
    }


def _supplier_section(legs, bucket_trends):
    """CD-3 — the supplier cross-check, published beside the panel, never in it.

    A supplier's datacenter revenue and a hyperscaler's capex are largely the
    same dollar seen from opposite sides of the invoice, so the two are related
    by a RATIO and never by a sum. The ratio is not a reconciliation and is not
    expected to reach 1.0; it is a corroboration, and what carries information
    is a sharp move in it.
    """
    out = {"legs": {}, "covered": [], "combined": {}, "crosscheck": {}}
    members = {}
    # dcrev transitions travel with the section so they can reach the alert
    # rule. Without this they were computed, rendered on page 17, and could
    # never fire — the one supplier state with thesis meaning was unalertable.
    dc_transitions = []
    for tick, leg in sorted((legs or {}).items()):
        ttm = trend.ttm_by_quarter(leg.quarters) if leg.quarters else {}
        tq = sorted(ttm, key=trend._cq_sort)
        out["legs"][tick] = {
            "ticker": tick, "status": leg.status, "detail": leg.detail,
            "axes": leg.axes, "concept": leg.concept, "instances": leg.instances,
            "quarters": [{"q": q, "value": leg.quarters[q]}
                         for q in sorted(leg.quarters, key=trend._cq_sort)],
            "ttm_series": [{"q": q, "value": ttm[q]} for q in tq],
            "ttm": ttm[tq[-1]] if tq else None,
            "latest_quarter": tq[-1] if tq else None,
            "restatements": leg.restatements[-8:],
            "restatement_count": len(leg.restatements),
            "dropped": len(leg.dropped),
            # The ruled mapping travels WITH the figure, everywhere it goes. A
            # mapped number that loses its provenance en route to a renderer is
            # a measurement it was never entitled to become.
            "mapping": leg.mapping,
            "is_mapped": leg.is_mapped,
            "partial_periods": len(leg.partial),
        }
        # The datacenter-revenue series gets its own phase state, against its
        # own ratified band (`dcrev:supplier`, CD-3b). It is NOT the same series
        # as the supplier's own capex, which the phase board classifies against
        # `issuer:supplier`, so it does not borrow that band.
        dc_yoy = trend.issuer_yoy(leg.quarters) if leg.quarters else {}
        dc_obs = []
        if len(dc_yoy) >= phases.N_CONFIRM + 1:
            dc_obs = phases.classify(dc_yoy, "dcrev:supplier",
                                     series_key="dcrev:{}".format(tick))
        dc_transitions += phases.transitions(dc_obs, "dcrev:{}".format(tick))
        cur = phases.current(dc_obs)
        out["legs"][tick].update({
            "dc_state": cur.state if cur else phases.STATE_INSUFFICIENT,
            "dc_flags": list(cur.flags) if cur else [],
            "dc_latest_yoy": cur.yoy if cur else None,
            "dc_latest_delta": cur.delta if cur else None,
            "dc_band": phases.band_for("dcrev:supplier"),
            "dc_band_measured_on": config.dead_band_measured_on("dcrev:supplier"),
            "dc_yoy_series": [{"q": q, "yoy": dc_yoy[q]}
                              for q in sorted(dc_yoy, key=trend._cq_sort)],
            "dc_observations": [_obs_json(o) for o in dc_obs],
        })
        if leg.is_covered:
            out["covered"].append(tick)
            members[tick] = leg.quarters

    if len(members) >= 1:
        ttm, membership = trend.matched_ttm_series(members)
        qs = sorted(ttm, key=trend._cq_sort)
        out["combined"] = {
            "members": sorted(members),
            "ttm_series": [{"q": q, "value": ttm[q], "members": len(membership[q])}
                           for q in qs],
            "ttm": ttm[qs[-1]] if qs else None,
            "latest_quarter": qs[-1] if qs else None,
        }
        hyper = bucket_trends.get("hyperscaler")
        if hyper and qs:
            shared = sorted(set(ttm) & set(hyper.ttm), key=trend._cq_sort)
            series = [{"q": q, "ratio": ttm[q] / hyper.ttm[q], "dc": ttm[q],
                       "capex": hyper.ttm[q],
                       "dc_members": len(membership.get(q, [])),
                       "capex_members": len(hyper.membership.get(q, []))}
                      for q in shared if hyper.ttm[q]]
            # A ratio whose DENOMINATOR lost a member is not comparable to the
            # quarter before it — the jump is arithmetic, not economic. Live:
            # 2026Q2 reads 52.8% against 44.1%, and Meta simply has not filed.
            warn = None
            if len(series) >= 2 and series[-1]["capex_members"] < series[-2]["capex_members"]:
                warn = ("capex denominator fell from {} to {} members at {} — the "
                        "move in the ratio is partly a membership change, not a "
                        "change in spending").format(
                            series[-2]["capex_members"], series[-1]["capex_members"],
                            series[-1]["q"])
            out["crosscheck"] = {
                "against": "hyperscaler", "series": series,
                "latest_ratio": series[-1]["ratio"] if series else None,
                "latest_quarter": series[-1]["q"] if series else None,
                "warning": warn,
            }
    out["frontier"] = _supplier_frontier(legs, bucket_trends.get("hyperscaler"))
    out["dc_transitions"] = dc_transitions
    return out


def _supplier_frontier(legs, hyper):
    """Supplier quarters the demand panel has not reached yet. CD-GAP2A A3.

    Four of five suppliers close off-calendar — NVDA in July, MU in August, SMCI
    in June — so they routinely file a quarter the hyperscalers will not report
    until late October. The cross-check correctly refuses a ratio there: there
    is no denominator yet. But the numerator exists, and it was being discarded.

    This is the daemon's earliest read. It is deliberately NOT a ratio, NOT a
    phase state and NOT aggregated with anything: it is one supplier's own
    discrete quarter against its own prior quarter and its own year-ago quarter,
    labelled as sitting ahead of the demand panel so it can never be mistaken
    for a panel figure.
    """
    if hyper is None or not getattr(hyper, "ttm", None):
        return {}
    demand = max(hyper.ttm, key=trend._cq_sort)
    rows = []
    for tick, leg in sorted((legs or {}).items()):
        qmap = leg.quarters or {}
        ahead = [q for q in qmap if trend._cq_sort(q) > trend._cq_sort(demand)]
        for q in sorted(ahead, key=trend._cq_sort):
            rows.append({
                "ticker": tick,
                "q": q,
                "value": qmap[q],
                "qoq": _growth(qmap.get(_shift(q, -1)), qmap[q]),
                "yoy": _growth(qmap.get(_shift(q, -4)), qmap[q]),
                "prior_q": _shift(q, -1),
                "year_ago_q": _shift(q, -4),
                "basis": "the supplier's own discrete quarter — not a TTM, not a "
                         "ratio, and not in any aggregate",
            })
    return {"demand_frontier": demand, "rows": rows,
            "quarters_ahead": sorted({r["q"] for r in rows}, key=trend._cq_sort)}


def _shift(cq, n):
    """Calendar quarter shifted by `n` quarters."""
    y, q = trend._cq_sort(cq)
    idx = y * 4 + q + n
    return "{}Q{}".format((idx - 1) // 4, (idx - 1) % 4 + 1)


def _growth(prior, latest):
    if prior is None or latest is None or prior <= 0:
        return None
    return latest / prior - 1.0


def build(roster, indexed_by_cik, now_unix=None, supplier_legs=None):
    """Assemble the whole published view-model for one scan."""
    now_unix = int(now_unix if now_unix is not None else time.time())
    t = trend.build(roster, indexed_by_cik)

    issuers = {}
    for cik, e in roster.items():
        indexed = indexed_by_cik.get(cik)
        if indexed is None:
            continue
        view = divergence.build_issuer_view(e, indexed)
        obs = t["issuer_obs"].get(e.ticker_display, [])
        cur = phases.current(obs)
        series = t["issuer_series"].get(e.ticker_display, {})
        yoy = trend.issuer_yoy(series) if series else {}
        comm = view.commitments
        issuers[e.ticker_display] = {
            "cik": e.cik,
            "ticker": e.ticker_display,
            "bucket": e.bucket,
            "notes": e.notes,
            "state": t["issuer_states"].get(e.ticker_display, phases.STATE_INSUFFICIENT),
            "flags": list(cur.flags) if cur else [],
            "quarters_in_state": cur.quarters_in_state if cur else 0,
            "entered": cur.entered if cur else None,
            "direction": cur.direction if cur else None,
            "latest_yoy": cur.yoy if cur else None,
            "latest_delta": cur.delta if cur else None,
            "band": phases.band_for("issuer:{}".format(e.bucket)),
            "ttm_capex": view.ttm_capex,
            "ttm_issuance": view.ttm_issuance,
            "credit_ratio": view.ratio,
            "coverage": list(view.statuses),
            "quarters": [{"q": q, "value": series[q]} for q in
                         sorted(series, key=trend._cq_sort)],
            # CD-GAP1 P1: why there is no state, and what IS known anyway. None
            # for a classified name — an explanation is only owed for a gap.
            "disclosure": disclosure.classify(
                e, series, coverage=view.statuses,
                state=t["issuer_states"].get(e.ticker_display),
                last_tagged=_last_tagged(indexed, e)),
            "yoy_series": [{"q": q, "yoy": yoy[q]} for q in
                           sorted(yoy, key=trend._cq_sort)],
            "observations": [_obs_json(o) for o in obs],
            "commitments": {
                "status": comm.status if comm else "ABSENT",
                "detail": comm.detail if comm else "",
                "latest": (comm.latest.value if comm and comm.latest else None),
                "concept": comm.concept if comm else None,
                "points": ([{"end": p.period_end, "value": p.value}
                            for p in comm.points[-12:]] if comm else []),
                # Calendar-keyed so a renderer can plot it beside anything else
                # on the panel's time axis without re-aligning it itself.
                "points_cq": ([{"q": normalize.calendar_align(p.period_end)[0],
                                "value": p.value} for p in comm.points]
                              if comm else []),
            },
        }

    buckets = {}
    for b, bt in t["bucket_trends"].items():
        qs = sorted(bt.yoy, key=trend._cq_sort)
        latest = qs[-1] if qs else None
        members = [issuers[m] for m in bt.membership.get(latest, []) if m in issuers]
        caps = [m["ttm_capex"] for m in members if m["ttm_capex"]]
        buckets[b] = {
            "bucket": b,
            "state": t["bucket_states"].get(b),
            "band": phases.band_for("bucketsum:{}".format(b)),
            "latest_quarter": latest,
            "latest_yoy": bt.yoy.get(latest) if latest else None,
            "ttm": bt.ttm.get(latest) if latest else None,
            "membership": bt.membership.get(latest, []),
            "member_count": len(bt.membership.get(latest, [])),
            "min_members": trend.MIN_BUCKET_MEMBERS,
            "top2_share": divergence.concentration(caps),
            "yoy_series": [{"q": q, "yoy": bt.yoy[q],
                            "members": bt.membership[q]} for q in qs],
            "ttm_series": [{"q": q, "ttm": bt.ttm[q],
                            "members": len(bt.membership.get(q, []))}
                           for q in sorted(bt.ttm, key=trend._cq_sort)],
            "composition_events": [
                {"quarter": q, "ticker": tk, "change": ch}
                for (_, q, tk, ch) in bt.composition_events[-12:]],
            "breadth": t["breadth"].get(b, {}),
            "observations": [_obs_json(o) for o in t["bucket_obs"].get(b, [])],
        }

    tt = t["total_trend"]
    tq = sorted(tt.yoy, key=trend._cq_sort)
    total = {
        "state": t["total_state"],
        "band": phases.band_for("total:panel"),
        "latest_quarter": tq[-1] if tq else None,
        "latest_yoy": tt.yoy.get(tq[-1]) if tq else None,
        "ttm": tt.ttm.get(tq[-1]) if tq else None,
        "member_count": len(tt.membership.get(tq[-1], [])) if tq else 0,
        "membership": tt.membership.get(tq[-1], []) if tq else [],
        "yoy_series": [{"q": q, "yoy": tt.yoy[q]} for q in tq],
        "ttm_series": [{"q": q, "ttm": tt.ttm[q],
                        "members": len(tt.membership.get(q, []))}
                       for q in sorted(tt.ttm, key=trend._cq_sort)],
        "observations": [_obs_json(o) for o in t["total_obs"]],
    }

    # --- View 0 companions. Published, not recomputed by any renderer. ---
    def _ser(vals, mem, key):
        return [{"q": q, key: vals[q], "members": len(mem.get(q, []))}
                for q in sorted(vals, key=trend._cq_sort)]

    panel = {
        "issuance_ttm": _ser(t["issuance_ttm"], t["issuance_membership"], "value"),
        "issuance_membership_latest": (
            t["issuance_membership"][max(t["issuance_membership"], key=trend._cq_sort)]
            if t["issuance_membership"] else []),
        "frontier_pair": _frontier_pair(
            t["total_trend"].ttm, t["total_trend"].membership,
            t["issuance_ttm"], t["issuance_membership"]),
        "commitments": _ser(t["commitments_stock"], t["commitments_membership"], "value"),
        "commitments_membership_latest": (
            t["commitments_membership"][max(t["commitments_membership"], key=trend._cq_sort)]
            if t["commitments_membership"] else []),
        "breadth_series": [dict(q=q, **t["panel_breadth_series"][q])
                           for q in sorted(t["panel_breadth_series"], key=trend._cq_sort)],
    }

    # Constant-membership levels. Matched membership makes a COMPARISON safe; a
    # plotted LEVEL needs its own guard, because the panel's matched membership
    # runs 1 -> 12 names across its 66 quarters and a level drawn over that shows
    # arrivals as growth. See trend.constant_membership_panel.
    def _const(cp):
        if not cp:
            return {"members": [], "series": [], "member_count": 0, "coverage": 0.0,
                    "lagging": []}
        return {"members": cp.members, "member_count": len(cp.members),
                "coverage": cp.coverage,
                "first_quarter": cp.quarters[0], "last_quarter": cp.quarters[-1],
                "series": [{"q": q, "value": cp.level[q]} for q in cp.quarters],
                "lagging": [{"ticker": t, "last_quarter": q, "last_value": v}
                            for t, q, v in cp.lagging]}

    panel["constant"] = {
        "capex": _const(t["const_capex"]),
        "issuance": _const(t["const_issuance"]),
        "commitments": _const(t["const_commitments"]),
        "buckets": {b: _const(d) for b, d in t["const_buckets"].items()},
        # Same names, same window, both legs — see trend.build.
        "jaws_capex": _const(t["jaws_capex"]),
        "jaws_issuance": _const(t["jaws_issuance"]),
    }

    # The forward-commitment leg is REFUSED as a panel aggregate, not omitted by
    # accident. Disclosure is event-driven and ragged — Oracle has 13 gaps in 16
    # observations — so no constant-membership window exists at any tested
    # setting, and the varying-membership sum is visibly an artifact: it falls
    # from $255.5B to $45.2B between 2026Q1 and 2026Q2 solely because Meta's
    # $237.7B stops being disclosed. A faint area drawn over that would put an
    # 82% collapse on the front page that never happened.
    if not panel["constant"]["commitments"]["members"]:
        cs = panel.get("commitments") or []
        worst = None
        for a, b in zip(cs, cs[1:]):
            drop = (a["value"] - b["value"]) / a["value"] if a["value"] else 0.0
            if drop > 0.5 and (worst is None or drop > worst[0]):
                worst = (drop, a, b)
        panel["commitments_panel"] = {
            "status": "REFUSED-NO-CONSTANT-MEMBERSHIP",
            "detail": ("no window of {}+ quarters has constant membership; the "
                       "varying-membership sum is a disclosure artifact"
                       .format(trend.MIN_PANEL_QUARTERS)
                       + ("" if not worst else
                          " — {} ${:,.1f}B to {} ${:,.1f}B ({:.0f}% of it a "
                          "membership change from {} to {} issuers)".format(
                              worst[1]["q"], worst[1]["value"] / 1e9,
                              worst[2]["q"], worst[2]["value"] / 1e9,
                              100 * worst[0], worst[1]["members"],
                              worst[2]["members"]))),
            "disclosing_issuers": (cs[-1]["members"] if cs else 0),
        }
    else:
        panel["commitments_panel"] = {"status": "OK", "detail": "", "disclosing_issuers": 0}

    # B5 — the MIXED-BASIS refusal, which outranks the membership one because it
    # holds even when membership is perfectly constant.
    #
    # `ContractualObligation`, `PurchaseObligation` and
    # `UnrecordedUnconditionalPurchaseObligationBalanceSheetAmount` are three
    # different measures with three different scopes. Adding them produces a
    # number with no defined meaning, and it has been on the front page as
    # though it had one. Per-issuer figures are untouched: each is internally
    # consistent and comparable to its own history, which is exactly why the A4
    # deltas are computed per issuer and never summed.
    #
    # Held until GAP2 P2 assigns basis classes. Once assigned, a total may be
    # published WITHIN one class and still never across them.
    bases = {}
    for tick, iss in (issuers or {}).items():
        c = (iss.get("commitments") or {})
        if c.get("status") == commitments.STATUS_COVERED and c.get("concept"):
            bases.setdefault(c["concept"], []).append(tick)
    if len(bases) > 1:
        panel["commitments_panel"] = {
            "status": "REFUSED-MIXED-BASIS",
            "detail": ("the {} disclosing issuers use {} different concepts, which "
                       "are not the same measure: {}. Summing them yields a number "
                       "with no defined meaning. Per-issuer figures and deltas are "
                       "unaffected — what fails is ADDING them. Held until basis "
                       "classes are assigned (GAP2 P2)".format(
                           sum(len(v) for v in bases.values()), len(bases),
                           "; ".join("{} ({})".format(k, ", ".join(sorted(v)))
                                     for k, v in sorted(bases.items())))),
            "disclosing_issuers": sum(len(v) for v in bases.values()),
            "basis_classes": {k: sorted(v) for k, v in sorted(bases.items())},
        }

    # The divergence chart plots a RATIO, so the ratio is published rather than
    # divided in a renderer. Two views dividing the same pair independently is
    # exactly how a dashboard starts disagreeing with the brief.
    tt_ttm, iss_ttm = tt.ttm, t["issuance_ttm"]
    panel["credit_ratio_series"] = [
        {"q": q, "ratio": iss_ttm[q] / tt_ttm[q],
         "members": len(t["issuance_membership"].get(q, []))}
        for q in sorted(set(tt_ttm) & set(iss_ttm), key=trend._cq_sort)
        if tt_ttm[q]]

    all_trans = []
    for tick, obs in t["issuer_obs"].items():
        all_trans += phases.transitions(obs, tick)
    for b, obs in t["bucket_obs"].items():
        all_trans += phases.transitions(obs, "bucket:{}".format(b))
    all_trans += phases.transitions(t["total_obs"], "total:panel")

    suppliers = _supplier_section(supplier_legs, t["bucket_trends"])
    # A supplier's DATACENTER REVENUE phase is the one with thesis meaning, so
    # it joins the transition record on the same footing as an issuer's capex.
    all_trans += suppliers.pop("dc_transitions", [])

    return {
        "generated_unix": now_unix,
        "bands_measured_on": __import__("capex_daemon.config", fromlist=["x"]).DEAD_BAND_MEASURED_ON,
        "issuers": issuers,
        "buckets": buckets,
        "total": total,
        "panel": panel,
        "suppliers": suppliers,
        "transitions": [{"series_key": x.series_key, "quarter": x.quarter,
                         "from_state": x.from_state, "to_state": x.to_state,
                         "yoy": x.yoy, "delta": x.delta, "event_key": x.event_key}
                        for x in all_trans],
    }


def save(con, snap):
    con.execute("INSERT OR REPLACE INTO meta_kv(key, value) VALUES (?,?)",
                (SNAPSHOT_KEY, json.dumps(snap)))
    con.commit()
    return snap


def load(con):
    row = con.execute("SELECT value FROM meta_kv WHERE key=?", (SNAPSHOT_KEY,)).fetchone()
    return json.loads(row[0]) if row else None


# CD-GAP2A A4 — the alert gate for a commitment-stock jump. BOTH UNSET.
#
# E8: no threshold ships without a measured distribution behind it, and an unset
# constant is None whose consumer must surface that rather than substitute a
# default. Deltas are PUBLISHED; nothing ALERTS until Mando ratifies a pair.
#
# Measured 2026-09-02 over 308 observation-to-observation pairs across 21
# disclosing issuers:
#
#     p50 1.000x   p75 1.210x   p90 2.000x   p95 3.203x   max 2372x
#
# The tail is dominated by near-zero bases, so a bare multiple is a bad gate: at
# the measured p95 of 3.203x it fires 15 times and SIX of those move less than
# $1B — WULF $0.000B->$0.118B reads 846x, CLSK $0.007B->$0.041B reads 6.54x.
# Neither is a forward-demand event; both are a small denominator.
#
# Hence a PAIR, proposed and held: multiple >= 2.0x (the measured p90) AND an
# absolute move >= $1B. That fires on 16 of 308 pairs (5.2%), and every one is
# materially large — SMCI's citing case at 3.39x/+$24.10B, META +$53.24B,
# AVGO +$128.06B, AMD +$13.54B.
#
# **That pair alone is still wrong, and deploying it is what showed why.** A
# large base makes the multiple small however enormous the absolute move. META's
# four largest increases:
#
#     2025Q2->Q3  +$53.24B  2.90x   caught
#     2025Q3->Q4  +$49.86B  1.61x   MISSED
#     2025Q4->Q1  +$106.62B 1.81x   MISSED
#     2026Q1->Q2  +$111.64B 1.47x   MISSED
#
# META went $27.95B -> $349.31B in four quarters — a $321B forward-demand build,
# the largest on the panel — and a multiple-only gate sees one step of four. So a
# second, independent arm on the absolute move. Measured over the 147 increases:
# p50 $0.41B, p90 $7.77B, p95 $16.45B, p97.5 $49.86B, max $128.06B.
#
# Proposed and held:
#
#     (multiple >= 2.0x AND move >= $1B)  OR  (move >= $20B)
#
# 19 of 308 pairs (6.2%). $20B sits just above the measured p95 of increases. The
# arms are complementary by construction: the first catches a small issuer
# tripling, the second catches a large one adding more than most issuers hold.
#
# Concentration, for Mando to weigh: META 5, SMCI 5, AMD 2 of the 19. Adding the
# absolute arm improved this — under the multiple alone SMCI was 5 of 16, a third
# of the channel.
# RATIFIED by Mando, ORDER CD-BRIEF1 B4, 2026-09-02.
COMMITMENT_JUMP_MULTIPLE = 2.0
COMMITMENT_JUMP_MIN_DELTA = 1_000_000_000.0
COMMITMENT_JUMP_ABSOLUTE = 20_000_000_000.0

# Above this multiple, a move is QUARANTINED as BASIS-SUSPECT rather than
# alerted: published, listed separately, and queued for a presentation check.
#
# A 2372x move is not a growth rate until someone has confirmed the two
# observations measure the same thing. The failure it guards against is E23 in
# its sharpest form — a concept whose scope changed between filings produces an
# enormous "increase" that is really a change of subject, and an alert on that
# is worse than silence because it is confidently wrong.
#
# Quarantine is a QUEUE, not a verdict. The first case cleared — see
# `COMMITMENT_BASIS_CHECKS`.
COMMITMENT_BASIS_SUSPECT_MULTIPLE = 10.0

# Presentation checks performed, per E23. Keyed (ticker, to_quarter).
# `verdict` is REAL (the disclosure is unchanged and the move is genuine) or
# BASIS-CHANGE (the concept's scope moved and the two points are not comparable).
COMMITMENT_BASIS_CHECKS = {
    ("AVGO", "2026Q2"): {
        "verdict": "REAL",
        "checked": "2026-09-02",
        "evidence": (
            "Presentation compared across the two 10-Qs. Both carry the same "
            "note 10 'Commitments and Contingencies' table, the same line item "
            "'Purchase Commitments', the same fiscal-year rows and the same "
            "units. Filed 2026-03-11 (period 2026-02-01): 2026(rem) $28M, 2027 "
            "$12M, 2028 $10M, 2029 $4M. Filed 2026-06-09 (period 2026-05-03): "
            "2026(rem) $22M, 2027 $55,214M, 2028 $72,870M, 2029 $4M. The "
            "concept, the table and the presentation are IDENTICAL; what "
            "changed is the obligation. The near-term rows carry the whole "
            "move while 2029 and later are unchanged, which is what a real "
            "multi-year supply commitment looks like. Corroborated by "
            "RevenueRemainingPerformanceObligation in the same filing: "
            "$45.0B -> $164.6B."),
        "note": ("The quarantine did its job by forcing the check, and the "
                 "check cleared it. Suppressing this as a tagging artefact "
                 "would have hidden the largest single forward-demand event "
                 "on the panel."),
    },
}


def commitment_deltas(snap, frontier=None):
    """Observation-to-observation change in each issuer's forward-commitment stock.

    SMCI is the citing case: $10.10B to $34.20B between two snapshots — a server
    assembler committing to 3.4x the components — and nothing said so. That is
    precisely the forward-demand event the commitments leg exists to catch, and
    it was visible only by diffing two PDFs by hand.

    **Same basis by construction.** Each delta is one issuer against its own
    previous observation on its own concept, never across issuers. The
    cross-issuer comparability problem is real — `ContractualObligation`,
    `PurchaseObligation` and `UnrecordedUnconditionalPurchaseObligation...` are
    not the same measure — but it does not arise here, because nothing is
    compared across issuers and no delta is summed into anything.

    **A stock disclosed on the issuer's own schedule.** Consecutive observations
    are not consecutive quarters, so the gap is published with the delta: a 3.4x
    move over two quarters and over eight are different facts.
    """
    out = []
    for tick, iss in sorted((snap.get("issuers") or {}).items()):
        c = iss.get("commitments") or {}
        pts = c.get("points_cq") or []
        if len(pts) < 2 or not c.get("concept"):
            continue
        prev, cur = pts[-2], pts[-1]
        if frontier and trend._cq_sort(cur["q"]) < trend._cq_sort(frontier):
            continue
        base, latest = prev.get("value"), cur.get("value")
        if base is None or latest is None:
            continue
        gap = trend._cq_index(cur["q"]) - trend._cq_index(prev["q"])
        out.append({
            "ticker": tick,
            "bucket": iss.get("bucket"),
            "concept": c.get("concept"),
            "from_q": prev["q"], "to_q": cur["q"],
            "quarters_between": gap,
            "from_value": base, "to_value": latest,
            "delta": latest - base,
            "multiple": (latest / base) if base > 0 else None,
            "event_key": "commit:{}:{}:{}:{:.0f}".format(
                tick, prev["q"], cur["q"], latest),
        })
    return out


def commitment_alert_lines(snap, prior_keys=(), multiple=None, min_delta=None,
                           absolute=None):
    """Commitment jumps worth announcing. Empty while every bound is UNSET.

    E8 in its literal form: the consumer of an unset constant surfaces the
    absence rather than substituting a default. A threshold picked to make SMCI
    fire would be one fitted to a single observation, so the bounds stay None
    until ratified and this returns nothing.

    **Two independent arms, because one measure cannot see both shapes.**

      * `multiple` AND `min_delta` together — a small issuer tripling. Neither
        works alone: a multiple by itself fires on a near-zero base (WULF
        $0.000B -> $0.118B reads 846x), and a floor by itself fires on routine
        drift at a large issuer.
      * `absolute` — a large issuer adding more than most issuers hold. This arm
        exists because META added $111.64B in one quarter at 1.47x, which no
        defensible multiple would ever catch.

    An arm that is not fully armed simply does not fire; the other still can.
    """
    multiple = COMMITMENT_JUMP_MULTIPLE if multiple is None else multiple
    min_delta = COMMITMENT_JUMP_MIN_DELTA if min_delta is None else min_delta
    absolute = COMMITMENT_JUMP_ABSOLUTE if absolute is None else absolute
    rel_armed = multiple is not None and min_delta is not None
    if not rel_armed and absolute is None:
        return []
    prior = set(prior_keys or ())
    out = []
    for d in commitment_deltas(snap, frontier=_frontier_quarter(snap)):
        if d["event_key"] in prior:
            continue
        by_multiple = (rel_armed and d["multiple"] is not None
                       and d["multiple"] >= multiple and d["delta"] >= min_delta)
        by_size = absolute is not None and d["delta"] >= absolute
        if not (by_multiple or by_size):
            continue
        row = dict(d, reason="forward-commitment stock {} ({:+,.1f}B) {} -> {}"
                   .format("{:.2f}x".format(d["multiple"]) if d["multiple"]
                           else "from a zero base",
                           d["delta"] / 1e9, d["from_q"], d["to_q"]),
                   armed_by="multiple" if by_multiple else "absolute")
        row.update(basis_status(d))
        out.append(row)
    return out


BASIS_OK = "OK"
BASIS_SUSPECT = "BASIS-SUSPECT"
BASIS_VERIFIED_REAL = "BASIS-VERIFIED-REAL"
BASIS_VERIFIED_CHANGE = "BASIS-VERIFIED-CHANGE"


def basis_status(delta, threshold=None):
    """Is this move large enough that its BASIS must be checked before it alerts?

    Returns the status plus, where a check has been done, its verdict and
    evidence. A move above the threshold with no recorded check is quarantined:
    it publishes, it is listed separately, and it does not alert.
    """
    threshold = COMMITMENT_BASIS_SUSPECT_MULTIPLE if threshold is None else threshold
    m = delta.get("multiple")
    check = COMMITMENT_BASIS_CHECKS.get((delta.get("ticker"), delta.get("to_q")))
    if check:
        verdict = (BASIS_VERIFIED_REAL if check["verdict"] == "REAL"
                   else BASIS_VERIFIED_CHANGE)
        return {"basis": verdict, "basis_checked": check["checked"],
                "basis_evidence": check["evidence"],
                "alertable": check["verdict"] == "REAL"}
    if threshold is not None and m is not None and m >= threshold:
        return {"basis": BASIS_SUSPECT, "basis_checked": None,
                "basis_evidence": None, "alertable": False}
    return {"basis": BASIS_OK, "basis_checked": None, "basis_evidence": None,
            "alertable": True}


def commitment_alerts_and_quarantine(snap, prior_keys=()):
    """(alertable, quarantined) — the split B1's page one prints separately."""
    rows = commitment_alert_lines(snap, prior_keys=prior_keys)
    return ([r for r in rows if r.get("alertable", True)],
            [r for r in rows if not r.get("alertable", True)])


def _frontier_quarter(snap, lookback=ALERT_LOOKBACK_QUARTERS):
    """The oldest quarter a transition may alert from.

    Anchored on the newest quarter any classified series reached, so a panel
    mid-filing-season does not go silent just because one issuer is ahead.
    """
    qs = [o["quarter"] for i in (snap.get("issuers") or {}).values()
          for o in (i.get("observations") or [])]
    qs += [o["quarter"] for b in (snap.get("buckets") or {}).values()
           for o in (b.get("observations") or [])]
    qs += [o["quarter"] for o in ((snap.get("total") or {}).get("observations") or [])]
    if not qs:
        return None
    y, n = trend._cq_sort(max(qs, key=trend._cq_sort))
    idx = y * 4 + n - lookback
    return "{}Q{}".format((idx - 1) // 4, (idx - 1) % 4 + 1)


def alert_lines(snap, prior_keys=()):
    """P5 alert bar — STATE TRANSITIONS ONLY, per standing rule.

    Three triggers, deliberately narrow:
      * any hyperscaler entering DECELERATING
      * any bucket-level or total-panel transition
      * any state change touching CRWV, flagged for the THESES intersection

    MIRROR names are excluded from alerts by standing rule; SNOW's CONTRACTING
    read stays visible on the board as the calibration ghost but never alerts.

    **Only transitions at the FRONTIER alert.** An event key is content-derived,
    so changing the classifier mints new keys for OLD quarters and every one of
    them looks unseen. Measured: the CONTRACTING-exit fix made a rebuild alert
    `bucket:builder 2013Q3 CONTRACTING->PLATEAU` — a state change from thirteen
    years ago, announced as news in 2026. The first-run rule does not catch this
    because `phase_events` is not empty; only the interpretation changed.

    A transition is news when it happened in the quarter the panel has just
    reached. Anything earlier is history, however freshly derived — it is still
    RECORDED, so it cannot alert later, but it is never announced.
    """
    out = []
    prior = set(prior_keys or ())
    frontier = _frontier_quarter(snap)
    for t in snap.get("transitions", []):
        if t["event_key"] in prior:
            continue
        if frontier and trend._cq_sort(t["quarter"]) < trend._cq_sort(frontier):
            continue
        key, to = t["series_key"], t["to_state"]
        issuer = snap["issuers"].get(key)
        if issuer and issuer["bucket"] == "mirror":
            continue
        why = None
        if issuer and issuer["bucket"] == "hyperscaler" and to == phases.STATE_DECELERATING:
            why = "hyperscaler entering DECELERATING"
        elif key.startswith("dcrev:") and to == phases.STATE_DECELERATING:
            # The supplier analog of the hyperscaler rule, and it is keyed on
            # DATACENTER REVENUE rather than on the supplier's own capex —
            # deliberately, because those are different claims. NVDA's capex is
            # a ~$7B series covering its own facilities; a DECELERATING on it
            # says nothing about the buildout, and was once logged as though it
            # did. Its datacenter revenue is the other side of the
            # hyperscalers' invoice and is what a bend would be about.
            why = "supplier datacenter revenue entering DECELERATING"
        elif key.startswith("bucket:") or key == "total:panel":
            why = "aggregate transition"
        elif key == "CRWV":
            why = "CRWV state change — THESES intersection"
        if why:
            out.append({"series_key": key, "quarter": t["quarter"],
                        "from_state": t["from_state"], "to_state": to,
                        "reason": why, "event_key": t["event_key"]})
    return out
