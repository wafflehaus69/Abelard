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

from . import commitments, config, divergence, normalize, phases, trend

SNAPSHOT_KEY = "panel_snapshot"


def _obs_json(o):
    return {"quarter": o.quarter, "yoy": o.yoy, "delta": o.delta,
            "direction": o.direction, "state": o.state, "flags": list(o.flags),
            "quarters_in_state": o.quarters_in_state, "entered": o.entered}


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
    return out


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

    return {
        "generated_unix": now_unix,
        "bands_measured_on": __import__("capex_daemon.config", fromlist=["x"]).DEAD_BAND_MEASURED_ON,
        "issuers": issuers,
        "buckets": buckets,
        "total": total,
        "panel": panel,
        "suppliers": _supplier_section(supplier_legs, t["bucket_trends"]),
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


def alert_lines(snap, prior_keys=()):
    """P5 alert bar — STATE TRANSITIONS ONLY, per standing rule.

    Three triggers, deliberately narrow:
      * any hyperscaler entering DECELERATING
      * any bucket-level or total-panel transition
      * any state change touching CRWV, flagged for the THESES intersection

    MIRROR names are excluded from alerts by standing rule; SNOW's CONTRACTING
    read stays visible on the board as the calibration ghost but never alerts.
    """
    out = []
    prior = set(prior_keys or ())
    for t in snap.get("transitions", []):
        if t["event_key"] in prior:
            continue
        key, to = t["series_key"], t["to_state"]
        issuer = snap["issuers"].get(key)
        if issuer and issuer["bucket"] == "mirror":
            continue
        why = None
        if issuer and issuer["bucket"] == "hyperscaler" and to == phases.STATE_DECELERATING:
            why = "hyperscaler entering DECELERATING"
        elif key.startswith("bucket:") or key == "total:panel":
            why = "aggregate transition"
        elif key == "CRWV":
            why = "CRWV state change — THESES intersection"
        if why:
            out.append({"series_key": key, "quarter": t["quarter"],
                        "from_state": t["from_state"], "to_state": to,
                        "reason": why, "event_key": t["event_key"]})
    return out
