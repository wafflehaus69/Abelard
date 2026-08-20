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

from . import commitments, divergence, phases, trend

SNAPSHOT_KEY = "panel_snapshot"


def _obs_json(o):
    return {"quarter": o.quarter, "yoy": o.yoy, "delta": o.delta,
            "direction": o.direction, "state": o.state, "flags": list(o.flags),
            "quarters_in_state": o.quarters_in_state, "entered": o.entered}


def build(roster, indexed_by_cik, now_unix=None):
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
        "observations": [_obs_json(o) for o in t["total_obs"]],
    }

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
