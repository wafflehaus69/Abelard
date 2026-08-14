"""Read-only JSON export for the dossier dashboard — M10-Dash §4.1.

Aggregates the store into the shape the dashboard renders. Read-only: opens the store
in SQLite read-only URI mode, makes no network call, and never writes back.

Every resolution/actor-count read goes through :mod:`consensus.resolution` (the
chokepoint). The dashboard is the FIFTH consumer of "unresolved", and the previous four
each re-derived it and one of them failed open; nothing here re-implements it.

The power projection (Panel B) is the honest answer to "is this worth continuing?", so
it is computed from the SAME effective-n reasoning M0-B used, not a friendlier one:
significance rides on independent market BLOCKS (correlated footprints inside one market
count once — the M0-C "22 signals = one May event" failure), so the projection counts
distinct RESOLVED markets, never raw dossier rows.
"""

from __future__ import annotations

import datetime as dt
import json
import sqlite3
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

from . import resolution as _res

#: MDE = k * sigma / sqrt(n_eff); one-sided alpha 0.05, power 0.80 => k = 2.486.
#: sigma = 0.50 is the conservative Bernoulli ceiling (M0-B §4.2).
_K = 2.486
_SIGMA = 0.50

#: Decision anchors carried verbatim from M0-B: a follower entering after the price has
#: absorbed the footprint cannot plausibly retain more than 10pp, and ~5pp is the floor
#: that survives round-trip friction.
PLAUSIBLE_CEILING = 0.10
TRADEABLE_FLOOR = 0.05


def blocks_for_mde(mde: float) -> int:
    """Independent market-blocks needed to detect an effect of size ``mde``."""
    return int(round((_K * _SIGMA / mde) ** 2))


def _rows(con: sqlite3.Connection) -> list[dict[str, Any]]:
    cur = con.execute("SELECT * FROM dossiers")
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, r)) for r in cur.fetchall()]


def _price_band(vwap: Any) -> str:
    try:
        v = float(vwap)
    except (TypeError, ValueError):
        return "unknown"
    if v > 0.90:
        return "favorite>0.90"      # the carry/yield-harvest band, NOT signal
    if v < 0.10:
        return "longshot<0.10"
    return "contested"


def _notional_bucket(n: Any) -> str:
    try:
        v = float(n)
    except (TypeError, ValueError):
        return "unknown"
    for edge, name in ((1e4, "<10k"), (5e4, "10-50k"), (2.5e5, "50-250k"), (1e6, "250k-1M")):
        if v < edge:
            return name
    return ">1M"


def _freshness(row: dict[str, Any]) -> str:
    fs, det = row.get("first_seen_ts"), row.get("detection_ts")
    if fs is None or det is None:
        return "unresolved"          # chokepoint discipline: not "established"
    age = (det - fs) / 86400
    return "fresh<=7d" if age <= 7 else ("recent<=30d" if age <= 30 else "established")


def build_export(con: sqlite3.Connection, *, now_ts: int) -> dict[str, Any]:
    rows = _rows(con)
    resolved = [r for r in rows if r.get("resolved")]

    # --- Panel B: the compounding dataset, in BLOCKS (the binding unit) -------------
    by_day: dict[str, set[str]] = defaultdict(set)
    for r in resolved:
        ts = r.get("resolution_ts") or r.get("last_scan_ts")
        if ts:
            day = dt.datetime.fromtimestamp(int(ts), dt.timezone.utc).strftime("%Y-%m-%d")
            by_day[day].add(r["condition_id"])
    # cumulative resolved-dossier count per day, computed once rather than re-scanned
    n_by_day: dict[str, int] = {}
    running = 0
    for day in sorted(by_day):
        running += sum(1 for r in resolved
                       if r.get("resolution_ts") and dt.datetime.fromtimestamp(
                           int(r["resolution_ts"]), dt.timezone.utc).strftime("%Y-%m-%d") == day)
        n_by_day[day] = running
    cumulative, seen = [], set()
    for day in sorted(by_day):
        seen |= by_day[day]
        cumulative.append({"date": day, "blocks": len(seen), "dossiers": n_by_day[day]})
    blocks_now = len({r["condition_id"] for r in resolved})
    span_days = max(1, len(by_day))
    rate = blocks_now / span_days if span_days else 0.0

    def eta(target: int) -> dict[str, Any]:
        need = max(0, target - blocks_now)
        days = (need / rate) if rate > 0 else None
        return {"target_blocks": target, "blocks_remaining": need,
                "days_remaining": round(days) if days is not None else None,
                "reachable": days is not None}

    # --- Panel C: restraint ---------------------------------------------------------
    untierable = [r for r in rows if not _res.is_complete_score(r.get("tier"))]
    refused = sum(1 for r in untierable if r.get("composite_peak") is None)

    # --- Panel D: texture -----------------------------------------------------------
    bands = Counter(_price_band(r.get("entry_vwap")) for r in rows)
    tex = {
        "by_category": dict(Counter((r.get("market_category") or "unknown") for r in rows)),
        "by_price_band": dict(bands),
        "by_notional": dict(Counter(_notional_bucket(r.get("headline_notional")) for r in rows)),
        "by_freshness": dict(Counter(_freshness(r) for r in rows)),
        "by_tier_peak": dict(Counter((r.get("tier_peak") or "NONE") for r in rows)),
        # Reported SEPARATELY so the contested signal is never drowned by the carry trade.
        "favorite_harvest_share": round(
            bands.get("favorite>0.90", 0) / len(rows), 4) if rows else 0.0,
    }

    # --- Panel A: recent artifacts, uncertainty attached -----------------------------
    recent = sorted(rows, key=lambda r: (r.get("detection_ts") or 0), reverse=True)[:60]
    cards = []
    for r in recent:
        wallets = _res_wallets(r)
        cards.append({
            "dossier_id": r["dossier_id"],
            "market": r.get("market_question"),
            "category": r.get("market_category"),
            "tier": r.get("tier"), "tier_peak": r.get("tier_peak"),
            "composite": r.get("composite"),
            # §4.4: contested is the informed slice; headline carries the carry-trade
            # confound. Never collapsed into one number.
            "contested_notional": r.get("contested_notional"),
            "headline_notional": r.get("headline_notional"),
            "factors": {k: r.get(f"{k.lower()}_factor") for k in ("F", "S", "D", "C")},
            "entry_vwap": r.get("entry_vwap"),
            "detection_ts": r.get("detection_ts"),
            "raw_wallets": _res.raw_wallet_count(wallets),
            "actor_count": _res.actor_count(r),           # None = UNRESOLVED
            "collapse_state": _res.collapse_state(r, wallets),
            "resolved": bool(r.get("resolved")),
            "outcome_for_side": r.get("outcome_for_side"),
            "score_complete": _res.is_complete_score(r.get("tier")),
        })

    return {
        "generated_ts": now_ts,
        "totals": {
            "dossiers": len(rows),
            "resolved_dossiers": len(resolved),
            "resolved_blocks": blocks_now,
            "alerted": sum(1 for r in rows if r.get("alerted_ts")),
            "untierable": len(untierable),
            "false_positives_refused": refused,
        },
        "accumulation": {
            "series": cumulative,
            "blocks_per_day": round(rate, 3),
            "observed_days": span_days,
            "milestones": {
                "ceiling_0.10": {**eta(blocks_for_mde(PLAUSIBLE_CEILING)),
                                 "mde": PLAUSIBLE_CEILING},
                "tradeable_0.05": {**eta(blocks_for_mde(TRADEABLE_FLOOR)),
                                   "mde": TRADEABLE_FLOOR},
            },
            "assumption": (
                "Projection assumes the observed resolved-BLOCK rate continues. Blocks, "
                "not dossier rows, are the unit: correlated footprints inside one market "
                "count once (the M0-C failure). MDE = 2.486 x 0.50 / sqrt(blocks)."
            ),
        },
        "restraint": {
            "alert_bar": 0.80,
            "calibrated_rate_per_week": 0.5,
            "cluster_arm": "closed",
            "cluster_arm_reason": (
                "Coordination alerting is deliberately disabled. A funding mesh collapses "
                "a cluster toward ONE actor, so coordination DEFLATES evidence rather than "
                "amplifying it, and the measured data had no resolvable clusters to "
                "calibrate against."
            ),
        },
        "texture": tex,
        "recent": cards,
    }


def _res_wallets(row: dict[str, Any]) -> list[Any]:
    cw = row.get("cluster_wallets")
    try:
        return json.loads(cw) if cw else [row.get("wallet")]
    except Exception:
        return [row.get("wallet")]


def write_html(data: dict[str, Any], out_path: str) -> str:
    """Write the SELF-CONTAINED, PRE-RENDERED dashboard.

    v1.18: the page is built as static markup by :mod:`consensus.dossier_html` and needs
    NO JavaScript to show its data. The previous version rendered client-side from an
    inlined payload, which worked in a browser and showed the owner four empty panels,
    because the surface the file is actually viewed through renders HTML without running
    scripts: the static prose appeared and every JS-populated panel was blank. A page
    that needs JS also cannot fail loud when JS is the thing missing.
    """
    from . import dossier_html
    page = dossier_html.render_page(data)
    Path(out_path).parent.mkdir(parents=True, exist_ok=True)
    Path(out_path).write_text(page, encoding="utf-8")
    return out_path


def write_export(db_path: str, out_path: str, *, now_ts: int) -> dict[str, Any]:
    con = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    try:
        data = build_export(con, now_ts=now_ts)
    finally:
        con.close()
    Path(out_path).parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as fh:
        json.dump(data, fh, indent=1)
    if out_path.endswith(".json"):
        stem = out_path[:-5]
        # A JS sibling for the in-place template (a file:// page cannot fetch() a local
        # .json), and the SELF-CONTAINED page, which is the artifact a human opens or
        # forwards. The sibling form renders empty the moment the page is moved away
        # from its directory, so the inlined one is the real deliverable.
        with open(stem + ".js", "w", encoding="utf-8") as fh:
            fh.write("window.DOSSIER_EXPORT = ")
            json.dump(data, fh, indent=1)
            fh.write(";\n")
        write_html(data, stem + "_dashboard.html")
    return data
