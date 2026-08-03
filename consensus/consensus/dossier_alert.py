"""Dossier alerting — M10-D §3.5.

An alert is a POINTER to a dossier, never a recommendation. Nothing here computes or
implies expected value, and no alert text may suggest an action (§4.1).

QUIET-WEEK DISCIPLINE (v1.7 §1.2, §3.5). AUTHORITATIVE calibration 2026-07-31, on the
fill-factor-only composite after v1.16 §1 stopped multiplying latency in. Etherscan key
live; 14 daily replayed scans on the Basilic live tape, alerting once per dossier:

    composite >= 0.70 (the CRITICAL tier)   1.5/week
    composite >= 0.75                       1.0/week
    composite >= 0.80                       0.5/week   <- adopted: highest bar that still speaks
    composite >= 0.85                       0.0/week   <- silent

ALERT_COMPOSITE_MIN defaults to 0.80, DELIBERATELY SEPARATE from the CRITICAL tier
(0.70): the tier ranks dossiers for browsing ("worth a look"), the bar decides what is
worth interrupting a human for.

Two superseded calibrations, kept because a threshold nobody can re-derive is a magic
number: (1) 0.90 @ 0.5/wk, measured before the F-imputation fix — those composites were
inflated and ~83% of CRITICAL firings were artifacts; (2) a run where the latency
elevator multiplied into the composite, which broke the [0,1] scale (values to 1.27) and
made every bar in [0.75, 0.90] return an identical, inert 2.0/wk. The scale invariant is
now restored (max observed 0.841) and each threshold step changes the rate.

CLUSTER ARM: HELD (v1.16 §2.2) — see AlertRule.cluster_arm_enabled.
"""

from __future__ import annotations

import sqlite3
import time
from dataclasses import dataclass, field
from typing import Any

# Calibrated defaults — see the module docstring for the measurement.
ALERT_COMPOSITE_MIN = 0.80
ALERT_CLUSTER_MIN_ACTORS = 3      # post-collapse ACTORS, not raw wallets (§4.2)
# A dossier re-pages only if its peak composite rises this far above the value that
# consumed its previous page. Large enough that scan-to-scan jitter cannot re-page
# (which would defeat the quiet-week discipline), small enough that a real escalation
# still reaches the owner.
RE_ALERT_ESCALATION = 0.05


@dataclass(frozen=True)
class AlertRule:
    """Pre-registered, owner-configurable alert thresholds (§3.5)."""

    composite_min: float = ALERT_COMPOSITE_MIN
    cluster_min_actors: int = ALERT_CLUSTER_MIN_ACTORS
    categories: tuple[str, ...] = field(default_factory=tuple)  # empty = all
    require_contested: bool = True    # standing 0.10-0.90 gate on the signal surface
    # v1.16 §2.2: HELD until funding-mesh collapse is wired into the scan and the arm
    # is calibrated on real post-collapse actor counts. Single-wallet alerting proceeds.
    cluster_arm_enabled: bool = False

    @classmethod
    def from_config(cls, m10: Any) -> "AlertRule":
        """Build from the m10 config block — the owner-configurable path (§3.5). The
        module constants are only the fallback when a field is absent."""
        return cls(
            composite_min=float(getattr(m10, "alert_composite_min", ALERT_COMPOSITE_MIN)),
            cluster_min_actors=int(getattr(m10, "alert_cluster_min_actors",
                                           ALERT_CLUSTER_MIN_ACTORS)),
            categories=tuple(getattr(m10, "alert_categories", ()) or ()),
        )


def _reasons(row: dict[str, Any], rule: AlertRule) -> list[str]:
    out = []
    # High-water mark, not the frozen/decayed value: a footprint that peaked above the
    # bar must not escape alerting because a later scan re-scored it lower (constraint 6
    # — decay is trajectory, not retraction).
    comp = row.get("composite_peak")
    if comp is None:
        comp = row.get("composite")
    if comp is not None and comp >= rule.composite_min:
        out.append(f"composite {comp:.3f} >= {rule.composite_min:.2f}")
    # v1.16 §2.2 — the cluster/coordination arm is HELD (disabled), not shipped at a
    # threshold that cannot fire. m10 does not yet compute funding-mesh collapse, so
    # actor_count_post_collapse is NULL everywhere. Alerting on the RAW wallet count
    # instead would overstate evidence in the single most consequential direction (a
    # 20-wallet mesh that is secretly one actor is n=1 — the Mojtaba case). Unresolved
    # is NOT "20". Re-enable only once collapse is wired AND calibrated.
    if rule.cluster_arm_enabled:
        actors = row.get("actor_count_post_collapse")
        if actors is not None and actors >= rule.cluster_min_actors:
            out.append(f"coordinated cluster: {actors} post-collapse actors "
                       f">= {rule.cluster_min_actors}")
    return out


def evaluate(con: sqlite3.Connection, *, rule: AlertRule | None = None,
             now: int | None = None, mark: bool = True) -> list[dict[str, Any]]:
    """Return alerts for dossiers that newly cross the bar. Dedupe is by
    ``alerted_ts``: a footprint that persists across daily scans pages ONCE.

    Each alert is a pointer (dossier_id + the facts needed to decide whether to open
    it) plus WHY it fired. No recommendation, no EV, no action language."""
    rule = rule or AlertRule()
    now = now or int(time.time())
    # Pending = never alerted, OR escalated materially above the composite that consumed
    # the last page. Without the second arm one marginal early crossing silences a
    # footprint forever, however much stronger it later becomes.
    cur = con.execute(
        "SELECT * FROM dossiers WHERE alerted_ts IS NULL "
        "   OR (alerted_composite IS NOT NULL "
        "       AND COALESCE(composite_peak, composite) >= alerted_composite + ?)",
        (RE_ALERT_ESCALATION,))
    cols = [d[0] for d in cur.description]
    alerts: list[dict[str, Any]] = []
    for raw in cur.fetchall():
        row = dict(zip(cols, raw))
        if rule.categories:
            # The collector prefixes stray-adopted markets ("stray:geopolitics"), so an
            # exact-match filter would silently drop exactly the newly-discovered
            # markets a watch list most wants.
            cat = (row.get("market_category") or "")
            if cat.split(":", 1)[-1].strip() not in rule.categories and cat not in rule.categories:
                continue
        if rule.require_contested:
            v = row.get("entry_vwap")
            if v is None or not (0.10 <= v <= 0.90):
                continue
        why = _reasons(row, rule)
        if not why:
            continue
        alerts.append({
            "dossier_id": row["dossier_id"],
            "market_question": row.get("market_question"),
            "market_category": row.get("market_category"),
            "tier": row.get("tier"), "tier_peak": row.get("tier_peak"),
            # the value the bar was evaluated against (peak, not the decayed current)
            "composite": row.get("composite_peak") if row.get("composite_peak") is not None
            else row.get("composite"),
            "contested_notional": row.get("contested_notional"),
            # None = UNRESOLVED, carried through as-is (never imputed to 1)
            "actor_count_post_collapse": row.get("actor_count_post_collapse"),
            "detection_ts": row.get("detection_ts"),
            "why": why,
            # Pointer, not advice. The dossier is the artifact to read.
            "open_with": f"consensus dossier show {row['dossier_id']}",
        })
    if mark and alerts:
        con.executemany(
            "UPDATE dossiers SET alerted_ts=?, alerted_reason=?, alerted_composite=? "
            "WHERE dossier_id=?",
            [(now, "; ".join(a["why"]), a.get("composite"), a["dossier_id"])
             for a in alerts])
        con.commit()
    return alerts


def render_alerts(alerts: list[dict[str, Any]], *, rule: AlertRule | None = None) -> str:
    """Human-facing alert text. Deliberately flat and factual."""
    rule = rule or AlertRule()
    if not alerts:
        return ("CONSENSUS dossier alerts: none.\n"
                "  (a quiet week is the expected state; value shows on an event)")
    lines = [f"CONSENSUS dossier alerts: {len(alerts)}"]
    for a in alerts:
        cn = a.get("contested_notional")
        cn_s = f"${cn:,.0f}" if isinstance(cn, (int, float)) else "—"
        lines.append(
            f"  [{a.get('tier') or '—'}] {(a.get('market_question') or '—')[:56]}\n"
            f"      {'; '.join(a['why'])} · contested {cn_s} · "
            f"actors {a['actor_count_post_collapse'] if a['actor_count_post_collapse'] is not None else 'unresolved'}\n"
            f"      open: {a['open_with']}")
    lines.append("  These are pointers to dossiers for human review. Not trade signals; "
                 "no expected value is implied or computed.")
    return "\n".join(lines)
