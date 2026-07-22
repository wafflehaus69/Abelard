"""Positioning-event schema + flag computation (SM-4 STEP 3). Every leg emits
this shape. Deterministic: event_id is a hash of identity fields, no randomness.
"""
import hashlib
import json
import os

from .scorecard import QUALITATIVE_SEEDS

SEED_ROLES = {s["name"]: s["role"] for s in QUALITATIVE_SEEDS}
BUY_SIDES = {"purchase", "P", "buy"}
SELL_SIDES = {"sale", "sale_full", "sale_partial", "S", "sell"}


def load_registry(path):
    if not os.path.exists(path):
        return {"by_name": {}, "entries": []}
    d = json.load(open(path))
    by_name = {e["name"]: e for e in d.get("entries", [])}
    return {"by_name": by_name, "entries": d.get("entries", [])}


def _direction(side):
    if side in BUY_SIDES:
        return "buy"
    if side in SELL_SIDES:
        return "sell"
    return "other"


def event_id(leg, filing_ref, ticker, side, tx_date):
    key = "|".join(str(x) for x in (leg, filing_ref, ticker, side, tx_date))
    return hashlib.sha1(key.encode()).hexdigest()[:16]


def cluster_flag(con, ticker, direction, tx_date, min_persons, window_days):
    """Count distinct persons (whole universe) trading the same ticker in the
    same direction within a rolling window ending at tx_date. Flags at
    min_persons. Deterministic against the DB at scan time."""
    if not ticker or direction == "other":
        return None
    sides = BUY_SIDES if direction == "buy" else SELL_SIDES
    ph = ",".join("?" for _ in sides)
    rows = con.execute(
        "SELECT COUNT(DISTINCT person_id) FROM congress_trades "
        "WHERE ticker=? AND side IN ({}) AND superseded=0 "
        "AND tx_date <= ? AND tx_date >= date(?, ?)".format(ph),
        (ticker, *sides, tx_date, tx_date, "-{} days".format(window_days)),
    ).fetchone()
    n = rows[0] if rows else 0
    if n >= min_persons:
        return {"n_persons": n, "window_days": window_days, "direction": direction}
    return None


def make_event(scan_id, leg, person, role, registry_status, ticker, side,
               instrument, amount, tx_date, disclosure_date, lag_days,
               plan_flag, source, filing_ref, overlay, con,
               entity=None, shares=None, value=None):
    conv, watch = overlay.match(ticker)
    direction = _direction(side)
    cl = cluster_flag(con, ticker, direction, tx_date,
                      overlay.min_persons, overlay.window_days)
    sentinel = None
    if person in SEED_ROLES:
        sentinel = {"role": SEED_ROLES[person], "note": "seed-list person"}
    amt = None
    if amount is not None:
        amt = {"low": amount[0], "high": amount[1]}
    elif shares is not None or value is not None:
        amt = {"shares": shares, "value": value}
    return {
        "event_id": event_id(leg, filing_ref, ticker, side, tx_date),
        "scan_id": scan_id,
        "leg": leg,
        "person": person,
        "entity": entity,
        "role": role,
        "registry_status": registry_status,
        "ticker": ticker,
        "side": side,
        "instrument": instrument,
        "amount": amt,
        "tx_date": tx_date,
        "disclosure_date": disclosure_date,
        "lag_days": lag_days,
        "plan_flag": plan_flag,
        "flags": {
            "conviction_overlay": conv,
            "watchlist_overlay": watch,
            "cluster": cl,
            "sentinel": sentinel,
        },
        "source": source,
        "filing_ref": filing_ref,
    }
