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
    # A cluster is a span on the TRADE clock, so a row whose trade date cannot be true
    # is not a member of anyone's: it would be counted into whichever window its
    # mistyped date happened to land in.
    rows = con.execute(
        "SELECT COUNT(DISTINCT person_id) FROM congress_trades "
        "WHERE ticker=? AND side IN ({}) AND superseded=0 AND date_flag IS NULL "
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
               entity=None, shares=None, value=None,
               date_flag=None, date_subclass=None, tx_date_suggested=None):
    """A disclosure is an event on the DISCLOSURE clock whatever its trade date says, so
    one whose trade date is quarantined is still emitted, whole. What it does not get is
    anything computed on the trade clock: its lag is None, and no cluster span is drawn
    around a date that cannot be placed. flags.trade_date says why, with any suggested
    date labelled as a hypothesis. tx_date itself is passed through exactly as filed --
    it is part of the event's identity and is never corrected."""
    conv, watch = overlay.match(ticker)
    direction = _direction(side)
    cl = None if date_flag else cluster_flag(con, ticker, direction, tx_date,
                                             overlay.min_persons, overlay.window_days)
    if date_flag:
        lag_days = None
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
            # None on a clean row. Otherwise which rule took the row off the trade
            # clock (flag), what its date string looks like (subclass), and for the
            # year-slip pattern a suggested date that is a hypothesis, not a repair.
            "trade_date": ({"flag": date_flag, "subclass": date_subclass,
                            "suggested": tx_date_suggested}
                           if (date_flag or date_subclass) else None),
        },
        "source": source,
        "filing_ref": filing_ref,
    }
