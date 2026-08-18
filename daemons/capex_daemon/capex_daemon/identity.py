"""Entity registry — CIK-keyed identity and tier transitions (E10, R1, R3).

Entity state keys on CIK. Ticker and name are display attributes, resolved at
read time and re-resolved every scan; neither is ever a key.

**Rename detection compares stored name against freshly fetched name — across
successive scans.** It does NOT diff `name` against the `formerNames` list. That
naive comparison is broken by a real EDGAR artifact: APLD carries a `formerNames`
entry whose value is *identical* to its current name, ended 2026-08-06, with no
8-K item 5.03 and no new name anywhere. Diffing against the list reports a rename
that never happened. `formerNames` is read here for context only, never for
detection.

Tier transitions are automatic at the ruled boundary (R1) and never silent: a
graduation is logged, and a downgrade is logged as a regression because it means
coverage went backwards.
"""
import re
import time

from . import universe

# Legal-form suffixes and punctuation carry no identity. "Applied Digital Corp."
# and "Applied Digital Corporation" are the same registrant; "Keel
# Infrastructure Corp." and "Bitfarms Ltd" are not.
_SUFFIXES = (
    "incorporated", "corporation", "company", "limited", "holdings", "holding",
    "inc", "corp", "co", "ltd", "llc", "lp", "plc", "nv", "sa", "ag", "group",
)
_PUNCT = re.compile(r"[^\w\s]+")
_WS = re.compile(r"\s+")

FIELD_NAME = "name"
FIELD_TICKER = "ticker"
FIELD_SIC = "sic"
FIELD_FYE = "fiscal_year_end"

DIRECTION_UP = "graduation"
DIRECTION_DOWN = "regression"
DIRECTION_LATERAL = "lateral"

_TIER_RANK = {
    universe.TIER_THIN: 0,
    universe.TIER_UNRULED_BAND: 1,
    universe.TIER_CORE: 2,
    universe.TIER_ANNUAL_DEGRADED: -1,
    universe.TIER_MIRROR: -1,
    universe.TIER_EXCLUDED: -2,
}


def normalize_name(name):
    """Casefolded, de-punctuated, suffix-stripped form for across-scan comparison."""
    if not name:
        return ""
    s = _PUNCT.sub(" ", name).casefold()
    parts = [p for p in _WS.sub(" ", s).strip().split(" ") if p]
    while parts and parts[-1] in _SUFFIXES:
        parts.pop()
    return " ".join(parts)


def preferred_display_ticker(tickers):
    """Pick the common-share line out of a CIK's listed tickers.

    Warrants, preferreds and secondary lines are DERIVED from the common symbol
    and are therefore longer than it — usually a suffix of it. Measured across
    the universe: CLSK/CLSKW, SLNH/SLNHP, PLD/PLDGP, GPUS/GPUS-PD are all
    prefix pairs; GDS/GDHLF is not, and there the shorter symbol is still the
    common ADR. So: prefer a ticker that every other ticker starts with, else
    the shortest, tie-broken alphabetically for determinism.

    Order-independent by construction — the bug this replaces was reading the
    same source two ways and getting CLSK one time and CLSKW the next.
    """
    cands = [t for t in (tickers or ()) if t]
    if not cands:
        return None
    if len(cands) == 1:
        return cands[0]
    for t in sorted(cands, key=lambda x: (len(x), x)):
        others = [o for o in cands if o != t]
        if others and all(o.startswith(t) for o in others):
            return t
    return sorted(cands, key=lambda x: (len(x), x))[0]


class Snapshot:
    """One scan's view of an entity's registry state."""

    __slots__ = ("cik", "name", "tickers", "exchanges", "sic", "sic_description",
                 "fiscal_year_end", "entity_type", "former_names")

    def __init__(self, cik, name, tickers, exchanges, sic, sic_description,
                 fiscal_year_end, entity_type, former_names=()):
        self.cik = cik
        self.name = name
        self.tickers = tuple(tickers or ())
        self.exchanges = tuple(exchanges or ())
        self.sic = sic
        self.sic_description = sic_description
        self.fiscal_year_end = fiscal_year_end
        self.entity_type = entity_type
        self.former_names = tuple(former_names or ())

    @property
    def ticker_display(self):
        """The COMMON-share ticker, never a preferred or warrant line (R-CD2-6).

        A CIK routinely lists several securities. Taking the first is
        order-dependent and wrong the moment the source reorders: the same map
        read two ways gave CLSK and CLSKW for CleanSpark. Resolution is by
        structure instead — see `preferred_display_ticker`.

        Returns None for a non-traded filer; callers display CIK + name.
        """
        return preferred_display_ticker(self.tickers)

    @property
    def display_label(self):
        """What a human should see. Falls back to CIK + name when untraded."""
        t = self.ticker_display
        return t if t else "CIK {} ({})".format(self.cik, self.name or "unnamed")

    def __repr__(self):
        return "Snapshot(cik={} name={!r})".format(self.cik, self.name)


def from_submissions(doc):
    """Build a Snapshot from a parsed submissions JSON document."""
    from . import config
    return Snapshot(
        cik=config.cik10(doc.get("cik")),
        name=doc.get("name"),
        tickers=doc.get("tickers"),
        exchanges=doc.get("exchanges"),
        sic=doc.get("sic"),
        sic_description=doc.get("sicDescription"),
        fiscal_year_end=doc.get("fiscalYearEnd"),
        entity_type=doc.get("entityType"),
        former_names=tuple((f or {}).get("name") for f in (doc.get("formerNames") or [])),
    )


class IdentityEvent:
    __slots__ = ("cik", "field", "old_value", "new_value", "observed_unix")

    def __init__(self, cik, field, old_value, new_value, observed_unix):
        self.cik = cik
        self.field = field
        self.old_value = old_value
        self.new_value = new_value
        self.observed_unix = observed_unix

    def __repr__(self):
        return "IdentityEvent({} {}: {!r} -> {!r})".format(
            self.cik, self.field, self.old_value, self.new_value)


def diff(stored, snapshot, now_unix):
    """Discontinuities between the stored snapshot and a fresh one.

    `stored` is a mapping of the previous scan's values, or None on first sight.
    First sight is NOT a rename — there is no prior value to differ from.
    """
    if stored is None:
        return []
    events = []
    old_name, new_name = stored.get("name_current"), snapshot.name
    if normalize_name(old_name) != normalize_name(new_name):
        events.append(IdentityEvent(snapshot.cik, FIELD_NAME, old_name, new_name, now_unix))
    old_ticker, new_ticker = stored.get("ticker_display"), snapshot.ticker_display
    if (old_ticker or "") != (new_ticker or ""):
        events.append(IdentityEvent(snapshot.cik, FIELD_TICKER, old_ticker, new_ticker, now_unix))
    if str(stored.get("sic") or "") != str(snapshot.sic or ""):
        events.append(IdentityEvent(snapshot.cik, FIELD_SIC, stored.get("sic"), snapshot.sic, now_unix))
    if (stored.get("fiscal_year_end") or "") != (snapshot.fiscal_year_end or ""):
        events.append(IdentityEvent(snapshot.cik, FIELD_FYE, stored.get("fiscal_year_end"),
                                    snapshot.fiscal_year_end, now_unix))
    return events


def load_stored(con, cik):
    row = con.execute(
        "SELECT cik, ticker_display, name_current, bucket, sic, fiscal_year_end, "
        "entity_type FROM entities WHERE cik=?", (cik,)).fetchone()
    if row is None:
        return None
    keys = ("cik", "ticker_display", "name_current", "bucket", "sic",
            "fiscal_year_end", "entity_type")
    return dict(zip(keys, row))


def record(con, snapshot, bucket, now_unix=None):
    """Upsert the entity and write any identity discontinuities. Returns events."""
    now_unix = int(now_unix if now_unix is not None else time.time())
    stored = load_stored(con, snapshot.cik)
    events = diff(stored, snapshot, now_unix)
    for e in events:
        con.execute(
            "INSERT OR REPLACE INTO identity_events"
            "(cik, observed_unix, field, old_value, new_value) VALUES (?,?,?,?,?)",
            (e.cik, e.observed_unix, e.field,
             None if e.old_value is None else str(e.old_value),
             None if e.new_value is None else str(e.new_value)))
    con.execute(
        "INSERT OR REPLACE INTO entities"
        "(cik, ticker_display, name_current, bucket, sic, fiscal_year_end, "
        " entity_type, last_resolved_unix) VALUES (?,?,?,?,?,?,?,?)",
        (snapshot.cik, snapshot.ticker_display, snapshot.name, bucket,
         str(snapshot.sic or ""), snapshot.fiscal_year_end or "",
         snapshot.entity_type or "", now_unix))
    con.commit()
    return events


def direction(old_tier, new_tier):
    if old_tier is None:
        return DIRECTION_LATERAL
    a, b = _TIER_RANK.get(old_tier, -1), _TIER_RANK.get(new_tier, -1)
    if b > a:
        return DIRECTION_UP
    if b < a:
        return DIRECTION_DOWN
    return DIRECTION_LATERAL


def record_tier(con, cik, new_tier, consecutive_quarters, reason, now_unix=None):
    """Log a tier crossing. Automatic per R1 — logged, never ruled per name.

    Returns the event dict when the tier changed, else None. A regression is
    logged with direction='regression' because a tier that goes backwards means
    coverage was lost, which must never pass silently.
    """
    now_unix = int(now_unix if now_unix is not None else time.time())
    row = con.execute(
        "SELECT tier FROM coverage WHERE cik=? AND series_kind='capex'", (cik,)).fetchone()
    old_tier = row[0] if row else None
    if old_tier == new_tier:
        return None
    d = direction(old_tier, new_tier)
    con.execute(
        "INSERT OR REPLACE INTO tier_events"
        "(cik, observed_unix, old_tier, new_tier, consecutive_quarters, direction, reason)"
        " VALUES (?,?,?,?,?,?,?)",
        (cik, now_unix, old_tier, new_tier, consecutive_quarters, d, reason))
    con.commit()
    return {"cik": cik, "old_tier": old_tier, "new_tier": new_tier,
            "direction": d, "consecutive_quarters": consecutive_quarters}
