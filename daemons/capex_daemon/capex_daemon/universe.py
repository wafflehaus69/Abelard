"""Universe roster and coverage-derived tiering.

The roster file is DATA, not code. Tier is COMPUTED from measured coverage and
re-evaluated every scan (CD-1-SPEC 3) — the CSV never carries a tier. Its
`bucket` column is the economic species used for aggregate decomposition (R4),
which is a different axis from coverage tier entirely.

`tier_override` can only force a name OUT (EXCLUDE). It can never promote a name
up a tier: tiers are earned by measured coverage, never asserted.
"""
import csv
import os

from . import config

UNIVERSE_CSV = os.path.join(os.path.dirname(__file__), "data", "universe.csv")

# `host` and `sidecar` are admitted but NOT aggregated (see divergence.BUCKET_ORDER):
#   host    — owns/hosts datacenter property, but its capex is not separable from a
#             larger consolidated line (IRM records management, AMT towers, CCOI
#             telecom). Summing their consolidated capex into the datacenter total
#             would inflate it with spend that is not datacenter spend.
#   sidecar — admitted, but its capex is already counted inside another member.
#             BTBT consolidates WYFI, so summing both double-counts the same dollars.
# `supplier` is a CROSS-CHECK bucket, not a spending bucket: its members sell
# the buildout rather than buy it, so their revenue is the same dollar as
# someone else's capex seen from the other side of the invoice. It is absent
# from `trend.AGGREGATED_BUCKETS` by design (CD-R2 §2.3) and must stay absent.
# `landlord` merges what were `reit` and `host`. Ruled by Mando 2026-08-26: they
# are all property-owners renting capacity to the buildout, the split was thin
# on its own terms (AMT and IRM are REITs, and sat under `host`), and a two-name
# REIT bucket had already gone dark for a month on one late filing. Five members
# means the MIN_BUCKET_MEMBERS floor is not one absence away from breaching.
#
# The old names remain VALID so an older roster still loads; nothing in the
# panel assigns them any more.
BUCKETS = ("hyperscaler", "builder", "landlord", "fpi", "mirror", "sidecar",
           "supplier", "reit", "host")

# Sub-type survives the merge — the distinction is real even where the bucket
# boundary was not, and a reader asking "is this a REIT?" deserves an answer.
LANDLORD_SUBTYPE = {
    "0001297996": "reit",   # DLR
    "0001101239": "reit",   # EQIX
    "0001053507": "reit",   # AMT
    "0001020569": "reit",   # IRM
    "0001158324": "host",   # CCOI
}


def landlord_subtype(cik):
    return LANDLORD_SUBTYPE.get(config.cik10(cik)) if cik else None

TIER_CORE = "CORE"
TIER_THIN = "THIN"
TIER_ANNUAL_DEGRADED = "ANNUAL-DEGRADED"
TIER_MIRROR = "MIRROR"
TIER_EXCLUDED = "EXCLUDED"
# Names with 4..11 consecutive derivable quarters. The two live rulings disagree
# about this band (CD-1-SPEC 3.1); until Mando rules, membership is reported as
# unruled rather than assigned to a guessed side. Nothing may key on this.
TIER_UNRULED_BAND = "UNRULED-BAND"


class Entity:
    """A universe member. Keyed on CIK; ticker is a display attribute (E10)."""

    __slots__ = ("cik", "ticker_display", "bucket", "tier_override", "notes")

    def __init__(self, cik, ticker_display, bucket, tier_override, notes):
        self.cik = config.cik10(cik)
        self.ticker_display = ticker_display
        self.bucket = bucket
        self.tier_override = tier_override or None
        self.notes = notes or ""

    def __repr__(self):
        return "Entity(cik={} display={} bucket={})".format(
            self.cik, self.ticker_display, self.bucket)


def load(path=UNIVERSE_CSV):
    """Roster keyed by 10-digit CIK. Duplicate CIK is a loud failure (E1)."""
    out = {}
    with open(path, newline="", encoding="utf-8") as fh:
        for row in csv.DictReader(fh):
            if not row.get("cik", "").strip():
                continue
            e = Entity(row["cik"], row["ticker_display"].strip(),
                       row["bucket"].strip(), row.get("tier_override", "").strip(),
                       row.get("notes", "").strip())
            if e.bucket not in BUCKETS:
                raise ValueError("{}: unknown bucket {!r}".format(e.ticker_display, e.bucket))
            if e.cik in out:
                raise ValueError("duplicate CIK {} in {}".format(e.cik, path))
            out[e.cik] = e
    if not out:
        raise ValueError("universe roster is empty: {}".format(path))
    return out


def tier_for(entity, consecutive_quarters):
    """Coverage tier for an entity given its measured consecutive-quarter count.

    Returns (tier, reason). Never guesses across the unruled band, and never
    substitutes a default for the unset CORE threshold (E8).
    """
    if entity.tier_override:
        return TIER_EXCLUDED, "tier_override={}".format(entity.tier_override)
    if entity.bucket == "mirror":
        return TIER_MIRROR, "ruled MIRROR; own capex is not the read"
    if entity.bucket == "fpi":
        return TIER_ANNUAL_DEGRADED, "FPI; no structured quarterly capex exists"

    n = consecutive_quarters
    if n is None:
        return TIER_THIN, "no coverage measured yet"
    if n < config.THIN_MAX_QUARTERS:
        return TIER_THIN, "{} consecutive quarters, below the {}-quarter floor".format(
            n, config.THIN_MAX_QUARTERS)
    if n < config.SHORT_HISTORY_QUARTERS:
        return TIER_CORE, "{} consecutive quarters; graduated at {} (R-B6-2), SHORT-HISTORY".format(
            n, config.CORE_MIN_QUARTERS)
    return TIER_CORE, "{} consecutive quarters".format(n)


def is_short_history(consecutive_quarters):
    """True when a CORE member's TTM rests on under three years of history.

    Graduation at four quarters is ruled and automatic; the thinness of the
    resulting series is disclosed on the panel row rather than used to withhold
    membership (R-B6-2).
    """
    if consecutive_quarters is None:
        return True
    return consecutive_quarters < config.SHORT_HISTORY_QUARTERS
