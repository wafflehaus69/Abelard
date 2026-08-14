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

BUCKETS = ("hyperscaler", "builder", "reit", "fpi", "mirror")

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
    if config.CORE_MIN_QUARTERS is None:
        # Both live rulings agree above 11; they disagree on 4..11.
        if n >= 12:
            return TIER_CORE, "{} consecutive quarters; above the contested band".format(n)
        return TIER_UNRULED_BAND, (
            "{} consecutive quarters sits in the 4-11 band where R1 and the ratified "
            "CORE=13 roster conflict (CD-1-SPEC 3.1); unruled".format(n))
    if n >= config.CORE_MIN_QUARTERS:
        return TIER_CORE, "{} consecutive quarters".format(n)
    return TIER_UNRULED_BAND, "{} consecutive quarters".format(n)
