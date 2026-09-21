"""CD-FRONTIER-CLOSE F3 — every published series, and the guard that stands on it.

**Inconsistent guards are how the 2026-09-12 defect leaked.** Buckets refused a
one-member latest point; the total had no floor at all. The constant-membership
panel had a floor but chose its window end by "newest quarter any member
reached". Each guard was reasonable where it stood, and nobody could see that
the set of them was uneven, because the set was never written down in one place.

So this is that place. Every path the snapshot publishes appears here, and each
entry declares exactly one of:

  * `floor` — the minimum members behind the LATEST published figure, and where
    that is enforced; or
  * `no_floor_because` — why a membership floor does not apply to this thing
    (a census, a single name by construction, a figure refused outright); or
  * `metadata` — not a figure at all.

and, for anything that sums members over quarters, whether it is `frontier`
gated (a trailing quarter joins only once its reporters cover the prior
quarter's dollars — see `trend.frontier_split`).

`tests/test_guards.py` builds a real snapshot and fails if any published path is
missing from this table. A new series cannot ship without declaring its guard.
"""
from . import trend

FLOOR = trend.MIN_BUCKET_MEMBERS

# kind: what the thing IS, which is what decides which guard is honest for it.
MATCHED_SUM = "matched sum"
CONSTANT_LEVEL = "constant-membership level"
DERIVED = "derived from guarded series"
CENSUS = "census"
SINGLE = "single name by construction"
REFUSED = "refused outright"
META = "metadata"

GUARDS = {
    # --- matched sums: floor on the latest point + frontier gate -------------
    "total": dict(
        kind=MATCHED_SUM, floor=FLOOR, frontier=True,
        enforced="trend.build: state INSUFFICIENT-MEMBERSHIP below the floor "
                 "(NEW, F3 — the total had none); snapshot total.floor"),
    "buckets.*": dict(
        kind=MATCHED_SUM, floor=FLOOR, frontier=True,
        enforced="trend.build: state INSUFFICIENT-MEMBERSHIP (since CD-PH1); "
                 "snapshot buckets.*.floor"),
    "panel.issuance_ttm": dict(
        kind=MATCHED_SUM, floor=FLOOR, frontier=True,
        enforced="panel.issuance_floor (NEW, F3); the thesis credit clause "
                 "withholds its direction below the floor"),
    "suppliers.combined": dict(
        kind=MATCHED_SUM, floor=FLOOR, frontier=True,
        enforced="suppliers.combined.floor (NEW, F3)"),
    "panel.commitments": dict(
        kind=REFUSED, frontier=False,
        no_floor_because="a matched STOCK across three concepts that are not the "
                         "same measure; its total is REFUSED-MIXED-BASIS on every "
                         "surface that would show it (B5), so no figure from it is "
                         "ever published as a panel number. Per-issuer stocks are "
                         "shown instead, each a single name."),

    # --- constant-membership levels: floor + coverage + frontier -------------
    "panel.constant.capex": dict(
        kind=CONSTANT_LEVEL, floor=FLOOR, frontier=True,
        enforced="constant_membership_panel(min_members) + COVERAGE_FLOOR; "
                 "window end frontier-gated (NEW, F3 — it ended at the newest "
                 "quarter ANY member reached, and went empty on 2026-09-12)"),
    "panel.constant.issuance": dict(kind=CONSTANT_LEVEL, floor=FLOOR, frontier=True,
                                    enforced="as panel.constant.capex"),
    "panel.constant.commitments": dict(kind=CONSTANT_LEVEL, floor=FLOOR, frontier=True,
                                       enforced="as panel.constant.capex"),
    "panel.constant.buckets.*": dict(kind=CONSTANT_LEVEL, floor=FLOOR, frontier=True,
                                     enforced="as panel.constant.capex"),
    "panel.constant.jaws_capex": dict(
        kind=CONSTANT_LEVEL, floor=FLOOR, frontier=True,
        enforced="constant panel over the capex/issuance intersection; build() "
                 "also requires >= floor names before attempting it"),
    "panel.constant.jaws_issuance": dict(
        kind=CONSTANT_LEVEL, floor=FLOOR, frontier=True,
        enforced="same members and window as jaws_capex, by construction"),

    # --- derived: guarded by what they are derived from ----------------------
    "panel.credit_ratio_series": dict(
        kind=DERIVED, frontier=True,
        no_floor_because="a ratio of the total and the credit leg, each floored and "
                         "frontier-gated; every row carries its member count"),
    "panel.frontier_pair": dict(
        kind=DERIVED, frontier=True,
        no_floor_because="built from the gated total and credit series over their "
                         "shared quarters; labelled a currency read, with every "
                         "in-window entry published as a composition event"),
    "suppliers.crosscheck": dict(
        kind=DERIVED, frontier=True,
        no_floor_because="a ratio of suppliers.combined over the hyperscaler bucket, "
                         "both floored and gated; each row carries both member "
                         "counts, and a falling denominator raises its own warning"),

    # --- census: counts, not sums --------------------------------------------
    "panel.breadth_series": dict(
        kind=CENSUS, frontier=False,
        no_floor_because="a count of names in each state and direction, not a sum "
                         "of their dollars — one name reads as 1, never as a panel "
                         "figure, so there is nothing for a floor to protect"),

    # --- single names: nothing is aggregated ---------------------------------
    "issuers.*": dict(kind=SINGLE, frontier=False,
                      no_floor_because="one issuer's own series"),
    "suppliers.legs.*": dict(kind=SINGLE, frontier=False,
                             no_floor_because="one supplier's own series"),
    "suppliers.frontier": dict(
        kind=SINGLE, frontier=False,
        no_floor_because="A3: each row is one supplier's own quarter against its own "
                         "history, labelled as ahead of the demand panel; its demand "
                         "frontier is read from the gated hyperscaler series"),

    # --- not figures ---------------------------------------------------------
    "generated_unix": dict(kind=META),
    "bands_measured_on": dict(kind=META),
    "transitions": dict(kind=META, note="state changes of the series above; alerts "
                                        "are frontier-gated separately (E31)"),
    "since_last_scan": dict(kind=META, note="B1 record of the scan's own changes"),
    "panel.issuance_partial": dict(kind=META, note="held quarters of the credit leg"),
    "panel.issuance_floor": dict(kind=META, note="the credit leg's floor verdict"),
    "panel.issuance_membership_latest": dict(kind=META),
    "panel.commitments_membership_latest": dict(kind=META),
    "panel.commitments_panel": dict(kind=META, note="the commitments refusal status"),
    "suppliers.covered": dict(kind=META),
}


def normalise(path):
    """`buckets.hyperscaler` -> `buckets.*`, so per-name maps match one entry."""
    for prefix in ("buckets.", "issuers.", "suppliers.legs.", "panel.constant.buckets."):
        if path.startswith(prefix) and path.count(".") == prefix.count("."):
            return prefix + "*"
    return path


def published_paths(snap):
    """Every aggregate-level path a snapshot publishes, normalised."""
    out = set()
    for k, v in snap.items():
        if k in ("panel", "suppliers"):
            continue
        if k in ("buckets", "issuers"):
            out.update(normalise("{}.{}".format(k, n)) for n in v)
        else:
            out.add(k)
    for k, v in (snap.get("panel") or {}).items():
        if k == "constant":
            for ck, cv in v.items():
                if ck == "buckets":
                    out.update(normalise("panel.constant.buckets.{}".format(b)) for b in cv)
                else:
                    out.add("panel.constant.{}".format(ck))
        else:
            out.add("panel.{}".format(k))
    for k, v in (snap.get("suppliers") or {}).items():
        if k == "legs":
            out.update(normalise("suppliers.legs.{}".format(n)) for n in v)
        else:
            out.add("suppliers.{}".format(k))
    return out


def undeclared(snap):
    """Published paths with no guard declared. Must be empty."""
    return sorted(p for p in published_paths(snap) if p not in GUARDS)


def table():
    """The audit, as rows: (path, kind, floor, frontier, enforcement-or-reason)."""
    rows = []
    for path, g in GUARDS.items():
        if g["kind"] == META:
            continue
        rows.append((path, g["kind"], g.get("floor"), g.get("frontier"),
                     g.get("enforced") or g.get("no_floor_because")))
    return rows
