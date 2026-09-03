"""C4/C5 — composition aggregate and the credit-to-capex divergence view.

**C4.** An aggregate is published as total *plus* its three-bucket decomposition
*plus* a top-2 concentration share per bucket, always together (R4/R4a, E14). The
builder bucket spans two orders of magnitude — CRWV at $16.6B TTM against RIOT at
$0.28B — so a bucket subtotal is an honest sum whose shape a reader cannot infer
without knowing how concentrated it is. No weighted index: sums and composition.

**C5.** The thesis metric is TTM debt issuance over TTM capex, per issuer and per
bucket. It is computed on TTM only: measured quarterly ratios are 0% in
non-issuance quarters and 120–148% in issuance quarters, which is not a series.

The falsifier is rendered as three co-plotted series — capex growth, credit
issuance, forward commitments — because the claim under test is about their
*divergence*, and a single ratio hides which leg moved. Coverage statuses travel
inline: a refused issuance total shows REFUSED, never zero (E1).
"""
from . import commitments, config, facts_api, issuance, normalize, tagmap, universe

BUCKET_ORDER = ("hyperscaler", "builder", "reit")

STATUS_OK = "OK"
STATUS_NO_CAPEX = "CAPEX-UNRESOLVED"
STATUS_ISSUANCE_REFUSED = "ISSUANCE-REFUSED"
STATUS_SHORT = "SHORT-HISTORY"
# A summed issuance total that comes out negative means at least one contributing
# concept is stated net of repayments, so the sum is not gross issuance at all.
# Measured on WULF, whose six-concept stack nets to -$0.88B. Publishing "-40% of
# capex funded by credit" would be worse than publishing nothing, so the ratio is
# withheld and the cause named (E1).
STATUS_ISSUANCE_NET_NEGATIVE = "ISSUANCE-NET-NEGATIVE"
# Issuance resolved, but no debt quarter overlaps the capex TTM window. Distinct
# from a refusal and from a true zero — MSFT explicitly tags 0 proceeds, which is
# a fact, whereas this is an absence of observation (E16: decompose coverage).
STATUS_ISSUANCE_NO_OVERLAP = "ISSUANCE-NO-WINDOW-OVERLAP"

# Numerator and denominator agree to three significant figures. The shape of one
# fact resolved into both legs, which would make the ratio a tautology — so it is
# surfaced for a look rather than left to be spotted by eye on a dashboard.
STATUS_SUSPECT_IDENTITY = "SUSPECT-IDENTITY"

# The same concept feeding both legs. Not a suspicion but a defect: the ratio
# would be one fact divided by itself, so it is refused rather than published.
STATUS_RATIO_TAUTOLOGY = "RATIO-TAUTOLOGY"


class IssuerView:
    """One issuer's row in the divergence view, with its coverage statuses."""

    def __init__(self, cik, ticker, bucket, ttm_capex, ttm_issuance, ratio,
                 statuses, quarters, commitments_series=None):
        self.cik = cik
        self.ticker = ticker
        self.bucket = bucket
        self.ttm_capex = ttm_capex
        self.ttm_issuance = ttm_issuance
        self.ratio = ratio
        self.statuses = statuses
        self.quarters = quarters
        self.commitments = commitments_series

    def __repr__(self):
        return "IssuerView({} capex={} ratio={})".format(
            self.ticker, self.ttm_capex, self.ratio)


def _merged_issuance(indexed, contributing, keyed="period_end"):
    """Sum the contributing issuance concepts into one discrete-quarter series.

    `keyed="period_end"` is what the ratio window uses — it must line up with
    capex period-ends exactly. `keyed="calendar"` is what the AGGREGATE uses,
    for the same reason bucket sums key on calendar quarters (trend.py): issuers
    close on different months and cannot otherwise be added.

    Extracted so the panel-level credit leg on the front page and the per-issuer
    ratio come from ONE computation. Two paths would drift.
    """
    merged = {}
    for concept in contributing:
        rowsd = [f for f in indexed.get(concept, [])
                 if f.unit == "USD" and f.period_start]
        pairs = [(f, concept) for f in facts_api.dedupe_latest_filed(rowsd)]
        for r in normalize.discrete_quarters(pairs):
            k = r.period_end if keyed == "period_end" else r.calendar_quarter
            merged[k] = merged.get(k, 0.0) + r.value
    return merged


def issuer_issuance_calendar_series(indexed):
    """{calendar_quarter: issuance} for one issuer, or None when unusable.

    Returns None on the same refusals the ratio honours — a refused issuer is
    absent from the panel credit line rather than contributing a zero to it.
    """
    debt_res = tagmap.resolve(indexed, tagmap.DEBT)
    res = issuance.resolve_total(indexed, debt_res)
    if res.is_refused or not res.contributing:
        return None
    return _merged_issuance(indexed, res.contributing, keyed="calendar") or None


SIGNIFICANT_FIGURES = 3


def _sig(v, figures=SIGNIFICANT_FIGURES):
    """`v` rounded to `figures` significant figures, or None."""
    if v is None or v == 0:
        return v
    import math
    exp = math.floor(math.log10(abs(v)))
    return round(v, -(exp - (figures - 1)))


def suspect_identity(ttm_capex, ttm_issuance, figures=SIGNIFICANT_FIGURES):
    """Do the numerator and denominator agree to `figures` significant figures?

    A credit-to-capex ratio of exactly 100% is the shape of one fact resolved
    into both legs — a financing concept landing in the capex map, or the
    reverse. That would make the ratio a tautology and the divergence
    unfalsifiable, which is the whole thesis.

    **It is a flag for a look, not a verdict.** Measured on IREN, which prompted
    the rule: capex $2,998,006,000 against issuance $3,000,000,000 — equal to
    three figures and not the same number, drawn from
    `PaymentsToAcquirePropertyPlantAndEquipment` and `ProceedsFromConvertibleDebt`
    respectively, which share no fact. Convertible notes are issued in round
    amounts and a real capex number landed 0.07% away from one. So the gate
    fires, the concepts are disjoint, and the answer is coincidence.

    Zero is excluded deliberately: an explicitly tagged zero on both legs is a
    real double zero, not a suspicious agreement (E16).
    """
    if not ttm_capex or not ttm_issuance:
        return False
    if _sig(ttm_capex, figures) != _sig(ttm_issuance, figures):
        return False
    return True


# B7 — a flag with no resolution is a question left open on the page forever.
# Every SUSPECT-IDENTITY that has been investigated states its verdict in the
# status itself, because the status is what a reader sees.
STATUS_SUSPECT_VERIFIED_COINCIDENCE = "SUSPECT-IDENTITY-VERIFIED-COINCIDENCE"

IDENTITY_RESOLUTIONS = {
    "IREN": {
        "verdict": "COINCIDENCE",
        "checked": "2026-09-02",
        "evidence": (
            "capex $2,998,006,000 from PaymentsToAcquirePropertyPlantAndEquipment; "
            "issuance $3,000,000,000 from ProceedsFromConvertibleDebt. Disjoint "
            "concepts, no shared fact, 0.07% apart. Convertible notes are issued "
            "in round amounts and a real capex figure landed next to one; the "
            "display rounding 1.000665 to '+100%' is what made it look like an "
            "identity."),
    },
}


def identity_resolution(ticker):
    return IDENTITY_RESOLUTIONS.get(ticker)


def shares_a_concept(capex_concept, issuance_concepts):
    """Is the same concept feeding both legs? The distinguishing question when
    SUSPECT-IDENTITY fires — disjoint concepts point to coincidence, a shared
    one points to a mapping defect."""
    return bool(capex_concept and capex_concept in set(issuance_concepts or ()))


def build_issuer_view(entity, indexed):
    """Compute one issuer's capex, issuance and ratio with statuses attached."""
    statuses = []
    capex_res = tagmap.resolve(indexed, tagmap.CAPEX, cik=entity.cik)
    if capex_res.is_multi_line or capex_res.is_unresolved:
        return IssuerView(entity.cik, entity.ticker_display, entity.bucket,
                          None, None, None, [STATUS_NO_CAPEX], 0)

    rows = normalize.discrete_quarters(tagmap.series_facts(indexed, capex_res))
    n = normalize.consecutive_run(rows)
    ttm_capex = normalize.ttm(rows)
    if universe.is_short_history(n):
        statuses.append(STATUS_SHORT)

    debt_res = tagmap.resolve(indexed, tagmap.DEBT)
    issuance_res = issuance.resolve_total(indexed, debt_res)
    ttm_issuance = None
    if issuance_res.is_refused:
        statuses.append(STATUS_ISSUANCE_REFUSED)
    elif issuance_res.contributing:
        merged = _merged_issuance(indexed, issuance_res.contributing)
        if rows:
            window = [r.period_end for r in rows[-config.ANCHOR_WINDOW_QUARTERS:]]
            vals = [merged[e] for e in window if e in merged]
            if not vals:
                # An explicitly tagged zero is a FACT, not an absence. Microsoft
                # tags 0 debt proceeds for the fiscal year covering this window;
                # excluding it would quietly shrink the bucket denominator and
                # overstate the hyperscaler ratio. Only a genuine absence of
                # observation gets NO-WINDOW-OVERLAP (R-CD2-2, E16).
                if _explicit_zero_over(indexed, issuance_res.contributing, rows):
                    ttm_issuance = 0.0
                else:
                    statuses.append(STATUS_ISSUANCE_NO_OVERLAP)
            else:
                ttm_issuance = sum(vals)
                if ttm_issuance < 0:
                    statuses.append(STATUS_ISSUANCE_NET_NEGATIVE)
                    ttm_issuance = None

    # `ttm_issuance == 0` is a real 0%, not a missing value — see _explicit_zero_over.
    ratio = (ttm_issuance / ttm_capex) if (ttm_issuance is not None and ttm_capex) else None
    contributing = (issuance_res.contributing if issuance_res else ()) or ()
    if suspect_identity(ttm_capex, ttm_issuance):
        if shares_a_concept(capex_res.current_concept, contributing):
            # One fact over itself is not a measurement. Publishing it as a
            # ratio would make the divergence unfalsifiable at exactly the
            # place the thesis is tested, so the ratio is refused outright.
            statuses.append(STATUS_RATIO_TAUTOLOGY)
            ratio = None
        else:
            res = identity_resolution(entity.ticker_display)
            statuses.append(STATUS_SUSPECT_VERIFIED_COINCIDENCE
                            if res and res["verdict"] == "COINCIDENCE"
                            else STATUS_SUSPECT_IDENTITY)
    if not statuses:
        statuses.append(STATUS_OK)
    return IssuerView(entity.cik, entity.ticker_display, entity.bucket, ttm_capex,
                      ttm_issuance, ratio, statuses, n,
                      commitments.forward_commitments(entity.cik, indexed))


def _explicit_zero_over(indexed, concepts, capex_rows, unit="USD"):
    """True when a contributing concept tags an explicit 0 spanning the window.

    Distinguishes "the issuer said none" from "we have no observation" — the
    difference between a real 0% and an exclusion.
    """
    if len(capex_rows) < config.ANCHOR_WINDOW_QUARTERS:
        return False
    start = capex_rows[-config.ANCHOR_WINDOW_QUARTERS].period_start
    end = capex_rows[-1].period_end
    for c in concepts:
        for f in indexed.get(c, []):
            if (f.unit == unit and f.value == 0 and f.period_start
                    and f.period_start <= start and f.period_end >= end):
                return True
    return False


def concentration(values, top=2):
    """Share of a bucket subtotal held by its largest `top` members."""
    vals = sorted((v for v in values if v), reverse=True)
    total = sum(vals)
    if not total:
        return None
    return sum(vals[:top]) / total


def composition(views):
    """C4 — total plus decomposition plus concentration, as one object.

    Deliberately returns them together. A caller cannot obtain the headline
    without also holding the composition, which is the point of E14.
    """
    buckets = {}
    for b in BUCKET_ORDER:
        members = [v for v in views if v.bucket == b and v.ttm_capex]
        subtotal = sum(v.ttm_capex for v in members)
        buckets[b] = {
            "members": sorted(members, key=lambda v: -(v.ttm_capex or 0)),
            "subtotal": subtotal,
            "top2_share": concentration([v.ttm_capex for v in members]),
            "n": len(members),
        }
    total = sum(b["subtotal"] for b in buckets.values())
    excluded = [v for v in views if not v.ttm_capex or v.bucket not in BUCKET_ORDER]
    return {
        "total": total,
        "buckets": buckets,
        "excluded": [(v.ticker, v.statuses) for v in excluded],
        "coverage": "{} of {} universe members contribute".format(
            sum(b["n"] for b in buckets.values()), len(views)),
    }


def divergence_rows(views):
    """C5 — per-issuer credit-to-capex with statuses inline, never a bare zero."""
    out = []
    for v in sorted(views, key=lambda v: (v.bucket, -(v.ttm_capex or 0))):
        if v.ttm_capex is None:
            out.append((v.ticker, v.bucket, None, None, None, ",".join(v.statuses)))
            continue
        out.append((v.ticker, v.bucket, v.ttm_capex, v.ttm_issuance, v.ratio,
                    ",".join(v.statuses)))
    return out


def bucket_divergence(views):
    """C5 — the same metric at bucket level.

    Issuers whose issuance total is refused are excluded from the numerator AND
    named, so the bucket ratio is never quietly computed over a partial
    denominator (E16 — a coverage number must decompose).
    """
    out = {}
    for b in BUCKET_ORDER:
        members = [v for v in views if v.bucket == b and v.ttm_capex]
        contributing = [v for v in members if v.ttm_issuance is not None]
        refused = [v.ticker for v in members if v.ttm_issuance is None]
        capex = sum(v.ttm_capex for v in contributing)
        debt = sum(v.ttm_issuance for v in contributing)
        out[b] = {
            "ratio": (debt / capex) if capex else None,
            "ttm_capex_of_contributors": capex,
            "ttm_issuance": debt,
            "contributing": [v.ticker for v in contributing],
            "excluded_no_issuance": refused,
        }
    return out


def falsifier_series(views, quarters=8):
    """C5 — the three co-plotted legs the Hayes falsifier is stated in.

    capex growth (is the build decelerating), credit issuance (is borrowing
    rising), forward commitments (is contracted future spend rising). The claim
    is about their divergence, so they are returned together and never collapsed
    into one number.
    """
    legs = {"capex_ttm": {}, "issuance_ttm": {}, "commitments": {}}
    for v in views:
        if v.ttm_capex:
            legs["capex_ttm"][v.ticker] = v.ttm_capex
        if v.ttm_issuance is not None:
            legs["issuance_ttm"][v.ticker] = v.ttm_issuance
        if v.commitments and v.commitments.status == commitments.STATUS_COVERED:
            latest = v.commitments.latest
            if latest:
                legs["commitments"][v.ticker] = latest.value
    return legs
