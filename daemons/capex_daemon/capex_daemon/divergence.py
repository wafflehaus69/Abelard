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


def build_issuer_view(entity, indexed):
    """Compute one issuer's capex, issuance and ratio with statuses attached."""
    statuses = []
    capex_res = tagmap.resolve(indexed, tagmap.CAPEX)
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
        smap = issuance.build_series_map(indexed, issuance_res.contributing)
        merged = {}
        for concept in issuance_res.contributing:
            rowsd = [f for f in indexed.get(concept, [])
                     if f.unit == "USD" and f.period_start]
            pairs = [(f, concept) for f in facts_api.dedupe_latest_filed(rowsd)]
            for r in normalize.discrete_quarters(pairs):
                merged[r.period_end] = merged.get(r.period_end, 0.0) + r.value
        if rows:
            window = [r.period_end for r in rows[-config.ANCHOR_WINDOW_QUARTERS:]]
            vals = [merged[e] for e in window if e in merged]
            if not vals:
                statuses.append(STATUS_ISSUANCE_NO_OVERLAP)
            else:
                ttm_issuance = sum(vals)
                if ttm_issuance < 0:
                    statuses.append(STATUS_ISSUANCE_NET_NEGATIVE)
                    ttm_issuance = None

    ratio = (ttm_issuance / ttm_capex) if (ttm_issuance and ttm_capex) else None
    if not statuses:
        statuses.append(STATUS_OK)
    return IssuerView(entity.cik, entity.ticker_display, entity.bucket, ttm_capex,
                      ttm_issuance, ratio, statuses, n,
                      commitments.forward_commitments(entity.cik, indexed))


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
