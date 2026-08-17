"""CD-G3 — TTM anchor reconciliation. Reports, never corrects (E1, E5, R2a).

Capital deployed over a trailing year should show up as movement in the gross
carrying value of what was bought. It does — but only at TTM. Measured quarterly
residuals ran +-20% for MSFT and META and as wide as -694% for RIOT, because
disposals, retirements, impairments, acquisitions and paid-vs-placed-in-service
timing all land inside a quarter. Over four quarters they largely wash: observed
ratios were MSFT 0.98x, META 1.01x, RIOT 0.98x, ORCL 0.91x, EQIX 0.73x.

So this is an **order-of-magnitude bound, not a precision bound**. Its job is
catching the plausible-stale-resolution class — AMZN read 23x low from a tag
abandoned in 2017 — not adjudicating a 20% gap. A ratio inside 0.5x-2.0x is
silence; outside it is a flag for a human, never an adjustment.

Deployment must include finance-lease additions. Microsoft's lease additions run
to $9.15B in a single quarter and Meta's anchor concept bundles finance-lease
right-of-use assets by construction, so reconciling cash capex alone guarantees
a false failure on the two largest names.
"""
from datetime import date

from . import config, normalize, tagmap

VERDICT_RECONCILED = "RECONCILED"
VERDICT_FLAGGED = "FLAGGED"
VERDICT_UNANCHORED = "UNANCHORED"
VERDICT_INSUFFICIENT = "INSUFFICIENT-HISTORY"

LEASE_CONCEPT = "RightOfUseAssetObtainedInExchangeForFinanceLeaseLiability"


class AnchorCheck:
    __slots__ = ("cik", "window_start", "window_end", "deployed", "delta_anchor",
                 "ratio", "verdict", "anchor_concept", "detail")

    def __init__(self, cik, window_start, window_end, deployed, delta_anchor,
                 ratio, verdict, anchor_concept, detail):
        self.cik = cik
        self.window_start = window_start
        self.window_end = window_end
        self.deployed = deployed
        self.delta_anchor = delta_anchor
        self.ratio = ratio
        self.verdict = verdict
        self.anchor_concept = anchor_concept
        self.detail = detail

    def __repr__(self):
        return "AnchorCheck({} {} ratio={})".format(self.cik, self.verdict, self.ratio)


def _instants(indexed, concept, unit="USD"):
    """{period_end: value} for instant facts, latest filed wins."""
    best = {}
    for f in indexed.get(concept, []):
        if f.unit != unit or f.period_start is not None or f.value is None:
            continue
        cur = best.get(f.period_end)
        if cur is None or (f.filed or "") >= (cur.filed or ""):
            best[f.period_end] = f
    return {k: v.value for k, v in best.items()}


def deployment_rows(indexed, capex_resolution, unit="USD"):
    """Discrete-quarter capital deployed = cash capex + finance-lease additions."""
    from . import facts_api
    capex = normalize.discrete_quarters(tagmap.series_facts(indexed, capex_resolution))
    lease_facts = [f for f in indexed.get(LEASE_CONCEPT, [])
                   if f.unit == unit and f.period_start]
    lease = {}
    if lease_facts:
        pairs = [(f, LEASE_CONCEPT) for f in facts_api.dedupe_latest_filed(lease_facts)]
        lease = {r.period_end: r.value for r in normalize.discrete_quarters(pairs)}
    for r in capex:
        r.value = r.value + lease.get(r.period_end, 0.0)
    return capex, bool(lease_facts)


def reconcile(cik, indexed, capex_resolution, anchor_resolution, unit="USD"):
    """Run CD-G3 for one issuer over its most recent TTM window."""
    if anchor_resolution.is_unresolved or anchor_resolution.is_multi_line:
        why = ("no gross-basis anchor concept present"
               if anchor_resolution.is_unresolved
               else "anchor concepts disagree near the frontier: {}".format(
                   ", ".join(anchor_resolution.multi_line_concepts)))
        return AnchorCheck(cik, None, None, None, None, None,
                           VERDICT_UNANCHORED, None, why)

    rows, _ = deployment_rows(indexed, capex_resolution, unit)
    window = normalize.ttm_window(rows)
    if window is None:
        return AnchorCheck(cik, None, None, None, None, None, VERDICT_INSUFFICIENT,
                           anchor_resolution.current_concept,
                           "fewer than {} discrete quarters".format(
                               config.ANCHOR_WINDOW_QUARTERS))
    deployed = normalize.ttm(rows)
    sel = rows[-config.ANCHOR_WINDOW_QUARTERS:]
    start_end, end_end = sel[0].period_start, sel[-1].period_end

    concept = anchor_resolution.concept_for(end_end)
    instants = _instants(indexed, concept, unit)
    opening = _nearest_instant(instants, start_end)
    closing = _nearest_instant(instants, end_end)
    if opening is None or closing is None:
        return AnchorCheck(cik, start_end, end_end, deployed, None, None,
                           VERDICT_UNANCHORED, concept,
                           "anchor lacks an instant at one or both window ends "
                           "(opening={}, closing={})".format(opening, closing))
    delta = closing[1] - opening[1]
    if delta == 0:
        return AnchorCheck(cik, start_end, end_end, deployed, delta, None,
                           VERDICT_UNANCHORED, concept,
                           "anchor did not move across the window; ratio undefined")
    ratio = deployed / delta
    lo, hi = config.ANCHOR_BAND
    verdict = VERDICT_RECONCILED if lo <= ratio <= hi else VERDICT_FLAGGED
    return AnchorCheck(cik, start_end, end_end, deployed, delta, ratio, verdict, concept,
                       "deployed {:,.0f} vs anchor movement {:,.0f} over {}..{}".format(
                           deployed, delta, opening[0], closing[0]))


def _nearest_instant(instants, target, tolerance_days=20):
    """Closest instant to a target date, within tolerance. (date, value) or None."""
    if not instants:
        return None
    t = date.fromisoformat(target)
    best = None
    for k, v in instants.items():
        try:
            d = abs((date.fromisoformat(k) - t).days)
        except ValueError:
            continue
        if d <= tolerance_days and (best is None or d < best[0]):
            best = (d, k, v)
    return (best[1], best[2]) if best else None
