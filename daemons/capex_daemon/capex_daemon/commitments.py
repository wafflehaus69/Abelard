"""C2/C3 — forward-spend series: purchase obligations, and RIOT-class deposits.

Contracted-but-unspent obligations lead reported capex, and for the largest
spenders they are enormous: Meta's purchase obligations ran $34.1B to $279.0B
across six quarters. This is the closest thing in structured data to the
"announced rather than reported" variable the guidance leg was wanted for.

Coverage is per-issuer and published as a status, never as a zero:

  COVERED             a structured series exists
  UNCOVERED-UNTAGGED  the issuer discloses the figure and does not XBRL-tag it.
                      MSFT's $194.06B datacenter purchase-commitment table is the
                      case that named this status — it carries no ix:nonFraction
                      at all, so it is disclosed-dark by ruling and stays that way.
  ABSENT              no such disclosure found

C3 — equipment deposits. RIOT presents "Deposits on equipment" as a cash-flow
line distinct from capex, tagged `PaymentsToAcquireMachineryAndEquipment`. It is
cash advanced for equipment not yet delivered, so it leads capitalization — and
it must never be summed into capex, which would double-count on delivery. HUT
tags the SAME concept to its purchases line, which is why this series is keyed
per issuer from a verified line-mapping rather than from the concept name (E23).
"""
from . import facts_api, normalize

STATUS_COVERED = "COVERED"
STATUS_UNCOVERED_UNTAGGED = "UNCOVERED-UNTAGGED"
STATUS_ABSENT = "ABSENT"

# Ordered by preference: the consolidated total first, then the ladder members.
COMMITMENT_CONCEPTS = (
    "UnrecordedUnconditionalPurchaseObligationBalanceSheetAmount",
    "ContractualObligation",
    "PurchaseObligation",
    "LongTermPurchaseCommitmentAmount",
)

# Issuers that disclose a material forward-commitment figure with NO XBRL tag.
# Ruled disclosed-dark; the series publishes the status, never a zero (E1).
UNTAGGED_DISCLOSURES = {
    "0000789019": (
        "MSFT datacenter purchase-commitment table, $194.06B, carries no "
        "ix:nonFraction in the 10-K and no R-file renders it"),
}

# C3 — verified line-mappings. Concept name alone is NOT sufficient (E23).
DEPOSIT_LINE_MAPPING = {
    "0001167419": ("PaymentsToAcquireMachineryAndEquipment", "Deposits on equipment"),
}


class CommitmentPoint:
    __slots__ = ("period_end", "value", "concept", "unit")

    def __init__(self, period_end, value, concept, unit):
        self.period_end = period_end
        self.value = value
        self.concept = concept
        self.unit = unit

    def __repr__(self):
        return "CommitmentPoint({} {})".format(self.period_end, self.value)


class CommitmentSeries:
    def __init__(self, cik, status, points, concept, detail):
        self.cik = cik
        self.status = status
        self.points = points
        self.concept = concept
        self.detail = detail

    @property
    def latest(self):
        return self.points[-1] if self.points else None

    def growth(self, n=4):
        """Change over the last n observations, or None."""
        if len(self.points) < n + 1:
            return None
        old, new = self.points[-(n + 1)].value, self.points[-1].value
        return (new / old - 1) if old else None

    def __repr__(self):
        return "CommitmentSeries({} {} n={})".format(self.cik, self.status, len(self.points))


def _instants(indexed, concept, unit="USD"):
    best = {}
    for f in indexed.get(concept, []):
        if f.unit != unit or f.period_start is not None or f.value is None:
            continue
        cur = best.get(f.period_end)
        if cur is None or (f.filed or "") >= (cur.filed or ""):
            best[f.period_end] = f
    return best


def forward_commitments(cik, indexed, unit="USD"):
    """C2 — the forward-spend series for one issuer, with coverage status."""
    best_concept, best_points = None, []
    for concept in COMMITMENT_CONCEPTS:
        rows = _instants(indexed, concept, unit)
        if len(rows) > len(best_points):
            best_concept, best_points = concept, rows
    if best_points:
        pts = [CommitmentPoint(k, v.value, best_concept, unit)
               for k, v in sorted(best_points.items())]
        return CommitmentSeries(cik, STATUS_COVERED, pts, best_concept,
                                "{} observations".format(len(pts)))
    if cik in UNTAGGED_DISCLOSURES:
        return CommitmentSeries(cik, STATUS_UNCOVERED_UNTAGGED, [], None,
                                UNTAGGED_DISCLOSURES[cik])
    return CommitmentSeries(cik, STATUS_ABSENT, [], None,
                            "no purchase-obligation concept present")


def equipment_deposits(cik, indexed, unit="USD"):
    """C3 — the deposits leading indicator, only where the line is verified.

    Returns None when this issuer has no verified deposits mapping. Absence of a
    mapping is not absence of the concept: HUT carries the same concept as its
    capex line, and reading it as deposits would understate its capex ~30x.
    """
    mapping = DEPOSIT_LINE_MAPPING.get(cik)
    if not mapping:
        return None
    concept, label = mapping
    rows = [f for f in indexed.get(concept, []) if f.unit == unit and f.period_start]
    if not rows:
        return None
    pairs = [(f, concept) for f in facts_api.dedupe_latest_filed(rows)]
    quarters = normalize.discrete_quarters(pairs)
    return {"cik": cik, "concept": concept, "line_label": label, "rows": quarters}
