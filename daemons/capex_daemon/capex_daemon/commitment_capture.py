"""P2 C2 — the commitments leg through the parser, because the API cannot see it.

companyfacts drops dimension-qualified facts (E6). Four issuers moved their
forward-commitment disclosure onto dimensioned tags, so the daemon's published
figure froze at whatever they last tagged undimensioned:

    AMZN   $32.41B at 2024Q2      live: $137.21B of leases not yet commenced,
                                        beside $130.07B of media/energy/software
    NVDA   $45.77B at 2025Q3      live: $279B of supply and capacity alone
    MU     $6.70B at 2023Q3       live: $5.5B goods/services/capital additions
    CLSK   $0.27B at 2024Q3       live: dimensioned, and REFUSED — see below

NVDA is the measure of the damage. The panel published $45.77B while the filing
said $279B of supply and capacity, $29B of cloud services, $36B of AI-cloud
agreements, $25B + $20B of datacenter leases not commenced, $8B of capital
expenditure obligations and $25B of equity investment commitments — six classes
in one note, which the concept name would have flattened into one number.

**A rule is a verified line-mapping, never a concept name (E23).** Each rule
names the concept, the axis and the member, and carries the accession it was
read in. Every rule below was read in the filing, and then re-read by a second
reader whose instruction was to refute it.

**Totals are never captured.** NVIDIA tags its $366B total as a sibling member
on the same axis as the components it contains; AMZN's $650.03B "Total
commitments" contains debt principal, interest and lease liabilities. Capturing
either alongside its components would double-count the note. Totals are recorded
with `is_total=True`, kept for reconciliation, and published only as context.

**A member name may not be trusted to identify a nested fact.** Microsoft's
$329.1B is one text node wrapped twice, once under `LeaseContractualTermAxis` =
OperatingLeaseMember and once = FinanceLeaseMember. The adversarial check found
the two contexts bound the OPPOSITE way round from the first reading — so a rule
that resolved "which wrapper is real" by member name would have picked the wrong
node. The rule therefore accepts EITHER member and collapses by value, and the
capture takes one figure per period end rather than summing.
"""
from . import commitment_basis, ixbrl

STATUS_CAPTURED = "CAPTURED"
STATUS_REFUSED = "CAPTURE-REFUSED"


class Rule:
    """One verified figure: what to match, what it means, where that was read."""

    __slots__ = ("ticker", "key", "cls", "concept", "axis", "members", "label",
                 "accession", "form", "filed", "quote", "is_total", "note",
                 "qualifiers", "cls_before", "reclass_from", "reclass_evidence")

    def __init__(self, ticker, key, cls, concept, axis="", members=(), label="",
                 accession="", form="", filed="", quote="", is_total=False, note="",
                 qualifiers=(), cls_before=None, reclass_from=None,
                 reclass_evidence=""):
        self.ticker = ticker
        self.key = key
        self.cls = cls
        self.qualifiers = tuple(qualifiers)
        # A DATED reclass: the same line meant something else before the issuer
        # re-worded it. Points before `reclass_from` carry `cls_before`.
        self.cls_before = cls_before
        self.reclass_from = reclass_from
        self.reclass_evidence = reclass_evidence
        self.concept = concept
        self.axis = axis
        self.members = (members,) if isinstance(members, str) else tuple(members)
        self.label = label
        self.accession = accession
        self.form = form
        self.filed = filed
        self.quote = quote
        self.is_total = is_total
        self.note = note

    @property
    def rule_key(self):
        return "{}:{}".format(self.ticker, self.key)

    @property
    def buildout(self):
        return self.cls in commitment_basis.BUILDOUT_CLASSES and not self.is_total

    def cls_at(self, period_end):
        """The class this line carried at `period_end` (dated reclass, Z1c)."""
        if self.reclass_from and self.cls_before and period_end < self.reclass_from:
            return self.cls_before
        return self.cls

    def json(self):
        return {"ticker": self.ticker, "key": self.key, "rule_key": self.rule_key,
                "class": self.cls, "concept": self.concept, "axis": self.axis,
                "members": list(self.members), "label": self.label,
                "accession": self.accession, "form": self.form, "filed": self.filed,
                "quote": self.quote, "is_total": self.is_total, "note": self.note,
                "buildout": self.buildout, "qualifiers": list(self.qualifiers),
                "cls_before": self.cls_before, "reclass_from": self.reclass_from,
                "reclass_evidence": self.reclass_evidence}

    def __repr__(self):
        return "Rule({} {})".format(self.rule_key, self.cls)


def _local(name):
    """`us-gaap:OtherCommitment` / `{ns}OtherCommitment` -> `OtherCommitment`."""
    return str(name or "").split(":")[-1].split("}")[-1]


def matches(rule, fact):
    """Does this parsed fact carry exactly the rule's concept and member?"""
    if _local(fact.concept) != rule.concept or fact.value is None:
        return False
    dims = {_local(a): _local(m) for a, m in (getattr(fact, "dims", None) or {}).items()}
    if not rule.axis:
        return not dims
    if dims.get(rule.axis) not in rule.members:
        return False
    # A second axis means a narrower cut of the same line (a legal entity, a
    # product); capturing it as the line itself would understate.
    return len(dims) == 1


def facts_for(rule, facts):
    """{period_end: value} for one rule, nested duplicates collapsed.

    One value at one period end under two contexts is ONE disclosure wrapped
    twice (Microsoft's lease figure), not two facts. Taking one rather than
    summing is the whole point: summing is the error this module exists to
    prevent.
    """
    out = {}
    for f in facts:
        if not matches(rule, f):
            continue
        if getattr(f, "period_start", None) is not None:   # commitments are instants
            continue
        pe = f.period_end
        if pe and (pe not in out or abs(f.value) > abs(out[pe])):
            out[pe] = f.value
    return out


def rules_for(ticker):
    return tuple(CAPTURE.get(ticker, ()))


def capture_from_facts(ticker, facts):
    """[(rule, {period_end: value})] for every rule this issuer has."""
    return [(r, facts_for(r, facts)) for r in rules_for(ticker)]


def refusal_for(ticker):
    return REFUSED.get(ticker)


# ---------------------------------------------------------------------------
# The verified rules. Each was read in the filing named on it, then re-read by
# a second reader instructed to refute it.
# ---------------------------------------------------------------------------

CAPTURE = {}
#: Issuers whose live disclosure is dimensioned and still cannot be captured,
#: with the reason. An issuer here keeps its frozen tagged figure and says why.
REFUSED = {}


def _register(*rules):
    for r in rules:
        CAPTURE.setdefault(r.ticker, []).append(r)


_UUPO = "UnrecordedUnconditionalPurchaseObligationBalanceSheetAmount"
_AMZN_AXIS = "UnrecordedUnconditionalPurchaseObligationByCategoryOfItemPurchasedAxis"
_NVDA_AXIS = "OtherCommitmentsAxis"

_register(
    # ---- AMZN — 10-Q 0001018724-26-000026, filed 2026-07-31 --------------
    # R1(a): the tagged line is content/energy/software; the buildout figure is
    # a second member of the same concept.
    Rule("AMZN", "leases-not-commenced", commitment_basis.LEASES_NOT_COMMENCED,
         _UUPO, _AMZN_AXIS, "OperatingAndFinanceLeasesLeaseNotYetCommencedMember",
         label="Leases not yet commenced",
         accession="0001018724-26-000026", form="10-Q", filed="2026-07-31",
         quote="$137,214,000,000 at 2026-06-30, on the leases-not-yet-commenced "
               "member of the purchase-obligation category axis"),
    Rule("AMZN", "content-energy-software", commitment_basis.MIXED_UNSEPARABLE,
         _UUPO, _AMZN_AXIS,
         "LongTermAgreementsToAcquireAndLicenseDigitalMediaContentProcureEnergy"
         "AndLicenseSoftwareMember",
         label="Long-term agreements to acquire and license digital media "
               "content, procure energy and license software",
         accession="0001018724-26-000026", form="10-Q", filed="2026-07-31",
         quote="$130,065,000,000 at 2026-06-30 — the line the panel froze at "
               "$32.41B in 2024Q2",
         note="Reclassified by Mando 2026-09-28 (Z1), DATED: AMZN added "
              "\"acquire property and equipment\" to this line's footnote (2) "
              "with the 10-Q for 2025-03-31 and never renamed the XBRL member, so "
              "from that quarter the line blends PP&E purchasing with content "
              "licensing and publishes no split. Earlier points keep "
              "CONTENT-ENERGY-SOFTWARE, which was exact for them. Neither class "
              "is buildout, so no published total turns on it.",
         cls_before=commitment_basis.CONTENT_ENERGY_SOFTWARE,
         reclass_from="2025-03-31",
         reclass_evidence="footnote (2) without PP&E through the FY2024 10-K "
                          "0001018724-25-000004; with PP&E from the 10-Q "
                          "0001018724-25-000036 (period 2025-03-31, filed "
                          "2025-05-02) onward. Hand-checked in all six filings "
                          "2024-06-30 .. 2025-09-30."),
    Rule("AMZN", "total-commitments", commitment_basis.NOT_A_COMMITMENT,
         "ContractualObligation", label="Total commitments",
         accession="0001018724-26-000026", form="10-Q", filed="2026-07-31",
         quote="$650,034,000,000 — contains debt principal and interest "
               "($220.31B), operating and finance lease liabilities and the two "
               "rows above", is_total=True,
         note="Kept for reconciliation only: 220,309 + 116,350 + 16,660 + "
              "11,070 + 137,214 + 130,065 + 18,366 = 650,034."),

    # ---- NVDA — 10-Q 0001045810-26-000075, filed 2026-08-26 --------------
    # Note 10 prints the table: Supply and capacity 279, Cloud service
    # agreements 29, Data center leases not commenced 25, Equity investments 25,
    # Capital expenditures 8, Total 366.
    Rule("NVDA", "supply-and-capacity", commitment_basis.SUPPLY,
         "OtherCommitment", _NVDA_AXIS, "SupplyAndCapacityCommitmentsMember",
         label="Supply and capacity",
         accession="0001045810-26-000075", form="10-Q", filed="2026-08-26",
         quote="\"increasing supply commitments from $119 billion last quarter "
               "to $279 billion as of July 26, 2026 … for our data center "
               "infrastructure systems, primarily memory and manufacturing "
               "facilities\"",
         note="The single largest forward-demand figure on the panel, and the "
              "panel published $45.77B for NVDA while it stood."),
    Rule("NVDA", "cloud-service-agreements", commitment_basis.SUPPLY,
         "OtherCommitment", _NVDA_AXIS, "CloudServiceAgreementCommitmentsMember",
         label="Cloud service agreements",
         accession="0001045810-26-000075", form="10-Q", filed="2026-08-26",
         quote="\"These commitments provide the cloud infrastructure to support "
               "our research and development\" — capacity purchased, the same "
               "shape as AMD's cloud-services commitments, which are SUPPLY"),
    Rule("NVDA", "ai-cloud-agreements", commitment_basis.SUPPLY,
         "OtherCommitment", _NVDA_AXIS, "AICloudPartnershipCommitmentsMember",
         label="AI cloud agreements",
         accession="0001045810-26-000075", form="10-Q", filed="2026-08-26",
         quote="\"if AI clouds do not successfully sell committed capacity to "
               "third-party customers, we have agreed to purchase that capacity\"",
         note="Ruled by Mando 2026-09-28 (Z1): stays SUPPLY with a CONTINGENT "
              "sub-tag. NVIDIA buys this capacity only if the AI cloud fails to "
              "sell it — a backstop, not a firm order — and the sub-tag travels "
              "with the figure so no reader mistakes one for the other.",
         qualifiers=(commitment_basis.QUALIFIER_CONTINGENT,)),
    Rule("NVDA", "dc-leases-not-commenced", commitment_basis.LEASES_NOT_COMMENCED,
         "OtherCommitment", _NVDA_AXIS, "DataCenterLeaseNotYetCommencedMember",
         label="Data center leases not commenced",
         accession="0001045810-26-000075", form="10-Q", filed="2026-08-26",
         quote="\"expected to begin between the third quarter of fiscal year "
               "2027 and fiscal year 2033 and have terms up to twenty years\"; "
               "maturity ladder 0+1+1+2+1+20 = 25"),
    Rule("NVDA", "dc-leases-third-party", commitment_basis.LEASES_NOT_COMMENCED,
         "OtherCommitment", _NVDA_AXIS,
         "DataCenterForThirdPartyLeaseNotYetCommencedMember",
         label="Data center leases not commenced, for third parties",
         accession="0001045810-26-000075", form="10-Q", filed="2026-08-26",
         quote="second commitments table, \"Additional Commitments\""),
    Rule("NVDA", "capex-obligations", commitment_basis.SUPPLY_CAPEX,
         "OtherCommitment", _NVDA_AXIS, "CapitalExpenditureObligationsMember",
         label="Capital expenditures",
         accession="0001045810-26-000075", form="10-Q", filed="2026-08-26",
         quote="\"obligations for data center equipment and infrastructure used "
               "for engineering and manufacturing operations\""),
    Rule("NVDA", "equity-investments", commitment_basis.NOT_A_COMMITMENT,
         "OtherCommitment", _NVDA_AXIS, "EquityInvestmentCommitmentsMember",
         label="Equity investments",
         accession="0001045810-26-000075", form="10-Q", filed="2026-08-26",
         quote="\"certain equity investments in AI model makers, infrastructure "
               "financiers, and other private companies, subject to certain "
               "contingencies\" — an investment, not a purchase"),
    Rule("NVDA", "total-first-table", commitment_basis.MIXED_UNSEPARABLE,
         "OtherCommitment", _NVDA_AXIS, "FuturePurchaseAndOtherCommitmentsMember",
         label="Total (first commitments table)",
         accession="0001045810-26-000075", form="10-Q", filed="2026-08-26",
         quote="279 + 29 + 25 + 25 + 8 = 366", is_total=True,
         note="A TOTAL tagged as a sibling member of its own components. "
              "Capturing it beside them would double the note."),
    Rule("NVDA", "total-second-table", commitment_basis.MIXED_UNSEPARABLE,
         "OtherCommitment", _NVDA_AXIS, "FutureAdditionalCommitmentsMember",
         label="Total (Additional Commitments table)",
         accession="0001045810-26-000075", form="10-Q", filed="2026-08-26",
         quote="36 + 20 = 56", is_total=True),

    # ---- MSFT — 10-K 0001193125-26-323660, filed 2026-07-29 -------------
    # The $194.06B purchase-commitment table is still untagged and still
    # publishes UNCOVERED-UNTAGGED. These are DIFFERENT figures in the same
    # filing, and they are tagged.
    Rule("MSFT", "leases-not-commenced", commitment_basis.LEASES_NOT_COMMENCED,
         _UUPO, "LeaseContractualTermAxis",
         ("OperatingLeaseMember", "FinanceLeaseMember"),
         label="Additional leases, primarily for datacenters, not yet commenced",
         accession="0001193125-26-323660", form="10-K", filed="2026-07-29",
         quote="$329,100,000,000 at 2026-06-30 — ONE text node wrapped twice, "
               "under both lease-term members, at the identical value",
         note="Accepts either member deliberately. The adversarial re-read found "
              "the two nested contexts bound the opposite way round from the "
              "first reading, so a rule keyed on one member name would resolve "
              "to the wrong node."),
    Rule("MSFT", "construction-commitments", commitment_basis.SUPPLY_CAPEX,
         "CommitmentsFairValueDisclosure", "PropertyPlantAndEquipmentByTypeAxis",
         "BuildingBuildingImprovementsAndLeaseholdImprovementsMember",
         label="Committed for the construction of buildings and building "
               "improvements",
         accession="0001193125-26-323660", form="10-K", filed="2026-07-29",
         quote="$34,600,000,000 at 2026-06-30; the filing's own contractual-"
               "obligations table shows construction commitments of $34,566M"),

    # ---- MU — 10-K 0000723125-25-000067 era, dimensioned ----------------
    Rule("MU", "goods-services-capital", commitment_basis.SUPPLY_CAPEX,
         _UUPO, _AMZN_AXIS, "GoodsServicesAndCapitalAdditionsMember",
         label="Noncancelable commitments for goods, services and capital additions",
         accession="0000723125-25-000067", form="10-K", filed="2025-10-01",
         quote="the concept the panel froze on at $6.7B in 2023, now carried "
               "under a category member companyfacts drops"),
    Rule("MU", "leases-not-commenced", commitment_basis.LEASES_NOT_COMMENCED,
         _UUPO, _AMZN_AXIS, "FinancingLeaseLeaseNotYetCommencedMember",
         label="Obligations for leases executed but not yet commenced",
         accession="0000723125-25-000067", form="10-K", filed="2025-10-01",
         quote="$1.16B at 2025-08-28, $1.13B at 2025-11-27"),

    # ---- IRM — 10-K 0001020569-26-000013, filed 2026-02-12 -------------
    # R1(a): IRM's buildout figure sits beside the tagged purchase-commitments
    # table, on its own member.
    Rule("IRM", "construction-costs", commitment_basis.SUPPLY_CAPEX,
         "OtherCommitment", _NVDA_AXIS, "ConstructionCostsMember",
         label="Contractual commitments for future construction costs",
         accession="0001020569-26-000013", form="10-K", filed="2026-02-12",
         quote="$1,085,725,000 at 2025-12-31 — the datacenter construction "
               "commitments R1(a) ruled IRM's buildout row",
         note="IRM's tagged PURCHASE COMMITMENTS total ($234.0M) is software "
              "maintenance and support, and is annotative. The FY2022 10-K "
              "carried construction inside that total; from FY2023 it was "
              "lifted out, which is why the tagged line fell 72% with no real "
              "decline behind it."),
)

REFUSED["CLSK"] = (
    "CLSK's class split exists only under `ContractualObligationDueInNextTwelveMonths`, "
    "and the filing uses that one concept for its whole multi-year table, cutting it by "
    "`AwardDateAxis` into FY2026/FY2027/FY2028 columns. Capturing a twelve-month concept "
    "as a commitment STOCK would place a one-year figure beside other issuers' total "
    "stocks inside the same class — the precise error the classes exist to prevent. "
    "CLSK keeps its frozen tagged figure and this reason until the tagging is "
    "internally consistent or a rule is ruled for it.")


# ---------------------------------------------------------------------------
# Fetching and caching — same shape as the supplier leg's harvest, and for the
# same reason: only instances absent from the cache are fetched, so a quiet
# night costs one submissions request per captured issuer.
# ---------------------------------------------------------------------------

INSTANCE_LIMIT = 14


def harvest(entity, con, http=None, limit=INSTANCE_LIMIT, submissions_doc=None,
            fetch=None):
    """Fetch and store any captured commitment facts filed since last time.

    Returns (instances_added, failures). An instance that yields nothing is
    still recorded, so an issuer with no matching member is not re-fetched every
    night forever.
    """
    from . import edgar, suppliers
    ticker = entity.ticker_display
    if not rules_for(ticker):
        return 0, []
    seen = {r[0] for r in con.execute(
        "SELECT instance_key FROM commitment_capture_instances WHERE cik=?",
        (entity.cik,))}
    added, failures = 0, []
    for report_date, accession, docname in suppliers.instance_index(
            entity.cik, http, limit=limit, submissions_doc=submissions_doc):
        if report_date in seen:
            continue
        try:
            blob = (fetch or edgar.fetch_document)(
                entity.cik, accession, docname, http=http)
            facts = blob if isinstance(blob, list) else ixbrl.parse_instance(blob)
        except Exception as exc:
            failures.append((report_date, str(exc)))
            continue
        n = 0
        for rule, points in capture_from_facts(ticker, facts):
            for period_end, value in points.items():
                con.execute(
                    "INSERT OR REPLACE INTO commitment_capture_facts(cik, "
                    "instance_key, rule_key, period_end, value, accession) "
                    "VALUES (?,?,?,?,?,?)",
                    (entity.cik, report_date, rule.rule_key, period_end, value,
                     accession))
                n += 1
        con.execute(
            "INSERT OR REPLACE INTO commitment_capture_instances(cik, "
            "instance_key, facts, accession) VALUES (?,?,?,?)",
            (entity.cik, report_date, n, accession))
        added += 1
    con.commit()
    return added, failures


def rows_from_db(entity_or_ticker, cik, con):
    """[{rule, points}] rebuilt from cache — no network, no re-parsing.

    A period end reported by several filings takes the NEWEST filing's value
    (E-26: filers file it wrong, and the newest filing supersedes).
    """
    ticker = getattr(entity_or_ticker, "ticker_display", entity_or_ticker)
    rules = {r.rule_key: r for r in rules_for(ticker)}
    if not rules:
        return []
    best = {}
    for instance_key, rule_key, period_end, value, accession in con.execute(
            "SELECT instance_key, rule_key, period_end, value, accession FROM "
            "commitment_capture_facts WHERE cik=? ORDER BY instance_key", (cik,)):
        if rule_key in rules:
            best[(rule_key, period_end)] = (value, accession, instance_key)
    out = []
    for rule_key, rule in rules.items():
        pts = {pe: v for (rk, pe), (v, _a, _i) in best.items() if rk == rule_key}
        if pts:
            out.append({"rule": rule, "points": pts})
    return out
