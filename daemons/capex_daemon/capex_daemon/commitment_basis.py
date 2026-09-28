"""GAP2 P2 — what each forward-commitment figure actually CONTAINS.

**Ratified by Mando 2026-09-22 (ruling R1 on CD-GAP2-P2-VERIFY).** Every class
below was assigned by reading the filing behind the issuer's latest tagged
figure, not by reading the concept name — E23's largest citation to date. The
XBRL concept predicts nothing: `UnrecordedUnconditionalPurchaseObligation...`
alone carries supply commitments, supply including capex, leases not yet
commenced, lessor lease *receipts*, prepaid rent received, and a recorded
liability. `ContractualObligation` carries Meta's $349.31B of supply and a 2016
royalty from a predecessor business.

The axis that matters is not which concept was used. It is **is this figure
about the buildout at all?**

  SUPPLY                   unconditional purchase, supply or capacity obligations
  SUPPLY+CAPEX             as above, explicitly including PP&E or datacenter build
  LEASES-NOT-COMMENCED     signed leases not yet started
  CONTENT-ENERGY-SOFTWARE  real commitments, not infrastructure
  MIXED-UNSEPARABLE        several classes in one total, not separable from it
  NOT-A-COMMITMENT         receipts, prepaid rent, debt, liabilities, royalties, SAFEs
  UNCLASSIFIED             discloses, not yet presentation-verified

**Only the first three feed anything.** Hayes Leg 3, the Brief's since-page and
every commitment alert read `BUILDOUT_CLASSES` and nothing else.
NOT-A-COMMITMENT is excluded everywhere; CONTENT-ENERGY-SOFTWARE publishes in an
annotative table that is explicitly not part of the buildout;
MIXED-UNSEPARABLE publishes with its reason and joins no total.

**UNCLASSIFIED is the default, and it is not buildout.** A newly admitted issuer
that starts tagging a commitment concept does not enter Leg 3 by existing. It
enters when someone reads its filing.

**Two rows per issuer, where the tagged figure is not the buildout figure.**
AMZN and IRM both tag a non-buildout total and disclose their buildout figure
*beside* it, untagged by that concept. The class travels with the row, not with
the issuer: AMZN's tagged line is media content, energy and software
(annotative), while AMZN's buildout row is $137.2B of leases not yet commenced —
which is where it belongs, beside Microsoft's not-yet-commenced leases rather
than in a supply total. `BUILDOUT_ROWS` declares those; `commitment_capture`
fills them by presentation (C2).
"""

SUPPLY = "SUPPLY"
SUPPLY_CAPEX = "SUPPLY+CAPEX"
LEASES_NOT_COMMENCED = "LEASES-NOT-COMMENCED"
CONTENT_ENERGY_SOFTWARE = "CONTENT-ENERGY-SOFTWARE"
MIXED_UNSEPARABLE = "MIXED-UNSEPARABLE"
NOT_A_COMMITMENT = "NOT-A-COMMITMENT"
UNCLASSIFIED = "UNCLASSIFIED"

#: The only classes that feed Leg 3, the since-page and alerts (R1).
BUILDOUT_CLASSES = (SUPPLY, SUPPLY_CAPEX, LEASES_NOT_COMMENCED)
#: Published, annotated, and explicitly not part of the buildout.
ANNOTATIVE_CLASSES = (CONTENT_ENERGY_SOFTWARE,)
#: Published with a reason, joins no total.
WITHHELD_CLASSES = (MIXED_UNSEPARABLE, UNCLASSIFIED)
#: Removed from the deltas, the since-page and Leg 3. Listed only as excluded.
EXCLUDED_CLASSES = (NOT_A_COMMITMENT,)

CLASS_ORDER = (SUPPLY, SUPPLY_CAPEX, LEASES_NOT_COMMENCED,
               CONTENT_ENERGY_SOFTWARE, MIXED_UNSEPARABLE, UNCLASSIFIED,
               NOT_A_COMMITMENT)

CLASS_MEANING = {
    SUPPLY: "unconditional purchase, supply or capacity obligations",
    SUPPLY_CAPEX: "supply obligations that explicitly include PP&E or datacenter build",
    LEASES_NOT_COMMENCED: "signed leases that have not yet commenced",
    CONTENT_ENERGY_SOFTWARE: "real commitments, unrelated to the buildout",
    MIXED_UNSEPARABLE: "several classes in one total, not separable from it",
    UNCLASSIFIED: "discloses a figure; its contents have not been read yet",
    NOT_A_COMMITMENT: "not a forward commitment at all",
}


class Basis:
    """One verified figure: its class, what it contains, and where that was read."""

    __slots__ = ("ticker", "cls", "contains", "line_label", "accession",
                 "as_of", "form", "filed", "note")

    def __init__(self, ticker, cls, contains, line_label="", accession="",
                 as_of="", form="", filed="", note=""):
        self.ticker = ticker
        self.cls = cls
        self.contains = contains
        self.line_label = line_label
        self.accession = accession
        self.as_of = as_of
        self.form = form
        self.filed = filed
        self.note = note

    @property
    def buildout(self):
        return self.cls in BUILDOUT_CLASSES

    def json(self):
        return {"ticker": self.ticker, "class": self.cls, "contains": self.contains,
                "line_label": self.line_label, "accession": self.accession,
                "as_of": self.as_of, "form": self.form, "filed": self.filed,
                "note": self.note, "buildout": self.buildout}

    def __repr__(self):
        return "Basis({} {} @{})".format(self.ticker, self.cls, self.as_of)


def _b(ticker, cls, contains, **kw):
    return Basis(ticker, cls, contains, **kw)


#: The class of the figure the daemon publishes TODAY — the tagged series.
#: Keyed by display ticker. Every entry was read in the filing named here.
TAGGED_BASIS = {r.ticker: r for r in [
    # ---- SUPPLY ---------------------------------------------------------
    _b("AVGO", SUPPLY, "unconditional purchase commitments, primarily inventory",
       line_label="Total (Purchase Commitments column)",
       accession="0001730168-26-000080", as_of="2026-08-02", form="10-Q", filed="2026-09-10"),
    _b("SMCI", SUPPLY, "non-cancelable commitments to buy inventory and non-inventory items",
       line_label="Purchase Commitments (prose, not a table)",
       accession="0001375365-26-000022", as_of="2026-06-30", form="10-K", filed="2026-08-31"),
    _b("AMD", SUPPLY, "unconditional commitments: wafer/capacity supply and cloud services",
       line_label="Unconditional commitments (Total column)",
       accession="0000002488-26-000123", as_of="2026-06-27", form="10-Q", filed="2026-08-05"),
    # ---- SUPPLY+CAPEX ---------------------------------------------------
    _b("META", SUPPLY_CAPEX, "non-cancelable supply, datacenter construction and cloud; "
       "excludes debt and all leases",
       line_label="non-cancelable contractual commitments (narrative total)",
       accession="0001628280-26-050705", as_of="2026-06-30", form="10-Q", filed="2026-07-30",
       note="Despite the ContractualObligation concept, this is not a contractual-"
            "obligations total: debt and leases are stated separately."),
    _b("NVDA", SUPPLY_CAPEX, "inventory and component supply, long-lived assets (PP&E) "
       "and multi-year cloud service agreements",
       line_label="Total (Commitments column)",
       accession="0001045810-25-000209", as_of="2025-07-27", form="10-Q", filed="2025-08-27",
       note="Frozen: NVDA moved to dimensional tags, which companyfacts drops."),
    _b("ORCL", SUPPLY_CAPEX, "unconditional obligations incl. datacenter capacity and equipment",
       line_label="Total (Unconditional Obligations maturity table)",
       accession="0001193125-26-389274", as_of="2026-08-31", form="10-Q", filed="2026-09-11"),
    _b("MU", SUPPLY_CAPEX, "noncancelable commitments explicitly including PP&E acquisition; "
       "excludes debt and leases",
       line_label="noncancelable commitments with remaining terms in excess of one year",
       accession="0000723125-23-000054", as_of="2023-08-31", form="10-K", filed="2023-10-06",
       note="Frozen 11 fiscal quarters: MU moved to dimensional tags."),
    _b("CLSK", SUPPLY_CAPEX, "entirely capex: miner purchases, modular immersion datacenters "
       "(purchase, construction and installation)",
       line_label="Total",
       accession="0000950170-24-132565", as_of="2024-09-30", form="10-K", filed="2024-12-03",
       note="Frozen 7 quarters: CLSK moved to dimensional tags."),
    _b("FRMI", SUPPLY_CAPEX, "unconditional purchase obligations incl. datacenter equipment",
       line_label="Total unconditional purchase obligations (Note 8)",
       accession="0002071778-26-000051", as_of="2026-06-30", form="10-Q", filed="2026-08-14"),
    _b("CIFR", SUPPLY_CAPEX, "open purchase commitments, largely Bitmain miners",
       line_label="Total (Open Purchase Commitment column)",
       accession="0000950170-24-025442", as_of="2023-12-31", form="10-K", filed="2024-03-05"),
    _b("MARA", SUPPLY_CAPEX, "remaining unpaid amounts under equipment purchase agreements "
       "for mining hardware (capitalized on delivery)",
       line_label="remaining commitments of approximately $64.6 million",
       accession="0001507605-26-000022", as_of="2026-06-30", form="10-Q", filed="2026-08-06"),
    _b("HUT", SUPPLY_CAPEX, "capital-equipment purchase orders (AI equipment)",
       line_label="Purchase agreements (narrative paragraph)",
       accession="0001558370-24-011968", as_of="2023-10-31", form="10-Q", filed="2024-08-13",
       note="Not a clean unconditional-purchase total; every dollar is capex."),
    # ---- LEASES-NOT-COMMENCED -------------------------------------------
    _b("EQIX", LEASES_NOT_COMMENCED, "lease commitments that have not yet commenced "
       "(operating and finance)",
       line_label="total lease commitments of approximately $708 million",
       accession="0001101239-26-000147", as_of="2026-06-30", form="10-Q", filed="2026-07-29"),
    _b("SNOW", LEASES_NOT_COMMENCED, "office operating leases signed but not yet commenced",
       line_label="committed $77.6 million for leases signed but not yet commenced",
       accession="0001640147-26-000037", as_of="2026-07-31", form="10-Q", filed="2026-09-04",
       note="OFFICES, not datacenters. In the class, and not a datacenter signal."),
    _b("APLD", LEASES_NOT_COMMENCED, "executed leases not yet commenced (datacenter)",
       line_label="leases which are executed but not yet commenced",
       accession="0001144879-25-000069", as_of="2025-08-31", form="10-Q", filed="2025-10-09"),
    _b("IREN", LEASES_NOT_COMMENCED, "total lease payments under GPU lease financing",
       line_label="Total lease payments under these agreements (PP&E note)",
       accession="0001878848-25-000081", as_of="2025-09-30", form="10-Q", filed="2025-11-06"),
    # ---- CONTENT-ENERGY-SOFTWARE (annotative; never Leg 3) ---------------
    _b("AMZN", CONTENT_ENERGY_SOFTWARE, "digital media content, energy procurement and "
       "software licensing",
       line_label="Unconditional purchase obligations",
       accession="0001018724-24-000130", as_of="2024-06-30", form="10-Q", filed="2024-08-02",
       note="R1(a): AMZN's BUILDOUT row is a separate fact — leases not yet commenced. "
            "The tagged line is frozen at 2024Q2 because the live figure is "
            "dimension-tagged (…DigitalMediaContentProcureEnergyAndLicenseSoftwareMember, "
            "$130.065B at 2026-06-30), which companyfacts drops. "
            "RE-OPENED 2026-09-23: AMZN added \"acquire property and equipment\" to "
            "this line's footnote after the 2024Q2 figure R1(a) was ruled on, and did "
            "not rename the member. MIXED-UNSEPARABLE on the live evidence; held here "
            "at the ruled class because both classes are non-buildout and no published "
            "number turns on it. See commitment_capture.CAPTURE['AMZN']."),
    _b("GOOGL", CONTENT_ENERGY_SOFTWARE, "content licensing agreements with fixed or "
       "minimum guaranteed commitments",
       line_label="certain content licensing agreements … of $7.7 billion (Note 10)",
       accession="0001652044-26-000018", as_of="2025-12-31", form="10-K", filed="2026-02-05",
       note="Confirmed at the tag itself: the concept sits on that one sentence."),
    _b("IRM", CONTENT_ENERGY_SOFTWARE, "purchase commitments, mostly software maintenance "
       "and support",
       line_label="PURCHASE COMMITMENTS(1) — column total",
       accession="0001020569-26-000013", as_of="2025-12-31", form="10-K", filed="2026-02-12",
       note="R1(a): IRM's BUILDOUT row is a separate figure — datacenter construction "
            "commitments, disclosed beside this one and untagged by this concept."),
    # ---- MIXED-UNSEPARABLE ----------------------------------------------
    _b("SPCX", MIXED_UNSEPARABLE, "capex-bearing launch/datacenter obligations, supply, "
       "cloud services AND a spectrum-license acquisition, in one total",
       line_label="Total",
       accession="0001628280-26-052535", as_of="2026-06-30", form="10-Q", filed="2026-08-04",
       note="R1(b): excluded from buildout totals until the spectrum-license component "
            "is separable from the total. It is disclosed, not separable — so it is "
            "published with the reason and joins nothing."),
    # ---- NOT-A-COMMITMENT (excluded everywhere) -------------------------
    _b("CORZ", NOT_A_COMMITMENT, "lease payments the company EXPECTS TO RECEIVE as lessor",
       line_label="Operating lease payments expected to be received …",
       accession="0001839341-26-000014", as_of="2026-06-30", form="10-Q", filed="2026-07-28",
       note="Cash inflow tagged under a purchase-obligation concept. Confirmed by hand."),
    _b("WULF", NOT_A_COMMITMENT, "prepaid rent RECEIVED (lessor accounting)",
       line_label="Prepaid Rent",
       accession="0001083301-26-000166", as_of="2026-06-30", form="10-Q", filed="2026-08-05",
       note="Mis-tag: the fact sits in the lessor prose of the Leases note."),
    _b("KEEL", NOT_A_COMMITMENT, "long-term debt principal (~97.5%) plus lease payments, "
       "from a liquidity-risk maturity table",
       line_label="unlabeled sum row under 'Long-term debt' and 'Leases'",
       accession="0001812477-26-000023", as_of="2026-06-30", form="10-Q", filed="2026-08-10",
       note="R1(c): routed to the credit-leg consistency check — if it is debt "
            "outstanding it belongs in the debt stock and nowhere else."),
    _b("CCOI", NOT_A_COMMITMENT, "a RECORDED liability: installment payment agreement, "
       "current portion",
       line_label="Installment payment agreement, current portion",
       accession="0001104659-22-056265", as_of="2022-03-31", form="10-Q", filed="2022-05-05"),
    _b("RIOT", NOT_A_COMMITMENT, "minimum annual royalty payments of a predecessor business",
       line_label="Minimum annual royalty payments per year",
       accession="0001079973-17-000206", as_of="2016-12-31", form="10-K", filed="2017-03-31",
       note="A 2016 fact from Bioptix, the shell RIOT was before mining."),
    _b("WYFI", NOT_A_COMMITMENT, "a milestone-contingent commitment to fund an equity "
       "investment (a SAFE)",
       line_label="may be obligated to invest up to an additional $2 million",
       accession="0001213900-26-088026", as_of="2024-06-30", form="10-Q", filed="2026-08-12"),
]}

#: Issuers whose BUILDOUT figure is a different fact from the one they tag with
#: a commitment concept. Declared here, captured by presentation in
#: `commitment_capture` (C2) — declaring it is not capturing it.
BUILDOUT_ROWS = {
    "AMZN": _b("AMZN", LEASES_NOT_COMMENCED,
               "leases signed but not yet commenced (datacenters and other)",
               line_label="leases that have not yet commenced",
               note="R1(a). Sits beside the tagged media/energy/software line."),
    "IRM": _b("IRM", SUPPLY_CAPEX,
              "datacenter construction commitments",
              line_label="data center construction commitments",
              note="R1(a). Disclosed separately from the tagged purchase-commitments "
                   "table, and untagged by that concept."),
}


def basis_for(ticker):
    """The class of the figure published for `ticker` today. Never None."""
    b = TAGGED_BASIS.get(ticker)
    if b is not None:
        return b
    return Basis(ticker, UNCLASSIFIED, "not yet read", note="no presentation check on record")


def class_of(ticker):
    return basis_for(ticker).cls


def is_buildout(ticker):
    """Does this issuer's PUBLISHED series feed Leg 3, the since-page and alerts?"""
    return class_of(ticker) in BUILDOUT_CLASSES


def is_excluded(ticker):
    """NOT-A-COMMITMENT: removed from the deltas, the since-page and Leg 3."""
    return class_of(ticker) in EXCLUDED_CLASSES


def buildout_row(ticker):
    """The separately-disclosed buildout figure for AMZN/IRM, or None."""
    return BUILDOUT_ROWS.get(ticker)


def classified_tickers(cls):
    return sorted(t for t, b in TAGGED_BASIS.items() if b.cls == cls)


def group(tickers):
    """{class: [ticker, …]} in CLASS_ORDER, for a renderer that ranks within a class."""
    out = {}
    for t in tickers:
        out.setdefault(class_of(t), []).append(t)
    return {c: sorted(out[c]) for c in CLASS_ORDER if c in out}


def table():
    """Every ratified row, for the verify doc and the dashboard's basis column."""
    rows = [b.json() for b in TAGGED_BASIS.values()]
    for b in BUILDOUT_ROWS.values():
        r = b.json()
        r["row"] = "buildout"
        rows.append(r)
    for r in rows:
        r.setdefault("row", "tagged")
    return sorted(rows, key=lambda r: (CLASS_ORDER.index(r["class"]), r["ticker"]))
