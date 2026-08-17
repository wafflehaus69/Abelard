"""Leg C — event-time debt issuance from the EX-FILING FEES XBRL exhibit.

The prospectus body is prose. Its filing-fee exhibit is not: it is a clean XBRL
instance in the `ffd` taxonomy, filed the day after pricing, carrying one context
per tranche via the typed dimension `ffd:OfferingAxis` -> `dei:lineNo`. That
makes debt issuance **event-time and structured** rather than quarterly and
lagged — the single most useful thing CD-R1 found on the credit side.

Five traps, all measured in CD-R1 and all handled here:

1. **Preliminary prospectuses carry blank amounts.** A 424B2 marked "SUBJECT TO
   COMPLETION" prints `$    ` where the principal will go. `ffd:FnlPrspctsFlg`
   discriminates; a preliminary is skipped, never parsed for a total.
2. **Sum of tranches does not equal the stated total.** Meta's six tranches sum
   to exactly $25,000,000,000 against `TtlOfferingAmt` 24,967,390,000 — the
   difference is offering discount. Both are recorded; neither is silently
   preferred, and they are never reconciled away.
3. **Exchange offers are not new money.** MSFT's recent 424B3s state the company
   "will not receive any cash proceeds". Registering securities is not raising
   cash, so a fee exhibit alone cannot tell you money moved.
4. **8-K item codes are unreliable** for discovery — Meta's debt 8-K was filed
   under items 8.01/9.01, not 1.01/2.03.
5. **Use-of-proceeds is not attributable.** MSFT and META are boilerplate
   general-corporate; ORCL names capital expenditures. This module records the
   security type and the money, and makes no attribution claim.

Watermarks advance only on success-with-items; dedup keys are content-derived,
never time-derived (E12).
"""
from . import edgar, ixbrl

FFD = "ffd"
SECURITY_TYPE_DEBT = "Debt"

# Fee-exhibit documents are conventionally named *filingfees*; EDGAR does not
# guarantee it, so discovery falls back to scanning the filing index.
_FEE_HINTS = ("filingfees", "filing_fees", "exfilingfees", "ex-filing-fees")


class Tranche:
    __slots__ = ("line_no", "security_type", "amount_registered", "max_aggregate",
                 "fee_rate", "fee_amount")

    def __init__(self, line_no, security_type, amount_registered, max_aggregate,
                 fee_rate, fee_amount):
        self.line_no = line_no
        self.security_type = security_type
        self.amount_registered = amount_registered
        self.max_aggregate = max_aggregate
        self.fee_rate = fee_rate
        self.fee_amount = fee_amount

    @property
    def is_debt(self):
        return self.security_type == SECURITY_TYPE_DEBT

    def __repr__(self):
        return "Tranche(line={} {} {})".format(
            self.line_no, self.security_type, self.amount_registered)


class Offering:
    """One filing-fee exhibit, parsed."""

    def __init__(self, cik, accession, document, tranches, total_offering,
                 net_fee, registration_file, submission_type, is_final,
                 filed=None, period=None):
        self.cik = cik
        self.accession = accession
        self.document = document
        self.tranches = tranches
        self.total_offering = total_offering
        self.net_fee = net_fee
        self.registration_file = registration_file
        self.submission_type = submission_type
        self.is_final = is_final
        self.filed = filed
        self.period = period

    @property
    def debt_tranches(self):
        return [t for t in self.tranches if t.is_debt]

    @property
    def debt_principal(self):
        """Sum of registered debt principal, or None when nothing is extractable."""
        vals = [t.amount_registered for t in self.debt_tranches
                if t.amount_registered is not None]
        return sum(vals) if vals else None

    @property
    def tranche_sum_vs_total(self):
        """(sum of debt tranches, stated total, difference) — reported, never reconciled.

        The gap is real and structural: registered principal is face value, while
        TtlOfferingAmt is net of offering discount. Meta's differ by $32.6M,
        Oracle's by $38.2M. Collapsing them would invent precision.
        """
        s = self.debt_principal
        if s is None or self.total_offering is None:
            return (s, self.total_offering, None)
        return (s, self.total_offering, s - self.total_offering)

    @property
    def dedup_key(self):
        """Content-derived, never time-derived (E12)."""
        return "{}:{}".format(self.cik, self.accession)

    def __repr__(self):
        return "Offering({} {} final={} debt={})".format(
            self.accession, self.submission_type, self.is_final, self.debt_principal)


def _by_concept(facts):
    out = {}
    for f in facts:
        out.setdefault(f.concept, []).append(f)
    return out


def _scalar(byc, concept, cast=float):
    rows = [f for f in byc.get(concept, []) if not f.dims]
    if not rows:
        return None
    v = rows[0].value
    try:
        return cast(v)
    except (TypeError, ValueError):
        return v


def _text(raw, concept):
    """Non-numeric ffd facts (security type, flags) are not carried by the numeric parser."""
    import re
    m = re.search(r"<ffd:{0}[^>]*>([^<]*)</ffd:{0}>".format(concept), raw)
    return m.group(1).strip() if m else None


def parse_fee_exhibit(source, cik=None, accession=None, document=None):
    """Parse an EX-FILING FEES instance into an Offering."""
    raw = source if isinstance(source, (bytes, bytearray)) else open(source, "rb").read()
    text = raw.decode("utf-8", errors="replace")
    facts = ixbrl.parse_instance(raw)
    byc = _by_concept(facts)

    # Per-tranche facts are discriminated by the typed OfferingAxis dimension.
    lines = {}
    for f in facts:
        ln = f.dims.get("OfferingAxis")
        if ln is None:
            continue
        lines.setdefault(ln, {})[f.concept] = f.value

    # Security type is a non-numeric fact, so read it positionally from the raw
    # instance: the Nth OfferingSctyTp belongs to the Nth tranche line.
    import re
    types = re.findall(r"<ffd:OfferingSctyTp[^>]*>([^<]*)</ffd:OfferingSctyTp>", text)

    tranches = []
    for i, ln in enumerate(sorted(lines, key=lambda x: (len(x), x))):
        d = lines[ln]
        tranches.append(Tranche(
            line_no=ln,
            security_type=types[i].strip() if i < len(types) else None,
            amount_registered=d.get("AmtSctiesRegd"),
            max_aggregate=d.get("MaxAggtOfferingPric"),
            fee_rate=d.get("FeeRate"),
            fee_amount=d.get("FeeAmt"),
        ))

    final_flag = (_text(text, "FnlPrspctsFlg") or "").lower() == "true"
    return Offering(
        cik=cik, accession=accession, document=document, tranches=tranches,
        total_offering=_scalar(byc, "TtlOfferingAmt"),
        net_fee=_scalar(byc, "NetFeeAmt"),
        registration_file=_text(text, "RegnFileNb"),
        submission_type=_text(text, "SubmssnTp") or _text(text, "SubmissnTp"),
        is_final=final_flag,
    )


def find_fee_documents(index_html):
    """Fee-exhibit filenames from a filing's archive index page."""
    import re
    names = set(re.findall(r'>([A-Za-z0-9_.\-]+\.xml)<', index_html))
    return sorted(n for n in names if any(h in n.lower() for h in _FEE_HINTS))


def candidate_filings(submissions_doc, forms=("424B2", "424B3", "424B5", "FWP", "8-K"),
                      since=None):
    """Debt-event candidates from a submissions index.

    Deliberately NOT filtered on 8-K item codes: Meta's debt 8-K was filed under
    items 8.01/9.01 rather than 1.01/2.03, so an item filter drops real events.
    """
    recent = ((submissions_doc or {}).get("filings") or {}).get("recent") or {}
    cols = ("form", "filingDate", "accessionNumber", "primaryDocument")
    series = [recent.get(c) or [] for c in cols]
    if not series[0]:
        return []
    out = []
    for form, filed, accession, doc in zip(*series):
        if form not in forms:
            continue
        if since and (filed or "") < since:
            continue
        out.append({"form": form, "filed": filed, "accession": accession,
                    "primary_document": doc})
    return sorted(out, key=lambda r: r["filed"], reverse=True)


def ingest(cik, submissions_doc, http=None, since=None, limit=25, seen=None):
    """Walk candidate filings and return parsed final debt offerings.

    Returns (offerings, skipped) where skipped explains every rejection —
    a filing dropped without a reason is indistinguishable from one never seen.
    """
    seen = set(seen or ())
    offerings, skipped = [], []
    for cand in candidate_filings(submissions_doc, since=since)[:limit]:
        key = "{}:{}".format(edgar.config.cik10(cik), cand["accession"])
        if key in seen:
            skipped.append((cand["accession"], "already ingested"))
            continue
        try:
            index = edgar.fetch_document(cik, cand["accession"], "", http)
        except Exception as exc:
            skipped.append((cand["accession"], "index fetch failed: {}".format(exc)))
            continue
        docs = find_fee_documents(index)
        if not docs:
            skipped.append((cand["accession"], "no fee exhibit in filing"))
            continue
        raw = edgar.fetch_document(cik, cand["accession"], docs[0], http)
        off = parse_fee_exhibit(raw.encode("utf-8"), cik=edgar.config.cik10(cik),
                                accession=cand["accession"], document=docs[0])
        off.filed = cand["filed"]
        if not off.is_final:
            skipped.append((cand["accession"], "preliminary prospectus, FnlPrspctsFlg not true"))
            continue
        if not off.debt_tranches:
            skipped.append((cand["accession"], "no Debt tranches in fee exhibit"))
            continue
        offerings.append(off)
    return offerings, skipped


def advance_watermark(current, offerings):
    """Advance only on success-with-items, only to the newest ingested item (E12)."""
    if not offerings:
        return current
    newest = max(o.filed for o in offerings if o.filed)
    if current and newest <= current:
        return current
    return newest
