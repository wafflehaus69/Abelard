"""Classify what a 13F line actually IS, from stored evidence only.

thirteenf_holdings has no instrument vocabulary. Its entire notion of type is the
13F putCall tag, so convertible notes, convertible preferred, warrants, units, ADRs
and multi-class common all render identically as a long equity position. Measured
on production: 26 convertible rows at $2,014,913,635 in Q2 alone (264 rows,
$17,242,546,817 across all periods) counted as equity conviction, and a filer
holding $899,412 of CORZ common beside $42,018,000 of CORZW warrants reads as one
$42.9M equity stake.

NO SUFFIX RULES. The obvious rule — a trailing W means warrant — is wrong in both
directions and expensively so. Measured across Q2: it would misclassify
$1,578,772,294 of common (GLW $1.27B, SNOW $277.7M, plus BW, NOW, CDW, PANW) to
correctly catch $47,466,880 of warrants, a 33x false-to-true ratio. And it misses
real warrants that do not end in W at all — FLYX/WS, OPENL. Ticker strings are the
resolver's output, not the filing's statement, and must never be the discriminator.

Evidence used, strongest first:
  1. putCall           — the filing's own tag. Options are decided here, full stop.
  2. FIGI securityType — "Warrant" / "Preference" / "Common Stock". Authoritative
                         when we have it; NULL on rows cached before it was stored.
  3. titleOfClass      — the filer's own words: COM, CAP STK CL A, NOTE 1.500%,
                         *W EXP, PFD, UNIT. Also NULL on older rows.
  4. CUSIP structure   — always present, no backfill needed. Characters 7-8 are the
                         issue number and are ALPHABETIC for a debt issue; that is
                         the CUSIP standard, not a heuristic. Verified against the
                         production set: the split is clean with no overlap, and
                         value/shares confirms it — debt issues land at 0.82-8.61
                         (dollars of par) while the preferred land at $44.88-$68.28
                         (a real unit price).
  5. sshPrnamtType     — PRN means `shares` is dollars of par, corroborating debt.

Where nothing states a class, the answer is `common`, because a 13F line is an
equity position by default — but a row that LOOKS like a bond descriptor and has no
supporting evidence returns `unresolved` rather than being asserted either way.
"""
import re

# A Bloomberg fixed-income descriptor that the resolver emitted into the ticker
# field: "GOOGL 6.25 05/15/29 A", "BILL 0 04/01/30". Used only to corroborate the
# CUSIP, never on its own — and never as a prefix match, because `ticker LIKE
# 'GOOGL%'` catches Alphabet's common stock and all three of its convertible lines
# with nothing in any column to separate them.
BOND_DESCRIPTOR = re.compile(
    r"^([A-Z.]{1,6})\s+([\d.]+)\s+(\d{2}/\d{2}/\d{2,4})(?:\s+([A-Z]))?$")

COMMON = "common"
OPTION_CALL = "option_call"
OPTION_PUT = "option_put"
CONVERTIBLE_NOTE = "convertible_note"
CONVERTIBLE_PREFERRED = "convertible_preferred"
WARRANT = "warrant"
UNIT = "unit"
UNRESOLVED = "unresolved"

# Instrument classes that are NOT common stock and must never be summed into an
# equity position for the issuer without saying so.
NON_COMMON = frozenset((OPTION_CALL, OPTION_PUT, CONVERTIBLE_NOTE,
                        CONVERTIBLE_PREFERRED, WARRANT, UNIT))


def issuer_id(cusip):
    """The 6-character CUSIP issuer prefix — the correct rollup key.

    GOOGL 02079K305, GOOG 02079K107 and both GOOGL convertible series 02079K404 /
    02079K602 all share 02079K. Alphabet's true exposure is $4,494,739,012 across
    8 filers, 12 rows and 4 distinct ticker strings, and no page joined them
    because the join was attempted on ticker. BRK/B and BRK/A likewise share
    084670. This needs no resolver and no backfill.
    """
    if not cusip or len(cusip) < 6:
        return None
    return cusip[:6].upper()


def is_debt_issue(cusip):
    """True when the CUSIP's issue number is alphabetic, i.e. a debt issue.

    CUSIP is 6 issuer + 2 issue + 1 check digit. Equity issues are numeric; fixed
    income uses letters. 37940XAU6 (GPN 1.5 03/01/31) has 'A'; 02079K404 (GOOGL
    6.25 preferred) has '4'.
    """
    if not cusip or len(cusip) < 8:
        return False
    return cusip[6].isalpha()


def classify(put_call=None, cusip=None, ticker=None, security_type=None,
             title_of_class=None, shares_type=None):
    """Return an instrument_class string. Stored evidence only; never a suffix."""
    pc = (put_call or "").lower()
    if pc == "call":
        return OPTION_CALL
    if pc == "put":
        return OPTION_PUT

    st = (security_type or "").strip().lower()
    if st:
        # FIGI spells warrants "Equity WRT", which does not contain "warrant" —
        # matching only the full word missed all 9 warrant CUSIPs in the corpus,
        # including CORZW, OPENW/OPENL and both Chesapeake lines.
        if "warrant" in st or re.search(r"\bwrt\b", st):
            return WARRANT
        if "unit" in st:
            return UNIT
        if "preference" in st or "preferred" in st:
            return CONVERTIBLE_PREFERRED
        if st in ("common stock", "ads", "adr", "depositary receipt"):
            return COMMON

    title = (title_of_class or "").strip().upper()
    if title:
        if "WARRANT" in title or re.search(r"\*W\b|\bWTS?\b", title):
            return WARRANT
        if "UNIT" in title:
            return UNIT
        if "NOTE" in title or "DEB" in title or "BOND" in title:
            return CONVERTIBLE_NOTE
        if "PFD" in title or "PREF" in title:
            return CONVERTIBLE_PREFERRED

    # CUSIP structure — always available, no backfill required.
    if is_debt_issue(cusip):
        return CONVERTIBLE_NOTE
    if (shares_type or "").upper() == "PRN":
        # par-denominated without an alphabetic issue: corroborates debt
        return CONVERTIBLE_NOTE

    if ticker and BOND_DESCRIPTOR.match(ticker.strip()):
        # Carries a coupon and a maturity, but the CUSIP issue number is numeric,
        # so it is not a debt issue: a convertible preferred or depositary share.
        # Corroborated by the implied unit price — these land at $44.88-$68.28
        # against the notes at 0.82-8.61 per dollar of par, two clean clusters with
        # no overlap. GOOGL 6.25 05/15/29 A and B are exactly this shape.
        # Common stock cannot reach here: a common ticker never contains a space.
        return CONVERTIBLE_PREFERRED

    return COMMON


def issuer_ticker(ticker):
    """The equity symbol a bond descriptor belongs to, or the ticker unchanged.

    'GOOGL 6.25 05/15/29 A' -> 'GOOGL'. Lets a convertible be linked to the
    issuer's equity line without any code doing a prefix match on the symbol.
    """
    if not ticker:
        return None
    m = BOND_DESCRIPTOR.match(ticker.strip())
    return m.group(1) if m else ticker.strip()
