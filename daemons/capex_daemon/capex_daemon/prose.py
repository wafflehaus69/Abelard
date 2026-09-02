"""CD-GAP1 P3 — capex read out of filing PROSE, for issuers that tag none.

Admitted by Mando 2026-08-26 for Nebius, which files 245 6-Ks and 16 20-Fs and
was ruled into the panel as a major neocloud while contributing nothing.

**Premise corrected 2026-09-02.** This module was written on the claim that
Nebius puts *zero capex in companyfacts*. Re-measured against the live API, that
is false: `PaymentsToAcquirePropertyPlantAndEquipment` carries 58 facts (19 USD,
39 RUB), every one an annual 364/365-day duration, running 2020 through FY2025
at $4,066.0M — and the three anchors hardcoded below were already among them.
What companyfacts does NOT carry is anything shorter than a year, so no
quarterly or half-yearly series can be derived from it and NBIS is correctly
`FPI-ANNUAL-BASIS` today.

That is what this module is actually for: **granularity and arrival time**, not
the absence of data. The interim releases give 2025Q1/Q2 and 2026Q1/Q2 halves
months before the 20-F lands. A cheaper first move — an annual row straight off
the API — exists and has not been ruled on. See CD-GAP1-VERIFY §P3-COMPANYFACTS.

Regex-tier, zero LLM (E2). Restricted to issuers with an explicit entry in
`PROSE_SOURCES` — this is not a general prose reader and must not become one by
accident.

**The basis is broader than the panel's, and that is ruled, marked, and
load-bearing.** Nebius's interim releases report

    "Purchases of property and equipment AND INTANGIBLE ASSETS"

where every other name in the panel contributes PP&E payments only. Intangibles
are not PP&E. Under [E23] — concept identity is not semantic identity — a figure
that means something broader may not silently join a series that assumes it does
not. So every published row carries `basis` and the `DERIVED-FROM-PROSE` cause,
and the issuer sits in a bucket that no aggregate reads.

**A semi-annual reporter can never reach the ladder, and that is structural.**
Nebius publishes Q1 and H1 (and FY), so differencing yields Q1 and Q2 of each
year and nothing else — Q3 is never reported separately and Q4 is only ever
inside an H2 lump. The derived series is therefore permanently non-contiguous:

    2025Q1 $543.9M   2025Q2 $510.6M   [Q3, Q4 unobtainable]
    2026Q1 $2,472.9M 2026Q2 $5,657.4M

which means no TTM, no TTM YoY, and no phase state — ever, from this source. The
row exists to publish a LEVEL and a like-for-like half-over-half growth read for
a name that was contributing nothing at all. It is not a countdown to
eligibility, and it must not be rendered as one.

**Anchor on the table, never on the phrase.** "Capital expenditures" appears in
the forward-looking-statements boilerplate of nearly every release; keying on it
returns boilerplate at close to a 100% rate. Measured across 41 exhibits, a real
capex table appears in 2 — the earnings releases.
"""
import re

BASIS_PPE_PLUS_INTANGIBLES = "PPE-PLUS-INTANGIBLES"

# The one sentence that introduces the real table, in the issuer's own words.
TABLE_ANCHOR = re.compile(r"information about our capital expenditures", re.I)
PERIOD_RE = re.compile(
    r"(three|six|nine|twelve)\s+months\s+ended\s+([A-Z][a-z]+)\s+(\d{1,2})|"
    r"year\s+ended\s+(december)\s+(31)", re.I)
YEARS_RE = re.compile(r"\b(20\d\d)\b")
FIGURE_RE = re.compile(r"\(?\s*([\d,]+\.\d)\s*\)?")

MONTH_Q = {"march": 1, "june": 2, "september": 3, "december": 4}
SPAN_Q = {"three": 1, "six": 2, "nine": 3, "twelve": 4, "year": 4}

PROSE_SOURCES = {
    "0001513845": {                      # Nebius Group N.V.
        "ticker": "NBIS",
        "basis": BASIS_PPE_PLUS_INTANGIBLES,
        "line_label": "Purchases of property and equipment and intangible assets",
        "ruled_on": "2026-08-26",
        "ruled_by": "Mando",
        "forms": ("6-K", "20-F"),
        "exhibit_re": r"(ex99d1|20f)\.htm$",
        # Verified 2026-08-26 against the FY2024 20-F, which carries three years
        # in one table: 2022 14.6, 2023 83.4, 2024 807.7 (USD millions).
        "anchors": {"2022": 14.6, "2023": 83.4, "2024": 807.7},
    },
}

_ENTITIES = (("&#8203;", ""), ("&#160;", " "), ("&nbsp;", " "),
             ("&#8239;", " "), ("&#8212;", "-"), ("&#36;", "$"))


def source_for(cik):
    from . import config
    return PROSE_SOURCES.get(config.cik10(cik)) if cik else None


def plain_text(body):
    txt = body if isinstance(body, str) else body.decode("utf-8", "replace")
    for a, b in _ENTITIES:
        txt = txt.replace(a, b)
    return re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", txt))


def parse_table(plain):
    """One capex table -> [(cumulative_quarters, year, value_musd)] or [].

    Returns CUMULATIVE periods exactly as reported — "six months ended June 30"
    stays a six-month figure. Turning those into discrete quarters is
    differencing and belongs to the caller, which already owns that operation
    for XBRL and must not grow a second copy of it here.
    """
    m = TABLE_ANCHOR.search(plain)
    if not m:
        return []
    seg = plain[m.end():m.end() + 460]
    per = PERIOD_RE.search(seg)
    if not per:
        return []
    span_word = (per.group(1) or "year").lower()
    month = (per.group(2) or per.group(4) or "").lower()
    n_q = SPAN_Q.get(span_word)
    if not n_q or month not in MONTH_Q:
        return []
    end_q = MONTH_Q[month]
    years = YEARS_RE.findall(seg[:per.end() + 90])
    figs = [float(v.replace(",", "")) for v in FIGURE_RE.findall(seg)]
    if not years or not figs:
        return []
    # The table prints one column per year, in the order the years appear.
    out = []
    for i, y in enumerate(years[:len(figs)]):
        out.append((n_q, end_q, int(y), figs[i]))
    return out


def half_over_half(rows):
    """Like-for-like growth on the periods the issuer actually reports.

    The honest read for a semi-annual filer: this year's six-month figure
    against last year's six-month figure. Not a TTM YoY and never labelled one.
    """
    halves = {y: v for n_q, _e, y, v in rows if n_q == 2}
    if len(halves) < 2:
        return None
    ys = sorted(halves)
    prior, latest = halves[ys[-2]], halves[ys[-1]]
    if prior <= 0:
        return None
    return latest / prior - 1.0


def discrete_from_cumulative(rows):
    """Cumulative year-to-date figures -> discrete calendar quarters.

    Same differencing the XBRL normalizer performs, applied to prose: a
    six-month figure minus the three-month figure of the same fiscal year is
    that year's second quarter. A period with no shorter companion in its own
    year is skipped rather than guessed.
    """
    by_year = {}
    for n_q, end_q, year, val in rows:
        by_year.setdefault(year, {})[n_q] = (end_q, val)
    out = {}
    for year, spans in by_year.items():
        for n_q in sorted(spans):
            end_q, val = spans[n_q]
            prev = spans.get(n_q - 1)
            if n_q == 1:
                out["{}Q{}".format(year, end_q)] = val * 1e6
            elif prev:
                out["{}Q{}".format(year, end_q)] = (val - prev[1]) * 1e6
    return out
