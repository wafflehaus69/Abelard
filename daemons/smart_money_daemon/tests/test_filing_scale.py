"""13F VALUE units, resolved per FILING and stored.

Form 13F states no unit for VALUE anywhere — not the cover page, not the info
table. Whole dollars are mandated since the 2023 amendments, but filers still
report thousands. Verified against EDGAR: Duquesne filed thousands at 2022-09-30,
whole dollars at 2022-12-31, then reverted — so the unit is a property of the
FILING, not the filer, and a per-filer scale would mis-scale a whole quarter on
backfill.
"""
import os
import tempfile

from smart_money import db as dbmod, filing_scale, thirteenf


def _db():
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    return path


def _price(con, ticker, date, close):
    con.execute("INSERT OR REPLACE INTO prices VALUES (?,?,?,?,?,?,?,?)",
                (ticker, date, close, close, "eod", 0, 0, "test"))


def _hold(con, cik, acc, period, cusip, ticker, value, shares,
          pc="long", stype="SH"):
    con.execute(
        "INSERT OR REPLACE INTO thirteenf_holdings(cik, accession, period, "
        "filed_date, cusip, ticker, issuer, put_call, value, shares, "
        "ingested_at_unix, shares_type) VALUES (?,?,?,?,?,?,?,?,?,?,0,?)",
        (cik, acc, period, period, cusip, ticker, ticker or cusip, pc, value,
         shares, stype))


# ------------------------------------------------------------------ parsing ---

_INFO = """<?xml version="1.0"?>
<informationTable xmlns="http://www.sec.gov/edgar/document/thirteenf/informationtable">
 <infoTable><nameOfIssuer>Natera Inc</nameOfIssuer><titleOfClass>COM</titleOfClass>
  <cusip>632307104</cusip><value>864923</value>
  <shrsOrPrnAmt><sshPrnamt>3186306</sshPrnamt><sshPrnamtType>SH</sshPrnamtType></shrsOrPrnAmt>
 </infoTable>
 <infoTable><nameOfIssuer>Global Pmts Inc</nameOfIssuer><titleOfClass>NOTE 1.500%</titleOfClass>
  <cusip>37940XAU6</cusip><value>222694485</value>
  <shrsOrPrnAmt><sshPrnamt>247500000</sshPrnamt><sshPrnamtType>PRN</sshPrnamtType></shrsOrPrnAmt>
 </infoTable>
</informationTable>"""


def test_parse_captures_shares_type_and_title():
    """sshPrnamtType is the only field that says whether `shares` is a share count
    or dollars of par. Without it the convertible notes' par is summed into real
    share counts."""
    h = thirteenf.parse_holdings(_INFO)
    assert h["632307104"]["shares_type"] == "SH"
    assert h["632307104"]["title_of_class"] == "COM"
    assert h["37940XAU6"]["shares_type"] == "PRN"
    assert h["37940XAU6"]["title_of_class"] == "NOTE 1.500%"


def test_mixed_share_types_on_one_cusip_are_marked_not_blended():
    xml = _INFO.replace("632307104", "37940XAU6")   # same cusip, SH then PRN
    h = thirteenf.parse_holdings(xml)
    assert h["37940XAU6"]["shares_type"] == "MIXED", (
        "a cusip reporting both SH and PRN has a meaningless summed share count "
        "and must say so rather than pick a winner")


_COVER = """<?xml version="1.0"?>
<edgarSubmission xmlns="http://www.sec.gov/edgar/thirteenffiler">
 <formData><summaryPage>
  <tableEntryTotal>95</tableEntryTotal>
  <tableValueTotal>5210860</tableValueTotal>
 </summaryPage></formData></edgarSubmission>"""


def test_parse_cover_reads_the_declared_control_totals():
    c = thirteenf.parse_cover(_COVER)
    assert c["entry_total"] == 95
    assert c["value_total"] == 5210860


def test_cover_absent_fields_are_none_not_zero():
    c = thirteenf.parse_cover("<edgarSubmission></edgarSubmission>")
    assert c["entry_total"] is None and c["value_total"] is None


# ----------------------------------------------------------------- anchoring ---

def test_thousands_filing_is_detected():
    """Duquesne's real Q2 shape: value 864,923 against 3,186,306 shares on a
    ~$271 stock is an implied $0.27 — the thousands fingerprint."""
    path = _db()
    try:
        con = dbmod.connect(path)
        for tk, val, sh, close in (("NTRA", 864923, 3186306, 271.0),
                                   ("GOOGL", 120184, 336300, 357.0),
                                   ("TSM", 281613, 589680, 477.0)):
            _hold(con, "1536411", "acc-K", "2026-06-30", "cu" + tk, tk, val, sh)
            _price(con, tk, "2026-06-30", close)
        con.commit()
        scale, ratios = filing_scale.price_anchor(con, "1536411", "acc-K",
                                                  "2026-06-30")
        assert scale == 1000, (scale, ratios)
    finally:
        os.remove(path)


def test_dollar_filing_is_detected():
    path = _db()
    try:
        con = dbmod.connect(path)
        for tk, val, sh, close in (("NTRA", 864923000, 3186306, 271.0),
                                   ("GOOGL", 120184000, 336300, 357.0),
                                   ("TSM", 281613000, 589680, 477.0)):
            _hold(con, "1029160", "acc-D", "2026-06-30", "cu" + tk, tk, val, sh)
            _price(con, tk, "2026-06-30", close)
        con.commit()
        scale, _r = filing_scale.price_anchor(con, "1029160", "acc-D", "2026-06-30")
        assert scale == 1
    finally:
        os.remove(path)


def test_par_denominated_rows_are_not_used_as_anchors():
    """A PRN row's `shares` is dollars of par, so its implied price is meaningless
    and would drag the median."""
    path = _db()
    try:
        con = dbmod.connect(path)
        for tk, val, sh, close in (("NTRA", 864923000, 3186306, 271.0),
                                   ("GOOGL", 120184000, 336300, 357.0),
                                   ("TSM", 281613000, 589680, 477.0)):
            _hold(con, "1029160", "acc-P", "2026-06-30", "cu" + tk, tk, val, sh)
            _price(con, tk, "2026-06-30", close)
        # three par rows that would look like "thousands" if admitted
        for i in range(3):
            _hold(con, "1029160", "acc-P", "2026-06-30", "bond%d" % i,
                  "BOND%d" % i, 222694485, 247500000, stype="PRN")
            _price(con, "BOND%d" % i, "2026-06-30", 100.0)
        con.commit()
        scale, ratios = filing_scale.price_anchor(con, "1029160", "acc-P",
                                                  "2026-06-30")
        assert scale == 1, (scale, ratios)
        assert len(ratios) == 3, "PRN rows must be excluded from the anchor set"
    finally:
        os.remove(path)


def test_no_price_coverage_is_undetermined_not_a_default_of_one():
    """Defaulting to dollars is how a thousands filer reads 1000x low with no
    signal. NULL means unresolved and must stay NULL."""
    path = _db()
    try:
        con = dbmod.connect(path)
        _hold(con, "999", "acc-U", "2026-06-30", "cu1", "AAA", 100, 10)
        con.commit()                       # no price row at all -> nothing to anchor
        r = filing_scale.resolve(con, "999", "acc-U", "2026-06-30")
        assert r["value_scale"] is None
        assert r["scale_basis"] == "undetermined"
    finally:
        os.remove(path)


def test_a_single_anchor_decides_but_is_marked_weak():
    """The clusters are 1.0 and 0.001 — three orders apart — so one anchor decides.
    Requiring three would leave a one-position filer permanently undetermined, and
    Affinity and Founders Fund hold exactly one."""
    path = _db()
    try:
        con = dbmod.connect(path)
        _hold(con, "2059583", "acc-W", "2026-06-30", "cu1", "QXO", 5000, 500)
        _price(con, "QXO", "2026-06-30", 10.0)
        con.commit()
        r = filing_scale.resolve(con, "2059583", "acc-W", "2026-06-30")
        assert r["value_scale"] == 1
        assert r["scale_basis"] == "price_anchored_weak", r
        assert r["anchors"] == 1
    finally:
        os.remove(path)


def test_unanchorable_filing_inherits_from_the_same_filer_and_says_so():
    """A fallback, never the model — it is exactly the per-filer assumption that
    Duquesne's 2022-12-31 filing disproves, so it must be labelled."""
    path = _db()
    try:
        con = dbmod.connect(path)
        # anchorable filing: thousands
        for tk, val, sh, close in (("NTRA", 864923, 3186306, 271.0),
                                   ("GOOGL", 120184, 336300, 357.0),
                                   ("TSM", 281613, 589680, 477.0)):
            _hold(con, "1536411", "acc-anch", "2026-06-30", "cu" + tk, tk, val, sh)
            _price(con, tk, "2026-06-30", close)
        # older filing with no priced holding of its own
        _hold(con, "1536411", "acc-old", "2024-06-30", "cuZZ", "ZZZ", 1000, 10)
        con.commit()
        rows = {r["accession"]: r for r in filing_scale.plan(con)}
        assert rows["acc-anch"]["value_scale"] == 1000
        assert rows["acc-old"]["value_scale"] == 1000
        assert rows["acc-old"]["scale_basis"] == "inherited"
        assert rows["acc-old"]["inherited_from"] == "acc-anch"
    finally:
        os.remove(path)


def test_a_filer_with_no_anchorable_filing_stays_undetermined():
    path = _db()
    try:
        con = dbmod.connect(path)
        _hold(con, "777", "acc-a", "2026-06-30", "cu1", "AAA", 100, 10)
        _hold(con, "777", "acc-b", "2026-03-31", "cu1", "AAA", 100, 10)
        con.commit()                       # no prices anywhere
        rows = filing_scale.plan(con)
        assert all(r["value_scale"] is None for r in rows), rows
        assert all(r["scale_basis"] == "undetermined" for r in rows)
    finally:
        os.remove(path)


# ------------------------------------------------------------- the guarantee ---

def test_value_usd_is_raw_until_a_scale_is_stamped():
    path = _db()
    try:
        con = dbmod.connect(path)
        _hold(con, "1536411", "acc-K", "2026-06-30", "cu1", "NTRA", 864923, 3186306)
        con.commit()
        v, vusd, vs = con.execute(
            "SELECT value, value_usd, value_scale FROM thirteenf_holdings"
        ).fetchone()
        assert vs is None
        assert vusd == v == 864923, "unresolved reads as raw, the prior behaviour"
    finally:
        os.remove(path)


def test_value_usd_applies_the_stamped_scale():
    path = _db()
    try:
        con = dbmod.connect(path)
        _hold(con, "1536411", "acc-K", "2026-06-30", "cu1", "NTRA", 864923, 3186306)
        con.commit()
        filing_scale.apply_to_filing(con, "1536411", "acc-K", 1000)
        con.commit()
        v, vusd = con.execute(
            "SELECT value, value_usd FROM thirteenf_holdings").fetchone()
        assert v == 864923, "raw value must never be overwritten"
        assert vusd == 864_923_000
    finally:
        os.remove(path)


def test_scale_is_per_filing_not_per_filer():
    """The whole point. Duquesne filed thousands, then dollars, then thousands
    again. Stamping one filing must not touch the other."""
    path = _db()
    try:
        con = dbmod.connect(path)
        _hold(con, "1536411", "acc-2022Q3", "2022-09-30", "cu1", "NTRA", 1763330, 10)
        _hold(con, "1536411", "acc-2022Q4", "2022-12-31", "cu1", "NTRA",
              2020266796, 10)
        con.commit()
        filing_scale.apply_to_filing(con, "1536411", "acc-2022Q3", 1000)
        filing_scale.apply_to_filing(con, "1536411", "acc-2022Q4", 1)
        con.commit()
        got = dict(con.execute(
            "SELECT accession, value_usd FROM thirteenf_holdings").fetchall())
        assert got["acc-2022Q3"] == 1_763_330_000
        assert got["acc-2022Q4"] == 2_020_266_796
    finally:
        os.remove(path)


def test_control_total_counts_what_was_actually_stored():
    path = _db()
    try:
        con = dbmod.connect(path)
        for i in range(4):
            _hold(con, "1", "acc-C", "2026-06-30", "cu%d" % i, "T%d" % i, 100, 1)
        con.commit()
        n, v = filing_scale.control_total(con, "1", "acc-C")
        assert n == 4 and v == 400
    finally:
        os.remove(path)


def test_record_meta_roundtrips():
    path = _db()
    try:
        con = dbmod.connect(path)
        _hold(con, "1536411", "acc-K", "2026-06-30", "cu1", "NTRA", 864923, 3186306)
        con.commit()
        r = filing_scale.resolve(con, "1536411", "acc-K", "2026-06-30")
        r["filed_date"] = "2026-08-14"
        filing_scale.record_meta(con, r, entry_total=95, value_total=5210860)
        con.commit()
        row = con.execute(
            "SELECT entry_total, value_total, parsed_rows, scale_basis "
            "FROM thirteenf_filing_meta").fetchone()
        assert row[0] == 95 and row[1] == 5210860
        assert row[2] == 1
        assert row[3] == "undetermined"
    finally:
        os.remove(path)
