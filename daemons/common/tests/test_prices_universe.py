"""PS-1 Phase 2.1 — universe adapter tests. No network.

The parsers are exercised against small captured samples in the exact shape the
live sources serve. The identity tests are where the real risk lives: A3's
dual-class collision, and the four-notation problem that P0.4(v) flagged and
that bit for real during the build (iShares says ``BRKB``, Wikipedia says
``BRK.B``; keyed raw they became two instruments for one security).
"""

from __future__ import annotations

import pytest

from abelard_common.prices import universe as U

SEC_MAP = {
    "AAPL": ("0000320193", "Apple Inc.", "Nasdaq"),
    "GOOGL": ("0001652044", "Alphabet Inc.", "Nasdaq"),
    "GOOG": ("0001652044", "Alphabet Inc.", "Nasdaq"),
    "BRK-A": ("0001067983", "BERKSHIRE HATHAWAY INC", "NYSE"),
    "BRK-B": ("0001067983", "BERKSHIRE HATHAWAY INC", "NYSE"),
    "BF-B": ("0000014693", "BROWN FORMAN CORP", "NYSE"),
    "CMCSA": ("0001166691", "Comcast Corp", "Nasdaq"),
}

IWM_CSV = """iShares Russell 2000 ETF
Fund Holdings as of,"Aug 31, 2026"
Inception Date,"May 22, 2000"
Shares Outstanding,"273,650,000.00"
Stock,"-"
Bond,"-"
Cash,"-"
Other,"-"

Ticker,Name,Sector,Asset Class,Market Value,Weight (%),Notional Value,Quantity,Price,Location,Exchange,Currency,FX Rate,Market Currency,Accrual Date
"FROG","JFROG","Information Technology","Equity","1","0.35","1","1","100.07","United States","NASDAQ","USD","1.00","USD","-"
"MOGA","MOOG INC CLASS A","Industrials","Equity","1","0.34","1","1","371.50","United States","NYSE","USD","1.00","USD","-"
"WOW","WIDEOPENWEST","Communication","Equity","1","0.10","1","1","5.00","United States","NYSE","USD","1.00","USD","-"
"XTSLA","BLK CSH FND TREASURY","Other","Cash","1","0.20","1","1","1.00","United States","-","USD","1.00","USD","-"
"""


# --------------------------------------------------------------- ishares csv --

def test_ishares_csv_skips_metadata_and_reads_the_as_of_date():
    rows, as_of = U.parse_ishares(IWM_CSV, "RUT", "ishares_iwm")
    assert as_of == "Aug 31, 2026"
    assert [r.ticker for r in rows] == ["FROG", "MOGA", "WOW"]


def test_ishares_drops_non_equity_rows():
    """Cash and futures lines are holdings but not securities -- 6 of 1,967 in
    the live IWM file."""
    rows, _ = U.parse_ishares(IWM_CSV, "RUT", "ishares_iwm")
    assert "XTSLA" not in {r.ticker for r in rows}


def test_ishares_sector_is_normalised_to_gics_spelling():
    """Two measured deviations: 'Communication' -> 'Communication Services',
    and the 'Other' bucket is not a sector at all."""
    rows, _ = U.parse_ishares(IWM_CSV, "RUT", "ishares_iwm")
    by = {r.ticker: r.sector for r in rows}
    assert by["WOW"] == "Communication Services"
    assert by["FROG"] == "Information Technology"


def test_ishares_missing_header_fails_loud():
    with pytest.raises(U.UniverseError):
        U.parse_ishares("just\nsome\nlines\n", "RUT", "ishares_iwm")


# ------------------------------------------------------------------ identity --

def _c(ticker, source, **kw):
    return U.Constituent(ticker=ticker, name=kw.pop("name", ticker),
                         index_code=kw.pop("index_code", "SPX"), source=source, **kw)


def test_dual_class_gets_distinct_instrument_ids():
    """A3. Both share CIK 0001652044; the class discriminator keeps them apart."""
    rows = [
        _c("GOOGL", "wikipedia_spx", name="Alphabet Inc. (Class A)",
           cik="0001652044", class_code="A"),
        _c("GOOG", "wikipedia_spx", name="Alphabet Inc. (Class C)",
           cik="0001652044", class_code="C"),
    ]
    merged, _ = U.assign_instrument_ids(rows, SEC_MAP)
    ids = {t: U.instrument_id(c) for t, c in merged.items()}
    assert ids == {"GOOGL": "0001652044.A", "GOOG": "0001652044.C"}
    assert len(set(ids.values())) == 2


def test_concatenated_notation_merges_with_the_dotted_form():
    """THE build bug. iShares serves 'BRKB', Wikipedia serves 'BRK.B'. Keyed on
    the raw string they became two instruments, the second with no CIK. The
    canonical key comes from a reverse index over the SEC file's own tickers, so
    nothing is transformed unless SEC vouches for the result."""
    rows = [
        _c("BRK.B", "wikipedia_spx", cik="0001067983", sector="Financials"),
        _c("BRKB", "ishares_ivv", sector="Financials"),
    ]
    merged, raw_to_key = U.assign_instrument_ids(rows, SEC_MAP)
    assert len(merged) == 1
    (key, c), = merged.items()
    assert key == "BRK-B" and c.cik == "0001067983"
    assert raw_to_key["BRKB"] == "BRK-B" and raw_to_key["BRK.B"] == "BRK-B"


def test_a_genuine_five_letter_ticker_is_not_mangled():
    """CMCSA and GOOGL are real tickers, not concatenated share classes. A
    length rule would corrupt them; a lookup does not."""
    merged, _ = U.assign_instrument_ids([_c("CMCSA", "ishares_ivv")], SEC_MAP)
    assert list(merged) == ["CMCSA"]
    assert merged["CMCSA"].cik == "0001166691"


def test_ordinal_fallback_when_no_source_names_the_class():
    """The Berkshire case: two tickers on one CIK and no '(Class X)' anywhere,
    because BRK-A is not an index member. The fallback must be deterministic
    and it must be visible as a fallback."""
    rows = [_c("BRK-A", "ishares_ivv"), _c("BRK-B", "ishares_ivv")]
    merged, _ = U.assign_instrument_ids(rows, SEC_MAP)
    assert merged["BRK-A"].class_code == "1"
    assert merged["BRK-B"].class_code == "2"
    assert U.class_source_for(merged["BRK-A"], ordinal_used=True) == "ordinal"


def test_single_class_issuer_gets_class_zero():
    merged, _ = U.assign_instrument_ids([_c("AAPL", "wikipedia_spx")], SEC_MAP)
    assert U.instrument_id(merged["AAPL"]) == "0000320193.0"
    assert U.class_source_for(merged["AAPL"], ordinal_used=False) == "single"


def test_a_name_with_no_cik_is_provisional_never_dropped():
    merged, _ = U.assign_instrument_ids([_c("ZZZQ", "ishares_iwm")], SEC_MAP)
    assert U.instrument_id(merged["ZZZQ"]) == "NOCIK.ZZZQ"
    assert merged["ZZZQ"].cik is None


def test_normalise_renders_every_notation():
    assert U.normalise("BRK.B") == {
        "dot": "BRK.B", "dash": "BRK-B", "concat": "BRKB", "vendor": "BRK-B"}


def test_merge_prefers_a_populated_field_over_a_blank_one():
    """Wikipedia carries sub-industry, iShares does not; the merge must not let
    the second source blank the first."""
    rows = [
        _c("AAPL", "wikipedia_spx", cik="0000320193",
           sector="Information Technology", sub_industry="Tech Hardware"),
        _c("AAPL", "ishares_ivv", sector="Information Technology"),
    ]
    merged, _ = U.assign_instrument_ids(rows, SEC_MAP)
    assert merged["AAPL"].sub_industry == "Tech Hardware"


# --------------------------------------------------------------- sec client --

def test_sec_client_refuses_a_blank_contact():
    """SEC returns 403 for a browser UA. Fail loud rather than send one -- the
    same rule config.edgar_contact() enforces for SM and Capex."""
    for bad in ("", "   ", "Abelard"):
        with pytest.raises(U.UniverseError) as e:
            U.sec_client(bad)
        assert "contact" in str(e.value).lower()


def test_sec_client_accepts_an_email():
    c = U.sec_client("someone@example.com")
    assert "someone@example.com" in c.user_agent
