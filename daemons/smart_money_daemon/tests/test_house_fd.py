"""SM-C1 House annual-FD parser helper tests. Pure functions, no PDF / no pdfplumber."""
from smart_money import house_fd_ingest as fd


def test_parse_band_formats():
    assert fd._parse_band("$1,001 - $15,000") == (1001, 15000)
    assert fd._parse_band("$5,000,001 - $25,000,000") == (5000001, 25000000)
    assert fd._parse_band("Over $50,000,000") == (50000000, None)   # open top band
    assert fd._parse_band("None") == (None, None)
    assert fd._parse_band("") == (None, None)


def test_header_detection_tx_optional():
    with_tx = [(30, "Asset"), (260, "Owner"), (300, "Value"), (320, "of"), (340, "Asset"),
               (400, "Income"), (440, "Type(s)"), (500, "Income"), (530, "Tx.")]
    a1 = fd._find_assets_header(with_tx)
    assert a1 and {"Asset", "Owner", "Value", "Income", "Tx."} <= set(a1)
    # the recovered ~19% variant: header ends without a Tx. column
    no_tx = [(30, "Asset"), (260, "Owner"), (300, "Value"), (400, "Income"),
             (440, "Type(s)"), (500, "Income")]
    a2 = fd._find_assets_header(no_tx)
    assert a2 and "Tx." not in a2 and "Value" in a2 and "Income" in a2
    assert fd._find_assets_header([(30, "Source"), (260, "Type"), (400, "Amount")]) is None


def test_finalize_ticker_type_owner_band():
    r = fd._finalize({"asset": ["Apple", "Inc.", "-", "Common", "Stock", "(AAPL)", "[ST]"],
                      "owner": "SP", "value": ["$5,000,001", "-", "$25,000,000"],
                      "income": ["Dividends"]})
    assert r["ticker"] == "AAPL" and r["asset_type"] == "ST" and r["owner"] == "SP"
    assert r["value_lo"] == 5000001 and r["value_hi"] == 25000000
    assert r["income_type"] == "Dividends" and "[ST]" not in r["asset_name"]
    opt = fd._finalize({"asset": ["Alphabet", "(GOOGL)", "[OP]"], "owner": "SP",
                        "value": ["$1,000,001", "-", "$5,000,000"], "income": ["None"]})
    assert opt["ticker"] == "GOOGL" and opt["asset_type"] == "OP"          # option kept distinct
    rp = fd._finalize({"asset": ["11", "Zinfandel", "Lane", "[RP]"], "owner": "JT",
                       "value": ["$5,000,001", "-", "$25,000,000"], "income": ["Rent"]})
    assert rp["ticker"] is None and rp["asset_type"] == "RP" and rp["value_lo"] == 5000001


def test_col_bounds_and_bucket():
    bounds = fd._col_bounds({"Asset": 30, "Owner": 260, "Value": 300, "Income": 400, "Tx.": 530})
    b = fd._bucket([(35, "Apple"), (262, "SP"), (305, "$5,000,001"), (405, "Dividends")], bounds)
    assert b["Asset"] == ["Apple"] and b["Owner"] == ["SP"]
    assert b["Value"] == ["$5,000,001"] and b["Income"] == ["Dividends"]


def test_bracket_tickers_and_closed_type_vocabulary():
    """Two faces of one bug found by the Phase H capture gate.

    Filers write tickers in SQUARE brackets ("ARK INNOVATION [ARKK]"), which a
    parens-only rule missed. And because _TYPE_RE accepted ANY short bracketed token,
    ETF symbols like [QQQ]/[GLD]/[VOO] were stored as asset TYPES — inventing 27 bogus
    codes and pushing those rows out of the ST/OP/EF capture denominator entirely.
    """
    def fin(name):
        return fd._finalize({"asset": name.split(), "owner": "SP",
                             "value": ["$1,001", "-", "$15,000"], "income": ["None"]})
    r = fin("ARK INNOVATION [ARKK] [ST]")
    assert r["ticker"] == "ARKK" and r["asset_type"] == "ST", r
    # a bracketed ETF symbol with no type code is a TICKER, never a type
    r = fin("INVESCO QQQ TRUST [QQQ]")
    assert r["ticker"] == "QQQ" and r["asset_type"] is None, r
    # parens still win when present
    assert fin("Apple Inc. (AAPL) [ST]")["ticker"] == "AAPL"
    # a real type code must never be mistaken for a symbol
    r = fin("11 Zinfandel Lane [RP]")
    assert r["ticker"] is None and r["asset_type"] == "RP", r
    # digit-leading codes are real types too (4K/5C/5F/5P)
    assert fin("Retirement Plan [5F]")["asset_type"] == "5F"
    assert fin("Thrift [4K]")["asset_type"] == "4K"
    assert fin("Retirement Plan [5F]")["ticker"] is None


def test_every_known_type_code_resolves_as_a_type_not_a_ticker():
    for code in sorted(fd._FD_TYPES):
        r = fd._finalize({"asset": ["Asset", "[{}]".format(code)], "owner": "SP",
                          "value": ["x"], "income": ["None"]})
        if len(code) <= 3:
            assert r["asset_type"] == code, (code, r)
        assert r["ticker"] is None, "type code leaked into ticker: {}".format(code)


def test_band_complete_detects_a_wrapped_band():
    """A value cell ending mid-band means the second amount wrapped to the next line."""
    assert fd._band_complete(["$500,001", "-"]) is False
    assert fd._band_complete(["$500,001", "-", "$1,000,000"]) is True
    assert fd._band_complete(["None"]) is True
    assert fd._band_complete(["Over", "$50,000,000"]) is True
    assert fd._band_complete([]) is True


def test_coverage_year_reads_the_explicit_index_field():
    """{N}FD.zip holds annuals COVERING CY N (filed mostly N+1) and says so via `Year`.
    Inferring `zip_year - 1` put every House coverage year one year early, which also
    moved the Phase F flow cutoff back a full year."""
    assert fd._cov_year({"Year": "2025"}, 2025) == 2025
    assert fd._cov_year({"Year": " 2023 "}, 2099) == 2023
    # fallback only when the field is absent or unusable
    assert fd._cov_year({}, 2024) == 2024
    assert fd._cov_year({"Year": ""}, 2024) == 2024
