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
