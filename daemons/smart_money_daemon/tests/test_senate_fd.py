"""SM-C2 Senate annual-FD parser tests. Pure functions on synthetic grid HTML, no network."""
from smart_money import senate_fd_ingest as sfd
from smart_money.house_fd_ingest import _parse_band


def _cell(strong_inner, atype, owner, value, income="Dividends"):
    return ("<tr><td>1</td><td><strong class='marginit-right'>{}</strong></td>"
            "<td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>--</td></tr>"
            .format(strong_inner, atype, owner, value, income))


GRID = '<table id="grid_items">{}</table>'.format("".join([
    _cell('<a href="http://finance.yahoo.com/q?s=DUK">DUK</a> - Duke Energy Corporation (NYSE)',
          "Corporate SecuritiesStock", "Joint", "$1,001 - $15,000"),
    _cell("MFC-Manulife Financial Corporation (NYSE)", "Corporate SecuritiesStock",
          "Spouse", "$15,001 - $50,000"),
    _cell("MUB - iShares National Muni Bond ETF", "Corporate SecuritiesStock",
          "Self", "None (or less than $1,001)"),
    _cell("International Business Machines Corporation (IBM)", "Corporate SecuritiesStock",
          "Dependent", "Over $50,000,000"),
    _cell("VTV", "Corporate SecuritiesStock", "Self", "$50,001 - $100,000"),
    _cell("USAA - Federal Savings Bank", "Bank Deposit", "Joint", "--"),
    _cell("SPY - SPDR S&amp;P 500 ETF", "Mutual FundsExchange Traded Fund/Note", "Self",
          "Unascertainable"),
]))


def test_parse_annual_assets_full_grid():
    rows, status = sfd.parse_annual_assets(GRID)
    assert status == "ok" and len(rows) == 7
    # yahoo-link ticker + owner/type/band
    assert rows[0]["ticker"] == "DUK" and rows[0]["owner"] == "JT"
    assert rows[0]["asset_type"] == "ST" and (rows[0]["value_lo"], rows[0]["value_hi"]) == (1001, 15000)
    # unspaced "TICKER-Company" prefix, Spouse
    assert rows[1]["ticker"] == "MFC" and rows[1]["owner"] == "SP"
    # spaced prefix + lowercase company start + "less than" floor band
    assert rows[2]["ticker"] == "MUB" and (rows[2]["value_lo"], rows[2]["value_hi"]) == (0, 1001)
    # trailing (IBM) paren + Dependent + open-top band
    assert rows[3]["ticker"] == "IBM" and rows[3]["owner"] == "DC"
    assert (rows[3]["value_lo"], rows[3]["value_hi"]) == (50000000, None)
    # bare all-caps symbol
    assert rows[4]["ticker"] == "VTV"
    # bank deposit: heuristics suppressed -> no phantom ticker; unmapped type -> None (House convention)
    assert rows[5]["ticker"] is None and rows[5]["asset_type"] is None
    assert (rows[5]["value_lo"], rows[5]["value_hi"]) == (None, None)
    # ETF type + unascertainable band
    assert rows[6]["ticker"] == "SPY" and rows[6]["asset_type"] == "EF"
    assert rows[6]["value_lo"] is None


def test_no_grid_returns_status():
    assert sfd.parse_annual_assets("<html>no assets table</html>") == ([], "no_grid")


def test_extract_ticker_guards():
    # exchange token in parens is not a ticker
    assert sfd._extract_ticker("", "Some Company (NYSE)", "ST") is None
    # prose name with a single leading cap + dash must not false-match
    assert sfd._extract_ticker("", "Bristol-Meyers Squibb Co Com", "ST") is None
    # non-equity asset type suppresses heuristic extraction
    assert sfd._extract_ticker("", "ABC - Some Muni Bond", "Government S") is None
    # yahoo link is trusted for ANY asset type
    assert sfd._extract_ticker('<a href="http://finance.yahoo.com/q?s=T">T</a>',
                               "AT&T", "Bank Deposit") == "T"


def test_norm_owner_and_asset_type():
    assert (sfd._norm_owner("Joint"), sfd._norm_owner("Spouse")) == ("JT", "SP")
    assert (sfd._norm_owner("Dependent Child"), sfd._norm_owner("Self")) == ("DC", "Self")
    assert sfd._asset_type("Corporate SecuritiesStock") == "ST"
    # "option" must win over "stock" for a stock-option row
    assert sfd._asset_type("Corporate SecuritiesStock Option") == "OP"
    assert sfd._asset_type("Mutual FundsExchange Traded Fund/Note") == "EF"
    # unmapped non-equity types -> None, never truncated free text
    assert sfd._asset_type("Bank Deposit") is None
    assert sfd._asset_type("Government SecuritiesMunicipal Security") is None


def test_band_floor_and_sentinels():
    assert _parse_band("None (or less than $1,001)") == (0, 1001)
    assert _parse_band("--") == (None, None)
    assert _parse_band("Unascertainable") == (None, None)


def test_classify_body_soft_block_vs_empty():
    # real report skeleton with an assets grid -> ok
    assert sfd._classify_body(GRID) == "ok"
    # real eFD report page (Part 3 / Part 4 headers) but no assets grid -> genuinely empty
    assert sfd._classify_body("<h2>Part 3. Assets</h2><h2>Part 4. Transactions</h2>") == "no_grid"
    # a WAF / interstitial / rate-limit body carries neither -> retriable soft_block
    assert sfd._classify_body("<title>Access Denied</title>") == "soft_block"
    assert sfd._classify_body("") == "soft_block"
