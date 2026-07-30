"""Parse-time price/value sanity guard (SM Form 4 corruption fix).

Filer-side corruption in EDGAR Form 4 filings poisons the derived dollar value.
The guard quarantines an untrustworthy value (NULLs it, records a reason in
value_flag) while keeping raw shares/price. These tests pin every mechanism seen
in the corpus and the clean-row path.
"""
import os
import tempfile

from smart_money import db as dbmod
from smart_money.form4 import (parse_ownership, persist_transactions,
                               value_sanity_flag, PRICE_SANITY_MAX,
                               VALUE_SANITY_MAX)


def _fresh():
    fd, p = tempfile.mkstemp(suffix=".db"); os.close(fd); return p


def _doc(txn_xml):
    return ("""<?xml version="1.0"?>
<ownershipDocument>
 <aff10b5One>0</aff10b5One>
 <issuer><issuerCik>0000012345</issuerCik><issuerName>Acme Corp</issuerName>
  <issuerTradingSymbol>ACME</issuerTradingSymbol></issuer>
 <reportingOwner>
  <reportingOwnerId><rptOwnerCik>0001234567</rptOwnerCik>
   <rptOwnerName>Doe John</rptOwnerName></reportingOwnerId>
  <reportingOwnerRelationship><isDirector>1</isDirector></reportingOwnerRelationship>
 </reportingOwner>
 <nonDerivativeTable>""" + txn_xml + """
 </nonDerivativeTable>
</ownershipDocument>""")


# --- equity common-stock buy of 1000 @ $10 (clean) -------------------------
CLEAN = """
  <nonDerivativeTransaction>
   <securityTitle><value>Common Stock</value></securityTitle>
   <transactionCoding><transactionCode>P</transactionCode></transactionCoding>
   <transactionShares><value>1000</value></transactionShares>
   <transactionPricePerShare><value>10</value></transactionPricePerShare>
   <transactionDate><value>2026-06-10</value></transactionDate>
   <transactionAcquiredDisposedCode><value>A</value></transactionAcquiredDisposedCode>
   <postTransactionAmounts><sharesOwnedFollowingTransaction><value>5000</value>
    </sharesOwnedFollowingTransaction></postTransactionAmounts>
  </nonDerivativeTransaction>"""

# --- value-denominated debt note (FINS/KYN mechanism): principal in price,
#     valueOwnedFollowingTransaction instead of sharesOwnedFollowingTransaction
DEBT = """
  <nonDerivativeTransaction>
   <securityTitle><value>5.0% Senior Unsecured Notes due 2030</value></securityTitle>
   <transactionCoding><transactionCode>P</transactionCode></transactionCoding>
   <transactionShares><value>40000000</value></transactionShares>
   <transactionPricePerShare><value>40000000</value></transactionPricePerShare>
   <transactionDate><value>2026-07-08</value></transactionDate>
   <transactionAcquiredDisposedCode><value>A</value></transactionAcquiredDisposedCode>
   <postTransactionAmounts><valueOwnedFollowingTransaction><value>40000000</value>
    </valueOwnedFollowingTransaction></postTransactionAmounts>
  </nonDerivativeTransaction>"""

# --- total-proceeds-in-price-field, small share count (STNG mechanism):
#     price 1230435 (the total) > $1M ceiling, though shares only 15000
TOTAL_IN_PRICE = """
  <nonDerivativeTransaction>
   <securityTitle><value>Common Shares</value></securityTitle>
   <transactionCoding><transactionCode>S</transactionCode></transactionCoding>
   <transactionShares><value>15000</value></transactionShares>
   <transactionPricePerShare><value>1230435</value></transactionPricePerShare>
   <transactionDate><value>2026-06-22</value></transactionDate>
   <transactionAcquiredDisposedCode><value>D</value></transactionAcquiredDisposedCode>
   <postTransactionAmounts><sharesOwnedFollowingTransaction><value>62668</value>
    </sharesOwnedFollowingTransaction></postTransactionAmounts>
  </nonDerivativeTransaction>"""

# --- sub-ceiling per-share price but astronomical value: the last-resort
#     magnitude guard. price 2500 < $10k ceiling, but 60M shares * 2500 = 1.5e11
#     > $100B, so the value cap fires (shares- or value-field corruption).
VALUE_OVER = """
  <nonDerivativeTransaction>
   <securityTitle><value>Common Stock</value></securityTitle>
   <transactionCoding><transactionCode>P</transactionCode></transactionCoding>
   <transactionShares><value>60000000</value></transactionShares>
   <transactionPricePerShare><value>2500</value></transactionPricePerShare>
   <transactionDate><value>2026-10-17</value></transactionDate>
   <transactionAcquiredDisposedCode><value>A</value></transactionAcquiredDisposedCode>
   <postTransactionAmounts><sharesOwnedFollowingTransaction><value>60000000</value>
    </sharesOwnedFollowingTransaction></postTransactionAmounts>
  </nonDerivativeTransaction>"""


def test_value_sanity_flag_pure_cases():
    # clean equity trade -> trusted
    assert value_sanity_flag(1000, 10.0, 10000.0) is None
    # value-denominated debt -> flagged regardless of magnitude
    assert value_sanity_flag(40000000, 40000000, None, value_denominated=True) \
        == "value_denominated"
    # per-share price over the ceiling
    assert value_sanity_flag(15000, PRICE_SANITY_MAX + 1, 1.0) == "price_over_max"
    # AREB-class total-in-price ($50k/share on a ~$1 stock) is now caught
    assert value_sanity_flag(1316, 49999.99, 65_824_086.0) == "price_over_max"
    # BRK.A-scale price ($600k) is flagged for a normal ticker ...
    assert value_sanity_flag(10, 600000.0, 6_000_000.0) == "price_over_max"
    # ... but exempted when the caller marks it high-price-ok (BRK.A)
    assert value_sanity_flag(10, 600000.0, 6_000_000.0, high_price_ok=True) is None
    # sub-ceiling price but value over the cap
    assert value_sanity_flag(60000000, 2500.0, VALUE_SANITY_MAX + 1) == "value_over_max"
    # close cross-check: price 100x off the EOD close
    assert value_sanity_flag(1000, 5025.0, 5_025_000.0, close=50.25) == "price_vs_close"
    # close cross-check within band -> trusted
    assert value_sanity_flag(1000, 50.30, 50_300.0, close=50.25) is None


def test_parse_captures_value_denominated_and_title():
    p = parse_ownership(_doc(DEBT))
    t = p["txns"][0]
    assert t["value_denominated"] is True
    assert "Senior Unsecured Notes" in t["security_title"]
    q = parse_ownership(_doc(CLEAN))
    assert q["txns"][0]["value_denominated"] is False


def _persist(xml, ticker="ACME"):
    path = _fresh()
    con = dbmod.connect(path)
    persist_transactions(con, "0001-26-000001", parse_ownership(xml), ticker,
                         "2026-07-10")
    con.commit()
    row = con.execute(
        "SELECT shares, price, value, value_flag FROM form4_transactions "
        "WHERE tx_index=0").fetchone()
    con.close(); os.remove(path)
    return row  # (shares, price, value, value_flag)


# Common Stock sold at $600k/share — implausible for any US equity except BRK.A.
HIGH_PRICE = """
  <nonDerivativeTransaction>
   <securityTitle><value>Common Stock</value></securityTitle>
   <transactionCoding><transactionCode>S</transactionCode></transactionCoding>
   <transactionShares><value>10</value></transactionShares>
   <transactionPricePerShare><value>600000</value></transactionPricePerShare>
   <transactionDate><value>2026-06-10</value></transactionDate>
   <transactionAcquiredDisposedCode><value>D</value></transactionAcquiredDisposedCode>
   <postTransactionAmounts><sharesOwnedFollowingTransaction><value>100</value>
    </sharesOwnedFollowingTransaction></postTransactionAmounts>
  </nonDerivativeTransaction>"""


def test_persist_high_price_flagged_for_normal_ticker():
    shares, price, value, flag = _persist(_doc(HIGH_PRICE), ticker="ACME")
    assert flag == "price_over_max" and value is None


def test_persist_brk_a_bypasses_price_ceiling():
    # BRK.A genuinely trades ~$600k; the ticker allowlist exempts it (BRK.A / BRK-A
    # / BRKA all normalize to the same key).
    for tk in ("BRK.A", "BRK-A", "BRKA"):
        shares, price, value, flag = _persist(_doc(HIGH_PRICE), ticker=tk)
        assert flag is None and value == 6_000_000.0, (tk, flag, value)


def test_persist_clean_row_keeps_value():
    shares, price, value, flag = _persist(_doc(CLEAN))
    assert (shares, price, value, flag) == (1000.0, 10.0, 10000.0, None)


def test_persist_debt_quarantined_keeps_raw_inputs():
    shares, price, value, flag = _persist(_doc(DEBT))
    assert flag == "value_denominated"
    assert value is None            # derived dollar value withheld
    assert shares == 40000000.0     # raw inputs preserved for forensics
    assert price == 40000000.0


def test_persist_total_in_price_flagged():
    shares, price, value, flag = _persist(_doc(TOTAL_IN_PRICE))
    assert flag == "price_over_max"
    assert value is None and shares == 15000.0 and price == 1230435.0


def test_persist_value_over_cap_flagged():
    shares, price, value, flag = _persist(_doc(VALUE_OVER))
    assert flag == "value_over_max"
    assert value is None and shares == 60000000.0


def test_persist_close_crosscheck_flags_dropped_decimal():
    # A cheap stock with the decimal dropped (50.25 -> 5025) slips under both
    # ceilings; only the EOD-close cross-check catches it.
    path = _fresh()
    try:
        con = dbmod.connect(path)
        con.execute("INSERT INTO prices(ticker, date, close, adj_close, price_type,"
                    " asof_unix, fetched_at_unix, source) VALUES"
                    " ('ACME','2026-06-10',50.25,50.25,'eod',0,0,'test')")
        con.commit()
        xml = _doc("""
  <nonDerivativeTransaction>
   <securityTitle><value>Common Stock</value></securityTitle>
   <transactionCoding><transactionCode>P</transactionCode></transactionCoding>
   <transactionShares><value>1000</value></transactionShares>
   <transactionPricePerShare><value>5025</value></transactionPricePerShare>
   <transactionDate><value>2026-06-10</value></transactionDate>
   <transactionAcquiredDisposedCode><value>A</value></transactionAcquiredDisposedCode>
   <postTransactionAmounts><sharesOwnedFollowingTransaction><value>2000</value>
    </sharesOwnedFollowingTransaction></postTransactionAmounts>
  </nonDerivativeTransaction>""")
        persist_transactions(con, "0001-26-000002", parse_ownership(xml), "ACME",
                             "2026-07-10")
        con.commit()
        flag = con.execute("SELECT value_flag FROM form4_transactions "
                           "WHERE accession='0001-26-000002'").fetchone()[0]
        assert flag == "price_vs_close"
    finally:
        con.close(); os.remove(path)
