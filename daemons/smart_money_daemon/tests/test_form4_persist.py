"""SM-F4 Step 1: Form 4 persistence idempotency + insider person upsert."""
import os
import tempfile

from smart_money import db as dbmod
from smart_money.form4 import persist_transactions, parse_ownership

SAMPLE_XML = """<?xml version="1.0"?>
<ownershipDocument>
 <aff10b5One>0</aff10b5One>
 <issuer><issuerCik>0000012345</issuerCik><issuerName>Acme Corp</issuerName>
  <issuerTradingSymbol>ACME</issuerTradingSymbol></issuer>
 <reportingOwner>
  <reportingOwnerId><rptOwnerCik>0001234567</rptOwnerCik>
   <rptOwnerName>Doe John</rptOwnerName></reportingOwnerId>
  <reportingOwnerRelationship><isDirector>0</isDirector>
   <isOfficer>1</isOfficer><officerTitle>CEO</officerTitle>
   <isTenPercentOwner>1</isTenPercentOwner></reportingOwnerRelationship>
 </reportingOwner>
 <nonDerivativeTable>
  <nonDerivativeTransaction>
   <transactionCoding><transactionCode>P</transactionCode></transactionCoding>
   <transactionShares><value>1000</value></transactionShares>
   <transactionPricePerShare><value>10</value></transactionPricePerShare>
   <transactionDate><value>2026-06-10</value></transactionDate>
   <transactionAcquiredDisposedCode><value>A</value></transactionAcquiredDisposedCode>
   <postTransactionAmounts><sharesOwnedFollowingTransaction><value>5000</value>
    </sharesOwnedFollowingTransaction></postTransactionAmounts>
  </nonDerivativeTransaction>
  <nonDerivativeTransaction>
   <transactionCoding><transactionCode>A</transactionCode></transactionCoding>
   <transactionShares><value>200</value></transactionShares>
   <transactionPricePerShare><value>0</value></transactionPricePerShare>
   <transactionDate><value>2026-06-11</value></transactionDate>
  </nonDerivativeTransaction>
 </nonDerivativeTable>
</ownershipDocument>"""


def _fresh():
    fd, p = tempfile.mkstemp(suffix=".db"); os.close(fd); return p


def test_parse_captures_cik_role_ownership_after():
    p = parse_ownership(SAMPLE_XML)
    assert p["owner_cik"] == "0001234567"
    assert p["issuer_cik"] == "12345"  # zero-stripped issuer CIK
    assert "officer:CEO" in p["role"] and "10pct" in p["role"]
    assert p["txns"][0]["owned_after"] == "5000"


def test_persist_stores_issuer_cik():
    path = _fresh()
    try:
        con = dbmod.connect(path)
        persist_transactions(con, "0001-26-000009", parse_ownership(SAMPLE_XML),
                             "ACME", "2026-06-12")
        con.commit()
        v = con.execute(
            "SELECT DISTINCT issuer_cik FROM form4_transactions").fetchone()[0]
        assert v == "12345", v
    finally:
        os.remove(path)


def test_persist_idempotent_all_codes_and_person_upsert():
    path = _fresh()
    try:
        con = dbmod.connect(path)
        parsed = parse_ownership(SAMPLE_XML)
        n1, _ = persist_transactions(con, "0001-26-000001", parsed, "ACME", "2026-06-12")
        con.commit()
        assert n1 == 2  # BOTH codes persisted (P and A), corpus is complete
        rows = con.execute("SELECT COUNT(*) FROM form4_transactions").fetchone()[0]
        assert rows == 2
        # insider person upserted with CIK, congress rows untouched
        pr = con.execute(
            "SELECT type, cik_or_chamber FROM persons WHERE name='Doe John'").fetchone()
        assert pr == ("insider", "0001234567")
        # value computed
        v = con.execute(
            "SELECT value FROM form4_transactions WHERE code='P'").fetchone()[0]
        assert v == 10000.0
        # re-persist same filing -> idempotent, no new rows
        persist_transactions(con, "0001-26-000001", parsed, "ACME", "2026-06-12")
        con.commit()
        assert con.execute(
            "SELECT COUNT(*) FROM form4_transactions").fetchone()[0] == 2
    finally:
        os.remove(path)
