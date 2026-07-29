"""SM-O1 P1 Table II tests: parse_ownership extracts derivative rows, and
persist_derivatives is idempotent by (accession, tx_index)."""
import os
import tempfile

from smart_money import db as dbmod
from smart_money import form4

DERIV_XML = """<ownershipDocument>
  <issuer>
    <issuerName>Acme Corp</issuerName>
    <issuerCik>0000000999</issuerCik>
    <issuerTradingSymbol>ACME</issuerTradingSymbol>
  </issuer>
  <reportingOwner>
    <reportingOwnerId>
      <rptOwnerName>Doe John</rptOwnerName>
      <rptOwnerCik>0000001111</rptOwnerCik>
    </reportingOwnerId>
    <reportingOwnerRelationship>
      <isOfficer>1</isOfficer>
      <officerTitle>CEO</officerTitle>
    </reportingOwnerRelationship>
  </reportingOwner>
  <aff10b5One>0</aff10b5One>
  <derivativeTable>
    <derivativeTransaction>
      <securityTitle><value>Employee Stock Option</value></securityTitle>
      <conversionOrExercisePrice><value>21.20</value></conversionOrExercisePrice>
      <transactionDate><value>2026-07-24</value></transactionDate>
      <exerciseDate><value>2019-04-23</value></exerciseDate>
      <expirationDate><value>2029-04-22</value></expirationDate>
      <transactionCoding><transactionCode>M</transactionCode></transactionCoding>
      <transactionAmounts>
        <transactionShares><value>1500</value></transactionShares>
        <transactionPricePerShare><value>21.20</value></transactionPricePerShare>
      </transactionAmounts>
      <underlyingSecurity>
        <underlyingSecurityTitle><value>Common Stock</value></underlyingSecurityTitle>
        <underlyingSecurityShares><value>1500</value></underlyingSecurityShares>
      </underlyingSecurity>
    </derivativeTransaction>
  </derivativeTable>
</ownershipDocument>"""


def test_parse_extracts_derivative_fields():
    p = form4.parse_ownership(DERIV_XML)
    assert p["txns"] == [], "no non-derivative transactions in this fixture"
    assert len(p["deriv_txns"]) == 1, p["deriv_txns"]
    d = p["deriv_txns"][0]
    assert d["security_title"] == "Employee Stock Option", d
    assert d["exercise_price"] == "21.20", d
    assert d["code"] == "M", d
    assert d["shares"] == "1500", d
    assert d["exercise_date"] == "2019-04-23", d
    assert d["expiration_date"] == "2029-04-22", d
    assert d["underlying_title"] == "Common Stock", d
    assert d["underlying_shares"] == "1500", d


def test_persist_derivatives_idempotent_and_typed():
    path = tempfile.mktemp(suffix=".db")
    con = dbmod.connect(path)
    parsed = form4.parse_ownership(DERIV_XML)
    n1 = form4.persist_derivatives(con, "ACC-1", parsed, "ACME", "2026-07-25")
    con.commit()
    n2 = form4.persist_derivatives(con, "ACC-1", parsed, "ACME", "2026-07-25")  # rerun
    con.commit()
    rows = con.execute("SELECT COUNT(*) FROM form4_derivatives").fetchone()[0]
    assert n1 == 1 and n2 == 1, (n1, n2)      # each call iterates one txn
    assert rows == 1, "two runs must leave ONE row (idempotent by PK)"
    r = con.execute("SELECT security_title, code, exercise_price, expiration_date, "
                    "underlying_shares, issuer_cik, ingest_regime "
                    "FROM form4_derivatives").fetchone()
    assert r == ("Employee Stock Option", "M", 21.2, "2029-04-22", 1500.0,
                 "999", "watchlist"), r
    con.close()
    os.unlink(path)
