"""SM-C2 P3 roster resolution tests. Stubbed roster entries, no network."""
from smart_money import db as dbmod, queries as q, roster

ENTRIES = [
    {"first": "Nancy", "last": "Pelosi", "nick": None, "official": "Nancy Pelosi",
     "type": "rep", "state": "CA", "district": 11, "party": "Democrat",
     "end": "2027-01-03"},
    {"first": "Mark", "last": "Green", "nick": None, "official": "Mark E. Green",
     "type": "rep", "state": "TN", "district": 7, "party": "Republican",
     "end": "2027-01-03"},
    # roster carries the COMMON name; filers write the LEGAL one ("Thomas H")
    {"first": "Tommy", "last": "Tuberville", "nick": None, "official": "Tommy Tuberville",
     "type": "sen", "state": "AL", "party": "Republican", "end": "2027-01-03"},
    {"first": "Mark", "last": "Warner", "nick": None, "official": "Mark R. Warner",
     "type": "sen", "state": "VA", "party": "Democrat", "end": "2027-01-03"},
    # historical House namesake in another state — must not leak into the Senate key
    {"first": "John", "last": "Warner", "nick": None, "official": "John Warner",
     "type": "rep", "state": "NY", "party": "Republican", "end": "1901-03-03"},
    # a long-retired senator: outside SENATE_RECENT_FLOOR, so not a Senate key candidate
    {"first": "Absalom", "last": "Oldname", "nick": None, "official": "Absalom Oldname",
     "type": "sen", "state": "OH", "party": "Whig", "end": "1850-03-03"},
]


def _add(con, doc, chamber, last, first, state, ticker="AAPL", yr=2026):
    con.execute(
        "INSERT OR REPLACE INTO congress_holdings(doc_id, chamber, filing_year, period, "
        "member_last, member_first, state_dist, person_id, row_idx, asset_name, ticker, "
        "asset_type, owner, value_lo, value_hi, income_type, ingested_at_unix) "
        "VALUES(?,?,?,?,?,?,?,NULL,0,?,?,'ST','Self',1001,15000,NULL,0)",
        (doc, chamber, yr, str(yr), last, first, state, ticker + " Inc", ticker))


def _seeded(path):
    con = dbmod.connect(path)
    _add(con, "h1", "house", "Pelosi", "Nancy", "CA11")
    _add(con, "h2", "house", "Green", "Mark", "TN07")
    _add(con, "s1", "senate", "Tuberville", "Thomas H", None)   # legal vs common name
    _add(con, "s2", "senate", "Warner", "Mark R", None)
    _add(con, "c1", "senate", "Shulli", "John", None)           # candidate, never served
    con.commit()
    return con


def test_resolve_deterministic_keys_only(tmp_path):
    con = _seeded(str(tmp_path / "r.db"))
    tally = roster.sync(con, entries=ENTRIES)
    got = dict(con.execute("SELECT member_last, party FROM congress_member_roster"))
    kinds = dict(con.execute("SELECT member_last, match_kind FROM congress_member_roster"))
    assert got["Pelosi"] == "Democrat" and got["Green"] == "Republican"
    # surname key bridges legal->common name without any fuzzy guess
    assert got["Tuberville"] == "Republican"
    # the historical House Warner (NY) must not leak into the Senate surname key
    assert got["Warner"] == "Democrat"
    # a filer who never served resolves to NOTHING — never guessed
    assert got["Shulli"] is None and kinds["Shulli"] == "unmatched"
    assert tally["unmatched"] == 1 and tally["unique"] == 4
    con.close()


def test_house_state_key_requires_state_agreement(tmp_path):
    """A House surname that matches only in a DIFFERENT state must not resolve."""
    con = dbmod.connect(str(tmp_path / "s.db"))
    _add(con, "h9", "house", "Pelosi", "Nancy", "TX02")     # right name, wrong state
    con.commit()
    roster.sync(con, entries=ENTRIES)
    assert con.execute("SELECT party FROM congress_member_roster").fetchone()[0] is None
    con.close()


def test_breadth_party_split_reconciles(tmp_path):
    path = str(tmp_path / "b.db")
    con = _seeded(path)
    roster.sync(con, entries=ENTRIES)
    con.commit()
    con.close()
    ro = q.connect_ro(path)
    row = [r for r in q.q_congress_breadth(ro, 1)["rows"] if r["ticker"] == "AAPL"][0]
    assert row["holder_count"] == 5 and row["house"] == 2 and row["senate"] == 3
    assert row["dem"] == 2 and row["rep"] == 2 and row["party_unknown"] == 1
    # the party split must always account for every holder
    assert row["dem"] + row["rep"] + row["ind"] + row["party_unknown"] == row["holder_count"]
    ro.close()


def test_gaps_roll_lists_every_identity(tmp_path):
    path = str(tmp_path / "g.db")
    con = _seeded(path)
    roster.sync(con, entries=ENTRIES)
    con.commit()
    con.close()
    ro = q.connect_ro(path)
    g = q.q_congress_gaps(ro)
    assert g["count"] == 5 and g["resolved"] == 4 and g["unresolved"] == 1
    shulli = [r for r in g["rows"] if r["member"].startswith("Shulli")][0]
    assert shulli["party"] is None and shulli["match_kind"] == "unmatched"
    assert "FLOORS" in g["note"]
    ro.close()


def test_breadth_degrades_when_roster_never_synced(tmp_path):
    """No roster table content -> every holder is party_unknown, never a wrong label."""
    path = str(tmp_path / "n.db")
    con = _seeded(path)
    con.commit()
    con.close()
    ro = q.connect_ro(path)
    row = [r for r in q.q_congress_breadth(ro, 1)["rows"] if r["ticker"] == "AAPL"][0]
    assert row["party_unknown"] == row["holder_count"] == 5
    assert row["dem"] == 0 and row["rep"] == 0
    ro.close()


def test_norm_strips_suffixes():
    assert roster.norm("Fleming, Jr") == "fleming"
    assert roster.norm("McConnell, Jr.") == "mcconnell"
    assert roster.norm("Hagerty, IV") == "hagerty"
    assert roster.norm("O'Rourke") == "orourke"
