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
    # members_only=False keeps the candidate, so the unknown bucket is exercised
    row = [r for r in q.q_congress_breadth(ro, 1, members_only=False)["rows"]
           if r["ticker"] == "AAPL"][0]
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


def test_breadth_excludes_candidates_by_default(tmp_path):
    """A candidate who filed but never served is not an insider -> out of the breadth
    counts unless explicitly asked for."""
    path = str(tmp_path / "c.db")
    con = _seeded(path)
    roster.sync(con, entries=ENTRIES)
    con.commit()
    con.close()
    ro = q.connect_ro(path)
    only = [r for r in q.q_congress_breadth(ro, 1)["rows"] if r["ticker"] == "AAPL"][0]
    assert only["holder_count"] == 4 and only["party_unknown"] == 0, only
    allf = [r for r in q.q_congress_breadth(ro, 1, members_only=False)["rows"]
            if r["ticker"] == "AAPL"][0]
    assert allf["holder_count"] == 5 and allf["party_unknown"] == 1, allf
    ro.close()


def test_member_book_surfaces_spouse_proxy(tmp_path):
    """Owner is first-class: a book held mostly by a spouse must report that share, since
    for some members the positions are not in their own name at all."""
    path = str(tmp_path / "m.db")
    con = dbmod.connect(path)

    def hold(ridx, ticker, owner, lo, hi):
        con.execute(
            "INSERT OR REPLACE INTO congress_holdings(doc_id, chamber, filing_year, period, "
            "member_last, member_first, state_dist, person_id, row_idx, asset_name, ticker, "
            "asset_type, owner, value_lo, value_hi, income_type, ingested_at_unix) "
            "VALUES('d1','house',2025,'2025','Pelosi','Nancy','CA11',NULL,?,?,?,'ST',?,?,?,"
            "NULL,0)", (ridx, ticker + " Inc", ticker, owner, lo, hi))
    hold(0, "GOOGL", "SP", 5000001, 25000000)     # spouse
    hold(1, "AAPL", "SP", 5000001, 25000000)      # spouse
    hold(2, "MSFT", "Self", 1001, 15000)          # tiny self position
    con.commit()
    roster.sync(con, entries=ENTRIES)
    con.commit()
    con.close()
    ro = q.connect_ro(path)
    b = q.q_member_book(ro)
    assert b["member"].startswith("Pelosi") and b["party"] == "Democrat"
    assert b["count"] == 3 and b["year"] == 2025
    assert b["owner_split"]["spouse"] > b["owner_split"]["self"]
    assert b["spouse_share"] > 99, b            # overwhelmingly not in her own name
    assert b["proxy_share"] > 99, b
    # rows carry owner + a band-midpoint value and sort biggest-first
    assert b["rows"][0]["owner"] == "spouse" and b["rows"][0]["midpoint"] == 15000000
    assert abs(sum(r["pct_of_book"] for r in b["rows"]) - 100) < 0.1
    ro.close()


def test_member_book_lists_only_confirmed_members(tmp_path):
    """The member picker must not offer candidates."""
    path = str(tmp_path / "p.db")
    con = _seeded(path)
    roster.sync(con, entries=ENTRIES)
    con.commit()
    con.close()
    ro = q.connect_ro(path)
    labels = [m["label"] for m in q.q_member_book(ro)["members"]]
    assert any(l.startswith("Pelosi") for l in labels)
    assert not any(l.startswith("Shulli") for l in labels), labels
    ro.close()


_REAL = [
    # Jim Jordan: the seat has many historical OH "Jordan" rows of differing parties, so
    # a surname+STATE key is ambiguous and the filer's legal "James D" never matches the
    # roster's common "Jim". Only the DISTRICT key resolves him.
    {"first": "Jim", "last": "Jordan", "nick": None, "official": "Jim Jordan",
     "type": "rep", "state": "OH", "district": 4, "party": "Republican",
     "end": "2027-01-03"},
    {"first": "Edward", "last": "Jordan", "nick": None, "official": "Edward Jordan",
     "type": "rep", "state": "OH", "district": 9, "party": "Democrat",
     "end": "1885-03-03"},
    # accented surname — the filing writes it unaccented
    {"first": "Nanette", "last": "Barragán", "nick": None,
     "official": "Nanette Barragán", "type": "rep", "state": "CA", "district": 44,
     "party": "Democrat", "end": "2027-01-03"},
    # compound surname the roster files under one part
    {"first": "Anna Paulina", "last": "Luna", "nick": None,
     "official": "Anna Paulina Luna", "type": "rep", "state": "FL", "district": 13,
     "party": "Republican", "end": "2027-01-03"},
]


def test_district_key_resolves_where_state_key_is_ambiguous():
    idx = roster.build_index(_REAL)
    r = roster.resolve(idx, "house", "Jordan", "James D", "OH04")
    assert r["party"] == "Republican" and r["match_kind"] == "unique", r
    # the same surname in a DIFFERENT district must not borrow that answer
    r2 = roster.resolve(idx, "house", "Jordan", "James D", "OH09")
    assert r2["party"] == "Democrat", r2


def test_accented_surname_folds_not_strips():
    """Stripping non-ASCII turned 'Barragan'+acute into 'barragn' and lost a sitting
    member; it must FOLD to 'barragan'."""
    assert roster.norm("Barragán") == "barragan"
    idx = roster.build_index(_REAL)
    r = roster.resolve(idx, "house", "Barragan", "Nanette", "CA44")
    assert r["party"] == "Democrat", r


def test_compound_surname_falls_back_to_trailing_token():
    assert roster.surname_keys("Paulina Luna") == ["paulina luna", "luna"]
    idx = roster.build_index(_REAL)
    r = roster.resolve(idx, "house", "Paulina Luna", "Anna", "FL13")
    assert r["party"] == "Republican", r
    # the LEADING token must never be a key, or 'Van Duyne' would match every 'Van'
    assert "van" not in roster.surname_keys("Van Duyne")


def test_candidate_for_a_seat_still_does_not_resolve():
    """A candidate files listing the district they are RUNNING for. Nobody by that
    surname ever held it, so they must stay unmatched."""
    idx = roster.build_index(_REAL)
    r = roster.resolve(idx, "house", "Finnie", "Shaun", "OH04")
    assert r["party"] is None and r["match_kind"] == "unmatched", r


def test_sync_is_a_full_rebuild_not_an_append(tmp_path):
    """state_dist is NULL for Senate identities and SQLite treats NULL != NULL in a
    PRIMARY KEY, so INSERT OR REPLACE alone duplicated every Senate row per sync."""
    con = _seeded(str(tmp_path / "dup.db"))
    roster.sync(con, entries=ENTRIES)
    n1 = con.execute("SELECT count(*) FROM congress_member_roster").fetchone()[0]
    roster.sync(con, entries=ENTRIES)
    roster.sync(con, entries=ENTRIES)
    n3 = con.execute("SELECT count(*) FROM congress_member_roster").fetchone()[0]
    assert n1 == n3, "roster table grew across syncs"
    con.close()


def test_norm_strips_suffixes():
    assert roster.norm("Fleming, Jr") == "fleming"
    assert roster.norm("McConnell, Jr.") == "mcconnell"
    assert roster.norm("Hagerty, IV") == "hagerty"
    assert roster.norm("O'Rourke") == "orourke"
