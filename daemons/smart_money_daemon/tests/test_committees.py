"""SM-C3 Phase R: committee membership join.

The load-bearing property is that a PARTIAL join must announce itself. A committee cut
that silently covers 68% of the corpus reads as complete.
"""
import os
import tempfile

from smart_money import committees as cm, db as dbmod, roster

COMMITTEES = [
    {"thomas_id": "HSAG", "name": "House Committee on Agriculture", "type": "house",
     "subcommittees": [{"thomas_id": "15", "name": "Forestry"}]},
    {"thomas_id": "SSAF", "name": "Senate Committee on Agriculture", "type": "senate",
     "subcommittees": []},
]
MEMBERSHIP = {
    "HSAG": [{"bioguide": "H001", "title": "Chairman", "rank": 1, "party": "majority"}],
    "HSAG15": [{"bioguide": "H001", "rank": 2, "party": "majority"}],
    "SSAF": [{"bioguide": "S001", "title": "Ranking Member", "rank": 1,
              "party": "minority"},
             {"bioguide": None, "rank": 9, "party": "majority"}],   # malformed
}


def _db():
    fd, p = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    return p


def test_flatten_recovers_the_parent_of_a_subcommittee_seat():
    """Subcommittee ids are parent+own (HSAG + 15). A seat must never lose its
    committee."""
    rows = cm.flatten(COMMITTEES, MEMBERSHIP)
    sub = [r for r in rows if r[1] == "HSAG15"][0]
    assert sub[2] == "HSAG", "parent recovered by prefix"
    assert sub[3] == "House Committee on Agriculture - Forestry"
    parent = [r for r in rows if r[1] == "HSAG"][0]
    assert parent[2] is None and parent[4] == "house"


def test_a_membership_row_without_a_bioguide_is_dropped_not_keyed_on_none():
    rows = cm.flatten(COMMITTEES, MEMBERSHIP)
    assert all(r[0] for r in rows), "no None bioguide keys"
    assert len([r for r in rows if r[1] == "SSAF"]) == 1


def test_sync_is_idempotent_and_reports_unknown_ids():
    p = _db()
    con = dbmod.connect(p)
    try:
        s1 = cm.sync(con, COMMITTEES, MEMBERSHIP)
        assert s1["memberships"] == 3 and s1["members"] == 2
        assert s1["unknown_ids"] == []
        n1 = con.execute("SELECT COUNT(*) FROM congress_committees").fetchone()[0]
        cm.sync(con, COMMITTEES, MEMBERSHIP)
        n2 = con.execute("SELECT COUNT(*) FROM congress_committees").fetchone()[0]
        assert n1 == n2 == 3, "a re-sync must not duplicate seats"
    finally:
        con.close()
        os.unlink(p)


def test_a_membership_id_missing_from_the_committee_file_is_reported():
    """Files drifting apart must be surfaced, not absorbed as a nameless row."""
    p = _db()
    con = dbmod.connect(p)
    try:
        s = cm.sync(con, COMMITTEES, dict(MEMBERSHIP, ZZZZ=[{"bioguide": "X001"}]))
        assert s["unknown_ids"] == ["ZZZZ"]
        txt = cm.render(cm.coverage(con), s)
        assert "UNNAMED committee ids" in txt and "ZZZZ" in txt
    finally:
        con.close()
        os.unlink(p)


_H = ("INSERT INTO congress_holdings(doc_id, chamber, coverage_year, member_last, "
      "member_first, state_dist, row_idx, asset_name, ticker, asset_type, value_lo, "
      "value_hi, ingested_at_unix) VALUES(?,?,?,?,'A',?,?,'Asset',?,'ST',1,2,0)")
_R = ("INSERT OR REPLACE INTO congress_member_roster(chamber, member_last, member_first, "
      "state_dist, party, state, match_kind, synced_at_unix, bioguide) "
      "VALUES(?,?,'A',?,'Republican','NC','unique',0,?)")


def _corpus(con):
    cm.sync(con, COMMITTEES, MEMBERSHIP)
    # seated: bioguide H001 has a committee. unseen: resolved but no seat. unmatched: no
    # bioguide at all (the candidate case).
    con.execute(_R, ("house", "Seated", "NC01", "H001"))
    con.execute(_R, ("house", "NoSeat", "NC02", "H999"))
    con.execute(_R, ("house", "Unmatched", "NC03", None))
    con.execute(_R, ("senate", "Sen", None, "S001"))
    for i, (last, sd, cham) in enumerate(
            [("Seated", "NC01", "house"), ("NoSeat", "NC02", "house"),
             ("Unmatched", "NC03", "house"), ("Sen", None, "senate")]):
        con.execute(_H, ("d%d" % i, cham, 2025, last, sd, i, "AAPL"))
        con.execute(_H, ("e%d" % i, cham, 2025, last, sd, 100 + i, None))
    con.commit()


def test_coverage_counts_only_filers_with_an_actual_committee_seat():
    """A resolved bioguide that sits on nothing is NOT covered. Counting it would
    overstate what a committee cut can see."""
    p = _db()
    con = dbmod.connect(p)
    try:
        _corpus(con)
        cov = cm.coverage(con)
        assert cov["house"]["rows"] == 6 and cov["house"]["with_committee"] == 2
        assert cov["house"]["members"] == 3
        assert cov["house"]["members_with_committee"] == 1
        assert cov["senate"]["with_committee"] == 2
    finally:
        con.close()
        os.unlink(p)


def test_coverage_reports_ticker_bearing_rows_separately():
    """Ticker rows are what a COMMITTEE x HOLDINGS cut actually operates on."""
    p = _db()
    con = dbmod.connect(p)
    try:
        _corpus(con)
        h = cm.coverage(con)["house"]
        assert h["tick_rows"] == 3 and h["tick_with_committee"] == 1
    finally:
        con.close()
        os.unlink(p)


def test_coverage_is_anchor_only_not_the_whole_history():
    p = _db()
    con = dbmod.connect(p)
    try:
        _corpus(con)
        con.execute(_H, ("old", "house", 2019, "Seated", "NC01", 900, "MSFT"))
        con.commit()
        assert cm.coverage(con)["house"]["rows"] == 6, "2019 is not an anchor year"
    finally:
        con.close()
        os.unlink(p)


def test_render_states_the_partial_join_and_the_current_congress_limit():
    """Both standing limits must be on the artifact, not in someone's head."""
    p = _db()
    con = dbmod.connect(p)
    try:
        _corpus(con)
        txt = cm.render(cm.coverage(con))
        assert "BLIND to the remainder" in txt
        assert "CURRENT-CONGRESS ONLY" in txt
        assert "never what they sat on in a past coverage year" in txt
        assert "cannot be attributed" in txt, "senate cause is not guessed at"
        assert "house" in txt and "senate" in txt and "ALL" in txt
    finally:
        con.close()
        os.unlink(p)


# ---- bioguide now survives the roster resolve ----

ENTRIES = [
    {"first": "Tim", "last": "Testman", "nick": None, "official": None,
     "bioguide": "T001", "type": "rep", "state": "NC", "district": 1,
     "party": "Republican", "end": "2027-01-03"},
    {"first": "Sam", "last": "Senator", "nick": None, "official": None,
     "bioguide": "S002", "type": "sen", "state": "NC", "district": None,
     "party": "Democrat", "end": "2029-01-03"},
]


def test_roster_sync_persists_the_bioguide():
    """Committee membership is organised by bioguide. Without it the join would have to
    re-run the whole name-matching argument a second time."""
    p = _db()
    con = dbmod.connect(p)
    try:
        con.execute(_H, ("d1", "house", 2025, "Testman", "NC01", 0, "AAPL"))
        con.commit()
        roster.sync(con, ENTRIES)
        row = con.execute("SELECT party, match_kind, bioguide FROM "
                          "congress_member_roster").fetchone()
        assert row[0] == "Republican" and row[2] == "T001", row
    finally:
        con.close()
        os.unlink(p)


def test_an_unmatched_identity_carries_no_bioguide():
    p = _db()
    con = dbmod.connect(p)
    try:
        con.execute(_H, ("d1", "house", 2025, "Nobody", "ZZ99", 0, "AAPL"))
        con.commit()
        roster.sync(con, ENTRIES)
        row = con.execute("SELECT match_kind, bioguide FROM "
                          "congress_member_roster").fetchone()
        assert row == ("unmatched", None), row
    finally:
        con.close()
        os.unlink(p)


def test_two_people_agreeing_on_party_do_not_yield_a_bioguide():
    """Same key, same party, two different people is a PARTY match, not an identity
    match. Attaching a committee on that basis would be a false claim about a person."""
    twins = [dict(ENTRIES[0], bioguide="T001", first="Tim", end="2019-01-03"),
             dict(ENTRIES[0], bioguide="T002", first="Tom", end="2027-01-03")]
    p = _db()
    con = dbmod.connect(p)
    try:
        con.execute(_H, ("d1", "house", 2025, "Testman", "NC01", 0, "AAPL"))
        con.commit()
        roster.sync(con, twins)
        party, bg = con.execute("SELECT party, bioguide FROM "
                                "congress_member_roster").fetchone()
        assert party == "Republican", "party still resolves"
        assert bg is None, "but the person does not"
    finally:
        con.close()
        os.unlink(p)


def test_uncovered_rows_are_split_into_two_named_causes():
    """A member who resolved cleanly but left Congress is NOT a roster-join failure.
    Collapsing the two causes makes the committee gap look like a matcher bug."""
    p = _db()
    con = dbmod.connect(p)
    try:
        _corpus(con)
        h = cm.coverage(con)["house"]
        # NoSeat resolved to H999 (a real person, no current seat) -> left_congress
        # Unmatched has no bioguide at all -> no_bioguide
        assert h["left_congress"] == 2 and h["no_bioguide"] == 2, h
        assert h["with_committee"] + h["left_congress"] + h["no_bioguide"] == h["rows"]
        txt = cm.render(cm.coverage(con))
        assert "no bioguide" in txt and "left Cong." in txt
        assert "strictly WORSE than that rate" in txt
    finally:
        con.close()
        os.unlink(p)


# ---- COMMITTEE x HOLDINGS cut ----

from smart_money import queries as q                                    # noqa: E402


def _cut_db():
    """Two seated members, so the detail view's min_holders=2 bar has something to
    clear - a committee cut showing single-holder rows would be noise."""
    p = _db()
    con = dbmod.connect(p)
    _corpus(con)
    con.execute(_H, ("x1", "house", 2025, "Seated", "NC01", 500, "MSFT"))
    con.execute(_R, ("house", "Seated2", "NC04", "H001"))
    con.execute(_H, ("x2", "house", 2025, "Seated2", "NC04", 501, "AAPL"))
    con.execute(_H, ("x3", "house", 2025, "Seated2", "NC04", 502, "MSFT"))
    con.commit()
    con.close()
    return p


def test_committee_roll_lists_only_what_is_joinable():
    p = _cut_db()
    ro = q.connect_ro(p)
    try:
        res = q.q_committee_holdings(ro)
        assert res["committee_id"] is None and res["count"] >= 1
        hsag = [r for r in res["rows"] if r["committee_id"] == "HSAG"][0]
        assert hsag["seats"] == 1 and hsag["filers_we_hold"] == 1
        assert hsag["committee_name"] == "House Committee on Agriculture"
    finally:
        ro.close()
        os.unlink(p)


def test_committee_cut_counts_only_seated_members_latest_year():
    p = _cut_db()
    ro = q.connect_ro(p)
    try:
        res = q.q_committee_holdings(ro, committee_id="HSAG")
        tk = {r["ticker"]: r for r in res["rows"]}
        assert set(tk) == {"AAPL", "MSFT"}, tk
        assert tk["AAPL"]["holder_count"] == 2, "both seated members, not the unseated"
        assert res["seats"] == 1, "one bioguide holds the seat; two identities map to it"
    finally:
        ro.close()
        os.unlink(p)


def test_committee_cut_carries_coverage_so_it_never_reads_as_complete():
    p = _cut_db()
    ro = q.connect_ro(p)
    try:
        res = q.q_committee_holdings(ro, committee_id="HSAG")
        assert res["coverage_pct"] is not None
        assert res["coverage"]["house"]["rows"] > res["coverage"]["house"][
            "with_committee"], "the corpus is genuinely wider than the cut"
        assert "CURRENT-Congress snapshot" in res["note"]
        assert "not evidence" in res["causal_note"]
    finally:
        ro.close()
        os.unlink(p)


def test_committee_cut_shows_the_anchor_year_spread():
    """Present-tense membership against dated holdings. The mismatch must be visible."""
    p = _db()
    con = dbmod.connect(p)
    _corpus(con)
    con.execute(_R, ("house", "Old", "NC09", "H001"))     # same seat, older anchor
    con.execute(_H, ("o1", "house", 2022, "Old", "NC09", 700, "AAPL"))
    con.commit()
    con.close()
    ro = q.connect_ro(p)
    try:
        res = q.q_committee_holdings(ro, committee_id="HSAG")
        assert res["anchor_years"] == [2022, 2025], res["anchor_years"]
        aapl = [r for r in res["rows"] if r["ticker"] == "AAPL"][0]
        assert aapl["anchor_years"] == "2022,2025"
    finally:
        ro.close()
        os.unlink(p)


def test_committee_cut_survives_committees_never_synced():
    """A surface must degrade to 'we do not have this', never to a silent empty."""
    p = _db()
    con = dbmod.connect(p)
    con.execute(_H, ("d1", "house", 2025, "Testman", "NC01", 0, "AAPL"))
    con.commit()
    con.close()
    ro = q.connect_ro(p)
    try:
        res = q.q_committee_holdings(ro)
        assert res["rows"] == [] and res["coverage_pct"] in (None, 0.0)
    finally:
        ro.close()
        os.unlink(p)


# ---- /committees view ----

def test_view_roll_and_detail_render_with_the_caveats():
    from smart_money import dashboard as dash
    p = _cut_db()
    ro = q.connect_ro(p)
    try:
        roll = dash.view_committees(ro, dash._params({}))
        assert "Committee roll" in roll and "House Committee on Agriculture" in roll
        assert "filers we hold" in roll
        detail = dash.view_committees(ro, dash._params({"cmte": ["HSAG"]}))
        assert "AAPL" in detail and "MSFT" in detail
        for txt in (roll, detail):
            assert "Partial by construction" in txt
            assert "Present-tense seats, dated holdings" in txt
            assert "not evidence of anything by itself" in txt
            assert "left" in txt and "Congress carries no seat" in txt
    finally:
        ro.close()
        os.unlink(p)


def test_view_is_registered_and_cmte_param_is_sanitised():
    from smart_money import dashboard as dash
    assert dash.ROUTES.get("/committees") is dash.view_committees
    assert dash._params({"cmte": ["HS'; DROP--"]})["cmte"] == "HSDROP"
    assert dash._params({})["cmte"] == ""


def test_view_says_so_when_committees_were_never_synced():
    from smart_money import dashboard as dash
    p = _db()
    con = dbmod.connect(p)
    con.execute(_H, ("d1", "house", 2025, "Testman", "NC01", 0, "AAPL"))
    con.commit()
    con.close()
    ro = q.connect_ro(p)
    try:
        out = dash.view_committees(ro, dash._params({}))
        assert "No committee membership synced yet" in out
    finally:
        ro.close()
        os.unlink(p)


def test_committee_csv_switches_columns_between_roll_and_detail():
    from smart_money import dashboard as dash
    p = _cut_db()
    ro = q.connect_ro(p)
    try:
        roll = dash._build_committees_csv(ro, dash._params({}), full=True)
        assert roll.splitlines()[0] == "committee_id,committee_name,seats,filers_we_hold"
        det = dash._build_committees_csv(ro, dash._params({"cmte": ["HSAG"]}), full=True)
        assert det.splitlines()[0] == ("ticker,instrument,holder_count,floor_exposure,"
                                       "anchor_years")
    finally:
        ro.close()
        os.unlink(p)
