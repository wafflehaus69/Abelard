"""Per-line Clerk filing-status capture, supersession, and the residue label.

Anchored on real documents. Blumenauer OR03 filed 20019580 (2021-10-05, six
lines all 'New'), then on 2021-10-12 filed 20019618 deleting all six by Clerk
line ID and 20019619 re-filing them as new. Two of those six lines are Dutch
Bros, and the source PDF genuinely lists that asset twice — verified against
the filing — so within-filing repeats must survive.
"""
import os
import tempfile

from smart_money import db as dbmod
from smart_money.amendments import apply_supersedes
from smart_money.house_ingest import STATUS_LINE_RE, normalize_row
from smart_money.queries import annotate_potential_amendments


# ---------------------------------------------------------------- parsing ---

def test_status_line_matches_both_pdf_vintages():
    """Pre-2022 PDFs extract 'FIlINg STATuS:'; 2022+ decode to NULs which
    _lines strips to 'F S:'. Missing either vintage loses every amendment on
    that side of the cutover — 2023 onward is 100% the mangled form."""
    for line, want in (
        ("FIlINg STATuS: Deleted", "Deleted"),
        ("FIlINg STATuS: New", "New"),
        ("F S: Amended", "Amended"),
        ("F S: New", "New"),
        ("FILING STATUS: Amended", "Amended"),
    ):
        m = STATUS_LINE_RE.match(line)
        assert m, "unmatched caption vintage: {!r}".format(line)
        assert m.group(1).capitalize() == want


def test_status_line_does_not_match_other_annotations():
    for line in ("S O: R.W. Allen & Associates, Inc.", "D: Part of my spouse's",
                 "DESCRIPTION: New York holdings", "L: something"):
        assert not STATUS_LINE_RE.match(line), line


def _row(asset, amount="$1,001 - $15,000", tx="09/15/2021", **kw):
    r = {"asset": asset, "amount": [amount], "tx_type": "P",
         "tx_date": tx, "notif_date": "10/05/2021", "owner": "SP"}
    r.update(kw)
    return r


def test_annotations_are_lifted_out_of_the_asset_name():
    """Older filings bleed the caption into the asset column; that is how
    'Dutch Bros Inc. Class A (BROS) [ST] FIlINg STATuS: New DESCRIPTION: ...'
    reached the screen."""
    n = normalize_row(_row(
        ["Dutch", "Bros", "Inc.", "Class", "A", "(BROS)", "[ST]", "FIlINg",
         "STATuS:", "New", "DESCRIPTION:", "Part", "of", "my", "retirement"]),
        "20019580")
    assert n["asset_name"] == "Dutch Bros Inc. Class A (BROS) [ST]"
    assert n["filing_status"] == "New"
    assert "FIlINg" not in n["asset_name"]
    assert "DESCRIPTION" not in n["asset_name"]
    assert "Part of my retirement" in (n["comment"] or "")
    assert n["ticker"] == "BROS"


def test_status_from_parser_wins_over_text_scrape():
    n = normalize_row(_row(["Acme", "(ACME)", "[ST]"], filing_status="Deleted"),
                      "X")
    assert n["filing_status"] == "Deleted"


def test_clerk_line_id_passed_through():
    """Present only on lines that amend or delete a prior record, which makes
    its presence positive evidence rather than an inference."""
    n = normalize_row(_row(["Acme", "(ACME)", "[ST]"], clerk_id="2000085014"), "X")
    assert n["clerk_line_id"] == "2000085014"
    assert normalize_row(_row(["Acme", "(ACME)", "[ST]"]), "X")["clerk_line_id"] is None


def test_description_parens_cannot_hijack_the_ticker():
    """Descriptions are free text and do contain parentheses; the ticker regex
    must never see them."""
    n = normalize_row(_row(
        ["Vanguard", "Total", "(VTI)", "[ST]", "DESCRIPTION:", "held", "at",
         "(Fidelity)"]), "X")
    assert n["ticker"] == "VTI"


# ----------------------------------------------------------- supersession ---

def _mk(con, pid, name):
    con.execute("INSERT OR IGNORE INTO persons(person_id, name, type, "
                "cik_or_chamber) VALUES (?,?,'congress','house')", (pid, name))


def _filing(con, fid, filed, label="PTR OR03 2021"):
    con.execute("INSERT INTO ingested_filings(filing_id, chamber, status, "
                "report_label, filed_date, ingested_at_unix) VALUES "
                "(?,?,?,?,?,0)", (fid, "house", "electronic", label, filed))


def _trade(con, pid, fid, ref, status=None, disc="2021-10-05", ticker="BROS"):
    con.execute(
        "INSERT INTO congress_trades(person_id, ticker, side, amt_low, amt_high,"
        " tx_date, disclosure_date, lag_days, chamber, source, raw_ref, "
        "asset_type, filing_id, filing_status) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (pid, ticker, "purchase", 1001, 15000, "2021-09-15", disc, 20, "house",
         "house_clerk", ref, "Stock", fid, status))


def _db():
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    return path


def _bros(con):
    """The real three-filing sequence."""
    _mk(con, 139, "Blumenauer, Earl")
    _filing(con, "20019580", "2021-10-05")
    _filing(con, "20019618", "2021-10-12")
    _filing(con, "20019619", "2021-10-12")
    for ref in ("#2", "#3"):
        _trade(con, 139, "20019580", "20019580" + ref, "New", "2021-10-05")
        _trade(con, 139, "20019618", "20019618" + ref, "Deleted", "2021-10-12")
        _trade(con, 139, "20019619", "20019619" + ref, "New", "2021-10-12")
    con.commit()


def test_bros_sequence_resolves_to_the_refiled_pair():
    path = _db()
    try:
        con = dbmod.connect(path)
        _bros(con)
        apply_supersedes(con)
        live = con.execute("SELECT filing_id, COUNT(*) FROM congress_trades "
                           "WHERE superseded=0 GROUP BY filing_id").fetchall()
        assert live == [("20019619", 2)], live
    finally:
        os.remove(path)


def test_within_filing_repeats_survive_together():
    """The source lists Dutch Bros twice in one filing. Whole filings win or
    lose; superseding individual rows would drop a real disclosure."""
    path = _db()
    try:
        con = dbmod.connect(path)
        _bros(con)
        apply_supersedes(con)
        n = con.execute("SELECT COUNT(*) FROM congress_trades WHERE "
                        "superseded=0 AND filing_id='20019619'").fetchone()[0]
        assert n == 2, "both real lines must survive, not one"
    finally:
        os.remove(path)


def test_deleted_line_is_never_live_even_without_an_original():
    """A retraction is not a transaction. Scoring it live would invent one the
    filer explicitly withdrew."""
    path = _db()
    try:
        con = dbmod.connect(path)
        _mk(con, 1, "Rep A")
        _filing(con, "D1", "2025-03-01")
        _trade(con, 1, "D1", "D1#1", "Deleted")
        con.commit()
        apply_supersedes(con)
        assert con.execute("SELECT superseded FROM congress_trades"
                           ).fetchone()[0] == 1
    finally:
        os.remove(path)


def test_plain_new_status_is_not_amendment_evidence():
    """12,166 of 12,289 tokens are 'New'. Treating that as an amendment signal
    would supersede ordinary independent filings."""
    path = _db()
    try:
        con = dbmod.connect(path)
        _mk(con, 1, "Rep A")
        _filing(con, "N1", "2025-02-01")
        _filing(con, "N2", "2025-03-01")
        _trade(con, 1, "N1", "N1#1", "New", "2025-02-01")
        _trade(con, 1, "N2", "N2#1", "New", "2025-03-01")
        con.commit()
        stats = apply_supersedes(con)
        assert stats["superseded"] == 0, stats
    finally:
        os.remove(path)


def test_legacy_report_label_path_still_supersedes():
    """Regression: the old gate catches a handful of filings and must keep
    working after the repoint."""
    path = _db()
    try:
        con = dbmod.connect(path)
        _mk(con, 1, "Rep A")
        _filing(con, "F1", "2025-02-01", "Periodic Transaction Report")
        _filing(con, "F2", "2025-03-01", "Periodic Transaction Report Amendment")
        _trade(con, 1, "F1", "F1#1", None, "2025-02-01")
        _trade(con, 1, "F2", "F2#1", None, "2025-03-01")
        con.commit()
        stats = apply_supersedes(con)
        assert stats["superseded"] == 1, stats
        rows = dict(con.execute("SELECT filing_id, superseded FROM "
                                "congress_trades").fetchall())
        assert rows["F1"] == 1 and rows["F2"] == 0
    finally:
        os.remove(path)


def test_supersede_is_idempotent():
    path = _db()
    try:
        con = dbmod.connect(path)
        _bros(con)
        assert apply_supersedes(con) == apply_supersedes(con)
    finally:
        os.remove(path)


# --------------------------------------------------------- residue labels ---

def _crow(name, disc, fid, tx="2021-02-10"):
    return {"name": name, "side": "purchase", "amt_low": 1001, "amt_high": 15000,
            "tx_date": tx, "disclosure_date": disc, "owner": "SP",
            "filing_id": fid, "filing_status": None}


def test_cross_filing_repeat_is_labelled_with_the_earlier_date():
    rows = annotate_potential_amendments([
        _crow("Allen, Richard W.", "2021-03-14", "20018393"),
        _crow("Allen, Richard W.", "2023-08-10", "20023082"),
    ])
    early = [r for r in rows if r["disclosure_date"] == "2021-03-14"][0]
    late = [r for r in rows if r["disclosure_date"] == "2023-08-10"][0]
    assert "note" not in early, "the original disclosure must stay unmarked"
    assert late["note"] == "*POTENTIAL AMENDMENT* DTD 03/14/21", late


def test_within_filing_repeat_is_not_labelled():
    """Same filing, two lines: the source says two transactions. Not a repeat."""
    rows = annotate_potential_amendments([
        _crow("Blumenauer, Earl", "2021-10-12", "20019619"),
        _crow("Blumenauer, Earl", "2021-10-12", "20019619"),
    ])
    assert all("note" not in r for r in rows)


def test_distinct_transactions_are_not_labelled():
    rows = annotate_potential_amendments([
        _crow("Rep A", "2021-03-14", "F1", tx="2021-02-10"),
        _crow("Rep A", "2023-08-10", "F2", tx="2021-02-26"),
    ])
    assert all("note" not in r for r in rows)


def test_label_does_not_drop_or_merge_rows():
    """Mark, never drop: the row count out equals the row count in."""
    rows = annotate_potential_amendments([
        _crow("Rep A", "2021-03-14", "F1"),
        _crow("Rep A", "2023-08-10", "F2"),
        _crow("Rep A", "2024-01-02", "F3"),
    ])
    assert len(rows) == 3
    assert sum(1 for r in rows if "note" in r) == 2
