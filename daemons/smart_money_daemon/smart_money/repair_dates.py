"""Dates already stored: find every one that cannot be true, and deal with it by class.

A dry run (the default) opens the database READ-ONLY and changes nothing -- not a row,
not the schema. It works on a database that has never been migrated.

--apply takes a full backup through a plain connection FIRST, before the schema
migration and before any row changes, so the snapshot is the database exactly as it
was. It then migrates, re-scans, and applies four passes of very different risk:

  1. NORMALISE   EDGAR's timezone designator on a Form 4 trade date ('2026-07-03-05:00')
                 is cut, leaving the date. Parser-caused and lossless: the offset carries
                 nothing for a date-only field, and every reader already compares on the
                 date. The original strings go to an artifact before any change.
  2. QUARANTINE  A row whose dates cannot all be true gets date_flag and its reason:
                 out of range, a filing date that is itself impossible, or a trade dated
                 after the filing that reports it. That is a quarantine on the TRADE
                 clock only -- the row stays a full disclosure on the filing clock --
                 and the dates are NOT corrected. date_subclass records what the string
                 looks like, never a cause; a year-slip row also carries
                 tx_date_suggested, a labelled hypothesis that is never applied.
                 scorecard.py met the 3031 row first and wrote the house answer beside
                 it -- "Drop and count, never silently coerce" -- and the order agrees:
                 fix what the parser caused, quarantine the rest. A congress row's
                 lag_days is left as computed: the column is NOT NULL, and date_flag is
                 what says -368,891 is not a lag.
  2b. MARK       A purchase or sale (Form 4 code P or S, a congressional stock trade)
                 dated on a day with no US session gets date_subclass and NO flag. The
                 mark states what the string shows, not that the date is wrong, and the
                 row stays on every board.
  3. OGE         filed_date rewritten M/D/YYYY -> ISO, with the label's string copied
                 into filed_date_raw FIRST, so the conversion destroys nothing.
  4. CACHES      Derived verdicts computed from a bad date are listed. ticker_status is
                 recomputed only with --reclassify, because that probes Yahoo.

And a residue report that changes nothing: rows no guard can catch.
"""
import argparse
import collections
import csv
import datetime as dt
import os
import sqlite3
import sys

from . import dates
from . import db as dbmod


def _rows(con, sql, params=()):
    prev = con.row_factory
    con.row_factory = sqlite3.Row
    try:
        return [dict(r) for r in con.execute(sql, params)]
    finally:
        con.row_factory = prev


def _cols(con, table):
    return {r[1] for r in con.execute("PRAGMA table_info({})".format(table))}


def _unmarked(con, table):
    """The rows the repair may still judge: no flag and no sub-class. On a database
    never migrated the columns do not exist, and every row is unmarked."""
    cols = _cols(con, table)
    bits = [c + " IS NULL" for c in ("date_flag", "date_subclass") if c in cols]
    return " AND ".join(bits) or "1=1"


def scan(con, today=None):
    """Judge every stored row through dates.judge -- the function the writers use, so
    the repair and a fresh ingest cannot classify the same row differently."""
    from .efd_ingest import MARKET_ASSET_TYPE
    from .form4 import OPEN_MARKET
    found = {"tz": [], "quarantine": [], "marks": [], "oge": [], "caches": [],
             "residue": {}}
    cal = dates.Calendar.load(con)

    for table in ("form4_transactions", "form4_derivatives"):
        extra = ", expiration_date" if table == "form4_derivatives" else ""
        for r in _rows(con, "SELECT rowid AS rid, accession, tx_index, ticker, code, "
                            "tx_date, filed_date{} FROM {} WHERE tx_date IS NOT NULL AND {}"
                            .format(extra, table, _unmarked(con, table))):
            raw = r["tx_date"]
            head = dates.iso10(raw)
            if head and raw != head:
                found["tz"].append(dict(r, table=table, head=head))
            market = (table == "form4_transactions"
                      and (r["code"] or "").upper() in OPEN_MARKET)
            flag, sub, sugg = dates.judge(raw, r["filed_date"], cal, market, today)
            row = dict(r, table=table, reason=flag, subclass=sub, suggested=sugg,
                       source=r["accession"], anchor=r["filed_date"])
            if flag:
                exp = dates.iso10(r.get("expiration_date"))
                row["note"] = "expiry_before_trade" if (exp and head and exp < head) else ""
                found["quarantine"].append(row)
            elif sub:
                found["marks"].append(row)

    for r in _rows(con, "SELECT rowid AS rid, filing_id, raw_ref, ticker, asset_type, "
                        "tx_date, disclosure_date, lag_days, chamber FROM congress_trades "
                        "WHERE tx_date IS NOT NULL AND {}"
                        .format(_unmarked(con, "congress_trades"))):
        flag, sub, sugg = dates.judge(
            r["tx_date"], r["disclosure_date"], cal,
            r["asset_type"] == MARKET_ASSET_TYPE, today)
        row = dict(r, table="congress_trades", reason=flag, subclass=sub, suggested=sugg,
                   source=r["raw_ref"], anchor=r["disclosure_date"], note="",
                   code=r["asset_type"])
        if flag:
            found["quarantine"].append(row)
        elif sub:
            found["marks"].append(row)

    from .oge_ingest import filed_iso
    for r in _rows(con, "SELECT rowid AS rid, doc_id, line_no, filer, filed_date "
                        "FROM oge_holdings WHERE filed_date IS NOT NULL"):
        if dates.iso10(r["filed_date"]) != r["filed_date"]:
            found["oge"].append(dict(r, iso=filed_iso(r["filed_date"])))

    for r in _rows(con, "SELECT ticker, verdict, last_trade_date FROM ticker_status"):
        if dates.date_flag(r["last_trade_date"], today=today):
            found["caches"].append(dict(r, table="ticker_status"))
    for r in _rows(con, "SELECT ticker, shares, shares_asof, concept FROM market_cap"):
        if dates.date_flag(r["shares_asof"], today=today):
            found["caches"].append(dict(r, table="market_cap", owner="SM-MC1"))

    found["residue"] = residue(con, today)
    return found


def residue(con, today=None):
    """What no guard can catch, counted and named -- never touched.

    A trade dated years before its filing, with the filing's own month and day, is the
    fingerprint of a year typo. It is also the fingerprint of a genuine late report --
    one 2025 CNK filing reports gifts dated 2009-2016 -- and nothing on the row tells
    the two apart. So they are characterised here, and left alone. Rows the guard
    itself quarantines are excluded, so nothing is counted twice."""
    unmarked = ("date_flag IS NULL" if "date_flag" in _cols(con, "form4_transactions")
                else "1=1")
    out = {}
    out["same_month_day_3y_before_filing"] = [
        r for r in _rows(con, """
            SELECT accession, ticker, tx_date, filed_date FROM form4_transactions
            WHERE {}
              AND substr(tx_date,6,5) = substr(filed_date,6,5)
              AND CAST(substr(filed_date,1,4) AS INTEGER)
                  - CAST(substr(tx_date,1,4) AS INTEGER) >= 3
            ORDER BY tx_date""".format(unmarked))
        if not dates.row_flag(r["tx_date"], r["filed_date"], today=today)]
    out["over_3000_days_before_filing"] = con.execute("""
        SELECT COUNT(*) FROM form4_transactions WHERE {}
          AND substr(tx_date,1,10) >= ?
          AND julianday(substr(filed_date,1,10)) - julianday(substr(tx_date,1,10)) > 3000
    """.format(unmarked), (dates.FLOOR,)).fetchone()[0]
    # The accession's middle segment is the filing year. Where it disagrees with the
    # stored filed_date by more than a year, the FILED date is the suspect -- and that
    # is the anchor every classification above leans on.
    mism = []
    for r in _rows(con, "SELECT DISTINCT accession, filed_date FROM form4_transactions"):
        try:
            yy = int(r["accession"].split("-")[1])
            fy = int((r["filed_date"] or "")[:4])
        except (AttributeError, IndexError, ValueError):
            continue
        if abs((2000 + yy if yy < 80 else 1900 + yy) - fy) > 1:
            mism.append(r)
    out["accession_year_disagrees_with_filed_date"] = mism
    return out


def pending(found):
    return sum(len(found[k]) for k in ("tz", "quarantine", "marks", "oge"))


def report(found, limit=60):
    L = ["J1 DATE SANITY - stored dates that cannot be true", "=" * 72, ""]
    tz = found["tz"]
    L.append("PASS 1 NORMALISE timezone-suffixed trade dates: {:,}".format(len(tz)))
    for t, n in collections.Counter(r["table"] for r in tz).most_common():
        L.append("    {:<22} {:,}".format(t, n))
    for r in tz[:5]:
        L.append("    e.g. {:<24} {!r} -> {}".format(r["accession"], r["tx_date"], r["head"]))
    q = found["quarantine"]
    L += ["", "PASS 2 QUARANTINE on the TRADE clock: {} rows. Every one stays a full".format(
              len(q)),
          "disclosure on the filing clock; no date is corrected.",
          "    by rule:"]
    for reason, n in collections.Counter(r["reason"] for r in q).most_common():
        L.append("      {:<22} {}".format(reason, n))
    L.append("    by what the string looks like:")
    for sub, n in collections.Counter(r["subclass"] or "(the rule says it all)"
                                      for r in q).most_common():
        L.append("      {:<22} {}".format(sub, n))
    for r in q[:limit]:
        L.append("    {:<18} {:<26} {:<7} {!r:<13} filed {:<11} {:<14} {:<19} {}{}".format(
            r["table"], (r["source"] or "")[:26], (r.get("ticker") or "-")[:7],
            r["tx_date"], (r["anchor"] or "-")[:10], r["reason"], r["subclass"] or "-",
            ("suggest " + r["suggested"]) if r["suggested"] else "",
            (" " + r["note"]) if r.get("note") else ""))
    if any(r["suggested"] for r in q):
        L += ["    " + dates.SUGGESTION_RULE]
    m = found["marks"]
    L += ["", "PASS 2b MARK, NOT quarantine: {} purchases and sales (Form 4 code P or S; "
              "congress Stock)".format(len(m)),
          "dated on a day with no US session. That is what the string shows and all the",
          "mark claims - a private transaction or a foreign listing trades on such days.",
          "They keep their place on every board. Today the mark is read by the brief",
          "footer's count and the congressional row notes; no board filters on it."]
    for (t, code), n in collections.Counter(
            (r["table"], r.get("code") or "-") for r in m).most_common():
        L.append("      {:<20} {:<8} {}".format(t, code, n))
    L += ["", "PASS 3 OGE filed dates not ISO: {}".format(len(found["oge"]))]
    for raw, n in collections.Counter(r["filed_date"] for r in found["oge"]).most_common(5):
        from .oge_ingest import filed_iso
        L.append("    {!r:<14} x{:<4} -> {}".format(raw, n, filed_iso(raw) or "REFUSED (kept raw)"))
    L += ["", "PASS 4 derived caches built on a bad date: {}".format(len(found["caches"]))]
    for r in found["caches"]:
        if r["table"] == "ticker_status":
            L.append("    ticker_status {:<7} verdict={:<18} last_trade={}   "
                     "recompute with --reclassify (probes Yahoo)".format(
                         r["ticker"], r["verdict"], r["last_trade_date"]))
        else:
            L.append("    market_cap    {:<7} shares_asof={}  NOT touched - owned by "
                     "{}".format(r["ticker"], r["shares_asof"], r["owner"]))
    res = found["residue"]
    L += ["", "RESIDUE - characterised, NOT touched. No guard can separate these from",
          "genuine late reports; the numbers are stated so they are not mistaken for a",
          "clean corpus.",
          "    same month-day as the filing, >=3 years before it: {}".format(
              len(res["same_month_day_3y_before_filing"])),
          "    dated more than 3,000 days before the filing:      {}".format(
              res["over_3000_days_before_filing"]),
          "    accession year disagrees with filed_date by >1y:   {}".format(
              len(res["accession_year_disagrees_with_filed_date"]))]
    for r in res["same_month_day_3y_before_filing"][:12]:
        L.append("      {:<24} {:<7} tx {} filed {}".format(
            r["accession"], r["ticker"] or "-", r["tx_date"], r["filed_date"]))
    for r in res["accession_year_disagrees_with_filed_date"][:5]:
        L.append("      filed_date suspect: {} filed {}".format(r["accession"], r["filed_date"]))
    return "\n".join(L)


def backup(db_path, stamp):
    """A full copy through SQLite's backup API, on a plain connection that runs no
    migration -- so the snapshot is the database exactly as it was, schema included.
    Refuses to overwrite an existing snapshot."""
    dest = os.path.join(os.path.dirname(os.path.abspath(db_path)),
                        "pre_date_sanity_{}.db".format(stamp))
    if os.path.exists(dest):
        raise SystemExit("REFUSE: backup {} already exists".format(dest))
    src = sqlite3.connect(db_path, timeout=30)
    out = sqlite3.connect(dest)
    try:
        src.backup(out)
    finally:
        out.close()
        src.close()
    return dest


def _artifact(folder, name, rows, cols):
    path = os.path.join(folder, name)
    with open(path, "w", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(cols)
        for r in rows:
            w.writerow([r.get(c) for c in cols])
    return path


def apply(con, found, folder):
    """Write the three row passes in ONE transaction. Counts are rows actually changed,
    not rows attempted -- a guard in each WHERE makes a stale scan change nothing."""
    os.makedirs(folder, exist_ok=True)
    # The original strings, written down BEFORE the first change.
    _artifact(folder, "tz_normalised.csv", found["tz"],
              ["table", "accession", "tx_index", "tx_date", "head"])
    _artifact(folder, "quarantined.csv", found["quarantine"],
              ["table", "source", "ticker", "tx_date", "anchor", "reason", "subclass",
               "suggested", "note"])
    _artifact(folder, "non_trading_marks.csv", found["marks"],
              ["table", "source", "ticker", "code", "tx_date", "anchor", "subclass"])
    _artifact(folder, "oge_converted.csv", found["oge"],
              ["doc_id", "line_no", "filer", "filed_date", "iso"])
    done = collections.Counter({"tz": 0, "quarantine": 0, "marks": 0, "oge": 0})
    for r in found["tz"]:
        done["tz"] += con.execute(
            "UPDATE {} SET tx_date=? WHERE rowid=? AND tx_date=?".format(r["table"]),
            (r["head"], r["rid"], r["tx_date"])).rowcount
    for r in found["quarantine"]:
        done["quarantine"] += con.execute(
            "UPDATE {} SET date_flag=?, date_subclass=?, tx_date_suggested=? "
            "WHERE rowid=? AND date_flag IS NULL".format(r["table"]),
            (r["reason"], r["subclass"], r["suggested"], r["rid"])).rowcount
    for r in found["marks"]:
        done["marks"] += con.execute(
            "UPDATE {} SET date_subclass=? WHERE rowid=? AND date_flag IS NULL "
            "AND date_subclass IS NULL".format(r["table"]),
            (r["subclass"], r["rid"])).rowcount
    for r in found["oge"]:
        done["oge"] += con.execute(
            "UPDATE oge_holdings SET filed_date_raw=COALESCE(filed_date_raw, filed_date), "
            "filed_date=? WHERE rowid=? AND filed_date=?",
            (r["iso"], r["rid"], r["filed_date"])).rowcount
    con.commit()
    return dict(done)


def reclassify(con, found, probe=True):
    from . import survivorship
    tickers = [r["ticker"] for r in found["caches"] if r["table"] == "ticker_status"]
    if not tickers:
        return {}
    return survivorship.classify(con, probe=probe, tickers=tickers, force=True)


def main(argv=None):
    ap = argparse.ArgumentParser(
        description="J1 stored-date repair. Dry run by default: read-only, nothing written.")
    ap.add_argument("--db", default=dbmod.DB_PATH_DEFAULT)
    ap.add_argument("--apply", action="store_true",
                    help="back up, migrate, then normalise, quarantine and convert")
    ap.add_argument("--reclassify", action="store_true",
                    help="with --apply, recompute ticker_status verdicts built on a bad "
                         "date (probes Yahoo)")
    args = ap.parse_args(argv)

    ro = sqlite3.connect("file:{}?mode=ro".format(os.path.abspath(args.db)), uri=True)
    try:
        found = scan(ro)
    finally:
        ro.close()
    print(report(found))
    if not args.apply:
        print("\nDRY RUN - read-only, nothing written. Re-run with --apply to write.")
        return 0
    # Only a ticker_status verdict is something --reclassify can act on. The market_cap
    # row in the same list is reported and never touched, and counting it here made
    # every later --apply --reclassify write a fresh 700 MB backup to change nothing.
    recomputable = [r for r in found["caches"] if r["table"] == "ticker_status"]
    if not pending(found) and not (args.reclassify and recomputable):
        print("\nNothing to apply - no backup taken, nothing written.")
        return 0

    stamp = dt.datetime.now().strftime("%Y%m%d-%H%M%S")
    snap = backup(args.db, stamp)
    print("\nbackup    {}  (taken before the migration and before any row change)".format(snap))
    con = dbmod.connect(args.db)
    con.execute("PRAGMA busy_timeout=30000")
    try:
        found = scan(con)       # re-scan after the migration, on the connection that writes
        folder = os.path.join(os.path.dirname(os.path.abspath(args.db)), "repairs",
                              "date_sanity_{}".format(stamp))
        print("applied   {}".format(apply(con, found, folder)))
        print("artifacts {}".format(folder))
        after = scan(con)
        print("re-scan   tz={} quarantine={} marks={} oge={}  (all must be 0)".format(
            len(after["tz"]), len(after["quarantine"]), len(after["marks"]),
            len(after["oge"])))
        if args.reclassify:
            print("reclassified {}".format(reclassify(con, found)))
    finally:
        con.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
