"""F4 (ORDER SM-2): enumerate Senate paper filings, count only, never parse or
OCR. Source is the browser-harvested eFD index dump (same harvest that produced
the electronic PTR list). Inserts paper filings into ingested_filings with
status='paper' so DATA_QUALITY reports a real per-person-per-year number
instead of None. Idempotent by filing_id.
"""
import argparse
import datetime as dt
import json
import sys
import time

from . import db as dbmod


def _load_rows(dump_path):
    """Decode the harvest tool-result dump into index rows. The file is
    [{type,text}] where text is a (possibly double-encoded) JSON array with
    trailing bytes, so raw_decode tolerates the trailing junk."""
    raw = open(dump_path, encoding="utf-8").read()
    dec = json.JSONDecoder()
    wrapper, _ = dec.raw_decode(raw.lstrip())
    inner = wrapper[0]["text"]
    val, _ = dec.raw_decode(inner)
    if isinstance(val, str):
        val, _ = dec.raw_decode(val)
    return val


def enumerate_paper(con, dump_path) -> dict:
    rows = _load_rows(dump_path)
    seen = set()
    inserted = 0
    for r in rows:
        if r.get("kind") != "paper" or not r.get("uuid"):
            continue
        uuid = r["uuid"]
        if uuid in seen:
            continue
        seen.add(uuid)
        if con.execute(
            "SELECT 1 FROM ingested_filings WHERE filing_id=?", (uuid,)
        ).fetchone():
            continue
        filed = dt.datetime.strptime(r["date"], "%m/%d/%Y").date().isoformat()
        # Paper rows carry office="Senator"; the name lives in first/last.
        office = (r.get("office") or "").strip()
        if office in ("", "Senator", "Former Senator", "Candidate"):
            last = (r.get("last") or "").strip().title()
            first = (r.get("first") or "").strip().title()
            person = ", ".join(p for p in (last, first) if p) or "Unknown"
        else:
            person = office
        con.execute(
            "INSERT INTO ingested_filings VALUES (?,?,?,?,?,?,?,?)",
            (uuid, "senate", "paper", person, "Paper PTR", filed, 0,
             int(time.time())),
        )
        inserted += 1
    con.commit()
    return {"paper_seen": len(seen), "inserted": inserted}


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default=dbmod.DB_PATH_DEFAULT)
    ap.add_argument("--dump", required=True, help="harvested index tool-result dump")
    args = ap.parse_args(argv)
    con = dbmod.connect(args.db)
    stats = enumerate_paper(con, args.dump)
    print("[senate-paper] {}".format(stats))
    return 0


if __name__ == "__main__":
    sys.exit(main())
