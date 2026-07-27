"""Mojibake counter (ORDER SM-R1 P1). Read-only, deterministic, no network.

Counts suspected mis-decoded-UTF-8 artifacts in the person/issuer name columns
already ingested. DETECT AND COUNT, NEVER FIX -- a read-only reporting layer does
not rewrite the corpus; fixing means re-ingesting with a forced UTF-8 decode,
which is out of scope. The count is the standing argument for the abelard_common
UTF-8-decode hoist order's priority, and it lands in the brief's data-quality footer.

Root cause (why the count is expected nonzero): the daemon's ingest paths omit a
forced UTF-8 decode, so on a cp1252 host a non-ASCII senator/issuer name can be
written mis-decoded into the DB (VESTIGIAL_INVENTORY.md).

Heuristic: mojibake = UTF-8 bytes read as cp1252/Latin-1. High-precision markers:
  - a lead byte U+00C3/U+00C2 followed by a UTF-8 continuation byte U+0080..U+00BF,
  - the U+00E2 U+0080 prefix of a mis-decoded curly quote or dash,
  - the U+FFFD replacement char.
A legitimate accented name is non-ASCII but is NOT mojibake by itself, so two
numbers are reported per column: suspected_mojibake (the markers) and
non_ascii_total (the superset context).
"""

from __future__ import annotations

import argparse
import re
import sys

from . import db as dbmod

# Built from code points (pure-ASCII source, no literal non-ASCII, no ambiguous
# escapes). _CONT = a UTF-8 continuation byte class U+0080..U+00BF.
_CONT = "[" + chr(0x80) + "-" + chr(0xBF) + "]"
MOJIBAKE_RE = re.compile(
    "[" + chr(0xC2) + chr(0xC3) + "]" + _CONT   # C3/C2 lead + continuation
    + "|" + chr(0xE2) + chr(0x80) + _CONT        # mis-decoded punctuation prefix
    + "|" + chr(0xFFFD)                          # U+FFFD replacement char
)

# ORDER-named person/issuer name columns, plus the other issuer/name bearers so the
# sweep is complete. Every column exists after db.connect() creates the schema.
NAME_COLUMNS = [
    ("persons", "name"),
    ("form4_transactions", "reporting_person"),
    ("form4_transactions", "issuer"),
    ("congress_trades", "asset_name"),
    ("congress_trades", "owner"),
    ("thirteenf_holdings", "issuer"),
    ("ingested_filings", "person_name"),
    ("cusip_ticker", "name"),
]


def scan_mojibake(con, columns=NAME_COLUMNS):
    """Count suspected-mojibake and non-ASCII values across name columns.

    Returns {"per_column": [ {table, column, nonnull, suspected_mojibake,
    non_ascii_total} ... ], "total_suspected_mojibake": int,
    "total_non_ascii": int}. Fail-loud: a missing table/column raises
    sqlite3.OperationalError (a real schema mismatch, never silently skipped)."""
    per_column = []
    total_suspected = 0
    total_non_ascii = 0
    for table, column in columns:
        rows = con.execute(
            f"SELECT {column} FROM {table} WHERE {column} IS NOT NULL"
        ).fetchall()
        vals = [r[0] for r in rows if isinstance(r[0], str)]
        suspected = sum(1 for v in vals if MOJIBAKE_RE.search(v))
        non_ascii = sum(1 for v in vals if not v.isascii())
        per_column.append({
            "table": table, "column": column, "nonnull": len(vals),
            "suspected_mojibake": suspected, "non_ascii_total": non_ascii,
        })
        total_suspected += suspected
        total_non_ascii += non_ascii
    return {
        "per_column": per_column,
        "total_suspected_mojibake": total_suspected,
        "total_non_ascii": total_non_ascii,
    }


def format_footer_line(result):
    """One-line data-quality footer fragment. No parens/colons/semicolons/dashes,
    per the message doctrine."""
    return (
        f"suspected mojibake {result['total_suspected_mojibake']} "
        f"across {len(result['per_column'])} name columns "
        f"non ascii {result['total_non_ascii']} detected not fixed"
    )


def main(argv=None):
    ap = argparse.ArgumentParser(description="count suspected mojibake in name columns")
    ap.add_argument("--db", default=dbmod.DB_PATH_DEFAULT)
    args = ap.parse_args(argv)
    con = dbmod.connect(args.db)
    try:
        result = scan_mojibake(con)
    finally:
        con.close()
    print("table                 column            nonnull  suspected  non_ascii")
    for c in result["per_column"]:
        print(f"{c['table']:21} {c['column']:17} {c['nonnull']:7}  "
              f"{c['suspected_mojibake']:9}  {c['non_ascii_total']:9}")
    print(f"\nTOTAL suspected_mojibake={result['total_suspected_mojibake']} "
          f"non_ascii={result['total_non_ascii']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
