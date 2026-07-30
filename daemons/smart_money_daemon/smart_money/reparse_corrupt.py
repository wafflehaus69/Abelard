"""Targeted re-parse of Form 4 accessions whose stored dollar value is corrupt.

Filer-side corruption (debt principal in the price field, total proceeds in the
per-share field, a dropped decimal, an ADS unit mismatch) poisoned the derived
`value` for a small set of accessions. The ingest guard (form4.value_sanity_flag)
now NULLs and flags such rows on parse, but rows persisted BEFORE the guard keep
their bad value and a NULL value_flag. This re-fetches each affected accession
from EDGAR, deletes its existing form4_transactions rows, and re-persists them
through the guard.

Targeted, not a full rescan (the order preferred this). Scope is auto-detected
from the pre-guard corruption signature — a per-share price over the sanity
ceiling OR an over-cap dollar value — unless explicit --accession values are
given. filed_date and ingest_regime are carried over from the existing rows.
Table II (derivatives) rows are untouched. Fetch is delete-safe (delete only
after a successful fetch+parse) and persist is idempotent. UA header + EDGAR
pace. No LLM.
"""
import argparse
import sys

from . import db as dbmod
from . import form4
from .efd_ingest import load_env

# Pre-guard corruption signature, evaluated from the DB alone: a per-share price
# above the sanity ceiling or a dollar value above the cap. Catches every known
# corrupt accession; anything subtler (a sub-ceiling dropped decimal) can be
# passed explicitly with --accession.
CORRUPT_SQL = (
    "SELECT DISTINCT accession, issuer_cik FROM form4_transactions "
    "WHERE code IN ('P','S') "
    "AND (abs(coalesce(price,0)) > ? OR abs(coalesce(value,0)) > ?) "
    "ORDER BY accession")


def _txt_path(issuer_cik, accession):
    """EDGAR full-submission .txt sub-path for form4.fetch_form4_from_txt."""
    cik = str(issuer_cik).lstrip("0")
    acc_nodash = accession.replace("-", "")
    return "edgar/data/{}/{}/{}.txt".format(cik, acc_nodash, accession)


def reparse(con, contact, targets, pace=None):
    """targets: iterable of (accession, issuer_cik). Returns a list of
    (accession, status, rows_persisted, rows_flagged)."""
    results = []
    for accession, issuer_cik in targets:
        if not issuer_cik:
            results.append((accession, "skip_no_issuer_cik", 0, 0))
            continue
        meta = con.execute(
            "SELECT filed_date, ingest_regime FROM form4_transactions "
            "WHERE accession=? LIMIT 1", (accession,)).fetchone()
        filed_date = meta[0] if meta else None
        regime = (meta[1] if meta and meta[1] else "watchlist")
        try:
            parsed = form4.fetch_form4_from_txt(
                contact, _txt_path(issuer_cik, accession), pace=pace)
        except Exception as exc:  # noqa: BLE001 - report, never guess; no delete
            results.append((accession, "fetch_error:" + str(exc)[:60], 0, 0))
            continue
        if not parsed:
            results.append((accession, "no_ownership_doc", 0, 0))
            continue
        ticker = parsed.get("symbol")
        con.execute("DELETE FROM form4_transactions WHERE accession=?", (accession,))
        n, _ = form4.persist_transactions(con, accession, parsed, ticker,
                                          filed_date, regime)
        con.commit()
        flagged = con.execute(
            "SELECT count(*) FROM form4_transactions "
            "WHERE accession=? AND value_flag IS NOT NULL", (accession,)).fetchone()[0]
        results.append((accession, "reparsed", n, flagged))
    return results


def main(argv=None):
    ap = argparse.ArgumentParser(
        description="Targeted re-parse of corrupt Form 4 accessions")
    ap.add_argument("--db", default=dbmod.DB_PATH_DEFAULT)
    ap.add_argument("--accession", action="append", default=[],
                    help="specific accession(s) to re-parse; default auto-detects "
                         "the pre-guard corruption signature")
    ap.add_argument("--pace", type=float, default=None,
                    help="inter-request sleep override (seconds)")
    ap.add_argument("--dry-run", action="store_true",
                    help="list targets and fetch nothing")
    args = ap.parse_args(argv)
    con = dbmod.connect(args.db)
    env = load_env()
    contact = env.get("EDGAR_CONTACT")
    if not contact:
        print("FATAL missing EDGAR_CONTACT", file=sys.stderr)
        return 2
    if args.accession:
        targets = []
        for acc in args.accession:
            r = con.execute("SELECT DISTINCT issuer_cik FROM form4_transactions "
                            "WHERE accession=?", (acc,)).fetchone()
            targets.append((acc, r[0] if r else None))
    else:
        targets = con.execute(
            CORRUPT_SQL, (form4.PRICE_SANITY_MAX, form4.VALUE_SANITY_MAX)).fetchall()
    print("[reparse] {} target accession(s)".format(len(targets)))
    for acc, cik in targets:
        print("  target {} issuer_cik={}".format(acc, cik))
    if args.dry_run:
        return 0
    results = reparse(con, contact, targets, pace=args.pace)
    tot_rows = tot_flag = 0
    for acc, status, n, flagged in results:
        print("[reparse] {} {} rows={} flagged={}".format(acc, status, n, flagged))
        tot_rows += n
        tot_flag += flagged
    print("[reparse] done accessions={} rows_persisted={} rows_flagged={}".format(
        len(results), tot_rows, tot_flag))
    return 0


if __name__ == "__main__":
    sys.exit(main())
