"""SM-R1 brief render test. Requires reportlab + abelard_common (present in the
Basilic venv and Windows Python312). It self-skips where reportlab is absent, so
the plain-assert runner passes under bare WSL python3 too."""
import os
import sys
import tempfile

# Make abelard_common importable for a local/dev run if it is not installed.
_COMMON = os.path.join(os.path.dirname(__file__), "..", "..", "common")
if os.path.isdir(_COMMON) and _COMMON not in sys.path:
    sys.path.insert(0, _COMMON)

from smart_money import db as dbmod
from smart_money import queries as q


def _has_reportlab():
    try:
        import reportlab  # noqa: F401
        return True
    except ImportError:
        return False


def _fixture_db():
    path = tempfile.mktemp(suffix=".db")
    con = dbmod.connect(path)
    con.execute(
        "INSERT INTO form4_transactions(accession, tx_index, reporting_person, "
        "reporting_cik, issuer, issuer_cik, ticker, code, plan_flag, shares, price, "
        "value, ownership_after, tx_date, filed_date, role, ingest_regime) "
        "VALUES('A',0,'Insider','1','Co','9','ZZZ','P',0,100,1.0,100,NULL,"
        "'2026-06-01','2026-06-02',NULL,'watchlist')")
    entries, _ = q._load_registry()
    for e in entries:
        if e.get("person_id") is not None:
            con.execute("INSERT OR IGNORE INTO persons(person_id, name, type, "
                        "cik_or_chamber) VALUES(?,?,?,?)",
                        (e["person_id"], e["name"], "congress", e.get("chamber") or "house"))
    con.commit()
    con.close()
    return path


def test_brief_renders_valid_pdf():
    if not _has_reportlab():
        print("SKIP test_brief_renders_valid_pdf: reportlab not installed here")
        return
    from smart_money import brief
    path = _fixture_db()
    con = q.connect_ro(path)
    out = tempfile.mktemp(suffix=".pdf")
    result = brief.render_brief(con, out, window=90, anchor="2026-07-01", scheduled=True)
    data = open(result, "rb").read()
    assert data[:5] == b"%PDF-", "output is not a PDF"
    assert len(data) > 1500, "PDF suspiciously small ({} bytes)".format(len(data))
    con.close()
    os.unlink(path)
    os.unlink(out)
