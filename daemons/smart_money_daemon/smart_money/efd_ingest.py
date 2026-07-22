"""Phase 1a Senate eFD ingest per ORDER SM-1.

Scripts-first, no LLM. Fail-loud: unmapped amount band, unmapped side, or
unexpected table structure raises with the filing uuid — fix the map, rerun;
detail pages are disk-cached so reruns refetch nothing already seen.
Resume-safe: filing IDs present in ingested_filings are skipped, interrupt
and rerun converge to identical DB state.
"""
import argparse
import datetime as dt
import html as htmlmod
import json
import pathlib
import re
import sys
import time

from . import db as dbmod
from .efd_session import bootstrap, get_ptr_html, post_data

RAW_DIR_DEFAULT = "data/raw/efd"
PACE_SECONDS = 0.5
VIEW_RE = re.compile(r"/search/view/(ptr|paper)/([0-9a-f-]{8,})/")
TAG_RE = re.compile(r"<[^>]+>")
TR_RE = re.compile(r"<tr[^>]*>(.*?)</tr>", re.S)
TD_RE = re.compile(r"<td[^>]*>(.*?)</td>", re.S)

# All bands observed on eFD PTRs. Open top band stores amt_high NULL —
# documented in DATA_QUALITY.md. Unmapped text = IngestError, never bucketed.
AMOUNT_BANDS = {
    "$1,001 - $15,000": (1001, 15000),
    "$15,001 - $50,000": (15001, 50000),
    "$50,001 - $100,000": (50001, 100000),
    "$100,001 - $250,000": (100001, 250000),
    "$250,001 - $500,000": (250001, 500000),
    "$500,001 - $1,000,000": (500001, 1000000),
    "$1,000,001 - $5,000,000": (1000001, 5000000),
    "$5,000,001 - $25,000,000": (5000001, 25000000),
    "$25,000,001 - $50,000,000": (25000001, 50000000),
    "Over $50,000,000": (50000001, None),
}

SIDE_MAP = {
    "purchase": "purchase",
    "sale (full)": "sale_full",
    "sale (partial)": "sale_partial",
    "exchange": "exchange",
}

_last_call = 0.0


class IngestError(RuntimeError):
    pass


def _pace():
    global _last_call
    wait = PACE_SECONDS - (time.monotonic() - _last_call)
    if wait > 0:
        time.sleep(wait)
    _last_call = time.monotonic()


def load_env(path: str = ".env") -> dict:
    env = {}
    p = pathlib.Path(path)
    if p.exists():
        for line in p.read_text().splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            env[k.strip()] = v.strip()
    return env


def _clean(fragment: str) -> str:
    return re.sub(r"\s+", " ", htmlmod.unescape(TAG_RE.sub(" ", fragment))).strip()


def _iso(mdY: str) -> str:
    return dt.datetime.strptime(mdY.strip(), "%m/%d/%Y").date().isoformat()


def parse_search_row(row):
    first, last, office, link_html, filed = row
    m = VIEW_RE.search(link_html)
    if not m:
        raise IngestError("unrecognized filing link {!r}".format(link_html[:200]))
    kind, uuid = m.groups()
    return {
        "first": first.strip(),
        "last": last.strip(),
        "office": office.strip(),
        "kind": kind,  # ptr = electronic, paper = scanned
        "uuid": uuid,
        "label": _clean(link_html),
        "filed": _iso(filed),
    }


def search_year(sess, year: int, raw_dir: pathlib.Path):
    """All PTR search rows for a calendar year, page cache on disk."""
    rows = []
    start = 0
    total = None
    page = 0
    while True:
        cache = raw_dir / "search_{}_p{:03d}.json".format(year, page)
        if cache.exists():
            body = json.loads(cache.read_text())
        else:
            _pace()
            body = post_data(
                sess,
                {
                    "draw": "1",
                    "start": str(start),
                    "length": "100",
                    "report_types": "[11]",
                    "filer_types": "[]",
                    "first_name": "",
                    "last_name": "",
                    "submitted_start_date": "01/01/{} 00:00:00".format(year),
                    "submitted_end_date": "12/31/{} 23:59:59".format(year),
                    "candidate_state": "",
                    "senator_state": "",
                    "office_id": "",
                    "order[0][column]": "4",
                    "order[0][dir]": "asc",
                },
            )
            cache.write_text(json.dumps(body))
        data = body.get("data")
        if data is None or "recordsTotal" not in body:
            raise IngestError(
                "year {} page {} malformed search response".format(year, page)
            )
        total = body["recordsTotal"]
        rows.extend(parse_search_row(r) for r in data)
        start += 100
        page += 1
        if start >= total or not data:
            break
    if total is not None and len(rows) != total:
        raise IngestError(
            "year {} row count {} != recordsTotal {}".format(year, len(rows), total)
        )
    return rows


def parse_ptr_table(raw_html: str, uuid: str):
    """Transaction rows from an electronic PTR page. 9 cells expected."""
    out = []
    for tr in TR_RE.findall(raw_html):
        tds = TD_RE.findall(tr)
        if not tds:
            continue
        cells = [_clean(td) for td in tds]
        if len(cells) != 9:
            raise IngestError(
                "ptr {} unexpected row shape {} cells {!r}".format(
                    uuid, len(cells), cells[:4]
                )
            )
        out.append(cells)
    return out


def norm_ticker(text):
    t = text.strip().upper()
    if t in ("--", "", "N/A", "NONE"):
        return None
    t = t.split()[0]
    if ":" in t:  # exchange-prefixed like NYSE:ABC
        t = t.split(":")[-1]
    if t.endswith(".US"):
        t = t[: -len(".US")]
    t = t.strip(".,")
    return t or None


def norm_side(text, uuid):
    key = text.strip().lower()
    if key not in SIDE_MAP:
        raise IngestError("ptr {} unmapped side {!r}".format(uuid, text))
    return SIDE_MAP[key]


def norm_amount(text, uuid):
    key = re.sub(r"\s+", " ", text).strip()
    if key not in AMOUNT_BANDS:
        raise IngestError("ptr {} unmapped amount band {!r}".format(uuid, text))
    return AMOUNT_BANDS[key]


def upsert_person(con, filing) -> int:
    name = filing["office"]
    for suffix in (" (Senator)", " (Former Senator)", " (Candidate)"):
        if name.endswith(suffix):
            name = name[: -len(suffix)]
            break
    meta = json.dumps(
        {"office": filing["office"], "first": filing["first"], "last": filing["last"]}
    )
    row = con.execute(
        "SELECT person_id FROM persons WHERE name=?", (name,)
    ).fetchone()
    if row:
        return row[0]
    cur = con.execute(
        "INSERT INTO persons(name, type, cik_or_chamber, meta) "
        "VALUES (?, 'congress', 'senate', ?)",
        (name, meta),
    )
    return cur.lastrowid


def ingest_filing(con, sess, filing, raw_dir: pathlib.Path) -> str:
    uuid = filing["uuid"]
    seen = con.execute(
        "SELECT status FROM ingested_filings WHERE filing_id=?", (uuid,)
    ).fetchone()
    if seen:
        return "skip"
    if filing["kind"] == "paper":
        person_id = upsert_person(con, filing)
        con.execute(
            "INSERT INTO ingested_filings VALUES (?,?,?,?,?,?,?,?)",
            (
                uuid,
                "senate",
                "paper",
                filing["office"],
                filing["label"],
                filing["filed"],
                0,
                int(time.time()),
            ),
        )
        con.commit()
        return "paper"

    cache = raw_dir / "ptr_{}.html".format(uuid)
    if cache.exists():
        raw = cache.read_text(errors="replace")
    else:
        _pace()
        raw = get_ptr_html(sess, uuid)
        cache.write_text(raw, errors="replace")

    person_id = upsert_person(con, filing)
    rows = parse_ptr_table(raw, uuid)
    disclosure = filing["filed"]
    inserted = 0
    for cells in rows:
        idx, tx_date, owner, ticker, asset_name, asset_type, side, amount, comment = (
            cells
        )
        tx_iso = _iso(tx_date)
        low, high = norm_amount(amount, uuid)
        lag = (dt.date.fromisoformat(disclosure) - dt.date.fromisoformat(tx_iso)).days
        con.execute(
            "INSERT OR REPLACE INTO congress_trades("
            "person_id, ticker, side, amt_low, amt_high, tx_date, disclosure_date,"
            "lag_days, chamber, source, raw_ref, owner, asset_name, asset_type,"
            "comment, filing_id) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                person_id,
                norm_ticker(ticker),
                norm_side(side, uuid),
                low,
                high,
                tx_iso,
                disclosure,
                lag,
                "senate",
                "efd",
                "{}#{}".format(uuid, idx),
                owner,
                asset_name,
                asset_type,
                None if comment == "--" else comment,
                uuid,
            ),
        )
        inserted += 1
    con.execute(
        "INSERT INTO ingested_filings VALUES (?,?,?,?,?,?,?,?)",
        (
            uuid,
            "senate",
            "electronic",
            filing["office"],
            filing["label"],
            filing["filed"],
            inserted,
            int(time.time()),
        ),
    )
    con.commit()
    return "electronic"


def _filing_from_index_row(row):
    """Adapt a harvested index row to the filing dict ingest_filing expects.
    Index rows carry uuid, office, filed. office is 'Last, First (Senator)'."""
    office = row["office"]
    name_part = re.sub(r"\s*\((?:Senator|Former Senator|Candidate)\)\s*$", "", office)
    first = last = ""
    if "," in name_part:
        last, first = [p.strip() for p in name_part.split(",", 1)]
    return {
        "first": first,
        "last": last,
        "office": office,
        "kind": "ptr",
        "uuid": row["uuid"],
        "label": "Periodic Transaction Report for {}".format(row["filed"]),
        "filed": _iso(row["filed"]),
    }


def _ingest_from_index(con, ua, raw_dir, args):
    """Detail-only ingest from a browser-harvested index. Uses the light
    agreement session (no WAF-blocked data-endpoint probe); detail-page GETs
    are not behind the WAF."""
    data = json.loads(pathlib.Path(args.index_file).read_text())
    rows = data["rows"]
    print(
        "[efd] index-file mode: {} electronic PTRs (harvested {})".format(
            len(rows), data.get("harvested_date", "?")
        )
    )
    sess = bootstrap(ua, probe=False)
    print("[efd] light session bootstrapped (agreement only)")
    details = 0
    for row in rows:
        filing = _filing_from_index_row(row)
        outcome = ingest_filing(con, sess, filing, raw_dir)
        if outcome == "electronic":
            details += 1
            if details % 100 == 0:
                print("[efd] {}/{} details ingested".format(details, len(rows)), flush=True)
        if args.max_details and details >= args.max_details:
            print("[efd] SMOKE stop after {} details".format(details))
            return 0
    con.execute(
        "INSERT OR REPLACE INTO meta_kv VALUES ('senate_efd_source', "
        "'browser_index_20260720_plus_requests_details')"
    )
    con.commit()
    n_tr = con.execute(
        "SELECT COUNT(*) FROM congress_trades WHERE chamber='senate'"
    ).fetchone()[0]
    n_f = con.execute(
        "SELECT COUNT(*) FROM ingested_filings WHERE chamber='senate'"
    ).fetchone()[0]
    print("[efd] DONE senate filings={} trades={}".format(n_f, n_tr))
    return 0


def main(argv=None):
    ap = argparse.ArgumentParser(description="Senate eFD PTR ingest, Phase 1a")
    ap.add_argument("--db", default=dbmod.DB_PATH_DEFAULT)
    ap.add_argument("--raw", default=RAW_DIR_DEFAULT)
    ap.add_argument("--start-year", type=int, default=dt.date.today().year)
    ap.add_argument("--min-year", type=int, default=2008)
    ap.add_argument(
        "--max-details",
        type=int,
        default=0,
        help="smoke mode, stop after N electronic details, horizon not recorded",
    )
    ap.add_argument(
        "--index-file",
        default=None,
        help="ingest details from a browser-harvested PTR index JSON instead of "
        "the WAF-blocked search endpoint (see recon/EFD_WAF_FINDING.md)",
    )
    args = ap.parse_args(argv)

    raw_dir = pathlib.Path(args.raw)
    raw_dir.mkdir(parents=True, exist_ok=True)
    con = dbmod.connect(args.db)

    env = load_env()
    contact = env.get("EDGAR_CONTACT")
    if not contact:
        print("FATAL missing EDGAR_CONTACT in .env", file=sys.stderr)
        return 2
    ua = "Abelard-SmartMoney mdiba personal research {}".format(contact)

    if args.index_file:
        return _ingest_from_index(con, ua, raw_dir, args)

    sess = bootstrap(ua)
    print("[efd] session bootstrapped")

    details = 0
    horizon_year = None
    for year in range(args.start_year, args.min_year - 1, -1):
        rows = search_year(sess, year, raw_dir)
        electronic = sum(1 for r in rows if r["kind"] == "ptr")
        paper = len(rows) - electronic
        print(
            "[efd] year={} filings={} electronic={} paper={}".format(
                year, len(rows), electronic, paper
            ),
            flush=True,
        )
        if electronic == 0 and year < args.start_year:
            horizon_year = year
            print("[efd] horizon hit at {}, stopping walk".format(year))
            break
        for filing in rows:
            outcome = ingest_filing(con, sess, filing, raw_dir)
            if outcome == "electronic":
                details += 1
                if details % 100 == 0:
                    print("[efd] {} electronic filings ingested".format(details), flush=True)
            if args.max_details and details >= args.max_details:
                print("[efd] SMOKE stop after {} details".format(details))
                return 0
    if horizon_year is not None:
        con.execute(
            "INSERT OR REPLACE INTO meta_kv VALUES ('senate_efd_horizon_year', ?)",
            (str(horizon_year + 1),),
        )
        con.execute(
            "INSERT OR REPLACE INTO meta_kv VALUES ('senate_efd_walk_stopped_at', ?)",
            (str(horizon_year),),
        )
        con.commit()
    n_tr = con.execute("SELECT COUNT(*) FROM congress_trades").fetchone()[0]
    n_f = con.execute("SELECT COUNT(*) FROM ingested_filings").fetchone()[0]
    print("[efd] DONE filings={} trades={}".format(n_f, n_tr))
    return 0


if __name__ == "__main__":
    sys.exit(main())
