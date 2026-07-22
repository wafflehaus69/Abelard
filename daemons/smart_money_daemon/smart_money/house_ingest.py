"""Phase 1b House Clerk ingest per ORDER SM-1.

Extraction library: pdfplumber (CHOSEN over pypdf — positional words enable
layout-versioned column parsing; recorded in DATA_QUALITY.md).

Layout v1 = the digital PTR generation with header columns
ID/Owner/Asset/Transaction Type/Date/Notification Date/Amount/Cap Gains,
column x-anchors read from the header row itself. Scanned PDFs (no text
layer) = SKIPPED-PAPER. Text PDFs with no recognizable header = unparsed
layout, sample copied to data/raw/house_unparsed/, counted, never guessed.
Unmapped amount band or transaction code = hard IngestError — extend the
map and rerun; every artifact is disk-cached so reruns refetch nothing.
Resume-safe by DocID in ingested_filings.
"""
import argparse
import datetime as dt
import io
import json
import pathlib
import re
import shutil
import sys
import time
import xml.etree.ElementTree as ET
import zipfile

import pdfplumber
import requests

from . import db as dbmod
from .efd_ingest import AMOUNT_BANDS, IngestError, load_env, norm_ticker

RAW_DIR_DEFAULT = "data/raw/house"
UNPARSED_DIR = "data/raw/house_unparsed"
ZIP_URL = "https://disclosures-clerk.house.gov/public_disc/financial-pdfs/{year}FD.zip"
PDF_URLS = (
    "https://disclosures-clerk.house.gov/public_disc/ptr-pdfs/{year}/{doc}.pdf",
    "https://disclosures-clerk.house.gov/public_disc/financial-pdfs/{year}/{doc}.pdf",
)
PACE_SECONDS = 0.5
DATE_RE = re.compile(r"^\d{2}/\d{2}/\d{4}$")
TICKER_RE = re.compile(r"\(([A-Za-z0-9.\-]{1,10})\)")
CODE_RE = re.compile(r"\[([A-Z]{2,3})\]")

# House asset-type codes, fd.house.gov/reference/asset-type-codes.aspx.
# ST maps to Stock so Phase 2's asset_type filter works across chambers.
ASSET_CODES = {
    "ST": "Stock",
    "OP": "Stock Option",
    "GS": "Government Security",
    "CS": "Corporate Bond",
    "MF": "Mutual Fund",
    "EF": "Exchange Traded Fund",
    "CT": "Cryptocurrency",
}

SIDE_MAP = {
    "P": "purchase",
    "S": "sale",
    "S (PARTIAL)": "sale_partial",
    "E": "exchange",
}

HOUSE_BANDS = dict(AMOUNT_BANDS)
HOUSE_BANDS.update(
    {
        "$50,000,001 - $100,000,000": (50000001, 100000000),
        "Over $100,000,000": (100000001, None),
        # Spouse/dependent-child filers may stop at "over $1M", open-ended.
        "Spouse/DC Over $1,000,000": (1000001, None),
        "Over $1,000,000": (1000001, None),
    }
)

_last_call = 0.0


def _pace():
    global _last_call
    wait = PACE_SECONDS - (time.monotonic() - _last_call)
    if wait > 0:
        time.sleep(wait)
    _last_call = time.monotonic()


def _get(url, ua):
    _pace()
    return requests.get(url, headers={"User-Agent": ua}, timeout=120)


def fetch_year_zip(year, raw_dir, ua):
    """Returns parsed index entries or None when the zip does not exist."""
    zpath = raw_dir / "{}FD.zip".format(year)
    if not zpath.exists():
        r = _get(ZIP_URL.format(year=year), ua)
        if r.status_code == 404:
            return None
        if r.status_code != 200:
            raise IngestError("{}FD.zip HTTP {}".format(year, r.status_code))
        zpath.write_bytes(r.content)
    with zipfile.ZipFile(zpath) as z:
        xml_names = [n for n in z.namelist() if n.endswith(".xml")]
        if not xml_names:
            raise IngestError("{}FD.zip has no XML index".format(year))
        root = ET.fromstring(z.read(xml_names[0]))
    out = []
    for m in root:
        f = {e.tag: (e.text or "").strip() for e in m}
        if f.get("FilingType") != "P":
            continue
        out.append(f)
    return out


def fetch_pdf(year, doc_id, raw_dir, ua):
    pdir = raw_dir / "pdfs" / str(year)
    pdir.mkdir(parents=True, exist_ok=True)
    path = pdir / "{}.pdf".format(doc_id)
    if path.exists():
        return path
    for tmpl in PDF_URLS:
        r = _get(tmpl.format(year=year, doc=doc_id), ua)
        if r.status_code == 200 and r.content[:5] == b"%PDF-":
            path.write_bytes(r.content)
            return path
    return None


def _lines(page):
    grouped = {}
    for w in page.extract_words():
        text = w["text"].replace("\x00", "")
        grouped.setdefault(round(w["top"] / 4), []).append(
            (w["x0"], text)
        )
    out = []
    for key in sorted(grouped):
        out.append(sorted(grouped[key]))
    return out


HEADER_ORDER = (
    "ID", "Owner", "Asset", "Transaction", "Date", "Notification", "Amount", "Cap.",
)


def _find_header(line):
    texts = [t for _, t in line]
    if all(h in texts for h in HEADER_ORDER):
        anchors = {}
        for x, t in line:
            if t in HEADER_ORDER and t not in anchors:
                anchors[t] = x
        return anchors
    return None


def parse_ptr_pdf(path):
    """Returns (rows, status). rows = list of dicts. status in
    {ok, paper, unparsed_layout}."""
    with pdfplumber.open(path) as pdf:
        all_words = sum(len(p.extract_words()) for p in pdf.pages)
        if all_words == 0:
            return [], "paper"
        rows = []
        header_seen = False
        current = None  # persists across pages: rows wrap over page breaks
        bounds = None
        for page in pdf.pages:
            page_anchors = None
            for line in _lines(page):
                if page_anchors is None:
                    page_anchors = _find_header(line)
                    if page_anchors:
                        header_seen = True
                        cols = sorted(
                            (x, name) for name, x in page_anchors.items()
                        )
                        bounds = [
                            (name, x, cols[i + 1][0] - 2 if i + 1 < len(cols) else 10000)
                            for i, (x, name) in enumerate(cols)
                        ]
                    continue
                joined = " ".join(t for _, t in line)
                if joined.startswith("* For the complete list"):
                    current = None
                    page_anchors = None
                    continue
                if re.match(r"^(F S:|S O:|D:|L:|F I|T R)", joined):
                    continue
                bucket = {name: [] for name, _, _ in bounds}
                for x, t in line:
                    for name, lo, hi in bounds:
                        if lo - 4 <= x < hi:
                            bucket[name].append(t)
                            break
                date_txt = " ".join(bucket["Date"])
                if DATE_RE.match(date_txt):
                    current = {
                        "owner": " ".join(bucket["Owner"]) or "Self",
                        "asset": bucket["Asset"][:],
                        "tx_type": " ".join(bucket["Transaction"]),
                        "tx_date": date_txt,
                        "notif_date": " ".join(bucket["Notification"]),
                        "amount": bucket["Amount"][:],
                    }
                    rows.append(current)
                elif current is not None and (bucket["Asset"] or bucket["Amount"]):
                    a = bucket["Asset"]
                    if a and (
                        a[0] == "D:" or (a[0] == "D" and len(a) > 1 and a[1] == ":")
                    ):
                        # description line, spans columns — keep as comment
                        current.setdefault("desc", []).extend(
                            a[1:] + bucket["Amount"]
                        )
                    else:
                        current["asset"].extend(a)
                        current["amount"].extend(bucket["Amount"])
        if not rows and not header_seen:
            return [], "unparsed_layout"
        return rows, "ok"


BAND_RE = re.compile(
    r"(?:Spouse[/\\]DC )?[Oo]ver \$[\d,]+|\$[\d,]+ - \$[\d,]+", re.IGNORECASE
)


def _canon_band(text: str) -> str:
    """Case/separator-insensitive band key. Kills the variant class
    ('Spouse/DC over $1,000,000' vs 'Spouse\\DC Over $1,000,000')."""
    return re.sub(r"\s+", " ", text.replace("\\", "/")).strip().lower()


HOUSE_BANDS_CANON = {_canon_band(k): v for k, v in HOUSE_BANDS.items()}
HOUSE_BANDS_CANON.setdefault(_canon_band("Spouse/DC Over $1,000,000"), (1000001, None))


def normalize_row(row, doc_id):
    asset_text = " ".join(row["asset"])
    amount_text = re.sub(r"\s+", " ", " ".join(row["amount"])).strip()
    m_band = BAND_RE.search(amount_text)
    if m_band and _canon_band(m_band.group(0)) in HOUSE_BANDS_CANON:
        low, high = HOUSE_BANDS_CANON[_canon_band(m_band.group(0))]
        spill = (amount_text[: m_band.start()] + amount_text[m_band.end():]).strip()
    else:
        # House filers may report exact dollar values instead of bands.
        # Accepts "$1,234", "$1,234.56", and degenerate "$.01" fractional rows.
        m_exact = re.match(
            r"^\$((?:[\d,]+(?:\.\d+)?)|(?:\.\d+))(?=\s|$)(?!\s*-)", amount_text
        )
        if not m_exact:
            raise IngestError(
                "house {} unmapped amount band {!r}".format(doc_id, amount_text)
            )
        low = high = int(round(float(m_exact.group(1).replace(",", ""))))
        spill = amount_text[m_exact.end():].strip()
    comment_bits = [b for b in (" ".join(row.get("desc", [])), spill) if b]
    side_key = re.sub(r"\s+", " ", row["tx_type"]).strip().upper()
    if side_key not in SIDE_MAP:
        raise IngestError(
            "house {} unmapped transaction code {!r}".format(doc_id, row["tx_type"])
        )
    m = TICKER_RE.search(asset_text)
    ticker = norm_ticker(m.group(1)) if m else None
    c = CODE_RE.search(asset_text)
    code = c.group(1) if c else None
    asset_type = ASSET_CODES.get(code, code)
    return {
        "ticker": ticker,
        "side": SIDE_MAP[side_key],
        "amt_low": low,
        "amt_high": high,
        "tx_date": dt.datetime.strptime(row["tx_date"], "%m/%d/%Y").date().isoformat(),
        "owner": row["owner"],
        "asset_name": re.sub(r"\s+", " ", asset_text).strip(),
        "asset_type": asset_type,
        "comment": " | ".join(comment_bits) or None,
    }


def upsert_person(con, filing):
    name = "{}, {}".format(filing.get("Last", ""), filing.get("First", "")).strip(", ")
    meta = json.dumps(
        {
            "state_dst": filing.get("StateDst"),
            "prefix": filing.get("Prefix"),
            "suffix": filing.get("Suffix"),
        }
    )
    row = con.execute("SELECT person_id FROM persons WHERE name=?", (name,)).fetchone()
    if row:
        return row[0]
    return con.execute(
        "INSERT INTO persons(name, type, cik_or_chamber, meta) "
        "VALUES (?, 'congress', 'house', ?)",
        (name, meta),
    ).lastrowid


def ingest_filing(con, filing, year, raw_dir, ua):
    doc_id = filing["DocID"]
    if con.execute(
        "SELECT 1 FROM ingested_filings WHERE filing_id=?", (doc_id,)
    ).fetchone():
        return "skip"
    disclosure = (
        dt.datetime.strptime(filing["FilingDate"], "%m/%d/%Y").date().isoformat()
    )
    label = "PTR {} {}".format(filing.get("StateDst", ""), filing.get("Year", ""))
    person_name = "{}, {}".format(filing.get("Last", ""), filing.get("First", ""))

    def record(status, n):
        con.execute(
            "INSERT INTO ingested_filings VALUES (?,?,?,?,?,?,?,?)",
            (doc_id, "house", status, person_name, label, disclosure, n,
             int(time.time())),
        )
        con.commit()

    pdf_path = fetch_pdf(year, doc_id, raw_dir, ua)
    if pdf_path is None:
        record("fetch_failed", 0)
        return "fetch_failed"
    rows, status = parse_ptr_pdf(pdf_path)
    if status == "paper":
        upsert_person(con, filing)
        record("paper", 0)
        return "paper"
    if status == "unparsed_layout":
        dest = pathlib.Path(UNPARSED_DIR)
        dest.mkdir(parents=True, exist_ok=True)
        shutil.copy2(pdf_path, dest / pdf_path.name)
        record("unparsed_layout", 0)
        return "unparsed_layout"
    person_id = upsert_person(con, filing)
    for i, row in enumerate(rows, 1):
        n = normalize_row(row, doc_id)
        lag = (
            dt.date.fromisoformat(disclosure) - dt.date.fromisoformat(n["tx_date"])
        ).days
        con.execute(
            "INSERT OR REPLACE INTO congress_trades("
            "person_id, ticker, side, amt_low, amt_high, tx_date, disclosure_date,"
            "lag_days, chamber, source, raw_ref, owner, asset_name, asset_type,"
            "comment, filing_id) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                person_id, n["ticker"], n["side"], n["amt_low"], n["amt_high"],
                n["tx_date"], disclosure, lag, "house", "house_clerk",
                "{}#{}".format(doc_id, i), n["owner"], n["asset_name"],
                n["asset_type"], n["comment"], doc_id,
            ),
        )
    record("electronic", len(rows))
    return "electronic"


def main(argv=None):
    ap = argparse.ArgumentParser(description="House Clerk PTR ingest, Phase 1b")
    ap.add_argument("--db", default=dbmod.DB_PATH_DEFAULT)
    ap.add_argument("--raw", default=RAW_DIR_DEFAULT)
    ap.add_argument("--start-year", type=int, default=dt.date.today().year)
    ap.add_argument("--min-year", type=int, default=2008)
    ap.add_argument("--max-details", type=int, default=0, help="smoke mode")
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

    details = 0
    horizon = None
    for year in range(args.start_year, args.min_year - 1, -1):
        entries = fetch_year_zip(year, raw_dir, ua)
        if entries is None:
            horizon = year + 1
            print("[house] {}FD.zip missing, horizon {}".format(year, horizon))
            break
        if not entries and year < args.start_year:
            horizon = year + 1
            print("[house] year {} has zero PTR entries, horizon {}".format(year, horizon))
            break
        print("[house] year={} ptr_filings={}".format(year, len(entries)), flush=True)
        for filing in entries:
            outcome = ingest_filing(con, filing, year, raw_dir, ua)
            if outcome == "electronic":
                details += 1
                if details % 100 == 0:
                    print("[house] {} parsed filings".format(details), flush=True)
            if args.max_details and details >= args.max_details:
                print("[house] SMOKE stop after {}".format(details))
                return 0
    if horizon is not None:
        con.execute(
            "INSERT OR REPLACE INTO meta_kv VALUES ('house_horizon_year', ?)",
            (str(horizon),),
        )
        con.commit()
    stats = con.execute(
        "SELECT status, COUNT(*) FROM ingested_filings WHERE chamber='house' "
        "GROUP BY status"
    ).fetchall()
    print("[house] DONE {}".format(dict(stats)))
    return 0


if __name__ == "__main__":
    sys.exit(main())
