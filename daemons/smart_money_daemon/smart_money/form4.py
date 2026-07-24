"""Form 4 tail leg (SM-4 STEP 3 Leg B). EDGAR daily index for yesterday+today,
filtered to issuers whose ticker is on the overlay (or insider-type registry
entries). Open-market transactions (code P, S) become events; A/M/G are counted
in the envelope only. The 10b5-1 plan flag rides every event.
"""
import re
import time
import xml.etree.ElementTree as ET

import requests

UA_TMPL = "Abelard-SmartMoney mdiba personal research {}"
TICKERS_URL = "https://www.sec.gov/files/company_tickers.json"
DAILY_IDX = "https://www.sec.gov/Archives/edgar/daily-index/{y}/QTR{q}/form.{ymd}.idx"
ARCH = "https://www.sec.gov/Archives/edgar/data/{cik}/{acc_nodash}/{doc}"
PACE = 0.15
OPEN_MARKET = {"P", "S"}
COUNTED_ONLY = {"A", "M", "G", "F", "C"}


def _ua(contact):
    return {"User-Agent": UA_TMPL.format(contact)}


def ticker_to_cik(contact, tickers):
    """Map requested tickers -> zero-padded CIK using EDGAR's registry."""
    time.sleep(PACE)
    r = requests.get(TICKERS_URL, headers=_ua(contact), timeout=30)
    r.raise_for_status()
    want = {t.upper() for t in tickers}
    out = {}
    for row in r.json().values():
        tk = row["ticker"].upper()
        if tk in want:
            out[tk] = str(row["cik_str"]).zfill(10)
    return out


def daily_form4(contact, date, pace=None):
    """(cik, issuer, path) Form 4 rows for a date (datetime.date)."""
    q = (date.month - 1) // 3 + 1
    url = DAILY_IDX.format(y=date.year, q=q, ymd=date.strftime("%Y%m%d"))
    time.sleep(PACE if pace is None else pace)
    r = requests.get(url, headers=_ua(contact), timeout=30)
    if r.status_code != 200:
        return None  # index may not exist yet (weekend/holiday/today-early)
    rows = []
    for line in r.text.splitlines():
        if not line.startswith("4 "):
            continue
        # form.idx columns: "4  <issuer>  <cik>  <YYYYMMDD>  edgar/data/.../acc.txt"
        # date is YYYYMMDD without dashes; anchor on the 8-digit date + edgar path.
        m = re.match(r"^4\s+(.+?)\s+(\d+)\s+(\d{8})\s+(edgar/\S+)\s*$", line)
        if m:
            ymd = m.group(3)
            rows.append({
                "issuer": m.group(1).strip(),
                "cik": m.group(2),
                "date": "{}-{}-{}".format(ymd[:4], ymd[4:6], ymd[6:]),
                "path": m.group(4),
            })
    return rows


def fetch_form4_xml(contact, path):
    """path like edgar/data/CIK/ACC.txt -> parse the ownership XML doc."""
    acc = path.rsplit("/", 1)[-1].replace(".txt", "")
    cik = path.split("/")[2]
    acc_nodash = acc.replace("-", "")
    idx = ARCH.format(cik=cik, acc_nodash=acc_nodash, doc="index.json")
    time.sleep(PACE)
    d = requests.get(idx, headers=_ua(contact), timeout=30).json()
    doc = None
    for it in d["directory"]["item"]:
        if it["name"].lower().endswith(".xml"):
            doc = it["name"]
    if not doc:
        return None
    url = ARCH.format(cik=cik, acc_nodash=acc_nodash, doc=doc)
    time.sleep(PACE)
    return parse_ownership(requests.get(url, headers=_ua(contact), timeout=30).text)


ARCHIVES = "https://www.sec.gov/Archives/{path}"


def fetch_form4_from_txt(contact, path, pace=None):
    """SINGLE-fetch path (SM-U1 PH1 optimization): pull the full submission .txt
    directly from the daily-index path and extract the inline ownership XML,
    skipping the index.json round-trip. Halves EDGAR requests per filing.
    Returns parsed dict or None. `pace` overrides the inter-request sleep."""
    time.sleep(PACE if pace is None else pace)
    r = requests.get(ARCHIVES.format(path=path), headers=_ua(contact), timeout=30)
    if r.status_code != 200:
        return None
    m = re.search(r"<ownershipDocument>.*?</ownershipDocument>", r.text, re.S)
    if not m:
        return None
    return parse_ownership(m.group(0))


def parse_ownership(raw_xml):
    root = ET.fromstring(raw_xml.encode())

    def txt(path):
        e = root.find(path)
        return (e.text or "").strip() if e is not None else ""

    owner = txt(".//reportingOwner/reportingOwnerId/rptOwnerName")
    owner_cik = txt(".//reportingOwner/reportingOwnerId/rptOwnerCik")
    issuer = txt(".//issuer/issuerName")
    issuer_cik = txt(".//issuer/issuerCik")
    symbol = txt(".//issuer/issuerTradingSymbol")
    plan = txt(".//aff10b5One") == "1"
    rel = ".//reportingOwner/reportingOwnerRelationship/"
    roles = []
    if txt(rel + "isDirector") == "1":
        roles.append("director")
    if txt(rel + "isOfficer") == "1":
        roles.append("officer:" + (txt(rel + "officerTitle") or "?"))
    if txt(rel + "isTenPercentOwner") == "1":
        roles.append("10pct")
    role = ",".join(roles)
    txns = []
    for t in root.findall(".//nonDerivativeTransaction"):
        def g(p):
            e = t.find(p)
            return (e.text or "").strip() if e is not None else ""
        txns.append({
            "code": g(".//transactionCoding/transactionCode"),
            "shares": g(".//transactionShares/value"),
            "price": g(".//transactionPricePerShare/value"),
            "date": g(".//transactionDate/value"),
            "ad": g(".//transactionAcquiredDisposedCode/value"),
            "owned_after": g(
                ".//postTransactionAmounts/sharesOwnedFollowingTransaction/value"),
        })
    return {"owner": owner, "owner_cik": owner_cik, "issuer": issuer,
            "issuer_cik": issuer_cik.lstrip("0") or issuer_cik,
            "symbol": symbol, "plan_flag": plan, "role": role, "txns": txns}


def _f(x):
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def persist_transactions(con, accession, parsed, ticker, filed_date,
                         regime="watchlist"):
    """SM-F4 Step 1: persist EVERY parsed transaction into form4_transactions and
    upsert the reporting person (type=insider, CIK carried). Idempotent by
    (accession, tx_index) — re-running the same filing is a no-op. Congress rows
    are never touched. Returns (rows_persisted, person_upserted)."""
    cik = parsed.get("owner_cik") or None
    if parsed.get("owner"):
        con.execute(
            "INSERT INTO persons(name, type, cik_or_chamber, meta) "
            "VALUES (?, 'insider', ?, ?) "
            "ON CONFLICT(name) DO UPDATE SET "
            "cik_or_chamber=COALESCE(excluded.cik_or_chamber, persons.cik_or_chamber)",
            (parsed["owner"], cik, parsed.get("role") or None),
        )
    n = 0
    for i, t in enumerate(parsed.get("txns", [])):
        shares = _f(t.get("shares"))
        price = _f(t.get("price"))
        value = round(shares * price, 2) if shares is not None and price is not None else None
        con.execute(
            "INSERT OR IGNORE INTO form4_transactions("
            "accession, tx_index, reporting_person, reporting_cik, issuer,"
            "issuer_cik, ticker, code, plan_flag, shares, price, value,"
            "ownership_after, tx_date, filed_date, role, ingest_regime)"
            " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (accession, i, parsed.get("owner"), cik, parsed.get("issuer"),
             parsed.get("issuer_cik") or None, ticker,
             t.get("code"), 1 if parsed.get("plan_flag") else 0, shares, price,
             value, _f(t.get("owned_after")), t.get("date"), filed_date,
             parsed.get("role") or None, regime),
        )
        n += 1
    return n, bool(parsed.get("owner"))
