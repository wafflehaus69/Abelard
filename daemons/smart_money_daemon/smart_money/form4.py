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


def daily_form4(contact, date):
    """(cik, issuer, path) Form 4 rows for a date (datetime.date)."""
    q = (date.month - 1) // 3 + 1
    url = DAILY_IDX.format(y=date.year, q=q, ymd=date.strftime("%Y%m%d"))
    time.sleep(PACE)
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


def parse_ownership(raw_xml):
    root = ET.fromstring(raw_xml.encode())

    def txt(path):
        e = root.find(path)
        return (e.text or "").strip() if e is not None else ""

    owner = txt(".//reportingOwner/reportingOwnerId/rptOwnerName")
    issuer = txt(".//issuer/issuerName")
    symbol = txt(".//issuer/issuerTradingSymbol")
    plan = txt(".//aff10b5One") == "1"
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
        })
    return {"owner": owner, "issuer": issuer, "symbol": symbol,
            "plan_flag": plan, "txns": txns}
