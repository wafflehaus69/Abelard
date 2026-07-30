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
        # Value-denominated securities (notes/bonds) report a dollar VALUE owned
        # after the transaction, not a share count. For those the per-share price
        # field carries the aggregate principal (footnoted), so shares*price is
        # meaningless — a structural signal the sanity guard keys on. See FINS/KYN
        # in the corpus: "5.364% Series C Senior Unsecured Notes ...".
        has_val = t.find(
            ".//postTransactionAmounts/valueOwnedFollowingTransaction/value") is not None
        has_sh = t.find(
            ".//postTransactionAmounts/sharesOwnedFollowingTransaction/value") is not None
        txns.append({
            "code": g(".//transactionCoding/transactionCode"),
            "security_title": g(".//securityTitle/value"),
            "shares": g(".//transactionShares/value"),
            "price": g(".//transactionPricePerShare/value"),
            "date": g(".//transactionDate/value"),
            "ad": g(".//transactionAcquiredDisposedCode/value"),
            "owned_after": g(
                ".//postTransactionAmounts/sharesOwnedFollowingTransaction/value"),
            "value_denominated": has_val and not has_sh,
        })
    # SM-O1 Table II: derivative transactions (options, warrants, rights). Some
    # fields are legitimately empty for certain security types (a performance
    # right has no exercise price or expiry) — recorded as empty, never guessed.
    deriv_txns = []
    for t in root.findall(".//derivativeTransaction"):
        def gd(p):
            e = t.find(p)
            return (e.text or "").strip() if e is not None else ""
        deriv_txns.append({
            "security_title": gd(".//securityTitle/value"),
            "code": gd(".//transactionCoding/transactionCode"),
            "shares": gd(".//transactionShares/value"),
            "price": gd(".//transactionPricePerShare/value"),
            "exercise_price": gd(".//conversionOrExercisePrice/value"),
            "date": gd(".//transactionDate/value"),
            "exercise_date": gd(".//exerciseDate/value"),
            "expiration_date": gd(".//expirationDate/value"),
            "underlying_title": gd(".//underlyingSecurity/underlyingSecurityTitle/value"),
            "underlying_shares": gd(".//underlyingSecurity/underlyingSecurityShares/value"),
        })
    return {"owner": owner, "owner_cik": owner_cik, "issuer": issuer,
            "issuer_cik": issuer_cik.lstrip("0") or issuer_cik,
            "symbol": symbol, "plan_flag": plan, "role": role, "txns": txns,
            "deriv_txns": deriv_txns}


def _f(x):
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


# --- price/value sanity guard ------------------------------------------------
# EDGAR Form 4 filings carry filer-side corruption that poisons any dollar
# aggregation. Four mechanisms are present in the corpus, all in the SOURCE XML
# (our extraction is faithful — the raw <transactionPricePerShare> literally
# holds the bad number):
#   1. debt/notes report the aggregate PRINCIPAL in transactionPricePerShare
#      (footnoted), and post-transaction is a dollar value not a share count
#      (value_denominated) -> shares*price = principal-squared (FINS, KYN).
#   2. some filers put the TOTAL proceeds in the per-share field (STNG's footnote
#      says so verbatim; HYEX, MYNZ, REEMF) -> value inflated by the share count.
#   3. some drop the decimal point: 1031.414 -> 1031414 (LLY, PSX rows).
#   4. ADS filings quote a per-ADS price against an ORDINARY-share count, off by
#      the ADS ratio (SVRE, "Each ADS represents 43,200 ... ordinary shares").
# We cannot recover the true dollar value without guessing, and doctrine is fail
# loud / never guess. So a flagged row keeps its raw shares/price for forensics
# and NULLs the derived `value`, recording the reason in value_flag. Share and
# insider-count metrics are unaffected. Mirrors queries.q_net_flows' read guard;
# fixing it here is the real solution (the query guard only masked net-$).
# Mechanism 4 (ADS) is NOT caught here — per-share price is plausible — and is a
# documented residual pending an ADS-ratio or market-cap cross-check.
PRICE_SANITY_MAX = 1_000_000.0   # > BRK.A (~$600k); no real US equity/ADS trades above
VALUE_SANITY_MAX = 1e11          # $100B; no single insider open-market txn nears it
CLOSE_RATIO_MAX = 10.0           # flag if per-share price is >10x off the EOD close


def value_sanity_flag(shares, price, value, value_denominated=False, close=None):
    """Reason string if the derived dollar `value` is untrustworthy, else None.
    Pure function, no I/O. `close` is the EOD close for the tx date when known;
    the close cross-check catches sub-ceiling corruption (e.g. a dropped decimal
    on a cheap stock) and is skipped when no close is available."""
    if value_denominated:
        return "value_denominated"
    if price is not None and abs(price) > PRICE_SANITY_MAX:
        return "price_over_max"
    if value is not None and abs(value) > VALUE_SANITY_MAX:
        return "value_over_max"
    if (close and close > 0 and price and price > 0
            and not (close / CLOSE_RATIO_MAX <= price <= close * CLOSE_RATIO_MAX)):
        return "price_vs_close"
    return None


def eod_close(con, ticker, tx_date):
    """EOD close for (ticker, tx_date) from the prices cache, or None. Best-effort
    input to the close cross-check: any miss (no row, no table) returns None and
    the check is skipped rather than blocking ingest."""
    if not ticker or not tx_date:
        return None
    try:
        row = con.execute(
            "SELECT close FROM prices WHERE ticker=? AND date=? AND close IS NOT NULL "
            "ORDER BY price_type LIMIT 1", (ticker, tx_date)).fetchone()
    except Exception:  # noqa: BLE001 - prices table optional; never block ingest
        return None
    return row[0] if row and row[0] is not None else None


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
        # Sanity guard: quarantine an untrustworthy dollar value (NULL it, record
        # the reason) while keeping raw shares/price. The close cross-check is
        # looked up lazily only when the cheap checks pass, so the hot universal
        # ingest path pays one indexed prices lookup per otherwise-clean priced row.
        flag = value_sanity_flag(shares, price, value, t.get("value_denominated", False))
        if flag is None and price and price > 0:
            close = eod_close(con, ticker, t.get("date"))
            if close and not (close / CLOSE_RATIO_MAX <= price <= close * CLOSE_RATIO_MAX):
                flag = "price_vs_close"
        con.execute(
            "INSERT OR IGNORE INTO form4_transactions("
            "accession, tx_index, reporting_person, reporting_cik, issuer,"
            "issuer_cik, ticker, code, plan_flag, shares, price, value,"
            "ownership_after, tx_date, filed_date, role, ingest_regime, value_flag)"
            " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (accession, i, parsed.get("owner"), cik, parsed.get("issuer"),
             parsed.get("issuer_cik") or None, ticker,
             t.get("code"), 1 if parsed.get("plan_flag") else 0, shares, price,
             None if flag else value, _f(t.get("owned_after")), t.get("date"),
             filed_date, parsed.get("role") or None, regime, flag),
        )
        n += 1
    return n, bool(parsed.get("owner"))


def persist_derivatives(con, accession, parsed, ticker, filed_date,
                        regime="watchlist"):
    """SM-O1 P1: persist every Table II (derivative) transaction into
    form4_derivatives. Idempotent by (accession, tx_index) — re-running a filing
    is a no-op. Mirrors persist_transactions for the derivative leg; the person
    upsert is left to persist_transactions (called alongside, even for
    derivative-only filings). Returns rows_persisted."""
    cik = parsed.get("owner_cik") or None
    n = 0
    for i, t in enumerate(parsed.get("deriv_txns", [])):
        con.execute(
            "INSERT OR IGNORE INTO form4_derivatives("
            "accession, tx_index, reporting_person, reporting_cik, issuer,"
            "issuer_cik, ticker, security_title, code, plan_flag, shares, price,"
            "exercise_price, tx_date, exercise_date, expiration_date,"
            "underlying_title, underlying_shares, filed_date, role, ingest_regime)"
            " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (accession, i, parsed.get("owner"), cik, parsed.get("issuer"),
             parsed.get("issuer_cik") or None, ticker,
             t.get("security_title") or None, t.get("code"),
             1 if parsed.get("plan_flag") else 0, _f(t.get("shares")),
             _f(t.get("price")), _f(t.get("exercise_price")), t.get("date"),
             t.get("exercise_date") or None, t.get("expiration_date") or None,
             t.get("underlying_title") or None, _f(t.get("underlying_shares")),
             filed_date, parsed.get("role") or None, regime),
        )
        n += 1
    return n
