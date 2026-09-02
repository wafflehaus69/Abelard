"""PS-1 Phase 2.1 — universe sync. One adapter, three configured sources.

    Wikipedia S&P 500   membership + GICS sector & sub-industry + CIK
    Wikipedia NASDAQ-100 membership only (its taxonomy is ICB, and no CIK)
    iShares holdings CSV membership + GICS sector + weight + as-of date

Ruling 1: v1 ships **SPX + NDX**. IVV is configured ON as an independent second
sector opinion on the S&P 500; IWM is wired but OFF, so switching the Russell
2000 on later is a config row rather than new code.

**Identity** (A3). ``instrument_id = <cik10>.<class>``, never CIK alone —
GOOG/GOOGL, FOX/FOXA, NWS/NWSA and BRK-A/BRK-B each share one CIK, and keying on
it would collide two price series into one ``UNIQUE(instrument_id, date)`` slot
and raise a fact-change every night. The class discriminator is resolved by
lookup, never by string surgery: ``CMCSA`` and ``GOOGL`` are genuine five-letter
tickers, not concatenated share classes.

**No lxml.** ``abelard_common`` declares only ``requests``; the table parser
here is stdlib ``html.parser``. A shared library should not grow a dependency
for one caller.
"""

from __future__ import annotations

import csv
import io
import re
from dataclasses import dataclass, field
from html.parser import HTMLParser
from typing import Iterable, Sequence

from ..http_client import HttpClient
from .schema import PriceStoreError

SP500_URL = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
NDX_URL = "https://en.wikipedia.org/wiki/List_of_NASDAQ-100_companies"
# The changes table is NOT on the main list page -- it moved to its own
# article, the same restructuring that split the NDX pages. Verified 2026-09-02:
# "List of S&P 500 companies" has exactly two tables, constituents and a navbox.
SP500_CHANGES_URL = ("https://en.wikipedia.org/wiki/"
                     "Historical_components_of_the_S%26P_500")
SEC_TICKERS_URL = "https://www.sec.gov/files/company_tickers_exchange.json"
ISHARES = {
    "IVV": ("SPX", "https://www.ishares.com/us/products/239726/"
                   "ishares-core-sp-500-etf/latest-holdings.csv"),
    "IWM": ("RUT", "https://www.ishares.com/us/products/239710/"
                   "ishares-russell-2000-etf/latest-holdings.csv"),
}
# Ruling 1: v1 is SPX + NDX. IVV rides along as a cross-check; IWM stays off.
DEFAULT_ISHARES = ("IVV",)

# The iShares sector column is GICS-shaped with two deviations, measured.
SECTOR_FIXUPS = {"Communication": "Communication Services"}
NOT_A_SECTOR = {"Other", "-", ""}

_CLASS_RE = re.compile(r"\(Class\s+([A-Z])\)", re.IGNORECASE)


class UniverseError(PriceStoreError):
    def __init__(self, message: str, *, stage: str = "universe") -> None:
        super().__init__(message, stage=stage)


@dataclass(frozen=True)
class IndexChange:
    """One dated add/remove from the historical-components table."""

    effective_date: str
    added_ticker: str | None
    removed_ticker: str | None
    reason: str


@dataclass
class Constituent:
    ticker: str
    name: str
    index_code: str
    source: str
    cik: str | None = None
    sector: str | None = None
    sub_industry: str | None = None
    class_code: str | None = None
    weight: float | None = None


# --------------------------------------------------------------- html tables --

class _TableParser(HTMLParser):
    """Minimal wikitable extractor: rows of cell text, per table."""

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.tables: list[list[list[str]]] = []
        self._t: list[list[str]] | None = None
        self._row: list[str] | None = None
        self._cell: list[str] | None = None

    def handle_starttag(self, tag, attrs):
        if tag == "table":
            self._t = []
        elif tag == "tr" and self._t is not None:
            self._row = []
        elif tag in ("td", "th") and self._row is not None:
            self._cell = []

    def handle_endtag(self, tag):
        if tag == "table" and self._t is not None:
            self.tables.append(self._t)
            self._t = None
        elif tag == "tr" and self._t is not None and self._row is not None:
            if self._row:
                self._t.append(self._row)
            self._row = None
        elif tag in ("td", "th") and self._cell is not None and self._row is not None:
            self._row.append(re.sub(r"\s+", " ", "".join(self._cell)).strip())
            self._cell = None

    def handle_data(self, data):
        if self._cell is not None:
            self._cell.append(data)


def _pick_table(html: str, want: Iterable[str]) -> list[list[str]]:
    """The first table whose header row contains all of ``want``."""
    p = _TableParser()
    p.feed(html)
    lowered = [w.lower() for w in want]
    for tbl in p.tables:
        if not tbl:
            continue
        header = [c.lower() for c in tbl[0]]
        if all(any(w in h for h in header) for w in lowered) and len(tbl) > 20:
            return tbl
    raise UniverseError("no table found with headers {}".format(list(want)))


def _column(header: Sequence[str], *names: str) -> int:
    for i, h in enumerate(header):
        for n in names:
            if n.lower() in h.lower():
                return i
    raise UniverseError("column {} not in {}".format(names, list(header)))


# ------------------------------------------------------------------ sources --

def fetch_sp500(client: HttpClient) -> list[Constituent]:
    """The only source that carries GICS sector, sub-industry AND CIK."""
    tbl = _pick_table(client.get_text(SP500_URL), ("symbol", "gics sector"))
    h = tbl[0]
    i_sym = _column(h, "symbol")
    i_sec = _column(h, "gics sector")
    i_sub = _column(h, "gics sub")
    i_cik = _column(h, "cik")
    i_nam = _column(h, "security")
    out = []
    for row in tbl[1:]:
        if len(row) <= max(i_sym, i_sec, i_sub, i_cik, i_nam):
            continue
        name = row[i_nam]
        m = _CLASS_RE.search(name)
        out.append(Constituent(
            ticker=row[i_sym].strip(), name=name, index_code="SPX",
            source="wikipedia_spx", cik=row[i_cik].strip().zfill(10),
            sector=row[i_sec].strip() or None,
            sub_industry=row[i_sub].strip() or None,
            class_code=m.group(1).upper() if m else None,
        ))
    if not out:
        raise UniverseError("S&P 500 table parsed to zero rows")
    return out


def fetch_ndx(client: HttpClient) -> list[Constituent]:
    """Membership ONLY. The table's taxonomy is ICB, not GICS, and there is no
    CIK column — so no classification row is emitted (A6). An NDX-only name
    carries no sector rather than a hand-mapped guess."""
    tbl = _pick_table(client.get_text(NDX_URL), ("ticker", "company"))
    h = tbl[0]
    i_sym, i_nam = _column(h, "ticker", "symbol"), _column(h, "company")
    out = []
    for row in tbl[1:]:
        if len(row) <= max(i_sym, i_nam):
            continue
        name = row[i_nam]
        m = _CLASS_RE.search(name)
        out.append(Constituent(
            ticker=row[i_sym].strip(), name=name, index_code="NDX",
            source="wikipedia_ndx",
            class_code=m.group(1).upper() if m else None,
        ))
    if not out:
        raise UniverseError("NASDAQ-100 table parsed to zero rows")
    return out


def parse_ishares(text: str, index_code: str, source: str) -> tuple[list[Constituent], str | None]:
    """iShares ``latest-holdings.csv``: nine metadata lines, then the header.

    Returns (constituents, as_of). Non-equity rows (cash, futures) are dropped —
    6 of 1,967 in IWM.
    """
    lines = text.splitlines()
    as_of = None
    for line in lines[:9]:
        if line.lower().startswith("fund holdings as of"):
            parts = next(csv.reader([line]), [])
            as_of = parts[1].strip() if len(parts) > 1 else None
    start = next((i for i, l in enumerate(lines) if l.lower().startswith("ticker,")), None)
    if start is None:
        raise UniverseError("{}: no 'Ticker,' header row in holdings CSV".format(source))
    out = []
    for row in csv.DictReader(io.StringIO("\n".join(lines[start:]))):
        if (row.get("Asset Class") or "").strip() != "Equity":
            continue
        sector = (row.get("Sector") or "").strip()
        sector = SECTOR_FIXUPS.get(sector, sector)
        try:
            weight = float((row.get("Weight (%)") or "").replace(",", ""))
        except ValueError:
            weight = None
        out.append(Constituent(
            ticker=(row.get("Ticker") or "").strip(),
            name=(row.get("Name") or "").strip(),
            index_code=index_code, source=source,
            sector=None if sector in NOT_A_SECTOR else sector,
            weight=weight,
        ))
    return out, as_of


def fetch_ishares(client: HttpClient, fund: str) -> tuple[list[Constituent], str | None]:
    index_code, url = ISHARES[fund]
    return parse_ishares(client.get_text(url), index_code, "ishares_" + fund.lower())


def sec_client(contact: str) -> HttpClient:
    """SEC requires a declared contact in the User-Agent and returns 403 without
    one — a browser UA is refused. Same rule the SM and Capex daemons follow via
    ``EDGAR_CONTACT``; fail loud rather than send a blank one."""
    if not contact or "@" not in contact:
        raise UniverseError(
            "SEC requires a contact in the User-Agent (an email). Set EDGAR_CONTACT "
            "or pass --contact; sending a blank or browser UA gets a 403.",
            stage="sec_contact",
        )
    return HttpClient(user_agent="Abelard PS-1 prices {}".format(contact))


def parse_changes(html: str) -> list[IndexChange]:
    """The historical-components table: a two-level header
    (``Effective Date | Added | Removed | Reason``) over (``Ticker | Security``),
    so seven physical columns.

    This is the survivorship fix. Backfilling five years of CURRENT members only
    means every 2021-2025 validation in the handoff runs on names that survived
    to 2026 — optimistic by construction, and invisibly so.
    """
    p = _TableParser()
    p.feed(html)
    for tbl in p.tables:
        if len(tbl) < 20:
            continue
        head = " ".join(tbl[0]).lower()
        if "added" not in head or "removed" not in head or "effective" not in head:
            continue
        out: list[IndexChange] = []
        for row in tbl[1:]:
            if len(row) < 5 or row[0].lower().startswith("ticker"):
                continue
            date = _parse_us_date(row[0])
            if not date:
                continue
            out.append(IndexChange(
                effective_date=date,
                added_ticker=(row[1].strip() or None),
                removed_ticker=(row[3].strip() or None),
                reason=(row[5].strip() if len(row) > 5 else ""),
            ))
        if out:
            return out
    raise UniverseError("no add/remove table on the historical-components page")


_MONTH_NAMES = ["January", "February", "March", "April", "May", "June", "July",
                "August", "September", "October", "November", "December"]
# Both spellings, because the two sources disagree: Wikipedia writes
# "August 18, 2026" and the iShares header writes "Aug 31, 2026". Accepting only
# the full name made every weight row fall back to the sync date, which in turn
# made every historical reconciliation report INSUFFICIENT -- two bugs chained
# into one silent wrong answer.
_MONTHS = {m: i for i, m in enumerate(_MONTH_NAMES, start=1)}
_MONTHS.update({m[:3]: i for i, m in enumerate(_MONTH_NAMES, start=1)})


def _parse_us_date(text: str) -> str | None:
    m = re.match(r"([A-Za-z]+)\.?\s+(\d{1,2}),\s*(\d{4})", text.strip())
    if not m:
        return None
    month = _MONTHS.get(m.group(1)) or _MONTHS.get(m.group(1)[:3])
    if not month:
        return None
    return "{}-{:02d}-{:02d}".format(m.group(3), month, int(m.group(2)))


def fetch_changes(client: HttpClient) -> list[IndexChange]:
    return parse_changes(client.get_text(SP500_CHANGES_URL))


def fetch_sec_map(client: HttpClient) -> dict[str, tuple[str, str, str]]:
    """ticker -> (cik10, name, exchange). Keyless. 10,391 rows.

    ``client`` must carry a contact UA — build it with :func:`sec_client`.
    """
    body = client.get_json(SEC_TICKERS_URL)
    fields = body["fields"]
    i_c, i_n, i_t = fields.index("cik"), fields.index("name"), fields.index("ticker")
    i_e = fields.index("exchange") if "exchange" in fields else None
    out = {}
    for row in body["data"]:
        out[str(row[i_t]).upper()] = (
            str(row[i_c]).zfill(10), row[i_n],
            row[i_e] if i_e is not None else "",
        )
    return out


# ----------------------------------------------------------------- identity --

def normalise(ticker: str) -> dict[str, str]:
    """One ticker in every notation in play. Resolution is by lookup elsewhere;
    this only renders the forms so an alias row exists for each."""
    t = ticker.strip().upper()
    dot = t.replace("-", ".")
    dash = t.replace(".", "-")
    return {"dot": dot, "dash": dash, "concat": t.replace(".", "").replace("-", ""),
            "vendor": dash}


def assign_instrument_ids(
    constituents: Sequence[Constituent],
    sec_map: dict[str, tuple[str, str, str]],
) -> tuple[dict[str, Constituent], dict[str, str]]:
    """ticker -> Constituent, with ``cik`` and ``class_code`` settled.

    The rule, per AD.3:

    1. class from a ``(Class X)`` parenthetical where the source named one;
    2. absent, and the CIK carries one ticker -> ``'0'``;
    3. absent, and the CIK carries several (the Berkshire case, where Wikipedia
       does not disambiguate because BRK-A is not an index member) ->
       deterministic ordinal by ticker, marked ``class_source='ordinal'`` so the
       fallback is visibly a fallback.
    """
    # Merge on a CANONICAL ticker, not the raw string. Wikipedia says 'BRK.B',
    # iShares says 'BRKB'; keyed raw they become two instruments for one
    # security, and the second would carry no CIK. The canonical form comes from
    # a reverse index over the SEC file's own tickers -- so 'BRKB' resolves
    # because SEC lists 'BRK-B' whose concat form is 'BRKB'. That is lookup, not
    # string surgery: nothing is transformed unless the SEC file vouches for the
    # result. 'CMCSA' and 'GOOGL' stay themselves, because they match exactly.
    concat_index: dict[str, str] = {}
    for t in sec_map:
        concat_index.setdefault(t.replace(".", "").replace("-", ""), t)

    def canonical(raw: str) -> tuple[str, str | None]:
        forms = normalise(raw)
        for form in (raw, forms["dash"], forms["dot"]):
            if form in sec_map:
                return form, sec_map[form][0]
        hit = concat_index.get(forms["concat"])
        if hit:
            return hit, sec_map[hit][0]
        return raw, None

    merged: dict[str, Constituent] = {}
    raw_to_key: dict[str, str] = {}
    for c in constituents:
        key, cik = canonical(c.ticker)
        # Both directions: the source's own notation AND the canonical form.
        # The first occurrence has its ticker rewritten to the canonical one, so
        # a later pass over the same row objects must still resolve.
        raw_to_key[c.ticker] = key
        raw_to_key[key] = key
        cur = merged.get(key)
        if cur is None:
            c.ticker = key
            c.cik = c.cik or cik
            merged[key] = c
            continue
        cur.cik = cur.cik or c.cik or cik
        cur.sector = cur.sector or c.sector
        cur.sub_industry = cur.sub_industry or c.sub_industry
        cur.class_code = cur.class_code or c.class_code

    by_cik: dict[str, list[str]] = {}
    for t, c in merged.items():
        if c.cik:
            by_cik.setdefault(c.cik, []).append(t)

    for cik, tickers in by_cik.items():
        if len(tickers) == 1:
            t = tickers[0]
            if not merged[t].class_code:
                merged[t].class_code = "0"
            continue
        for ordinal, t in enumerate(sorted(tickers), start=1):
            if not merged[t].class_code:
                merged[t].class_code = str(ordinal)
    return merged, raw_to_key


def instrument_id(c: Constituent) -> str:
    if not c.cik:
        # Never a silent drop. Provisional, flagged, and reported nightly.
        return "NOCIK." + c.ticker
    return "{}.{}".format(c.cik, c.class_code or "0")


def class_source_for(c: Constituent, ordinal_used: bool) -> str:
    if c.cik is None:
        return "single"
    if ordinal_used:
        return "ordinal"
    return "wikipedia" if c.class_code and c.class_code.isalpha() else "single"


# --------------------------------------------------------------------- sync --

@dataclass
class SyncReport:
    as_of: str
    instruments: int = 0
    memberships: int = 0
    classifications: int = 0
    provisional: list[str] = field(default_factory=list)
    weights: int = 0
    historical_rows: int = 0
    historical_names: int = 0
    disagreements: list[tuple[str, str, str, str]] = field(default_factory=list)
    departures: list[tuple[str, str]] = field(default_factory=list)

    def render(self) -> str:
        out = ["universe sync as_of {}".format(self.as_of),
               "  instruments      {}".format(self.instruments),
               "  memberships      {}".format(self.memberships),
               "  classifications  {}".format(self.classifications),
               "  provisional (no CIK): {}".format(len(self.provisional))]
        if self.provisional:
            out.append("     " + " ".join(sorted(self.provisional)[:25]))
        out.append("  classification disagreements (logged, NOT blocking): {}"
                   .format(len(self.disagreements)))
        for t, a, b, c in self.disagreements[:20]:
            out.append("     {:<7} {} says {!r} | {} says {!r}".format(t, a, b, "", c)
                       .replace("|  says", "|"))
        out.append("  index departures recorded: {}".format(len(self.departures)))
        out.append("  index weights stored: {}".format(self.weights))
        out.append("  historical membership: {} as-of rows over {} departed names "
                   "(survivorship backfill)".format(
                       self.historical_rows, self.historical_names))
        return "\n".join(out)


def sync(
    con,
    client: HttpClient,
    as_of: str,
    contact: str = "",
    funds: Sequence[str] = DEFAULT_ISHARES,
    include_ndx: bool = True,
    history_since: str | None = None,
) -> SyncReport:
    """Pull every configured source and write as-of rows. Never deletes.

    ``client`` serves Wikipedia and iShares (browser UA); SEC gets its own
    client built from ``contact``, because it refuses a browser UA outright.
    """
    rep = SyncReport(as_of=as_of)
    sec_map = fetch_sec_map(sec_client(contact))

    rows: list[Constituent] = list(fetch_sp500(client))
    if include_ndx:
        rows += fetch_ndx(client)
    # The holdings file carries its OWN "Fund Holdings as of" date. Weights must
    # be stamped with THAT, not with the sync date: they describe the fund on the
    # file's date, and a reconciliation of a past session needs the weights that
    # were in force then. Stamping them "today" made every historical
    # reconciliation report INSUFFICIENT -- found by running it.
    fund_as_of: dict[str, str] = {}
    for fund in funds:
        got, f_asof = fetch_ishares(client, fund)
        if f_asof:
            parsed = _parse_us_date(f_asof)
            if parsed:
                for c in got:
                    fund_as_of[c.source] = parsed
        rows += got

    merged, raw_to_key = assign_instrument_ids(rows, sec_map)
    ordinals = {
        t for t, c in merged.items()
        if c.class_code and c.class_code.isdigit() and c.class_code != "0"
    }

    for ticker, c in sorted(merged.items()):
        iid = instrument_id(c)
        provisional = 1 if not c.cik else 0
        if provisional:
            rep.provisional.append(ticker)
        con.execute(
            "INSERT INTO instruments (instrument_id, cik, class_code, class_source,"
            " name, primary_ticker, source, provisional, first_seen, last_seen)"
            " VALUES (?,?,?,?,?,?,?,?,?,?)"
            " ON CONFLICT(instrument_id) DO UPDATE SET last_seen=excluded.last_seen,"
            "   name=COALESCE(instruments.name, excluded.name)",
            (iid, c.cik, c.class_code or "0",
             class_source_for(c, ticker in ordinals), c.name, ticker, c.source,
             provisional, as_of, as_of),
        )
        rep.instruments += 1
        for notation, form in normalise(ticker).items():
            con.execute(
                "INSERT OR IGNORE INTO ticker_aliases (instrument_id, ticker,"
                " notation, valid_from, valid_to, source) VALUES (?,?,?,?,NULL,?)",
                (iid, form, notation, as_of, c.source),
            )

    # Membership, as-of. Every (instrument, index, source) seen today is present.
    seen: set[tuple[str, str, str]] = set()
    for c in rows:
        iid = instrument_id(merged[raw_to_key[c.ticker]])
        key = (iid, c.index_code, c.source)
        if key in seen:
            continue
        seen.add(key)
        try:
            con.execute("INSERT INTO index_membership VALUES (?,?,?,1,?)",
                        (iid, c.index_code, as_of, c.source))
            rep.memberships += 1
        except Exception:
            pass

    for c in rows:
        if c.weight is None:
            continue
        iid = instrument_id(merged[raw_to_key[c.ticker]])
        try:
            con.execute(
                "INSERT INTO index_weights (instrument_id, index_code, as_of,"
                " weight, source) VALUES (?,?,?,?,?)",
                (iid, c.index_code, fund_as_of.get(c.source, as_of), c.weight,
                 c.source))
            rep.weights += 1
        except Exception:
            pass

    # A name that WAS present under a source and is not in today's pull has left.
    # It is not deleted -- a new as-of row with present=0 records the exit.
    for r in con.execute(
        "SELECT m.instrument_id, m.index_code, m.source, MAX(m.as_of) AS latest,"
        "       m.present FROM index_membership m GROUP BY m.instrument_id,"
        " m.index_code, m.source"
    ).fetchall():
        key = (r["instrument_id"], r["index_code"], r["source"])
        if r["present"] == 1 and r["latest"] < as_of and key not in seen:
            try:
                con.execute("INSERT INTO index_membership VALUES (?,?,?,0,?)",
                            (r["instrument_id"], r["index_code"], as_of, r["source"]))
                rep.departures.append((r["instrument_id"], r["index_code"]))
            except Exception:
                pass

    # Classification: append-only, one row per (instrument, taxonomy, source).
    # Wikipedia and IVV may disagree; BOTH are kept and the disagreement is
    # logged for Mando. CR-1 owns any blocking behaviour-mismatch gate.
    per_ticker: dict[str, dict[str, str]] = {}
    for c in rows:
        if not c.sector:
            continue
        ckey = raw_to_key[c.ticker]
        iid = instrument_id(merged[ckey])
        try:
            con.execute(
                "INSERT INTO classification (instrument_id, taxonomy, sector,"
                " sub_industry, as_of, source) VALUES (?,'GICS',?,?,?,?)",
                (iid, c.sector, c.sub_industry, as_of, c.source))
            rep.classifications += 1
        except Exception:
            pass
        per_ticker.setdefault(ckey, {})[c.source] = c.sector

    for ticker, bysrc in sorted(per_ticker.items()):
        if len(set(bysrc.values())) > 1:
            items = sorted(bysrc.items())
            rep.disagreements.append((ticker, items[0][0], items[0][1], items[1][1]))

    if history_since:
        rep.historical_rows, rep.historical_names = backfill_membership(
            con, client, sec_map, since=history_since)

    con.commit()
    return rep


def backfill_membership(
    con,
    client: HttpClient,
    sec_map: dict[str, tuple[str, str, str]],
    since: str,
) -> tuple[int, int]:
    """Write as-of membership for names that have LEFT the index since ``since``.

    Without this, a five-year backfill of current members is a survivorship
    sample: every 2021-2025 test in the handoff runs only on names that made it
    to 2026. Measured on the live table, 104 distinct names left the S&P 500
    since 2021-01-04 — roughly a sixth of the period's universe, missing, and
    invisible unless you go looking.

    Each departed name gets an instrument row, an alias, a present=1 row as of
    ``since`` (it was in the index then) and a present=0 row at its effective
    removal date. Its prices are then fetched by the ordinary backfill, because
    ``_targets`` reads the LATEST as-of row per (instrument, index, source) and
    a name that has left is simply excluded from today's fetch set — while its
    history stays queryable as-of.
    """
    changes = [c for c in fetch_changes(client) if c.effective_date >= since]
    concat_index: dict[str, str] = {}
    for t in sec_map:
        concat_index.setdefault(t.replace(".", "").replace("-", ""), t)

    current = {r[0] for r in con.execute(
        "SELECT DISTINCT primary_ticker FROM instruments")}
    rows = 0
    names: set[str] = set()
    for ch in changes:
        tk = (ch.removed_ticker or "").strip()
        if not tk or tk in current:
            continue
        forms = normalise(tk)
        hit = next((f for f in (tk, forms["dash"], forms["dot"]) if f in sec_map), None)
        hit = hit or concat_index.get(forms["concat"])
        cik = sec_map[hit][0] if hit else None
        canon = hit or tk
        iid = "{}.0".format(cik) if cik else "NOCIK." + canon
        names.add(canon)
        con.execute(
            "INSERT INTO instruments (instrument_id, cik, class_code, class_source,"
            " name, primary_ticker, source, provisional, first_seen, last_seen)"
            " VALUES (?,?,'0','single',?,?,'wikipedia_changes',?,?,?)"
            " ON CONFLICT(instrument_id) DO NOTHING",
            (iid, cik, canon, canon, 0 if cik else 1, since, ch.effective_date))
        for notation, form in normalise(canon).items():
            con.execute(
                "INSERT OR IGNORE INTO ticker_aliases (instrument_id, ticker,"
                " notation, valid_from, valid_to, source) VALUES (?,?,?,?,?,?)",
                (iid, form, notation, since, ch.effective_date, "wikipedia_changes"))
        for as_of_row, present in ((since, 1), (ch.effective_date, 0)):
            try:
                con.execute("INSERT INTO index_membership VALUES (?,?,?,?,?)",
                            (iid, "SPX", as_of_row, present, "wikipedia_changes"))
                rows += 1
            except Exception:
                pass
    con.commit()
    return rows, len(names)
