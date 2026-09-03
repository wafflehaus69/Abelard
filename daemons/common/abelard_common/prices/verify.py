"""PS-1B Phase 2V.2–2V.4 — cross-vendor verification, drafts, and hole fill.

Three jobs that must not be confused with each other:

* **Verify** (2V.2). Compare what Tiingo says against what the store holds and
  record the verdict. Writes to ``verification`` and nothing else.
* **Draft** (2V.3). Where they disagree on a price, quarantine the session and
  auto-write a *staging* file with ``authored_by: ""`` so
  ``corrections.load()`` refuses it. The machine may propose; only a human may
  sign. Nothing here writes a fact or a correction.
* **Fill** (2V.4). Where the primary left a ``vendor_null`` and the verifier has
  the session, INSERT it. This is the one place 2V writes a price, and it is a
  first write, never a change — G3, ruled by Mando 2026-09-02.

**Why fill and correct are separate code, not a shared function with a flag.**
G3 draws the line at "has this date ever had a value". A hole is an absence and
filling it invents nothing that was there before; a held value is a claim, and
changing it revises the record. A single function with `overwrite=True` would
put one bad call between those two, which is exactly the distinction the whole
substrate is built on. Different tables (``fills`` vs ``corrections``),
different authority (automatic vs human), different code.
"""

from __future__ import annotations

import json
import sqlite3
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Sequence

from . import reconstruct as R
from .calendar import sessions_between
from .schema import PriceStoreError
from .vendor_tiingo import (CROSS_VENDOR_EPS, TiingoBar, TiingoError,
                            TiingoUnknownSymbol, TiingoVendor, check_quota,
                            vendor_symbol)

DRAFTS_DIR = Path("abelard_common/corrections/drafts")

# A dividend is compared to the cent; anything finer is vendor rounding.
DIV_TOLERANCE = 0.005
# A split ratio is exact arithmetic (2:1, 3:2); this is float noise only.
SPLIT_TOLERANCE = 1e-4
# A hole must be this many sessions old before it is filled. Today's absence is
# usually a session that has not settled, not a hole.
FILL_MIN_AGE_SESSIONS = 2


@dataclass
class Comparison:
    """What a second vendor said about one name. Pure result, no I/O."""

    sessions_compared: int = 0
    agreements: int = 0
    price_disagreements: list[tuple[str, float, float]] = field(default_factory=list)
    ca_disagreements: list[str] = field(default_factory=list)
    holes_vendor_has: dict[str, TiingoBar] = field(default_factory=dict)

    @property
    def kind(self) -> str:
        if self.sessions_compared == 0:
            return "INSUFFICIENT"
        if self.price_disagreements:
            return "DISAGREE_PRICE"
        if self.ca_disagreements:
            return "DISAGREE_CA"
        return "VERIFIED"

    @property
    def detail(self) -> str:
        bits = []
        if self.price_disagreements:
            bits.append("{} price disagreements, worst {}".format(
                len(self.price_disagreements),
                max(self.price_disagreements,
                    key=lambda d: abs(d[1] / d[2] - 1) if d[2] else 0)[0]))
        if self.ca_disagreements:
            bits.append("; ".join(self.ca_disagreements[:4]))
        if self.holes_vendor_has:
            bits.append("{} holes the verifier can fill".format(
                len(self.holes_vendor_has)))
        return " | ".join(bits) or "{} sessions agree".format(self.agreements)


def compare_series(
    held: dict[str, float],
    held_status: dict[str, str],
    bars: Sequence[TiingoBar],
    declared_splits: Sequence[R.Split],
    declared_divs: Sequence[R.Dividend],
) -> Comparison:
    """Pure comparison. ``held`` is raw close by date from ``prices_raw``."""
    c = Comparison()
    by_date = {b.date: b for b in bars}

    for date, bar in sorted(by_date.items()):
        ours = held.get(date)
        status = held_status.get(date)
        if bar.raw_close is None:
            continue
        if ours is None or status in ("vendor_null",):
            # The primary has no usable price and the verifier does.
            if status == "vendor_null" or date not in held:
                c.holes_vendor_has[date] = bar
            continue
        if status != "ok":
            continue                      # already quarantined; not a fact to check
        c.sessions_compared += 1
        if ours and abs(bar.raw_close / ours - 1.0) <= CROSS_VENDOR_EPS:
            c.agreements += 1
        else:
            c.price_disagreements.append((date, bar.raw_close, ours))

    # Splits: every session where the verifier carries a factor != 1 must match a
    # declared split on the same date and ratio, and vice versa.
    theirs = {b.date: b.split_factor for b in bars if abs(b.split_factor - 1.0) > SPLIT_TOLERANCE}
    ours_s = {s.effective_date: s.ratio for s in declared_splits}
    span = {b.date for b in bars}
    for d, ratio in sorted(theirs.items()):
        if d not in ours_s:
            c.ca_disagreements.append(
                "verifier reports a {}:1 split on {} that is not declared".format(
                    ratio, d))
        elif abs(ours_s[d] - ratio) > SPLIT_TOLERANCE:
            c.ca_disagreements.append(
                "split ratio on {} differs: declared {} vs verifier {}".format(
                    d, ours_s[d], ratio))
    for d, ratio in sorted(ours_s.items()):
        if d in span and d not in theirs:
            c.ca_disagreements.append(
                "declared {}:1 split on {} that the verifier does not carry".format(
                    ratio, d))

    theirs_d = {b.date: b.div_cash for b in bars if b.div_cash}
    ours_d = {d.ex_date: d.amount for d in declared_divs}
    for d, amt in sorted(theirs_d.items()):
        if d not in ours_d:
            c.ca_disagreements.append(
                "verifier reports a {:.4f} dividend on {} that is not declared"
                .format(amt, d))
        elif abs(ours_d[d] - amt) > DIV_TOLERANCE:
            c.ca_disagreements.append(
                "dividend on {} differs: declared {:.4f} vs verifier {:.4f}"
                .format(d, ours_d[d], amt))
    return c


# ------------------------------------------------------------------- drafts --

def draft_for(
    instrument_id: str,
    ticker: str,
    comparison: Comparison,
    as_of: str,
    vendor_name: str = "tiingo",
) -> dict:
    """A staging payload the machine wrote and only a human can apply.

    ``authored_by`` is empty ON PURPOSE. ``corrections.load()`` refuses an
    unattributed file, so this cannot be applied by any path that does not go
    through a person adding their name. The machine has done the work of finding
    and evidencing the disagreement; the judgement of which vendor is right stays
    human, because nothing in the data settles it — MNST needed a human to know
    Yahoo was the broken one.
    """
    rows = []
    for date, theirs, ours in comparison.price_disagreements:
        ratio = theirs / ours if ours else None
        rows.append({
            "instrument_id": instrument_id,
            "ticker": ticker,
            "date": date,
            "corrected_close": theirs,
            "kind": "corrected",
            "reason": ("PROPOSED, UNSIGNED. The primary and the verifier "
                       "disagree on this session. Decide which is right before "
                       "signing; the ratio is the clue."),
            "evidence": {
                "held_primary": ours,
                "verifier": theirs,
                "verifier_name": vendor_name,
                "ratio_verifier_to_held": round(ratio, 6) if ratio else None,
                "note": ("a clean 2.0 or 0.5 usually means a split applied to "
                         "one vendor's session and not the other"),
            },
        })
    return {
        "authored_by": "",
        "source": vendor_name,
        "drafted_at": as_of,
        "note": ("AUTO-DRAFTED by the verification sweep. Unsigned by design: "
                 "corrections.load() refuses an empty authored_by. Fill in a "
                 "name and a reason per row only after deciding which vendor is "
                 "right."),
        "rows": rows,
    }


def write_draft(payload: dict, ticker: str, as_of: str,
                root: Path | str = DRAFTS_DIR) -> Path:
    d = Path(root)
    d.mkdir(parents=True, exist_ok=True)
    path = d / "{}_{}.json".format(as_of, ticker)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return path


# ------------------------------------------------------------------- sweep --

@dataclass
class SweepResult:
    run_asof: int
    checked: list[tuple[str, str, str]] = field(default_factory=list)   # id, ticker, kind
    drafts: list[str] = field(default_factory=list)
    quarantined: int = 0
    errors: list[str] = field(default_factory=list)

    def counts(self) -> dict[str, int]:
        out: dict[str, int] = {}
        for _i, _t, k in self.checked:
            out[k] = out.get(k, 0) + 1
        return out

    def render(self) -> str:
        out = ["[verify] {} names: {}".format(
            len(self.checked),
            " ".join("{}={}".format(k, v) for k, v in sorted(self.counts().items())))]
        out.append("[verify] sessions quarantined: {}".format(self.quarantined))
        for p in self.drafts:
            out.append("[verify] DRAFT (unsigned) {}".format(p))
        for e in self.errors:
            out.append("[verify] ERROR {}".format(e))
        return "\n".join(out)


def _held(con: sqlite3.Connection, iid: str) -> tuple[dict[str, float], dict[str, str]]:
    prices, status = {}, {}
    for r in con.execute(
        "SELECT date, close, status FROM prices_raw WHERE instrument_id=? ORDER BY date",
        (iid,)):
        status[r["date"]] = r["status"]
        if r["close"] is not None:
            prices[r["date"]] = r["close"]
    return prices, status


def _declared(con: sqlite3.Connection, iid: str):
    splits, divs = [], []
    for r in con.execute(
        "SELECT effective_date, kind, ratio, amount FROM corporate_actions"
        " WHERE instrument_id=? ORDER BY effective_date", (iid,)):
        if r["kind"] == "split" and r["ratio"]:
            splits.append(R.Split(r["effective_date"], r["ratio"]))
        elif r["kind"] == "dividend" and r["amount"] is not None:
            divs.append(R.Dividend(r["effective_date"], r["amount"]))
    return splits, divs


def rotation_targets(con: sqlite3.Connection, n: int) -> list[tuple[str, str]]:
    """The ``n`` names verified longest ago (never-verified first)."""
    seen = {r["instrument_id"]: r["as_of"] for r in con.execute(
        "SELECT instrument_id, MAX(as_of) AS as_of FROM verification"
        " WHERE vendor='tiingo' GROUP BY instrument_id")}
    rows = con.execute(
        "SELECT DISTINCT m.instrument_id, i.primary_ticker FROM index_membership m"
        " JOIN instruments i USING (instrument_id) WHERE m.present=1"
        " ORDER BY i.primary_ticker").fetchall()
    ordered = sorted(rows, key=lambda r: (seen.get(r["instrument_id"], ""),
                                          r["primary_ticker"] or ""))
    return [(r["instrument_id"], r["primary_ticker"]) for r in ordered[:n]]


def sweep(
    con: sqlite3.Connection,
    vendor: TiingoVendor,
    n: int,
    as_of: str,
    since: str,
    run_asof: int | None = None,
    drafts_root: Path | str = DRAFTS_DIR,
    progress=None,
) -> SweepResult:
    """The rotation. Verifies ``n`` names; writes verdicts, quarantines and
    unsigned drafts. Never a fact, never a correction."""
    run_asof = run_asof or int(time.time())
    targets = rotation_targets(con, n)
    # Up front, against the whole plan: a sweep that dies at request 43 of 60
    # has spent the quota and left the store half-verified.
    check_quota(con, len(targets))
    res = SweepResult(run_asof=run_asof)

    for iid, ticker in targets:
        symbol = vendor_symbol(con, iid)
        if not symbol:
            res.errors.append("{}: no vendor alias".format(ticker))
            continue
        try:
            bars = vendor.daily(symbol, since, as_of)
        except TiingoUnknownSymbol:
            _record(con, iid, as_of, "TIINGO_UNKNOWN", Comparison(), run_asof)
            res.checked.append((iid, ticker, "TIINGO_UNKNOWN"))
            continue
        except TiingoError as exc:
            res.errors.append("{}: {}".format(ticker, str(exc)[:140]))
            continue

        held, status = _held(con, iid)
        splits, divs = _declared(con, iid)
        cmp_ = compare_series(held, status, bars, splits, divs)
        _record(con, iid, as_of, cmp_.kind, cmp_, run_asof)
        res.checked.append((iid, ticker, cmp_.kind))

        if cmp_.price_disagreements:
            for date, _theirs, _ours in cmp_.price_disagreements:
                con.execute(
                    "INSERT OR IGNORE INTO quarantine (instrument_id, date, reason,"
                    " source, released, quarantined_at, run_asof)"
                    " VALUES (?,?,?,?,0,?,?)",
                    (iid, date,
                     "cross-vendor price disagreement with tiingo", "verify",
                     run_asof, run_asof))
                res.quarantined += 1
            path = write_draft(
                draft_for(iid, ticker, cmp_, as_of), ticker, as_of, drafts_root)
            res.drafts.append(str(path))
        con.commit()
        if progress:
            progress("{} {} {}".format(ticker, cmp_.kind, cmp_.detail[:90]))
    return res


def _record(con, iid, as_of, kind, cmp_, run_asof) -> None:
    con.execute(
        "INSERT OR REPLACE INTO verification (instrument_id, as_of, vendor,"
        " sessions_compared, agreements, disagreements, kind, detail, run_asof)"
        " VALUES (?,?,'tiingo',?,?,?,?,?,?)",
        (iid, as_of, cmp_.sessions_compared, cmp_.agreements,
         len(cmp_.price_disagreements) + len(cmp_.ca_disagreements),
         kind, cmp_.detail[:500], run_asof))
    con.commit()


# -------------------------------------------------------------- hole fill --

@dataclass
class FillResult:
    filled: list[tuple[str, str, float]] = field(default_factory=list)
    unfillable: list[tuple[str, str]] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)

    def render(self) -> str:
        out = ["[fill] filled {} holes from the verifier".format(len(self.filled))]
        out.append("[fill] holes neither vendor has: {}".format(len(self.unfillable)))
        for e in self.errors[:5]:
            out.append("[fill] ERROR {}".format(e))
        return "\n".join(out)


def fillable_holes(
    con: sqlite3.Connection,
    as_of: str,
    min_age_sessions: int = FILL_MIN_AGE_SESSIONS,
) -> dict[str, list[str]]:
    """instrument_id -> vendor_null dates old enough to fill.

    Today's absence is usually a session that has not settled rather than a
    hole, so a gap must age before it is treated as one.
    """
    out: dict[str, list[str]] = {}
    for r in con.execute(
        "SELECT instrument_id, date FROM prices_raw WHERE status='vendor_null'"
        " ORDER BY instrument_id, date"):
        if len(sessions_between(r["date"], as_of)) < min_age_sessions:
            continue
        out.setdefault(r["instrument_id"], []).append(r["date"])
    return out


class YahooAsFiller:
    """Adapts the primary vendor to the filler interface.

    G3 says a ``vendor_null`` may be filled by **any sourced vendor**, and the
    primary is one of them: the 2026-08-28 outage is a transient-fault
    hypothesis before it is a coverage gap, so the cheapest and most faithful
    first move is to ask Yahoo again. A hole the primary fills on a retry was
    never a coverage gap at all, and recording it as sourced from the verifier
    would misattribute it.
    """

    source = "yahoo_v8"

    def __init__(self, vendor):
        self._v = vendor

    def daily(self, symbol: str, start: str, end: str) -> list[TiingoBar]:
        series = self._v.fetch(symbol, start, end)
        return [TiingoBar(b.date, b.close, b.close, 1.0, 0.0, b.volume)
                for b in series.bars]


def fill_holes(
    con: sqlite3.Connection,
    vendor,
    as_of: str,
    run_asof: int | None = None,
    limit: int | None = None,
    progress=None,
    source: str | None = None,
    enforce_quota: bool = True,
) -> FillResult:
    """G3: a ``vendor_null`` is a FIRST WRITE and may be filled automatically
    with attribution. A held value is never touched here — there is no code path
    in this function that can reach one, which is the point.
    """
    run_asof = run_asof or int(time.time())
    source = source or getattr(vendor, "source", "tiingo")
    res = FillResult()
    holes = fillable_holes(con, as_of)
    names = list(holes)[:limit] if limit else list(holes)
    # Only the metered vendor has a ceiling to respect; the primary is free.
    if enforce_quota:
        check_quota(con, len(names))

    for iid in names:
        dates = holes[iid]
        symbol = vendor_symbol(con, iid)
        if not symbol:
            res.errors.append("{}: no vendor alias".format(iid))
            continue
        try:
            bars = {b.date: b for b in vendor.daily(symbol, min(dates), max(dates))}
        except TiingoUnknownSymbol:
            res.unfillable.extend((iid, d) for d in dates)
            continue
        except TiingoError as exc:
            res.errors.append("{}: {}".format(iid, str(exc)[:140]))
            continue
        for d in dates:
            bar = bars.get(d)
            if bar is None or bar.raw_close is None:
                res.unfillable.append((iid, d))
                continue
            # Two shapes of hole, and each gets the mechanism that fits it.
            #
            #   * The primary returned the session with no price, so a
            #     vendor_null row already occupies (instrument_id, date).
            #     prices_raw is insert-only, so that row STAYS -- it is the
            #     record that the primary had nothing -- and the fill is an
            #     overlay the view honours.
            #   * The primary never returned the session at all, so the slot is
            #     empty and the fill is a plain insert with status='filled'.
            #
            # Either way `fills` carries the evidence, so "what did we take from
            # the verifier" has one answer regardless of which shape it was.
            try:
                con.execute(
                    "INSERT INTO prices_raw (instrument_id, date, close, status,"
                    " source, fetched_at, run_asof) VALUES (?,?,?,'filled',?,?,?)",
                    (iid, d, bar.raw_close, source, int(time.time()), run_asof))
            except sqlite3.IntegrityError:
                pass          # a vendor_null row holds the slot; overlay it is
            con.execute(
                "INSERT OR IGNORE INTO fills (instrument_id, date, filled_close,"
                " source, evidence, filled_at, run_asof) VALUES (?,?,?,?,?,?,?)",
                (iid, d, bar.raw_close, source,
                 json.dumps({"adj_close": bar.adj_close, "volume": bar.volume,
                             "split_factor": bar.split_factor,
                             "primary_status": "vendor_null",
                             "filled_from": source}),
                 int(time.time()), run_asof))
            res.filled.append((iid, d, bar.raw_close))
        con.commit()
        if progress:
            progress("{} filled {}/{}".format(symbol, len(
                [f for f in res.filled if f[0] == iid]), len(dates)))
    return res


def fills_for(con: sqlite3.Connection, instrument_id: str) -> dict[str, float]:
    """Filled closes by date, for the view rebuild."""
    return {r["date"]: r["filled_close"] for r in con.execute(
        "SELECT date, filled_close FROM fills WHERE instrument_id=?",
        (instrument_id,))}
