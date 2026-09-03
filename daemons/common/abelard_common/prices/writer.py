"""PS-1 Phase 2 — the one writer.

Every daemon reads this store; only this module writes it. The order's five
verbs live here: ``nightly``, ``backfill``, ``refetch``, ``universe_sync`` (in
``universe.py``) and ``status``.

The rules this module is built around, each traceable to a defect CR-R0 measured
in the layer it replaces:

* **One ``run_asof`` per run** (E13 window alignment). Every row a run writes
  carries the same stamp, so "what did the store know at run R" is answerable.
* **``last_date_held`` comes from the rows the vendor RETURNED**, never the span
  requested. Recording the request as if it were the response is what froze 343
  tickers, SPY among them, while the nightly went on counting successes.
* **A fact never changes.** If the vendor now offers a different raw close for a
  date already held, that is a fail-loud fact-change event: the name is skipped,
  nothing is written, and it is reported. It is never an update.
* **Request span is ``last_date_held -> today``**, not "the next missing day".
  The old cache walked forward one session per night, ~13 months in arrears; a
  span request closes the gap in one call.
* **Telemetry before persistence.** The run row is written and committed before
  the data commits, so a crashed run still leaves evidence it started.
"""

from __future__ import annotations

import datetime as dt
import json
import sqlite3
import time
from dataclasses import dataclass, field, replace
from typing import Callable, Iterable, Sequence

from . import reconstruct as R
from .calendar import is_final_session, sessions_behind
from .schema import PriceStoreError
from .vendor import VendorError, VendorSeries, YahooVendor

# The nightly asks from last_date_held forward. This caps a first-ever or
# long-stale name so one run cannot silently turn into a backfill.
MAX_NIGHTLY_SPAN_DAYS = 400

# 5y floor, per the history-depth ruling.
DEFAULT_BACKFILL_START = "2021-01-04"


@dataclass
class NameResult:
    """What happened to one instrument in one run. The unit of telemetry and of
    the status report."""

    instrument_id: str
    symbol: str
    status: str  # ok | no_rows | fact_change | quarantined | vendor_error
    rows_returned: int = 0
    rows_inserted: int = 0
    actions_detected: int = 0
    quarantined: int = 0
    last_date_held: str | None = None
    detail: str = ""


@dataclass
class RunResult:
    run_asof: int
    names: list[NameResult] = field(default_factory=list)
    requests_made: int = 0

    def counts(self) -> dict[str, int]:
        out: dict[str, int] = {}
        for n in self.names:
            out[n.status] = out.get(n.status, 0) + 1
        return out

    @property
    def fact_changes(self) -> list[NameResult]:
        return [n for n in self.names if n.status == "fact_change"]


# ------------------------------------------------------------------ helpers --

def _today() -> str:
    return dt.date.today().isoformat()


def held_raw_closes(con: sqlite3.Connection, instrument_id: str) -> dict[str, float]:
    """The dates we have actually called FACTS.

    ``status='ok'`` only, and deliberately so. A quarantined row is a record of
    what the vendor said, not a claim about the world — including it here would
    freeze a corrupt name forever: once quarantined, every later (correct) fetch
    would look like a fact change and the name could never recover.
    """
    held = {
        r["date"]: r["close"]
        for r in con.execute(
            "SELECT date, close FROM prices_raw WHERE instrument_id=?"
            " AND close IS NOT NULL AND status='ok'",
            (instrument_id,),
        )
    }
    # A human correction is the exit from perpetual fail-loud. Where Mando has
    # authorised one, the corrected value is what we BELIEVE, so it is what the
    # vendor is compared against -- otherwise a vendor legitimately fixing its
    # own bad print would fail the fact gate forever with no way to accept it.
    # The prices_raw row itself is never touched.
    # A session quarantined AFTER ingest (by the cross-vendor sweep) is no
    # longer a fact we would compare a vendor against -- same reasoning as the
    # status='quarantined' exclusion above, and for the same reason: leaving it
    # in would freeze the name, since every later correct fetch would read as a
    # fact change.
    for d in quarantined_dates(con, instrument_id):
        held.pop(d, None)
    held.update(corrections_for(con, instrument_id))
    return held


def _fills_for(con: sqlite3.Connection, instrument_id: str) -> dict[str, float]:
    """Closes taken from the verification vendor to fill a hole the primary
    left. Kept out of ``held_raw_closes``: a fill is not something the primary
    ever claimed, so it is not a fact to hold the primary to."""
    try:
        return {r["date"]: r["filled_close"] for r in con.execute(
            "SELECT date, filled_close FROM fills WHERE instrument_id=?",
            (instrument_id,))}
    except sqlite3.OperationalError:
        return {}          # store predates v3


def quarantined_dates(con: sqlite3.Connection, instrument_id: str) -> set[str]:
    """Dates under an unreleased post-ingest quarantine.

    Append-only, so "current" is the newest row per date and a release is a
    later row with released=1 rather than an edit.
    """
    return {
        r["date"] for r in con.execute(
            "SELECT date, released FROM quarantine q WHERE instrument_id=?"
            " AND quarantined_at = (SELECT MAX(q2.quarantined_at) FROM quarantine q2"
            "   WHERE q2.instrument_id=q.instrument_id AND q2.date=q.date)",
            (instrument_id,))
        if not r["released"]
    }


def corrections_for(con: sqlite3.Connection, instrument_id: str) -> dict[str, float]:
    """The latest authored correction per date. Append-only, so 'latest' is
    max(authored_at) and the superseded ones stay on the record."""
    return {
        r["date"]: r["corrected_close"]
        for r in con.execute(
            "SELECT date, corrected_close FROM corrections WHERE instrument_id=?"
            " AND corrected_close IS NOT NULL AND authored_at = ("
            "  SELECT MAX(c2.authored_at) FROM corrections c2"
            "  WHERE c2.instrument_id = corrections.instrument_id"
            "    AND c2.date = corrections.date)",
            (instrument_id,),
        )
    }


def current_factor_version(con: sqlite3.Connection, instrument_id: str) -> int:
    row = con.execute(
        "SELECT MAX(version) FROM adjustment_factors WHERE instrument_id=?",
        (instrument_id,),
    ).fetchone()
    return int(row[0]) if row and row[0] is not None else 0


def declared_actions(
    con: sqlite3.Connection, instrument_id: str
) -> tuple[list[R.Split], list[R.Dividend]]:
    """Everything the vendor has ever declared for this name, from the store."""
    splits, divs = [], []
    for r in con.execute(
        "SELECT effective_date, kind, ratio, amount FROM corporate_actions "
        "WHERE instrument_id=? ORDER BY effective_date",
        (instrument_id,),
    ):
        if r["kind"] == "split" and r["ratio"]:
            splits.append(R.Split(r["effective_date"], r["ratio"]))
        elif r["kind"] == "dividend" and r["amount"] is not None:
            divs.append(R.Dividend(r["effective_date"], r["amount"]))
    return splits, divs


def _record_actions(
    con: sqlite3.Connection, instrument_id: str, series: VendorSeries, run_asof: int
) -> int:
    """Append newly declared corporate actions. Returns how many are new.

    ``corporate_actions`` is append-only, so a re-declaration of something we
    already hold is an INSERT that the primary key rejects — caught and counted,
    never an upsert.
    """
    new = 0
    for s in series.splits:
        try:
            con.execute(
                "INSERT INTO corporate_actions (instrument_id, effective_date, kind,"
                " ratio, amount, declared_at, source, run_asof)"
                " VALUES (?,?,'split',?,NULL,?,?,?)",
                (instrument_id, s.effective_date, s.ratio, series.fetched_at,
                 "yahoo_v8_events", run_asof),
            )
            new += 1
        except sqlite3.IntegrityError:
            pass
    for d in series.dividends:
        try:
            con.execute(
                "INSERT INTO corporate_actions (instrument_id, effective_date, kind,"
                " ratio, amount, declared_at, source, run_asof)"
                " VALUES (?,?,'dividend',NULL,?,?,?,?)",
                (instrument_id, d.ex_date, d.amount, series.fetched_at,
                 "yahoo_v8_events", run_asof),
            )
            new += 1
        except sqlite3.IntegrityError:
            pass
    return new


def _rebuild_view(
    con: sqlite3.Connection,
    instrument_id: str,
    splits: Sequence[R.Split],
    dividends: Sequence[R.Dividend],
    version: int,
    computed_at: int,
) -> None:
    """Recompute the factor series at ``version`` and rebuild ``adjusted_view``.

    ``adjusted_view`` is derived and therefore replaceable — that is why the
    schema deliberately leaves it unlocked. ``adjustment_factors`` rows are
    immutable, so a re-version writes a new version beside the old one.
    """
    rows = [
        R.RawBar(r["date"], None, None, None, r["close"], None, r["status"])
        for r in con.execute(
            "SELECT date, close, status FROM prices_raw WHERE instrument_id=? "
            "ORDER BY date", (instrument_id,)
        )
    ]
    blocked = quarantined_dates(con, instrument_id)
    usable = {b.date: b.close for b in rows
              if b.status in ("ok", "filled") and b.close is not None
              and b.date not in blocked}
    # A fill overlays a vendor_null slot the primary already occupies. Applied
    # before corrections, because a correction outranks a fill: one is a human
    # adjudication, the other an automatic first write.
    for d, v in _fills_for(con, instrument_id).items():
        if d not in blocked:
            usable.setdefault(d, v)
    # The view honours corrections; the fact does not move. A correction applies
    # to ANY date, including one currently quarantined -- rescuing a session the
    # detector could not adjudicate is exactly what the human path is for.
    usable.update(corrections_for(con, instrument_id))
    if not usable:
        return
    factors = R.adjustment_factor_series(sorted(usable), usable, dividends, splits)
    con.executemany(
        "INSERT OR IGNORE INTO adjustment_factors (instrument_id, date, factor,"
        " version, computed_at) VALUES (?,?,?,?,?)",
        [(instrument_id, d, f, version, computed_at) for d, f in factors.items()],
    )
    con.execute("DELETE FROM adjusted_view WHERE instrument_id=?", (instrument_id,))
    con.executemany(
        "INSERT INTO adjusted_view (instrument_id, date, adj_close, factor_version)"
        " VALUES (?,?,?,?)",
        [(instrument_id, d, usable[d] * factors[d], version) for d in sorted(usable)],
    )


# -------------------------------------------------------------------- ingest --

def ingest_series(
    con: sqlite3.Connection,
    instrument_id: str,
    series: VendorSeries,
    run_asof: int,
    now_epoch: float | None = None,
) -> NameResult:
    """Fold one fetched series into the store. The heart of every verb.

    Order of operations matters and is not arbitrary:

    1. record declared actions FIRST — reconstruction depends on them;
    2. reconstruct raw from the vendor's close plus the FULL declared history
       (store + this response), not just this response's events;
    3. detect anomalies on the VENDOR series (where a declared split must be
       absent), quarantine the affected span;
    4. compare against held facts — a disagreement aborts the name entirely;
    5. insert, then re-version factors if anything new was declared.
    """
    res = NameResult(instrument_id, series.symbol, "ok")

    # Drop any session that has not closed yet. An in-progress session is not a
    # fact, and prices_raw is insert-only -- committing an intraday print would
    # make the next fetch a fact change for a price that was never wrong, only
    # unfinished. Observed during the build across ~240 names.
    series = replace(series, bars=[b for b in series.bars
                                   if is_final_session(b.date, now_epoch)])
    res.rows_returned = sum(1 for b in series.bars if b.close is not None)
    if not series.bars:
        res.status = "no_rows"
        return res

    res.actions_detected = _record_actions(con, instrument_id, series, run_asof)
    splits, dividends = declared_actions(con, instrument_id)

    raw = R.reconstruct(series.bars, splits)
    anomalies = R.detect_anomalies(series.bars, splits)
    span = R.quarantine_span(anomalies, [b.date for b in series.bars], splits)
    raw = R.apply_quarantine(raw, span)
    res.quarantined = len(span)

    # (4) A recorded fact does not change.
    #
    # The comparison is asymmetric on purpose. On the HELD side only facts count
    # (see held_raw_closes). On the OFFERED side EVERY reconstructed value counts,
    # quarantined or not: the question is "has the vendor changed its story about
    # a date we already committed to?", and it has done so whether or not this
    # fetch also happens to trip the anomaly detector. Filtering the offered side
    # by status (as a first draft did) let a restatement ride in behind a
    # quarantine and skip the fact gate entirely.
    offered = {b.date: b.close for b in raw if b.close is not None}
    changes = R.fact_changes(held_raw_closes(con, instrument_id), offered)
    if changes:
        res.status = "fact_change"
        res.detail = "; ".join(c.message() for c in changes[:3])
        if len(changes) > 3:
            res.detail += " (+{} more)".format(len(changes) - 3)
        return res

    for a in anomalies:
        con.execute(
            "INSERT OR IGNORE INTO adjustment_events (instrument_id, effective_date,"
            " implied_ratio, kind, detected_at, evidence, version)"
            " VALUES (?,?,?,?,?,?,?)",
            (instrument_id, a.date, a.implied_ratio, a.kind, series.fetched_at,
             json.dumps(a.evidence), current_factor_version(con, instrument_id) + 1),
        )

    inserted = 0
    for b in raw:
        try:
            con.execute(
                "INSERT INTO prices_raw (instrument_id, date, open, high, low, close,"
                " volume, status, source, fetched_at, run_asof)"
                " VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                (instrument_id, b.date, b.open, b.high, b.low, b.close, b.volume,
                 b.status, "yahoo_v8", series.fetched_at, run_asof),
            )
            inserted += 1
        except sqlite3.IntegrityError:
            # Already held and identical (step 4 proved it). Idempotent re-run.
            pass
    res.rows_inserted = inserted

    con.executemany(
        "INSERT OR REPLACE INTO vendor_adjusted (instrument_id, date,"
        " vendor_adjclose, source, fetched_at) VALUES (?,?,?,?,?)",
        [(instrument_id, d, v, "yahoo_v8", series.fetched_at)
         for d, v in series.vendor_adjclose.items()],
    )

    if res.actions_detected or current_factor_version(con, instrument_id) == 0:
        version = current_factor_version(con, instrument_id) + 1
        _rebuild_view(con, instrument_id, splits, dividends, version, series.fetched_at)
    else:
        _rebuild_view(con, instrument_id, splits, dividends,
                      current_factor_version(con, instrument_id), series.fetched_at)

    # (2.5) last_date_held from RETURNED rows, and only from a non-null close.
    res.last_date_held = series.last_date
    con.execute(
        "INSERT INTO freshness (instrument_id, last_date_held, last_fetch_at,"
        " last_fetch_status) VALUES (?,?,?,?)"
        " ON CONFLICT(instrument_id) DO UPDATE SET"
        "   last_date_held=COALESCE(excluded.last_date_held, freshness.last_date_held),"
        "   last_fetch_at=excluded.last_fetch_at,"
        "   last_fetch_status=excluded.last_fetch_status",
        (instrument_id, res.last_date_held, series.fetched_at,
         "quarantined" if span else "ok"),
    )
    if span:
        res.status = "quarantined"
    elif res.rows_returned == 0:
        res.status = "no_rows"
    return res


# --------------------------------------------------------------------- verbs --

def _targets(con: sqlite3.Connection) -> list[tuple[str, str]]:
    """(instrument_id, vendor symbol) for every name present in any index.

    Membership is as-of, so "present" means the latest as-of row for that
    (instrument, index) says present=1.
    """
    return [
        (r["instrument_id"], r["ticker"])
        for r in con.execute(
            "SELECT DISTINCT m.instrument_id, a.ticker FROM index_membership m"
            " JOIN ticker_aliases a ON a.instrument_id = m.instrument_id"
            "   AND a.notation='vendor' AND a.valid_to IS NULL"
            " WHERE m.present=1 AND m.as_of = ("
            "   SELECT MAX(m2.as_of) FROM index_membership m2"
            "   WHERE m2.instrument_id=m.instrument_id AND m2.index_code=m.index_code"
            "     AND m2.source=m.source)"
            " ORDER BY a.ticker"
        )
    ]


def _open_run(con: sqlite3.Connection, run_asof: int) -> None:
    """Telemetry before persistence: the run row is committed before any data."""
    con.execute(
        "INSERT OR IGNORE INTO run_telemetry (run_asof, started_at, status)"
        " VALUES (?,?,'running')",
        (run_asof, int(time.time())),
    )
    con.commit()


def _close_run(con: sqlite3.Connection, run: RunResult, status: str) -> None:
    agg = {
        "rows_returned": sum(n.rows_returned for n in run.names),
        "rows_inserted": sum(n.rows_inserted for n in run.names),
        "actions": sum(n.actions_detected for n in run.names),
        "quarantined": sum(1 for n in run.names if n.status == "quarantined"),
    }
    con.execute(
        "UPDATE run_telemetry SET finished_at=?, requests_made=?, rows_returned=?,"
        " rows_inserted=?, names_refetched=?, actions_detected=?, quarantined=?,"
        " status=? WHERE run_asof=?",
        (int(time.time()), run.requests_made, agg["rows_returned"],
         agg["rows_inserted"], len(run.names), agg["actions"], agg["quarantined"],
         status, run.run_asof),
    )
    con.commit()


def _run_over(
    con: sqlite3.Connection,
    vendor: YahooVendor,
    targets: Sequence[tuple[str, str]],
    span_for: Callable[[str], tuple[str, str] | None],
    run_asof: int,
    progress: Callable[[str], None] | None = None,
) -> RunResult:
    run = RunResult(run_asof=run_asof)
    _open_run(con, run_asof)
    for iid, symbol in targets:
        span = span_for(iid)
        if span is None:
            continue
        start, end = span
        try:
            series = vendor.fetch(symbol, start, end)
            run.requests_made += 1
        except VendorError as exc:
            run.names.append(NameResult(iid, symbol, "vendor_error",
                                        detail=str(exc)[:200]))
            con.execute(
                "INSERT INTO freshness (instrument_id, last_fetch_at, last_fetch_status)"
                " VALUES (?,?,?) ON CONFLICT(instrument_id) DO UPDATE SET"
                "  last_fetch_at=excluded.last_fetch_at,"
                "  last_fetch_status=excluded.last_fetch_status",
                (iid, int(time.time()), "vendor_error"),
            )
            con.commit()
            continue
        try:
            res = ingest_series(con, iid, series, run_asof)
            con.commit()
        except Exception:
            con.rollback()
            raise
        run.names.append(res)
        if progress:
            progress("{} {} rows={} ins={} {}".format(
                symbol, res.status, res.rows_returned, res.rows_inserted,
                res.detail[:80]))
    _close_run(con, run, "ok")
    return run


def nightly(
    con: sqlite3.Connection,
    vendor: YahooVendor,
    run_asof: int | None = None,
    today: str | None = None,
    progress: Callable[[str], None] | None = None,
) -> RunResult:
    """Append the sessions each name is missing. One request per name."""
    run_asof = run_asof or int(time.time())
    end = today or _today()
    floor = (dt.date.fromisoformat(end)
             - dt.timedelta(days=MAX_NIGHTLY_SPAN_DAYS)).isoformat()

    fresh = {
        r["instrument_id"]: r["last_date_held"]
        for r in con.execute("SELECT instrument_id, last_date_held FROM freshness")
    }

    def span_for(iid: str) -> tuple[str, str] | None:
        held = fresh.get(iid)
        # From the day after what we hold, to today -- a SPAN, not the next
        # single session. This is what closes a gap in one call instead of
        # crawling forward one day per night.
        start = max(held, floor) if held else floor
        if start > end:
            return None
        return (start, end)

    return _run_over(con, vendor, _targets(con), span_for, run_asof, progress)


def backfill(
    con: sqlite3.Connection,
    vendor: YahooVendor,
    since: str = DEFAULT_BACKFILL_START,
    today: str | None = None,
    limit: int | None = None,
    run_asof: int | None = None,
    progress: Callable[[str], None] | None = None,
) -> RunResult:
    """Full history for the whole universe. One-time, throttled by the vendor's
    pacing. Idempotent: re-running inserts nothing new and changes no facts."""
    run_asof = run_asof or int(time.time())
    end = today or _today()
    targets = _targets(con)
    if limit:
        targets = targets[:limit]
    return _run_over(con, vendor, targets, lambda _iid: (since, end), run_asof, progress)


def refetch(
    con: sqlite3.Connection,
    vendor: YahooVendor,
    n: int,
    since: str = DEFAULT_BACKFILL_START,
    today: str | None = None,
    run_asof: int | None = None,
    progress: Callable[[str], None] | None = None,
) -> RunResult:
    """The rotation sweep: the ``n`` names refetched longest ago, full history.

    Amended scope (A1/A2): this is VERIFICATION, not the corporate-action
    detector — the nightly's events block already catches a split on its
    effective date. What the sweep catches is vendor corruption of the MNST
    kind, back-dated revisions, and silent restatements, all of which surface
    as either a quarantine or a fail-loud fact change.
    """
    run_asof = run_asof or int(time.time())
    end = today or _today()
    order = {
        r["instrument_id"]: r["last_full_refetch_at"] or 0
        for r in con.execute(
            "SELECT instrument_id, last_full_refetch_at FROM freshness")
    }
    targets = sorted(_targets(con), key=lambda t: order.get(t[0], 0))[:n]
    run = _run_over(con, vendor, targets, lambda _iid: (since, end), run_asof, progress)
    con.executemany(
        "UPDATE freshness SET last_full_refetch_at=? WHERE instrument_id=?",
        [(run_asof, nm.instrument_id) for nm in run.names],
    )
    con.commit()
    return run


# -------------------------------------------------------------------- status --

@dataclass
class StatusReport:
    latest_session: str | None
    lagging: list[tuple[str, str, str | None]]  # (instrument_id, ticker, last_date)
    quarantined: list[tuple[str, str]]          # (instrument_id, date)
    provisional: list[str]
    # NOT the same thing as RunReport.fact_changes, which is a HELD value being
    # revised. These are vendor-corruption DETECTIONS: the vendor's own adjusted
    # series stepping by a ratio it declared, so its history sits on two scales.
    # No fact has changed. Reported as "fact-change events" until 2026-09-03,
    # which on a clean first backfill of Basilic read as seven revised facts when
    # the true count was zero -- and that line is what an operator reads nightly.
    vendor_corruptions: list[tuple[str, str]]

    @property
    def ok(self) -> bool:
        # An unadjudicated corruption keeps the store unclean, and so keeps the
        # nightly's exit code at 1, until a human rules on it.
        return not self.lagging and not self.vendor_corruptions

    def render(self) -> str:
        out = ["latest session held: {}".format(self.latest_session or "(none)")]
        out.append("names lagging >1 session: {}".format(len(self.lagging)))
        for iid, tk, last in self.lagging[:20]:
            out.append("   {:<10} {:<8} last={}".format(iid, tk, last or "never"))
        if len(self.lagging) > 20:
            out.append("   ... {} more".format(len(self.lagging) - 20))
        out.append("quarantined sessions: {}".format(len(self.quarantined)))
        out.append("provisional instruments (no CIK): {}".format(len(self.provisional)))
        out.append("vendor-corruption detections: {}".format(
            len(self.vendor_corruptions)))
        return "\n".join(out)


def status(con: sqlite3.Connection) -> StatusReport:
    """Freshness enforcement (2.5).

    A name lagging the latest held session by more than one is reported. No name
    is ever silently dropped from a downstream window because it is stale — that
    is the READER's decision, and this makes it visible so the reader can make
    it.
    """
    row = con.execute("SELECT MAX(last_date_held) FROM freshness").fetchone()
    latest = row[0] if row else None
    lagging: list[tuple[str, str, str | None]] = []
    if latest:
        # SESSIONS behind, not days. Over Thanksgiving a perfectly current name
        # is four calendar days stale and zero sessions behind; a day-counting
        # ledger pages somebody every holiday and is learned to be ignored.
        for r in con.execute(
            "SELECT f.instrument_id, f.last_date_held, i.primary_ticker AS ticker"
            " FROM freshness f JOIN instruments i USING (instrument_id)"
            " ORDER BY f.last_date_held"
        ):
            if sessions_behind(r["last_date_held"], latest) > 1:
                lagging.append(
                    (r["instrument_id"], r["ticker"] or "?", r["last_date_held"]))
    quarantined = [
        (r["instrument_id"], r["date"])
        for r in con.execute(
            "SELECT instrument_id, date FROM prices_raw WHERE status='quarantined'"
            " ORDER BY instrument_id, date")
    ]
    provisional = [
        r["instrument_id"]
        for r in con.execute(
            "SELECT instrument_id FROM instruments WHERE provisional=1 ORDER BY 1")
    ]
    vendor_corruptions = [
        (r["instrument_id"], r["effective_date"])
        for r in con.execute(
            "SELECT instrument_id, effective_date FROM adjustment_events"
            " WHERE kind='vendor_corruption' ORDER BY effective_date DESC LIMIT 100")
    ]
    return StatusReport(latest, lagging, quarantined, provisional,
                       vendor_corruptions)
