"""PS-1 — index-level reconciliation. The systemic-failure check.

Every other detector in this substrate is **per name**: a split not applied, a
step that survives, a fact that changed. All of them are blind to the failure
that matters most operationally — *many names wrong at once*. A vendor that
serves a whole slice of the universe stale, or shifted by a session, or on a
different adjustment vintage, produces no per-name anomaly at all. Each series
looks internally consistent. Only the aggregate moves.

So: rebuild the cap-weighted index return from the constituents we hold, using
the ETF's own published weights, and compare it against the ETF's actual return.
If our panel is healthy the two agree to a basis point or two; if a slice of it
is stale or mangled, the rebuilt return drifts and the check fails loudly.

The weights are already in the iShares holdings file we parse for membership, so
this costs one extra series (the ETF itself) and no extra sources.

**What the tolerance means.** ~10 bp is not a claim about tracking error. Weights
are as-of the file's date and drift intraday; a handful of members are always
missing; the ETF's own NAV return differs slightly from its price return. The
band is set so that ordinary drift passes and a *systemic* break — dozens of
names stale, an off-by-one session, a vintage shift — cannot.
"""

from __future__ import annotations

import sqlite3
import time
from dataclasses import dataclass, field
from typing import Sequence

DEFAULT_TOLERANCE_BP = 10.0

# Below this, the rebuilt return is not a meaningful comparison and the check
# reports INSUFFICIENT rather than a pass -- an empty panel must never look like
# a clean bill of health (E1: never empty success).
MIN_WEIGHT_COVERAGE = 0.80


@dataclass
class Reconciliation:
    as_of: str
    index_code: str
    benchmark: str
    rebuilt_return: float | None
    actual_return: float | None
    diff_bp: float | None
    members_used: int
    members_missing: int
    weight_covered: float
    tolerance_bp: float
    status: str          # pass | fail | insufficient
    detail: str = ""
    missing: list[str] = field(default_factory=list)

    @property
    def passed(self) -> bool:
        return self.status == "pass"

    def render(self) -> str:
        if self.rebuilt_return is None or self.actual_return is None:
            return "[reconcile] {} {} {}: {}".format(
                self.as_of, self.index_code, self.status.upper(), self.detail)
        return (
            "[reconcile] {} {} vs {} {}: rebuilt {:+.4f}% actual {:+.4f}% "
            "diff {:+.1f}bp (tol {:.0f}) members {}/{} weight {:.1%}{}".format(
                self.as_of, self.index_code, self.benchmark, self.status.upper(),
                100 * self.rebuilt_return, 100 * self.actual_return, self.diff_bp,
                self.tolerance_bp, self.members_used,
                self.members_used + self.members_missing, self.weight_covered,
                "" if self.passed else "  <-- " + self.detail)
        )


def _adj(con: sqlite3.Connection, instrument_id: str, date: str) -> float | None:
    row = con.execute(
        "SELECT adj_close FROM adjusted_view WHERE instrument_id=? AND date=?",
        (instrument_id, date)).fetchone()
    return row[0] if row else None


def effective_close(con: sqlite3.Connection, instrument_id: str,
                    date: str) -> float | None:
    """The raw close this session is actually known by, or None.

    ``prices_raw`` is insert-only, so a session the primary returned with no
    price keeps its ``vendor_null`` row forever — that row is the record that
    the primary had nothing, and it must stay. G3 fills such a hole from a
    second sourced vendor, but because the slot is already occupied the fill
    cannot be written into ``prices_raw`` and lands in ``fills`` instead. The
    comment at that insert calls it "an overlay the view honours".

    NOTHING HONOURED IT. ``fills`` was written and never read by any query in
    the package, so a filled hole stayed invisible and the substrate could not
    heal. On 2026-09-19 that left 5,688 sessions across 2026-09-03..18 looking
    permanently empty while the vendor served every one of them on request, and
    the nightly SPX reconciliation reported 0.0% coverage for sixteen straight
    days. This function is the honouring.

    Precedence, strongest evidence first:
      1. ``prices_raw`` with status ``ok`` or ``filled`` — what a vendor said
         about the session directly, a fill that found an empty slot included.
      2. ``fills`` — the attributed overlay for a slot a ``vendor_null`` holds.

    A ``quarantined`` row is deliberately NOT a source: it is a session under
    suspicion, and resolving it is a human's job, not a fallback's.
    """
    row = con.execute(
        "SELECT close FROM prices_raw WHERE instrument_id=? AND date=?"
        " AND status IN ('ok','filled') AND close IS NOT NULL",
        (instrument_id, date)).fetchone()
    if row and row[0]:
        return row[0]
    row = con.execute(
        "SELECT filled_close FROM fills WHERE instrument_id=? AND date=?"
        " AND filled_close IS NOT NULL",
        (instrument_id, date)).fetchone()
    return row[0] if row and row[0] else None


def _price_return(con: sqlite3.Connection, instrument_id: str,
                  date: str, prior: str) -> float | None:
    """A PRICE return for one session — not a total return.

    **This distinction is the whole accuracy of the check.** The benchmark is an
    ETF *price* return; ``adjusted_view`` is a *total* return. Comparing them
    bakes in the day's dividend wedge, and on a heavy ex-dividend session that
    wedge is larger than the entire tolerance: measured 2026-09-01, 17 S&P names
    went ex-div and the two rebuilds differed by **12.8 bp** against a 10 bp
    band. Like-for-like the same session reconciles at **-2.6 bp**.

    So the rebuild runs on ``prices_raw`` — true traded prices — with any split
    falling inside the window divided back out, since a raw series steps at a
    split by construction.

    Reads the EFFECTIVE close (see ``effective_close``), not ``status='ok'``
    alone: a filled hole is a real traded price with attribution, and ignoring
    it made a transient vendor gap look like a permanent one.
    """
    a = effective_close(con, instrument_id, date)
    b = effective_close(con, instrument_id, prior)
    if not a or not b or b <= 0:
        return None
    ratio = 1.0
    for r in con.execute(
        "SELECT ratio FROM corporate_actions WHERE instrument_id=? AND kind='split'"
        " AND effective_date > ? AND effective_date <= ?",
        (instrument_id, prior, date),
    ):
        if r[0]:
            ratio *= r[0]
    return (a / b) * ratio - 1.0


def _benchmark_distribution(con: sqlite3.Connection, benchmark: str,
                            date: str, prior: str) -> float:
    """Per-share cash the benchmark ITSELF paid with an ex-date in (prior, date].

    Read from wherever the benchmark lives: ``corporate_actions`` when it is a
    held instrument, ``reference_dividends`` when it is a reference series. One
    amount per ex-date, so a payout declared by two sources is not counted twice.
    """
    per_date: dict[str, float] = {}
    for d, amount in con.execute(
        "SELECT effective_date, amount FROM corporate_actions WHERE instrument_id=?"
        " AND kind='dividend' AND amount IS NOT NULL"
        " AND effective_date > ? AND effective_date <= ?",
        (benchmark, prior, date),
    ):
        per_date[d] = max(per_date.get(d, 0.0), amount)
    for d, amount in con.execute(
        "SELECT ex_date, amount FROM reference_dividends WHERE series_id=?"
        " AND ex_date > ? AND ex_date <= ?",
        (benchmark, prior, date),
    ):
        per_date[d] = max(per_date.get(d, 0.0), amount)
    return sum(per_date.values())


def _benchmark_return(con: sqlite3.Connection, benchmark: str,
                      date: str, prior: str) -> float | None:
    """The ETF's return with its OWN distribution added back on its ex-date.

    The rebuild is a constituent PRICE return, and on an ordinary day the ETF's
    price return matches it. On the ETF's own ex-dividend date it does not: the
    fund's price drops by the quarterly payout while no constituent does. On
    2026-09-15 IVV paid $2.203; its price return was -0.7325% against -0.4495%
    for the S&P 500 itself, and a panel that matched the index to 0.8bp failed
    the check at +27.5bp -- a guaranteed false failure four times a year. So the
    payout is added back, and only the benchmark's: constituents stay price
    returns, exactly as ``_price_return`` requires.
    """
    dist = _benchmark_distribution(con, benchmark, date, prior)
    r = _price_return(con, benchmark, date, prior)
    if r is not None:
        base = effective_close(con, benchmark, prior)
        return r + (dist / base if dist and base else 0.0)
    rows = {}
    for d in (date, prior):
        row = con.execute(
            "SELECT value FROM reference_series WHERE series_id=? AND date=?"
            " AND value IS NOT NULL ORDER BY fetched_at DESC LIMIT 1",
            (benchmark, d)).fetchone()
        if row:
            rows[d] = row[0]
    if len(rows) < 2 or not rows[prior]:
        return None
    return (rows[date] + dist) / rows[prior] - 1.0


def latest_weight_asof(con: sqlite3.Connection, index_code: str,
                       on_or_before: str) -> str | None:
    row = con.execute(
        "SELECT MAX(as_of) FROM index_weights WHERE index_code=? AND as_of<=?",
        (index_code, on_or_before)).fetchone()
    return row[0] if row and row[0] else None


def default_target(con: sqlite3.Connection,
                   now_epoch: float | None = None) -> tuple[str | None, str]:
    """The session the nightly reconciles, plus a note when it is not the newest
    session any name holds.

    It used to be ``MAX(last_date_held)`` and nothing else. That aggregate is
    set by the single FASTEST name: on every night of the 2026-09 outage one
    name (HUBB) had today's close at 21:00 while the other 517 did not, so the
    target was a session almost nobody held, and the check reported
    INSUFFICIENT every night about a session that was merely still arriving.

    The writer no longer records a not-yet-delivered session at all, so "most
    names absent from today" is now the normal, honest state at 21:00, and must
    not read as a failure. When the newest held session is one the vendor has
    not had a full day to settle, it is reported UNSETTLED and the previous
    session, which has, is reconciled as the session of record. The check still
    runs every night; it is one session behind, which is the true price of not
    believing an unfinished answer. A PAST session is never excused: once a
    session is settled, missing weight is INSUFFICIENT, loudly, exactly as
    before.
    """
    from .calendar import is_vendor_settled, previous_session
    row = con.execute("SELECT MAX(last_date_held) FROM freshness").fetchone()
    newest = row[0] if row and row[0] else None
    if newest is None:
        return None, ""
    if is_vendor_settled(newest, now_epoch):
        return newest, ""
    prior = previous_session(newest)
    return prior, (
        "{} UNSETTLED: the vendor has not had a full day to deliver it; "
        "reconciling {} as the session of record".format(newest, prior))


def reconcile_session(
    con: sqlite3.Connection,
    date: str,
    prior: str,
    index_code: str = "SPX",
    benchmark: str = "IVV",
    tolerance_bp: float = DEFAULT_TOLERANCE_BP,
) -> Reconciliation:
    """Rebuild ``index_code``'s return for ``date`` and compare to ``benchmark``.

    ``prior`` must be the previous TRADING session, not the previous day — the
    caller gets it from ``calendar.previous_session`` so a holiday does not
    silently become a two-day return on one side of the comparison.
    """
    # The weights in force at the PRIOR close. The session's return, close to
    # close, is earned by the basket held going into it; a holdings file dated
    # the session itself describes the basket AFTER that day's trading, which on
    # a rebalance day is a different basket. 2026-09-18, a rebalance Friday:
    # +9.3bp against IVV with the same-day file, +6.6bp with the pre-rebalance
    # one. Off-by-one, and it only shows on the days the basket changes.
    w_asof = latest_weight_asof(con, index_code, prior)
    if w_asof is None:
        return Reconciliation(date, index_code, benchmark, None, None, None, 0, 0,
                              0.0, tolerance_bp, "insufficient",
                              "no index_weights on or before {}".format(prior))

    weights = list(con.execute(
        "SELECT instrument_id, weight FROM index_weights"
        " WHERE index_code=? AND as_of=? AND weight IS NOT NULL",
        (index_code, w_asof)))
    total_w = sum(r["weight"] for r in weights)
    if total_w <= 0:
        return Reconciliation(date, index_code, benchmark, None, None, None, 0, 0,
                              0.0, tolerance_bp, "insufficient", "weights sum to zero")

    used_w = 0.0
    weighted = 0.0
    used = 0
    missing: list[str] = []
    for r in weights:
        iid, w = r["instrument_id"], r["weight"]
        ret = _price_return(con, iid, date, prior)
        if ret is None:
            missing.append(iid)
            continue
        weighted += w * ret
        used_w += w
        used += 1

    covered = used_w / total_w
    if covered < MIN_WEIGHT_COVERAGE:
        return Reconciliation(
            date, index_code, benchmark, None, None, None, used, len(missing),
            covered, tolerance_bp, "insufficient",
            "only {:.1%} of index weight has both sessions".format(covered),
            missing[:50])

    # Renormalise over what we actually hold: the question is whether the names
    # we have moved as the index did, not whether we hold all of them. Coverage
    # is reported separately so a shrinking panel is visible on its own.
    rebuilt = weighted / used_w

    actual = _benchmark_return(con, benchmark, date, prior)
    if actual is None:
        return Reconciliation(
            date, index_code, benchmark, rebuilt, None, None, used, len(missing),
            covered, tolerance_bp, "insufficient",
            "benchmark {} has no pair of sessions".format(benchmark), missing[:50])
    diff_bp = (rebuilt - actual) * 10_000
    ok = abs(diff_bp) <= tolerance_bp
    return Reconciliation(
        date, index_code, benchmark, rebuilt, actual, diff_bp, used, len(missing),
        covered, tolerance_bp, "pass" if ok else "fail",
        "" if ok else "rebuilt index return diverges from {} by {:.1f}bp — a "
                      "systemic vendor failure looks exactly like this".format(
                          benchmark, diff_bp),
        missing[:50])


def record(con: sqlite3.Connection, rec: Reconciliation, run_asof: int) -> None:
    con.execute(
        "INSERT OR REPLACE INTO reconciliation (as_of, index_code, benchmark,"
        " rebuilt_return, actual_return, diff_bp, members_used, members_missing,"
        " tolerance_bp, passed, detail, run_asof)"
        " VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
        (rec.as_of, rec.index_code, rec.benchmark, rec.rebuilt_return,
         rec.actual_return, rec.diff_bp, rec.members_used, rec.members_missing,
         rec.tolerance_bp, 1 if rec.passed else 0,
         rec.detail or rec.status, run_asof))
    con.commit()


def run(
    con: sqlite3.Connection,
    date: str,
    prior: str,
    run_asof: int | None = None,
    pairs: Sequence[tuple[str, str]] = (("SPX", "IVV"),),
    tolerance_bp: float = DEFAULT_TOLERANCE_BP,
) -> list[Reconciliation]:
    run_asof = run_asof or int(time.time())
    out = []
    for index_code, benchmark in pairs:
        rec = reconcile_session(con, date, prior, index_code, benchmark, tolerance_bp)
        record(con, rec, run_asof)
        out.append(rec)
    return out
