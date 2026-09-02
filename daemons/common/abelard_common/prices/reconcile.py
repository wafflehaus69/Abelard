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
    """
    a = con.execute(
        "SELECT close FROM prices_raw WHERE instrument_id=? AND date=? AND status='ok'",
        (instrument_id, date)).fetchone()
    b = con.execute(
        "SELECT close FROM prices_raw WHERE instrument_id=? AND date=? AND status='ok'",
        (instrument_id, prior)).fetchone()
    if not a or not b or not a[0] or not b[0] or b[0] <= 0:
        return None
    ratio = 1.0
    for r in con.execute(
        "SELECT ratio FROM corporate_actions WHERE instrument_id=? AND kind='split'"
        " AND effective_date > ? AND effective_date <= ?",
        (instrument_id, prior, date),
    ):
        if r[0]:
            ratio *= r[0]
    return (a[0] / b[0]) * ratio - 1.0


def _benchmark_return(con: sqlite3.Connection, benchmark: str,
                      date: str, prior: str) -> float | None:
    """The ETF's own price return, from whichever store holds it."""
    r = _price_return(con, benchmark, date, prior)
    if r is not None:
        return r
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
    return rows[date] / rows[prior] - 1.0


def latest_weight_asof(con: sqlite3.Connection, index_code: str,
                       on_or_before: str) -> str | None:
    row = con.execute(
        "SELECT MAX(as_of) FROM index_weights WHERE index_code=? AND as_of<=?",
        (index_code, on_or_before)).fetchone()
    return row[0] if row and row[0] else None


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
    w_asof = latest_weight_asof(con, index_code, date)
    if w_asof is None:
        return Reconciliation(date, index_code, benchmark, None, None, None, 0, 0,
                              0.0, tolerance_bp, "insufficient",
                              "no index_weights on or before {}".format(date))

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
