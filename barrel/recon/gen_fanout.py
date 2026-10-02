"""Funder fan-out on a fixed window (MR-15 B3).

Fan-out is a property of the funder, not of the token, so it is measured on one window for every
token in a chunk: the full CALENDAR MONTH that contains the chunk (also for the two partial chunks,
2025-03 and 2026-09), and expressed locally as distinct recipients per day of that window.

The query has to be told which senders to report. That list is the funders in the chunk's own
exported aligned-set rows. The file in the repo carries the placeholder __FUNDERS__; the runner
substitutes the addresses at run time (--funders-from), so they reach the query text sent to
Dune and nothing else, exactly as the owner-wallet filter does.

No line between "hub" and "dedicated" is in the query. The rate is returned; the line is set on
the calibration slice in v1.2 and applied in recon/actors.py.
"""
import calendar
import datetime as dt
import pathlib

ROOT = pathlib.Path(__file__).resolve().parents[1]


def month_window(d0: dt.date) -> tuple[dt.date, dt.date, int]:
    first = d0.replace(day=1)
    days = calendar.monthrange(d0.year, d0.month)[1]
    return first, first + dt.timedelta(days=days), days


def build(t0: dt.date, t1: dt.date, days: int, what: str) -> str:
    return f"""-- Funder fan-out, {what}: SOL transfers {t0} .. {t1} (exclusive), {days} day(s).
-- Rows name wallets: run with --private-rows or --export. The senders reported are the chunk's own
-- funders, substituted at run time (--funders-from). No threshold, no class, no verdict here.
SELECT s.from_owner AS funder,
       approx_distinct(s.to_owner) AS recipients,
       count(*) AS n_transfers,
       count(DISTINCT CAST(s.block_time AS date)) AS active_days,
       {days} AS window_days
FROM tokens_solana.sol_transfers s
WHERE s.block_time >= TIMESTAMP '{t0} 00:00:00' AND s.block_time < TIMESTAMP '{t1} 00:00:00'
  AND CAST(s.amount AS double) >= 1e6
  AND s.to_owner <> s.from_owner
  AND s.from_owner IN (__FUNDERS__)
GROUP BY 1"""


def chunk(d0: str, d1: str) -> str:
    t0, t1, days = month_window(dt.date.fromisoformat(d0))
    return build(t0, t1, days, f"calendar month of the chunk {d0} .. {d1}")


def one_day(day: str) -> str:
    d = dt.date.fromisoformat(day)
    return build(d, d + dt.timedelta(days=1), 1, f"one-day proving run {day}")


if __name__ == "__main__":
    out = ROOT / "recon" / "sql"
    (out / "fanout_day_2026-09-01.sql").write_text(one_day("2026-09-01"), encoding="utf-8")
    print("written: fanout_day_2026-09-01.sql")
