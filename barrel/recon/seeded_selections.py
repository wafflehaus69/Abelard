"""Seeded selections recorded in docs/RUNBOOK_ARCHITECT_2026-10-01.md (MR-13). No network, no credits.
python barrel/recon/seeded_selections.py"""
import datetime as dt
import hashlib

SEED = "20261001"
BOOST = dt.date(2026, 7, 21)
CALIBRATION_END = dt.date(2025, 7, 10)
WINDOW_END = dt.date(2026, 9, 20)


def h(key: str) -> str:
    return hashlib.sha256(f"{SEED}|{key}".encode()).hexdigest()


def burned_week() -> dt.date:
    weeks = [dt.date(2026, 7, 27) + dt.timedelta(days=7 * i) for i in range(8)]
    return min(weeks, key=lambda w: h(w.isoformat()))


def c1_block(era: str, first: dt.date, last_start: dt.date) -> tuple[dt.date, dt.date]:
    n = (last_start - first).days + 1
    start = first + dt.timedelta(days=int(h(f"C1|{era}"), 16) % n)
    return start, start + dt.timedelta(days=89)


if __name__ == "__main__":
    w = burned_week()
    print("burned week:", w, "->", w + dt.timedelta(days=6))
    print("C1 pre_boost block:", *c1_block("pre_boost", CALIBRATION_END + dt.timedelta(days=1), BOOST - dt.timedelta(days=90)))
    print("C1 post_boost: whole era", BOOST, "->", WINDOW_END, "less the burned week")
