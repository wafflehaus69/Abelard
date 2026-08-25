"""Run orchestration: scan, enrich, and the lead view.

Window alignment [E13]: ``now_unix`` is computed ONCE here and passed down.
Nothing downstream calls ``time.time()``. Cost telemetry is opened before any
work and closed after, so a crash mid-run still leaves the record that the work
happened.
"""

from __future__ import annotations

import hashlib
import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path

from . import adv_pdf, archive, config, ledger, monthly_csv, normalize, transitions
from .errors import FduError, HaltRequested
from .feed import FirmRecord, feed_generated_on, parse_feed, parse_manifest
from .fetch import Fetcher


def _run_id(kind: str, now_unix: int) -> str:
    return hashlib.sha256(f"{kind}|{now_unix}".encode()).hexdigest()[:12]


@dataclass
class ScanResult:
    run_id: str
    snapshot_date: str
    firms_seen: int
    firms_added: int
    firms_changed: int
    firms_removed: int
    enrich_queued: int
    fetch_calls: int
    fetch_bytes: int

    def render(self) -> str:
        return (
            f"scan {self.run_id} snapshot={self.snapshot_date}\n"
            f"  firms seen     {self.firms_seen:>7,}\n"
            f"  added          {self.firms_added:>7,}\n"
            f"  changed        {self.firms_changed:>7,}\n"
            f"  removed        {self.firms_removed:>7,}\n"
            f"  enrich queued  {self.enrich_queued:>7,}\n"
            f"  fetched        {self.fetch_calls} calls, {self.fetch_bytes:,} bytes\n"
            f"  llm calls            0   cost $0.00"
        )


def _feed_cache_dir() -> Path:
    d = config.state_home() / "feeds"
    d.mkdir(parents=True, exist_ok=True)
    return d


def scan(conn: sqlite3.Connection, fetcher: Fetcher, *, kinds: tuple[str, ...] = ("FIRM_SEC",),
         keep_feed: bool = False) -> ScanResult:
    """Pull the current bulk snapshot, diff it against the ledger, record movement.

    The feed is a snapshot and the publisher keeps only ~8 days, so the change
    rows written here are the only history that will ever exist for this
    interval. That is why ``firm_change`` is append-only.
    """
    if config.halt_requested():
        raise HaltRequested("halt engaged; scan refused")

    now_unix = int(time.time())
    run_id = _run_id("scan", now_unix)
    ledger.start_run(conn, run_id, "scan", now_unix)

    manifest = parse_manifest(fetcher.get_json(config.COMPILATION_MANIFEST, surface="manifest"))

    records: list[FirmRecord] = []
    snapshot_date = ""
    cache = _feed_cache_dir()
    for kind in kinds:
        entry = manifest.get(kind)
        if entry is None:
            raise FduError(f"manifest carries no {kind} feed; have {sorted(manifest)}")
        dest = cache / entry["name"]
        if not dest.exists():
            fetcher.download_to(f"{config.COMPILATION_DIR}/{entry['name']}", dest, surface=f"feed_{kind}")
        records.extend(parse_feed(dest))
        snapshot_date = feed_generated_on(dest) or entry["date"]
        if not keep_feed:
            dest.unlink(missing_ok=True)

    previous = ledger.load_firms(conn)
    prev_snapshot = None
    row = conn.execute("SELECT snapshot_date FROM firm LIMIT 1").fetchone()
    if row:
        prev_snapshot = row["snapshot_date"]

    added = changed = queued = 0
    seen: set[str] = set()

    conn.execute("BEGIN")
    try:
        for rec in records:
            seen.add(rec.crd)
            key = normalize.change_key(rec)
            old = previous.get(rec.crd)
            if old is None:
                added += 1
                ledger.upsert_firm(conn, rec, change_key=key, now_unix=now_unix,
                                   snapshot_date=snapshot_date, changed=True)
                continue
            moved = normalize.diff_fields(old, rec)
            if moved:
                changed += 1
                ledger.record_changes(conn, rec.crd, moved, now_unix=now_unix,
                                      from_snapshot=prev_snapshot, to_snapshot=snapshot_date,
                                      run_id=run_id)
                if normalize.should_enrich(moved):
                    queued += 1
            ledger.upsert_firm(conn, rec, change_key=key, now_unix=now_unix,
                               snapshot_date=snapshot_date, changed=bool(moved))
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise

    removed = len(set(previous) - seen)

    tel = fetcher.telemetry
    result = ScanResult(run_id, snapshot_date, len(records), added, changed, removed,
                        queued, tel.calls, tel.bytes_down)
    ledger.finish_run(conn, run_id, firms_seen=len(records), firms_changed=changed,
                      firms_added=added, firms_removed=removed, snapshot_date=snapshot_date,
                      fetch_calls=tel.calls, fetch_bytes=tel.bytes_down, status="ok")
    return result


def _pending_enrich(conn: sqlite3.Connection, limit: int, *, backfill: bool) -> list[str]:
    if backfill:
        # Ascending AUM, and the bias is deliberate. A first cut ordered
        # largest-first, which front-loaded the mega-advisers: the slowest
        # documents in the corpus (up to 1,750 pages / 82 MB) and the least
        # relevant, since a consolidator is an acquirer rather than a
        # succession target. Small and mid RIAs are both the thesis population
        # and ~40x cheaper to fetch. Firms with no reported AUM (ERAs) come last.
        sql = (
            "SELECT f.crd FROM firm f LEFT JOIN adv_detail d ON d.crd = f.crd "
            "WHERE d.crd IS NULL "
            "ORDER BY CASE WHEN f.aum_total IS NULL THEN 1 ELSE 0 END, f.aum_total ASC "
            "LIMIT ?"
        )
        return [r["crd"] for r in conn.execute(sql, (limit,))]
    sql = (
        "SELECT DISTINCT c.crd FROM firm_change c "
        "LEFT JOIN adv_detail d ON d.crd = c.crd "
        "WHERE c.field IN (%s) AND (d.crd IS NULL OR d.fetched_unix < c.observed_unix) "
        "ORDER BY c.observed_unix DESC LIMIT ?"
    ) % ",".join("?" * len(normalize.ENRICH_TRIGGER_FIELDS))
    params = [*sorted(normalize.ENRICH_TRIGGER_FIELDS), limit]
    return [r["crd"] for r in conn.execute(sql, params)]


def enrich(conn: sqlite3.Connection, fetcher: Fetcher, *, limit: int | None = None,
           backfill: bool = False, delay: float | None = None) -> dict:
    """Pull and reduce ADV documents for firms whose watched fields moved.

    Documents are parsed in memory and discarded. Nothing about a named person
    is returned by ``adv_pdf`` and nothing about one is written here.
    """
    if config.halt_requested():
        raise HaltRequested("halt engaged; enrich refused")

    now_unix = int(time.time())
    run_id = _run_id("enrich", now_unix)
    ledger.start_run(conn, run_id, "backfill" if backfill else "enrich", now_unix)

    cap = limit or config.ADV_FETCH_DEFAULT_LIMIT
    pace = config.ADV_FETCH_DELAY_S if delay is None else delay
    targets = _pending_enrich(conn, cap, backfill=backfill)

    ok = failed = 0
    errors: list[str] = []
    for i, crd in enumerate(targets):
        if config.halt_requested():
            errors.append("halt engaged mid-run; stopped early")
            break
        try:
            facts = adv_pdf.fetch_and_extract(fetcher, crd)
        except FduError as exc:
            failed += 1
            errors.append(f"{crd}: {exc}")
            continue
        ledger.upsert_adv_detail(conn, facts.as_row(now_unix))
        ok += 1
        if i + 1 < len(targets):
            fetcher.pace(pace)

    tel = fetcher.telemetry
    ledger.finish_run(conn, run_id, docs_fetched=ok, fetch_calls=tel.calls,
                      fetch_bytes=tel.bytes_down, status="ok" if not failed else "partial",
                      note="; ".join(errors[:5]) or None)
    return {
        "run_id": run_id,
        "targets": len(targets),
        "extracted": ok,
        "failed": failed,
        "errors": errors,
        "fetch_calls": tel.calls,
        "fetch_bytes": tel.bytes_down,
    }


def monthly(conn: sqlite3.Connection, fetcher: Fetcher) -> dict:
    """Pull the SEC monthly bulk CSV and store the fields the daily feed lacks.

    Strictly additive. This does NOT drive change detection -- a month is far
    too coarse and the daily feed already does that -- it supplies Item 4
    successions and registration status detail that the XML feed simply does
    not carry.
    """
    if config.halt_requested():
        raise HaltRequested("halt engaged; monthly refused")

    now_unix = int(time.time())
    run_id = _run_id("monthly", now_unix)
    ledger.start_run(conn, run_id, "monthly", now_unix)

    rows = monthly_csv.fetch_monthly(fetcher)
    payload = []
    successions = self_successions = 0
    for r in rows:
        is_self = r.is_self_succession
        if r.has_succession:
            successions += 1
            if is_self:
                self_successions += 1
        payload.append({
            "crd": r.crd,
            "observed_unix": now_unix,
            "acquired_name": r.acquired_name,
            "acquired_sec_no": r.acquired_sec_no,
            "acquired_crd": r.acquired_crd,
            "acquired_count": r.acquired_count,
            "is_self_succession": None if is_self is None else int(is_self),
            "latest_filing": r.latest_filing,
            "sec_status": r.sec_status,
            "sec_status_date": r.sec_status_date,
            "relying_advisers": r.relying_advisers,
            "control_related": r.control_related,
            "common_control": r.common_control,
        })

    conn.execute("BEGIN")
    try:
        written = ledger.upsert_monthly(conn, payload)
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise

    tel = fetcher.telemetry
    ledger.finish_run(conn, run_id, firms_seen=written, fetch_calls=tel.calls,
                      fetch_bytes=tel.bytes_down, status="ok")
    return {
        "run_id": run_id,
        "rows": written,
        "successions": successions,
        "self_successions": self_successions,
        "third_party": successions - self_successions,
        "fetch_calls": tel.calls,
        "fetch_bytes": tel.bytes_down,
    }


def backfill_archive(conn: sqlite3.Connection, fetcher: Fetcher, *, limit: int | None = None,
                     delay: float = 1.0, preserve: bool = True) -> dict:
    """B1 + B2: ingest archive snapshots and diff adjacent pairs into events.

    Registered population only (ERAs OUT, ruling R1). Resumable: snapshots
    already recorded are skipped, so an interrupted run continues where it
    stopped. Diffing is chronological and gap-aware -- a pair spanning a missing
    month is flagged, never counted as a one-month move.
    """
    if config.halt_requested():
        raise HaltRequested("halt engaged; backfill refused")

    now_unix = int(time.time())
    run_id = _run_id("archive", now_unix)
    ledger.start_run(conn, run_id, "archive", now_unix)

    files, undatable = archive.census(fetcher)
    reg = archive.registered_only(files)
    gaps = set(archive.coverage_gaps(reg))
    done = ledger.ingested_files(conn)
    todo = [f for f in reg if f.filename not in done]
    if limit:
        todo = todo[:limit]

    prev = None
    ingested = failed = events_written = 0
    errors: list[str] = []

    # Re-establish the chronological predecessor if resuming mid-archive.
    for af in todo:
        if config.halt_requested():
            errors.append("halt engaged mid-run; stopped early")
            break
        try:
            payload = archive.fetch_archive_file(fetcher, af, preserve=preserve)
            snap = archive.parse_snapshot(af, payload)
        except FduError as exc:
            failed += 1
            errors.append(f"{af.filename}: {exc}")
            prev = None          # a hole: do not diff across a failed parse
            continue

        conn.execute("BEGIN")
        try:
            ledger.record_snapshot(conn, {
                "snapshot_date": snap.snapshot_date, "source_file": snap.source_file,
                "era": snap.era, "n_columns": snap.n_columns, "n_rows": snap.n_rows,
                "skipped_rows": snap.skipped,
                "absent_fields": ",".join(snap.absent_fields) or None,
                "ingested_unix": now_unix,
            })
            if prev is not None:
                evs = transitions.diff_snapshots(prev, snap)
                events_written += ledger.append_transitions(conn, [e.as_row() for e in evs])
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise

        prev = snap
        ingested += 1
        fetcher.pace(delay)

    tel = fetcher.telemetry
    ledger.finish_run(conn, run_id, firms_seen=ingested, fetch_calls=tel.calls,
                      fetch_bytes=tel.bytes_down,
                      status="ok" if not failed else "partial",
                      note="; ".join(errors[:5]) or None)
    return {
        "run_id": run_id, "archive_files": len(files), "registered": len(reg),
        "undatable": undatable, "coverage_gaps": len(gaps),
        "already_ingested": len(done), "attempted": len(todo),
        "ingested": ingested, "failed": failed, "events": events_written,
        "errors": errors, "fetch_calls": tel.calls, "fetch_bytes": tel.bytes_down,
    }
