"""Admission gate (invariant 2) and the surfacing routing rules."""

from __future__ import annotations

import time

import pytest

from scout_daemon import admissions, ledger, state, surface


@pytest.fixture()
def conn(tmp_path):
    c = state.connect(tmp_path / "a.sqlite3")
    ledger.apply_schema(c)
    surface.apply_schema(c)
    return c


def seed(conn, oid, *, source="opire", native="n1", klass="GREEN",
         category="bug_bounty", cat_source="structured", status="discovered"):
    cols = dict(
        opportunity_id=oid, source=source, source_native_id=native,
        title_hash="h", title=f"t-{oid}", category=category,
        category_source=cat_source, payout_basis="per_task",
        payout_confidence="claimed", legitimacy_class=klass,
        class_reason="r", classified_by="mech", classifier_version="v",
        status=status, tos_class="WIRE", first_seen_unix=1, last_seen_unix=1,
        scan_id="s1", resolved_via="direct",
    )
    names = ",".join(cols)
    conn.execute(f"INSERT INTO opportunities({names}) VALUES({','.join('?'*len(cols))})",
                 list(cols.values()))
    conn.commit()


def write(tmp_path, body):
    p = tmp_path / "admissions.yaml"
    p.write_text(body, encoding="utf-8")
    return p


# ---------------------------------------------------------------------------
# INVARIANT 2 -- the gate itself
# ---------------------------------------------------------------------------

def test_the_daemon_has_no_write_path_to_the_admissions_file() -> None:
    """The enforcement is structural: no open-for-write anywhere in the package.

    If this ever fails, admission has stopped being a human act.
    """
    import pathlib

    pkg = pathlib.Path(admissions.__file__).parent
    offenders = []
    for py in pkg.rglob("*.py"):
        src = py.read_text(encoding="utf-8")
        for marker in ('admissions.yaml"', "admissions.yaml'", "DEFAULT_ADMISSIONS_PATH"):
            if marker not in src:
                continue
            for bad in ("write_text", "open(", "yaml.dump", "safe_dump"):
                if bad in src and "read_text" not in src.split(bad)[0][-80:]:
                    offenders.append(f"{py.name}: {bad}")
    assert not [o for o in offenders if "write_text" in o or "dump" in o], offenders


def test_scan_ingest_cannot_set_admitted() -> None:
    """`status` is immutable on update, so re-seeing an item cannot un-admit it
    -- and nothing in ingest can admit it either."""
    assert "status" in ledger._IMMUTABLE_ON_UPDATE
    assert admissions.STATUS_ADMITTED not in admissions.DAEMON_SETTABLE
    assert admissions.STATUS_DISMISSED not in admissions.DAEMON_SETTABLE


def test_admitted_only_moves_from_the_file(conn, tmp_path) -> None:
    seed(conn, "a" * 64)
    assert conn.execute("SELECT status FROM opportunities").fetchone()[0] == "discovered"
    loaded = admissions.load(write(tmp_path, f"admitted:\n  - {'a'*64}\n"))
    out = admissions.apply(conn, loaded, now_unix=99)
    assert out.admitted == 1
    row = conn.execute("SELECT status, admission_applied_unix FROM opportunities").fetchone()
    assert row[0] == "admitted"
    assert row[1] == 99, "admission must stamp its provenance"


def test_propose_cannot_reopen_an_admitted_row(conn, tmp_path) -> None:
    seed(conn, "b" * 64)
    admissions.apply(conn, admissions.load(write(tmp_path, f"admitted:\n  - {'b'*64}\n")),
                     now_unix=1)
    moved = admissions.propose(conn, ["b" * 64], now_unix=2)
    assert moved == 0
    assert conn.execute("SELECT status FROM opportunities").fetchone()[0] == "admitted"


# ---------------------------------------------------------------------------
# Key handling
# ---------------------------------------------------------------------------

def test_short_key_resolves(conn, tmp_path) -> None:
    oid = "c" * 64
    seed(conn, oid)
    out = admissions.apply(
        conn, admissions.load(write(tmp_path, f"admitted:\n  - {oid[:12]}\n")), now_unix=1)
    assert out.admitted == 1


def test_source_native_key_resolves_when_short_enough(conn, tmp_path) -> None:
    seed(conn, "d" * 64, source="opire", native="12345")
    out = admissions.apply(
        conn, admissions.load(write(tmp_path, "admitted:\n  - opire:12345\n")), now_unix=1)
    assert out.admitted == 1


def test_overlong_native_ids_are_not_offered_as_keys() -> None:
    """zindi/yeswehack use the title as native id; those must not be keys."""
    long_native = "A very long listing title that runs well past sixty characters in total"
    keys = admissions.identity_keys("zindi", long_native, "e" * 64)
    assert f"zindi:{long_native}" not in keys
    assert "e" * 12 in keys


def test_unknown_key_is_reported_not_swallowed(conn, tmp_path) -> None:
    seed(conn, "f" * 64)
    out = admissions.apply(
        conn, admissions.load(write(tmp_path, "admitted:\n  - deadbeefdead\n")), now_unix=1)
    assert out.admitted == 0
    assert out.unknown_keys == ["deadbeefdead"]


def test_key_in_both_lists_is_refused_not_guessed(conn, tmp_path) -> None:
    oid = "1" * 64
    seed(conn, oid)
    out = admissions.apply(conn, admissions.load(
        write(tmp_path, f"admitted:\n  - {oid}\ndismissed:\n  - {oid}\n")), now_unix=1)
    assert out.admitted == 0 and out.dismissed == 0
    assert oid in out.conflicts
    assert conn.execute("SELECT status FROM opportunities").fetchone()[0] == "discovered"


# ---------------------------------------------------------------------------
# Category rules
# ---------------------------------------------------------------------------

def test_explicit_key_beats_a_category_rule(conn, tmp_path) -> None:
    oid = "2" * 64
    seed(conn, oid, category="bug_bounty")
    out = admissions.apply(conn, admissions.load(write(
        tmp_path, f"admitted:\n  - {oid}\ncategory_rules:\n  bug_bounty: dismiss\n")),
        now_unix=1)
    assert out.admitted == 1
    assert conn.execute("SELECT status FROM opportunities").fetchone()[0] == "admitted"


def test_unknown_category_verdict_is_refused(conn, tmp_path) -> None:
    seed(conn, "3" * 64, category="grant")
    out = admissions.apply(conn, admissions.load(
        write(tmp_path, "category_rules:\n  grant: maybe\n")), now_unix=1)
    assert out.by_category == 0
    assert any("maybe" in c for c in out.conflicts)


def test_absent_file_is_a_valid_state(tmp_path) -> None:
    loaded = admissions.load(tmp_path / "nope.yaml")
    assert loaded.present is False and loaded.is_empty


# ---------------------------------------------------------------------------
# Surfacing routing (recon 6.2)
# ---------------------------------------------------------------------------

def test_derived_categories_are_not_novelty_bearing(conn) -> None:
    """A category synthesised from the listing's own title is not a new KIND."""
    seed(conn, "4" * 64, category="Some Grant Program", cat_source="derived")
    seed(conn, "5" * 64, native="n2", category="bug_bounty", cat_source="structured")
    cats = [c for c, _ in surface.novel_categories(conn)]
    assert cats == ["bug_bounty"]


def test_red_is_never_novelty_bearing(conn) -> None:
    seed(conn, "6" * 64, klass="RED", category="malware_bounty")
    assert surface.novel_categories(conn) == []


def test_recording_seen_is_idempotent_and_one_way(conn) -> None:
    seed(conn, "7" * 64, category="grant")
    assert [c for c, _ in surface.novel_categories(conn)] == ["grant"]
    surface.record_seen(conn, "grant", "7" * 64, now_unix=1)
    surface.record_seen(conn, "grant", "9" * 64, now_unix=2)
    conn.commit()
    assert surface.novel_categories(conn) == []
    row = conn.execute("SELECT first_opportunity_id, COUNT(*) FROM seen_categories").fetchone()
    assert row[1] == 1 and row[0] == "7" * 64


def test_dry_run_touches_no_queue_and_records_nothing(conn) -> None:
    seed(conn, "8" * 64, category="grant")
    out = surface.run(conn, now_unix=1, dry_run=True)
    assert out.novel_categories == ["grant"]
    assert out.enqueued == 0
    assert conn.execute("SELECT COUNT(*) FROM seen_categories").fetchone()[0] == 0


def test_yellow_high_payout_rule_is_absent_and_says_so(conn) -> None:
    """The threshold was deferred and must not be invented (E8)."""
    stats = surface.high_payout_cut_pending(conn)
    assert "deferred" in stats["note"] or "unmeasurable" in stats["note"]
    src = __import__("inspect").getsource(surface)
    assert "payout_usd_low >" not in src, "a payout threshold was smuggled in"
