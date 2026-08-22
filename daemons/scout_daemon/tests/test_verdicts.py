"""Verdict history and the E21 effective-verdict derivation."""

from __future__ import annotations

import pytest

from scout_daemon import ledger, state, verdicts
from scout_daemon.verdicts import RECOVERY_SCANS, VETO_SCANS, derive


def obs(*pairs):
    """(disagreed, persona) pairs -> observation tuples, oldest first."""
    return [(1000 + i, d, p) for i, (d, p) in enumerate(pairs)]


# ---------------------------------------------------------------------------
# The asymmetry itself
# ---------------------------------------------------------------------------

def test_veto_takes_effect_on_a_single_observation() -> None:
    """Downward is fast -- this is the cheap-error bias, not an accident."""
    assert VETO_SCANS == 1
    assert derive(obs((False, False), (True, False))).vetoed is True


def test_recovery_needs_two_consecutive_clean_scans() -> None:
    assert RECOVERY_SCANS == 2
    one_clean = derive(obs((True, False), (False, False)))
    assert one_clean.vetoed is True, "one clean scan must NOT clear a veto"
    two_clean = derive(obs((True, False), (False, False), (False, False)))
    assert two_clean.vetoed is False


def test_the_clean_run_must_be_consecutive() -> None:
    """clean, veto, clean is not two clean scans."""
    v = derive(obs((True, False), (False, False), (True, False), (False, False)))
    assert v.vetoed is True


def test_first_observation_enters_at_its_raw_verdict() -> None:
    """A brand-new clean row is GREEN immediately -- it does not serve a
    recovery period it never earned."""
    assert derive(obs((False, False))).vetoed is False
    assert derive(obs((True, False))).vetoed is True


def test_empty_history_is_not_vetoed() -> None:
    v = derive([])
    assert v.vetoed is False and v.scans_seen == 0


# ---------------------------------------------------------------------------
# Persona vetoes are exempt -- permanent and downward-only
# ---------------------------------------------------------------------------

def test_persona_veto_never_recovers() -> None:
    """No number of clean scans clears a persona veto. Debouncing a permanent
    gate would let two lucky scans unlock something ruled unlockable."""
    v = derive(obs((True, True), (False, False), (False, False), (False, False)))
    assert v.vetoed is True
    assert v.persona_locked is True


def test_persona_lock_survives_an_arbitrarily_long_clean_run() -> None:
    history = [(1, True, True)] + [(i + 2, False, False) for i in range(50)]
    assert derive(history).vetoed is True


def test_ordinary_veto_does_recover_unlike_persona() -> None:
    ordinary = derive(obs((True, False), (False, False), (False, False)))
    persona = derive(obs((True, True), (False, False), (False, False)))
    assert ordinary.vetoed is False and persona.vetoed is True


# ---------------------------------------------------------------------------
# Auditability
# ---------------------------------------------------------------------------

def test_debounce_held_flags_divergence_from_the_newest_raw_verdict() -> None:
    v = derive(obs((True, False), (False, False)))
    assert v.raw_latest_vetoed is False and v.vetoed is True
    assert v.debounce_held is True, "a held row must be visibly held"


def test_debounce_not_held_when_effective_matches_raw() -> None:
    assert derive(obs((False, False), (False, False))).debounce_held is False


def test_flip_count_counts_effective_transitions_only() -> None:
    """Raw churn that the debounce absorbs is not a flip of the effective state."""
    v = derive(obs((False, False), (True, False), (False, False)))
    assert v.flip_count == 1  # GREEN -> VETOED; the single clean scan did not clear it


# ---------------------------------------------------------------------------
# The table is append-only
# ---------------------------------------------------------------------------

@pytest.fixture()
def conn(tmp_path):
    # state.connect() creates schema_meta, which ledger.apply_schema() writes
    # to -- same construction order the other suites use.
    c = state.connect(tmp_path / "v.sqlite3")
    ledger.apply_schema(c)
    return c


def test_recording_the_same_scan_twice_cannot_rewrite_history(conn) -> None:
    for reason in ("first verdict", "REWRITTEN"):
        verdicts.record_verdict(
            conn, opportunity_id="x", scan_id="s1", observed_unix=1,
            mechanical_class="GREEN", legitimacy_class="GREEN",
            classes_disagreed=False, class_reason=reason,
        )
    conn.commit()
    rows = conn.execute("SELECT class_reason FROM opportunity_verdicts").fetchall()
    assert len(rows) == 1
    assert rows[0]["class_reason"] == "first verdict", "history must be immutable"


def test_schema_creates_the_verdict_table(conn) -> None:
    names = {r[0] for r in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'")}
    assert "opportunity_verdicts" in names


def test_effective_for_reads_history_in_time_order(conn) -> None:
    # Inserted out of order on purpose; derivation must sort by observed_unix.
    for sid, t, dis in (("s3", 300, False), ("s1", 100, True), ("s2", 200, False)):
        verdicts.record_verdict(
            conn, opportunity_id="x", scan_id=sid, observed_unix=t,
            mechanical_class="GREEN", legitimacy_class="GREEN",
            classes_disagreed=dis, class_reason=None,
        )
    conn.commit()
    v = verdicts.effective_for(conn, "x")
    assert v.scans_seen == 3
    assert v.vetoed is False  # veto at t=100, then two clean scans


# ---------------------------------------------------------------------------
# Invariant 4 / invariant 2: ranking and derivation never admit
# ---------------------------------------------------------------------------

def test_no_admission_path_in_the_verdict_module() -> None:
    """Same guard the GREEN invariant carries: nothing here writes `status`.

    Inspects executable string literals only -- a docstring that PROMISES the
    module never writes `admitted` must not itself trip the check.
    """
    import ast
    import inspect

    tree = ast.parse(inspect.getsource(verdicts))
    docs = {ast.get_docstring(n, clean=False) for n in ast.walk(tree)
            if isinstance(n, (ast.Module, ast.FunctionDef, ast.ClassDef))}
    literals = [n.value for n in ast.walk(tree)
                if isinstance(n, ast.Constant) and isinstance(n.value, str)
                and n.value not in docs]
    assert not [s for s in literals if "admitted" in s]
    assert not [s for s in literals if "status" in s.lower() and "UPDATE" in s]


# ---------------------------------------------------------------------------
# Judgeless scans must not count as clean votes
# ---------------------------------------------------------------------------

def _cost(conn, scan_id, *, calls, items):
    ledger.record_cost(
        conn, scan_id=scan_id, model="m", llm_calls=calls, input_tokens=0,
        output_tokens=0, cache_read_tokens=0, cache_creation_tokens=0,
        cost_usd=0.0, items_classified=items,
    )


def _obs(conn, oid, scan_id, t, disagreed):
    verdicts.record_verdict(
        conn, opportunity_id=oid, scan_id=scan_id, observed_unix=t,
        mechanical_class="GREEN", legitimacy_class="GREEN",
        classes_disagreed=disagreed, class_reason=None,
    )


def test_judgeless_scans_are_identified_from_cost_telemetry(conn) -> None:
    _cost(conn, "live", calls=1, items=10)
    _cost(conn, "dead", calls=0, items=10)
    _cost(conn, "nothing_queued", calls=0, items=0)   # legitimately no work
    conn.commit()
    assert verdicts.judgeless_scans(conn) == frozenset({"dead"})


def test_a_judgeless_scan_does_not_clear_a_veto(conn) -> None:
    """The bug: two dead scans wrote classes_disagreed=0 and recovered a row
    that the judge never actually looked at."""
    _cost(conn, "s1", calls=1, items=1)
    _cost(conn, "s2", calls=0, items=1)      # judge never ran
    _cost(conn, "s3", calls=0, items=1)      # judge never ran
    _obs(conn, "x", "s1", 100, True)         # real veto
    _obs(conn, "x", "s2", 200, False)        # spurious clean
    _obs(conn, "x", "s3", 300, False)        # spurious clean
    conn.commit()
    assert verdicts.effective_for(conn, "x").vetoed is True, \
        "two non-observations must not satisfy the two-clean-scan recovery"


def test_forensic_flag_reproduces_the_pre_fix_answer(conn) -> None:
    """include_judgeless=True is for reproducing history, never for a live read."""
    _cost(conn, "s1", calls=1, items=1)
    _cost(conn, "s2", calls=0, items=1)
    _cost(conn, "s3", calls=0, items=1)
    _obs(conn, "x", "s1", 100, True)
    _obs(conn, "x", "s2", 200, False)
    _obs(conn, "x", "s3", 300, False)
    conn.commit()
    assert verdicts.effective_for(conn, "x", include_judgeless=True).vetoed is False
    assert verdicts.effective_for(conn, "x").vetoed is True


def test_scans_seen_counts_only_real_observations(conn) -> None:
    _cost(conn, "s1", calls=1, items=1)
    _cost(conn, "s2", calls=0, items=1)
    _obs(conn, "x", "s1", 100, False)
    _obs(conn, "x", "s2", 200, False)
    conn.commit()
    assert verdicts.effective_for(conn, "x").scans_seen == 1


def test_effective_all_excludes_judgeless_too(conn) -> None:
    _cost(conn, "s1", calls=1, items=1)
    _cost(conn, "s2", calls=0, items=1)
    _cost(conn, "s3", calls=0, items=1)
    for oid in ("a", "b"):
        _obs(conn, oid, "s1", 100, True)
        _obs(conn, oid, "s2", 200, False)
        _obs(conn, oid, "s3", 300, False)
    conn.commit()
    eff = verdicts.effective_all(conn)
    assert all(e.vetoed for e in eff.values())
    assert all(e.scans_seen == 1 for e in eff.values())


def test_a_real_clean_scan_still_recovers(conn) -> None:
    """The fix must not make recovery impossible -- only unearned recovery."""
    for i, sid in enumerate(("s1", "s2", "s3")):
        _cost(conn, sid, calls=1, items=1)
    _obs(conn, "x", "s1", 100, True)
    _obs(conn, "x", "s2", 200, False)
    _obs(conn, "x", "s3", 300, False)
    conn.commit()
    assert verdicts.effective_for(conn, "x").vetoed is False


# ---------------------------------------------------------------------------
# Scan calibration (E22 amendment): the occasion is the variable, not the row
# ---------------------------------------------------------------------------

def test_a_three_tuple_observation_is_treated_as_representative() -> None:
    """Backward compatibility: callers predating calibration are unchanged."""
    assert derive(obs((True, False), (False, False), (False, False))).vetoed is False


def test_a_clean_verdict_from_a_deviant_scan_does_not_advance_recovery() -> None:
    veto = (1, True, False, True)
    clean_dev = (2, False, False, False)     # non-representative
    clean_rep = (3, False, False, True)
    assert derive([veto, clean_dev, clean_dev]).vetoed is True, \
        "two readings from unrepresentative scans must not clear a veto"
    assert derive([veto, clean_rep, clean_rep]).vetoed is False


def test_a_veto_from_a_deviant_scan_still_takes_effect_immediately() -> None:
    """One-directional: calibration slows recovery, never the veto."""
    v = derive([(1, False, False, True), (2, True, False, False)])
    assert v.vetoed is True


def test_calibration_can_only_withhold_recovery_never_grant_it() -> None:
    """Property: for any history, calibrating cannot turn VETOED into GREEN."""
    from itertools import product
    for pattern in product([True, False], repeat=4):
        for flags in product([True, False], repeat=4):
            hist_cal = [(i, d, False, r) for i, (d, r) in enumerate(zip(pattern, flags))]
            hist_raw = [(i, d, False, True) for i, d in enumerate(pattern)]
            if derive(hist_raw).vetoed:
                assert derive(hist_cal).vetoed, \
                    "calibration turned a VETOED history into GREEN"


def test_scan_deviance_scores_each_scan_against_the_pooled_rate(conn) -> None:
    _cost(conn, "calm", calls=1, items=1)
    _cost(conn, "harsh", calls=1, items=1)
    for i in range(20):                       # calm scan: 10% vetoed
        _obs(conn, f"r{i}", "calm", 100, i < 2)
    for i in range(20):                       # harsh scan: 90% vetoed
        _obs(conn, f"r{i}", "harsh", 200, i >= 2)
    conn.commit()
    z = verdicts.scan_deviance(conn)
    assert z["calm"] < 0 < z["harsh"]
    assert verdicts.deviant_scans(conn) == frozenset({"calm", "harsh"})


def test_deviance_threshold_is_adjustable_and_provisional() -> None:
    assert verdicts.DEVIANCE_Z == 2.0


def test_judgeless_scans_are_excluded_from_the_deviance_baseline(conn) -> None:
    """A scan the judge never saw has no rate to compare."""
    _cost(conn, "live", calls=1, items=1)
    _cost(conn, "dead", calls=0, items=1)
    for i in range(10):
        _obs(conn, f"r{i}", "live", 100, i < 3)
        _obs(conn, f"r{i}", "dead", 200, False)
    conn.commit()
    assert "dead" not in verdicts.scan_deviance(conn)
