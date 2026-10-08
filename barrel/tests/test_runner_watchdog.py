"""The metered runner's watchdog and its --refetch path, with Dune stubbed. No network.

A query that is being paid for is never left running without a cap, and rows of an execution that already ran can
be read again without running it again.
Run: python -m pytest barrel/tests/test_runner_watchdog.py"""
import json
import pathlib
import sys

import pytest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1] / "recon"))
import dune_run_sql as r  # noqa: E402

LEDGER = "| n | file | id | credits | total |\n|---|---|---|---|---|\n| 0 | x | `x` | 0.000 | 0.00 |\n\n" \
         "**Total consumed: 0.00 credits. Remaining: 25000. Usable after 15% reserve: 21250.**\n"


class Clock:
    """Stands in for the time module inside the runner: sleeping advances it."""
    def __init__(self): self.t = 0.0
    def time(self): return self.t
    def sleep(self, s): self.t += s


class Dune:
    """Records every request. `status` is called with the clock and returns the status body."""
    def __init__(self, clock, status, rows=None):
        self.clock, self.status, self.rows, self.calls, self.cancelled = clock, status, rows or [], [], False
    def __call__(self, method, path, key, body=None):
        self.calls.append((method, path))
        if path == "/sql/execute": return {"execution_id": "EXEC1"}
        if path.endswith("/cancel"): self.cancelled = True; return {"success": True}
        if path.endswith("/status"):
            if self.cancelled: return {"state": "QUERY_STATE_CANCELLED", "is_execution_finished": True, "execution_cost_credits": 1.0}
            return self.status(self.clock)
        if "/results" in path:
            if self.cancelled: return {"error": "execution was cancelled"}      # Dune has no result for a cancelled execution
            return {"result": {"rows": list(self.rows), "metadata": {"total_row_count": len(self.rows)}}}
        raise AssertionError(path)
    def posted(self, suffix): return [c for c in self.calls if c[0] == "POST" and c[1].endswith(suffix)]


@pytest.fixture
def env(tmp_path, monkeypatch):
    (tmp_path / "recon" / "out").mkdir(parents=True); (tmp_path / "docs").mkdir()
    (tmp_path / "docs" / "credits.md").write_text(LEDGER, encoding="utf-8")
    sql = tmp_path / "probe_day.sql"; sql.write_text("SELECT 1 AS one", encoding="utf-8")
    clock = Clock()
    monkeypatch.setattr(r, "ROOT", tmp_path); monkeypatch.setattr(r, "OUT", tmp_path / "recon" / "out")
    monkeypatch.setattr(r, "LEDGER", tmp_path / "docs" / "credits.md"); monkeypatch.setattr(r, "time", clock)
    monkeypatch.setattr(r.rt, "api_key", lambda: "k")
    monkeypatch.setattr(r.dune_usage, "usage", lambda: {"credits_included": 25000, "credits_used": 100})
    monkeypatch.setattr(r.owner_wallets, "owner_wallets", lambda: [])

    def run(dune, *flags):
        monkeypatch.setattr(r.rt, "dune", dune)
        monkeypatch.setattr(sys, "argv", ["dune_run_sql.py", str(sql), *flags])
        r.main()
    return tmp_path, clock, run


def running(clock): return {"state": "QUERY_STATE_EXECUTING", "is_execution_finished": False, "execution_cost_credits": 1.0}


def test_a_query_that_never_finishes_is_cancelled_even_when_the_time_limit_is_above_fifteen_minutes(env):
    tmp, clock, run = env
    d = Dune(clock, running)
    run(d, "--expect", "20", "--max-seconds", "1500", "--no-rows")
    assert d.posted("/cancel"), "the poll loop ended with the query still running"
    assert clock.t > 1500
    rec = json.loads(next((tmp / "recon" / "out").glob("run_probe_day_*.json")).read_text(encoding="utf-8"))
    assert rec["state"].startswith("CANCELLED BY WATCHDOG")


def test_a_cancel_that_is_refused_is_tried_again_and_said_out_loud(env, capsys):
    tmp, clock, run = env

    class Stubborn(Dune):
        def __call__(self, method, path, key, body=None):
            if path.endswith("/cancel"): self.calls.append((method, path)); return {"_http_error": 500}
            return super().__call__(method, path, key, body)
    d = Stubborn(clock, lambda c: {"state": "QUERY_STATE_EXECUTING", "is_execution_finished": False, "execution_cost_credits": 99.0})
    run(d, "--expect", "20", "--no-rows")
    assert len(d.posted("/cancel")) == 5
    assert "CANCEL NOT CONFIRMED" in capsys.readouterr().out


def test_refetch_reads_rows_of_a_run_already_made_and_executes_nothing(env):
    tmp, clock, run = env
    done = lambda c: {"state": "QUERY_STATE_COMPLETED", "is_execution_finished": True, "execution_cost_credits": 42.0}   # noqa: E731
    first = Dune(clock, done, rows=[{"one": 1}])
    run(first, "--expect", "20", "--no-rows")                       # the cost run: billed, rows not fetched
    assert not [c for c in first.calls if "/results" in c[1]]
    clock.sleep(1)                                                   # a later second, so the record names differ
    again = Dune(clock, done, rows=[{"one": 1}, {"one": None}])
    run(again, "--expect", "1", "--refetch", "EXEC1", "--export")
    assert not again.posted("/sql/execute") and not again.posted("/cancel")
    man = json.loads((tmp / "recon" / "export_manifest.json").read_text(encoding="utf-8"))
    assert man[-1]["n_rows"] == 2 and man[-1]["null_counts"] == {"one": 1} and man[-1]["execution_id"] == "EXEC1"
    ledger = (tmp / "docs" / "credits.md").read_text(encoding="utf-8")
    assert "(refetch, rows only) | `EXEC1…` | 0.000 |" in ledger      # billed once, when it ran


def test_refetch_refuses_an_execution_this_file_never_ran(env):
    tmp, clock, run = env
    d = Dune(clock, running)
    with pytest.raises(SystemExit, match="no run record"):
        run(d, "--expect", "1", "--refetch", "SOMEONE_ELSES", "--export")
    assert not d.calls


def test_refetch_never_cancels_and_never_fetches_an_execution_that_is_not_complete(env):
    tmp, clock, run = env
    (tmp / "recon" / "out" / "run_probe_day_x.json").write_text(json.dumps({"file": "probe_day.sql", "execution_id": "EXEC1"}), encoding="utf-8")
    d = Dune(clock, running)
    with pytest.raises(SystemExit, match="not completed"):
        run(d, "--expect", "1", "--refetch", "EXEC1", "--export")
    assert d.calls == [("GET", "/execution/EXEC1/status")]


def test_a_cancelled_private_run_leaves_no_rows_file(env):
    """A rows file holding only `null` was taken for the newest result by the scripts that read private/out."""
    tmp, clock, run = env
    d = Dune(clock, lambda c: {"state": "QUERY_STATE_EXECUTING", "is_execution_finished": False, "execution_cost_credits": 99.0})
    run(d, "--expect", "20", "--private-rows")
    assert d.posted("/cancel")
    assert not list((tmp / "private" / "out").glob("rows_*.json"))
    done = Dune(clock, lambda c: {"state": "QUERY_STATE_COMPLETED", "is_execution_finished": True, "execution_cost_credits": 1.0}, rows=[{"one": 1}])
    clock.sleep(1)
    run(done, "--expect", "20", "--private-rows")
    assert len(list((tmp / "private" / "out").glob("rows_*.json"))) == 1
