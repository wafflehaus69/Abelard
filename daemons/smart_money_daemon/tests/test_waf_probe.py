"""SM-C3 Phase W: probe log and window map.

The map's job is to replace an ASSERTION ("eFD blocks scripted access") with data. So the
load-bearing property is that it refuses to produce a verdict it cannot support.
"""
import time

from smart_money import db as dbmod, waf_probe


def _log(con, hour, ok, ms=100, ago=0):
    con.execute(
        "INSERT INTO efd_probe_log(probed_at_unix, probed_at_iso, hour_local, kind, ok, "
        "status, latency_ms, detail) VALUES(?,?,?,?,?,?,?,?)",
        (int(time.time()) - ago, "iso", hour, "search", 1 if ok else 0,
         "ok" if ok else "error", ms, ""))


def test_failed_probes_are_recorded_not_dropped(tmp_path):
    con = dbmod.connect(str(tmp_path / "p.db"))
    waf_probe.record(con, "search", False, "error", 30000, "timeout")
    row = con.execute("SELECT ok, status, latency_ms FROM efd_probe_log").fetchone()
    assert row == (0, "error", 30000), row
    con.close()


def test_window_map_buckets_by_hour(tmp_path):
    con = dbmod.connect(str(tmp_path / "m.db"))
    for _ in range(8):
        _log(con, 9, True)
    for _ in range(8):
        _log(con, 22, False)
    for _ in range(8):
        _log(con, 12, True)
    con.commit()
    rows = {r["hour"]: r for r in waf_probe.window_map(con)}
    assert rows[9]["rate"] == 100.0 and rows[9]["attempts"] == 8
    assert rows[22]["rate"] == 0.0
    txt = waf_probe._render_map(waf_probe.window_map(con), 14)
    assert "WINDOW CANDIDATE" in txt, txt
    con.close()


def test_no_verdict_below_the_data_floor(tmp_path):
    """n=5 is suggestive, not proof. The map must SAY so rather than declare a window."""
    con = dbmod.connect(str(tmp_path / "f.db"))
    for _ in range(3):
        _log(con, 9, True)
    for _ in range(2):
        _log(con, 22, False)
    con.commit()
    txt = waf_probe._render_map(waf_probe.window_map(con), 14)
    assert "INSUFFICIENT DATA" in txt, txt
    assert "WINDOW CANDIDATE" not in txt
    con.close()


def test_flat_availability_reports_no_window(tmp_path):
    """Enough data but no hour-of-day effect must read as 'no window', not a fishing
    expedition for the best-looking bucket."""
    con = dbmod.connect(str(tmp_path / "n.db"))
    for hour in (6, 9, 12, 15, 18, 21):
        for i in range(5):
            _log(con, hour, i < 4)          # ~80% everywhere
    con.commit()
    txt = waf_probe._render_map(waf_probe.window_map(con), 14)
    assert "NO WINDOW" in txt, txt
    con.close()


def test_empty_map_is_not_a_no_window_finding(tmp_path):
    con = dbmod.connect(str(tmp_path / "e.db"))
    txt = waf_probe._render_map(waf_probe.window_map(con), 14)
    assert "NO PROBES LOGGED" in txt and "NO WINDOW" not in txt
    con.close()


def test_map_window_is_time_bounded(tmp_path):
    """Old probes must age out so the map reflects current behaviour."""
    con = dbmod.connect(str(tmp_path / "t.db"))
    for _ in range(6):
        _log(con, 9, True, ago=60 * 86400)      # 60 days old
    con.commit()
    assert waf_probe.window_map(con, days=14) == []
    assert len(waf_probe.window_map(con, days=90)) == 1
    con.close()
