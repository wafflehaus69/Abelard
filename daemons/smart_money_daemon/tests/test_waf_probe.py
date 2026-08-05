"""SM-C3 Phase W: probe log and window map.

The map's job is to replace an ASSERTION ("eFD blocks scripted access") with data. So the
load-bearing property is that it refuses to produce a verdict it cannot support.
"""
import time

from smart_money import db as dbmod, waf_probe


def _log(con, hour, ok, ms=100, ago=0, weekend=False):
    # 2026-08-01 is a Saturday, 2026-07-29 a Wednesday.
    day = "2026-08-01" if weekend else "2026-07-29"
    con.execute(
        "INSERT INTO efd_probe_log(probed_at_unix, probed_at_iso, hour_local, kind, ok, "
        "status, latency_ms, detail) VALUES(?,?,?,?,?,?,?,?)",
        (int(time.time()) - ago, "{}T{:02d}:00:00".format(day, hour), hour, "search",
         1 if ok else 0, "ok" if ok else "error", ms, ""))


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
    assert "INSUFFICIENT WEEKDAY DATA" in txt, txt
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


def test_weekend_probes_cannot_carry_a_verdict(tmp_path):
    """Day of week is a CONFOUND: weekend load on eFD is a fraction of weekday load, so a
    load-shaped throttle lets weekend probes succeed at every hour. A pile of green
    weekend samples must NOT license a 'no window' finding."""
    con = dbmod.connect(str(tmp_path / "we.db"))
    for hour in (6, 9, 12, 15, 18, 21):
        for _ in range(10):
            _log(con, hour, True, weekend=True)       # 60 green weekend probes
    con.commit()
    wd, we = waf_probe.dow_split(con)
    assert we["n"] == 60 and wd["n"] == 0
    txt = waf_probe._render_map(waf_probe.window_map(con), 14, wd, we,
                                waf_probe.window_map(con, weekday_only=True))
    assert "INSUFFICIENT WEEKDAY DATA" in txt, txt
    assert "NO WINDOW" not in txt, "60 weekend probes must not produce a verdict"
    con.close()


def test_verdict_uses_weekday_rows_only(tmp_path):
    """With enough weekday data the verdict is computed from WEEKDAY hours, even when
    weekend samples would paint a rosier picture."""
    con = dbmod.connect(str(tmp_path / "wd.db"))
    for _ in range(12):
        _log(con, 9, True, weekend=False)             # weekday 09:00 healthy
    for _ in range(12):
        _log(con, 22, False, weekend=False)           # weekday 22:00 dead
    for _ in range(20):
        _log(con, 22, True, weekend=True)             # weekend 22:00 fine - a trap
    con.commit()
    wd, we = waf_probe.dow_split(con)
    assert wd["n"] == 24 and we["n"] == 20
    txt = waf_probe._render_map(waf_probe.window_map(con), 14, wd, we,
                                waf_probe.window_map(con, weekday_only=True))
    assert "WINDOW CANDIDATE (weekday only)" in txt, txt
    assert "09:00" in txt
    con.close()


def test_dow_split_reports_both_populations(tmp_path):
    con = dbmod.connect(str(tmp_path / "sp.db"))
    for _ in range(3):
        _log(con, 9, True, weekend=False)
    for _ in range(4):
        _log(con, 9, False, weekend=True)
    con.commit()
    wd, we = waf_probe.dow_split(con)
    assert (wd["n"], wd["ok"]) == (3, 3)
    assert (we["n"], we["ok"]) == (4, 0)
    con.close()


def test_host_offline_probes_are_excluded_from_availability(tmp_path):
    """A probe that cannot tell "eFD refused us" from "this host had no internet"
    produces a FRAUDULENT map. On 2026-08-04 Basilic lost outbound DNS/HTTPS and ten
    consecutive probes logged as eFD failures — which read as exactly the weekday
    throttle the map exists to measure. no_network rows must be excluded from every rate
    and reported separately."""
    con = dbmod.connect(str(tmp_path / "off.db"))
    for _ in range(10):
        _log(con, 9, True, weekend=False)                       # real successes
    for _ in range(10):                                          # local outage
        con.execute(
            "INSERT INTO efd_probe_log(probed_at_unix, probed_at_iso, hour_local, kind, "
            "ok, status, latency_ms, detail) VALUES(?,?,?,?,?,?,?,?)",
            (int(time.time()), "2026-07-29T09:00:00", 9, "search", 0,
             "no_network", 200, "host offline"))
    con.commit()
    rows = waf_probe.window_map(con)
    assert rows and rows[0]["attempts"] == 10, "offline probes must not inflate attempts"
    assert rows[0]["rate"] == 100.0, "a local outage must not read as an eFD refusal"
    wd, we = waf_probe.dow_split(con)
    assert wd["n"] == 10 and wd["ok"] == 10
    txt = waf_probe._render_map(rows, 14, wd, we, rows, excluded=10)
    assert "EXCLUDED as host-offline" in txt and "10 probe(s)" in txt
    con.close()
