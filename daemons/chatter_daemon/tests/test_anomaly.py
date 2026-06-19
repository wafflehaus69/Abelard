"""Anomaly compute — count modes (building/thin/spike/ok + sigma=0 guard, guard
ordering) and Trends within-record elevation (spike/ok/null-none/noisy-discounted)."""

from __future__ import annotations

from chatter_daemon.anomaly import compute_count_anomaly, compute_trend_anomaly
from chatter_daemon.baseline import Baseline


def _count(n, mean, std, *, count, floor=3, min_obs=5, z=2.0):
    return compute_count_anomaly(
        Baseline(n, mean, std), count=count, floor=floor, min_obs=min_obs, z_threshold=z
    )


def test_building_below_min_obs():
    a = _count(2, 5.0, 1.0, count=100)  # n < min_obs -> building, even on a huge count
    assert a.state == "building" and a.z is None and a.observations == 2


def test_thin_below_floor():
    a = _count(10, 5.0, 2.0, count=2, floor=3)
    assert a.state == "thin" and a.z is None


def test_spike_above_threshold():
    a = _count(10, 10.0, 2.0, count=18, z=2.0)  # z = (18-10)/2 = 4 >= 2
    assert a.state == "spike" and a.z == 4.0


def test_ok_below_threshold():
    a = _count(10, 10.0, 2.0, count=12, z=2.0)  # z = 1 < 2
    assert a.state == "ok" and a.z == 1.0


def test_sigma_zero_guard():
    a = _count(10, 5.0, 0.0, count=20, floor=3)  # std 0 -> no z, flagged
    assert a.state == "ok" and a.z is None and "sigma_zero" in a.note


def test_building_precedes_thin_and_z():
    # n < min_obs wins even when count is also below floor.
    a = _count(1, 5.0, 0.0, count=1, floor=3, min_obs=5)
    assert a.state == "building"


def test_thin_precedes_z():
    # below floor wins over a would-be spike (stops noise z-scoring huge).
    a = _count(10, 1.0, 0.5, count=2, floor=3)  # z would be 2 but count<floor
    assert a.state == "thin"


def test_trend_spike():
    a = compute_trend_anomaly(
        interest_24h=90.0, interest_7d=40.0, interest_monthly=30.0, noisy=False, ratio_threshold=1.5
    )
    assert a.state == "spike" and a.ratio == 2.25  # 90 / max(40,30)


def test_trend_ok():
    a = compute_trend_anomaly(
        interest_24h=45.0, interest_7d=40.0, interest_monthly=30.0, noisy=False, ratio_threshold=1.5
    )
    assert a.state == "ok" and a.ratio == 1.125


def test_trend_null_is_none():
    a = compute_trend_anomaly(
        interest_24h=None, interest_7d=None, interest_monthly=None, noisy=False, ratio_threshold=1.5
    )
    assert a.state == "none" and a.ratio is None


def test_trend_noisy_discounted():
    a = compute_trend_anomaly(
        interest_24h=90.0, interest_7d=40.0, interest_monthly=30.0, noisy=True, ratio_threshold=1.5
    )
    assert a.state == "spike" and a.discounted is True


def test_trend_elevation_off_zero_trailing():
    a = compute_trend_anomaly(
        interest_24h=12.0, interest_7d=0.0, interest_monthly=0.0, noisy=False, ratio_threshold=1.5
    )
    assert a.state == "spike" and "trailing ~0" in a.note
