"""SM-C3: the annual FD holdings refresh leg.

`leg_congress` covers PTR trades; this covers the annual HOLDINGS snapshot that
/congress, the member books and the Phase F fusion anchor are built on. Without it the
holdings corpus silently ages a full year.
"""
import pathlib
import time

from smart_money import house_fd_ingest as hfd, scan


def test_leg_degrades_per_chamber_and_never_raises(monkeypatch, tmp_path):
    """eFD is WAF-fronted and the House zip 404s early in a cycle. One chamber failing
    must be REPORTED, not allowed to take the nightly scan down."""
    def boom(*a, **k):
        raise RuntimeError("house index exploded")
    monkeypatch.setattr(hfd, "fetch_year_index", boom)
    monkeypatch.setattr(scan, "bootstrap",
                        lambda *a, **k: (_ for _ in ()).throw(RuntimeError("efd down")))
    srcs, counts = scan.leg_congress_annual(None, "x@example.com", tmp_path)
    by = {s["source"]: s for s in srcs}
    assert by["congress_annual:house"]["status"] == "DEGRADED"
    assert by["congress_annual:senate"]["status"] == "DEGRADED"
    assert "exploded" in by["congress_annual:house"]["note"]


def test_leg_emits_no_events():
    """Ingest-only by construction: the leg returns (sources, counts) and has no event
    channel at all, so an annual snapshot can never reach the decision queue."""
    import inspect
    src = inspect.getsource(scan.leg_congress_annual)
    assert "make_event" not in src, "annual holdings are a snapshot, never an event"
    assert src.rstrip().endswith("return sources, counts")


def test_year_index_cache_refreshes_when_stale(tmp_path, monkeypatch):
    """THE bug this leg would otherwise hit: fetch_year_index returns the cached zip if
    it merely EXISTS. For the CURRENT cycle that makes a nightly leg permanently blind —
    new annual filings land in the same zip all year."""
    raw = tmp_path
    zpath = raw / "2026FD.zip"
    zpath.write_bytes(b"stale")
    calls = []

    def fake_get(url, ua):
        calls.append(url)
        raise AssertionError("refetch attempted")   # we only care THAT it refetches

    monkeypatch.setattr(hfd, "_get", fake_get)
    # fresh cache (age 0) with max_age_days=1 -> must NOT refetch
    try:
        hfd.fetch_year_index(2026, raw, "ua", max_age_days=1)
    except AssertionError:
        raise AssertionError("refetched a FRESH cache")
    except Exception:
        pass                                        # zip is not a real archive; fine
    assert not calls, "fresh cache must be reused"
    # age the file past the limit -> must refetch
    old = time.time() - 3 * 86400
    import os
    os.utime(zpath, (old, old))
    try:
        hfd.fetch_year_index(2026, raw, "ua", max_age_days=1)
    except AssertionError:
        pass                                        # the refetch we wanted
    assert calls, "stale cache must trigger a refetch"


def test_no_max_age_preserves_historical_cache(tmp_path, monkeypatch):
    """Closed historical years must still be served from cache — refetching them every
    night would be pure waste."""
    raw = tmp_path
    z = raw / "2022FD.zip"
    z.write_bytes(b"old")
    old = time.time() - 400 * 86400
    import os
    os.utime(z, (old, old))
    monkeypatch.setattr(hfd, "_get", lambda *a, **k: (_ for _ in ()).throw(
        AssertionError("must not refetch a historical year")))
    try:
        hfd.fetch_year_index(2022, raw, "ua")        # no max_age_days
    except AssertionError:
        raise
    except Exception:
        pass                                        # not a real zip; the point is no refetch
