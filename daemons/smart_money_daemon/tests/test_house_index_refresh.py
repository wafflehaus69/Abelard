"""The House PTR leg must re-read the CURRENT year's index every night.

`fetch_year_zip` served the cached 2026FD.zip whenever it merely existed. The Clerk
republishes that same zip all year as PTRs arrive, so the cache froze the index at
the day it was first written — 2026-07-22 — and scan.py reported `[OK] house_clerk`
with zero new filings every night for two months while the live index kept growing
(52,265 bytes cached, 59,694 live on 2026-09-20). An empty success, reported as
health.

The annual-FD sibling (house_fd_ingest.fetch_year_index) had already found and
fixed the identical bug for itself; this leg never got the fix.
"""
import inspect
import io
import os
import time
import zipfile

from smart_money import house_ingest, scan


def _zip(filings):
    """A Clerk-shaped index zip: one XML, one <Member> per filing."""
    members = "".join(
        "<Member><DocID>{}</DocID><FilingType>{}</FilingType>"
        "<FilingDate>{}</FilingDate></Member>".format(doc, ftype, date)
        for doc, ftype, date in filings)
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        z.writestr("2026FD.xml", "<FinancialDisclosure>{}</FinancialDisclosure>"
                   .format(members))
    return buf.getvalue()


class _Resp:
    def __init__(self, content, status_code=200):
        self.content, self.status_code = content, status_code


JULY = [("20001", "P", "7/20/2026")]
SEPT = JULY + [("20002", "P", "9/15/2026"), ("20003", "P", "9/17/2026")]


def _age(path, seconds):
    t = time.time() - seconds
    os.utime(path, (t, t))


def test_the_current_year_is_refetched_and_new_filings_appear(tmp_path, monkeypatch):
    """THE regression. A July snapshot on disk, a September index live: the leg
    must see September's filings."""
    z = tmp_path / "2026FD.zip"
    z.write_bytes(_zip(JULY))
    _age(z, 60 * 86400)
    monkeypatch.setattr(house_ingest, "_get", lambda url, ua: _Resp(_zip(SEPT)))
    got = {f["DocID"] for f in house_ingest.fetch_year_zip(2026, tmp_path, "ua",
                                                            max_age_days=0)}
    assert got == {"20001", "20002", "20003"}, got


def test_max_age_zero_refetches_even_last_nights_copy(tmp_path, monkeypatch):
    """A nightly run lands a few minutes either side of 24h after the last. Any
    positive threshold near a day skips alternate nights; zero never skips."""
    z = tmp_path / "2026FD.zip"
    z.write_bytes(_zip(JULY))
    _age(z, 23 * 3600 + 58 * 60)
    calls = []

    def fake_get(url, ua):
        calls.append(url)
        return _Resp(_zip(SEPT))

    monkeypatch.setattr(house_ingest, "_get", fake_get)
    house_ingest.fetch_year_zip(2026, tmp_path, "ua", max_age_days=0)
    assert calls, "a 23h58m-old copy of the current year must still be refetched"


def test_a_closed_year_is_still_served_from_cache(tmp_path, monkeypatch):
    """Without max_age_days nothing changes: historical years are final and
    refetching them every night would be waste."""
    z = tmp_path / "2022FD.zip"
    z.write_bytes(_zip([("10001", "P", "3/1/2022")]))
    _age(z, 400 * 86400)
    monkeypatch.setattr(house_ingest, "_get", lambda *a, **k: (_ for _ in ()).throw(
        AssertionError("must not refetch a closed year")))
    got = house_ingest.fetch_year_zip(2022, tmp_path, "ua")
    assert [f["DocID"] for f in got] == ["10001"]


def test_a_failed_refresh_is_loud_not_stale(tmp_path, monkeypatch):
    """If the Clerk is down the leg must go DEGRADED, not quietly fall back to a
    months-old index and report OK again."""
    z = tmp_path / "2026FD.zip"
    z.write_bytes(_zip(JULY))
    _age(z, 86400)
    monkeypatch.setattr(house_ingest, "_get", lambda url, ua: _Resp(b"", 503))
    try:
        house_ingest.fetch_year_zip(2026, tmp_path, "ua", max_age_days=0)
    except house_ingest.IngestError as exc:
        assert "503" in str(exc)
    else:
        raise AssertionError("a failed refresh must raise, not serve the stale copy")


def test_the_nightly_leg_actually_asks_for_a_refresh():
    """The fix is worthless if the call site does not use it — the old comment
    said 'refresh current-year index' above a call that refreshed nothing."""
    src = inspect.getsource(scan.leg_congress)
    assert "fetch_year_zip(year, raw_dir, ua, max_age_days=0)" in src
