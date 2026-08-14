"""B1 acceptance tests for the roster and coverage-derived tiering."""
import pytest

from capex_daemon import config, universe


def test_roster_loads_and_is_cik_keyed():
    roster = universe.load()
    assert len(roster) == 23
    assert all(len(cik) == 10 and cik.isdigit() for cik in roster)
    # CIK is the key; ticker is a display attribute (E10).
    assert roster["0000789019"].ticker_display == "MSFT"
    assert roster["0001812477"].ticker_display == "KEEL"


def test_every_ruled_name_is_present():
    roster = universe.load()
    by_ticker = {e.ticker_display: e for e in roster.values()}
    core13 = ["MSFT", "GOOGL", "AMZN", "META", "ORCL", "DLR", "EQIX",
              "RIOT", "CORZ", "WULF", "CIFR", "HUT", "APLD"]
    thin3 = ["FRMI", "KEEL", "SPCX"]
    contested4 = ["CRWV", "IREN", "GLXY", "WYFI"]
    for t in core13 + thin3 + contested4 + ["NBIS", "BABA", "SNOW"]:
        assert t in by_ticker, "{} missing from roster".format(t)


def test_buckets_match_the_ruled_decomposition():
    """R4: aggregates decompose by economic species, not by coverage tier."""
    roster = universe.load()
    by_ticker = {e.ticker_display: e.bucket for e in roster.values()}
    assert by_ticker["MSFT"] == "hyperscaler"
    assert by_ticker["DLR"] == "reit"
    assert by_ticker["EQIX"] == "reit"
    assert by_ticker["WULF"] == "builder"
    assert by_ticker["SNOW"] == "mirror"
    assert by_ticker["NBIS"] == "fpi"


def test_thin_below_four_quarters():
    e = universe.load()["0002071778"]  # FRMI
    tier, reason = universe.tier_for(e, 2)
    assert tier == universe.TIER_THIN
    assert "below" in reason


def test_core_above_the_contested_band():
    """Both live rulings agree that >=12 is CORE."""
    e = universe.load()["0000789019"]  # MSFT
    tier, _ = universe.tier_for(e, 72)
    assert tier == universe.TIER_CORE


@pytest.mark.parametrize("n", [4, 7, 9, 11])
def test_contested_band_is_reported_unruled_not_guessed(n):
    """R1 says >=4 is CORE; the ratified CORE=13 roster implies >=12. Until
    Mando rules (CD-1-SPEC 3.1), 4..11 resolves to neither side."""
    e = universe.load()["0001769628"]  # CRWV, 9 quarters at recon
    tier, reason = universe.tier_for(e, n)
    assert tier == universe.TIER_UNRULED_BAND
    assert "conflict" in reason


def test_core_threshold_is_unset_pending_ruling():
    """E8: an unset constant stays None; consumers surface it, never default it."""
    assert config.CORE_MIN_QUARTERS is None
    assert config.CALENDAR_OFFSET_TOLERANCE_DAYS is None


def test_fpi_and_mirror_bypass_coverage_tiering():
    roster = universe.load()
    nbis, _ = universe.tier_for(roster["0001513845"], None)
    snow, _ = universe.tier_for(roster["0001640147"], 12)
    assert nbis == universe.TIER_ANNUAL_DEGRADED
    assert snow == universe.TIER_MIRROR


def test_anchor_band_matches_the_measured_ruling():
    """R2a: order-of-magnitude bound, sized to catch the 23x class."""
    assert config.ANCHOR_BAND == (0.5, 2.0)
    lo, hi = config.ANCHOR_BAND
    for measured in (0.98, 1.01, 0.91, 0.73):   # MSFT META ORCL EQIX at recon
        assert lo <= measured <= hi
    assert not (lo <= 23.3 <= hi)               # the AMZN stale-resolution miss
