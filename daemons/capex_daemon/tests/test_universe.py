"""B1 acceptance tests for the roster and coverage-derived tiering."""
import pytest

from capex_daemon import config, universe


def test_roster_loads_and_is_cik_keyed():
    roster = universe.load()
    # 23 original + 7 ratified CD-R2 2026-08-14 + 5 CD-3 suppliers (cross-check
    # bucket; they sell the buildout rather than buy it and are never summed in).
    assert len(roster) == 35
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
    # LANDLORD merge, ruled by Mando 2026-08-26: reit + host became one bucket.
    # DLR and EQIX were `reit`; AMT, IRM and CCOI were `host`. The split was thin
    # on its own terms — AMT and IRM are REITs — and a two-name REIT bucket had
    # gone dark for a month on one late filing.
    assert by_ticker["DLR"] == "landlord"
    assert by_ticker["EQIX"] == "landlord"
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
def test_graduation_at_four_makes_core_with_short_history(n):
    """R-B6-2: graduation stands at 4. Short history is disclosed on the row,
    never used to withhold membership."""
    e = universe.load()["0001769628"]  # CRWV, 9 quarters at recon
    tier, reason = universe.tier_for(e, n)
    assert tier == universe.TIER_CORE
    assert "SHORT-HISTORY" in reason
    assert universe.is_short_history(n) is True


def test_long_history_core_is_not_flagged_short():
    e = universe.load()["0000789019"]  # MSFT, 72 quarters
    tier, reason = universe.tier_for(e, 72)
    assert tier == universe.TIER_CORE
    assert "SHORT-HISTORY" not in reason
    assert universe.is_short_history(72) is False


def test_three_quarters_is_still_thin():
    e = universe.load()["0002071778"]  # FRMI
    assert universe.tier_for(e, 3)[0] == universe.TIER_THIN


def test_calendar_offset_tolerance_stays_unset():
    """E8: ruling (b) left it OPEN pending an observed distribution."""
    assert config.CALENDAR_OFFSET_TOLERANCE_DAYS is None
    assert config.CORE_MIN_QUARTERS == 4


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


# --- CD-R2 ratification, 2026-08-14 --------------------------------------

def test_ratified_adds_are_present_with_their_buckets():
    by = {e.ticker_display: e.bucket for e in universe.load().values()}
    assert by["MARA"] == "builder"
    assert by["CLSK"] == "builder"
    assert by["DGXX"] == "builder"
    assert by["IRM"] == "landlord"
    assert by["AMT"] == "landlord"
    assert by["CCOI"] == "landlord"


def test_prologis_and_tsm_are_not_admitted():
    """PLD ruled out (warehouses, not datacenters); TSM deferred (IFRS)."""
    tickers = {e.ticker_display for e in universe.load().values()}
    assert "PLD" not in tickers
    assert "TSM" not in tickers


def test_btbt_is_sidecar_so_it_never_double_counts_wyfi():
    """BTBT consolidates WhiteFiber. WYFI stays in the totals; BTBT is tracked
    on the side and excluded from every aggregate."""
    from capex_daemon import divergence
    roster = universe.load()
    by = {e.ticker_display: e.bucket for e in roster.values()}
    assert by["BTBT"] == "sidecar"
    assert by["WYFI"] == "builder"
    assert "sidecar" not in divergence.BUCKET_ORDER


def test_host_bucket_is_tracked_but_not_aggregated():
    """Their capex is real but not separable from a larger consolidated line."""
    from capex_daemon import divergence
    assert "host" in universe.BUCKETS
    assert "host" not in divergence.BUCKET_ORDER


def test_the_landlord_merge_preserves_sub_type():
    """The bucket boundary was thin; the distinction inside it is not. A reader
    asking 'is this a REIT?' still gets an answer."""
    from capex_daemon import universe
    roster = universe.load()
    members = {e.ticker_display: universe.landlord_subtype(c)
               for c, e in roster.items() if e.bucket == "landlord"}
    assert set(members) == {"DLR", "EQIX", "AMT", "IRM", "CCOI"}
    assert members["AMT"] == "reit" and members["IRM"] == "reit"
    assert members["CCOI"] == "host"


def test_landlord_is_aggregated_and_the_old_names_are_not():
    from capex_daemon import trend, universe
    assert "landlord" in trend.AGGREGATED_BUCKETS
    assert "reit" not in trend.AGGREGATED_BUCKETS
    assert "host" not in trend.AGGREGATED_BUCKETS
    # ...but the old names still LOAD, so a pre-merge roster does not raise.
    assert "reit" in universe.BUCKETS and "host" in universe.BUCKETS


def test_five_members_clears_the_floor_with_room():
    """The merge's whole point: one late filing can no longer dark the bucket."""
    from capex_daemon import trend, universe
    roster = universe.load()
    n = sum(1 for e in roster.values() if e.bucket == "landlord")
    assert n == 5 and n - 1 > trend.MIN_BUCKET_MEMBERS
