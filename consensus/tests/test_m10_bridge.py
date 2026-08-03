"""M10-D live-scan bridge tests: factor F restoration on the gated set, Rule-1
declaration when a lookup fails, and CAPTURE-WIDE persistence into the store."""

import consensus.dossier_store as ds
import consensus.m10 as m10
from consensus.firstseen import FirstSeen


class _Cand:
    """Minimal stand-in for a scored candidate."""

    def __init__(self, wallet, cid="0xM", token="0xT", composite=0.6, net=50_000.0, tier="ELEVATED"):
        self.wallet, self.condition_id, self.token_id = wallet, cid, token
        self.composite, self.net_stake_usdc, self.tier = composite, net, tier
        self.vwap_entry, self.first_bet_ts, self.last_bet_ts = 0.4, 1_700_000_000, 1_700_100_000
        self.factors = {"S": 0.5, "D": 0.6, "C": 0.7}
        self.factors_active = ["S", "D", "C"]
        self.cluster_ids, self.data_incomplete, self.notes = [], False, {}


def test_persist_captures_wide_and_maps_fields(tmp_path):
    db = str(tmp_path / "d.db")
    cands = [_Cand("0xA", composite=0.9, tier="CRITICAL"),
             _Cand("0xB", composite=0.2, tier="NONE"),          # below surfacing, still captured
             _Cand("0xC", net=10.0, tier="NONE")]               # below the size floor -> dropped
    cfg = type("M10", (), {"size_floor_usdc": 1000.0})()
    out = m10._persist(
        cands, db,
        market_meta={"0xM": {"question": "Will X?", "category": "geopolitics", "slug": "x"}},
        firstseen_meta={"0xA": {"ts": 1_600_000_000, "source": "activity"}},
        scan_ts=1_700_200_000, m10=cfg,
    )
    assert out == {"inserted": 2, "updated": 0}          # CAPTURE WIDE: the tier-NONE row too
    con = ds.connect(db)
    rows = {r["wallet"]: r for r in ds.query(con)}
    assert set(rows) == {"0xA", "0xB"}
    a = rows["0xA"]
    assert a["market_question"] == "Will X?" and a["market_category"] == "geopolitics"
    assert a["first_seen_ts"] == 1_600_000_000 and a["first_seen_source"] == "activity"
    # §4.4: the two notionals are DIFFERENT FACTS. The scan measures the headline
    # (no price filter); the contested slice is not measured here, so it is declared
    # unknown rather than imputed equal to the headline.
    assert a["headline_notional"] == 50_000.0
    assert a["contested_notional"] is None
    assert a["s_factor"] == 0.5
    # §4.2: the funding mesh is not computed by this scan, so the post-collapse actor
    # count is UNKNOWN — asserting 1 would claim a collapse was checked.
    assert a["actor_count_post_collapse"] is None
    assert ds.is_mesh_collapsed(a) is None       # unknown, must not fail open to False
    # a wallet that was never gated carries no imputed first-seen (Rule 1)
    assert rows["0xB"]["first_seen_ts"] is None


def test_cluster_roster_is_preserved_not_discarded(tmp_path):
    """§4.2: the scan tags candidates with cluster ids; _persist must recover the
    co-member roster from them. Hardcoding [self] threw away the raw count that §4.2
    requires be reported alongside the post-collapse count."""
    db = str(tmp_path / "d.db")
    a, b, c = _Cand("0xA"), _Cand("0xB"), _Cand("0xC")
    for x in (a, b):
        x.cluster_ids = ["clu-1"]          # A and B co-trade; C is solo
    # run_scan computes the roster and the actor count together (they must describe one
    # wallet set); _persist writes exactly what it is given.
    for x in (a, b):
        x.notes["cluster_roster"] = ["0xA", "0xB"]
    cfg = type("M10", (), {"size_floor_usdc": 1000.0})()
    m10._persist([a, b, c], db, market_meta={}, firstseen_meta={}, scan_ts=1, m10=cfg)
    rows = {r["wallet"]: r for r in ds.query(ds.connect(db))}
    import json as _j
    assert sorted(_j.loads(rows["0xA"]["cluster_wallets"])) == ["0xA", "0xB"]
    assert sorted(_j.loads(rows["0xB"]["cluster_wallets"])) == ["0xA", "0xB"]
    assert _j.loads(rows["0xC"]["cluster_wallets"]) == ["0xC"]


def test_persist_is_idempotent_across_scans(tmp_path):
    db = str(tmp_path / "d.db")
    cfg = type("M10", (), {"size_floor_usdc": 1000.0})()
    args = dict(market_meta={}, firstseen_meta={}, m10=cfg)
    assert m10._persist([_Cand("0xA")], db, scan_ts=1, **args)["inserted"] == 1
    assert m10._persist([_Cand("0xA")], db, scan_ts=2, **args)["updated"] == 1
    con = ds.connect(db)
    assert len(ds.query(con)) == 1


def test_tier_peak_latches_across_scans(tmp_path):
    """The durable high-water mark the in-memory latch could not provide."""
    db = str(tmp_path / "d.db")
    cfg = type("M10", (), {"size_floor_usdc": 1000.0})()
    args = dict(market_meta={}, firstseen_meta={}, m10=cfg)
    m10._persist([_Cand("0xA", composite=0.9, tier="CRITICAL")], db, scan_ts=1, **args)
    m10._persist([_Cand("0xA", composite=0.3, tier="WATCH")], db, scan_ts=2, **args)
    row = ds.query(ds.connect(db))[0]
    assert row["tier"] == "WATCH" and row["tier_peak"] == "CRITICAL"


def test_firstseen_resolver_contract():
    """The bridge only consumes .available/.ts/.source — a failed lookup is declared."""
    ok = FirstSeen("0xA", 1_600_000_000, "activity", True)
    bad = FirstSeen("0xB", None, "unavailable", False)
    assert ok.available and ok.ts and ok.source == "activity"
    assert not bad.available and bad.ts is None and bad.source == "unavailable"
