"""L2 tape store: dedupe identity, within-page duplicates, archive-not-drop,
gap ledger, market state."""

from __future__ import annotations

import pytest

from consensus.models import Trade
from consensus.tape import TapeStore, fill_key_base


@pytest.fixture
def tape(tmp_path):
    t = TapeStore(tmp_path / "tape.db")
    yield t
    t.close()


def _fill(i: int, *, cid: str = "0xCID", tx: str | None = None, **over):
    d = {
        "proxyWallet": f"0x{i:040x}", "side": "BUY", "asset": f"asset{i}",
        "conditionId": cid, "size": 1.5, "price": 0.25, "timestamp": 1000 + i,
        "transactionHash": tx or f"0xtx{i}", "slug": "test-market",
    }
    d.update(over)
    return d


def test_store_page_inserts_and_dedupes_across_polls(tape):
    page = [_fill(0), _fill(1)]
    c1 = tape.store_page(page, lane="market", poll_id=1, parsed_by=Trade.from_api)
    assert c1["new"] == 2 and c1["overlap"] == 0
    # Same fills seen again next poll -> pure overlap, nothing duplicated.
    c2 = tape.store_page(page, lane="market", poll_id=2, parsed_by=Trade.from_api)
    assert c2["new"] == 0 and c2["overlap"] == 2
    assert tape.stats()["fills"] == 2


def test_within_page_identical_tuples_are_distinct_fills(tape):
    f = _fill(7)
    c = tape.store_page([f, dict(f)], lane="market", poll_id=1, parsed_by=Trade.from_api)
    # Two identical tuples in ONE page = two real fills (occurrence-suffixed).
    assert c["new"] == 2 and c["dupes"] == 1
    assert tape.stats()["fills"] == 2


def test_unparseable_record_is_archived_not_dropped(tape):
    bad = _fill(3)
    del bad["price"]  # Trade.from_api returns None for this
    c = tape.store_page([bad], lane="market", poll_id=1, parsed_by=Trade.from_api)
    assert c["new"] == 1 and c["unparsed"] == 1
    s = tape.stats()
    # L2 is an archive: the raw record is kept, flagged unusable — never lost.
    assert s["fills"] == 1 and s["fills_unparsed"] == 1


def test_non_dict_records_are_archived_not_dropped(tape):
    """Review finding: malformed array elements (null/string/number) must land
    in the archive with parse_ok=0 — a counter alone is a silent drop."""
    c = tape.store_page(["not-a-dict", None, 42, _fill(0)],
                        lane="market", poll_id=1, parsed_by=Trade.from_api)
    assert c["raw"] == 4 and c["new"] == 4 and c["unparsed"] == 3
    s = tape.stats()
    assert s["fills"] == 4 and s["fills_unparsed"] == 3
    raws = [r[0] for r in tape._conn.execute(
        "SELECT raw FROM l2_trades WHERE parse_ok = 0"
    ).fetchall()]
    assert sorted(raws) == sorted(['"not-a-dict"', "null", "42"])


def test_occurrence_dict_spans_page_boundaries(tape):
    """Two REAL identical-tuple fills straddling a page boundary within one
    walk must both be stored (walk-scoped occurrence), while a fresh walk
    seeing the same tuples dedupes against them (cross-poll identity)."""
    f = _fill(7)
    walk_occurrence: dict[str, int] = {}
    c1 = tape.store_page([f], lane="market", poll_id=1, parsed_by=Trade.from_api,
                         occurrence=walk_occurrence)
    c2 = tape.store_page([dict(f)], lane="market", poll_id=1, parsed_by=Trade.from_api,
                         occurrence=walk_occurrence)
    assert c1["new"] == 1 and c2["new"] == 1 and c2["dupes"] == 1
    assert tape.stats()["fills"] == 2
    # Next walk (fresh occurrence): both tuples are overlap, nothing new.
    c3 = tape.store_page([f, dict(f)], lane="market", poll_id=2, parsed_by=Trade.from_api,
                         occurrence={})
    assert c3["new"] == 0 and c3["overlap"] == 2


def test_restrict_set_skips_untracked_markets(tape):
    page = [_fill(0, cid="0xTRACKED"), _fill(1, cid="0xOTHER")]
    c = tape.store_page(
        page, lane="global", poll_id=1, parsed_by=Trade.from_api,
        restrict_condition_ids={"0xTRACKED"},
    )
    assert c["new"] == 1 and c["skipped"] == 1


def test_fill_key_is_deterministic_and_sensitive():
    a = _fill(1)
    assert fill_key_base(a) == fill_key_base(dict(a))
    b = dict(a); b["price"] = 0.26
    assert fill_key_base(a) != fill_key_base(b)


def test_gap_ledger_and_stats(tape):
    tape.declare_gap(lane="market", condition_id="0xC", lo_ts=100, hi_ts=200,
                     declared_ts=250, reason="test rollover")
    assert tape.stats()["gaps_declared"] == 1


def test_market_upsert_and_tier_state(tape):
    assert tape.upsert_market("0xC", slug="s", question="q?", tags="geopolitics",
                              source="enumeration", now_ts=10) is True
    # Second upsert refreshes metadata, does not reset state, reports known.
    assert tape.upsert_market("0xC", slug="s2", question="q?", tags="geopolitics",
                              source="enumeration", now_ts=20) is False
    tape.update_market_poll_state("0xC", tier="hot", hot_until_ts=999,
                                  last_polled_ts=20, newest_fill_ts=15, last_new_fills=3)
    m = tape.markets()[0]
    assert m["slug"] == "s2" and m["tier"] == "hot" and m["newest_fill_ts"] == 15
    # newest_fill_ts is monotonic (MAX), never regresses.
    tape.update_market_poll_state("0xC", tier="quiet", hot_until_ts=0,
                                  last_polled_ts=30, newest_fill_ts=5, last_new_fills=0)
    assert tape.markets()[0]["newest_fill_ts"] == 15


def test_stray_lifecycle(tape):
    tape.record_stray("0xS", now_ts=1, fills=2)
    tape.record_stray("0xS", now_ts=2, fills=3)
    assert tape.unresolved_strays() == ["0xS"]
    tape.resolve_stray("0xS")
    assert tape.unresolved_strays() == []


def test_poll_ledger_roundtrip(tape):
    pid = tape.open_poll(invoked_ts=100, lane="market", condition_id="0xC")
    tape.close_poll(pid, pages=2, raw_records=2000, new_records=15,
                    overlap_found=1, gap_declared=0)
    row = tape._conn.execute("SELECT pages, new_records, overlap_found FROM l2_polls"
                             " WHERE id=?", (pid,)).fetchone()
    assert row == (2, 15, 1)
    with pytest.raises(Exception):
        tape.close_poll(pid, nonsense_field=1)
