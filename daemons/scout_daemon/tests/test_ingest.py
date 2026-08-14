"""Phase 1 invariants: watermark discipline, identity, and adapter totality.

The watermark tests are the important ones. Getting that logic wrong produces
no error, no gap, and no null -- just a permanently missing slice of history.
It is exactly the class of bug that only a test catches.
"""

from __future__ import annotations

import pytest

from scout_daemon import state
from abelard_common.dedupe import compute_dedupe_hash, normalize_text
from scout_daemon.identity import compute_opportunity_id
from scout_daemon.models import RawItem
from scout_daemon.sources import ADAPTERS
from scout_daemon.sources.base import AdapterResult, iso_to_unix, parse_amount, parse_currency


@pytest.fixture()
def conn(tmp_path):
    connection = state.connect(tmp_path / "t.sqlite3")
    yield connection
    connection.close()


# --------------------------------------------------------------------------
# Watermark discipline -- the footgun
# --------------------------------------------------------------------------

def test_watermark_advances_to_newest_item_not_now(conn) -> None:
    now = 1_800_000_000
    newest_item = now - 86_400          # a day old
    state.record_source_result(
        conn, "s", now_unix=now, status="ok", item_count=3,
        ingested_high_watermark_unix=newest_item,
    )
    health = state.get_health(conn, "s")
    assert health.last_successful_fetch_unix == newest_item
    assert health.last_successful_fetch_unix != now, (
        "advancing to `now` skips everything published between the newest "
        "ingested item and the poll -- silently and permanently"
    )


def test_watermark_holds_on_empty(conn) -> None:
    now = 1_800_000_000
    state.record_source_result(
        conn, "s", now_unix=now, status="ok", item_count=2,
        ingested_high_watermark_unix=now - 500,
    )
    state.record_source_result(
        conn, "s", now_unix=now + 3600, status="empty", item_count=0,
        ingested_high_watermark_unix=None,
    )
    assert state.get_health(conn, "s").last_successful_fetch_unix == now - 500


def test_watermark_holds_on_error_and_counts_failures(conn) -> None:
    now = 1_800_000_000
    state.record_source_result(
        conn, "s", now_unix=now, status="ok", item_count=1,
        ingested_high_watermark_unix=now - 100,
    )
    for step in range(1, 4):
        state.record_source_result(
            conn, "s", now_unix=now + step, status="error", item_count=0,
            ingested_high_watermark_unix=None, detail="boom",
        )
    health = state.get_health(conn, "s")
    assert health.last_successful_fetch_unix == now - 100
    assert health.consecutive_failures == 3


def test_failure_counter_resets_on_recovery(conn) -> None:
    now = 1_800_000_000
    state.record_source_result(
        conn, "s", now_unix=now, status="error", item_count=0,
        ingested_high_watermark_unix=None,
    )
    state.record_source_result(
        conn, "s", now_unix=now + 1, status="ok", item_count=1,
        ingested_high_watermark_unix=now,
    )
    assert state.get_health(conn, "s").consecutive_failures == 0


def test_high_watermark_ignores_deadlines(conn) -> None:
    """Deadlines are in the FUTURE; using one would leap the watermark ahead.

    Same failure as advancing to `now`, wearing a different hat.
    """
    now = 1_800_000_000
    result = AdapterResult(
        source="s",
        status="ok",
        items=[
            RawItem(source="s", native_id="1", title="a",
                    posted_unix=now - 10, deadline_unix=now + 999_999),
            RawItem(source="s", native_id="2", title="b",
                    posted_unix=now - 5, deadline_unix=now + 999_999),
        ],
    )
    assert result.high_watermark_unix() == now - 5


def test_first_run_uses_bounded_lookback(conn) -> None:
    now = 1_800_000_000
    since = state.since_unix_for_source(conn, "never-seen", 90 * 86_400, now)
    assert since == now - 90 * 86_400


# --------------------------------------------------------------------------
# Identity
# --------------------------------------------------------------------------

def test_identity_keys_on_source_and_native_id_not_title() -> None:
    """SC-R1 sampled a Dework task and an Opire reward both titled 'c1work'."""
    a = compute_opportunity_id("dework", "abc")
    b = compute_opportunity_id("opire", "abc")
    assert a != b
    assert a == compute_opportunity_id("dework", "abc")


def test_identity_survives_a_cosmetic_retitle() -> None:
    before = compute_opportunity_id("superteam_earn", "listing-42")
    after = compute_opportunity_id("superteam_earn", "listing-42")
    assert before == after


def test_dedupe_hash_collapses_cosmetic_variants() -> None:
    assert compute_dedupe_hash("Write a Blog Post!") == compute_dedupe_hash(
        "  write a   blog post  "
    )
    assert normalize_text(None) == ""
    assert compute_dedupe_hash("a") != compute_dedupe_hash("b")


# --------------------------------------------------------------------------
# Parsing helpers -- conservative by design
# --------------------------------------------------------------------------

@pytest.mark.parametrize(
    "text,expected",
    [
        ("$25 000 USD", 25000.0),
        ("1000 CHF", 1000.0),
        ("€2 000 EUR", 2000.0),
        ("Up To $1,850 Per CPA lead", 1850.0),
        ("no numbers here", None),
        (None, None),
    ],
)
def test_parse_amount(text, expected) -> None:
    assert parse_amount(text) == expected


@pytest.mark.parametrize(
    "text,expected",
    [("1000 CHF", "CHF"), ("$50", "USD"), ("€2 000 EUR", "EUR"), ("5000 USDC", "USDC")],
)
def test_parse_currency(text, expected) -> None:
    assert parse_currency(text) == expected


def test_iso_to_unix_is_none_on_junk() -> None:
    assert iso_to_unix("2026-08-23T16:59:59.000Z") == 1787504399
    assert iso_to_unix("not a date") is None
    assert iso_to_unix(None) is None


# --------------------------------------------------------------------------
# Adapter contract
# --------------------------------------------------------------------------

def test_every_roster_source_has_exactly_one_adapter() -> None:
    from scout_daemon import config

    for source in config.WIRE_SOURCES:
        assert source.name in ADAPTERS, f"no adapter for {source.name}"
    assert len(ADAPTERS) == len(config.WIRE_SOURCES)


def test_adapters_are_total_over_a_junk_payload() -> None:
    """A leaf module returns what it got; it does not raise on a weird shape."""

    class _JunkClient:
        def get_json(self, url, params=None):
            return {"unexpected": "shape"}

        def post_json(self, url, json_body=None):
            return {"data": {"nothing": "here"}}

        def get_text(self, url):
            return "<html><body>nothing useful</body></html>"

    client = _JunkClient()
    for name, adapter in ADAPTERS.items():
        result = adapter.fetch(client, now_unix=1_800_000_000, since_unix=0)
        assert result.status in {"ok", "empty", "error"}, name
        assert result.items == [] or all(
            isinstance(i, RawItem) for i in result.items
        ), name


def test_field_fit_matches_the_recon_gate() -> None:
    """title + payout + category -- the same three fields SC-R1 measured."""
    complete = RawItem(
        source="s", native_id="1", title="t", payout_raw="$5", category="c"
    )
    assert complete.field_fit() is True
    assert RawItem(source="s", native_id="1", title="t").field_fit() is False
