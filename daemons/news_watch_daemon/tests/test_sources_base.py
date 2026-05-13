"""Source plugin interface smoke tests — confirm the contract holds."""

from __future__ import annotations

import pytest

import typing

from news_watch_daemon.sources.base import (
    FetchedItem,
    FetchResult,
    FetchStatus,
    SourcePlugin,
)


def test_fetch_status_is_literal():
    """FetchStatus is a closed enum, not a free-form str.

    Tightened in Pass A (was `str` in foundation pass).
    """
    args = typing.get_args(FetchStatus)
    assert set(args) == {"ok", "rate_limited", "error", "partial"}


def test_sourceplugin_is_abstract():
    with pytest.raises(TypeError):
        SourcePlugin()  # type: ignore[abstract]


def test_partial_subclass_is_still_abstract():
    class Partial(SourcePlugin):
        @property
        def name(self) -> str:
            return "partial"

        # Missing fetch() and rate_limit_budget_remaining().

    with pytest.raises(TypeError):
        Partial()  # type: ignore[abstract]


def test_full_subclass_is_instantiable():
    class Dummy(SourcePlugin):
        @property
        def name(self) -> str:
            return "dummy"

        def fetch(self, since_unix: int) -> FetchResult:
            return FetchResult(source=self.name, fetched_at_unix=0, items=[], status="ok")

        def rate_limit_budget_remaining(self) -> float:
            return 1.0

    plugin = Dummy()
    result = plugin.fetch(0)
    assert result.source == "dummy"
    assert result.status == "ok"
    assert result.items == []
    assert plugin.rate_limit_budget_remaining() == 1.0
    # cadence_minutes default is None — plugin runs every cycle.
    assert plugin.cadence_minutes is None


def test_subclass_can_override_cadence_minutes():
    class Cadenced(SourcePlugin):
        @property
        def name(self) -> str:
            return "cadenced"

        def fetch(self, since_unix: int) -> FetchResult:
            return FetchResult(source=self.name, fetched_at_unix=0, items=[], status="ok")

        def rate_limit_budget_remaining(self) -> float:
            return 1.0

        @property
        def cadence_minutes(self) -> int | None:
            return 30

    assert Cadenced().cadence_minutes == 30


def test_fetched_item_is_frozen():
    item = FetchedItem(
        source_item_id="x",
        headline="h",
        url=None,
        published_at_unix=0,
        raw_source=None,
    )
    with pytest.raises(Exception):
        item.headline = "changed"  # type: ignore[misc]
    # Defaults
    assert item.tickers == []
    assert item.raw_body is None


def test_fetch_result_is_frozen():
    result = FetchResult(source="s", fetched_at_unix=0, items=[], status="ok")
    with pytest.raises(Exception):
        result.status = "error"  # type: ignore[misc]
    assert result.error_detail is None
