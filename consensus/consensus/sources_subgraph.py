"""L1 archival tape — Goldsky orderbook subgraph fetchers (read-only GraphQL).

This is the deep-history layer of the three-layer model (addendum v1.2 §1):
on-chain ``orderFilledEvent`` records from Nov 2022 to the ~Apr 28 2026
exchange-contract migration, where this deployment froze with a complete tape.
It is the M0-F / M0-C replay substrate.

Discipline (same as every fetcher):
  - Every page response is cached verbatim keyed by its exact GraphQL query,
    so an ``as_of`` replay reproduces the exact walk (deep-slice
    reproducibility — the build-order gate for this module).
  - Cursor pagination via ``id_gt`` ordered by ``id`` ascending — The Graph's
    canonical exhaustive walk; unlike ``skip``, it is unbounded. ``first`` is
    capped at 1000 by the server.
  - Walks return provenance (head block, page count, cursor span, indexer
    coverage bounds) alongside the events: a query spanning layers must report
    per-layer provenance (Rule 1 applied to time).

Verified live 2026-07-13: the deployment supports the ``or`` where-operator
and ``id_gt`` cursoring; ``_meta.block.number`` is the indexing head (frozen
at 87,814,766 / newest event 2026-04-28).
"""

from __future__ import annotations

from typing import Any

from .fetching import DataLayer
from .models import OrderFilledEvent

_SOURCE = "goldsky_subgraph"

_EVENT_FIELDS = (
    "id timestamp maker taker makerAssetId takerAssetId "
    "makerAmountFilled takerAmountFilled fee"
)


def get_subgraph_meta(dl: DataLayer) -> dict[str, Any]:
    """Indexing head + coverage bounds — recorded as walk provenance and used
    to state L1's coverage boundary explicitly in downstream reports."""
    data = dl.fetch_graphql(
        source=_SOURCE,
        url=dl.endpoints.goldsky_subgraph,
        query=(
            "{ _meta { block { number } hasIndexingErrors } "
            "newest: orderFilledEvents(first: 1, orderBy: timestamp, orderDirection: desc) { timestamp } "
            "oldest: orderFilledEvents(first: 1, orderBy: timestamp, orderDirection: asc) { timestamp } }"
        ),
    )
    meta = data.get("_meta") or {}
    newest = data.get("newest") or []
    oldest = data.get("oldest") or []
    return {
        "head_block": (meta.get("block") or {}).get("number"),
        "has_indexing_errors": meta.get("hasIndexingErrors"),
        "newest_event_ts": int(newest[0]["timestamp"]) if newest else None,
        "oldest_event_ts": int(oldest[0]["timestamp"]) if oldest else None,
    }


def _where_clause(
    *,
    asset_ids: list[str] | None,
    ts_gte: int | None,
    ts_lt: int | None,
    id_gt: str | None,
) -> str:
    """Graph-node forbids mixing column filters with ``or`` at the same level
    (verified live 2026-07-13: 'Cannot mix column filters with or operator'),
    so the shared filters are replicated INTO every ``or`` branch."""
    parts: list[str] = []
    if ts_gte is not None:
        parts.append(f'timestamp_gte: "{ts_gte}"')
    if ts_lt is not None:
        parts.append(f'timestamp_lt: "{ts_lt}"')
    if id_gt is not None:
        parts.append(f'id_gt: "{id_gt}"')
    base = ", ".join(parts)
    if asset_ids:
        branches: list[str] = []
        for a in asset_ids:
            for side in ("makerAssetId", "takerAssetId"):
                fields = f'{side}: "{a}"' + (f", {base}" if base else "")
                branches.append(f"{{ {fields} }}")
        return f"{{ or: [{', '.join(branches)}] }}"
    return f"{{ {base} }}" if base else "{}"


def get_order_filled_events(
    dl: DataLayer,
    *,
    asset_ids: list[str] | None = None,
    ts_gte: int | None = None,
    ts_lt: int | None = None,
    id_gt: str | None = None,
    first: int = 1000,
) -> list[OrderFilledEvent]:
    """One page of fill events, ordered by id ascending (cursor-stable)."""
    where = _where_clause(asset_ids=asset_ids, ts_gte=ts_gte, ts_lt=ts_lt, id_gt=id_gt)
    query = (
        f"{{ orderFilledEvents(first: {first}, orderBy: id, orderDirection: asc, "
        f"where: {where}) {{ {_EVENT_FIELDS} }} }}"
    )
    data = dl.fetch_graphql(source=_SOURCE, url=dl.endpoints.goldsky_subgraph, query=query)
    raw = data.get("orderFilledEvents")
    return dl.parse_records(
        raw, parser=OrderFilledEvent.from_api, source=_SOURCE, endpoint="/orderFilledEvents"
    )


def paginate_order_filled(
    dl: DataLayer,
    *,
    asset_ids: list[str] | None = None,
    ts_gte: int | None = None,
    ts_lt: int | None = None,
    page_size: int = 1000,
    max_records: int | None = None,
) -> tuple[list[OrderFilledEvent], dict[str, Any]]:
    """Exhaustive ``id_gt`` cursor walk over a slice of the L1 tape.

    Returns ``(events, provenance)``. Provenance records the indexing head,
    the walk's cursor span and page count, and whether the walk was truncated
    by ``max_records`` — never silently capped.
    """
    meta = get_subgraph_meta(dl)
    events: list[OrderFilledEvent] = []
    cursor: str | None = None
    pages = 0
    truncated = False
    while True:
        page = get_order_filled_events(
            dl, asset_ids=asset_ids, ts_gte=ts_gte, ts_lt=ts_lt,
            id_gt=cursor, first=page_size,
        )
        pages += 1
        events.extend(page)
        if max_records is not None and len(events) >= max_records:
            del events[max_records:]
            truncated = True
            break
        if len(page) < page_size:
            break
        cursor = page[-1].event_id
    provenance = {
        "source": _SOURCE,
        "layer": "L1",
        "head_block": meta["head_block"],
        "coverage_newest_ts": meta["newest_event_ts"],
        "coverage_oldest_ts": meta["oldest_event_ts"],
        "pages": pages,
        "events": len(events),
        "first_id": events[0].event_id if events else None,
        "last_id": events[-1].event_id if events else None,
        "truncated_by_max_records": truncated,
        "filter": {"asset_ids": asset_ids, "ts_gte": ts_gte, "ts_lt": ts_lt},
    }
    return events, provenance
