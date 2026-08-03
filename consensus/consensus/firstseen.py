"""Live wallet first-seen resolver — M10-D §3.1.

Factor F (freshness) needs each wallet's first Polymarket activity. Post-L1-freeze
(after 2026-04-28) the frozen subgraph cannot supply it for new wallets, so the live
dossier scan otherwise drops to S/D/C only. This restores F from live sources, gated
behind the fill-factor bar (only wallets that already clear S/D/C get resolved).

Sources ([VERIFY] 2026-07-27, both validated exact against the M0-F subgraph
first_seen ground truth):
  - PRIMARY: data-api ``/activity`` — earliest activity per wallet, keyless, and
    conceptually the M0-F definition ("first Polymarket trade"). Exact for any wallet
    whose history is under the data-api ~4k-record reach; ~1 call for a fresh wallet.
  - FALLBACK: Etherscan first USDC transfer (``sort=asc``) — always the earliest in a
    single call, no cap. Used ONLY when ``/activity`` caps. Needs ``ETHERSCAN_API_KEY``
    in the ENVIRONMENT (config reads os.environ, not .env — a deployment note).

Rule 1: a genuinely failed lookup declares F unavailable; it is NEVER imputed. A
``/activity`` result that hit the cap is NEVER returned as a first-seen — its (later)
oldest-reachable date would make an established wallet look fresh, inverting F. A
capped-without-fallback wallet is reported as ``established`` with a lower bound on age.
"""

from __future__ import annotations

import time
from dataclasses import dataclass

from .errors import DataLayerError
from .fetching import DataLayer
from .sources_polygon import get_erc20_transfers
from .sources_polymarket import get_wallet_activity

# data-api /activity practically reaches ~4k newest records (limit 500 x offset 3000+);
# 8 pages covers it. A wallet exceeding this is high-activity => established.
_ACTIVITY_PAGE = 500
_ACTIVITY_MAX_PAGES = 8


@dataclass(frozen=True)
class FirstSeen:
    """Resolved wallet first-seen. ``ts`` is the exact first-seen unix seconds when
    ``exact`` is True. When not exact the wallet is established but its precise birth
    was not resolvable here; ``min_age_days`` is a lower bound on its age (from the
    capped oldest-reachable activity), enough to place F at the low/established end."""

    wallet: str
    ts: int | None
    source: str  # "activity" | "etherscan" | "activity_capped" | "unavailable"
    exact: bool
    min_age_days: float | None = None
    detail: str | None = None
    # Activities strictly before ``before_ts``, MEASURED during the same walk. None
    # when not measurable — the caller must not substitute 0, which reads as "brand
    # new wallet" and silently disables m0f's prior-fills freshness discount.
    prior_fills: int | None = None

    @property
    def available(self) -> bool:
        return self.exact and self.ts is not None


def _activity_oldest(dl: DataLayer, wallet: str, before_ts: int | None = None
                     ) -> tuple[int | None, bool, str, int]:
    """Return (oldest_ts, reached_true_oldest, detail). ``reached_true_oldest`` is
    False if pagination hit the page cap (history exceeds the data-api reach).

    Termination is on an EMPTY page only — never on a short one. ``get_wallet_activity``
    returns PARSED records, so a page containing one unparseable record comes back short
    while still being a full page of history; stopping there would end the walk early and
    report a LATER first-seen, making an established wallet look fresh and INFLATING
    factor F (the same raw-vs-parsed rule ``paginate_market_trades`` documents). The cost
    is at most one extra request per wallet."""
    oldest: int | None = None
    prior = 0
    for page in range(_ACTIVITY_MAX_PAGES):
        acts = get_wallet_activity(
            dl, wallet, limit=_ACTIVITY_PAGE, offset=page * _ACTIVITY_PAGE
        )
        if not acts:
            return oldest, True, f"reached-oldest@{page}p", prior
        page_min = min((a.timestamp for a in acts if a.timestamp), default=None)
        if page_min is not None:
            oldest = page_min if oldest is None else min(oldest, page_min)
        if before_ts is not None:
            prior += sum(1 for a in acts if a.timestamp and a.timestamp < before_ts)
    # Capped: the count is a LOWER bound on prior activity, not a measurement.
    return oldest, False, f"capped@{_ACTIVITY_MAX_PAGES}p", prior


def _etherscan_first(dl: DataLayer, wallet: str) -> int | None:
    """Earliest USDC transfer touching the wallet (funding ~= activation), in one
    ascending-sorted call. None on a genuine no-transfers result."""
    tx = get_erc20_transfers(dl, wallet, sort="asc", page=1, offset=10)
    return min((t.timestamp for t in tx if getattr(t, "timestamp", None)), default=None)


def resolve_first_seen(dl: DataLayer, wallet: str, *, now: int | None = None,
                       before_ts: int | None = None) -> FirstSeen:
    """Resolve ``wallet``'s first-seen from live sources, cache-through. Never raises
    on a data-availability problem — returns a ``FirstSeen`` whose ``source`` records
    what happened, so the caller declares F unavailable/established rather than guessing.

    Only genuine transport breakage on BOTH sources yields ``source="unavailable"``.
    """
    now = now or int(time.time())
    # PRIMARY: /activity
    try:
        oldest, reached, detail, prior = _activity_oldest(dl, wallet, before_ts)
        if reached and oldest is not None:
            return FirstSeen(wallet, oldest, "activity", True, detail=detail,
                             prior_fills=(prior if before_ts is not None else None))
        capped_min_age = (now - oldest) / 86400 if oldest is not None else None
    except DataLayerError as e:
        oldest, capped_min_age, detail, prior = None, None, f"activity-err:{e}", None

    # FALLBACK: Etherscan first USDC transfer (exact, no cap) — only reached when
    # /activity capped or errored.
    try:
        eth = _etherscan_first(dl, wallet)
        if eth is not None:
            return FirstSeen(wallet, eth, "etherscan", True, detail=detail)
    except DataLayerError as e:
        detail = f"{detail}; etherscan-err:{e}"

    # Capped /activity with no usable fallback: established, precise birth unknown.
    if capped_min_age is not None:
        return FirstSeen(
            wallet, None, "activity_capped", False,
            min_age_days=capped_min_age, detail=detail,
        )
    # Both sources unusable: Rule 1 — F unavailable, declared, never imputed.
    return FirstSeen(wallet, None, "unavailable", False, detail=detail)
