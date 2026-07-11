"""Polygon chain data via the Etherscan V2 unified API (read-only) — feeds the
M5 funding graph.

ERC-20 transfer history (USDC.e / pUSD) is how M5 links proxy wallets into
actors. Provider facts (verified live 2026-07-10):

  - Standalone Polygonscan V1 (``api.polygonscan.com``) was sunset 2025-08-15
    and now 301-redirects to the migration docs. It cannot work with any key.
  - The replacement is Etherscan V2: ``https://api.etherscan.io/v2/api`` with a
    REQUIRED ``chainid=137`` param and a single unified **Etherscan** key
    (``ETHERSCAN_API_KEY`` in .env; register at etherscan.io — free tier:
    3 calls/sec, 100k calls/day, Polygon included).
  - Free tier caps ``tokentx`` at 1,000 records per request (as of 2026-07-01);
    paginate with ``page``+``offset<=1000`` or block-windowing.

The response schema (``{status, message, result}``) is unchanged from V1, so
parsing is untouched. When no key is configured a LIVE fetch fails loudly
rather than returning fabricated or partial data (Rule 1); replay mode never
needs the key (the cache key excludes it).

The API key is a secret: it goes on the wire but is deliberately kept out of
the cache key, and every error message passes through ``DataLayer._scrub``
(query-pattern redaction + known-secret replacement) before it can reach logs,
stdout, or a ``--json`` report.
"""

from __future__ import annotations

from typing import Any

from .errors import DataLayerError
from .fetching import DataLayer
from .models import Erc20Transfer

_SOURCE = "etherscan_polygon"

# Polygon PoS chain id — a protocol constant (address-space identifier), not a
# tunable algorithm parameter, hence not in config.yaml.
_POLYGON_CHAIN_ID = 137

# Etherscan V2 is a single endpoint dispatched by query params, so the URL path
# is empty and the cache key is formed from the params (module/action/address/
# chainid), minus the secret.
_ENDPOINT = ""


def get_erc20_transfers(
    dl: DataLayer,
    address: str,
    *,
    contract_address: str | None = None,
    startblock: int = 0,
    endblock: int = 999_999_999,
    sort: str = "asc",
    page: int | None = None,
    offset: int | None = None,
) -> list[Erc20Transfer]:
    """ERC-20 transfers touching ``address`` on Polygon (chainid 137).

    Optionally restrict to one token contract. Free-tier note: at most 1,000
    records per request — walk with ``page``/``offset`` or block windows.
    Raises :class:`DataLayerError` if no API key is configured for a live
    fetch, or on any provider error status; returns ``[]`` for a genuine
    "no transactions" result.
    """
    api_key = dl.loaded.secrets.etherscan_api_key
    if not api_key and not dl.replay:
        # Replay mode never touches the wire — the cache key excludes the
        # secret, so cached funding data must stay servable without it
        # (deterministic offline replay). Only a LIVE fetch needs the key.
        raise DataLayerError(
            "ETHERSCAN_API_KEY is not set; refusing to fetch chain data "
            "(Rule 1: no fabricated or partial funding data). Register a free "
            "key at etherscan.io and set it in .env.",
            source=_SOURCE,
        )

    request_params: dict[str, Any] = {
        "chainid": _POLYGON_CHAIN_ID,
        "module": "account",
        "action": "tokentx",
        "address": address,
        "startblock": startblock,
        "endblock": endblock,
        "sort": sort,
        "apikey": api_key,
    }
    if contract_address is not None:
        request_params["contractaddress"] = contract_address
    if page is not None:
        request_params["page"] = page
    if offset is not None:
        request_params["offset"] = offset

    # Cache key: identical, minus the secret.
    cache_params = {k: v for k, v in request_params.items() if k != "apikey"}

    body = dl.fetch(
        source=_SOURCE,
        base_url=dl.endpoints.etherscan_v2_api,
        endpoint=_ENDPOINT,
        request_params=request_params,
        cache_params=cache_params,
    )

    if not isinstance(body, dict):
        raise DataLayerError(
            f"{_SOURCE}: expected a JSON object, got {type(body).__name__}", source=_SOURCE
        )

    status = str(body.get("status", ""))
    message = str(body.get("message", ""))
    result = body.get("result")

    if status == "1" and isinstance(result, list):
        return dl.parse_records(
            result, parser=Erc20Transfer.from_api, source=_SOURCE, endpoint="/tokentx"
        )

    # Etherscan signals "nothing found" as status 0 with an empty list — a
    # legitimate empty result, not a failure.
    if status == "0" and isinstance(result, list) and (not result or "No transactions found" in message):
        return []

    # Anything else (rate limit, bad key, malformed) is a loud failure. Note
    # ``result`` can be an error *string* here; surface it (scrubbed upstream).
    raise DataLayerError(
        f"{_SOURCE} error: status={status!r} message={message!r} result={result!r}",
        source=_SOURCE,
    )
