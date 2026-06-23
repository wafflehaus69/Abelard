"""StockTwits source (Order 9) — public API, no key, browser UA.

Two public endpoints back two jobs; this module is the transport + parse layer for
both. NO credentials — StockTwits' public API needs none; a browser User-Agent is the
only requirement.

  - trending  (DISCOVERY, Phase B): GET /api/2/trending/symbols.json
      -> 30 pre-ranked symbol objects. The ranking IS the gate (top-30), so there is
      no mention-floor and no universe-validation: the symbols are cashtag-native and
      exchange-validated at the source. `trending_score`/`rank` are the momentum axis;
      `watchlist_count` is context (too stable to gate on). `fundamentals` and
      `trends.summary` are nullable — guard every access.
  - symbol stream (SENTIMENT, Phase C): GET /api/2/streams/symbol/{T}.json
      -> 30 messages/ticker, each a free-text body + an optional native Bull/Bear tag.

DEGRADE-CLEAN (non-negotiable). CloudFlare can wall any call with a 200 + HTML
challenge instead of JSON. Every method is best-effort: a CF wall / non-JSON /
transport / rate-limit failure raises `StockTwitsBlocked`, which the caller turns into
a soft per-surface (trending) or per-ticker (stream) failure and sets `degraded`. The
daemon never crashes on StockTwits and never fabricates — /smg/, Finnhub, and Trends
carry the scan when StockTwits is dark.
"""

from __future__ import annotations

import logging
from typing import Any

from abelard_common.http_client import HttpClient

API_BASE = "https://api.stocktwits.com/api/2"
TRENDING_URL = f"{API_BASE}/trending/symbols.json"
STREAM_URL = API_BASE + "/streams/symbol/{symbol}.json"

# The public endpoints answer a browser UA, not the daemon's default. A real Chrome
# string keeps us off the trivial-bot path (CF can still wall us — see degrade-clean).
BROWSER_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)


class StockTwitsBlocked(RuntimeError):
    """A soft, degrade-clean failure: CF wall (200 + HTML), non-JSON, or transport.

    Distinct from a crash — the caller logs it, marks the surface / ticker failed, sets
    `degraded`, and carries on. Never raised past a pull/stream boundary.
    """


class StockTwitsClient:
    """Public StockTwits client — no key, browser UA, injectable transport for tests.

    `trending()` powers ATTENTION discovery (Phase B); `symbol_stream()` powers the
    sentiment blend (Phase C). Both raise `StockTwitsBlocked` on any soft failure.
    """

    def __init__(
        self,
        *,
        user_agent: str = BROWSER_UA,
        client: HttpClient | None = None,
        logger: logging.Logger | None = None,
    ) -> None:
        self._log = logger or logging.getLogger("chatter_daemon.stocktwits")
        # Browser-ish Accept alongside the UA; the shared client forces UTF-8 decode.
        self._client = client or HttpClient(
            user_agent=user_agent,
            default_headers={"Accept": "application/json"},
            logger=self._log,
        )

    def trending(self) -> list[dict[str, Any]]:
        """The 30 trending symbol objects (DISCOVERY). Raises StockTwitsBlocked on a CF
        wall / non-JSON / transport failure — a 200 + HTML challenge makes `.json()`
        raise, which we map to a soft block here (never a crash)."""
        data = self._get_json(TRENDING_URL, what="trending")
        symbols = data.get("symbols") if isinstance(data, dict) else None
        if not isinstance(symbols, list) or not symbols:
            raise StockTwitsBlocked(
                "trending response was not JSON-with-symbols (CF challenge?)"
            )
        return [s for s in symbols if isinstance(s, dict)]

    def _get_json(self, url: str, *, what: str) -> Any:
        """GET + parse, mapping every failure to StockTwitsBlocked. The shared client
        raises on transport / 404 / 429; `.json()` raises on a non-JSON CF wall. We
        treat all of them as one soft, degrade-clean signal."""
        try:
            return self._client.get_json(url)
        except Exception as exc:  # transport, rate-limit, 404, or non-JSON (CF HTML)
            raise StockTwitsBlocked(
                f"{what} unavailable (CF wall or transport): {exc}"
            ) from exc


__all__ = ["BROWSER_UA", "StockTwitsBlocked", "StockTwitsClient", "STREAM_URL", "TRENDING_URL"]
