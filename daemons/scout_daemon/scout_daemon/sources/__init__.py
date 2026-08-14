"""Per-source adapters.

Each adapter is a leaf module: TOTAL over valid inputs. It returns whatever it
could extract and never raises for a merely-unexpected payload shape -- a
missing field yields `None`, not an exception. The orchestrator owns failure
cases, which is what lets one malformed source degrade to `error` without
taking the scan down.

Registration is explicit rather than discovered by import-scanning, so the set
of surfaces this daemon reads is a list a human can audit in one screen. That
matters more here than in most daemons: this list IS the containment boundary's
outer edge.
"""

from __future__ import annotations

from .base import Adapter, AdapterResult
from . import graphql_sources, html_sources, json_api, whitehat

ADAPTERS: dict[str, Adapter] = {}


def _register(*adapters: Adapter) -> None:
    for adapter in adapters:
        if adapter.source_name in ADAPTERS:
            raise RuntimeError(f"duplicate adapter for {adapter.source_name}")
        ADAPTERS[adapter.source_name] = adapter


_register(
    # --- work / agent-native, JSON APIs -----------------------------------
    json_api.SuperteamEarnAdapter(),
    json_api.OpireAdapter(),
    json_api.ZindiAdapter(),
    json_api.DealworkAdapter(),
    # --- white-hat (YELLOW-per-program regime, SC-1 §B) -------------------
    whitehat.SherlockAdapter(),
    whitehat.YesWeHackAdapter(),
    # --- GraphQL ----------------------------------------------------------
    graphql_sources.DeworkAdapter(),
    graphql_sources.GivethAdapter(),
    graphql_sources.QuestbookAdapter(),
    # --- server-rendered HTML --------------------------------------------
    html_sources.OpenTaskAdapter(),
    html_sources.EfEspAdapter(),
    html_sources.ArbitrumGrantsAdapter(),
    html_sources.AffiliateWatchAdapter(),
    html_sources.AffPayingAdapter(),
)

__all__ = ["Adapter", "AdapterResult", "ADAPTERS"]
