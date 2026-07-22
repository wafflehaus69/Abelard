"""Overlay config loader (SM-4 STEP 2). Mando-owned config/overlay.yaml;
the daemon reads, never writes. Unknown tickers simply do not flag."""
import os

import yaml

DEFAULT_PATH = os.path.join(os.path.dirname(__file__), "..", "config", "overlay.yaml")


class Overlay:
    def __init__(self, conviction, watchlist, min_persons, window_days):
        self.conviction = set(conviction)
        self.watchlist = set(watchlist)
        self.min_persons = min_persons
        self.window_days = window_days

    def match(self, ticker):
        """Return (conviction_bool, watchlist_bool) — exact match only."""
        if not ticker:
            return False, False
        t = ticker.upper()
        return t in self.conviction, t in self.watchlist


def load_overlay(path=None) -> Overlay:
    path = path or DEFAULT_PATH
    with open(path) as f:
        doc = yaml.safe_load(f) or {}
    cl = doc.get("cluster") or {}
    return Overlay(
        conviction=[t.upper() for t in (doc.get("conviction_book") or [])],
        watchlist=[t.upper() for t in (doc.get("watchlist") or [])],
        min_persons=int(cl.get("min_persons", 3)),
        window_days=int(cl.get("window_days", 30)),
    )
