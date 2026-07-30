"""Overlay config loader (SM-4 STEP 2). Mando-owned config/overlay.yaml;
the daemon reads, never writes. Unknown tickers simply do not flag."""
import os

import yaml

DEFAULT_PATH = os.path.join(os.path.dirname(__file__), "..", "config", "overlay.yaml")


class Overlay:
    def __init__(self, conviction, watchlist, min_persons, window_days,
                 trump_network=None, thiel_network=None):
        self.conviction = set(conviction)
        self.watchlist = set(watchlist)
        self.trump_network = set(trump_network or [])
        self.thiel_network = set(thiel_network or [])
        self.min_persons = min_persons
        self.window_days = window_days

    def match(self, ticker):
        """Return (conviction_bool, watchlist_bool) — exact match only."""
        if not ticker:
            return False, False
        t = ticker.upper()
        return t in self.conviction, t in self.watchlist

    def scoped(self):
        """Every ticker in any overlay set — the /trades watchlist scope."""
        return self.conviction | self.watchlist | self.trump_network | self.thiel_network

    def provenance(self, ticker):
        """Why an issuer is in scope: book | watch | trump | thiel | None.
        Priority book > watch > trump > thiel when a ticker is on more than one."""
        if not ticker:
            return None
        t = ticker.upper()
        if t in self.conviction:
            return "book"
        if t in self.watchlist:
            return "watch"
        if t in self.trump_network:
            return "trump"
        if t in self.thiel_network:
            return "thiel"
        return None


def load_overlay(path=None) -> Overlay:
    path = path or DEFAULT_PATH
    with open(path) as f:
        doc = yaml.safe_load(f) or {}
    cl = doc.get("cluster") or {}
    return Overlay(
        conviction=[t.upper() for t in (doc.get("conviction_book") or [])],
        watchlist=[t.upper() for t in (doc.get("watchlist") or [])],
        trump_network=[t.upper() for t in (doc.get("trump_network") or [])],
        thiel_network=[t.upper() for t in (doc.get("thiel_network") or [])],
        min_persons=int(cl.get("min_persons", 3)),
        window_days=int(cl.get("window_days", 30)),
    )
