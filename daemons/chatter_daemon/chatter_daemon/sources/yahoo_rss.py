"""Yahoo Finance per-ticker RSS source (CH-SRC-1) — the freshest per-ticker headline pulse.

Per active watchlist ticker, fetch ``finance.yahoo.com/rss/headline?s=<SYM>`` (keyless RSS) and
keep the items that actually NAME the ticker (title+description, via the shared relevance
matcher). Yahoo's ``?s=`` feed is a loosely-curated MARKET feed — recon 2026-07-15 measured only
~3-9 of 20 items per ticker as genuinely on-ticker — so the relevance filter is mandatory, and
what survives is a small but very fresh (~0.2-1h) headline set.

DEDUP vs Finnhub (CH-SRC-1 "no duplicated efforts"): Finnhub company-news runs earlier in the
same scan and arrives via ``prior_records``. A Yahoo headline whose normalized title already
appears in Finnhub's heads for that ticker is DROPPED — so Yahoo emits only the fresh, net-new
headlines Finnhub doesn't have yet, its whole value being the latency edge.

FAIL MODES: a per-ticker fetch/parse error is isolated (warn, skip that ticker). Yahoo deprecates
SILENTLY (HTTP 200 + a stale/empty feed), so a source-level freshness assertion flags it — zero
items scan-wide (or every ticker blocked) -> source error; a freshest item older than
``stale_after_h`` -> a loud warning. ``method="none"`` (headlines carry no stance); count-source
in the aggregate; keeps Chatter's locked per-ticker architecture.
"""

from __future__ import annotations

import logging
import re
from datetime import timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from xml.etree import ElementTree as ET

from abelard_common.company_aliases import load_name_map
from abelard_common.http_client import HttpClient, NotFound, RateLimited, TransportError

from ..config import (
    DEFAULT_YAHOO_MAX_ITEMS,
    DEFAULT_YAHOO_ROUNDUP_MAX,
    DEFAULT_YAHOO_STALE_AFTER_H,
)
from ..matching import count_named_tickers, title_mentions_ticker, watchlist_alias_map
from ..schema import Headline, Metrics, NormalizedRecord, Sentiment
from ..watchlist import WatchlistConfig
from .base import ScanContext, SourceResult

SOURCE_NAME = "yahoo_rss"
WINDOW_LABEL = "24h"
_YAHOO_RSS = "https://finance.yahoo.com/rss/headline"
# Yahoo serves the RSS to a browser UA (recon: 200 XML with a Chrome UA); a bot UA can be walled.
_BROWSER_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)
_NONWORD_RE = re.compile(r"[^a-z0-9]+")


def _norm_title(title: str) -> str:
    """Normalized title key for the dedup vs Finnhub — lowercase, punctuation-stripped, ws-collapsed."""
    return " ".join(_NONWORD_RE.sub(" ", title.lower()).split())


def _parse_items(xml_text: str) -> list[dict]:
    """Parse RSS <item>s -> {title, url, desc, pub_unix}. Encodes to bytes so ET tolerates the
    XML encoding declaration. Malformed XML raises ElementTree.ParseError (caller isolates it)."""
    root = ET.fromstring(xml_text.encode("utf-8"))
    out: list[dict] = []
    for it in root.findall(".//item"):
        title = (it.findtext("title") or "").strip()
        url = (it.findtext("link") or "").strip()
        desc = (it.findtext("description") or "").strip()
        pub_unix: int | None = None
        try:
            dt = parsedate_to_datetime(it.findtext("pubDate") or "")
            if dt is not None:
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                pub_unix = int(dt.timestamp())
        except (TypeError, ValueError):
            pub_unix = None
        if title and url:
            out.append({"title": title, "url": url, "desc": desc, "pub_unix": pub_unix})
    return out


class YahooRssSource:
    """Yahoo per-ticker RSS — fresh, relevance-filtered, Finnhub-deduped headline supplement."""

    name = SOURCE_NAME

    def __init__(
        self,
        *,
        company_names_path: str | Path | None = None,
        max_items: int = DEFAULT_YAHOO_MAX_ITEMS,
        stale_after_h: int = DEFAULT_YAHOO_STALE_AFTER_H,
        roundup_max: int = DEFAULT_YAHOO_ROUNDUP_MAX,
        client: HttpClient | None = None,
        logger: logging.Logger | None = None,
    ) -> None:
        self._log = logger or logging.getLogger("chatter_daemon.yahoo_rss")
        # A BROWSER UA is required (recon); do not inherit the daemon's bot UA.
        self.client = client or HttpClient(user_agent=_BROWSER_UA, logger=self._log)
        self._max_items = max_items
        self._stale_after_h = stale_after_h
        self._roundup_max = roundup_max  # drop a title naming >= this many watchlist tickers (0=off)
        self._shared_map = load_name_map(Path(company_names_path)) if company_names_path else {}

    def fetch(
        self, watchlist: WatchlistConfig, *, context: ScanContext, prior_records=None
    ) -> SourceResult:
        window = context.windows[WINDOW_LABEL]
        aliases = self._aliases(watchlist)                    # {SYMBOL: [name words]} for the gate
        finnhub_titles = self._finnhub_titles(prior_records)  # {SYMBOL: {norm titles}} for dedup

        records: list[NormalizedRecord] = []
        warnings: list[str] = []
        raw_items: list[str] = []
        blocked: list[str] = []
        total_items = 0
        freshest_unix: int | None = None
        actives = watchlist.active_tickers

        for spec in actives:
            sym = spec.symbol
            try:
                xml_text = self.client.get_text(_YAHOO_RSS, params={"s": sym})
            except NotFound:
                items: list[dict] = []  # no feed for this symbol -> honest zero
            except (RateLimited, TransportError) as exc:
                self._log.warning("yahoo fetch failed for %s: %s", sym, exc)
                blocked.append(sym)
                continue
            else:
                try:
                    items = _parse_items(xml_text)
                except ET.ParseError as exc:
                    self._log.warning("yahoo XML parse failed for %s: %s", sym, exc)
                    blocked.append(sym)
                    continue

            total_items += len(items)
            for it in items:
                if it["pub_unix"] is not None:
                    freshest_unix = (
                        it["pub_unix"] if freshest_unix is None
                        else max(freshest_unix, it["pub_unix"])
                    )

            heads = self._relevant_new_heads(sym, aliases.get(sym, ()), items, finnhub_titles, aliases)
            raw_items.extend(f"{sym}\t{h.title}" for h in heads)
            records.append(
                NormalizedRecord(
                    watchlist=watchlist.name,
                    scan_mode=context.scan_mode,
                    canonical_ts=context.canonical_ts,
                    window=window,
                    source=SOURCE_NAME,
                    ticker=sym,
                    matched_by=["symbol"],
                    metrics=Metrics(mention_count=len(heads), headlines=heads),
                    sentiment=Sentiment(method="none"),
                    flags=[],
                )
            )

        error = self._freshness(
            total_items, freshest_unix, context.canonical_unix, blocked, len(actives), warnings
        )
        return SourceResult(
            source=SOURCE_NAME, records=records, warnings=warnings, error=error, raw_items=raw_items
        )

    def _relevant_new_heads(self, sym, names, items, finnhub_titles, alias_map) -> list[Headline]:
        """Keep items whose TITLE names the ticker (not the blurb — matching the description
        over-attributes a single-name article to every ticker its blurb lists, the main
        cross-ticker duplicate source) AND aren't already in Finnhub's heads. A market roundup
        (a title naming >= roundup_max watchlist tickers) is dropped as low per-ticker signal.
        Deduped within the feed too."""
        seen_fin = finnhub_titles.get(sym.upper(), set())
        heads: list[Headline] = []
        seen_local: set[str] = set()
        for it in items[: self._max_items]:
            title = it["title"]
            if not title_mentions_ticker(title, sym, names):
                continue  # this ticker not named IN THE TITLE (Yahoo's ?s= feed is mixed)
            if self._roundup_max and count_named_tickers(title, alias_map) >= self._roundup_max:
                continue  # a market roundup naming many tickers -> drop (the cross-ticker dup)
            key = _norm_title(title)
            if not key or key in seen_fin or key in seen_local:
                continue  # blank, already in Finnhub (dedup), or a within-feed duplicate
            seen_local.add(key)
            heads.append(Headline(title=title, url=it["url"]))
        return heads

    def _freshness(self, total_items, freshest_unix, now, blocked, n_active, warnings) -> str | None:
        """Yahoo deprecates silently (200 + stale/empty). Every ticker blocked, or zero items
        scan-wide -> a source error (degrades the scan). A freshest item older than the threshold
        -> a loud staleness warning. Some-but-not-all blocked -> a warning. Healthy -> None."""
        if n_active and blocked and len(blocked) == n_active:
            return f"{len(blocked)}/{n_active} Yahoo unavailable: {', '.join(blocked)}"
        if total_items == 0:
            return "Yahoo returned zero items across the scan (feed deprecated?)"
        if freshest_unix is not None:
            age_h = (now - freshest_unix) / 3600
            if age_h > self._stale_after_h:
                warnings.append(
                    f"yahoo: freshest item {age_h:.0f}h old (> {self._stale_after_h}h) — stale/deprecated?"
                )
        if blocked:
            warnings.append(f"yahoo: {len(blocked)} tickers unavailable: {', '.join(blocked)}")
        return None

    def _finnhub_titles(self, prior_records) -> dict[str, set[str]]:
        """{SYMBOL: {normalized Finnhub headline titles}} from this run's earlier Finnhub source
        — the dedup set so Yahoo emits only net-new headlines. Empty if Finnhub didn't run."""
        out: dict[str, set[str]] = {}
        for r in prior_records or []:
            if getattr(r, "source", None) != "finnhub_news":
                continue
            heads = getattr(r.metrics, "headlines", None) or []
            out.setdefault(r.ticker.upper(), set()).update(_norm_title(h.title) for h in heads)
        return out

    def _aliases(self, watchlist: WatchlistConfig) -> dict[str, list[str]]:
        """`{SYMBOL: [lowercased aliases]}` for the relevance + roundup gates — the shared helper
        (S&P names + each ticker's own spec names; safe here since Yahoo's feed is ticker-scoped)."""
        return watchlist_alias_map(watchlist, self._shared_map)


__all__ = ["SOURCE_NAME", "YahooRssSource"]
