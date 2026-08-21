"""Server-rendered HTML surfaces.

These are the roster's fragile end and this module is written to fail HONESTLY
rather than to look productive. Every adapter here returns what it can actually
extract from a robots-PERMITTED path and reports `empty` with a diagnostic
`detail` string when a page's shape has moved. A parser that silently returns
zero items is indistinguishable from a dead source; one that says WHY it
returned zero is a finding.

ROBOTS DISCIPLINE IS A HARD CONSTRAINT HERE, not a preference:
  - EF ESP disallows `/api/`. This module reads only SSR pages, even though the
    API path would be far easier to parse. SC-R1's own sampled EF ESP items
    carry Salesforce-shaped keys that do NOT appear on any permitted page as of
    2026-08-10 -- so the permitted-path yield is honestly low, and that is
    reported rather than worked around.
  - Affiliate.Watch disallows query-string pagination (`/*?*page=`) and blocks
    named AI crawlers. Only the base document is read.

Neither of those is a bug to fix. Reading a disallowed path to raise a yield
number would trade a real compliance boundary for a cosmetic metric.
"""

from __future__ import annotations

import html as html_module
import json
import re
from typing import Any

from .. import config, models
from ..fetch import get_text
from .base import (AdapterResult, error_result, parse_amount, parse_currency,
                   parse_monetary, strip_html)

_NEXT_DATA_RE = re.compile(
    r'<script id="__NEXT_DATA__" type="application/json">(.*?)</script>', re.S
)
_DATA_PAGE_RE = re.compile(r'data-page="(.*?)"', re.S)


def _extract_next_data(text: str) -> dict | None:
    match = _NEXT_DATA_RE.search(text)
    if not match:
        return None
    try:
        return json.loads(match.group(1))
    except (ValueError, json.JSONDecodeError):
        return None


def _extract_inertia_page(text: str) -> dict | None:
    match = _DATA_PAGE_RE.search(text)
    if not match:
        return None
    try:
        return json.loads(html_module.unescape(match.group(1)))
    except (ValueError, json.JSONDecodeError):
        return None


def _find_records(node: Any, marker: str, depth: int = 0) -> list[dict]:
    """Depth-first hunt for a list of dicts containing `marker` as a key.

    Embedded-JSON blobs move their payload between releases; anchoring on a
    distinctive FIELD NAME survives a reshuffle that a fixed key path would not.
    """
    if depth > 8:
        return []
    if isinstance(node, list):
        if node and isinstance(node[0], dict) and marker in node[0]:
            return [n for n in node if isinstance(n, dict)]
        for child in node:
            found = _find_records(child, marker, depth + 1)
            if found:
                return found
    elif isinstance(node, dict):
        for child in node.values():
            found = _find_records(child, marker, depth + 1)
            if found:
                return found
    return []


class OpenTaskAdapter:
    """Agent-native task marketplace.

    ToS says: use documented APIs, do not scrape. The documented API needs an
    account, and invariant 5 forbids creating one -- so this reads the public
    SSR page only, which is the narrowest reading that stays inside both rules.

    Content arrives in React Server Component flight payloads
    (`self.__next_f.push([1,"..."])`), which is an internal serialization format
    with no compatibility promise. Treat any yield here as provisional.
    """

    source_name = "opentask"
    _FLIGHT_RE = re.compile(r'self\.__next_f\.push\(\[1,\s*"(.*?)"\]\)', re.S)
    # Observed shape (2026-08-10): the task link and its display title sit in
    # one node, and the title itself carries the price --
    #   "href":"/tasks/<id>","children":["Python Automation & Web Scraping - 25 USDC",
    # so id, title, and payout all come from a single match. Anchoring on the
    # href gives a stable native_id; anchoring on the price alone would not.
    _TASK_RE = re.compile(
        r'"href":"(/tasks/[A-Za-z0-9_-]+)"[^}]{0,400}?"children":\["([^"]{5,200}?)"',
        re.S,
    )
    _PRICE_RE = re.compile(r"(\d[\d,\.]*)\s*(USDC|USDT|USD)\b")

    def fetch(self, client: Any, *, now_unix: int, since_unix: int) -> AdapterResult:
        url = config.SOURCES_BY_NAME[self.source_name].base_url
        result = get_text(client, url)
        if result.is_error:
            return error_result(self.source_name, result)

        text = result.payload or ""
        blobs = self._FLIGHT_RE.findall(text)
        if not blobs:
            return AdapterResult(
                self.source_name, "empty",
                detail="no RSC flight payload found; page shape changed",
                resolved_via=url,
            )

        joined = "".join(blobs).encode().decode("unicode_escape", errors="replace")
        seen: set[str] = set()
        items: list[models.RawItem] = []
        for href, raw_title in self._TASK_RE.findall(joined):
            title = re.sub(r"\s+", " ", raw_title).strip(" -–—")
            if not title or href in seen or len(title) < 8:
                continue
            seen.add(href)
            price = self._PRICE_RE.search(title)
            amount = parse_amount(price.group(1)) if price else None
            currency = price.group(2) if price else None
            amount_text = price.group(0) if price else ""
            items.append(
                models.RawItem(
                    source=self.source_name,
                    native_id=href.rsplit("/", 1)[-1],
                    title=title,
                    url=f"https://opentask.ai{href}",
                    category="agent_task",
                    category_source="source_constant",
                    payout_raw=amount_text or None,
                    payout_usd_low=amount,
                    payout_usd_high=amount,
                    payout_currency=currency,
                    payout_kind=models.FIXED if amount else models.UNSTATED,
                    payout_basis=models.PER_TASK,
                    payout_confidence=models.CLAIMED,
                    identity_gate=models.GATE_ACCOUNT,
                    agent_permitted="yes",   # ToS s4 "Agent Use and Authority"
                    capital_required_usd=0.0,
                    tos_flags=["tos_prefers_documented_api"],
                    resolved_via=url,
                    raw={"parsed_from": "rsc_flight_payload"},
                )
            )
        return AdapterResult(
            source=self.source_name,
            status="ok" if items else "empty",
            items=items,
            detail="" if items else "flight payload present but no task/price pairs matched",
            resolved_via=url,
        )


class EfEspAdapter:
    """Ethereum Foundation Ecosystem Support Program.

    KNOWN LOW YIELD, and the reason is worth keeping. SC-R1 sampled three items
    carrying Salesforce keys (`Tags__c`, `RFP_Open_Date__c`, `Custom_URL_Slug__c`).
    On 2026-08-10 those keys appear on NO robots-permitted page: `/` and
    `/applicants` render marketing copy with an empty `pageProps`, and `/rfps`
    and `/wishlist` are 404. The structured data is presumably behind the
    `/api/` path that robots.txt disallows.

    This adapter therefore reads the permitted pages and reports honestly.
    Do not "fix" it by reading `/api/`.
    """

    source_name = "ef_esp"
    PAGES = (
        "https://esp.ethereum.foundation/",
        "https://esp.ethereum.foundation/applicants",
    )

    def fetch(self, client: Any, *, now_unix: int, since_unix: int) -> AdapterResult:
        items: list[models.RawItem] = []
        notes: list[str] = []
        for page in self.PAGES:
            result = get_text(client, page)
            if result.is_error:
                notes.append(f"{page}: {result.detail}")
                continue
            data = _extract_next_data(result.payload or "")
            if not data:
                notes.append(f"{page}: no __NEXT_DATA__")
                continue
            records = _find_records(data, "Tags__c") or _find_records(data, "Name")
            if not records:
                notes.append(f"{page}: __NEXT_DATA__ present but carries no RFP records")
                continue
            for record in records:
                title = str(record.get("Name") or "").strip()
                if not title:
                    continue
                description = record.get("Description__c")
                items.append(
                    models.RawItem(
                        source=self.source_name,
                        native_id=str(record.get("Id") or title[:60]),
                        title=title,
                        url=page,
                        category=record.get("Tags__c"),
                        category_source="structured",
                        counterparty="Ethereum Foundation",
                        payout_raw=strip_html(description, limit=200),
                        payout_usd_low=parse_monetary(description),
                        payout_currency=parse_currency(description, "USD"),
                        payout_kind=models.UNSTATED,
                        payout_basis=models.PROGRAM_POOL,
                        payout_confidence=models.CLAIMED,
                        identity_gate=models.GATE_ACCOUNT,
                        agent_permitted="unstated",
                        capital_required_usd=0.0,
                        resolved_via=page,
                        raw=record,
                    )
                )
        return AdapterResult(
            source=self.source_name,
            status="ok" if items else "empty",
            items=items,
            detail="; ".join(notes)[:400],
            resolved_via=self.PAGES[0],
        )


class ArbitrumGrantsAdapter:
    """Arbitrum Foundation grant programs.

    Program cards render into the raw HTML (13 "Managed by" blocks observed
    2026-08-10, matching SC-R1's count), so a text parser works without
    touching an API. These are PROGRAM POOLS -- `$10M in ARB` is a program
    size, not an award -- and are marked as such so ranking cannot put a
    program above a bounty on a single numeric axis.
    """

    source_name = "arbitrum_grants"
    _CARD_RE = re.compile(
        r"(Active|Inactive)\s+([A-Z][^<>]{4,80}?)\s{2,}(.{40,600}?)Managed by\s+([^<>]{3,60})",
        re.S,
    )

    def fetch(self, client: Any, *, now_unix: int, since_unix: int) -> AdapterResult:
        url = config.SOURCES_BY_NAME[self.source_name].base_url
        result = get_text(client, url)
        if result.is_error:
            return error_result(self.source_name, result)

        text = re.sub(r"\s+", " ", strip_html(result.payload, limit=400_000) or "")
        # strip_html collapses whitespace, so re-inflate the card boundary the
        # regex relies on by splitting on the literal marker instead.
        chunks = text.split("Managed by")
        items: list[models.RawItem] = []
        for index in range(len(chunks) - 1):
            body = chunks[index][-700:]
            manager = chunks[index + 1][:80].strip(" .,")
            status_match = re.search(r"\b(Active|Inactive)\b", body)
            status = status_match.group(1) if status_match else None
            tail = body[status_match.end():].strip() if status_match else body.strip()
            title = tail.split(".")[0][:100].strip() if tail else ""
            if not title or len(title) < 6:
                continue
            amount = parse_monetary(tail)
            currency = parse_currency(tail, "USD")
            items.append(
                models.RawItem(
                    source=self.source_name,
                    native_id=title[:80],
                    title=title,
                    url=url,
                    category="grant_program",
                    category_source="source_constant",
                    counterparty=manager,
                    payout_raw=tail[:200] or None,
                    payout_usd_low=amount if currency == "USD" else None,
                    payout_currency=currency,
                    payout_kind=models.POOL if amount else models.UNSTATED,
                    payout_basis=models.PROGRAM_POOL,
                    payout_confidence=models.CLAIMED,
                    identity_gate=models.GATE_ACCOUNT,
                    agent_permitted="unstated",
                    capital_required_usd=0.0,
                    tos_flags=[] if status == "Active" else ["program_inactive"],
                    resolved_via=url,
                    raw={"status": status, "managed_by": manager},
                )
            )
        return AdapterResult(
            source=self.source_name,
            status="ok" if items else "empty",
            items=items,
            detail="" if items else "no 'Managed by' program cards matched",
            resolved_via=url,
        )


class _AffiliateLaneAdapter:
    """Shared base for the affiliate lane.

    THE WHOLE LANE IS YELLOW-CONDITIONAL under SC-1 §2, and every item it
    produces is flagged accordingly. Affiliate income is a different economic
    species from a bounty: evergreen commission terms, no deadline, no per-task
    payout, and income realized only through traffic the tribe does not
    currently have. `payout_basis=PER_SALE_COMMISSION` keeps that visible in
    the data rather than leaving it to a reader's memory.

    Method, not margin, is the fraud line: fabricated reviews or misappropriated
    media are RED regardless of which program they promote. That judgment is
    Phase 2's; this adapter's job is to carry the flags that let it be made.
    """

    lane_flags = ["affiliate_lane_yellow_conditional", "ftc_disclosure_required"]

    def _item(self, *, native_id: str, title: str, url: str,
              payout_raw: str | None, category: str | None,
              counterparty: str | None, raw: dict) -> models.RawItem:
        return models.RawItem(
            source=self.source_name,           # type: ignore[attr-defined]
            native_id=native_id,
            title=title,
            url=url,
            category=category,
            category_source="structured" if category else None,
            counterparty=counterparty,
            payout_raw=payout_raw,
            # Deliberately NOT parsed into a USD number. "Up To $50 Per Sale +
            # Residuals" is a rate schedule, not an amount; reducing it to 50.0
            # would invent a payout that nobody is owed.
            payout_usd_low=None,
            payout_usd_high=None,
            payout_currency=parse_currency(payout_raw, None),
            payout_kind=models.COMMISSION if payout_raw else models.UNSTATED,
            payout_basis=models.PER_SALE_COMMISSION,
            payout_confidence=models.UNVERIFIED,
            identity_gate=models.GATE_ACCOUNT,
            agent_permitted="unstated",
            # Organic/content-based is the default lane; a paid-ad arbitrage
            # variant would set this true and carry a hard loss cap.
            paid_acquisition=False,
            capital_required_usd=0.0,
            tos_flags=list(self.lane_flags),
            resolved_via=url,
            raw=raw,
        )


class AffiliateWatchAdapter(_AffiliateLaneAdapter):
    """Affiliate program directory (Inertia/Laravel, JSON in `data-page`).

    SC-R1 measured 100% field fit over 100 embedded records. On 2026-08-10 the
    base document's `data-page` props carry locales/auth/config but NO program
    array, and `/programs`, `/affiliate-programs`, `/best` all 404. Since
    robots.txt disallows `?page=` pagination, there is no permitted path to the
    listing that this adapter can currently reach -- reported, not worked around.
    """

    source_name = "affiliate_watch"

    def fetch(self, client: Any, *, now_unix: int, since_unix: int) -> AdapterResult:
        url = config.SOURCES_BY_NAME[self.source_name].base_url
        result = get_text(client, url)
        if result.is_error:
            return error_result(self.source_name, result)

        page = _extract_inertia_page(result.payload or "")
        if not page:
            return AdapterResult(
                self.source_name, "empty",
                detail="no data-page payload found", resolved_via=url,
            )
        records = _find_records(page, "teaser_affiliate")
        if not records:
            return AdapterResult(
                self.source_name, "empty",
                detail=(
                    "data-page present but carries no program records "
                    "(props: locales/auth/config only); listing likely behind "
                    "robots-disallowed ?page= pagination"
                ),
                resolved_via=url,
            )

        items = []
        for record in records:
            name = str(record.get("name") or "").strip()
            if not name:
                continue
            categories = record.get("categories") or []
            category = None
            if isinstance(categories, list) and categories:
                first = categories[0]
                category = first.get("name") if isinstance(first, dict) else str(first)
            items.append(
                self._item(
                    native_id=str(record.get("slug") or record.get("id") or name),
                    title=name,
                    url=f"https://affiliate.watch/{record.get('slug')}",
                    payout_raw=record.get("teaser_affiliate"),
                    category=category,
                    counterparty=name,
                    raw=record,
                )
            )
        return AdapterResult(
            source=self.source_name,
            status="ok" if items else "empty",
            items=items,
            resolved_via=url,
        )


class AffPayingAdapter(_AffiliateLaneAdapter):
    """AffPaying affiliate-program directory. Fully permissive robots.

    Plain server-rendered HTML with no embedded JSON, so this parses anchor +
    commission-cell pairs. Inventory skews hard to iGaming/adult, which means a
    high RED-by-method rate is expected downstream -- that is a classification
    outcome, not an ingest failure, and the items are still surfaced per
    invariant 1.
    """

    source_name = "affpaying"
    # Observed shape (2026-08-10): each program is an <h2> wrapping the program
    # link; commission text sits in a later cell of the same row block. The
    # earlier attempt anchored on any <a> and a nearby number, which matched
    # navigation chrome and yielded 5 rows -- anchoring on the h2 is what makes
    # this a program list rather than a link list.
    _NAME_RE = re.compile(
        r'<h2[^>]*>\s*<a[^>]+href="(/[^"]+)"[^>]*>([^<]{2,80})</a>', re.S
    )
    _COMMISSION_RE = re.compile(
        r"(\d{1,3}(?:\.\d+)?\s*%[^<]{0,60}|\$\s?\d[\d,\.]*[^<]{0,60})"
    )

    def fetch(self, client: Any, *, now_unix: int, since_unix: int) -> AdapterResult:
        url = config.SOURCES_BY_NAME[self.source_name].base_url
        result = get_text(client, url)
        if result.is_error:
            return error_result(self.source_name, result)

        text = result.payload or ""
        seen: set[str] = set()
        items = []
        matches = list(self._NAME_RE.finditer(text))
        for index, match in enumerate(matches):
            href, raw_name = match.group(1), match.group(2)
            name = html_module.unescape(raw_name).strip()
            if not name or name.lower() in seen or len(name) < 3:
                continue
            seen.add(name.lower())
            # Commission lives between this program's name and the next one's,
            # so the row block is bounded by the following match rather than by
            # a fixed character window that could bleed across rows.
            end = matches[index + 1].start() if index + 1 < len(matches) else match.end() + 2500
            block = strip_html(text[match.end():end], limit=2500) or ""
            commission = self._COMMISSION_RE.search(block)
            items.append(
                self._item(
                    native_id=href.strip("/")[:80] or name,
                    title=name,
                    url=f"https://www.affpaying.com{href}",
                    payout_raw=(
                        re.sub(r"\s+", " ", commission.group(0)).strip()
                        if commission else None
                    ),
                    # AffPaying publishes categories only on category-FILTERED
                    # listing pages, not on this index. Left null rather than
                    # guessed; the consequence is a real field-fit shortfall
                    # against recon's 80%, reported rather than papered over.
                    category=None,
                    counterparty=name,
                    raw={"href": href},
                )
            )
        return AdapterResult(
            source=self.source_name,
            status="ok" if items else "empty",
            items=items,
            detail="" if items else "no anchor/commission row pairs matched",
            resolved_via=url,
        )


__all__ = [
    "OpenTaskAdapter",
    "EfEspAdapter",
    "ArbitrumGrantsAdapter",
    "AffiliateWatchAdapter",
    "AffPayingAdapter",
]
