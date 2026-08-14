"""Adapters for the plain-JSON surfaces: Superteam Earn, Opire, Zindi, Dealwork.

Field names here were read off live payloads during SC-1 Phase 1 (2026-08-10),
not carried from memory. Where a source's own vocabulary is misleading, the
adapter renames it and says why in a comment -- `pendingPrice` being the
sharpest case.
"""

from __future__ import annotations

from typing import Any

from .. import config, models
from ..fetch import get_json
from .base import (
    AdapterResult,
    error_result,
    iso_to_unix,
    parse_amount,
    parse_currency,
    strip_html,
)


class SuperteamEarnAdapter:
    """Solana-ecosystem bounty board. Cleanest surface on the roster.

    Carries a per-item `agentAccess` flag, which at recon read HUMAN_ONLY on
    20 of 21 listings. That is preserved verbatim into `agent_permitted` rather
    than being collapsed into a single source-level fact: an admissible source
    can be full of items an agent must not execute, and flattening that would
    hide the distinction the tribe most needs to see.
    """

    source_name = "superteam_earn"

    def fetch(self, client: Any, *, now_unix: int, since_unix: int) -> AdapterResult:
        url = config.SOURCES_BY_NAME[self.source_name].base_url
        result = get_json(client, url)
        if result.is_error:
            return error_result(self.source_name, result)

        rows = result.payload if isinstance(result.payload, list) else []
        items: list[models.RawItem] = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            slug = row.get("slug") or row.get("id") or ""
            token = row.get("token")
            amount = row.get("rewardAmount")
            low = row.get("minRewardAsk")
            high = row.get("maxRewardAsk")

            if amount is not None:
                kind, usd_low, usd_high = models.FIXED, float(amount), float(amount)
            elif low is not None or high is not None:
                kind = models.RANGE
                usd_low = float(low) if low is not None else None
                usd_high = float(high) if high is not None else None
            else:
                kind, usd_low, usd_high = models.UNSTATED, None, None

            sponsor = row.get("sponsor") or {}
            agent_access = row.get("agentAccess")
            counts = row.get("_count") or {}
            submissions = counts.get("Submission")

            items.append(
                models.RawItem(
                    source=self.source_name,
                    native_id=str(row.get("id") or slug),
                    title=str(row.get("title") or "").strip(),
                    url=f"https://superteam.fun/earn/listing/{slug}" if slug else None,
                    category=row.get("type"),           # bounty | project | hackathon
                    category_source="structured",
                    counterparty=sponsor.get("name"),
                    counterparty_verified=sponsor.get("isVerified"),
                    payout_raw=f"{amount} {token}" if amount is not None else None,
                    payout_usd_low=usd_low,
                    payout_usd_high=usd_high,
                    payout_currency=token,
                    payout_kind=kind,
                    payout_basis=models.PER_TASK,
                    # Sponsor-committed and the sponsor's verification state is
                    # published, which is strictly better than Opire's bare
                    # claim -- but it is still not escrow.
                    payout_confidence=models.CLAIMED,
                    escrow_verified=None,
                    deadline_unix=iso_to_unix(row.get("deadline")),
                    identity_gate=models.GATE_ACCOUNT,
                    agent_permitted=(
                        "yes" if agent_access == "AGENT_ALLOWED"
                        else "no" if agent_access == "HUMAN_ONLY"
                        else "unstated"
                    ),
                    capital_required_usd=0.0,
                    # Submissions so far = contention. A bounty pays a ranked
                    # subset, but Superteam does not publish how many win, so
                    # no award rate is derived from this.
                    contention=submissions if isinstance(submissions, int) else None,
                    resolved_via=url,
                    raw=row,
                )
            )
        return AdapterResult(
            source=self.source_name,
            status="ok" if items else "empty",
            items=items,
            resolved_via=url,
        )


class OpireAdapter:
    """OSS bounties on GitHub issues.

    THE PAYOUT HERE IS A CLAIM, NOT AN ESCROW. SC-R1 sampled a reward of
    $1,260,988 (`pendingPrice.value` = 126098800 US cents) on a throwaway repo
    with `isBotInstalled` false, titled "c1work". `pendingPrice` names itself
    honestly if you read it: it is *pending*. It is mapped to
    `payout_confidence=CLAIMED` with `escrow_verified=False`, and the org /
    bot-installed signals are preserved so a downstream filter can weigh them.
    """

    source_name = "opire"

    def fetch(self, client: Any, *, now_unix: int, since_unix: int) -> AdapterResult:
        url = config.SOURCES_BY_NAME[self.source_name].base_url
        result = get_json(client, url)
        if result.is_error:
            return error_result(self.source_name, result)

        rows = result.payload if isinstance(result.payload, list) else []
        items: list[models.RawItem] = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            price = row.get("pendingPrice") or {}
            value = price.get("value")
            unit = price.get("unit")
            usd = float(value) / 100.0 if (value is not None and unit == "USD_CENT") else None

            languages = row.get("programmingLanguages") or []
            project = row.get("project") or {}
            org = row.get("organization") or {}
            bot_installed = project.get("isBotInstalled")

            items.append(
                models.RawItem(
                    source=self.source_name,
                    native_id=str(row.get("id") or ""),
                    title=str(row.get("title") or "").strip(),
                    url=row.get("url"),
                    category=", ".join(languages) if languages else None,
                    category_source="structured" if languages else None,
                    counterparty=org.get("name"),
                    # Not a verification of the ORG -- it records whether the
                    # payout bot is actually installed on the repo, which is
                    # the closest available proxy for "this bounty is real".
                    counterparty_verified=bot_installed,
                    payout_raw=f"{value} {unit}" if value is not None else None,
                    payout_usd_low=usd,
                    payout_usd_high=usd,
                    payout_currency="USD" if usd is not None else None,
                    payout_kind=models.FIXED if usd is not None else models.UNSTATED,
                    payout_basis=models.PER_TASK,
                    payout_confidence=models.CLAIMED,
                    escrow_verified=False,
                    identity_gate=models.GATE_ACCOUNT,
                    agent_permitted="unstated",
                    capital_required_usd=0.0,
                    effort_note=f"languages: {', '.join(languages)}" if languages else None,
                    resolved_via=url,
                    raw=row,
                )
            )
        return AdapterResult(
            source=self.source_name,
            status="ok" if items else "empty",
            items=items,
            resolved_via=url,
        )


class ZindiAdapter:
    """African data-science competition platform.

    Rewards are free-text and genuinely multi-currency ("$25 000 USD",
    "1000 CHF", "€2 000 EUR"), so currency is parsed rather than assumed. A
    prize pool split across ranks is a pool, not a per-task payout.
    """

    source_name = "zindi"

    def fetch(self, client: Any, *, now_unix: int, since_unix: int) -> AdapterResult:
        url = config.SOURCES_BY_NAME[self.source_name].base_url
        result = get_json(client, url, per_page=50)
        if result.is_error:
            return error_result(self.source_name, result)

        payload = result.payload
        rows = payload.get("data", []) if isinstance(payload, dict) else []
        items: list[models.RawItem] = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            reward = row.get("reward")
            amount = parse_amount(reward)
            items.append(
                models.RawItem(
                    source=self.source_name,
                    native_id=str(row.get("id") or ""),
                    title=str(row.get("title") or "").strip(),
                    url=f"https://zindi.africa/competitions/{row.get('id')}",
                    category=row.get("kind"),           # competition | hackathon
                    category_source="structured",
                    counterparty=row.get("organization"),
                    payout_raw=reward,
                    payout_usd_low=amount,
                    payout_usd_high=amount,
                    payout_currency=parse_currency(reward),
                    payout_kind=models.POOL if amount is not None else models.UNSTATED,
                    # A ranked prize pool, not a fee for a unit of work.
                    payout_basis=models.PROGRAM_POOL,
                    payout_confidence=models.CLAIMED,
                    deadline_unix=iso_to_unix(row.get("end_time")),
                    identity_gate=models.GATE_ACCOUNT,
                    agent_permitted="unstated",
                    capital_required_usd=0.0,
                    # Entrant count is contention, NOT an award rate: Zindi
                    # publishes no winner count, so P(award) stays None rather
                    # than being inferred as 1/participants.
                    contention=(
                        row.get("participations_count")
                        if isinstance(row.get("participations_count"), int) else None
                    ),
                    effort_note="ML modelling; multi-week competition cadence",
                    resolved_via=url,
                    raw=row,
                )
            )
        return AdapterResult(
            source=self.source_name,
            status="ok" if items else "empty",
            items=items,
            resolved_via=url,
        )


class DealworkAdapter:
    """Agent-native marketplace -- humans and AI agents hire each other.

    Part of the category SC-R1 discovered that the seed list had no concept of.
    `eligibleWorkerTypes` is the field that makes it agent-native and is mapped
    straight onto `agent_permitted`.

    Its `/skill.md` is agent-directed documentation, which makes this source a
    live case for the Phase 3 injection guard. Phase 1 reads only the jobs API.
    """

    source_name = "dealwork"

    def fetch(self, client: Any, *, now_unix: int, since_unix: int) -> AdapterResult:
        url = config.SOURCES_BY_NAME[self.source_name].base_url
        result = get_json(client, url)
        if result.is_error:
            return error_result(self.source_name, result)

        payload = result.payload
        rows = payload.get("data", []) if isinstance(payload, dict) else []
        items: list[models.RawItem] = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            fixed = row.get("fixedPrice")
            low = row.get("budgetMin")
            high = row.get("budgetMax")

            if fixed is not None:
                kind = models.FIXED
                usd_low = usd_high = float(fixed)
                raw_payout = f"{fixed} USD fixed"
            elif low is not None or high is not None:
                kind = models.RANGE
                usd_low = float(low) if low is not None else None
                usd_high = float(high) if high is not None else None
                raw_payout = f"{low}-{high} USD"
            else:
                kind, usd_low, usd_high, raw_payout = models.UNSTATED, None, None, None

            worker_types = (row.get("eligibleWorkerTypes") or "").lower()
            items.append(
                models.RawItem(
                    source=self.source_name,
                    native_id=str(row.get("id") or ""),
                    title=str(row.get("title") or "").strip(),
                    url=f"https://dealwork.ai/jobs/{row.get('id')}",
                    category=row.get("category"),
                    category_source="structured",
                    counterparty=row.get("posterAccountId"),
                    payout_raw=raw_payout,
                    payout_usd_low=usd_low,
                    payout_usd_high=usd_high,
                    payout_currency="USD",
                    payout_kind=kind,
                    payout_basis=models.PER_TASK,
                    # Platform runs USD escrow released on approval, but the
                    # listing itself does not assert the escrow is funded.
                    payout_confidence=models.CLAIMED,
                    deadline_unix=iso_to_unix(row.get("deadline")),
                    identity_gate=models.GATE_ACCOUNT,
                    agent_permitted=(
                        "yes" if worker_types in ("any", "agent", "ai") else "unstated"
                    ),
                    capital_required_usd=0.0,
                    effort_note=strip_html(row.get("description"), limit=300),
                    resolved_via=url,
                    raw=row,
                )
            )
        return AdapterResult(
            source=self.source_name,
            status="ok" if items else "empty",
            items=items,
            resolved_via=url,
        )


__all__ = [
    "SuperteamEarnAdapter",
    "OpireAdapter",
    "ZindiAdapter",
    "DealworkAdapter",
]
