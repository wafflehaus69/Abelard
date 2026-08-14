"""GraphQL surfaces: Dework, Giveth QF, Questbook.

Every query below is a LITERAL. None is assembled from fetched content, and no
mutation shape exists anywhere in this module -- a read-only sensor that builds
its queries dynamically from what a source told it is one prompt-injection away
from writing. Introspection is disabled on Dework, so these were discovered by
probing live and reading the server's own "did you mean" errors.
"""

from __future__ import annotations

from typing import Any

from .. import config, models
from ..fetch import post_graphql
from .base import AdapterResult, error_result, iso_to_unix, strip_html

# ---------------------------------------------------------------------------
# Dework
# ---------------------------------------------------------------------------
# OPERATIONAL FOOTGUN, measured 2026-08-10: `getTasks` WITHOUT a `statuses`
# filter times out at 30s and returns nothing. With `statuses:[TODO]` it
# answers in well under a second. The filter is a correctness requirement, not
# a nicety -- an unfiltered query looks like a dead source rather than a slow
# one, and would silently mark Dework `error` on every scan.
# FIELD SET RESTORED 2026-08-11 against SC-R1's sampled shape. An earlier trim
# dropped `workspace`/`skills`, which left `counterparty` NULL on all 55 Dework
# rows -- and the live LLM veto pass then cited "empty counterparty" on 19 of
# its 109 vetoes. The model was reacting to a hole in the data, not to the
# opportunity. Trim query fields only against a sampled payload, never from
# memory of what looks necessary.
_DEWORK_QUERY = """
query ScoutTasks($limit: Int!) {
  getTasks(input: {limit: $limit, statuses: [TODO]}) {
    id
    name
    status
    dueDate
    createdAt
    permalink
    tags { label }
    skills { name }
    workspace {
      name
      organization { name }
    }
    rewards {
      amount
      peggedToUsd
      token { symbol usdPrice exp }
    }
  }
}
"""

_GIVETH_QUERY = """
query ScoutQfRounds {
  qfRounds {
    id
    name
    slug
    isActive
    beginDate
    endDate
    allocatedFund
    allocatedFundUSD
  }
}
"""

# `numberOfApplicationsSelected` restored 2026-08-11. Paired with
# `numberOfApplications` it is the only PUBLISHED award rate anywhere on the
# roster -- SC-R1 sampled TON Grants at 103 selected of 2,132 applications --
# and dropping it made P(award) underivable on every row in the corpus.
_QUESTBOOK_QUERY = """
query ScoutGrants($limit: Int!) {
  grants(limit: $limit) {
    _id
    title
    acceptingApplications
    deadlineS
    createdAtS
    numberOfApplications
    numberOfApplicationsSelected
    totalGrantFundingDisbursedUSD
    reward { committed asset token { label decimal } }
    workspace { title }
  }
}
"""


class DeworkAdapter:
    """DAO task board.

    SC-R1 measured 100% field-fit and simultaneously found the platform
    DORMANT: ~97.6% of the default feed carries no reward, much of the rewarded
    set is 2022-era, and sampled orgs include literal test junk. Extraction
    quality and inventory quality are different things, and this adapter is the
    case that proves it.

    Reward-less tasks are filtered out here rather than persisted, because a
    task with no reward is not an income opportunity -- that is a scope filter,
    not a legitimacy judgment, and the count of what was filtered is reported.
    """

    source_name = "dework"

    def __init__(self, limit: int = 200) -> None:
        self.limit = limit

    def fetch(self, client: Any, *, now_unix: int, since_unix: int) -> AdapterResult:
        url = config.SOURCES_BY_NAME[self.source_name].base_url
        result = post_graphql(client, url, _DEWORK_QUERY, {"limit": self.limit})
        if result.is_error:
            return error_result(self.source_name, result)

        rows = (result.payload or {}).get("getTasks") or []
        items: list[models.RawItem] = []
        rewardless = 0
        for row in rows:
            if not isinstance(row, dict):
                continue
            rewards = row.get("rewards") or []
            if not rewards:
                rewardless += 1
                continue

            reward = rewards[0] if isinstance(rewards[0], dict) else {}
            token = reward.get("token") or {}
            usd = None
            token_qty = None
            price_source = None
            raw_amount = reward.get("amount")
            try:
                # Dework stores amounts as integer strings in the token's base
                # units; `exp` is the decimal shift. 50000000000 USDC at exp 6
                # is 50_000.0 tokens, not fifty billion.
                if raw_amount is not None and token.get("exp") is not None:
                    token_qty = float(raw_amount) / (10 ** int(token["exp"]))
                    price = token.get("usdPrice")
                    if price:
                        usd = token_qty * float(price)
                        # The USD figure is a QUOTE, and naming its origin is
                        # what keeps it identifiable as one. An illiquid
                        # community token's quoted price should not silently
                        # become a dollar amount in the ledger.
                        price_source = (
                            f"dework:token.usdPrice={price} symbol="
                            f"{token.get('symbol')}"
                        )
            except (TypeError, ValueError, ArithmeticError):
                usd = token_qty = None
                price_source = None

            tags = [t.get("label") for t in (row.get("tags") or []) if isinstance(t, dict)]
            skills = [s.get("name") for s in (row.get("skills") or []) if isinstance(s, dict)]
            workspace = row.get("workspace") or {}
            organization = workspace.get("organization") or {}
            # Organization is the real counterparty; the workspace is a board
            # within it. Falling back to the workspace name is better than the
            # NULL that made 19 vetoes read as "unclear counterparty".
            counterparty = organization.get("name") or workspace.get("name")
            items.append(
                models.RawItem(
                    source=self.source_name,
                    native_id=str(row.get("id") or ""),
                    title=str(row.get("name") or "").strip(),
                    url=row.get("permalink"),
                    category=", ".join(t for t in tags if t) or None,
                    category_source="structured" if tags else None,
                    counterparty=counterparty,
                    effort_note=(
                        f"skills: {', '.join(s for s in skills if s)}" if skills else None
                    ),
                    payout_raw=(
                        f"{raw_amount} {token.get('symbol')} (exp {token.get('exp')})"
                        if raw_amount is not None else None
                    ),
                    payout_usd_low=usd,
                    payout_usd_high=usd,
                    payout_currency=token.get("symbol"),
                    payout_kind=models.FIXED if usd is not None else models.UNSTATED,
                    payout_basis=models.PER_TASK,
                    payout_confidence=models.CLAIMED,
                    payout_asset_class=models.classify_asset(token.get("symbol")),
                    payout_token_symbol=token.get("symbol"),
                    payout_token_quantity=token_qty,
                    indicative_usd_at_discovery=usd,
                    price_source=price_source,
                    deadline_unix=iso_to_unix(row.get("dueDate")),
                    posted_unix=iso_to_unix(row.get("createdAt")),
                    identity_gate=models.GATE_ACCOUNT,
                    agent_permitted="unstated",
                    capital_required_usd=0.0,
                    resolved_via=url,
                    raw=row,
                )
            )
        detail = f"filtered {rewardless} reward-less tasks of {len(rows)} returned"
        return AdapterResult(
            source=self.source_name,
            status="ok" if items else "empty",
            items=items,
            detail=detail,
            resolved_via=url,
        )


class GivethAdapter:
    """Giveth quadratic-funding rounds -- the live Gitcoin-adjacent surface.

    Rounds are PROGRAM POOLS, not per-task payouts, and there is no per-item
    category (every row is the same kind of thing), so `category_source` is
    `source_constant` rather than a pretended structured field.
    """

    source_name = "giveth_qf"

    def fetch(self, client: Any, *, now_unix: int, since_unix: int) -> AdapterResult:
        url = config.SOURCES_BY_NAME[self.source_name].base_url
        result = post_graphql(client, url, _GIVETH_QUERY)
        if result.is_error:
            return error_result(self.source_name, result)

        rows = (result.payload or {}).get("qfRounds") or []
        items: list[models.RawItem] = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            usd = row.get("allocatedFundUSD")
            items.append(
                models.RawItem(
                    source=self.source_name,
                    native_id=str(row.get("id") or ""),
                    title=str(row.get("name") or "").strip(),
                    url=f"https://giveth.io/qf/{row.get('slug')}",
                    category="quadratic_funding_round",
                    category_source="source_constant",
                    counterparty="Giveth",
                    payout_raw=(
                        f"{row.get('allocatedFund')} pool "
                        f"(${usd} USD)" if usd is not None else None
                    ),
                    payout_usd_low=float(usd) if usd is not None else None,
                    payout_usd_high=float(usd) if usd is not None else None,
                    payout_currency="USD",
                    payout_kind=models.POOL if usd is not None else models.UNSTATED,
                    payout_basis=models.PROGRAM_POOL,
                    payout_confidence=models.CLAIMED,
                    deadline_unix=iso_to_unix(row.get("endDate")),
                    posted_unix=iso_to_unix(row.get("beginDate")),
                    identity_gate=models.GATE_ACCOUNT,
                    agent_permitted="unstated",
                    capital_required_usd=0.0,
                    tos_flags=[] if row.get("isActive") else ["round_inactive"],
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


class QuestbookAdapter:
    """Questbook grant programs.

    TWO HAZARDS, both measured.

    1. `reward.committed` is a PROGRAM POOL, not a per-task payout. SC-R1
       sampled TON Grants with committed=150000 against
       totalGrantFundingDisbursedUSD=1,603,810 -- the "reward" understated
       actual disbursement by 10x because it is not the same quantity.
       `payout_basis=PROGRAM_POOL` is what stops a ranking from treating it as
       a task fee.
    2. The default ordering surfaces TEST JUNK. A live probe on 2026-08-10
       returned three consecutive rows titled "dfdfd" with committed=11. Junk
       is filtered on a title/applicant heuristic and the filtered count is
       reported rather than silently dropped.
    """

    source_name = "questbook"

    def __init__(self, limit: int = 200) -> None:
        self.limit = limit

    @staticmethod
    def _looks_like_test_junk(title: str, applications: int, committed: float) -> bool:
        stripped = (title or "").strip()
        if len(stripped) < 4:
            return True
        # A title of one repeated character class ("dfdfd", "aaaa", "test").
        if stripped.lower() in {"test", "testing", "asdf", "dfdfd", "abc"}:
            return True
        if len(set(stripped.lower())) <= 2:
            return True
        if applications == 0 and committed <= 50:
            return True
        return False

    def fetch(self, client: Any, *, now_unix: int, since_unix: int) -> AdapterResult:
        url = config.SOURCES_BY_NAME[self.source_name].base_url
        result = post_graphql(client, url, _QUESTBOOK_QUERY, {"limit": self.limit})
        if result.is_error:
            return error_result(self.source_name, result)

        rows = (result.payload or {}).get("grants") or []
        items: list[models.RawItem] = []
        junk = 0
        for row in rows:
            if not isinstance(row, dict):
                continue
            title = str(row.get("title") or "").strip()
            reward = row.get("reward") or {}
            token = reward.get("token") or {}
            committed = reward.get("committed")
            applications = row.get("numberOfApplications") or 0

            try:
                committed_value = float(committed) if committed is not None else 0.0
            except (TypeError, ValueError):
                committed_value = 0.0

            if self._looks_like_test_junk(title, applications, committed_value):
                junk += 1
                continue

            workspace = row.get("workspace") or {}
            label = token.get("label")

            # The only PUBLISHED award rate on the roster. SC-R1 sampled TON
            # Grants at 103 selected of 2,132 applications; both counters come
            # straight from the source, so this is observed rather than modelled.
            selected = row.get("numberOfApplicationsSelected")
            award_rate = None
            award_source = None
            if isinstance(applications, int) and applications > 0 and isinstance(
                selected, int
            ):
                award_rate = min(1.0, selected / applications)
                award_source = (
                    f"questbook:numberOfApplicationsSelected/{selected}"
                    f":numberOfApplications/{applications}"
                )

            items.append(
                models.RawItem(
                    source=self.source_name,
                    native_id=str(row.get("_id") or ""),
                    title=title,
                    url=f"https://questbook.app/grants/{row.get('_id')}",
                    # Questbook has no category FIELD. The workspace title is
                    # the real grouping a human uses to read this board ("TON
                    # Grants", "Axelar Researchers"), so it is carried as a
                    # DERIVED category and labelled as such -- never presented
                    # as structured data the source does not publish.
                    #
                    # This also keeps the field-fit metric comparable with
                    # SC-R1's 77%, which was measured the same derived way.
                    # Refusing to derive here would have scored the source 0%
                    # against a 77% prediction and read as a collapse, when the
                    # only thing that changed was the measurement.
                    category=workspace.get("title") or None,
                    category_source="derived" if workspace.get("title") else None,
                    counterparty=workspace.get("title"),
                    # Render only what is honestly known. Questbook returns
                    # `"token": null` on 22 of 80 rows, and the previous
                    # f-string turned that into "10000 None committed (pool)"
                    # -- and on 6 rows into scientific notation like "1e+21
                    # None". The live veto pass flagged exactly those strings
                    # as uninterpretable, which they were. A missing label is
                    # said plainly; the amount is never printed in exponent
                    # form, because no listing quotes a payout that way.
                    payout_raw=(
                        f"{committed_value:,.0f} {label} committed (pool)"
                        if committed is not None and label
                        else f"{committed_value:,.0f} (token unspecified) committed (pool)"
                        if committed is not None
                        else None
                    ),
                    payout_usd_low=committed_value if label == "USD" else None,
                    payout_usd_high=committed_value if label == "USD" else None,
                    payout_currency=label,
                    payout_kind=models.POOL if committed is not None else models.UNSTATED,
                    payout_basis=models.PROGRAM_POOL,
                    payout_confidence=models.CLAIMED,
                    deadline_unix=row.get("deadlineS"),
                    posted_unix=row.get("createdAtS"),
                    identity_gate=models.GATE_ACCOUNT,
                    agent_permitted="unstated",
                    capital_required_usd=0.0,
                    tos_flags=(
                        [] if row.get("acceptingApplications") else ["closed_to_applications"]
                    ),
                    contention=applications if isinstance(applications, int) else None,
                    award_rate=award_rate,
                    award_rate_source=award_source,
                    effort_note=(
                        f"{applications} applications; "
                        f"${row.get('totalGrantFundingDisbursedUSD')} disbursed to date"
                    ),
                    resolved_via=url,
                    raw=row,
                )
            )
        return AdapterResult(
            source=self.source_name,
            status="ok" if items else "empty",
            items=items,
            detail=f"filtered {junk} test-junk rows of {len(rows)} returned",
            resolved_via=url,
        )


__all__ = ["DeworkAdapter", "GivethAdapter", "QuestbookAdapter"]
