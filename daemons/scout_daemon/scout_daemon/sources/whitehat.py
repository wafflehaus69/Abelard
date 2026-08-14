"""White-hat security-research surfaces: Sherlock and YesWeHack.

These entered the roster under SC-1's white-hat carve-out, which moved security
research from RED to YELLOW-per-program. They are NOT ordinary work sources and
this module encodes the difference.

=========================== HARD BOUNDARY ===========================
This module CAPTURES SCOPE. It never acts on scope.

No adapter here -- and nothing in SC-1 -- tests, probes, scans, fuzzes,
connects to, or otherwise touches any target system named in a program's
scope. The scope text is read from the program's own published metadata and
stored as data for Mando's per-program admission decision. Target testing is
Builder work under OpSec scope monitoring, a separate and later order.

Scope-wandering is unauthorized access regardless of intent. Scope is a legal
boundary, not a preference, which is why the capture path and any future action
path must stay in different daemons.
=====================================================================

Order of checks, per SC-1 §B, and the order matters:

  1. NATURAL-PERSON / KYC FIRST. If a program requires an attestation an agent
     cannot truthfully make, the program is RED and the YELLOW carve-out does
     not apply. Checking this first prevents a well-scoped, well-paid program
     from being classified on its merits before anyone notices an agent cannot
     legitimately participate at all.
  2. SCOPE. `scope_published` false -> YELLOW held pending, never admitted as
     white-hat. Scope is never inferred from a description.
  3. CAPITAL. Surfaced, never hidden.
  4. SAFE-HARBOR / authorization language captured where present.
"""

from __future__ import annotations

import re
import time
from typing import Any

from .. import config, models
from ..fetch import get_json
from .base import AdapterResult, error_result, iso_to_unix, strip_html

# Lexicon for natural-person attestation. Category-first per invariant 6: this
# matches the CONCEPT (an identity claim an agent cannot truthfully make),
# not a list of platforms. NECESSARILY INCOMPLETE -- a program can require
# natural-person status in prose this misses, which is why a miss lands the
# item in YELLOW (needs judgment) rather than GREEN.
_NATURAL_PERSON_RE = re.compile(
    r"\bkyc\b|know your customer|identity verification|verify your identity|"
    r"natural person|proof of identity|government[- ]issued|passport|"
    r"\bw-?9\b|\bw-?8ben\b|tax form|must be (?:a |an )?(?:real|individual) person|"
    r"legal(?:ly)? (?:an )?adult|age verification",
    re.IGNORECASE,
)

# Safe-harbor / authorization language, captured where present. Its presence is
# informative; its ABSENCE is not proof of anything, and is recorded as null.
_SAFE_HARBOR_RE = re.compile(
    r"safe harbou?r|authoriz(?:ed|ation) to test|will not (?:pursue|initiate) "
    r"legal action|good faith|dmca|cfaa",
    re.IGNORECASE,
)

# Courtesy pacing for the per-item detail fetches both sources require.
_DETAIL_PACE_S = 0.25


def _detect_natural_person(*texts: str | None) -> tuple[bool | None, str | None]:
    """(required, evidence). None means 'no signal found', NOT 'not required'.

    The distinction is the whole point. Absence of KYC language in a program's
    published rules does not prove the platform will pay an entity that cannot
    complete identity verification -- it only proves the program did not say so
    where we looked. Returning None keeps that honest, and the classifier treats
    unknown as YELLOW rather than GREEN.
    """
    for text in texts:
        if not text:
            continue
        match = _NATURAL_PERSON_RE.search(text)
        if match:
            start = max(0, match.start() - 90)
            return True, text[start : match.end() + 90].strip()
    return None, None


def _detect_safe_harbor(*texts: str | None) -> str | None:
    for text in texts:
        if not text:
            continue
        match = _SAFE_HARBOR_RE.search(text)
        if match:
            start = max(0, match.start() - 150)
            return text[start : match.end() + 250].strip()
    return None


class SherlockAdapter:
    """Sherlock audit contests and public bug bounties.

    Best-instrumented white-hat source on the roster: the contest detail
    endpoint exposes `requires_kyc` as a first-class BOOLEAN and `scope` as a
    structured array of repos pinned to a commit hash. No prose inference
    needed for either gate -- which is exactly why it was worth adding.
    """

    source_name = "sherlock"
    LIST_URL = "https://mainnet-contest.sherlock.xyz/contests"
    DETAIL_URL = "https://mainnet-contest.sherlock.xyz/contests/{id}"

    def __init__(self, max_pages: int = 3, detail_limit: int = 40) -> None:
        self.max_pages = max_pages
        self.detail_limit = detail_limit

    def fetch(self, client: Any, *, now_unix: int, since_unix: int) -> AdapterResult:
        rows: list[dict] = []
        page = 1
        while page <= self.max_pages:
            result = get_json(client, self.LIST_URL, page=page)
            if result.is_error:
                if page == 1:
                    return error_result(self.source_name, result)
                break                      # partial pages: keep what we have
            payload = result.payload
            if not isinstance(payload, dict):
                break
            batch = payload.get("items") or []
            rows.extend(r for r in batch if isinstance(r, dict))
            if not payload.get("has_next"):
                break
            page += 1

        items: list[models.RawItem] = []
        for index, row in enumerate(rows):
            contest_id = row.get("id")
            detail: dict = {}
            if contest_id is not None and index < self.detail_limit:
                time.sleep(_DETAIL_PACE_S)
                detail_result = get_json(
                    client, self.DETAIL_URL.format(id=contest_id)
                )
                if detail_result.is_ok and isinstance(detail_result.payload, dict):
                    detail = detail_result.payload

            # --- gate 1: natural person, checked first --------------------
            requires_kyc = detail.get("requires_kyc")
            natural_person = bool(requires_kyc) if requires_kyc is not None else None

            # --- gate 2: scope, verbatim, never inferred ------------------
            scope = detail.get("scope")
            scope_published = bool(scope) if detail else None
            scope_text = None
            if scope:
                # Repo + branch + commit is the authorized boundary. The commit
                # hash matters: "the repo" is not a scope, a pinned tree is.
                parts = [
                    f"{s.get('repo')}@{s.get('branch_name')}"
                    f"#{(s.get('commit_hash') or '')[:12]}"
                    f" ({s.get('total_nsloc')} nSLOC)"
                    for s in scope
                    if isinstance(s, dict)
                ]
                scope_text = "; ".join(parts)[:4000]

            rewards = row.get("rewards")
            prize_pool = row.get("prize_pool")
            amount = float(rewards) if rewards else (
                float(prize_pool) if prize_pool else None
            )

            items.append(
                models.RawItem(
                    source=self.source_name,
                    native_id=str(contest_id or ""),
                    title=str(row.get("title") or "").strip(),
                    url=f"https://audits.sherlock.xyz/contests/{contest_id}",
                    category=row.get("type_label"),   # e.g. "Public Bug Bounty"
                    category_source="structured",
                    counterparty=str(row.get("title") or "").strip(),
                    payout_raw=(
                        f"{rewards} {row.get('token')}" if rewards else None
                    ),
                    payout_usd_low=amount,
                    payout_usd_high=amount,
                    payout_currency=row.get("token"),
                    payout_kind=models.POOL if amount else models.UNSTATED,
                    payout_basis=models.PROGRAM_POOL,
                    payout_confidence=models.CLAIMED,
                    deadline_unix=row.get("ends_at"),
                    posted_unix=row.get("starts_at"),
                    identity_gate=(
                        models.GATE_KYC if natural_person else models.GATE_ACCOUNT
                    ),
                    agent_permitted="unstated",
                    natural_person_required=natural_person,
                    scope_published=scope_published,
                    scope_text=scope_text,
                    safe_harbor_text=_detect_safe_harbor(detail.get("description")),
                    capital_required_usd=0.0,
                    effort_note="security research; high skill, unbounded effort",
                    tos_flags=["private_contest"] if row.get("private") else [],
                    resolved_via=self.LIST_URL,
                    raw={"list": row, "detail_keys": sorted(detail)[:40]},
                )
            )
        return AdapterResult(
            source=self.source_name,
            status="ok" if items else "empty",
            items=items,
            resolved_via=self.LIST_URL,
        )


class YesWeHackAdapter:
    """YesWeHack public bug-bounty and VDP programs.

    The sixth security platform -- the one a fixed five-name domain list let
    through as ordinary work at 97.6% field-fit during SC-R1. It is in the
    roster now because the rubric changed, and it is handled here rather than
    in `json_api.py` because the rubric that admits it is the per-program one.

    Scope arrives structured (`scopes[]` with asset type and value) plus an
    explicit `out_of_scope` list. KYC is NOT a field -- it is detected, when at
    all, from the program's prose rules, so `natural_person_required` is
    frequently None here. None means unknown, and unknown means YELLOW.
    """

    source_name = "yeswehack"
    LIST_URL = "https://api.yeswehack.com/programs"
    DETAIL_URL = "https://api.yeswehack.com/programs/{slug}"

    def __init__(self, max_pages: int = 2, detail_limit: int = 30) -> None:
        self.max_pages = max_pages
        self.detail_limit = detail_limit

    def fetch(self, client: Any, *, now_unix: int, since_unix: int) -> AdapterResult:
        rows: list[dict] = []
        page = 1
        while page <= self.max_pages:
            result = get_json(client, self.LIST_URL, page=page)
            if result.is_error:
                if page == 1:
                    return error_result(self.source_name, result)
                break
            payload = result.payload
            if not isinstance(payload, dict):
                break
            batch = payload.get("items") or []
            rows.extend(r for r in batch if isinstance(r, dict))
            pagination = payload.get("pagination") or {}
            if page >= int(pagination.get("nb_pages") or 1):
                break
            page += 1

        items: list[models.RawItem] = []
        for index, row in enumerate(rows):
            slug = row.get("slug")
            detail: dict = {}
            if slug and index < self.detail_limit:
                time.sleep(_DETAIL_PACE_S)
                detail_result = get_json(client, self.DETAIL_URL.format(slug=slug))
                if detail_result.is_ok and isinstance(detail_result.payload, dict):
                    detail = detail_result.payload

            rules = detail.get("rules")
            account_access = detail.get("account_access")

            # --- gate 1: natural person, checked first --------------------
            natural_person, evidence = _detect_natural_person(rules, account_access)

            # --- gate 2: scope ------------------------------------------
            scopes = detail.get("scopes") or []
            scopes_count = row.get("scopes_count") or 0
            # The list endpoint's count is enough to know scope EXISTS even
            # when the detail fetch was skipped by the cap; the verbatim text
            # requires the detail. Both facts are recorded separately.
            scope_published = bool(scopes) if detail else (scopes_count > 0)
            scope_text = None
            if scopes:
                parts = [
                    f"{s.get('scope')} [{s.get('scope_type_name') or s.get('scope_type')}"
                    f", value={s.get('asset_value')}]"
                    for s in scopes
                    if isinstance(s, dict)
                ]
                out_of_scope = detail.get("out_of_scope") or []
                scope_text = "IN SCOPE: " + "; ".join(parts)
                if out_of_scope:
                    scope_text += " || OUT OF SCOPE: " + "; ".join(
                        str(o) for o in out_of_scope
                    )
                scope_text = scope_text[:4000]

            business_unit = row.get("business_unit") or {}
            currency = business_unit.get("currency")
            low = row.get("bounty_reward_min")
            high = row.get("bounty_reward_max")
            has_bounty = bool(row.get("bounty"))

            tos_flags: list[str] = []
            if row.get("vdp"):
                # A VDP pays reputation, not money. Recording it as an income
                # opportunity without the flag would overstate the surface.
                tos_flags.append("vdp_no_monetary_bounty")
            if not row.get("public", True):
                tos_flags.append("private_program")

            items.append(
                models.RawItem(
                    source=self.source_name,
                    native_id=str(slug or ""),
                    title=str(row.get("title") or "").strip(),
                    url=f"https://yeswehack.com/programs/{slug}",
                    category=row.get("type"),          # bug-bounty | vdp
                    category_source="structured",
                    counterparty=business_unit.get("name"),
                    payout_raw=(
                        f"{low}-{high} {currency}" if has_bounty and high else None
                    ),
                    payout_usd_low=float(low) if low is not None else None,
                    payout_usd_high=float(high) if high is not None else None,
                    payout_currency=currency,
                    payout_kind=models.RANGE if has_bounty and high else models.UNSTATED,
                    payout_basis=models.PER_TASK,   # per accepted report
                    payout_confidence=models.CLAIMED,
                    posted_unix=iso_to_unix(row.get("last_update_at")),
                    identity_gate=(
                        models.GATE_KYC if natural_person else models.GATE_ACCOUNT
                    ),
                    agent_permitted="unstated",
                    natural_person_required=natural_person,
                    scope_published=scope_published,
                    scope_text=scope_text,
                    safe_harbor_text=_detect_safe_harbor(rules),
                    # `report_submission_cost` is denominated in YesWeHack
                    # reputation points, NOT money. Mapping it to
                    # capital_required would invent a dollar cost that does not
                    # exist. Recorded in raw; deliberately not monetized.
                    capital_required_usd=0.0,
                    effort_note=(
                        f"security research; {scopes_count} scopes; "
                        f"natural-person evidence: {evidence[:120]}" if evidence
                        else f"security research; {scopes_count} scopes"
                    ),
                    tos_flags=tos_flags,
                    resolved_via=self.LIST_URL,
                    raw={
                        "list": row,
                        "report_submission_cost_points": row.get(
                            "report_submission_cost"
                        ),
                        "rules_excerpt": strip_html(rules, limit=300),
                    },
                )
            )
        return AdapterResult(
            source=self.source_name,
            status="ok" if items else "empty",
            items=items,
            resolved_via=self.LIST_URL,
        )


__all__ = ["SherlockAdapter", "YesWeHackAdapter"]
