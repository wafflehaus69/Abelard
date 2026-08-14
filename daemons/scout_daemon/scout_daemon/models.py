"""The normalized item shape every adapter produces.

`RawItem` is deliberately wide and mostly-optional. Adapters fill what their
source actually publishes and leave the rest `None` -- an unset field is
honest, a guessed one is not. Phase 2 maps this onto the ledger; the fields
here exist because SC-R1 observed them in real payloads, not because a schema
sketch predicted them.

The payout decomposition is not over-engineering. SC-R1 sampled four genuinely
different payout shapes across the roster: a fixed `5000 USDC` (Superteam), a
token amount needing `amount / 10^exp * usdPrice` (Dework), a program POOL of
`committed: 150000` (Questbook), and a commission string "Up To $1,850 Per CPA
lead" (Affiliate.Watch). Collapsing those into one number is a magnitude error
of the CD-R1 R2 species, so `payout_raw` is always kept verbatim alongside
whatever was parsed out of it.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


# payout_kind
FIXED = "fixed"
RANGE = "range"
POOL = "pool"
COMMISSION = "commission"
UNSTATED = "unstated"

# payout_basis -- how to read the number, and the reason ranking is not a
# single numeric axis. A $10M program pool is not comparable to a $500 bounty.
PER_TASK = "per_task"
PROGRAM_POOL = "program_pool"
PER_SALE_COMMISSION = "per_sale_commission"

# payout_confidence -- a scraped number is a claim until something backs it.
ESCROWED = "escrowed"
CLAIMED = "claimed"
UNVERIFIED = "unverified"

# identity_gate, ascending strictness. `proof_of_humanity` is the hard RED
# trigger; `kyc` alone is YELLOW, because KYC is ordinary compliance and
# conflating the two would wrongly redden legitimate grant programs.
GATE_NONE = "none"
GATE_ACCOUNT = "account"
GATE_KYC = "kyc"
GATE_PROOF_OF_HUMANITY = "proof_of_humanity"


# payout_asset_class
ASSET_FIAT = "fiat"
ASSET_STABLECOIN = "stablecoin"
ASSET_VOLATILE_TOKEN = "volatile_token"
ASSET_POINTS = "points_or_xp"

_FIAT = frozenset({"USD", "EUR", "GBP", "CHF", "INR", "JPY", "CAD", "AUD"})
# Deliberately a SHORT list of widely-redeemable dollar-pegged tokens.
# NECESSARILY INCOMPLETE, and erring toward `volatile_token` is the safe
# direction: mislabelling a stablecoin as volatile costs a look; mislabelling
# a volatile token as stable understates what the tribe would be holding.
_STABLE = frozenset({"USDC", "USDT", "USDG", "DAI", "PYUSD", "USDP", "TUSD"})


def classify_asset(currency: str | None) -> str | None:
    """Asset class from a currency/token symbol. None when unstated."""
    if not currency:
        return None
    symbol = currency.strip().upper()
    if symbol in _FIAT:
        return ASSET_FIAT
    if symbol in _STABLE:
        return ASSET_STABLECOIN
    if symbol in {"XP", "POINTS", "POINT", "REP", "KARMA"}:
        return ASSET_POINTS
    return ASSET_VOLATILE_TOKEN


@dataclass
class RawItem:
    """One opportunity as fetched, before classification."""

    source: str
    native_id: str
    title: str

    url: str | None = None
    category: str | None = None
    category_source: str | None = None      # structured | derived | source_constant
    counterparty: str | None = None
    counterparty_verified: bool | None = None

    payout_raw: str | None = None
    payout_usd_low: float | None = None
    payout_usd_high: float | None = None
    payout_currency: str | None = None
    payout_kind: str = UNSTATED
    payout_basis: str = PER_TASK
    payout_confidence: str = UNVERIFIED
    escrow_verified: bool | None = None

    deadline_unix: int | None = None
    posted_unix: int | None = None

    identity_gate: str | None = None
    agent_permitted: str | None = None       # yes | no | unstated
    natural_person_required: bool | None = None
    capital_required_usd: float | None = None
    paid_acquisition: bool | None = None

    # White-hat regime (SC-1 §B). scope_text is captured VERBATIM and never
    # inferred: a program with no published scope cannot be admitted as
    # white-hat, and SC-1 records scope without ever acting on it.
    scope_published: bool | None = None
    scope_text: str | None = None
    safe_harbor_text: str | None = None

    # Asset class and indicative basis (Mando 2026-08-10). Token-denominated
    # compensation stays GREEN-eligible, but the asset posture must be visible
    # rather than inferred from a currency string.
    #
    # NOTE ON "BASIS": the scout sees LISTINGS, never RECEIPTS. It does not
    # execute work, so it never witnesses a payment. `indicative_usd_at_discovery`
    # is a quoted price at discovery WITH its provenance -- not a cost basis,
    # which is set when tokens actually land and belongs to whatever accepts
    # payment. Recording `price_source` is the point: an illiquid token's
    # quoted price should stay identifiable as a quote, not become a number.
    payout_asset_class: str | None = None
    payout_token_symbol: str | None = None
    payout_token_quantity: float | None = None
    indicative_usd_at_discovery: float | None = None
    price_source: str | None = None

    # Contention -- the one observable input to EXPECTED gain. Effort is not
    # measurable from a listing (the scout sees listings, never outcomes), but
    # how many others are competing for the same payout often is.
    #
    # `award_rate` carries the SAME provenance discipline as token prices:
    # the field it came from is named, because a rate derived from one source's
    # counters is not comparable to a rate guessed from another's, and a stale
    # counter should stay identifiable as one.
    contention: int | None = None          # applicants / submissions / entrants
    award_rate: float | None = None        # P(award), 0..1, only where PUBLISHED
    award_rate_source: str | None = None   # e.g. "questbook:selected/applications"

    effort_note: str | None = None
    tos_flags: list[str] = field(default_factory=list)

    # Provenance
    resolved_via: str = ""
    raw: dict[str, Any] = field(default_factory=dict)

    def extraction_hits(self) -> dict[str, bool]:
        """Which gate fields this item actually carries.

        The recon's field-fit gate was `title + payout + category`. Phase 1
        measures the same three against live data so predicted and observed
        yield are comparable rather than two different metrics wearing one name.
        """
        return {
            "title": bool(self.title and self.title.strip()),
            "payout": self.payout_usd_low is not None
            or self.payout_usd_high is not None
            or bool(self.payout_raw),
            "category": bool(self.category),
        }

    def field_fit(self) -> bool:
        return all(self.extraction_hits().values())


__all__ = [
    "RawItem",
    "FIXED", "RANGE", "POOL", "COMMISSION", "UNSTATED",
    "PER_TASK", "PROGRAM_POOL", "PER_SALE_COMMISSION",
    "ESCROWED", "CLAIMED", "UNVERIFIED",
    "GATE_NONE", "GATE_ACCOUNT", "GATE_KYC", "GATE_PROOF_OF_HUMANITY",
]
