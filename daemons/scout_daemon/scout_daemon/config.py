"""Configuration: state home, model pin, and the WIRE source roster.

The roster below is NOT authored here. It is transcribed from
`recon/SC-R1-RECON.md` §3.1 — the twelve surfaces that cleared the
pre-registered >=60% content-fit gate — and every entry was re-probed live
during SC-1 Phase 0 (2026-08-09) before being written down. `recon_field_fit`
is carried per source so Phase 1 can measure real yield against the recon's
prediction and report divergence rather than quietly absorbing it.

No credentials are required. Every WIRE source was selected for readability
without auth, which is what makes invariant 5 (no account creation, ever)
cheap to hold rather than a constraint we are working around.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

from .errors import ConfigError

_PACKAGE_DIR = Path(__file__).resolve().parent
_DAEMON_ROOT = _PACKAGE_DIR.parent
_DOTENV_PATH = _DAEMON_ROOT / ".env"

REDACTED = "***REDACTED***"

# ---------------------------------------------------------------------------
# State home
# ---------------------------------------------------------------------------
# Convention follows biz_daemon/config.py:128 -- `~/.openclaw/<daemon>/`.
#
# Cloud-sync hazard, re-confirmed on Orban 2026-08-09: HOME is C:\Users\mdiba
# and OneDrive is C:\Users\mdiba\OneDrive, a SIBLING of .openclaw, not a
# parent. The state home is therefore outside the sync tree. This is worth
# re-checking on any new host rather than assuming -- a SQLite file inside a
# sync root gets corrupted by the sync client mid-write, and the failure is
# intermittent and awful to diagnose. Basilic equivalent is ~/.openclaw/scout/
# under that host's HOME; verify at migration time, not now.

STATE_HOME = Path.home() / ".openclaw" / "scout"
DB_PATH = STATE_HOME / "scout.sqlite3"
AUDIT_LOG_PATH = STATE_HOME / "audit.jsonl"
QUARANTINE_DIR = STATE_HOME / "quarantine"

# ---------------------------------------------------------------------------
# Model pin
# ---------------------------------------------------------------------------
# Verified against the /claude-api skill's model catalog at build time
# (2026-08-09), not from memory -- per SC-1 Phase 0.4 and the precedent that
# the skill caught a bad Sonnet version assumption before it shipped.
#
# Bare alias, no date suffix: matches chatter_daemon/config.py:134 and
# biz_daemon/config.py:118. Date-suffixed variants are a documented 404 source.
#
# SC-1 orders "one batched Sonnet call", so Sonnet 4.6 is the pin. Recorded
# for the record: SC-R1 §5.5 measured this workload at ~$0.056/scan on Sonnet
# 4.6 ($3/$15 per MTok) versus ~$0.019 on Haiku 4.5 ($1/$5) -- roughly $1.68
# vs $0.57 a month at daily cadence. Both are noise; the pin is the order's
# call and cost is not the constraint. The constant exists so switching is a
# one-line change with a test, not a grep.
CLASSIFIER_MODEL_ID = "claude-sonnet-4-6"

# A no-op below the minimum cacheable prefix and harmless above it. Concretely:
# Sonnet 4.6's minimum is 1024 tokens and the classifier's system prompt is
# expected to land near ~900, so this likely will NOT engage at SC-1 sizes.
# Assume no cache savings in any cost projection until `cache_read_input_tokens`
# is observed non-zero on real scans.
CLASSIFIER_CACHE_BREAKPOINT = True

# Bounded client. The SDK default (600s timeout x 3 retries) can hang an
# unattended scan for ~30 minutes; biz_daemon/sentiment.py:167 sets the
# precedent for pinning both.
#
# Raised from 60s to 300s on 2026-08-11: one batched call over ~237 items
# generates ~11k output tokens, and 60s was not enough even streamed. Still
# bounded -- worst case is 300s x 2 retries on a single call, not half an hour.
ANTHROPIC_TIMEOUT_S = 300.0
ANTHROPIC_MAX_RETRIES = 2

# SC-1 Phase 3C orders ONE batched call per scan, so the batch is sized to
# cover the whole ambiguous+GREEN set (~237 items today) in a single request.
#
# This trades away the bounded-blast-radius property Phase 2 borrowed from
# biz_daemon's ATTENTION_BATCH_SIZE: with one call, a truncation or a malformed
# response degrades the ENTIRE pass to YELLOW-with-the-failure-as-reason rather
# than one chunk of eight. That degradation is loud and safe (never GREEN), but
# it is all-or-nothing. The guard against it is `max_tokens` headroom below.
CLASSIFY_BATCH_SIZE = 250

# Output ceiling for the classification call. ~237 items x ~45 tokens of
# verdict+reason is ~11k; 32k leaves room for a verbose batch without risking
# the `stop_reason == "max_tokens"` truncation that would void the whole pass.
CLASSIFY_MAX_TOKENS = 32000

# ---------------------------------------------------------------------------
# Kill switch (SC-1 Phase 3.4)
# ---------------------------------------------------------------------------
# Cheap now, load-bearing later. Two independent halts, either sufficient:
#   - env:  SCOUT_HALT=1
#   - file: ~/.openclaw/scout/HALT  (touch it; no content read)
# The file form exists so a halt survives a shell that isn't the daemon's and
# can be thrown by a human with no access to the service definition.
HALT_FILE = STATE_HOME / "HALT"
HALT_ENV_VAR = "SCOUT_HALT"


def fetching_halted() -> bool:
    """True when either halt channel is engaged. Checked before every fetch."""
    if os.environ.get(HALT_ENV_VAR, "").strip() not in ("", "0", "false", "False"):
        return True
    return HALT_FILE.exists()


# ---------------------------------------------------------------------------
# Source roster
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Source:
    """One WIRE surface.

    `lane` exists because SC-1's revised rubric treats affiliate/creator
    surfaces as YELLOW-conditional rather than ordinarily admissible, and
    because Mando's Q2 ruling gates whether that lane surfaces at all. Keeping
    it on the source (not inferred at classification time) means the gate is
    declarative and greppable.

    `recon_field_fit` is the SC-R1 measurement, carried so Phase 1 can report
    real-yield divergence. It is evidence, never a threshold -- no scoring
    weight ships without an observed distribution behind it.
    """

    name: str
    base_url: str
    access: str          # 'json_api' | 'graphql' | 'ssr_html'
    lane: str            # 'work' | 'grant' | 'affiliate' | 'agent_native'
    recon_field_fit: float
    recon_note: str = ""


# Transcribed from recon/SC-R1-RECON.md §3.1. All twelve re-probed live
# 2026-08-09; all returned HTTP 200 with non-empty payloads.
WIRE_SOURCES: tuple[Source, ...] = (
    Source(
        name="superteam_earn",
        base_url="https://superteam.fun/api/listings",
        access="json_api",
        lane="work",
        recon_field_fit=95.2,
        recon_note="Entire open inventory in one unauth GET. Per-item agentAccess flag "
                   "(20 of 21 HUMAN_ONLY at recon) -- carry it, do not collapse it.",
    ),
    Source(
        name="questbook",
        base_url="https://api-grants.questbook.app/graphql",
        access="graphql",
        lane="grant",
        recon_field_fit=77.0,
        recon_note="reward.committed is a PROGRAM POOL, not a per-task payout. "
                   "payout_basis must say so or ranking is meaningless.",
    ),
    Source(
        name="dework",
        base_url="https://api.deworkxyz.com/graphql",
        access="graphql",
        lane="work",
        recon_field_fit=100.0,
        recon_note="Extraction perfect, platform dormant: ~97.6% of the default feed is "
                   "reward-less, much of the rewarded set is 2022-era. Filter rewards != [] "
                   "and let the ledger settle dormancy with data (open question Q3).",
    ),
    Source(
        name="giveth_qf",
        base_url="https://mainnet.serve.giveth.io/graphql",
        access="graphql",
        lane="grant",
        recon_field_fit=94.4,
        recon_note="Full population is ~18 rounds; low cadence. No per-item category -- "
                   "category_source is 'source_constant' here.",
    ),
    Source(
        name="opire",
        base_url="https://api.opire.dev/rewards",
        access="json_api",
        lane="work",
        recon_field_fit=83.3,
        recon_note="pendingPrice is a CLAIM, not escrow. Recon sampled a $1,260,988 reward "
                   "on a throwaway repo with isBotInstalled=false. Never persist as verified.",
    ),
    Source(
        name="zindi",
        base_url="https://api.zindi.world/v1/competitions",
        access="json_api",
        lane="work",
        recon_field_fit=100.0,
        recon_note="Data-science competitions. Multi-currency rewards (USD/CHF/EUR) as free "
                   "text -- payout_currency is not always USD.",
    ),
    Source(
        name="dealwork",
        base_url="https://dealwork.ai/api/v1/jobs",
        access="json_api",
        lane="agent_native",
        recon_field_fit=100.0,
        recon_note="Agent-native marketplace -- the category SC-R1 discovered that the seed "
                   "list had no concept of. Small and micro-budget; significance is categorical.",
    ),
    Source(
        name="opentask",
        base_url="https://opentask.ai/tasks",
        access="ssr_html",
        lane="agent_native",
        recon_field_fit=100.0,
        recon_note="ToS: use documented APIs, do not scrape. Prefer the documented API path "
                   "if one is reachable without an account; otherwise read the SSR page only.",
    ),
    Source(
        name="ef_esp",
        base_url="https://esp.ethereum.foundation/",
        access="ssr_html",
        lane="grant",
        recon_field_fit=100.0,
        recon_note="Clean pipe, thin catch: 2 open RFPs at <=$300/$500 at recon. robots.txt "
                   "disallows /api/ -- read the SSR pages, never the API path.",
    ),
    Source(
        name="arbitrum_grants",
        base_url="https://arbitrum.foundation/grants",
        access="ssr_html",
        lane="grant",
        recon_field_fit=61.5,
        recon_note="Program pools, not per-award payouts. 4 of 13 active. Best used as a "
                   "change-detection target.",
    ),
    Source(
        name="affiliate_watch",
        base_url="https://affiliate.watch/",
        access="ssr_html",
        lane="affiliate",
        recon_field_fit=100.0,
        recon_note="AFFILIATE LANE -- YELLOW-conditional per SC-1 rubric. Commission terms, "
                   "no deadline, no per-task payout. robots.txt disallows '?page=' pagination "
                   "and blocks named AI crawlers; use the sitemap + detail pages.",
    ),
    Source(
        name="affpaying",
        base_url="https://www.affpaying.com/affiliate-programs",
        access="ssr_html",
        lane="affiliate",
        recon_field_fit=80.0,
        recon_note="AFFILIATE LANE -- YELLOW-conditional. Fully permissive robots. Heavy "
                   "iGaming/adult skew: expect a high RED-by-method rate downstream.",
    ),
    # --- Added by the SC-1 roster ruling, 2026-08-10 ------------------------
    # These two were MEASURED in SC-R1 (Sherlock 100% n=5, YesWeHack 97.6% n=42)
    # but excluded from the WIRE roster because the then-current rubric classed
    # security research RED-for-identification. The white-hat carve-out moved
    # the category to YELLOW-per-program, which made that roster stale by
    # construction -- a rubric change expires the roster it produced. The
    # recon field-fit numbers are reused as the baseline because they are real
    # measurements of these same endpoints, not estimates.
    Source(
        name="sherlock",
        base_url="https://mainnet-contest.sherlock.xyz/contests",
        access="json_api",
        lane="whitehat",
        recon_field_fit=100.0,
        recon_note="WHITE-HAT -- YELLOW-per-program. Best-instrumented source on the "
                   "roster: `requires_kyc` is a first-class boolean and `scope` is a "
                   "structured repo array pinned to commit hashes. Detail fetch required "
                   "for both gates.",
    ),
    Source(
        name="yeswehack",
        base_url="https://api.yeswehack.com/programs",
        access="json_api",
        lane="whitehat",
        recon_field_fit=97.6,
        recon_note="WHITE-HAT -- YELLOW-per-program. The sixth security platform: a fixed "
                   "five-name domain list admitted it as ordinary work at 97.6% field-fit "
                   "during SC-R1. Scope is structured; KYC is NOT a field and must be "
                   "detected from prose, so natural_person_required is often unknown.",
    ),
)

SOURCES_BY_NAME = {s.name: s for s in WIRE_SOURCES}

# Identifies this daemon to every surface it reads. A real contact string is a
# courtesy that costs nothing and buys goodwill when a source operator notices
# the traffic.
USER_AGENT = "AbelardScout/0.1 (+income-discovery sensor; read-only; contact mdibar05@gmail.com)"


def _load_dotenv(path: Path | None = None) -> None:
    """Load the daemon's .env, filling only gaps (shell vars win)."""
    load_dotenv(path if path is not None else _DOTENV_PATH, override=False)


def ensure_state_home() -> Path:
    """Create the state home if absent and return it."""
    STATE_HOME.mkdir(parents=True, exist_ok=True)
    QUARANTINE_DIR.mkdir(parents=True, exist_ok=True)
    return STATE_HOME


def anthropic_api_key(*, required: bool = True) -> str | None:
    """Resolve the Anthropic key.

    Not required at spine startup -- only at the classification pass's
    invocation, matching chatter_daemon's rule that source keys become
    required at their plugin's call site rather than at import.
    """
    _load_dotenv()
    key = os.environ.get("ANTHROPIC_API_KEY", "").strip()
    if key:
        return key
    if required:
        raise ConfigError(
            "ANTHROPIC_API_KEY is required for the classification pass. "
            "Set it in daemons/scout_daemon/.env or the shell."
        )
    return None


def configure_logging(level: int = logging.INFO) -> logging.Logger:
    """Daemon logger. Injected into HttpClient so redaction routes through it."""
    logger = logging.getLogger("scout_daemon")
    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(
            logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s")
        )
        logger.addHandler(handler)
    logger.setLevel(level)
    return logger


__all__ = [
    "STATE_HOME",
    "DB_PATH",
    "AUDIT_LOG_PATH",
    "QUARANTINE_DIR",
    "HALT_FILE",
    "HALT_ENV_VAR",
    "CLASSIFIER_MODEL_ID",
    "CLASSIFIER_CACHE_BREAKPOINT",
    "ANTHROPIC_TIMEOUT_S",
    "ANTHROPIC_MAX_RETRIES",
    "CLASSIFY_BATCH_SIZE",
    "USER_AGENT",
    "Source",
    "WIRE_SOURCES",
    "SOURCES_BY_NAME",
    "fetching_halted",
    "ensure_state_home",
    "anthropic_api_key",
    "configure_logging",
]
