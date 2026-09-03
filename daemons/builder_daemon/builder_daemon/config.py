"""Configuration. Carries no credential against any target, and never will.

WHAT IS DELIBERATELY ABSENT: there is no forge token, no platform login, no
wallet, no signing key, no `.env` key naming a counterparty. Invariant 3 is
enforced here by there being nothing to enforce -- a daemon that only ever
issues unauthenticated GETs has nothing to authenticate with. The structural
test in `tests/test_soul.py` asserts the absence rather than trusting it.

The one credential this daemon may hold is its own model API key, which buys
compute from Anthropic and reaches no counterparty of the work. That is a
different kind of thing from a GitHub token and the distinction is the whole
of invariant 3.
"""

from __future__ import annotations

import os
from pathlib import Path

_PACKAGE_DIR = Path(__file__).resolve().parent
_DAEMON_ROOT = _PACKAGE_DIR.parent

# ---------------------------------------------------------------------------
# State
# ---------------------------------------------------------------------------

STATE_HOME = Path.home() / ".openclaw" / "builder"
DB_PATH = STATE_HOME / "builder.sqlite3"
AUDIT_LOG_PATH = STATE_HOME / "audit.jsonl"
PACKET_DIR = STATE_HOME / "packets"

#: Scout's ledger. Opened READ-ONLY (`mode=ro`), always. The two daemons share a
#: fact base, not a connection: nothing here may write a row scout owns, and
#: `intake.connect_scout()` is the only opener.
SCOUT_DB_PATH = Path.home() / ".openclaw" / "scout" / "scout.sqlite3"

HALT_FILE = STATE_HOME / "HALT"
HALT_ENV_VAR = "BUILDER_HALT"

# ---------------------------------------------------------------------------
# Forges
# ---------------------------------------------------------------------------

#: Hosts whose issue URLs this daemon knows how to read.
#:
#: INVARIANT 6 APPLIES, AND THE DIRECTION MATTERS. This is a name list, and a
#: name list is never a rule on its own. Here it is safe because it can only
#: ever NARROW: an unrecognised host makes an item not-code-PR, so the item is
#: excluded from work. The list can cause the Builder to skip work it could have
#: done; it can never cause the Builder to take on work it cannot verify.
#:
#: That is the asymmetric-error ruling pointed at intake -- unknown resolves to
#: "not mine", never to "mine". Adding a host is a deliberate act with a test.
FORGE_HOSTS = frozenset({"github.com", "gitlab.com", "codeberg.org"})

USER_AGENT = (
    "AbelardBuilder/0.1 (+code-PR drafter; read-only; drafts only, never submits; "
    "contact mdibar05@gmail.com)"
)

HTTP_TIMEOUT_S = 30.0
HTTP_MIN_INTERVAL_S = 1.0

# ---------------------------------------------------------------------------
# Model
# ---------------------------------------------------------------------------

DRAFTER_MODEL_ID = "claude-sonnet-4-6"
DRAFTER_MAX_TOKENS = 32000


def state_home() -> Path:
    STATE_HOME.mkdir(parents=True, exist_ok=True)
    PACKET_DIR.mkdir(parents=True, exist_ok=True)
    return STATE_HOME


def halted() -> bool:
    """Kill switch. Either form halts; the file form needs no shell access."""
    if os.environ.get(HALT_ENV_VAR, "").strip() not in ("", "0", "false", "False"):
        return True
    return HALT_FILE.exists()


__all__ = [
    "STATE_HOME", "DB_PATH", "AUDIT_LOG_PATH", "PACKET_DIR", "SCOUT_DB_PATH",
    "HALT_FILE", "HALT_ENV_VAR", "FORGE_HOSTS", "USER_AGENT",
    "HTTP_TIMEOUT_S", "HTTP_MIN_INTERVAL_S",
    "DRAFTER_MODEL_ID", "DRAFTER_MAX_TOKENS",
    "state_home", "halted",
]
