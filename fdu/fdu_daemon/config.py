"""FDU configuration: state home, source endpoints, and the containment constants.

State lives OUTSIDE the repo at ``~/.openclaw/fdu/`` per the cross-daemon
convention. Keeping it out of any cloud-sync tree is not a style choice: a sync
client corrupts SQLite mid-write and the failure is intermittent. Re-verify on
any new host.
"""

from __future__ import annotations

import os
from pathlib import Path

from .errors import ConfigError

# --------------------------------------------------------------------------
# Identity
# --------------------------------------------------------------------------

DAEMON_NAME = "fdu"

# The declared User-Agent. SEC Fair Access asks operators to declare their
# traffic. We declare what this is and that it is read-only.
#
# It carries NO contact address by ruling R-PA1-2 (Mando, 2026-08-21): Mando's
# personal address is not declared to federal systems, and inventing one would
# be worse than declaring none. The consequence is measured, not assumed --
# www.sec.gov returns 403 to this UA from two independent networks. The hosts
# FDU actually reads (reports.adviserinfo.sec.gov) serve it without complaint.
#
# If a project contact address is ever provisioned (open question Q6), set
# FDU_CONTACT and it is appended here.
_CONTACT = os.environ.get("FDU_CONTACT", "").strip()
USER_AGENT = (
    f"Abelard-FDU/0.1 (read-only research sensor; {_CONTACT})"
    if _CONTACT
    else "Abelard-FDU/0.1 (read-only research sensor)"
)

# --------------------------------------------------------------------------
# Sources -- all read-only, all GET
# --------------------------------------------------------------------------

IAPD_REPORTS_BASE = "https://reports.adviserinfo.sec.gov/reports"
COMPILATION_MANIFEST = f"{IAPD_REPORTS_BASE}/CompilationReports/CompilationReports.manifest.json"
COMPILATION_DIR = f"{IAPD_REPORTS_BASE}/CompilationReports"

#: Per-firm Form ADV document. Carries Item 4 (Successions), Schedule A/B
#: (ownership) and the DRP pages -- none of which appear in the bulk feed.
#: NOT robots-disallowed; the disallowed brochure routes live on a different
#: host path and are never fetched.
ADV_PDF_TEMPLATE = IAPD_REPORTS_BASE + "/ADV/{crd}/PDF/{crd}.pdf"

#: The publisher retains roughly 8 days of dated feeds (measured 2026-08-21:
#: 08_14 through 08_21 present, 08_13 absent). Anything older than this must
#: come from our own archive, because it cannot be re-fetched.
FEED_RETENTION_DAYS = 8

# --------------------------------------------------------------------------
# Pacing -- I-1 "human-plausible rates"
# --------------------------------------------------------------------------

#: Seconds between per-firm document fetches. Deliberately unhurried: the
#: steady-state workload is ~66 documents/day, so there is nothing to gain by
#: going faster and a reputation to lose by it.
ADV_FETCH_DELAY_S = float(os.environ.get("FDU_ADV_DELAY", "2.0"))

#: Hard ceiling on documents fetched in one enrich run. A backfill that wants
#: more says so explicitly with --limit; the default cannot run away.
ADV_FETCH_DEFAULT_LIMIT = 200

HTTP_TIMEOUT_S = 90.0

# --------------------------------------------------------------------------
# State home
# --------------------------------------------------------------------------


def state_home() -> Path:
    """Resolve ``~/.openclaw/fdu/``. ``FDU_STATE_HOME`` overrides."""
    override = os.environ.get("FDU_STATE_HOME")
    base = Path(override) if override else Path.home() / ".openclaw" / DAEMON_NAME
    try:
        base.mkdir(parents=True, exist_ok=True)
    except OSError as exc:  # pragma: no cover - filesystem dependent
        raise ConfigError(f"cannot create state home {base}: {exc}") from exc
    return base


def db_path() -> Path:
    override = os.environ.get("FDU_DB_PATH")
    return Path(override) if override else state_home() / "fdu.sqlite3"


def halt_file() -> Path:
    return state_home() / "HALT"


def halt_requested() -> bool:
    """Two independent kill channels, either sufficient.

    The file form needs no shell access to the running service, which is the
    whole point of having it as well as the environment variable.
    """
    if os.environ.get("FDU_HALT", "").strip() not in ("", "0", "false", "False"):
        return True
    return halt_file().exists()
