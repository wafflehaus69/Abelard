"""The input contract (invariant 1): admitted rows of type code-PR, and nothing else.

TWO SEPARATE CLAIMS, BOTH ENFORCED HERE.

**Admitted only.** `select_work()` is the ONLY function that returns a set of
items to work on, and its SQL carries `status='admitted'` as a literal, not as a
parameter a caller could vary. There is no query in this package that reads
`discovered` or `proposed` rows, and none that orders by rank, segment or
payout. The Builder therefore cannot see the ranked queue and cannot choose its
own work -- not because it is configured not to, but because the code to do it
does not exist.

**Code-PR only, BY SHAPE, NOT BY NAME.** Scout's ledger has no work-type column.
Its `category` field is source-supplied and, for the bounty sources, holds the
repository's *programming language* -- 'Rust', 'TypeScript', 'C++, C, GLSL'.
Measured 2026-09-02: of the 23 agent-eligible GREEN rows, the category values
were twelve distinct language strings and the word 'bounty'. There is no
'code_pr' category to filter on and inventing one from a name list is exactly
the YesWeHack failure (invariant 6).

So the type is established structurally: a URL identifying a single ISSUE on a
known code forge, with a per-task payout, in the agent-eligible segment. That is
a fact about the work item, not a label somebody typed.

THE REHEARSAL CARVE-OUT, AND WHY IT IS NOT A HOLE. `load_one()` takes an
explicit `opportunity_id` and returns that row whatever its status. It does not
breach invariant 1 because it does not SELECT WORK: it cannot search, cannot
rank, and cannot return more than the single row whose id the caller already
knew. Mando naming a row is not the daemon choosing one. The distinction is
load-bearing and the tests assert both halves of it.
"""

from __future__ import annotations

import json
import re
import sqlite3
from dataclasses import dataclass
from pathlib import Path

from . import config
from .errors import IntakeError

#: A URL that identifies ONE issue on a code forge. GitHub/Codeberg use
#: `/owner/repo/issues/N`; GitLab uses `/owner/repo/-/issues/N`.
_ISSUE_URL = re.compile(
    r"^https?://(?P<host>[\w.-]+)/(?P<owner>[^/\s]+)/(?P<repo>[^/\s]+)"
    r"/(?:-/)?issues/(?P<number>\d+)/?$"
)

#: The segment scout assigns to work an agent may do at all. POOL and
#: HUMAN_ONLY are diverted upstream and must not reach the Builder.
AGENT_ELIGIBLE_SEGMENT = "GREEN"

#: Per-task, as opposed to a prize pool where payment is contingent on placing.
PER_TASK = "per_task"


@dataclass(frozen=True)
class WorkItem:
    """One admitted, code-PR-shaped unit of work."""

    opportunity_id: str
    source: str
    title: str
    url: str
    host: str
    owner: str
    repo: str
    issue_number: int
    payout_usd_low: float | None
    status: str
    raw: dict

    @property
    def repo_slug(self) -> str:
        return f"{self.owner}/{self.repo}"

    @property
    def short_id(self) -> str:
        return self.opportunity_id[:12]

    @property
    def is_admitted(self) -> bool:
        return self.status == "admitted"


def issue_shape(url: str | None) -> dict | None:
    """Parse an issue URL, or return None. Pure; no network, no database.

    Returns None for anything that is not a single-issue URL on a known forge --
    a repository root, a pull request, a discussion, a listing page, a host we
    do not read. Unknown always resolves to None, never to a guess.
    """
    if not url:
        return None
    m = _ISSUE_URL.match(url.strip())
    if not m:
        return None
    host = m.group("host").lower()
    if host.startswith("www."):
        host = host[4:]
    if host not in config.FORGE_HOSTS:
        return None
    return {
        "host": host,
        "owner": m.group("owner"),
        "repo": m.group("repo"),
        "number": int(m.group("number")),
    }


def is_code_pr(row) -> bool:
    """Does this ledger row denote code-PR work? Shape only.

    All three conditions are required. Dropping any one of them admits
    something the Builder cannot act on: a pool row pays on placement rather
    than on delivery, a HUMAN_ONLY row was already judged to need a person, and
    a non-issue URL has no defect to fix.
    """
    if issue_shape(_get(row, "url")) is None:
        return False
    if _get(row, "payout_basis") != PER_TASK:
        return False
    return _get(row, "rank_segment") == AGENT_ELIGIBLE_SEGMENT


def _get(row, key: str):
    try:
        return row[key]
    except (KeyError, IndexError, TypeError):
        return None


def connect_scout(path: Path | None = None) -> sqlite3.Connection:
    """Open scout's ledger READ-ONLY. The only opener in this package.

    `mode=ro` is a SQLite-level guarantee: a write through this handle raises
    rather than succeeding. Invariant 1's second half -- that the Builder cannot
    disturb the fact base it reads -- is therefore enforced by the driver, not
    by our discipline in never writing.
    """
    p = Path(path) if path else config.SCOUT_DB_PATH
    if not p.exists():
        raise IntakeError(f"scout ledger not found at {p}")
    conn = sqlite3.connect(f"file:{p.as_posix()}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    return conn


#: The one query that selects work. `status='admitted'` is a literal in the SQL
#: and there is no parameter that could relax it.
_SELECT_ADMITTED = (
    "SELECT opportunity_id, source, title, url, payout_usd_low, payout_basis,"
    " rank_segment, status, raw_json"
    " FROM opportunities WHERE status='admitted'"
)


def select_work(conn: sqlite3.Connection) -> list[WorkItem]:
    """Every admitted, code-PR-shaped item. The Builder's entire input.

    Returns them in a stable, arbitrary order (`opportunity_id`), NOT in rank
    order. The Builder is not permitted a view on which admitted work is worth
    more -- Mando already made that judgment by admitting it, and re-ranking
    here would quietly reintroduce the choosing the invariant forbids.

    An empty list is the expected result until a code row is admitted, and is
    not an error.
    """
    rows = conn.execute(_SELECT_ADMITTED + " ORDER BY opportunity_id").fetchall()
    return [_to_item(r) for r in rows if is_code_pr(r)]


def load_one(conn: sqlite3.Connection, opportunity_id: str) -> WorkItem:
    """Load ONE row by explicit id, for rehearsal. Cannot search, cannot rank.

    See the module docstring: this is not work selection. The caller must
    already know the id, which means a human chose it.
    """
    if not opportunity_id or not str(opportunity_id).strip():
        raise IntakeError("load_one requires an explicit opportunity_id")
    key = str(opportunity_id).strip()
    row = conn.execute(
        "SELECT opportunity_id, source, title, url, payout_usd_low, payout_basis,"
        " rank_segment, status, raw_json FROM opportunities"
        " WHERE opportunity_id=? OR substr(opportunity_id,1,12)=?",
        (key, key),
    ).fetchone()
    if row is None:
        raise IntakeError(f"no ledger row for {key!r}")
    if issue_shape(row["url"]) is None:
        raise IntakeError(
            f"{key!r} is not code-PR shaped: url={row['url']!r} is not a forge issue"
        )
    return _to_item(row)


def _to_item(row) -> WorkItem:
    shape = issue_shape(row["url"])
    if shape is None:
        raise IntakeError(f"{row['opportunity_id']!r} has no issue shape")
    try:
        raw = json.loads(row["raw_json"] or "{}")
    except (TypeError, ValueError):
        raw = {}
    return WorkItem(
        opportunity_id=row["opportunity_id"],
        source=row["source"],
        title=row["title"] or "",
        url=row["url"],
        host=shape["host"],
        owner=shape["owner"],
        repo=shape["repo"],
        issue_number=shape["number"],
        payout_usd_low=row["payout_usd_low"],
        status=row["status"],
        raw=raw if isinstance(raw, dict) else {},
    )


__all__ = [
    "WorkItem", "AGENT_ELIGIBLE_SEGMENT", "PER_TASK",
    "issue_shape", "is_code_pr", "connect_scout", "select_work", "load_one",
]
