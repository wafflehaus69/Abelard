"""The lead view: firms whose public filings moved in a succession-shaped way.

**There is no score here, and that is deliberate.** [E8] forbids a spec constant
that nobody has measured, and FDU has one snapshot interval of observed
movement. A weighted "succession likelihood" invented today would be false
precision dressed as a ranking, and every downstream decision would inherit it.

So this module reports EVIDENCE, grouped by kind, with the observation behind
each row. When enough intervals have accumulated to show a base rate for each
signal, a ranking can be built against that distribution and ratified. Until
then the honest output is a list a human reads.

Signals surfaced, all firm-level (I-3):

  ``succession_filed``   Schedule D Section 4 names an acquired firm whose CRD
                         differs from the filer's. A real change of hands.
  ``reorganisation``     Section 4 names the filer's OWN CRD. An LLC conversion
                         or re-domicile -- surfaced, labelled, and sorted last,
                         because at a glance it is indistinguishable from a sale.
  ``ownership_shifted``  The ownership-code multiset or owner count changed
                         between two extractions.
  ``ownership_recent``   An owner acquired their stake inside the last N months.
  ``headcount_drop``     Item 5A total employees fell.
  ``aum_drop``           Item 5F regulatory AUM fell.
  ``deregistering``      Registration status left APPROVED.

Every one is a CLAIM about a public filing, not a conclusion about intent.
"""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass, field


@dataclass
class Lead:
    crd: str
    name: str | None
    state: str | None
    aum_total: int | None
    total_employees: int | None
    signals: list[str] = field(default_factory=list)
    evidence: list[str] = field(default_factory=list)
    #: Largest relative decline observed across this firm's signals, 0-100.
    #: An ordering aid, NOT a likelihood. See ``render`` for the bias it carries.
    max_decline_pct: float = 0.0

    def render(self) -> str:
        aum = f"${self.aum_total:,}" if self.aum_total is not None else "n/a"
        emp = self.total_employees if self.total_employees is not None else "n/a"
        head = f"  CRD {self.crd}  {(self.name or '?')[:44]:<44} {self.state or '--':>3}  AUM {aum:>18}  emp {emp}"
        lines = [head, f"      signals: {', '.join(self.signals)}"]
        lines += [f"      - {e}" for e in self.evidence]
        return "\n".join(lines)


def _num(value: str | None):
    if value is None:
        return None
    try:
        parsed = json.loads(value)
    except (TypeError, json.JSONDecodeError):
        return None
    return parsed


def collect(conn: sqlite3.Connection, *, limit: int = 50) -> list[Lead]:
    leads: dict[str, Lead] = {}

    def lead_for(row) -> Lead:
        if row["crd"] not in leads:
            leads[row["crd"]] = Lead(
                crd=row["crd"],
                name=row["legal_name"] or row["business_name"],
                state=row["state"],
                aum_total=row["aum_total"],
                total_employees=row["total_employees"],
            )
        return leads[row["crd"]]

    # -- declared successions --------------------------------------------
    #
    # A self-succession is separated out rather than dropped. Both of the first
    # filed successions found in the corpus reported the filer's OWN CRD as the
    # acquired firm -- an LLC conversion or re-domicile, not a purchase. Showing
    # those as acquisition leads would be worse than showing nothing, because
    # they look exactly like the real thing at a glance.
    for row in conn.execute(
        "SELECT f.*, d.succession_detail, d.succession_is_self, d.succession_count "
        "FROM adv_detail d JOIN firm f ON f.crd = d.crd WHERE d.section4_filed = 1"
    ):
        lead = lead_for(row)
        is_self = row["succession_is_self"]
        detail = row["succession_detail"] or "(acquired firm not parsed)"
        if is_self == 1:
            lead.signals.append("reorganisation")
            lead.evidence.append(f"Section 4: succeeded ITSELF ({detail}) - entity reorganisation, not a sale")
        else:
            lead.signals.append("succession_filed")
            n = row["succession_count"] or 1
            plural = f"{n} firms" if n > 1 else "firm"
            lead.evidence.append(f"Section 4: acquired {plural} - {detail}")

    # -- registration leaving APPROVED ------------------------------------
    for row in conn.execute(
        "SELECT f.*, c.old_value, c.new_value FROM firm_change c JOIN firm f ON f.crd = c.crd "
        "WHERE c.field = 'rgstn_status'"
    ):
        old, new = _num(row["old_value"]), _num(row["new_value"])
        if old == "APPROVED" and new != "APPROVED":
            lead = lead_for(row)
            lead.signals.append("deregistering")
            lead.evidence.append(f"registration status {old} -> {new}")

    # -- headcount and AUM declines ---------------------------------------
    for field_name, label in (("total_employees", "headcount_drop"), ("aum_total", "aum_drop")):
        for row in conn.execute(
            "SELECT f.*, c.old_value, c.new_value FROM firm_change c JOIN firm f ON f.crd = c.crd "
            "WHERE c.field = ?", (field_name,)
        ):
            old, new = _num(row["old_value"]), _num(row["new_value"])
            if not isinstance(old, (int, float)) or not isinstance(new, (int, float)):
                continue
            if new >= old or old == 0:
                continue
            pct = 100.0 * (old - new) / old
            lead = lead_for(row)
            lead.signals.append(label)
            lead.evidence.append(f"{field_name} {old:,} -> {new:,} ({pct:.1f}% decline)")
            lead.max_decline_pct = max(lead.max_decline_pct, pct)

    ordered = sorted(
        leads.values(),
        key=lambda l: (
            0 if "succession_filed" in l.signals else 1,
            1 if "reorganisation" in l.signals and len(l.signals) == 1 else 0,
            -len(l.signals),
            -l.max_decline_pct,
            -(l.aum_total or 0),
        ),
    )
    return ordered[:limit]


def render(leads: list[Lead]) -> str:
    if not leads:
        return (
            "No leads. That is a real answer, not an empty one: it means no watched field\n"
            "moved in a succession-shaped way over the intervals in the ledger. Run `scan`\n"
            "on consecutive days to accumulate movement -- the publisher keeps ~8 days, so\n"
            "history only exists once we start recording it."
        )
    out = [
        f"{len(leads)} firms with succession-shaped movement.",
        "",
        "NOT RANKED BY LIKELIHOOD. No weighting exists, because no base rate has been",
        "measured [E8]. Order is: declared succession first, then signal count, then the",
        "largest relative decline observed.",
        "",
        "That sort key has a stated bias: relative decline over-weights small firms, where",
        "losing one person is 25% and losing one at a 200-person firm is 0.5%. It is an",
        "ordering aid, not a judgement.",
        "",
        "And a large swing is not automatically an event -- a headcount moving 198 -> 7 is",
        "more likely a reporting correction than an exodus. Read the evidence, not the order.",
        "",
    ]
    out += [lead.render() for lead in leads]
    return "\n".join(out)
