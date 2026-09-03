"""The provenance packet -- the primary artifact. The patch is its attachment.

WHY THE DEPENDENCY RUNS THIS WAY. A patch that cannot say where it came from is
not a deliverable: nobody can review it, nobody can attest to it, and the person
whose name would go on it cannot honestly claim it. So `emit()` takes the packet
as its first and required argument and the patch as an optional attachment.
There is no function here that writes a patch on its own.

That ordering is the enforcement. A packet can exist without a patch -- that is
every decline, and declines are first-class -- but a patch cannot exist without
a packet. `tests/test_soul.py` asserts the asymmetry, because "remember to write
the packet too" is exactly the kind of rule that decays into an empty template
under deadline.

WHAT GOES IN, per the SOUL:
    sources read          every URL fetched, and what was taken from it
    licences touched      of the repository, and of anything consulted
    generated vs adapted  which lines are fresh, which derived, and from where
    claims made           every assertion about behaviour, and how it was checked
    policy findings       gate one's full result, including the obligations that
                          attach to submission
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from pathlib import Path

from .outcomes import Outcome, Verdict


@dataclass(frozen=True)
class Provenance:
    """One line of the generated-vs-adapted record."""

    path: str
    origin: str          # "generated" | "adapted"
    derived_from: str = ""   # URL or file the adaptation came from
    note: str = ""

    def __post_init__(self) -> None:
        if self.origin == "adapted" and not self.derived_from:
            # An adaptation whose source is unnamed is indistinguishable from a
            # claim of original authorship, which is the specific thing this
            # record exists to prevent.
            raise ValueError(f"adapted line for {self.path!r} must name derived_from")


@dataclass(frozen=True)
class Claim:
    """An assertion the patch makes about behaviour, and how it was checked."""

    assertion: str
    checked_by: str
    passed: bool | None = None


@dataclass
class Packet:
    """The deliverable. Complete on its own; a patch may be attached to it."""

    opportunity_id: str
    repo_slug: str
    issue_url: str
    outcome: Outcome
    policy: Verdict | None = None
    liveness: Verdict | None = None
    sources_read: tuple[str, ...] = field(default_factory=tuple)
    licenses_touched: tuple[str, ...] = field(default_factory=tuple)
    provenance: tuple[Provenance, ...] = field(default_factory=tuple)
    claims: tuple[Claim, ...] = field(default_factory=tuple)
    obligations: tuple[str, ...] = field(default_factory=tuple)
    rehearsal: bool = False

    @property
    def short_id(self) -> str:
        return self.opportunity_id[:12]

    def to_dict(self) -> dict:
        def _verdict(v: Verdict | None) -> dict | None:
            if v is None:
                return None
            return {
                "result": v.result.value,
                "reason": v.reason,
                "evidence": list(v.evidence),
                "obligations": list(v.obligations),
            }

        return {
            "opportunity_id": self.opportunity_id,
            "short_id": self.short_id,
            "repo": self.repo_slug,
            "issue_url": self.issue_url,
            "outcome": self.outcome.value,
            "rehearsal": self.rehearsal,
            "gates": {"policy": _verdict(self.policy), "liveness": _verdict(self.liveness)},
            "sources_read": list(self.sources_read),
            "licenses_touched": list(self.licenses_touched),
            "generated_vs_adapted": [asdict(p) for p in self.provenance],
            "claims_made": [asdict(c) for c in self.claims],
            "obligations_on_submission": list(self.obligations),
        }

    def to_markdown(self) -> str:
        """Human-readable form. Mando reads this; the JSON is for the ladder."""
        lines = [
            f"# Provenance packet — {self.repo_slug} #{self.issue_url.rsplit('/', 1)[-1]}",
            "",
            f"**Outcome:** `{self.outcome.value}`"
            + ("  _(rehearsal — no patch produced)_" if self.rehearsal else ""),
            f"**Work item:** `{self.short_id}`  ·  {self.issue_url}",
            "",
            "## Gates",
        ]
        for name, verdict in (("Policy", self.policy), ("Liveness", self.liveness)):
            if verdict is None:
                lines.append(f"- **{name}:** not run")
                continue
            lines.append(f"- **{name}:** `{verdict.result.value}`"
                         + (f" — {verdict.reason}" if verdict.reason else ""))

        if self.obligations:
            lines += ["", "## Obligations that attach to submission",
                      "_These survive to the human who submits. They are not optional._", ""]
            lines += [f"- {o}" for o in self.obligations]

        lines += ["", "## Sources read", ""]
        lines += [f"- {u}" for u in self.sources_read] or ["- _(none)_"]

        if self.licenses_touched:
            lines += ["", "## Licences touched", ""]
            lines += [f"- {lic}" for lic in self.licenses_touched]

        if self.provenance:
            lines += ["", "## Generated vs adapted", ""]
            for p in self.provenance:
                origin = p.origin
                if p.derived_from:
                    origin += f" from {p.derived_from}"
                lines.append(f"- `{p.path}` — {origin}" + (f" ({p.note})" if p.note else ""))

        if self.claims:
            lines += ["", "## Claims made, and how each was checked", ""]
            for c in self.claims:
                mark = {True: "pass", False: "FAIL", None: "unchecked"}[c.passed]
                lines.append(f"- {c.assertion} — checked by {c.checked_by} [{mark}]")

        return "\n".join(lines) + "\n"


def emit(packet: Packet, out_dir: Path, *, patch: str | None = None) -> dict[str, Path]:
    """Write the packet, and OPTIONALLY attach a patch to it.

    The packet is the first, required argument and the patch is a keyword with a
    default. That signature is invariant 5: there is no call shape that writes a
    patch without writing the packet that explains it.

    A rehearsal packet refuses a patch outright rather than quietly dropping it,
    because silently discarding a caller's output is how a rehearsal mode
    becomes indistinguishable from a broken one.
    """
    if packet.rehearsal and patch is not None:
        raise ValueError("rehearsal emits no patch; refusing to discard one silently")

    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    stem = f"{packet.short_id}-{packet.repo_slug.replace('/', '_')}"

    written: dict[str, Path] = {}
    json_path = out_dir / f"{stem}.packet.json"
    json_path.write_text(json.dumps(packet.to_dict(), indent=2), encoding="utf-8")
    written["packet_json"] = json_path

    md_path = out_dir / f"{stem}.packet.md"
    md_path.write_text(packet.to_markdown(), encoding="utf-8")
    written["packet_md"] = md_path

    if patch is not None:
        patch_path = out_dir / f"{stem}.patch"
        patch_path.write_text(patch, encoding="utf-8")
        written["patch"] = patch_path

    return written


__all__ = ["Provenance", "Claim", "Packet", "emit"]
