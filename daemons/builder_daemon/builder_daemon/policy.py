"""GATE ONE -- per-repo contribution policy. Runs before any code is written.

DOCTRINE A.1.1 AT REPOSITORY GRANULARITY. ToS-hostility is a separate and
higher-priority gate than legitimacy. A project can be entirely legitimate, the
bounty real and the money good, and still be one the Tribe must not contribute
to, because its own policy says so. Legitimacy does not license entry, and this
gate runs first for that reason.

WHAT BLOCKS AND WHAT DOES NOT
-----------------------------
    CLA required             BLOCKS.     A legal instrument needing a signature
                                         Mando has not given and this daemon
                                         may never give.
    AI contributions banned  BLOCKS.     The draft could not be submitted
                                         without misrepresenting its origin.
    DCO / Signed-off-by      does NOT.   A per-commit attestation the human
                                         submitter makes at submission time --
                                         exactly where the identity rule
                                         already puts him. Carried forward as an
                                         obligation on the packet.
    Disclosure required      does NOT.   Satisfiable, and satisfying it is the
                                         identity rule's third clause. Carried
                                         forward as an obligation.

THE THIRD ANSWER. A marker found but not classifiable returns UNRESOLVED, not a
guess in either direction. Declining on an unreadable policy refuses real work
on no evidence; passing on one launders an unknown into a clearance. Escalating
is the only honest move and it costs Mando one look.

ASYMMETRY OF A MISS, STATED PLAINLY. Absence of any marker resolves to
`none_found`, which passes. A marker list cannot be complete (invariant 6), so
this gate WILL eventually miss a policy that exists. The cost of that miss is
bounded and small: the Builder produces a draft that should not be submitted,
and the packet -- which names every source read and says plainly that no policy
was found -- goes to Mando, who submits or does not. Because nothing here
submits, a missed policy cannot become a bad contribution without a human
first deciding to make it one. That is the containment argument for allowing
`none_found` to pass, and it holds only for as long as invariant 2 does.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field

from . import fetch
from .errors import FetchError
from .outcomes import GateResult, Verdict

# ---------------------------------------------------------------------------
# Where policy lives
# ---------------------------------------------------------------------------

#: Paths to try, in order. Necessarily incomplete (invariant 6) and safe in that
#: direction: an unfound file yields `none_found`, and the packet records every
#: path attempted so a miss is visible rather than silent.
CANDIDATE_PATHS = (
    "CONTRIBUTING.md",
    ".github/CONTRIBUTING.md",
    "docs/CONTRIBUTING.md",
    "CONTRIBUTING.rst",
    "CONTRIBUTING",
    ".github/PULL_REQUEST_TEMPLATE.md",
    ".github/pull_request_template.md",
    "CODE_OF_CONDUCT.md",
    # THE PROSE IS NOT THE ONLY PLACE A CLA LIVES, AND MISSING IT IS THE
    # DANGEROUS DIRECTION. Measured 2026-09-02: secondlife/viewer enforces a CLA
    # on every pull request from `.github/workflows/cla.yaml`
    # (contributor-assistant), while its CONTRIBUTING.md never says so. Reading
    # prose alone PASSED a repository whose every PR is gated on a signature
    # Mando has not given -- a false pass, which is the error this gate exists
    # to prevent. A workflow that automates a CLA check is stronger evidence
    # than a sentence about one, because it is the thing actually enforcing it.
    "CLA.md",
    ".github/CLA.md",
    ".github/workflows/cla.yaml",
    ".github/workflows/cla.yml",
    ".github/workflows/cla-assistant.yml",
)

_RAW_HOSTS = {
    "github.com": "https://raw.githubusercontent.com/{owner}/{repo}/HEAD/{path}",
    "codeberg.org": "https://codeberg.org/{owner}/{repo}/raw/branch/HEAD/{path}",
    "gitlab.com": "https://gitlab.com/{owner}/{repo}/-/raw/HEAD/{path}",
}

# ---------------------------------------------------------------------------
# Markers
# ---------------------------------------------------------------------------

_CLA = re.compile(
    r"contributor\s+licen[cs]e\s+agreement|\bCLA\b|cla-assistant|easycla|cla\.txt",
    re.I,
)
_CLA_NEGATED = re.compile(
    r"no\s+(?:\w+\s+){0,2}(?:CLA|contributor\s+licen[cs]e\s+agreement)"
    r"|(?:CLA|contributor\s+licen[cs]e\s+agreement)\s+is\s+not\s+required"
    r"|do(?:es)?\s+not\s+require\s+a\s+(?:CLA|contributor)",
    re.I,
)
_DCO = re.compile(
    r"developer\s+certificate\s+of\s+origin|\bDCO\b|signed-off-by|git\s+commit\s+-s\b",
    re.I,
)
#: MARKERS FOR "THIS POLICY IS ABOUT US".
#:
#: INVARIANT 6, DEMONSTRATED RATHER THAN ASSERTED. The first version of this
#: pattern required a compound -- "AI-generated", "AI tool", "generative AI" --
#: and was checked against invented fixtures, where it passed. Run against the
#: 19 real repositories on 2026-09-02 it missed three of the eight policies that
#: exist, including the only outright prohibition on the queue:
#:
#:     godotengine/godot   "Use of AI must be disclosed"        -> bare "AI"
#:     qtop/qtop           "If AI assistance materially..."     -> bare "AI"
#:     zed-industries/zed  "we don't accept contributions from
#:                          autonomous agents"                  -> no "AI" at all
#:
#: The zed case is the one that matters. A policy aimed precisely at this daemon
#: need never use the word "AI"; the vocabulary maintainers reach for is
#: "autonomous agent". A marker list built from imagination rather than from
#: measurement missed the single most on-point sentence in the corpus, which is
#: exactly why a name list may narrow but must never be the rule.
_AI = re.compile(
    r"\bAI\b|\bA\.I\.|generative\s+AI|\bLLMs?\b|large\s+language\s+model"
    r"|ChatGPT|Copilot|\bCodex\b|\bCursor\b|artificial\s+intelligence"
    r"|autonomous\s+agents?|AI\s+agents?|coding\s+agents?|\bagentic\b"
    r"|automated\s+(?:pull\s+request|contribution|submission)s?",
    re.I,
)
#: CONTRACTIONS ARE NOT OPTIONAL HERE. Measured 2026-09-02 across the 19
#: repositories behind the code queue, the single hardest prohibition on the
#: list is phrased "we don't accept contributions from autonomous agents"
#: (zed-industries/zed). A pattern requiring the literal "not accept" misses it
#: and passes the one repository that most clearly forbids this daemon's output.
_AI_PROHIBIT = re.compile(
    r"\b(?:do(?:n['’]?t|es\s*n['’]?t|\s+not)\s+(?:accept|submit|use|allow|want)"
    r"|(?:will|would|can)\s*n?['’]?o?t\s+(?:be\s+)?accept(?:ed)?"
    r"|are\s+prohibited|is\s+prohibited|prohibit(?:s|ed)?|forbidden|banned"
    r"|\bban\b|will\s+be\s+banned|reject(?:ed|s)?|closed\s+without\s+notice"
    r"|unacceptable|not\s+permitted|not\s+allowed|no\s+AI\b)",
    re.I,
)
#: `disclos\w*` rather than `\bdisclose\b`: the commonest real phrasing is the
#: PAST PARTICIPLE -- "Use of AI must be disclosed" (godotengine/godot) -- which
#: a word-boundary match on the bare stem does not reach.
#:
#: NOTE THE MISSING `\b` ON THE FIRST ALTERNATIVE, WHICH IS LOAD-BEARING.
#: Measured 2026-09-02, denoland/deno's policy reads "There is no penalty for
#: using AI tools, but PRs will be rejected if there is suspicion of UNDISCLOSED
#: AI usage." A leading word boundary cannot match inside "undisclosed", so the
#: sentence registered as a prohibition ("rejected") with no disclosure clause,
#: and the gate declined a repository whose policy opens by granting permission.
#: A false DECLINE is the expensive error for this gate -- it refuses real work
#: and the refusal looks principled -- so the negated form must match.
_AI_DISCLOSE = re.compile(
    r"disclos\w*"
    r"|\b(?:declar\w*|indicat\w*|state\s+clearly|must\s+mention"
    r"|let\s+us\s+know|inform\s+(?:us|maintainers)|note\s+(?:that|in))",
    re.I,
)

#: SCOPE: IS THE PROHIBITION AIMED AT CONTRIBUTIONS, OR AT THREAD CONDUCT?
#:
#: Measured 2026-09-02, two of the three prohibitions this gate found on the
#: live queue were about COMMENTS, not code:
#:
#:     go-gitea/gitea        "Do not use AI to reply to questions about your
#:                            issue or pull request."
#:     storybookjs/storybook "AI-generated comments on issues, pull requests or
#:                            discussions ... will be hidden"
#:
#: Both mention "pull request", so a noun-based scope test reads them as banning
#: contributions and declines two repositories that ban no such thing. The
#: discriminator has to be the OBJECT OF THE AI USE -- what the AI is being
#: forbidden to produce -- not which nouns appear in the sentence.
#:
#: These policies are still real obligations on the human who submits, so they
#: are carried into the packet rather than discarded. The gate stops blocking;
#: the duty survives.
_AI_THREAD_SCOPED = re.compile(
    r"\bto\s+(?:reply|repl(?:ies|ying)|respond|comment|answer|discuss)\b"
    r"|\bAI[- ]generated\s+(?:comment|repl|respon|review)"
    r"|\bAI\s+(?:to\s+)?(?:comment|repl|respon)",
    re.I,
)

#: A rule about VOLUME is not a rule about ORIGIN.
#:
#: Measured 2026-09-02, after the thread-scope fix denoland/deno STILL declined
#: -- on a different sentence: "Spamming issues or PRs: If you create multiple
#: issues or PRs ... [you will be banned]". That is an anti-spam rule. It
#: matched the AI pattern (via "automated ... pull requests") and the prohibition
#: pattern (via "banned"), and because a prohibition anywhere outranked a
#: disclosure anywhere, it buried deno's actual policy, which is disclosure and
#: says in terms "there is no penalty for using AI tools".
#:
#: The precedence itself is right -- a project may both permit disclosed
#: assistance and refuse autonomous agents, and the refusal binds. What was
#: wrong is what counted as a refusal. A ban earned by flooding a tracker says
#: nothing about whether a patch may be drafted with assistance, so it is
#: carried as an obligation on conduct and stops blocking.
_AI_SPAM_SCOPED = re.compile(
    r"\bspam\w*|\bmultiple\s+(?:issues|PRs|pull\s+requests)|\bflood\w*"
    r"|\blow[- ]quality\b|\bduplicat\w*|\bunsolicited\b|\bnoise\b",
    re.I,
)

#: A CLA offered as ONE OF TWO ACCEPTABLE OPTIONS is not a required CLA.
#:
#: Measured 2026-09-02, qtop/qtop reads "For source code contributions either a
#: Developer Certificate of Origin (DCO) or a Contributor License Agreement
#: (CLA) may be acceptable. DCO is now enforced across the qtop project."
#: A bare CLA-mention test declines that repository -- refusing real work over a
#: legal instrument the project explicitly does not require, when the operative
#: requirement is the DCO, which the SOUL states does not block.
_CLA_ALTERNATIVE = re.compile(
    r"developer\s+certificate\s+of\s+origin.{0,120}?\bor\b.{0,80}?"
    r"contributor\s+licen[cs]e\s+agreement"
    r"|contributor\s+licen[cs]e\s+agreement.{0,120}?\bor\b.{0,80}?"
    r"developer\s+certificate\s+of\s+origin",
    re.I | re.S,
)
_AI_PERMIT = re.compile(
    r"\b(?:welcome|allowed|permitted|acceptable|fine|encouraged|no\s+objection)\b",
    re.I,
)

CLA_REQUIRED = "required"
CLA_NOT_REQUIRED = "not_required"
UNKNOWN = "unknown"

AI_NONE = "none_found"
AI_PERMISSIVE = "permissive"
AI_DISCLOSURE = "disclosure_required"
AI_PROHIBITED = "prohibited"
AI_AMBIGUOUS = "ambiguous"


@dataclass(frozen=True)
class PolicyFinding:
    """What gate one established, and from which documents."""

    repo_slug: str
    cla: str = UNKNOWN
    dco: str = UNKNOWN
    ai_policy: str = AI_NONE
    ai_quote: str = ""
    sources_read: tuple[str, ...] = field(default_factory=tuple)
    sources_missing: tuple[str, ...] = field(default_factory=tuple)
    obligations: tuple[str, ...] = field(default_factory=tuple)


#: Abbreviations whose full stop does NOT end a sentence. Without this guard,
#: denoland/deno's real policy -- "If you use AI tools (e.g. Copilot, ChatGPT,
#: Claude, Cursor, etc.) to help write your contribution, you must disclose
#: this in your PR description." -- is cut at "e.g." and the operative clause
#: ("you must disclose") never reaches the classifier, which then reports an
#: unclassifiable policy and escalates a perfectly clear one.
_ABBREV = re.compile(r"\b(?:e\.g|i\.e|etc|vs|cf|al|resp|approx|Inc|Ltd|Co|No)\.", re.I)


def _sentence_around(text: str, match: re.Match) -> str:
    """The sentence containing a marker, trimmed. Quoted into the packet.

    Kept short deliberately: the packet cites evidence, it does not reproduce
    the document.
    """
    # Mask abbreviation periods with a same-length sentinel so offsets stay
    # aligned with the original text.
    masked = _ABBREV.sub(lambda m: m.group(0).replace(".", "\x00"), text)
    start = max(0, masked.rfind(".", 0, match.start()) + 1)
    end = masked.find(".", match.end())
    end = len(text) if end == -1 else end + 1
    return " ".join(text[start:end].split())[:300]


def analyze(
    repo_slug: str,
    documents: dict[str, str],
    missing: tuple = (),
    *,
    unreachable: tuple = (),
) -> PolicyFinding:
    """Classify policy from already-fetched documents. Pure -- no network.

    Split from `recon` so the gate's judgment can be tested against fixed text
    rather than against whatever a live repository happens to say today.

    CONFIRMED ABSENCE IS EVIDENCE; AN UNREACHABLE PATH IS NOT. `missing` holds
    paths that returned a definitive 404 -- the project does not publish that
    file. `unreachable` holds paths that could not be resolved at all. The
    distinction decides what an empty `documents` means:

      * every path confirmed 404  ->  the project publishes no contribution
                                      policy, so it states no CLA requirement
                                      and no AI restriction. That is a finding,
                                      and it PASSES.
      * anything unreachable      ->  we do not know what the project says.
                                      UNKNOWN, which escalates.

    Measured 2026-09-02: 7 of the 19 repositories behind the live code queue
    publish no CONTRIBUTING.md at any candidate path. Collapsing that into
    UNKNOWN escalated 37% of the queue to Mando with the same non-answer, which
    is not a gate -- it is a queue with extra steps. `fetch.get_text` already
    separates absence from failure; this signature is where that distinction was
    being thrown away.
    """
    blob = "\n\n".join(documents.values())
    obligations: list[str] = []
    confirmed_absent = not documents and not unreachable and bool(missing)

    if _CLA_NEGATED.search(blob):
        cla = CLA_NOT_REQUIRED
    elif _CLA_ALTERNATIVE.search(blob):
        # DCO-or-CLA. The contributor may choose the DCO, which does not block,
        # so no CLA is required of us.
        cla = CLA_NOT_REQUIRED
        obligations.append(
            "This project accepts a DCO sign-off in place of a CLA. Mando "
            "signs off at submission; the Builder must not sign in his name."
        )
    elif _CLA.search(blob):
        cla = CLA_REQUIRED
    elif documents or confirmed_absent:
        cla = CLA_NOT_REQUIRED
    else:
        cla = UNKNOWN

    if _DCO.search(blob):
        dco = CLA_REQUIRED
        obligations.append(
            "Sign off each commit (DCO). Mando makes this attestation at "
            "submission; the Builder must not write Signed-off-by in his name."
        )
    elif documents or confirmed_absent:
        dco = CLA_NOT_REQUIRED
    else:
        dco = UNKNOWN

    # EVERY AI-marker sentence, not just the first. Real policies split the
    # permission and the duty across sentences -- keycloak/keycloak grants use
    # in one ("Generative AI tools may be used to assist...") and imposes
    # disclosure in another. Reading only the first sentence classifies that as
    # ambiguous and escalates a policy that is in fact perfectly clear.
    sentences = [_sentence_around(blob, m) for m in _AI.finditer(blob)]
    ai_policy, quote = AI_NONE, ""
    if sentences:
        banning = [
            s for s in sentences
            if _AI_PROHIBIT.search(s) and not _AI_DISCLOSE.search(s)
        ]
        # Split the bans by what they are aimed at. A rule against AI-written
        # COMMENTS does not forbid an AI-drafted patch, and treating it as if it
        # did declines repositories that welcome the contribution.
        def _aimed_at_origin(s: str) -> bool:
            """Does this ban concern AI AUTHORSHIP, or merely thread conduct/volume?"""
            return not (_AI_THREAD_SCOPED.search(s) or _AI_SPAM_SCOPED.search(s))

        prohibiting = [s for s in banning if _aimed_at_origin(s)]
        thread_rules = [s for s in banning if not _aimed_at_origin(s)]
        disclosing = [s for s in sentences if _AI_DISCLOSE.search(s)]
        permitting = [s for s in sentences if _AI_PERMIT.search(s)]

        for rule in thread_rules[:2]:
            obligations.append(
                "This project restricts AI-generated comments/replies in "
                "threads. It does not forbid the contribution, but it binds "
                "how the submission is discussed. Policy text: " + rule[:160]
            )

        if prohibiting:
            # A ban outranks a disclosure clause elsewhere in the same document:
            # a project may both permit disclosed AI assistance AND refuse
            # autonomous agents, and the refusal is the binding one for us.
            ai_policy, quote = AI_PROHIBITED, prohibiting[0]
        elif disclosing:
            ai_policy, quote = AI_DISCLOSURE, disclosing[0]
            obligations.append(
                "Disclose AI assistance in the submission -- this project's "
                "policy speaks to it, so the identity rule's disclosure clause "
                "is live here. Policy text: " + quote[:160]
            )
        elif permitting:
            ai_policy, quote = AI_PERMISSIVE, permitting[0]
        elif thread_rules:
            # The only AI rule found governs thread conduct. As far as the
            # CONTRIBUTION is concerned the project has said nothing, and the
            # duty it did state is already carried in `obligations`. Escalating
            # here would ask Mando to adjudicate a policy that is perfectly
            # clear and simply not about whether we may send a patch.
            ai_policy, quote = AI_NONE, thread_rules[0]
        else:
            # Marker present, intent not established. Escalate rather than guess.
            ai_policy, quote = AI_AMBIGUOUS, sentences[0]

    return PolicyFinding(
        repo_slug=repo_slug,
        cla=cla,
        dco=dco,
        ai_policy=ai_policy,
        ai_quote=quote,
        sources_read=tuple(documents.keys()),
        sources_missing=tuple(missing),
        obligations=tuple(obligations),
    )


def recon(client, item) -> PolicyFinding:
    """Fetch the policy surface for a work item's repository, then analyze it."""
    template = _RAW_HOSTS.get(item.host)
    if template is None:
        return PolicyFinding(repo_slug=item.repo_slug, cla=UNKNOWN, dco=UNKNOWN)

    documents: dict[str, str] = {}
    missing: list[str] = []
    unreachable: list[str] = []
    for path in CANDIDATE_PATHS:
        url = template.format(owner=item.owner, repo=item.repo, path=path)
        try:
            doc = fetch.get_text(client, url, optional=True)
        except FetchError:
            # Could not resolve this path at all. Recorded separately from a
            # confirmed 404 so `analyze` cannot mistake a network problem for
            # the absence of a policy -- see its docstring.
            unreachable.append(url)
            continue
        if doc.found:
            documents[url] = doc.text
        else:
            missing.append(url)
    return analyze(
        item.repo_slug, documents, tuple(missing), unreachable=tuple(unreachable)
    )


def gate(finding: PolicyFinding) -> Verdict:
    """Turn a finding into PASS / DECLINE / UNRESOLVED."""
    evidence = finding.sources_read

    if finding.cla == CLA_REQUIRED:
        return Verdict(
            GateResult.DECLINE,
            reason=(
                f"{finding.repo_slug} requires a Contributor License Agreement. "
                "That is a signature Mando has not given and this daemon may "
                "not give on his behalf."
            ),
            evidence=evidence,
        )

    if finding.ai_policy == AI_PROHIBITED:
        return Verdict(
            GateResult.DECLINE,
            reason=(
                f"{finding.repo_slug} prohibits AI-generated contributions: "
                f"“{finding.ai_quote}”"
            ),
            evidence=evidence,
        )

    if finding.ai_policy == AI_AMBIGUOUS:
        return Verdict(
            GateResult.UNRESOLVED,
            reason=(
                f"{finding.repo_slug} has an AI-contribution policy this gate "
                f"could not classify: “{finding.ai_quote}”. Needs a human read."
            ),
            evidence=evidence,
        )

    if finding.cla == UNKNOWN:
        # Reached only when a path could not be RESOLVED -- a confirmed 404 on
        # every candidate is an answer (the project publishes no policy) and
        # leaves `cla` as not_required. This branch is the genuine don't-know.
        return Verdict(
            GateResult.UNRESOLVED,
            reason=(
                f"Could not establish the CLA position for {finding.repo_slug}: "
                "one or more policy paths were unreachable, so absence cannot "
                "be distinguished from a policy we failed to read."
            ),
            evidence=finding.sources_missing,
        )

    return Verdict(GateResult.PASS, evidence=evidence, obligations=finding.obligations)


__all__ = [
    "CANDIDATE_PATHS", "PolicyFinding",
    "CLA_REQUIRED", "CLA_NOT_REQUIRED", "UNKNOWN",
    "AI_NONE", "AI_PERMISSIVE", "AI_DISCLOSURE", "AI_PROHIBITED", "AI_AMBIGUOUS",
    "analyze", "recon", "gate",
]
