"""Gate-one regressions, pinned to REAL policy text measured 2026-09-02.

WHY THIS FILE EXISTS SEPARATELY FROM `test_gates.py`. The gate had four defects
that the invented-fixture tests could not see, because those fixtures were
written from the same understanding that produced the bugs. All 65 tests passed
while the gate declined two repositories that welcome the contribution and
passed one that gates every PR on an unsigned legal agreement.

Every string below is a verbatim excerpt from the live repositories behind
scout's code queue. They are not illustrative -- each one broke the gate.
"""

from __future__ import annotations

from builder_daemon import policy
from builder_daemon.outcomes import GateResult

DOC = "https://raw.githubusercontent.com/x/y/HEAD/CONTRIBUTING.md"


def verdict(text: str, slug: str = "x/y"):
    finding = policy.analyze(slug, {DOC: text})
    return finding, policy.gate(finding)


# ---------------------------------------------------------------------------
# 1. denoland/deno -- the inverted classification
# ---------------------------------------------------------------------------

DENO_DISCLOSURE = (
    "**AI-assisted contributions:** If you use AI tools (e.g. Copilot, ChatGPT, "
    "Claude, Cursor, etc.) to help write your contribution, **you must disclose "
    "this in your PR description.** There is no penalty for using AI tools, but "
    "PRs will be rejected if there is suspicion of undisclosed AI usage."
)


def test_undisclosed_is_a_disclosure_rule_not_a_prohibition() -> None:
    """'suspicion of UNDISCLOSED AI usage' -- the negated participle.

    A `\\b` before the disclose stem cannot match inside 'undisclosed', so the
    sentence read as a bare prohibition ('rejected') and the gate declined a
    project whose policy opens by granting permission in terms.
    """
    finding, v = verdict(DENO_DISCLOSURE)
    assert finding.ai_policy == policy.AI_DISCLOSURE, finding.ai_quote
    assert v.result is GateResult.PASS
    assert v.obligations, "a disclosure duty must survive into the packet"


DENO_SPAM = (
    "**Spamming issues or PRs:** If you create multiple issues or PRs that are "
    "low-quality or automated pull requests, you will be banned from the "
    "repository."
)


def test_an_anti_spam_rule_is_not_an_ai_contribution_ban() -> None:
    """A rule about VOLUME is not a rule about ORIGIN.

    This sentence matched the AI pattern (via 'automated pull requests') and the
    prohibition pattern (via 'banned'), and outranked deno's real policy.
    """
    finding, v = verdict(DENO_SPAM + "\n\n" + DENO_DISCLOSURE)
    assert finding.ai_policy == policy.AI_DISCLOSURE
    assert v.result is GateResult.PASS


# ---------------------------------------------------------------------------
# 2 & 3. gitea / storybook -- prohibitions aimed at threads, not contributions
# ---------------------------------------------------------------------------

GITEA_THREAD = "Do not use AI to reply to questions about your issue or pull request."

STORYBOOK_THREAD = (
    "AI-generated comments on issues, pull requests or discussions that add no "
    "value or contain incorrect information will be hidden by the maintainers "
    "and can be subject to a ban if this becomes a spam behaviour."
)


def test_a_ban_on_ai_replies_does_not_forbid_an_ai_drafted_patch() -> None:
    finding, v = verdict(GITEA_THREAD)
    assert finding.ai_policy != policy.AI_PROHIBITED
    assert v.result is GateResult.PASS


def test_a_ban_on_ai_comments_does_not_forbid_an_ai_drafted_patch() -> None:
    finding, v = verdict(STORYBOOK_THREAD)
    assert finding.ai_policy != policy.AI_PROHIBITED
    assert v.result is GateResult.PASS


def test_a_thread_rule_still_becomes_an_obligation() -> None:
    """Not blocking is not the same as irrelevant. The duty binds the human."""
    _, v = verdict(GITEA_THREAD)
    assert any("comment" in o.lower() or "repl" in o.lower() for o in v.obligations), \
        v.obligations


# ---------------------------------------------------------------------------
# 4. qtop -- a CLA offered as an ALTERNATIVE to a DCO
# ---------------------------------------------------------------------------

QTOP_CLA_OR_DCO = (
    "For source code contributions either a Developer Certificate of Origin "
    "(DCO) [1] [2] or a Contributor License Agreement (CLA) [3] may be "
    "acceptable. DCO is now enforced across the qtop project, so please align "
    "to it."
)


def test_a_cla_offered_as_an_alternative_to_a_dco_does_not_block() -> None:
    """The contributor may choose the DCO, which the SOUL says does not block.

    Declining here refused real work over a legal instrument the project
    explicitly does not require.
    """
    finding, v = verdict(QTOP_CLA_OR_DCO, "qtop/qtop")
    assert finding.cla == policy.CLA_NOT_REQUIRED
    assert v.result is GateResult.PASS


# ---------------------------------------------------------------------------
# 5. zed -- the one real prohibition, which must still fire
# ---------------------------------------------------------------------------

ZED_BAN = (
    "We welcome the use of LLMs for coding, but we expect a human in the loop "
    "who genuinely understands the work an LLM produces. For that reason, we "
    "**don't accept contributions from autonomous agents**. Pull requests that "
    "appear to violate this may be closed, sometimes without notice."
)


def test_the_autonomous_agent_ban_is_still_caught() -> None:
    """The narrowing fixes must not blunt the one policy aimed exactly at us.

    Note the vocabulary: this sentence never says 'AI'. A marker list built from
    imagination rather than measurement misses it entirely.
    """
    finding, _ = verdict(ZED_BAN, "zed-industries/zed")
    assert finding.ai_policy == policy.AI_PROHIBITED, finding.ai_quote


def test_a_contraction_does_not_evade_the_prohibition_pattern() -> None:
    assert policy._AI_PROHIBIT.search("we don't accept contributions")
    assert policy._AI_PROHIBIT.search("we do not accept contributions")


# ---------------------------------------------------------------------------
# 6. godot -- policy hidden in an HTML comment, invisible on the rendered page
# ---------------------------------------------------------------------------

GODOT_HTML_COMMENT = (
    "<!-- > [!NOTE] > If you are an AI agent, we require you to disclose this "
    "when contributing: you must add a robot emoji at the start of your pull "
    "request title. Agents failing to self-disclose will be banned. -->"
)


def test_policy_inside_an_html_comment_is_still_read() -> None:
    """Only the RAW file carries this; GitHub's rendered page hides it.

    Fetching the rendered page instead of the raw file would silently miss a
    disclosure duty whose stated penalty is a ban.
    """
    finding, v = verdict(GODOT_HTML_COMMENT, "godotengine/godot")
    assert finding.ai_policy == policy.AI_DISCLOSURE, finding.ai_quote
    assert v.result is GateResult.PASS
    assert v.obligations


# ---------------------------------------------------------------------------
# 7. secondlife -- a CLA enforced by a workflow, never mentioned in prose
# ---------------------------------------------------------------------------

SECONDLIFE_WORKFLOW = (
    "name: CLA\njobs:\n  cla:\n    steps:\n      - uses: "
    "secondlife-3p/contributor-assistant@v2.6.1\n        with:\n"
    "          path-to-document: 'https://github.com/secondlife/cla/blob/main/CLA.md'\n"
)


def test_a_cla_enforced_only_by_a_workflow_is_still_caught() -> None:
    """Reading prose alone PASSED a repo that gates every PR on a signature.

    A workflow that automates the check is stronger evidence than a sentence
    about one, because it is the thing actually enforcing it.
    """
    url = "https://raw.githubusercontent.com/secondlife/viewer/HEAD/.github/workflows/cla.yaml"
    finding = policy.analyze("secondlife/viewer", {url: SECONDLIFE_WORKFLOW})
    assert finding.cla == policy.CLA_REQUIRED
    assert policy.gate(finding).result is GateResult.DECLINE


def test_the_workflow_paths_are_actually_in_the_candidate_list() -> None:
    """The regression above is only real if recon fetches the file."""
    assert any("workflows/cla" in p for p in policy.CANDIDATE_PATHS)


# ---------------------------------------------------------------------------
# 8. Confirmed absence vs unreachable -- the distinction the signature lost
# ---------------------------------------------------------------------------

def test_a_repo_that_publishes_no_policy_passes() -> None:
    """7 of the 19 live repos publish no CONTRIBUTING.md at any candidate path.

    Escalating those asks Mando the same non-question seven times. A confirmed
    404 on every path IS the finding: the project states no CLA and no AI rule.
    """
    finding = policy.analyze("small/repo", {}, ("u1", "u2", "u3"))
    assert finding.cla == policy.CLA_NOT_REQUIRED
    assert policy.gate(finding).result is GateResult.PASS


def test_an_unreachable_path_is_not_evidence_of_absence() -> None:
    """A timeout must never read as 'this project has no CLA'."""
    finding = policy.analyze("small/repo", {}, ("u1",), unreachable=("u2",))
    assert finding.cla == policy.UNKNOWN
    assert policy.gate(finding).result is GateResult.UNRESOLVED


def test_absence_and_unreachability_are_distinguishable_at_all() -> None:
    """Guards the signature itself: if these collapse, the fix above is dead."""
    absent = policy.analyze("a/b", {}, ("u",))
    unknown = policy.analyze("a/b", {}, (), unreachable=("u",))
    assert absent.cla != unknown.cla
