"""The SOUL invariants, asserted structurally.

These are the tests that make SOUL.md enforcement rather than prose. They follow
the pattern scout uses for its admission gate: inspect the package's own source
with `ast`, not with substring searches over prose, because a docstring that
merely MENTIONS a forbidden thing must not trip a guard and a real call must not
escape one.

If one of these fails, an invariant has been broken. Fix the code, not the test.
"""

from __future__ import annotations

import ast
import inspect
import pathlib
import re

import pytest

import builder_daemon
from builder_daemon import cli, config, intake, outcomes, packet, policy

PKG = pathlib.Path(builder_daemon.__file__).parent
SOUL = PKG.parent / "SOUL.md"


def _sources():
    for py in sorted(PKG.rglob("*.py")):
        yield py, py.read_text(encoding="utf-8")


def _string_constants(tree):
    """Every executable string literal, docstrings excluded.

    Docstrings are prose ABOUT the code -- they name the forbidden things on
    purpose, to explain why they are forbidden. Only executable literals can
    actually do anything, so only they are inspected.
    """
    docstrings = set()
    for node in ast.walk(tree):
        if isinstance(node, (ast.Module, ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)):
            doc = ast.get_docstring(node, clean=False)
            if doc is not None:
                docstrings.add(doc)
    for node in ast.walk(tree):
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            if node.value not in docstrings:
                yield node.value


def _call_names(tree):
    """Dotted names of every call site, e.g. `client.post_json`, `requests.post`."""
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        f = node.func
        if isinstance(f, ast.Attribute):
            yield f.attr
        elif isinstance(f, ast.Name):
            yield f.id


# ---------------------------------------------------------------------------
# INVARIANT 2 -- artifacts only; no submission verb exists
# ---------------------------------------------------------------------------

#: Method names that would create, submit, or mutate something on a forge.
_SUBMISSION_CALLS = {
    "post_json", "post", "put", "patch", "delete",
    "create_pull", "create_pull_request", "create_issue", "create_comment",
    "merge", "push",
}


def test_no_submission_verb_is_called_anywhere() -> None:
    """Invariant 2 is enforced by absence, not by a guard clause.

    A guard can be bypassed by a caller. A call site that does not exist cannot.
    """
    offenders = []
    for py, src in _sources():
        for name in _call_names(ast.parse(src)):
            if name in _SUBMISSION_CALLS:
                offenders.append(f"{py.name}: {name}()")
    assert not offenders, f"submission-capable calls found: {offenders}"


def test_no_http_method_other_than_get_appears_as_a_literal() -> None:
    """A method string is how a write verb would sneak past the call-name check.

    CASE IS THE DISCRIMINATOR, and it has to be. This daemon's own output is
    called a *patch*, so lowercase `"patch"` is the deliverable (a diff) while
    uppercase `"PATCH"` would be the HTTP verb. Matching case-insensitively
    would flag the artifact and force the invariant to be weakened to
    accommodate it -- so the guard is exact-case and says why.
    """
    offenders = []
    for py, src in _sources():
        for text in _string_constants(ast.parse(src)):
            if text.strip() in {"POST", "PUT", "PATCH", "DELETE"}:
                offenders.append(f"{py.name}: {text!r}")
    assert not offenders, f"non-GET method literals: {offenders}"


def test_the_fetch_module_exports_no_write_verb() -> None:
    from builder_daemon import fetch

    assert set(fetch.__all__) == {"Document", "build_client", "get_text", "get_json"}
    assert not hasattr(fetch, "post_json")
    assert not hasattr(fetch, "post_graphql")


def test_the_cli_has_no_execution_or_submission_verb() -> None:
    """Phase 1 ships gates only. No execution path -- not even a disabled one."""
    parser = cli.build_parser()
    verbs = set()
    for action in parser._actions:
        if hasattr(action, "choices") and action.choices:
            verbs |= set(action.choices)
    assert verbs == {"queue", "rehearse", "doctor"}, verbs
    for forbidden in ("submit", "run", "execute", "draft", "open-pr", "pr"):
        assert forbidden not in verbs


# ---------------------------------------------------------------------------
# INVARIANT 3 -- no platform credential, no wallet
# ---------------------------------------------------------------------------

_CREDENTIAL_PATTERNS = (
    r"GITHUB_TOKEN", r"GH_TOKEN", r"GITLAB_TOKEN", r"FORGE_TOKEN",
    r"PRIVATE_KEY", r"MNEMONIC", r"SEED_PHRASE", r"WALLET",
    r"Authorization", r"Bearer ", r"ghp_", r"github_pat_",
)


def test_no_platform_credential_or_wallet_appears_anywhere() -> None:
    """Invariant 3. The daemon's own model key is not in scope here -- it buys
    compute and reaches no counterparty of the work. A forge token would."""
    offenders = []
    for py, src in _sources():
        for text in _string_constants(ast.parse(src)):
            for pattern in _CREDENTIAL_PATTERNS:
                if re.search(pattern, text, re.I):
                    offenders.append(f"{py.name}: {text[:60]!r} matched {pattern}")
    assert not offenders, offenders


def test_config_defines_no_credential_constant() -> None:
    """`*_MAX_TOKENS` is a model output budget, not a credential -- it is
    excluded by name rather than by loosening the pattern, so the exception
    stays visible instead of quietly widening the guard."""
    allowed = {"DRAFTER_MAX_TOKENS"}
    for n in dir(config):
        if n.startswith("_") or n in allowed:
            continue
        assert not re.search(r"token|secret|key|wallet|auth|credential", n, re.I), n


# ---------------------------------------------------------------------------
# INVARIANT 1 -- input is admitted rows only; no path to the ranked queue
# ---------------------------------------------------------------------------

def _opportunity_queries():
    for py, src in _sources():
        for text in _string_constants(ast.parse(src)):
            if "opportunities" in text and re.search(r"\bFROM\b", text, re.I):
                yield py.name, " ".join(text.split())


def test_every_ledger_query_is_admitted_scoped_or_id_scoped() -> None:
    """You may take admitted work, or you may name one specific row. Nothing else.

    The third permitted form is a pure aggregate: `SELECT COUNT(*) FROM
    opportunities` returns a number, never row content, and `doctor` needs it to
    report whether the ledger is readable at all. It is allowed by an exact
    match rather than by a category, so the exception cannot widen quietly.
    """
    allowed_aggregate = re.compile(r"^SELECT COUNT\(\*\) FROM opportunities$", re.I)
    offenders = []
    for name, sql in _opportunity_queries():
        if allowed_aggregate.match(sql):
            continue
        if re.search(r"status\s*=\s*'admitted'", sql, re.I):
            continue
        if re.search(r"opportunity_id\s*=\s*\?", sql, re.I):
            continue
        offenders.append(f"{name}: {sql[:110]}")
    assert not offenders, f"unscoped ledger queries: {offenders}"


def test_no_query_can_rank_or_choose_work() -> None:
    """The Builder must not have a view on which admitted work is worth more."""
    offenders = []
    for name, sql in _opportunity_queries():
        if re.search(r"ORDER BY\s+(rank|payout|segment)", sql, re.I):
            offenders.append(f"{name}: {sql[:110]}")
        if re.search(r"status\s*=\s*'(discovered|proposed)'", sql, re.I):
            offenders.append(f"{name}: reads a non-admitted status")
    assert not offenders, offenders


def test_select_work_hardcodes_admitted() -> None:
    """Not a parameter a caller could relax."""
    assert "status='admitted'" in intake._SELECT_ADMITTED
    src = inspect.getsource(intake.select_work)
    assert "status" not in src or "admitted" not in src.split("_SELECT_ADMITTED")[0]


def test_scout_ledger_is_opened_read_only() -> None:
    src = inspect.getsource(intake.connect_scout)
    assert "mode=ro" in src
    offenders = [
        py.name for py, s in _sources()
        if "sqlite3.connect" in s and py.name != "intake.py"
    ]
    assert not offenders, f"only intake.py may open a database: {offenders}"


def test_load_one_requires_an_explicit_id() -> None:
    """The rehearsal carve-out cannot become a search."""
    sig = inspect.signature(intake.load_one)
    assert "opportunity_id" in sig.parameters
    assert sig.parameters["opportunity_id"].default is inspect.Parameter.empty


# ---------------------------------------------------------------------------
# THE IDENTITY RULE
# ---------------------------------------------------------------------------

def test_no_attestation_is_ever_written_in_mandos_name() -> None:
    """`Signed-off-by` is a legal statement by a person. This daemon is not one.

    A trailer written here would follow the patch into a repository and attest
    something Mando did not personally attest -- which is precisely what "never
    represents itself as you" forbids.

    WHAT THIS MUST NOT CATCH. `policy.py` legitimately contains the string
    "signed-off-by" inside the regex that DETECTS a DCO requirement, and the
    word "Sign off" inside the obligation text that tells Mando to do it
    himself. Detecting a requirement and instructing a human are the opposite of
    emitting an attestation. So the guard matches the TRAILER SHAPE -- the token
    at the start of a line, followed by a colon -- which is the only form that
    would actually be written into a commit.
    """
    trailer = re.compile(r"^\s*(?:signed-off-by|co-authored-by)\s*:", re.I | re.M)
    offenders = []
    for py, src in _sources():
        for text in _string_constants(ast.parse(src)):
            if trailer.search(text):
                offenders.append(f"{py.name}: {text[:60]!r}")
    assert not offenders, offenders


def test_the_identity_rule_is_recorded_verbatim_in_the_soul() -> None:
    text = SOUL.read_text(encoding="utf-8")
    for clause in (
        "under your GitHub identity",
        "by your hand",
        "disclosed wherever the repo",
        "never represents itself as you and never submits",
    ):
        assert clause in text, f"identity rule clause missing: {clause!r}"


# ---------------------------------------------------------------------------
# INVARIANT 5 -- the packet is primary, the patch is its attachment
# ---------------------------------------------------------------------------

def test_a_patch_cannot_be_written_without_a_packet() -> None:
    """Structural, via the signature: packet first and required, patch keyword
    with a default. There is no other writer."""
    params = list(inspect.signature(packet.emit).parameters.values())
    assert params[0].name == "packet"
    assert params[0].default is inspect.Parameter.empty
    patch_param = inspect.signature(packet.emit).parameters["patch"]
    assert patch_param.kind is inspect.Parameter.KEYWORD_ONLY
    assert patch_param.default is None

    writers = [n for n in dir(packet) if n.startswith("emit") or n.startswith("write")]
    assert writers == ["emit"], f"a second artifact writer exists: {writers}"


def test_emit_writes_the_packet_even_with_no_patch(tmp_path) -> None:
    p = packet.Packet(
        opportunity_id="a" * 64, repo_slug="o/r",
        issue_url="https://github.com/o/r/issues/1",
        outcome=outcomes.Outcome.DECLINED_POLICY,
    )
    written = packet.emit(p, tmp_path)
    assert "packet_json" in written and "packet_md" in written
    assert "patch" not in written


def test_rehearsal_refuses_a_patch_rather_than_dropping_it(tmp_path) -> None:
    p = packet.Packet(
        opportunity_id="b" * 64, repo_slug="o/r",
        issue_url="https://github.com/o/r/issues/1",
        outcome=outcomes.Outcome.REHEARSED, rehearsal=True,
    )
    with pytest.raises(ValueError, match="refusing to discard"):
        packet.emit(p, tmp_path, patch="--- a/x\n+++ b/x\n")


def test_an_adapted_line_must_name_its_source() -> None:
    packet.Provenance(path="x.py", origin="generated")
    with pytest.raises(ValueError, match="derived_from"):
        packet.Provenance(path="x.py", origin="adapted")


# ---------------------------------------------------------------------------
# INVARIANT 6 -- declined-with-reason is first-class
# ---------------------------------------------------------------------------

def test_declines_are_terminal_outcomes_not_failures() -> None:
    assert outcomes.DECLINED <= outcomes.TERMINAL
    assert outcomes.ESCALATED <= outcomes.TERMINAL
    assert len(outcomes.DECLINED) >= 4


def test_the_outcome_vocabulary_has_no_failure_bucket() -> None:
    """A crash is an exception with a traceback, not an outcome. Naming failure
    as a result is how a taxonomy starts absorbing bugs as if they were data."""
    for member in outcomes.Outcome:
        assert not re.search(r"fail|error|crash|unknown", member.name, re.I), member


def test_a_decline_without_a_reason_is_refused() -> None:
    with pytest.raises(ValueError, match="requires a reason"):
        outcomes.Verdict(outcomes.GateResult.DECLINE)
    with pytest.raises(ValueError, match="requires a reason"):
        outcomes.Verdict(outcomes.GateResult.UNRESOLVED, reason="   ")
    outcomes.Verdict(outcomes.GateResult.PASS)   # a pass needs none


def test_closed_and_contested_stay_distinct() -> None:
    """Being late is not the same lesson as being second."""
    assert outcomes.Outcome.DECLINED_CLOSED != outcomes.Outcome.DECLINED_CONTESTED
    assert {outcomes.Outcome.DECLINED_CLOSED,
            outcomes.Outcome.DECLINED_CONTESTED} <= outcomes.DECLINED


# ---------------------------------------------------------------------------
# GATE ORDER
# ---------------------------------------------------------------------------

def test_policy_runs_before_liveness() -> None:
    """A repository that will not accept the contribution makes the issue's
    availability irrelevant -- and checking it first would mean fetching an
    issue we have no business working on."""
    src = inspect.getsource(builder_daemon.runner.run_gates)
    assert src.index("policy_mod.recon") < src.index("liveness_mod.check")


def test_a_dco_requirement_does_not_block() -> None:
    """It is a per-commit attestation the human submitter makes at submission,
    which is exactly where the identity rule already puts him."""
    finding = policy.analyze("o/r", {"u": "Please sign off your commits (Signed-off-by)."})
    assert finding.dco == policy.CLA_REQUIRED
    verdict = policy.gate(finding)
    assert verdict.result is outcomes.GateResult.PASS
    assert verdict.obligations, "the DCO must survive as an obligation on the packet"
