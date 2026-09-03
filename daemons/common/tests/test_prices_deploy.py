"""PS-1B Phase D — the deploy artifacts are checked here, not on the host.

A launchd job fails at 21:00 on a machine nobody is watching, and the two ways
this one can fail that way are both mechanical: a plist macOS will not parse,
and a runner naming a verb or flag the CLI does not have. Both are cheap to
assert and expensive to discover in a log the next morning.

The first of these is not hypothetical. The plist's explanatory comment was
written with `--` in it, which is illegal inside an XML comment; `plutil` and
launchd would both have refused the file. It parsed only after the comment was
rewritten, and this test is what stops it coming back.
"""

from __future__ import annotations

import plistlib
import re
import shlex
from pathlib import Path

import pytest

COMMON = Path(__file__).resolve().parents[1]
PLIST = COMMON / "deploy" / "com.abelard.prices.plist"
RUNNER = COMMON / "scripts" / "run_prices.sh"


def _plist() -> dict:
    with PLIST.open("rb") as fh:
        return plistlib.load(fh)


def test_plist_parses():
    """plistlib is the same parse launchd does. A `--` in a comment fails here."""
    assert _plist()["Label"] == "com.abelard.prices"


def test_plist_fires_on_schedule_never_on_boot():
    """RunAtLoad true would make a reboot indistinguishable from a real night,
    and would run `nightly` against whatever session happened to be in flight."""
    p = _plist()
    assert p["RunAtLoad"] is False
    assert "KeepAlive" not in p, "a fire-and-finish job must never be restarted"
    assert p["StartCalendarInterval"] == {"Hour": 21, "Minute": 0}


def test_plist_leaves_the_smart_money_slot_clear():
    """SM at 22:30 will read the freshness ledger as a precondition (Phase 4),
    so the timeout must expire before SM starts, not after."""
    p = _plist()
    start = p["StartCalendarInterval"]["Hour"] * 60 + p["StartCalendarInterval"]["Minute"]
    assert start + p["ExitTimeOut"] / 60 <= 22 * 60 + 30


def test_plist_points_at_the_runner_that_exists_in_this_repo():
    args = _plist()["ProgramArguments"]
    assert args[0] == "/bin/zsh"
    assert args[1].endswith("daemons/common/scripts/run_prices.sh")
    assert RUNNER.exists()


def _cli_verbs() -> set[str]:
    """Subparser names, read from the source rather than by importing the CLI —
    importing it pulls in `requests`, which is not a test dependency."""
    src = (COMMON / "abelard_common" / "prices" / "cli.py").read_text(encoding="utf-8")
    return set(re.findall(r'sub\.add_parser\(\s*"([a-z-]+)"', src))


def _runner_invocations() -> list[list[str]]:
    """Every `run <verb> [flags]` line in the runner."""
    out = []
    for line in RUNNER.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line.startswith("run ") and not line.startswith("run()"):
            out.append(shlex.split(line.split(";")[0])[1:])
    return out


def test_runner_calls_verbs_the_cli_actually_has():
    verbs = _cli_verbs()
    calls = _runner_invocations()
    assert calls, "parsed no invocations out of the runner — the parser is wrong"
    for call in calls:
        assert call[0] in verbs, "{} is not a CLI verb; have {}".format(
            call[0], sorted(verbs))


def test_runner_flags_are_flags_the_cli_declares():
    src = (COMMON / "abelard_common" / "prices" / "cli.py").read_text(encoding="utf-8")
    for call in _runner_invocations():
        for tok in call[1:]:
            if tok.startswith("-"):
                assert '"{}"'.format(tok) in src, \
                    "{} does not declare {}".format(call[0], tok)


def test_the_metered_leg_runs_last():
    """A quota refusal must not cost the store its nightly append."""
    order = [c[0] for c in _runner_invocations()]
    assert order[0] == "nightly"
    assert order[-1] == "verify"
    assert order.index("reference") < order.index("reconcile")


def test_runner_sets_an_absolute_store_path():
    """launchd does not expand `~`; a relative path silently creates a second,
    empty database wherever the job happened to start."""
    src = RUNNER.read_text(encoding="utf-8")
    m = re.search(r"export ABELARD_PRICES_DB_PATH=(\S+)", src)
    assert m, "the runner must set the store path explicitly"
    assert m.group(1).startswith(("$STATE", "/", "~")) and "$STATE" in m.group(1)


def test_no_secret_is_written_into_either_artifact():
    """The token comes from a mode-600 .env at runtime. A plist is world-readable
    and a runner is in git; neither may ever carry the value."""
    for path in (PLIST, RUNNER):
        text = path.read_text(encoding="utf-8")
        assert not re.search(r"[0-9a-f]{32,}", text), \
            "{} contains something shaped like a token".format(path.name)
        assert "TIINGO_API_TOKEN=" not in text.replace("$TIINGO_API_TOKEN", "")
