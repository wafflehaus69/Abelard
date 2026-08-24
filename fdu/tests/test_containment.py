"""Containment tests: the invariants, asserted structurally.

These are the tests that matter most. FDU's guarantees are supposed to hold by
CONSTRUCTION rather than by policy [E11], and a guarantee nobody checks is a
policy. Each test here walks the real source and fails on the absence of a
property, not on a mock.
"""

from __future__ import annotations

import ast
import re
from pathlib import Path

import pytest

PKG = Path(__file__).resolve().parent.parent / "fdu_daemon"
MODULES = sorted(PKG.glob("*.py"))


def _source(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def test_modules_exist():
    assert len(MODULES) >= 8, f"expected the package to be present, found {MODULES}"


# -- I-1: read-only ------------------------------------------------------


@pytest.mark.parametrize("path", MODULES, ids=lambda p: p.name)
def test_no_write_verbs_anywhere(path: Path):
    """No module may call a mutating HTTP verb.

    Checked on the AST rather than by grep so a comment mentioning POST does not
    fail the test and a cleverly built call does not pass it.
    """
    tree = ast.parse(_source(path))
    banned = {"post", "put", "patch", "delete", "post_json", "put_json"}
    for node in ast.walk(tree):
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
            assert node.func.attr not in banned, (
                f"{path.name}:{node.lineno} calls .{node.func.attr}() -- I-1 forbids any "
                f"write verb against an external surface"
            )


@pytest.mark.parametrize("path", MODULES, ids=lambda p: p.name)
def test_requests_module_level_verbs_absent(path: Path):
    src = _source(path)
    for verb in ("requests.post", "requests.put", "requests.patch", "requests.delete"):
        assert verb not in src, f"{path.name} references {verb}; I-1 forbids it"


def test_fetcher_exposes_no_write_method():
    from fdu_daemon.fetch import Fetcher

    public = {n for n in dir(Fetcher) if not n.startswith("_")}
    for banned in ("post", "put", "patch", "delete", "post_json"):
        assert banned not in public, f"Fetcher exposes .{banned}() -- containment boundary breached"


def test_fetcher_does_not_expose_underlying_client():
    """The wrapped session must not be reachable as a public attribute.

    ``abelard_common``'s HttpClient carries post_json; the whole reason this
    wrapper exists is that FDU must not be able to reach it.
    """
    from fdu_daemon.fetch import Fetcher

    public = {n for n in dir(Fetcher) if not n.startswith("_")}
    assert "session" not in public


# -- I-3: no contact capability, no per-person storage -------------------


def test_no_contact_verbs_in_cli():
    from fdu_daemon import cli

    parser = cli.build_parser()
    actions = [a for a in parser._actions if hasattr(a, "choices") and a.choices]
    commands: set[str] = set()
    for action in actions:
        commands |= set(action.choices or {})
    banned = {"contact", "email", "send", "outreach", "message", "call", "export-contacts"}
    assert not (commands & banned), f"CLI exposes a contact verb: {commands & banned}"


def test_ledger_stores_no_person_columns():
    """No column may name a person attribute.

    Schedule A is full of names, titles, CRD numbers and dates of birth. The
    extractor reads them to count them; if one ever reaches the schema, this
    fails.
    """
    from fdu_daemon import ledger

    person_terms = re.compile(
        r"(first|last|middle)_?name|owner_name|person_name|email|phone|dob|"
        r"date_of_birth|ssn|individual|principal_name|contact",
        re.I,
    )
    for table_name, cols in (
        ("firm", ledger._FIRM_COLUMNS),
        ("adv_detail", ledger._ADV_COLUMNS),
        ("firm_change", ledger._CHANGE_COLUMNS),
    ):
        for col in cols:
            assert not person_terms.search(col), (
                f"{table_name}.{col} looks like per-person data -- I-3 permits structure, not people"
            )


#: Fields whose name matches the person-data tripwire but which carry ENTITY
#: data, each admitted deliberately and with a reason. The broad regex below is
#: kept broad on purpose: a new name-shaped field must fail this test and be
#: argued for here, rather than slip through a loosened pattern.
_ENTITY_NAME_ALLOWLIST = {
    # An acquired adviser is a FIRM. Its registered name is public entity data of
    # exactly the kind already held in firm.legal_name for all 23,804 firms.
    # Caveat recorded rather than hidden: a sole-proprietor RIA can be registered
    # under a person's name, so this field can incidentally contain one. It is
    # still the firm's registered name, not a dossier -- no address, no contact
    # details, no per-person row, and nothing joinable to an individual.
    "succession_acquired_names",
}


def test_adv_facts_carries_no_names():
    from fdu_daemon.adv_pdf import AdvFacts

    facts = AdvFacts(crd="1")
    row = facts.as_row(0)
    person_terms = re.compile(r"name|email|phone|dob|ssn", re.I)
    offenders = [k for k in row if person_terms.search(k) and k not in _ENTITY_NAME_ALLOWLIST]
    assert not offenders, (
        f"AdvFacts row exposes {offenders}. If these are entity-level and not "
        f"per-person, add them to _ENTITY_NAME_ALLOWLIST with a written reason."
    )


def test_entity_name_allowlist_stays_small():
    """A tripwire with a growing allowlist is not a tripwire."""
    assert len(_ENTITY_NAME_ALLOWLIST) <= 3, (
        "the person-data allowlist is growing; re-examine whether FDU has drifted "
        "into storing people"
    )


def test_owner_names_are_still_forbidden():
    """The allowlist must not have opened a door for Schedule A owners."""
    from fdu_daemon.adv_pdf import AdvFacts

    row = AdvFacts(crd="1").as_row(0)
    for banned in ("owner_names", "direct_owner_names", "control_person_names"):
        assert banned not in row, f"{banned} is per-person data -- I-3 forbids it"


def test_extractor_returns_no_owner_names(sample_schedule_a_text):
    """Feed the parser a real-shaped Schedule A and assert names do not survive."""
    from fdu_daemon.adv_pdf import _owner_tables, _parse_owner_block

    tables = _owner_tables(sample_schedule_a_text)
    count, codes, controls, acquired = _parse_owner_block(tables["A"])
    assert count == 2
    assert sorted(codes) == ["B", "E"]
    assert controls == 2
    assert acquired == ["04/2018", "04/2018"]
    # The names present in the fixture must appear nowhere in the returned data.
    blob = repr((count, codes, controls, acquired))
    for name in ("TERZO", "STOCK", "JUSTIN", "JOSEPH"):
        assert name not in blob


# -- I-7: nothing touches scout -----------------------------------------


@pytest.mark.parametrize("path", MODULES, ids=lambda p: p.name)
def test_no_scout_paths(path: Path):
    src = _source(path)
    for banned in ("scout.sqlite3", "openclaw/scout", "scout_daemon"):
        assert banned not in src, f"{path.name} references {banned}; I-7 forbids it"


# -- kill switch ---------------------------------------------------------


def test_halt_blocks_fetch(monkeypatch, tmp_path):
    from fdu_daemon import config
    from fdu_daemon.errors import HaltRequested
    from fdu_daemon.fetch import Fetcher

    monkeypatch.setenv("FDU_STATE_HOME", str(tmp_path))
    monkeypatch.setenv("FDU_HALT", "1")
    assert config.halt_requested()
    with pytest.raises(HaltRequested):
        Fetcher().get_bytes("https://example.invalid/", surface="test")


def test_halt_via_file(monkeypatch, tmp_path):
    from fdu_daemon import config

    monkeypatch.setenv("FDU_STATE_HOME", str(tmp_path))
    monkeypatch.delenv("FDU_HALT", raising=False)
    assert not config.halt_requested()
    (tmp_path / "HALT").write_text("stop")
    assert config.halt_requested()


def test_user_agent_carries_no_personal_address(monkeypatch):
    """Ruling R-PA1-2: no personal contact address is declared to federal systems."""
    import importlib

    from fdu_daemon import config

    monkeypatch.delenv("FDU_CONTACT", raising=False)
    importlib.reload(config)
    assert "@" not in config.USER_AGENT, "a contact address leaked into the declared UA"
