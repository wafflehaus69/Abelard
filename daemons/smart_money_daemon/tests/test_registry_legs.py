"""Registry composition: the four legs must survive a manager-shelf sync.

sync_manager_registry read through artifact_path (state home ONLY) while every
reader resolves through find_artifact (state home, THEN the committed repo copy).
On a fresh state home the read found nothing, `kept` came out empty, and the
function silently wrote away the 12 congress, 2 trump_network and 1 thiel_network
entries it promises to leave untouched. Nothing asserted registry composition, so
nothing caught it — the live file ran as 19 manager_13f entries and no others, and
congressional sentinel activity became structurally unreachable rather than merely
quiet.
"""
import json
import os

from smart_money import scorecard


def _repo_registry(tmp_path):
    """A committed repo copy: the four legs, with a STALE manager shelf."""
    p = tmp_path / "repo_registry.json"
    entries = [
        {"person_id": 24, "name": "Evans, Dwight", "cik": None,
         "role": "performer", "scores": {"x": 1}},
        {"person_id": 407, "name": "McCormick, David H.", "cik": None,
         "role": "btc_flow_sentinel", "scores": None},
        {"person_id": 28, "name": "Foxx, Virginia", "cik": None,
         "role": "qualitative_watch", "scores": None},
        {"person_id": 32, "name": "Guest, Michael Patrick", "cik": None,
         "role": "performer_seeded", "scores": None},
        {"person_id": None, "name": "TRUMP DONALD J", "cik": "0000947033",
         "role": "trump_network", "scores": None},
        {"person_id": None, "name": "Trump Donald J. JR", "cik": "0002016181",
         "role": "trump_network", "scores": None},
        {"person_id": None, "name": "THIEL PETER", "cik": "0001211060",
         "role": "thiel_network", "scores": None},
        # the stale shelf that must NOT come back
        {"person_id": None, "name": "Old Fund", "cik": "0000000001",
         "role": "manager_13f", "scores": None},
    ]
    p.write_text(json.dumps({"as_of": "2026-07-22", "entries": entries}),
                 encoding="utf-8")
    return str(p)


def _wire(monkeypatch, state, repo):
    monkeypatch.setattr(scorecard.dbmod, "artifact_path",
                        lambda *a, **k: state)
    monkeypatch.setattr(scorecard.dbmod, "find_artifact",
                        lambda *a, **k: state if os.path.exists(state) else repo)


def _roles(path):
    with open(path, encoding="utf-8") as fh:
        out = {}
        for e in json.load(fh)["entries"]:
            out[e.get("role")] = out.get(e.get("role"), 0) + 1
        return out


def test_sync_on_a_fresh_state_home_keeps_the_other_legs(tmp_path, monkeypatch):
    """THE regression. State home empty, repo copy present: the manager shelf is
    rewritten and every other leg survives."""
    state = str(tmp_path / "state_registry.json")
    repo = _repo_registry(tmp_path)
    _wire(monkeypatch, state, repo)

    out_path, n_mgr = scorecard.sync_manager_registry(anchor="2026-08-14")

    assert out_path == state, "must write to the state home"
    roles = _roles(state)
    assert roles.get("performer") == 1
    assert roles.get("btc_flow_sentinel") == 1
    assert roles.get("qualitative_watch") == 1
    assert roles.get("performer_seeded") == 1
    assert roles.get("trump_network") == 2
    assert roles.get("thiel_network") == 1
    assert n_mgr == len(scorecard.MANAGER_13F_SEEDS)
    assert roles.get("manager_13f") == n_mgr
    # the stale repo shelf must not survive the rewrite
    with open(state, encoding="utf-8") as fh:
        names = {e["name"] for e in json.load(fh)["entries"]}
    assert "Old Fund" not in names


def test_sync_is_idempotent(tmp_path, monkeypatch):
    state = str(tmp_path / "state_registry.json")
    repo = _repo_registry(tmp_path)
    _wire(monkeypatch, state, repo)
    scorecard.sync_manager_registry(anchor="2026-08-14")
    first = _roles(state)
    scorecard.sync_manager_registry(anchor="2026-08-14")
    assert _roles(state) == first


def test_registry_always_carries_more_than_one_leg(tmp_path, monkeypatch):
    """Composition guard. A registry that is 100% one role is the failure signature,
    not a valid state — it means a leg-scoped writer clobbered the others."""
    state = str(tmp_path / "state_registry.json")
    repo = _repo_registry(tmp_path)
    _wire(monkeypatch, state, repo)
    scorecard.sync_manager_registry(anchor="2026-08-14")
    roles = _roles(state)
    assert len(roles) > 1, "single-role registry means a leg was clobbered: %r" % roles
    non_mgr = sum(v for k, v in roles.items() if k != "manager_13f")
    assert non_mgr > 0, roles


def test_restore_re_adds_only_the_missing_non_manager_legs(tmp_path):
    """The repair for registries already truncated in production."""
    state = str(tmp_path / "state_registry.json")
    repo = _repo_registry(tmp_path)
    # a truncated live file: managers only
    with open(state, "w", encoding="utf-8") as fh:
        json.dump({"as_of": "2026-07-31", "entries": [
            {"person_id": None, "name": "New Fund", "cik": "0000000009",
             "role": "manager_13f", "scores": None}]}, fh)

    missing, roles = scorecard.restore_registry_legs(state_path=state,
                                                     repo_path=repo)
    assert len(missing) == 7, roles
    assert roles == {"performer": 1, "btc_flow_sentinel": 1,
                     "qualitative_watch": 1, "performer_seeded": 1,
                     "trump_network": 2, "thiel_network": 1}
    # dry run wrote nothing
    assert _roles(state) == {"manager_13f": 1}

    scorecard.restore_registry_legs(state_path=state, repo_path=repo, apply=True)
    after = _roles(state)
    assert after.get("manager_13f") == 1, "the live shelf must not be replaced"
    assert after.get("trump_network") == 2
    assert sum(after.values()) == 8


def test_restore_is_idempotent(tmp_path):
    state = str(tmp_path / "state_registry.json")
    repo = _repo_registry(tmp_path)
    with open(state, "w", encoding="utf-8") as fh:
        json.dump({"as_of": "x", "entries": []}, fh)
    scorecard.restore_registry_legs(state_path=state, repo_path=repo, apply=True)
    first = _roles(state)
    missing, _ = scorecard.restore_registry_legs(state_path=state, repo_path=repo,
                                                 apply=True)
    assert missing == [], "second run must find nothing to re-add"
    assert _roles(state) == first
