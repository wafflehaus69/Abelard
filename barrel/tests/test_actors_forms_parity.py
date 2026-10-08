"""The member form and the build (grouped) form of the aligned set must give the same record.

Random small sets are generated as member rows, turned into group rows and member-funder rows the
way recon/sql/heavy_b1a_grouped_*.sql does it (GROUPING SETS over each member taken once as a
member and once as its funder), and both forms are run through recon/actors.py. Any disagreement
in size, actor count or state is a defect in one of the two readers.
Run: python -m pytest barrel/tests/test_actors_forms_parity.py"""
import collections
import pathlib
import random
import sys

import pytest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1] / "recon"))
import actors  # noqa: E402

KW = dict(fanout_threshold=100, labelled_cex={"EXCH"})
LINKS = [None, actors.LINK_SOL, actors.LINK_DELIVERY, actors.LINK_BOTH]


def make_members(rng: random.Random) -> list[dict]:
    """One token's members. Funders are drawn from other members, a small pool of outside wallets
    with small or large fan-out, an exchange, or nobody."""
    n = rng.randint(1, 9)
    wallets = ["cr"] + [f"w{i}" for i in range(1, n)]
    outside = {"F1": 3, "F2": 7, "HUB": 5000, "EXCH": 2}
    rows = []
    for w in wallets:
        pool = [x for x in wallets if x != w] + list(outside) + [None, None]
        funder = rng.choice(pool)
        is_creator = w == "cr"
        is_funded = (not is_creator) and rng.random() < 0.6
        rows.append({"w": w, "is_creator": is_creator, "is_funded": is_funded,
                     # a member is in the candidate list as the creator, as creator-funded, or as a creation-slot
                     # trader; only the last kind can be there on its same-funder count alone, and then it has one
                     "bundle_n": None if is_creator else rng.choice([None, 2, 4, 5, 9] if is_funded else [1, 2, 4, 5, 9]),
                     "link": rng.choice(LINKS[1:]) if is_funded else None,
                     "funder": funder})
    fan = {**outside, **{w: rng.choice([1, 4, 60, 400]) for w in wallets}}
    for r in rows:
        r["fan_out"] = fan.get(r["funder"]) if r["funder"] else None
    return rows


def to_grouped(members: list[dict]) -> list[dict]:
    """What the build query returns for these members: 'group' rows per (funder, b_only_n, link) and
    a 'member_funder' row for each wallet that is both a member and the funder of a member."""
    fan = {r["funder"]: r["fan_out"] for r in members if r["funder"]}
    groups = collections.defaultdict(list)
    for r in members:
        bo = None if (r["is_creator"] or r["is_funded"]) else r["bundle_n"]
        groups[(r["funder"], bo, r["link"])].append(r)
    out = [{"row_kind": "group", "mint": "T", "funder": f, "b_only_n": bo, "link": lk, "member": None,
            "creator": "cr", "n_members": len(v), "n_creator": sum(x["is_creator"] for x in v),
            "fan_out": fan.get(f)} for (f, bo, lk), v in groups.items()]
    by_w = {r["w"]: r for r in members}
    for x in sorted({r["funder"] for r in members if r["funder"] in by_w}):
        m = by_w[x]
        out.append({"row_kind": "member_funder", "mint": "T", "funder": None, "b_only_n": None, "member": x,
                    "member_own_funder": m["funder"], "member_link": m["link"],
                    "member_b_only_n": None if (m["is_creator"] or m["is_funded"]) else m["bundle_n"],
                    "n_members": 1, "fan_out": fan.get(m["funder"])})
    return out


@pytest.mark.parametrize("switch", [False, True])
@pytest.mark.parametrize("cut", [5, 4])
def test_member_and_grouped_forms_agree_on_random_sets(switch, cut):
    rng = random.Random(20261005)
    seen = collections.Counter()
    for _ in range(4000):
        members = make_members(rng)
        a = actors.token_record(actors.aligned(members, cut), delivery_as_creator=switch, **KW)
        groups, mfs = actors.split_grouped(to_grouped(members))
        b = actors.token_record_grouped(actors.aligned(groups, cut), mfs, bundle_min=cut, delivery_as_creator=switch, **KW)
        key = lambda r: (r["n_wallets"], r[actors.ACTORS_KEY], r["collapse_state"], r["n_funding_unknown"])  # noqa: E731
        assert key(a) == key(b), (members, a, b)
        seen[a["collapse_state"]] += 1
    assert all(seen[s] > 50 for s in ("unresolved", "collapsed", "independent", "solo")), seen   # the generator covers every state


def test_a_delivered_wallet_is_the_creators_actor_by_default():
    """MR-17 ruling 2. Off is the reading before the ruling, kept for comparison."""
    members = [{"w": "cr", "is_creator": True, "is_funded": False, "bundle_n": None, "link": None, "funder": "EXCH", "fan_out": 2},
               {"w": "a", "is_creator": False, "is_funded": True, "bundle_n": None, "link": actors.LINK_DELIVERY, "funder": None, "fan_out": None},
               {"w": "b", "is_creator": False, "is_funded": True, "bundle_n": None, "link": actors.LINK_SOL, "funder": "HUB", "fan_out": 5000}]
    assert actors.DELIVERY_AS_CREATOR is True
    ruled = actors.token_record(members, **KW)
    before = actors.token_record(members, delivery_as_creator=False, **KW)
    assert actors.resolution.actor_count(ruled) == 2 and ruled["collapse_state"] != "unresolved"     # a is the creator's; b stands alone
    assert actors.resolution.actor_count(before) is None and before["collapse_state"] == "unresolved"  # a has no SOL funder
    groups, mfs = actors.split_grouped(to_grouped(members))
    assert actors.resolution.actor_count(actors.token_record_grouped(groups, mfs, **KW)) == 2          # the build form, same default


def _both_forms(members, **extra):
    groups, mfs = actors.split_grouped(to_grouped(members))
    a = actors.token_record(members, **extra, **KW)
    b = actors.token_record_grouped(groups, mfs, **extra, **KW)
    assert actors.resolution.actor_count(a) == actors.resolution.actor_count(b), (a, b)
    return actors.resolution.actor_count(a)


def test_whoever_paid_its_sol_a_delivered_wallet_is_the_creators():
    """'Whoever paid its SOL': an exchange, a hub, or nobody. The wallet does not stand alone."""
    for funder, fan in (("EXCH", 2), ("HUB", 5000), (None, None)):
        members = [{"w": "cr", "is_creator": True, "is_funded": False, "bundle_n": None, "link": None, "funder": "EXCH", "fan_out": 2},
                   {"w": "a", "is_creator": False, "is_funded": True, "bundle_n": None, "link": actors.LINK_BOTH, "funder": funder, "fan_out": fan}]
        assert _both_forms(members) == 1, funder


def test_a_delivered_wallets_own_linking_funder_joins_the_creator_with_it():
    """The builder's reading, stated in the ruling record: actors are components. F1 is purpose-built and paid
    the delivered wallet a and an outside bundle wallet c; a is the creator's, so F1 and c are too."""
    members = [{"w": "cr", "is_creator": True, "is_funded": False, "bundle_n": None, "link": None, "funder": "EXCH", "fan_out": 2},
               {"w": "a", "is_creator": False, "is_funded": True, "bundle_n": None, "link": actors.LINK_DELIVERY, "funder": "F1", "fan_out": 3},
               {"w": "c", "is_creator": False, "is_funded": False, "bundle_n": 6, "link": None, "funder": "F1", "fan_out": 3},
               {"w": "d", "is_creator": False, "is_funded": False, "bundle_n": 6, "link": None, "funder": "HUB", "fan_out": 5000}]
    assert _both_forms(members) == 2                                   # {cr, a, F1, c} and d
    assert _both_forms(members, delivery_as_creator=False) == 3        # before the ruling: cr | {a, c} behind F1 | d


def test_the_constant_is_the_switch_when_a_reader_is_called(monkeypatch):
    """Found in review: bound as a default argument, the constant was read once at import and setting it did nothing."""
    members = [{"w": "cr", "is_creator": True, "is_funded": False, "bundle_n": None, "link": None, "funder": "EXCH", "fan_out": 2},
               {"w": "a", "is_creator": False, "is_funded": True, "bundle_n": None, "link": actors.LINK_DELIVERY, "funder": None, "fan_out": None}]
    groups, mfs = actors.split_grouped(to_grouped(members))
    assert actors.token_record(members, **KW)["collapse_state"] != "unresolved"
    monkeypatch.setattr(actors, "DELIVERY_AS_CREATOR", False)
    assert actors.token_record(members, **KW)["collapse_state"] == "unresolved"
    assert actors.token_record_grouped(groups, mfs, **KW)["collapse_state"] == "unresolved"
