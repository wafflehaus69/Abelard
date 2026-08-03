"""Funding-mesh collapse (v1.16 §2) — the safety invariant that stops a sybil mesh
being reported as a large coordinated cluster."""

from consensus.m10 import collapse_actors


def _f(funder, kind, error=None):
    return {"funder": funder, "funder_kind": kind, "error": error, "latency_s": 10}


def test_sybil_mesh_collapses_to_one_actor():
    """THE case this exists for (Mojtaba): 20 wallets, one purpose-built funder, is
    n=1 evidence — not 20."""
    mesh = {f"0xw{i}": _f("0xFUNDER", "dedicated") for i in range(20)}
    assert collapse_actors(mesh) == 1


def test_genuinely_independent_actors_do_not_collapse():
    indep = {f"0xw{i}": _f(f"0xF{i}", "dedicated") for i in range(4)}
    assert collapse_actors(indep) == 4


def test_cex_funder_links_nobody():
    """A CEX hot wallet funds thousands of unrelated people; sharing one is not
    evidence of coordination. Each stays its own actor."""
    same_cex = {f"0xw{i}": _f("0xBINANCE", "cex") for i in range(12)}
    assert collapse_actors(same_cex) == 12


def test_infra_and_unknown_funders_never_link():
    mixed = {"0xa": _f("0xINFRA", "nonpersonal"), "0xb": _f("0xINFRA", "nonpersonal"),
             "0xc": _f("0xQ", "unknown")}
    assert collapse_actors(mixed) == 3        # low-confidence never hardens into a link


def test_mixed_mesh_and_independents():
    mixed = {"0xa": _f("0xF1", "dedicated"), "0xb": _f("0xF1", "dedicated"),
             "0xc": _f("0xF2", "dedicated"), "0xd": _f("0xCEX", "cex")}
    assert collapse_actors(mixed) == 3        # {F1 mesh} + F2 + the cex-funded one


def test_any_unenriched_member_makes_it_unresolved():
    """Keeping the enrichment cap (owner ruling (a)) means some clusters cannot be
    resolved. That must be UNRESOLVED, never a partial count — a partial collapse can
    only UNDER-count actors, i.e. overstate coordination."""
    partial = {"0xa": _f("0xF1", "dedicated"), "0xb": None}
    assert collapse_actors(partial) is None


def test_failed_funding_lookup_is_unresolved_not_assumed():
    assert collapse_actors({"0xa": _f(None, None, error="rate limited")}) is None
    assert collapse_actors({"0xa": _f("0xF", None)}) is None    # classifier never ran
    assert collapse_actors({}) is None


def test_union_roster_and_actor_count_describe_the_same_wallet_set():
    """Regression: the persisted roster and the actor count MUST cover one wallet set.
    Previously the count was the MIN across a wallet's clusters while the roster was
    their UNION, so a resolved 3-wallet cluster's count of 1 could be stamped onto a
    20-wallet roster whose other members were never enriched — asserting a 20->1
    collapse that was never computed."""
    import consensus.m10 as m10

    class C:
        def __init__(self, w, clusters, funding):
            self.wallet, self.cluster_ids, self.notes = w, clusters, {"funding": funding}

    ded = {"funder": "0xF1", "funder_kind": "dedicated", "error": None}
    # W is in a small RESOLVED cluster and a large cluster with unenriched members
    cands = [C("W", ["m1", "x1"], ded), C("m2", ["m1"], ded)]
    cands += [C(f"u{i}", ["x1"], None) for i in range(18)]      # unenriched
    members = {}
    for c in cands:
        for cid in c.cluster_ids:
            members.setdefault(cid, set()).add(c.wallet)
    funding = {c.wallet: c.notes.get("funding") for c in cands}
    union = {"W"}
    for cid in cands[0].cluster_ids:
        union |= members[cid]
    actors = m10.collapse_actors({w: funding.get(w) for w in union})
    assert len(union) == 20
    assert actors is None, "a roster containing unenriched members must be UNRESOLVED"
