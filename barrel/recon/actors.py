"""Wallets are not actors: funder classification and funding-mesh collapse for BARREL (MR-11).

Three things live here, all pure functions over records already fetched. Nothing in this module
talks to Dune or to an RPC, and no threshold has a default: the caller passes the ruled value.

1. ``resolution`` is CONSENSUS's module, loaded from ``consensus/consensus/resolution.py``. It is
   not copied. It is the one place that decides what "not measured" means; BARREL records use
   its key (``actor_count_post_collapse``) and its readers (``actor_count``, ``collapse_state``,
   ``fmt``), so an unresolved count can never be read as a number.

2. ``collapse_actors`` is ported verbatim from ``consensus/consensus/m10.py`` (consensus commit
   f3aeb0d). It cannot be imported: m10 pulls in the CONSENSUS data layer at import time.
   ``tests/test_actors.py`` holds the two to the same answers whenever m10 is importable.

3. ``classify_funder`` is the label-first classifier ruled in MR-12 (ruling 4). m5's fan-out
   test alone does not classify on Solana (``docs/CROSSOVER_MR11.md``), so:

       nonpersonal   a known program-owned or infrastructure address          never links
       cex           a labelled exchange wallet, whatever its fan-out         never links
       factory       unlabelled, high fan-out, on the caller's factory list   LINKS
       hub           unlabelled, fan-out at or above the threshold            never links
       dedicated     unlabelled, fan-out below the threshold (purpose-built)  LINKS
       unknown       fan-out was not measured                                 never links

   The threshold is 400, provisional until v1.2 freezes it; 32 and 8,192 are the sensitivity
   values. The factory class is pre-registered but not shipped: the list is empty unless the
   caller passes one, and it ships in v1.2 only if the measurement in
   ``docs/FACTORY_CLASS.md`` is accepted.
"""
from __future__ import annotations

import importlib.util
import pathlib
import statistics
from typing import Any, Iterable

_REPO = pathlib.Path(__file__).resolve().parents[2]


def _load_resolution():
    path = _REPO / "consensus" / "consensus" / "resolution.py"
    spec = importlib.util.spec_from_file_location("consensus_resolution", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


resolution = _load_resolution()

KINDS = ("nonpersonal", "cex", "factory", "hub", "dedicated", "unknown")
LINKING = ("dedicated", "factory")
FANOUT_PROVISIONAL = 400                          # MR-12 ruling 4, a COUNT. Superseded by a per-day rate (MR-15 B3), set in v1.2;
                                                  # kept only to compare forms on rows that still carry counts
FANOUT_SENSITIVITY = (32, 8192)
BUNDLE_MIN = 5                                    # S8: creation-slot traders sharing one funder; applied here, never in a query
ACTORS_KEY = "actor_count_post_collapse"          # the key resolution.actor_count reads
U_SET_FUNDING = "SETA_FUNDING_UNKNOWN"            # u_codes entry when a set cannot be collapsed


def collapse_actors(member_funding: dict[str, dict[str, Any] | None]) -> int | None:
    """Verbatim port of consensus.m10.collapse_actors. None = UNRESOLVED when any member's
    funding is unknown: a partial collapse could only under-count actors."""
    if not member_funding:
        return None
    dedicated_funders: set[str] = set()
    standalone = 0
    for _wallet, fund in member_funding.items():
        if not fund or fund.get("error") or not fund.get("funder_kind"):
            return None
        if fund["funder_kind"] == "dedicated" and fund.get("funder"):
            dedicated_funders.add(fund["funder"])
        else:
            standalone += 1
    return len(dedicated_funders) + standalone


def classify_funder(funder: str | None, fan_out: int | None, *, fanout_threshold: int,
                    labelled_cex: Iterable[str] = (), nonpersonal: Iterable[str] = (),
                    factories: Iterable[str] = ()) -> str | None:
    """Kind of a funding source. None when there is no funder to classify."""
    if not funder:
        return None
    if funder in nonpersonal:
        return "nonpersonal"
    if funder in labelled_cex:
        return "cex"
    if funder in factories:
        return "factory"
    if fan_out is None:
        return "unknown"
    return "hub" if fan_out >= fanout_threshold else "dedicated"


def _fan(row: dict[str, Any], funder: str | None, rates: dict[str, float] | None) -> float | None:
    """The funder's fan-out measure. Under MR-15 (B3) it is recipients per day over the chunk's
    calendar month, from the separate fan-out query, passed in as ``rates``; a funder absent from
    a rates table that was supplied sent nothing in the window, which is a measured 0. Rows from
    before MR-15 carry a raw count in ``fan_out``, on whatever window their query scanned."""
    if rates is not None:
        return rates.get(funder, 0.0) if funder else None
    return row.get("fan_rate", row.get("fan_out"))


def _split(kw: dict) -> tuple[dict[str, float] | None, dict]:
    kw = dict(kw)
    return kw.pop("rates", None), kw


def member_funding(rows: list[dict[str, Any]], **kw) -> dict[str, dict[str, Any] | None]:
    """wallet -> funding record for one token's members. A member whose funder was not found
    in the scanned window maps to None, which makes the whole set unresolved. ``funder_kind``
    is what collapse_actors reads: a linking class is passed to it as 'dedicated', and the
    class itself is kept in ``funder_class``."""
    rates, kw = _split(kw)
    out: dict[str, dict[str, Any] | None] = {}
    for r in rows:
        cls = classify_funder(r.get("funder"), _fan(r, r.get("funder"), rates), **kw)
        out[r["w"]] = ({"funder": r["funder"], "funder_class": cls,
                        "funder_kind": "dedicated" if cls in LINKING else cls} if cls else None)
    return out


class _Components:
    def __init__(self):
        self.p: dict[Any, Any] = {}

    def find(self, x):
        self.p.setdefault(x, x)
        while self.p[x] != x:
            self.p[x] = self.p[self.p[x]]
            x = self.p[x]
        return x

    def union(self, a, b):
        self.p[self.find(a)] = self.find(b)

    def count(self) -> int:
        return len({self.find(x) for x in self.p})


def _record(n_wallets: int, actors: int | None, unknown: int) -> dict[str, Any]:
    rec = {"n_wallets": n_wallets, ACTORS_KEY: actors, "n_funding_unknown": unknown}
    rec["collapse_state"] = resolution.collapse_state(rec, [None] * n_wallets)
    rec["u_codes"] = [] if actors is not None else [U_SET_FUNDING]
    return rec


def token_record(rows: list[dict[str, Any]], **kw) -> dict[str, Any]:
    """Per-token set record from MEMBER rows: raw size, post-collapse actors (None = unresolved),
    and the u_code to add when unresolved. Read the actor count through ``resolution`` only.

    Collapse is collapse_actors' rule (wallets sharing a linking funder are one actor) plus MR-15
    R1: a member that is another member's linking funder is the same actor as the wallets it
    funds. Without R1 a creator and the wallets it funded are always at least two actors."""
    mf = member_funding(rows, **kw)
    unknown = sum(v is None for v in mf.values())
    if not mf or collapse_actors(mf) is None:
        return _record(len(mf), None, unknown)
    comp = _Components()
    node = {w: (("A", f["funder"]) if f["funder_kind"] == "dedicated" else ("W", w)) for w, f in mf.items()}
    for n in node.values():
        comp.find(n)
    linking_funders = {n[1] for n in node.values() if n[0] == "A"}
    for w in mf:                       # R1: member w is itself a linking funder of another member
        if w in linking_funders:
            comp.union(node[w], ("A", w))
    return _record(len(mf), comp.count(), unknown)


def in_aligned_set(row: dict[str, Any], bundle_min: int = BUNDLE_MIN) -> bool:
    """Set membership, decided locally from what the query returned. Member rows carry
    is_creator / is_funded / bundle_n; group rows and member-funder rows carry b_only_n (None for
    the creator and creator-funded members, the same-funder count for a wallet that is only a
    bundle candidate)."""
    if "b_only_n" in row:
        return row["b_only_n"] is None or row["b_only_n"] >= bundle_min
    if "bundle_n" in row:
        return bool(row["is_creator"] or row["is_funded"] or (row["bundle_n"] or 0) >= bundle_min)
    if "is_bundle" in row:      # member rows from before MR-14: the cut was applied in the query
        return True
    raise KeyError("row is neither a group row (b_only_n), a member row (bundle_n) nor an earlier member row (is_bundle)")


def aligned(rows: list[dict[str, Any]], bundle_min: int = BUNDLE_MIN) -> list[dict[str, Any]]:
    return [r for r in rows if in_aligned_set(r, bundle_min)]


def split_grouped(rows: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """The build query returns two kinds of row in one result: funder groups, and one row for each
    member that is also the funder of another member ('member_funder'). Rows from before MR-15
    have no row_kind and are all groups."""
    groups = [r for r in rows if r.get("row_kind", "group") == "group"]
    mfs = [r for r in rows if r.get("row_kind") == "member_funder"]
    return groups, mfs


def token_record_grouped(groups: list[dict[str, Any]], member_funders: list[dict[str, Any]] | None = None, **kw) -> dict[str, Any]:
    """Same record as token_record, from the BUILD form: one row per (token, funder, candidate
    class) with ``n_members``, plus the member-funder rows. A group with funder None is members
    whose funding was not found: the set is unresolved. A linking funder is one actor however many
    groups it appears in; any other group counts each member; and by R1 a member that is a linking
    funder is merged with the actor it funds.

    ``member_funders`` rows: {member, funder (the member's own), b_only_n}. When none are supplied
    (rows from before MR-15) the creator's is reconstructed from the group that holds the creator;
    other member-funders are then not seen, and the count can be one too high per such member."""
    rates, kw = _split(kw)
    n_wallets = sum(g["n_members"] for g in groups)
    unknown = sum(g["n_members"] for g in groups if not g.get("funder"))
    if not groups or unknown:
        return _record(n_wallets, None, unknown)

    def links(funder, row):
        return classify_funder(funder, _fan(row, funder, rates), **kw) in LINKING

    comp, standalone = _Components(), 0
    for g in groups:
        if links(g["funder"], g):
            comp.find(("A", g["funder"]))
        else:
            standalone += g["n_members"]
    if member_funders is None:
        member_funders = [{"member": g["creator"], "funder": g["funder"], "b_only_n": None, **{k: g[k] for k in ("fan_rate", "fan_out") if k in g}}
                          for g in groups if g.get("n_creator") and g.get("creator")]
    merged_standalone = 0
    for m in aligned(member_funders):
        x = m["member"]
        if ("A", x) not in comp.p:               # x does not link anyone in the aligned set
            continue
        if links(m.get("funder"), m):
            comp.union(("A", x), ("A", m["funder"]))
        else:
            merged_standalone += 1               # x was counted once as a standalone member
    return _record(n_wallets, comp.count() + standalone - merged_standalone, unknown)


def fund_to_first_buy_s(rows: list[dict[str, Any]]) -> float | None:
    """Median seconds from a member's first SOL inflow from its funder to its first acquisition of
    the token, across the set's members where both were measured (MR-12 ruling 2). None when it
    was measured for nobody; never 0."""
    v = []
    for r in rows:
        if "lat_s" in r:                                   # group rows: every member's seconds
            v += [x for x in (r["lat_s"] or []) if x is not None]
        elif r.get("fund_to_first_buy_s") is not None:     # member rows
            v.append(r["fund_to_first_buy_s"])
    return statistics.median(v) if v else None


def block_id(creator_row: dict[str, Any] | None, creator: str, **kw) -> str:
    """Block for effective-n, keyed by ADDRESS alone (MR-15 R2): the creator's funder when that
    funder links, otherwise the creator wallet. One wallet is one block whether it appears as a
    funder of creators, as a creator, or both."""
    rates, kw = _split(kw)
    if creator_row and creator_row.get("funder"):
        if classify_funder(creator_row["funder"], _fan(creator_row, creator_row["funder"], rates), **kw) in LINKING:
            return creator_row["funder"]
    return creator
