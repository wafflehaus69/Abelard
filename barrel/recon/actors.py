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
FANOUT_PROVISIONAL = 400                          # MR-12 ruling 4; v1.2 freezes it
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


def member_funding(rows: list[dict[str, Any]], **kw) -> dict[str, dict[str, Any] | None]:
    """wallet -> funding record for one token's members. A member whose funder was not found
    in the scanned window maps to None, which makes the whole set unresolved. ``funder_kind``
    is what collapse_actors reads: a linking class is passed to it as 'dedicated', and the
    class itself is kept in ``funder_class``."""
    out: dict[str, dict[str, Any] | None] = {}
    for r in rows:
        cls = classify_funder(r.get("funder"), r.get("fan_out"), **kw)
        out[r["w"]] = ({"funder": r["funder"], "funder_class": cls,
                        "funder_kind": "dedicated" if cls in LINKING else cls} if cls else None)
    return out


def token_record(rows: list[dict[str, Any]], **kw) -> dict[str, Any]:
    """Per-token set record: raw size, post-collapse actors (None = unresolved), and the
    u_code to add when it is unresolved. Read the actor count through ``resolution`` only."""
    mf = member_funding(rows, **kw)
    actors = collapse_actors(mf)
    rec = {"n_wallets": resolution.raw_wallet_count(list(mf)), ACTORS_KEY: actors,
           "n_funding_unknown": sum(v is None for v in mf.values())}
    rec["collapse_state"] = resolution.collapse_state(rec, list(mf))
    rec["u_codes"] = [] if actors is not None else [U_SET_FUNDING]
    return rec


def in_aligned_set(row: dict[str, Any], bundle_min: int = BUNDLE_MIN) -> bool:
    """Set membership, decided locally from what the query returned. Member rows carry
    is_creator / is_funded / bundle_n; group rows carry b_only_n (None for creator and
    creator-funded members, the same-funder count for a wallet that is only a bundle candidate)."""
    if "b_only_n" in row:
        return row["b_only_n"] is None or row["b_only_n"] >= bundle_min
    return bool(row.get("is_creator") or row.get("is_funded") or (row.get("bundle_n") or 0) >= bundle_min)


def aligned(rows: list[dict[str, Any]], bundle_min: int = BUNDLE_MIN) -> list[dict[str, Any]]:
    return [r for r in rows if in_aligned_set(r, bundle_min)]


def token_record_grouped(groups: list[dict[str, Any]], **kw) -> dict[str, Any]:
    """Same record as token_record, from the build form of the aligned-set query: one row per
    (token, funder) with ``n_members``. A group with funder None is members whose funding was
    not found, which makes the set unresolved. A linking funder's group is one actor; any other
    group counts each member."""
    n_wallets = sum(g["n_members"] for g in groups)
    unknown = sum(g["n_members"] for g in groups if not g.get("funder"))
    actors = None
    if groups and not unknown:
        linking, standalone = set(), 0
        for g in groups:      # one funder can appear in several groups; it is still one actor
            if classify_funder(g["funder"], g.get("fan_out"), **kw) in LINKING:
                linking.add(g["funder"])
            else:
                standalone += g["n_members"]
        actors = len(linking) + standalone
    rec = {"n_wallets": n_wallets, ACTORS_KEY: actors, "n_funding_unknown": unknown}
    rec["collapse_state"] = resolution.collapse_state(rec, [None] * n_wallets)
    rec["u_codes"] = [] if actors is not None else [U_SET_FUNDING]
    return rec


def fund_to_first_buy_s(rows: list[dict[str, Any]]) -> float | None:
    """Median seconds from a member's funding to its first acquisition of the token, over the
    members where both were measured. None when it was measured for nobody; never 0."""
    v = [r["fund_to_first_buy_s"] for r in rows if r.get("fund_to_first_buy_s") is not None]
    return statistics.median(v) if v else None


def block_id(creator_row: dict[str, Any] | None, creator: str, **kw) -> str:
    """Block for effective-n (MR-11 section 2, fallback per MR-12 ruling 5): the creator's funder
    when that funder links, otherwise the creator wallet itself, so each unlinked creator is its
    own block. 'F:' and 'C:' prefixes keep the two kinds apart."""
    if creator_row:
        cls = classify_funder(creator_row.get("funder"), creator_row.get("fan_out"), **kw)
        if cls in LINKING:
            return f"F:{creator_row['funder']}"
    return f"C:{creator}"
