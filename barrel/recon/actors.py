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

3. ``classify_funder`` is m5's classifier re-derived for Solana. m5 calls a funder an exchange
   when its fan-out passes a threshold calibrated on Polygon USDC. On Solana SOL transfers that
   discriminant does not separate exchanges from wallet factories (``docs/CROSSOVER_MR11.md``):
   half the labelled exchange wallets active in the calibration week paid 15 or fewer
   recipients, and about 15,000 unlabelled senders paid more than 1,000. So the kinds are:

       nonpersonal   a known program-owned or infrastructure address
       cex           a labelled exchange wallet (Dune ``cex_solana.addresses``)
       high_fanout   unlabelled, fan-out at or above the threshold: exchange, or factory, unknown which
       dedicated     unlabelled, fan-out below the threshold
       unknown       fan-out was not measured

   Only ``dedicated`` links wallets. Every other kind counts each wallet as its own actor, which
   can only overstate the number of actors, the safe direction for a safety screen.
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

KINDS = ("nonpersonal", "cex", "high_fanout", "dedicated", "unknown")
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
                    labelled_cex: Iterable[str] = (), nonpersonal: Iterable[str] = ()) -> str | None:
    """Kind of a funding source. None when there is no funder to classify."""
    if not funder:
        return None
    if funder in nonpersonal:
        return "nonpersonal"
    if funder in labelled_cex:
        return "cex"
    if fan_out is None:
        return "unknown"
    return "high_fanout" if fan_out >= fanout_threshold else "dedicated"


def member_funding(rows: list[dict[str, Any]], *, fanout_threshold: int,
                   labelled_cex: Iterable[str] = (), nonpersonal: Iterable[str] = ()
                   ) -> dict[str, dict[str, Any] | None]:
    """wallet -> funding record for one token's members. A member whose funder was not found
    in the scanned window maps to None, which makes the whole set unresolved."""
    out: dict[str, dict[str, Any] | None] = {}
    for r in rows:
        kind = classify_funder(r.get("funder"), r.get("fan_out"), fanout_threshold=fanout_threshold,
                               labelled_cex=labelled_cex, nonpersonal=nonpersonal)
        out[r["w"]] = {"funder": r["funder"], "funder_kind": kind} if kind else None
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


def fund_to_first_buy_s(rows: list[dict[str, Any]]) -> float | None:
    """Median seconds from a member's funding to its first acquisition of the token, over the
    members where both were measured. None when it was measured for nobody; never 0."""
    v = [r["fund_to_first_buy_s"] for r in rows if r.get("fund_to_first_buy_s") is not None]
    return statistics.median(v) if v else None


def block_id(creator_row: dict[str, Any] | None, launch_day: str, **kw) -> str:
    """Block for effective-n (MR-11 section 2): the creator's funder when that funder is
    dedicated, otherwise the launch day. 'F:' and 'D:' prefixes keep the two kinds apart."""
    if creator_row:
        kind = classify_funder(creator_row.get("funder"), creator_row.get("fan_out"), **kw)
        if kind == "dedicated":
            return f"F:{creator_row['funder']}"
    return f"D:{launch_day}"
