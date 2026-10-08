"""Organic taker counts from the cell rows of the org2 query (recon/gen_org2.py).

Pure functions over rows already fetched. Nothing here talks to Dune, no file is read, and no number that
decides anything has a home here.

The query returns, per token, counts of takers by cell (cls, fsx, bn, gs). Each wallet of a token is in
exactly one cell, so a count over any set of cells is a plain sum and a first-swap time is a minimum. Which
cells are organic is decided here, from what the rulings name (MR-6):

  the creator, the migration authority   cls C and M (the creator only where has_creator: see _cls)
  fee-share recipients (S7b)             fsx, for the codes the CALLER names. Which as-of applies when S7b is
                                         used as an organic exclusion is not ruled, so there is no default.
  same-slot bundle wallets (S8)          bn at or above actors.BUNDLE_MIN. The five is a verdict (MR-14
                                         ruling 5): it is read from actors when a function is called.

org1 = the takers left after those exclusions. org2 = those of them with at least one swap that was aged and
not bot-shaped when it happened (MR-7); the query decides that per swap, here it is a count that is summed.
The first-hour count of org2 is H5's marker (MR-13); it has no column in the 91-column schema.
Both collapsed twins (org1_actors_7d, org2_actors_7d) stay None: nothing measures them yet.
"""
from __future__ import annotations

import datetime as dt
import pathlib
import sys
from typing import Any, Iterable

sys.path.insert(0, str(pathlib.Path(__file__).parent))
import actors

# Fee-share codes, one per (token, wallet), set by the local code that reads the fee-share export.
FS_CODES = ("E", "L", "P")                  # E in force at graduation + 240 min; L becomes one later within 7 days; P only before
FS_AT_ENTRY = frozenset({"E"})              # the as-of-entry reading (c07b; H3 spec R7)
FS_THROUGH_DAY_7 = frozenset({"E", "L"})    # a recipient at any time from entry to the end of the count
FS_EVER = frozenset(FS_CODES)

U_NOT_MEASURED = "ORG_NOT_MEASURED"         # the token has no row in the result
U_S8 = "ORG_S8_NOT_MEASURED"                # no same-funder count for this token: bundle wallets could not be set apart
U_S7B = "ORG_S7B_NOT_SUPPLIED"              # the run was made without a fee-share list: recipients are still counted
U_CREATOR = "ORG_CREATOR_NOT_FOUND"         # created more than 70 days before its graduation day: the creator is still counted
PUMPSWAP_BIRTH = dt.date(2025, 3, 20)       # no PumpSwap event exists before it

KEYS = ("cls", "fsx", "bn", "gs")
COUNTS = ("n1", "n2", "n1_1h", "n2_1h", "n_aged", "n_notbot", "n_many", "n_quick", "n_nohist")


def by_token(rows: Iterable[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    out: dict[str, list[dict[str, Any]]] = {}
    for r in rows:
        out.setdefault(r["mint"], []).append(r)
    return out


def cells(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """A token's cells. A token with no taker comes back as one row with a NULL cell and zero counts
    (the query's LEFT JOIN from the universe); that row is the proof it was measured, not a cell."""
    return [r for r in rows if r["cls"] is not None]


def _cls(cell: dict[str, Any]) -> str:
    """The class an exclusion is applied to. The query marks the creator as the event query finds it, with a
    lookback from the chunk's first day; a token is given the same answer wherever it falls in a chunk only if
    the creator is set apart when the creation lies within 70 days before the token's OWN graduation day
    (has_creator). Otherwise the creator's cell counts as anyone else's, and the record says so (U_CREATOR)."""
    return "O" if cell["cls"] == "C" and not cell["has_creator"] else cell["cls"]


def organic_cell(cell: dict[str, Any], fs_exclude: Iterable[str], bundle_min: int | None = None) -> bool:
    """True when the wallets of this cell are organic by the v1 exclusion set."""
    if bundle_min is None:
        bundle_min = actors.BUNDLE_MIN          # read at call time: the constant in actors is the only one
    if cell["fsx"] is not None and cell["fsx"] not in FS_CODES:
        raise ValueError("a cell carries a fee-share code this module does not know")
    return (_cls(cell) == "O" and cell["fsx"] not in set(fs_exclude)
            and (cell["bn"] is None or cell["bn"] < bundle_min))


def _why_not(cell: dict[str, Any], fs_exclude: set[str], bundle_min: int) -> str:
    """The first exclusion that applies, in a fixed order, so the excluded takers add up without overlap."""
    if _cls(cell) != "O":
        return "creator" if cell["cls"] == "C" else "migrator"
    return "fee_share" if cell["fsx"] in fs_exclude else "bundle"


def _one(rows: list[dict[str, Any]], key: str) -> Any:
    v = {r[key] for r in rows}
    if len(v) != 1:
        raise ValueError(f"{key} differs between rows of one token: these are not the rows of one run")
    return v.pop()


def _min(cs: list[dict[str, Any]], key: str) -> int | None:
    v = [c[key] for c in cs if c[key] is not None]
    return min(v) if v else None


def s8_state(rows: list[dict[str, Any]], n_slot0: int | None = None) -> str:
    """'measured' or 'unknown'. Unknown when the token is outside the 3-day creation scope of the same-funder
    count, and, when the caller passes b1a's n_slot0 for the token, when no creation-slot trade was decoded."""
    return "measured" if _one(rows, "s8_scope") and n_slot0 != 0 else "unknown"


def token_record(rows: list[dict[str, Any]], *, fs_exclude: Iterable[str], bundle_min: int | None = None,
                 n_slot0: int | None = None) -> dict[str, Any]:
    """Organic counts of ONE token from its cell rows.

    ``fs_exclude``: the fee-share codes that make a wallet not organic (FS_AT_ENTRY, FS_THROUGH_DAY_7,
    FS_EVER, or empty). ``bundle_min``: the S8 cut, actors.BUNDLE_MIN when not given.

    A count is a number whenever the token has rows. What could not be applied is said in u_codes, never
    hidden in the number: an exclusion that was not measured leaves its wallets counted."""
    if bundle_min is None:
        bundle_min = actors.BUNDLE_MIN
    fs_exclude = set(fs_exclude)
    if not fs_exclude <= set(FS_CODES):
        raise ValueError("fs_exclude names a code that is not one of FS_CODES")
    twins = {"org1_actors_7d": None, "org2_actors_7d": None}
    if not rows:
        return {"org1_n_7d": None, "org2_n_7d": None, "org1_t_s": None, "org2_t_s": None,
                "org1_n_1h": None, "org2_n_1h": None, **twins, "u_codes": [U_NOT_MEASURED]}
    _one(rows, "mint")
    cs = cells(rows)
    kept = [c for c in cs if organic_cell(c, fs_exclude, bundle_min)]
    excluded = {"creator": 0, "migrator": 0, "fee_share": 0, "bundle": 0}
    for c in cs:
        if not organic_cell(c, fs_exclude, bundle_min):
            excluded[_why_not(c, fs_exclude, bundle_min)] += c["n1"]
    s8, supplied = s8_state(rows, n_slot0), _one(rows, "fs_supplied")
    u_codes = ([U_S8] if s8 == "unknown" else []) + ([] if supplied else [U_S7B]) \
        + ([] if _one(rows, "has_creator") else [U_CREATOR])
    return {
        "org1_n_7d": sum(c["n1"] for c in kept), "org2_n_7d": sum(c["n2"] for c in kept),
        "org1_t_s": _min(kept, "t1_s"), "org2_t_s": _min(kept, "t2_s"),
        "org1_n_1h": sum(c["n1_1h"] for c in kept), "org2_n_1h": sum(c["n2_1h"] for c in kept),   # org2_n_1h: H5's marker
        **twins,
        "n_takers": sum(c["n1"] for c in cs), "excluded": excluded,
        # why org1 and org2 differ, over the organic cells: takers with an aged swap, with a swap on a day they
        # were not bot-shaped, and takers that met each bot leg on some swap day
        "gap": {k: sum(c[k] for c in kept) for k in ("n_aged", "n_notbot", "n_many", "n_quick")},
        "s8_state": s8, "fs_state": "applied" if supplied else "not_supplied",
        "lb_days": _one(rows, "lb_days"), "u_codes": u_codes,
    }


def proxy_v1(rows: list[dict[str, Any]]) -> dict[str, int | None]:
    """What the event query calls org1 (not creator, not migrator, not a graduation-slot trader), rebuilt
    from the cells: class O with the graduation-slot flag false, summed over every other key."""
    cs = [c for c in cells(rows) if c["cls"] == "O" and not c["gs"]]
    return {"org1_n_7d": sum(c["n1"] for c in cs), "org1_t_s": _min(cs, "t1_s")}


def reconcile_proxy(rows: list[dict[str, Any]], ev_n: int | None, ev_t_s: int | None, n_owner: int = 0) -> str:
    """Hold the rebuilt proxy against the events export's org1_n_7d / org1_t_s for the same token.

      'equal'    both agree. The event query gives NULL, not 0, for a token with no such taker.
      'owner'    the event query counts up to ``n_owner`` more takers and its first swap is not later. That is
                 what an owner wallet trading the token looks like: the org2 query drops owner wallets
                 (MR-3.4) and the event query does not. Expected, not a failure; ``n_owner`` is how many
                 owner wallets there are, and only the caller can know which tokens they traded.
      'differs'  anything else: the two queries do not describe the same takers."""
    p = proxy_v1(rows)
    n, t, en = p["org1_n_7d"], p["org1_t_s"], ev_n or 0
    if n == en and t == ev_t_s:
        return "equal"
    if 0 < en - n <= n_owner and ev_t_s is not None and (t is None or ev_t_s <= t):
        return "owner"
    return "differs"


def bundle_hist(rows: list[dict[str, Any]]) -> dict[int, int]:
    """Takers of one token by same-funder count (cells with a count only): what the cut is applied to."""
    out: dict[int, int] = {}
    for c in cells(rows):
        if c["bn"] is not None:
            out[c["bn"]] = out.get(c["bn"], 0) + c["n1"]
    return dict(sorted(out.items()))


def bundle_takers_check(taker_rows: list[dict[str, Any]], member_rows: list[dict[str, Any]],
                        bundle_min: int | None = None) -> dict[str, int]:
    """Wallet by wallet: the same-funder count of gen_org2.bundle_takers against the aligned-set MEMBER rows of
    the same day. Member rows carry bundle_n; rows from before MR-14 carry is_bundle (the cut already applied),
    and then only the side of the cut can be compared. A taker with a count is a creation-slot trader, so it
    must be a member row. Pass = no 'different' and no 'not_in_members'."""
    if bundle_min is None:
        bundle_min = actors.BUNDLE_MIN
    member = {(m["mint"], m["w"]): m for m in member_rows}
    out = {"takers": len(taker_rows), "same": 0, "different": 0, "not_in_members": 0}
    for r in taker_rows:
        m = member.get((r["mint"], r["w"]))
        if m is None:
            out["not_in_members"] += 1
        elif "bundle_n" in m:
            out["same" if m["bundle_n"] == r["bn"] else "different"] += 1
        else:
            out["same" if bool(m["is_bundle"]) == (r["bn"] >= bundle_min) else "different"] += 1
    return out


def problems(rows: list[dict[str, Any]]) -> list[str]:
    """Checks on ONE token's rows that need no other data. An empty list is the pass."""
    out = []
    for key in ("mint", "s8_scope", "has_creator", "fs_supplied", "lb_days"):
        if len({r[key] for r in rows}) > 1:
            out.append(f"{key} differs between rows")
    cs = cells(rows)
    if len(rows) - len(cs) > (0 if cs else 1):
        out.append("a row with no cell beside other rows")
    if len({tuple(c[k] for k in KEYS) for c in cs}) != len(cs):
        out.append("a cell appears twice: the token is in the universe more than once")
    for c in cs:
        if not (0 < c["n1"] and 0 <= c["n2_1h"] <= c["n2"] <= c["n1"] and c["n2_1h"] <= c["n1_1h"] <= c["n1"]):
            out.append("counts out of order (expected n2_1h <= n2 <= n1 and n2_1h <= n1_1h <= n1)")
        if c["n2"] > min(c["n_aged"], c["n_notbot"]):
            out.append("more qualifying takers than takers with an aged swap or with a swap on a not-bot day")
        if c["t1_s"] is None or c["t1_s"] < 0 or (c["t2_s"] is not None and c["t2_s"] < c["t1_s"]):
            out.append("first-swap seconds out of order")
        if (c["n2"] > 0) != (c["t2_s"] is not None):
            out.append("a qualifying count without its time, or a time without its count")
        if c["n_nohist"]:
            out.append("a swap with no wallet-history row: the two event references disagree")
        if c["cls"] != "O" and c["n1"] != 1:
            out.append("more than one wallet in a creator or migrator cell")
        if c["bn"] is not None and not rows[0]["s8_scope"]:
            out.append("a same-funder count on a token outside the scope")
        if c["fsx"] is not None and not rows[0]["fs_supplied"]:
            out.append("a fee-share code on a run made without a list")
    return sorted(set(out))


def lookback_truncated(grad_day: dt.date, lb_days: int) -> bool:
    """True when the wallet-age lookback of a token's first swap day reaches back past PumpSwap's birth. For
    those tokens a wallet can only be as old as the venue: on 2025-03-20 itself no wallet is aged, and the
    first weeks understate org2 for every lookback. The calibration slice starts there."""
    return grad_day - dt.timedelta(days=lb_days) < PUMPSWAP_BIRTH
