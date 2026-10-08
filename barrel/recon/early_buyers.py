"""Early-buyer groups (kind b3, columns cluster_*, hypothesis H2): the local reader.

The query (recon/gen_heavy_b2_b3.py::cluster) returns facts and no cut. Everything that judges is
here, as pure functions over rows already fetched. Nothing in this module talks to Dune, and the
line between a hub and a purpose-built funder has no default: the caller passes the ruled value.

One row of the export (one per token and funder the reduction kept; funder None = a token with no group):

  mint, grad_time          the token and its graduation (pool creation) time, 'YYYY-MM-DD hh:mm:ss.fff UTC'
  funder                   the common funder; None on the single row of a token with no group at all
  labelled                 the funder is in the exchange label table (a fact, read in the query)
  recipients, window_days  the funder's distinct recipients of at least 0.001 SOL over the chunk's calendar
                           month and the days in that month; 0 = it sent nothing in the month (measured)
  n_buyers                 early buyers of the token that this funder paid in the 7 days before graduation
  buy_s                    the earliest member buys, seconds from graduation, ascending, at most BUY_S_KEPT
  n_early, n_funded, n_groups, n_pairs, n_max_any
                           counts over ALL groups of the token, taken before the reduction: early buyers,
                           early buyers with any funder, groups, (buyer, funder) pairs, largest group of any funder
  vqr                      the pool's derived virtual quote reserve, lamports; None = no qualifying buy
  ev_no_slot, ev_no_txi    events of the pool inside its horizon without a slot / without a transaction index
  t_first_s, q_first, b_first
                           the first member sell of all inside the horizon: seconds from graduation and the
                           pre-swap real reserves (kept to regress against the rows of the earlier text)
  t1_s, q_t1, b_t1, px_t1, net_base, n_sold_pre
                           one element per entry lag, in the order of LAGS: the first member sell at or after
                           the lag and within 24 hours of it (seconds from graduation, pre-swap real reserves,
                           price with the virtual reserve); the group's net base position from pool trades
                           before the lag; members that had already sold before it

The reduction and what it guarantees. Within a token, and separately for labelled and unlabelled
funders, the query keeps a group when it is larger than every group behind a funder with no more
recipients than its own. So for ANY line on recipients per day, the largest unlabelled group below
the line is among the kept rows (tests/test_early_buyers.py, on random tables). It guarantees
nothing else: a funder that only a LOCAL list rules out (a label the query did not see, an
infrastructure address) may have hidden a smaller group, and a factory group (unlabelled, above the
line, links) may have been dropped behind a larger hub. `blind_spots` counts the first; the second
has no count and matters only once the factory class is in force (v1.2).

NOT RULED, each kept in one function so a ruling changes one place and no query:
  the_cluster      one cluster per token = the largest group behind a linking funder
  cluster_actors   the collapsed column holds that group's size; the raw column the any-funder maximum
  at_lag           "detected" = the moment the size-th member bought; entry = graduation + lag
"""
from __future__ import annotations

import datetime as dt
import pathlib
import sys
from typing import Any, Iterable

sys.path.insert(0, str(pathlib.Path(__file__).parent))
import actors  # noqa: E402

LAGS = (15, 60, 240)        # entry lags, minutes: the order of every per-lag array the query returns
BUY_S_KEPT = 8              # earliest member buys the query returns per group (gen_heavy_b2_b3.cluster, `kept`)
SYNDICATE_MIN = 5           # H2 as pre-registered (M0_TASKING section 6): "a cluster of >= 5 wallets". It decides,
                            # so it is applied here and never in a query (MR-14 ruling 5)
PER_LAG = ("t1_s", "q_t1", "b_t1", "px_t1", "net_base", "n_sold_pre")

Row = dict[str, Any]


def parse_ts(s: str) -> dt.datetime:
    """A timestamp as Dune returns it: '2025-06-09 12:34:56.000 UTC'."""
    return dt.datetime.strptime(s.replace(" UTC", ""), "%Y-%m-%d %H:%M:%S.%f").replace(tzinfo=dt.timezone.utc)


def by_token(rows: Iterable[Row]) -> dict[str, list[Row]]:
    out: dict[str, list[Row]] = {}
    for r in rows:
        out.setdefault(r["mint"], []).append(r)
    return out


def groups(token_rows: Iterable[Row]) -> list[Row]:
    """The group rows of a token: everything but the placeholder row of a token with no group."""
    return [r for r in token_rows if r.get("funder")]


def rate(row: Row) -> float | None:
    """Fan-out as ruled (MR-15 B3): recipients per day of the chunk's calendar month."""
    if not row.get("funder") or row.get("recipients") is None:
        return None
    return row["recipients"] / row["window_days"]


def funder_kind(row: Row, *, line: float, labelled_cex: Iterable[str] = (), nonpersonal: Iterable[str] = (),
                factories: Iterable[str] = ()) -> str | None:
    """Kind of the row's funder by recon/actors.py, with `line` in recipients per day. A funder is an
    exchange when the query found its label OR the local list names it: the reduction was made on the
    query's label, so that fact is never dropped here."""
    f = row.get("funder")
    if f and row.get("labelled") and f not in nonpersonal:
        return "cex"
    return actors.classify_funder(f, rate(row), fanout_threshold=line, labelled_cex=labelled_cex,
                                  nonpersonal=nonpersonal, factories=factories)


def _order(r: Row) -> tuple:
    # the query's ORDER BY, in gf and again in ranked:
    #   CASE WHEN labelled THEN 0 ELSE recipients END, n_buyers DESC, funder
    return (0 if r["labelled"] else r["recipients"], -r["n_buyers"], r["funder"])


def reduce_reference(group_rows: Iterable[Row]) -> list[Row]:
    """The query's reduction, in Python, CTE by CTE (gf, ranked, kept of gen_heavy_b2_b3.cluster). Takes
    the FULL group table (mint, funder, labelled, recipients, n_buyers) and returns the rows the query
    keeps, each with the token-level counts the query attaches before it drops anything. Used by the
    tests, and to regress the reduction on full group tables held locally."""
    out = []
    for _mint, rs in by_token(group_rows).items():
        # gf: count(*), sum(n_buyers), max(n_buyers) OVER (PARTITION BY mint)
        n_groups, n_pairs, n_max_any = len(rs), sum(r["n_buyers"] for r in rs), max(r["n_buyers"] for r in rs)
        for lab in (False, True):                                  # PARTITION BY mint, labelled
            part = sorted((r for r in rs if bool(r["labelled"]) == lab), key=_order)
            runmax, set_by = None, set()
            for r in part:
                # gf: max(n_buyers) OVER (PARTITION BY mint, labelled ORDER BY .. ROWS UNBOUNDED PRECEDING) AS runmax
                runmax = r["n_buyers"] if runmax is None else max(runmax, r["n_buyers"])
                # ranked: row_number() OVER (PARTITION BY mint, labelled, runmax ORDER BY the same) AS rn; kept: rn = 1
                if runmax not in set_by:
                    set_by.add(runmax)
                    out.append({**r, "n_groups": n_groups, "n_pairs": n_pairs, "n_max_any": n_max_any})
    return out


def the_cluster(token_rows: Iterable[Row], *, line: float, **lists) -> Row | None:
    """NOT RULED (the schema has one cluster per token; H2 says "a cluster"). Reading taken: the
    token's cluster is the largest group behind a funder that links (MR-11: purpose-built or factory;
    an exchange, a hub, an infrastructure address never links). Equal sizes: fewer recipients, then
    the funder's address, which is the order of the query's reduction, so the answer from the kept
    rows is the answer from all rows. None when no funder links. A token with several syndicates
    shows one."""
    linking = [r for r in groups(token_rows) if funder_kind(r, line=line, **lists) in actors.LINKING]
    return min(linking, key=lambda r: (-r["n_buyers"], r["recipients"], r["funder"])) if linking else None


def raw_twin(token_rows: Iterable[Row]) -> dict[str, Any] | None:
    """The raw value beside the collapsed one (MR-12 ruling 3): the largest group of ANY funder, which
    is the schema's cluster_n as written, and its funder. On the calibration week this is an exchange
    or a hub on almost every token (CROSSOVER_MR11.md). None when the token has no row."""
    rows = list(token_rows)
    if not rows:
        return None
    g = groups(rows)
    if not g:
        return {"cluster_n": 0, "cluster_src": None, "labelled": None}
    top = [r for r in g if r["n_buyers"] == r["n_max_any"]]
    if not top:
        raise ValueError("no kept row carries the token's largest group: the rows are not one token's full export")
    r = min(top, key=lambda r: (bool(r["labelled"]), r["recipients"], r["funder"]))
    return {"cluster_n": r["n_max_any"], "cluster_src": r["funder"], "labelled": bool(r["labelled"])}


def cluster_actors(token_rows: Iterable[Row], *, line: float, **lists) -> int | None:
    """NOT RULED. Column 89 is "cluster_n after collapse; NULL when unresolved", and the hypotheses
    read the collapsed value (MR-12 ruling 3). Read literally through collapse_actors, wallets behind
    one linking funder are ONE actor, and no cluster could ever reach five. Reading taken (the
    builder's in CROSSOVER_MR11.md): applying the funder kind IS the collapse, so the column holds the
    size of the largest group behind a linking funder; 0 when the token has groups and none links.
    None only when the token has no row at all: it was not measured."""
    rows = list(token_rows)
    if not rows:
        return None
    c = the_cluster(rows, line=line, **lists)
    return c["n_buyers"] if c else 0


def detection_s(row: Row, size: int = SYNDICATE_MIN) -> int | None:
    """Seconds from graduation at which the group reached `size` members: the size-th earliest member
    buy. None when the group never reaches it."""
    if row["n_buyers"] < size:
        return None
    b = sorted(row.get("buy_s") or [])
    if size > len(b):
        raise ValueError(f"size {size} is above the {BUY_S_KEPT} buy offsets the query returns per group")
    return b[size - 1]


def at_lag(row: Row, lag: int, size: int = SYNDICATE_MIN) -> dict[str, Any]:
    """What the group row says at one entry lag. NOT RULED: H2's entry is "the first observable moment
    the cluster is detected + lag G", while every entry price of the event table is anchored on
    graduation. Reading taken: the entry is graduation + lag, and the cluster is `detectable` when its
    size-th member bought before that moment. `holding`: the members' pool trades before the lag net
    to a positive base position. t1 is the first member sell at or after the lag, within 24 hours."""
    i = LAGS.index(lag)
    d = detection_s(row, size)
    t1_s = row["t1_s"][i]
    return {"lag": lag, "detected_s": d, "detectable": d is not None and d < lag * 60,
            "net_base": row["net_base"][i], "holding": (row["net_base"][i] or 0) > 0,
            "n_sold_pre": row["n_sold_pre"][i], "t1_s": t1_s,
            "t1": None if t1_s is None else parse_ts(row["grad_time"]) + dt.timedelta(seconds=t1_s),
            "q_t1": row["q_t1"][i], "b_t1": row["b_t1"][i], "px_t1": row["px_t1"][i]}


def exit_value(base_in: float, q: float | None, b: float | None, v: float | None, *,
               sell_cost_bps: float) -> dict[str, float] | None:
    """The ruled fill for a position of `base_in` tokens at a pre-swap pool state (MR-14 order 5):
    spot-marked = base_in * (q + v) / B; curve proceeds = (q + v) * base_in / (B + base_in);
    exit-adjusted = the smaller of the curve proceeds and q (the virtual term sets the price and cannot
    be withdrawn), then net of the token's own observed sell cost. `sell_cost_bps` has no default: pass
    the token's measured cost, or 0.0 to say the gross value is wanted. `ratio` is exit-adjusted over
    spot-marked; the floor F it is read against is set in v1.2 and is not here. None when the state or
    the virtual reserve is missing: never an assumed value."""
    if base_in is None or q is None or b is None or v is None or b <= 0 or base_in <= 0:
        return None
    spot = base_in * (q + v) / b
    curve = (q + v) * base_in / (b + base_in)
    gross = min(curve, q)
    net = gross * (1 - sell_cost_bps / 1e4)
    return {"spot": spot, "curve": curve, "capped_by_reserve": curve > q, "exit_gross": gross,
            "exit_adjusted": net, "ratio": net / spot if spot > 0 else None}


def exit_at_lag(row: Row, lag: int, base_in: float, *, sell_cost_bps: float) -> dict[str, float] | None:
    """exit_value at the group's first sell at or after the lag. None when no member sold in the window
    (the time stop applies, from the event table) or the pool has no derived virtual reserve."""
    i = LAGS.index(lag)
    return exit_value(base_in, row["q_t1"][i], row["b_t1"][i], row.get("vqr"), sell_cost_bps=sell_cost_bps)


def cluster_columns(token_rows: Iterable[Row], *, line: float, size: int = SYNDICATE_MIN, **lists) -> dict[str, Any]:
    """The cluster_* values of one token at one fan-out line, raw and collapsed side by side.
    cluster_t1 and cluster_px_t1 come back per lag: the schema has one column for each and which lag
    fills it is not ruled. `is_syndicate` is H2's size cut on the collapsed value, applied here."""
    rows = list(token_rows)
    raw = raw_twin(rows)
    c = the_cluster(rows, line=line, **lists)
    actors_n = cluster_actors(rows, line=line, **lists)
    lags = {g: at_lag(c, g, size) for g in LAGS} if c else {}
    return {"cluster_n": raw["cluster_n"] if raw else None,
            "cluster_src_raw": raw["cluster_src"] if raw else None,
            "cluster_actors": actors_n,
            "cluster_src": c["funder"] if c else None,
            "is_syndicate": None if actors_n is None else actors_n >= size,
            "cluster_t1": {g: v["t1"] for g, v in lags.items()},
            "cluster_px_t1": {g: v["px_t1"] for g, v in lags.items()},
            "lags": lags}


def blind_spots(rows: Iterable[Row], *, labelled_cex: Iterable[str] = (), nonpersonal: Iterable[str] = ()) -> dict[str, int]:
    """Where the kept rows cannot be trusted to hold the largest linking group. The reduction ran on the
    query's label only. A kept row in the unlabelled part whose funder a LOCAL list says never links
    may have hidden a smaller group behind a funder with more recipients: on that token the size read
    from the kept rows is a lower bound. `query_only_labels` counts rows the query labelled and the
    local list does not (the label table is live); they are read as exchanges here either way."""
    never, cex = set(labelled_cex) | set(nonpersonal), set(labelled_cex)
    lower, query_only, n = set(), 0, 0
    for mint, rs in by_token(rows).items():
        n += 1
        for r in groups(rs):
            if not r["labelled"] and r["funder"] in never:
                lower.add(mint)
            if r["labelled"] and r["funder"] not in cex:
                query_only += 1
    return {"tokens": n, "tokens_lower_bound_only": len(lower), "query_only_labels": query_only}


def vqr_cross_check(rows: Iterable[Row], events_rows: Iterable[Row], tol_lamports: float = 1000.0) -> dict[str, Any]:
    """The row's derived virtual reserve against the event export's, per token. Both come from the same
    rule (MR-15 B1) on windows of different length (28 hours here, 8 days there), and the value is fixed
    for the pool's life, so they should agree to lamports: the derivation matched the pool accounts to
    within 77 lamports (VQR_CHECK.md), and the default tolerance is a loose multiple of that. A value on
    one side only is expected when the pool's first qualifying buy came after 28 hours. The lists hold
    mints: count them, do not print them."""
    ev = {r["mint"]: r.get("vqr") for r in events_rows}
    out: dict[str, Any] = {"tokens": 0, "agree": 0, "both_null": 0, "max_abs_diff": 0.0, "null_here_only": [],
                           "null_in_events_only": [], "differ": [], "not_in_events": [], "not_one_value": []}
    for mint, rs in by_token(rows).items():
        out["tokens"] += 1
        mine = {r.get("vqr") for r in groups(rs)}
        if len(mine) > 1:
            out["not_one_value"].append(mint)
        elif not mine:
            continue                                    # no group: the reserve rides on group rows only
        elif mint not in ev:
            out["not_in_events"].append(mint)
        else:
            a, b = next(iter(mine)), ev[mint]
            if a is None and b is None:
                out["both_null"] += 1
            elif a is None:
                out["null_here_only"].append(mint)
            elif b is None:
                out["null_in_events_only"].append(mint)
            else:
                out["max_abs_diff"] = max(out["max_abs_diff"], abs(a - b))
                if abs(a - b) <= tol_lamports:
                    out["agree"] += 1
                else:
                    out["differ"].append(mint)
    return out


def fill_counts(rows: Iterable[Row]) -> dict[str, Any]:
    """What the export manifest cannot count: nulls inside the per-lag arrays, the order-key fill the
    query reports (E38), and whether each returned price is the price of the returned reserves."""
    out: dict[str, Any] = {"rows": 0, "tokens": 0, "tokens_no_group": 0, "tokens_vqr_null": 0,
                           "tokens_ev_no_slot": 0, "tokens_ev_no_txi": 0, "px_not_from_reserves": 0,
                           "null": {c: [0] * len(LAGS) for c in PER_LAG}}
    for _mint, rs in by_token(rows).items():
        out["tokens"] += 1
        out["rows"] += len(rs)
        g = groups(rs)
        if not g:
            out["tokens_no_group"] += 1
            continue
        out["tokens_vqr_null"] += g[0].get("vqr") is None
        out["tokens_ev_no_slot"] += bool(g[0].get("ev_no_slot"))
        out["tokens_ev_no_txi"] += bool(g[0].get("ev_no_txi"))
        for r in g:
            for c in PER_LAG:
                for i in range(len(LAGS)):
                    out["null"][c][i] += r[c][i] is None
            for i in range(len(LAGS)):
                q, b, px = r["q_t1"][i], r["b_t1"][i], r["px_t1"][i]
                want = None if q is None or not b or r.get("vqr") is None else (q + r["vqr"]) / b
                if (px is None) != (want is None) or (px is not None and abs(px - want) > 1e-9 * abs(want)):
                    out["px_not_from_reserves"] += 1
    return out
