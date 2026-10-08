"""Heavy tier, groups B2 (c01-c04 authority and extension history) and B3 (cluster_* for H2).

  recon/sql/heavy_b2_auth_{day,week}.sql      one row per token; no wallet in the output
  recon/sql/heavy_b3_cluster_{day,week,pbday}.sql   one row per (token, common funder) the reduction
                                              keeps; names funders, so run with --private-rows

B2 returns the raw history facts as-of graduation + 240 min, plus the fill counts E35 asks for.
PASS / FAIL / UNKNOWN is assigned locally, never here.

B3 (build form, MR-18): early buyers = wallets with a PumpSwap buy on the graduation pool within
30 minutes of graduation. Funder = any sender of at least 0.001 SOL to that wallet in the 7 days
before graduation, the wallet itself excepted. A group is the early buyers of one token that one
funder paid; a wallet with several funders is in several groups. These are the definitions the
three runs of 2026-10-01 used, so the rows held from them remain a free regression.

One row per (token, funder) carries, with no cut of any kind: the group's size; its earliest
member buys as seconds from graduation; for each entry lag (15, 60, 240 minutes) the time, the
pre-swap real reserves and the price of the first member sell AT OR AFTER the lag and within 24
hours of it, the group's net base position from pool trades before the lag, and how many members
had already sold; the first member sell of all (kept only to regress against the held rows); the
funder's recipients over the chunk's calendar month (the measure of recon/gen_fanout.py, here for
every sender); and whether the funder is a labelled exchange wallet (a fact, read from the label
table). The price uses the pool's virtual quote reserve, derived from the buys this query already
reads with the event query's own expression (MR-15 B1): no qualifying buy, NULL price.

The group table is not exportable as it stands (113,709 rows a week even with the size cut the
runner now refuses), so it is reduced in the query WITHOUT A CONSTANT: within a token, and
separately for labelled and unlabelled funders, a group is kept when it is larger than every group
behind a funder with no more recipients than its own. Whatever line is later drawn on recipients
per day, the largest group below it is one of the rows kept (tests/test_early_buyers.py proves it
on random tables). Counts over ALL groups of the token are taken before anything is dropped and
ride on every kept row. A token with no group returns one row with a NULL funder, so a token that
is missing is never mistaken for a token with nothing to report.

Every window is per token (30 minutes, 7 days, each lag and its 24 hours). The fan-out is not: it
is a property of the funder over one fixed window, the chunk's calendar month (MR-15 B3).

Effective scans, counting each WITH block once per reference: SOL transfers 2 (the funding window,
graduation days + 8; the calendar month), buy events 1, sell events 1, graduation days + 3 each.
Every block that reads a large table is referenced once. `u` (two small event tables) is read 4 times.

Not in the query, applied locally by recon/early_buyers.py and recon/actors.py: the syndicate size,
the line between a hub and a purpose-built funder, which funder kinds link (MR-11), which group is
"the cluster", detection, the exit fill. Inside one transaction two events share an order key; the
time and the two reserves of a "first sell" are then taken by three aggregates on the same key.
"""
import datetime as dt
import pathlib

ROOT = pathlib.Path(__file__).resolve().parents[1]
WSOL = "So11111111111111111111111111111111111111112"


def universe(gd0: str, gd1: str) -> tuple[str, dict]:
    d0, d1 = dt.date.fromisoformat(gd0), dt.date.fromisoformat(gd1)
    p = dict(gd0=gd0, gd1=gd1, cr0=(d0 - dt.timedelta(days=3)).isoformat(),
             pool_end=(d1 + dt.timedelta(days=1)).isoformat(),
             ev1=(d1 + dt.timedelta(days=9)).isoformat(),
             st0=(d0 - dt.timedelta(days=7)).isoformat(), st1=(d1 + dt.timedelta(days=2)).isoformat())
    sql = f"""WITH comp AS (
  SELECT mint, evt_block_time AS ct FROM pumpdotfun_solana.pump_evt_completeevent
  WHERE evt_block_date BETWEEN DATE '{gd0}' AND DATE '{gd1}'),
pc AS (
  SELECT base_mint AS mint, quote_mint, pool, evt_block_time AS pt,
         row_number() OVER (PARTITION BY base_mint ORDER BY evt_block_time) AS rn
  FROM pumpdotfun_solana.pump_amm_evt_createpoolevent
  WHERE evt_block_date BETWEEN DATE '{gd0}' AND DATE '{p['pool_end']}'),
u AS (
  SELECT c.mint, p.pt AS grad_time, p.pool
  FROM comp c JOIN pc p ON p.mint = c.mint AND p.rn = 1 AND p.pt >= c.ct AND p.pt < c.ct + INTERVAL '1' DAY
  WHERE p.quote_mint = '{WSOL}')"""
    return sql, p


def auth(gd0: str, gd1: str) -> str:
    h, p = universe(gd0, gd1)
    rng = f"BETWEEN DATE '{p['cr0']}' AND DATE '{p['pool_end']}'"
    return f"""-- Heavy tier B2: c01-c02 inputs, graduations {gd0} .. {gd1}. History facts as-of grad + 240 min.
-- Token-2022 extensions (c03, c04) are not in this query: they use the raw-call method of
-- VALIDATION_1_5_S3_S4.md and apply only to Token-2022 mints.
-- Scope limit as elsewhere: mints initialised within 3 days before graduation; others n_init = 0.
{h},
init AS (
  SELECT 'spl' AS prog, account_mint AS mint, mintAuthority AS ma, freezeAuthority AS fa
  FROM spl_token_solana.spl_token_call_initializemint2 WHERE call_block_date {rng}
  UNION ALL
  SELECT 't22', account_mint, mintAuthority, freezeAuthority
  FROM spl_token_2022_solana.spl_token_2022_call_initializemint2 WHERE call_block_date {rng}
  UNION ALL
  SELECT 'spl', account_mint, mintAuthority, freezeAuthority
  FROM spl_token_solana.spl_token_call_initializemint WHERE call_block_date {rng}
  UNION ALL
  SELECT 't22', account_mint, mintAuthority, freezeAuthority
  FROM spl_token_2022_solana.spl_token_2022_call_initializemint WHERE call_block_date {rng}),
setauth AS (
  SELECT account_owned AS mint, CAST(authorityType AS varchar) AS atype, newAuthority AS na, call_block_time AS t, call_block_slot AS slot
  FROM spl_token_solana.spl_token_call_setauthority WHERE call_block_date {rng}
  UNION ALL
  SELECT account_mint, CAST(authorityType AS varchar), newAuthority, call_block_time, call_block_slot
  FROM spl_token_2022_solana.spl_token_2022_call_setauthority WHERE call_block_date {rng}),
i AS (SELECT u.mint, count(*) AS n_init, arbitrary(i.prog) AS prog, count(i.ma) AS init_mint_auth_filled, count(i.fa) AS init_freeze_auth_filled
      FROM u JOIN init i ON i.mint = u.mint GROUP BY 1),
s AS (SELECT u.mint, count(*) AS n_set, count(s.na) AS n_set_to_address,
             array_join(array_agg(DISTINCT s.atype), ',') AS set_types,
             max_by(s.na IS NULL, s.slot) FILTER (WHERE lower(s.atype) LIKE '%mint%') AS mint_auth_last_is_null,
             max_by(s.na IS NULL, s.slot) FILTER (WHERE lower(s.atype) LIKE '%freeze%') AS freeze_auth_last_is_null
      FROM u JOIN setauth s ON s.mint = u.mint AND s.t <= u.grad_time + INTERVAL '240' MINUTE GROUP BY 1)
SELECT u.mint, coalesce(i.n_init, 0) AS n_init, i.prog, i.init_mint_auth_filled, i.init_freeze_auth_filled,
       coalesce(s.n_set, 0) AS n_set, s.n_set_to_address, s.set_types, s.mint_auth_last_is_null, s.freeze_auth_last_is_null
FROM u LEFT JOIN i ON i.mint = u.mint LEFT JOIN s ON s.mint = u.mint"""


def cluster(gd0: str, gd1: str) -> str:
    import gen_fanout     # the ruled fan-out window is defined there, once (MR-15 B3)

    h, p = universe(gd0, gd1)
    d0, d1 = dt.date.fromisoformat(gd0), dt.date.fromisoformat(gd1)
    if (d0.year, d0.month) != (d1.year, d1.month):
        raise ValueError("early-buyer groups: a scope that spans two calendar months has no single fan-out window")
    lags = (15, 60, 240)      # entry lags in minutes; recon/early_buyers.py reads every per-lag array in this order
    kept = 8                  # earliest member buys returned per group; early_buyers.BUY_S_KEPT must say the same
    # a pool can be created up to a day after the chunk's last completion, and a sell counts until 240 minutes plus
    # 24 hours after graduation: the events must reach the third day after the last graduation day (the text that
    # ran stopped the buys one day after it and cut the last half hour of a pool created late that day)
    ev1 = (d1 + dt.timedelta(days=3)).isoformat()
    m0, m1, mdays = gen_fanout.month_window(d0)
    win = ("side = 'sell' AND t >= grad_time + INTERVAL '{g}' MINUTE"
           " AND t < grad_time + INTERVAL '{g}' MINUTE + INTERVAL '24' HOUR")
    per_w = ",\n".join(
        f"    min(k) FILTER (WHERE {win.format(g=g)}) AS k{g},\n"
        f"    min_by(t, k) FILTER (WHERE {win.format(g=g)}) AS s{g},\n"
        f"    min_by(q, k) FILTER (WHERE {win.format(g=g)}) AS q{g},\n"
        f"    min_by(b, k) FILTER (WHERE {win.format(g=g)}) AS b{g},\n"
        f"    sum(CASE WHEN side = 'buy' THEN base_amt ELSE -base_amt END)"
        f" FILTER (WHERE t < grad_time + INTERVAL '{g}' MINUTE) AS hold{g},\n"
        f"    bool_or(side = 'sell' AND t < grad_time + INTERVAL '{g}' MINUTE) AS sold{g}"
        for g in lags)
    # what the funding join carries for each early buyer, so that neither the events nor the join is read twice
    facts = [["grad_time", "n_early", "v", "ev_no_slot", "ev_no_txi"], ["t_buy", "k_any", "s_any", "q_any", "b_any"]]
    facts += [[f"{c}{g}" for c in ("k", "s", "q", "b", "hold", "sold")] for g in lags]
    carry = ",\n         ".join(", ".join(f"arbitrary(e.{c}) AS {c}" for c in line) for line in facts)
    per_g = ",\n".join(
        f"         min_by(date_diff('second', grad_time, s{g}), k{g}) AS t1_{g},"
        f" min_by(q{g}, k{g}) AS q1_{g}, min_by(b{g}, k{g}) AS b1_{g},\n"
        f"         sum(hold{g}) AS hold_{g}, count_if(sold{g}) AS nsp_{g}"
        for g in lags)
    # one order for the running maximum and for the row that set it: recipients ascending for unlabelled funders
    # (one constant for labelled ones, so their largest group is the only one kept), then size descending, then funder
    order = "CASE WHEN labelled THEN 0 ELSE recipients END, n_buyers DESC, funder"

    def arr(expr: str) -> str:
        return "ARRAY[" + ", ".join(expr.format(g=g) for g in lags) + "]"

    return f"""-- Heavy tier B3 (build form): early-buyer groups by common funder, cluster_* inputs (H2), graduations {gd0} .. {gd1}.
-- Rows name wallets: run with --private-rows. No threshold, no classification, no verdict here.
-- One row per (token, funder) kept by the reduction at the end; one row with a NULL funder for a token with no group.
-- Group sizes, month recipients and the label fact are returned; every cut is local (recon/early_buyers.py).
{h},
ev AS (      -- one pass over the buys and sells of the pools
  SELECT pool, 'buy' AS side, "user" AS w, evt_block_time AS t,
         -- order inside a second: block time is whole seconds, so slot first, then position in the slot
         evt_block_slot * 100000 + coalesce(evt_tx_index, 0) AS k,
         (evt_block_slot IS NULL) AS no_slot, (evt_tx_index IS NULL) AS no_txi,
         CAST(pool_quote_token_reserves AS double) AS q, CAST(pool_base_token_reserves AS double) AS b,
         -- ix_name is NULL on every decoded buy (buyevent_variant_probe.sql): net, what enters the curve, is the
         -- smaller of the two quote fields, whichever variant emitted the event
         least(CAST(quote_amount_in AS double), CAST(user_quote_amount_in AS double)) AS net,
         CAST(base_amount_out AS double) AS base_amt
  FROM pumpdotfun_solana.pump_amm_evt_buyevent
  WHERE evt_block_date BETWEEN DATE '{gd0}' AND DATE '{ev1}' AND pool IN (SELECT pool FROM u)
  UNION ALL
  SELECT pool, 'sell', "user", evt_block_time,
         evt_block_slot * 100000 + coalesce(evt_tx_index, 0),
         (evt_block_slot IS NULL), (evt_tx_index IS NULL),
         CAST(pool_quote_token_reserves AS double), CAST(pool_base_token_reserves AS double),
         CAST(NULL AS double), CAST(base_amount_in AS double)
  FROM pumpdotfun_solana.pump_amm_evt_sellevent
  WHERE evt_block_date BETWEEN DATE '{gd0}' AND DATE '{ev1}' AND pool IN (SELECT pool FROM u)),
evh AS (     -- the horizon of each token: graduation to the last lag plus 24 hours, wherever it falls in the chunk
  SELECT u.mint, u.grad_time, ev.*,
         -- virtual quote reserve, derived per pool from its own buys with the expression of the event query
         -- (MR-15 B1): V = B * x / base_out - Q - x at the largest buy of at least 0.001 SOL; none, NULL
         max_by(CASE WHEN ev.side = 'buy' AND ev.base_amt > 0 AND ev.net >= 1e6 THEN ev.b * ev.net / ev.base_amt - ev.q - ev.net END,
                CASE WHEN ev.side = 'buy' AND ev.base_amt > 0 AND ev.net >= 1e6 THEN ev.net END) OVER (PARTITION BY ev.pool) AS v,
         -- E38: the two columns the order key is built from were never null-counted per era; the counts come back
         count_if(ev.no_slot) OVER (PARTITION BY ev.pool) AS ev_no_slot,
         count_if(ev.no_txi) OVER (PARTITION BY ev.pool) AS ev_no_txi
  FROM ev JOIN u ON u.pool = ev.pool
  WHERE ev.t >= u.grad_time AND ev.t < u.grad_time + INTERVAL '240' MINUTE + INTERVAL '24' HOUR),
wal AS (     -- one row per (token, wallet): first early buy, first sell, and the first sell at or after each entry lag
  SELECT mint, w, arbitrary(grad_time) AS grad_time, arbitrary(v) AS v,
    arbitrary(ev_no_slot) AS ev_no_slot, arbitrary(ev_no_txi) AS ev_no_txi,
    min(t) FILTER (WHERE side = 'buy' AND t <= grad_time + INTERVAL '30' MINUTE) AS t_buy,
    min(k) FILTER (WHERE side = 'sell') AS k_any,
    min_by(t, k) FILTER (WHERE side = 'sell') AS s_any,
    min_by(q, k) FILTER (WHERE side = 'sell') AS q_any,
    min_by(b, k) FILTER (WHERE side = 'sell') AS b_any,
{per_w}
  FROM evh GROUP BY 1, 2),
early AS (   -- early buyers, owner wallets out before anything is counted; the token count rides on every row,
             -- so this block is read once
  SELECT *, count(*) OVER (PARTITION BY mint) AS n_early
  FROM wal WHERE t_buy IS NOT NULL AND w IS NOT NULL AND __NOT_OWNER(w)__),
fund0 AS (   -- every (token, early buyer, sender) in the 7 days before graduation: the only read of this join
  SELECT e.mint, e.w, s.from_owner AS funder,
         {carry}
  FROM tokens_solana.sol_transfers s JOIN early e ON s.to_owner = e.w
  WHERE s.block_time >= TIMESTAMP '{p['st0']} 00:00:00' AND s.block_time < TIMESTAMP '{p['st1']} 00:00:00'
    AND CAST(s.amount AS double) >= 1e6
    AND s.from_owner <> e.w
    AND s.block_time < e.grad_time AND s.block_time >= e.grad_time - INTERVAL '7' DAY
  GROUP BY 1, 2, 3),
fund AS (    -- an owner wallet is no funder either; wk = 1 marks each funded wallet once per token
  SELECT *, row_number() OVER (PARTITION BY mint, w ORDER BY funder) AS wk
  FROM fund0 WHERE __NOT_OWNER(funder)__),
grp AS (     -- one row per (token, funder). No cut of any kind.
  SELECT mint, funder, arbitrary(grad_time) AS grad_time, arbitrary(n_early) AS n_early, arbitrary(v) AS v,
         arbitrary(ev_no_slot) AS ev_no_slot, arbitrary(ev_no_txi) AS ev_no_txi,
         count(*) AS n_buyers, count_if(wk = 1) AS n_first,
         -- the earliest member buys, seconds from graduation: when the group reached any size is read locally
         slice(array_sort(array_agg(date_diff('second', grad_time, t_buy))), 1, {kept}) AS buy_s,
         min_by(date_diff('second', grad_time, s_any), k_any) AS t_first_s,
         min_by(q_any, k_any) AS q_first, min_by(b_any, k_any) AS b_first,
{per_g}
  FROM fund GROUP BY 1, 2),
fan AS (     -- recipients of every sender over the calendar month of the chunk, as recon/gen_fanout.py counts them
  SELECT s.from_owner AS a, approx_distinct(s.to_owner) AS recipients
  FROM tokens_solana.sol_transfers s
  WHERE s.block_time >= TIMESTAMP '{m0} 00:00:00' AND s.block_time < TIMESTAMP '{m1} 00:00:00'
    AND CAST(s.amount AS double) >= 1e6
    AND s.to_owner <> s.from_owner
  GROUP BY 1),
lab AS (SELECT DISTINCT address FROM cex_solana.addresses),
gl AS (      -- the two facts about the funder. No fan-out row in the month is a measured zero, as recon/actors.py reads it
  SELECT g.*, coalesce(f.recipients, 0) AS recipients, (l.address IS NOT NULL) AS labelled
  FROM grp g LEFT JOIN fan f ON f.a = g.funder LEFT JOIN lab l ON l.address = g.funder),
gf AS (      -- counts over ALL groups of the token, taken before anything is dropped; then the running largest group
  SELECT gl.*,
         count(*) OVER (PARTITION BY mint) AS n_groups,
         sum(n_first) OVER (PARTITION BY mint) AS n_funded,
         sum(n_buyers) OVER (PARTITION BY mint) AS n_pairs,
         max(n_buyers) OVER (PARTITION BY mint) AS n_max_any,
         max(n_buyers) OVER (PARTITION BY mint, labelled ORDER BY {order} ROWS UNBOUNDED PRECEDING) AS runmax
  FROM gl),
ranked AS (  -- rn = 1 is the group that set a new running maximum: larger than every group behind a funder with
             -- no more recipients than its own. A column against a window result; no number decides anything.
  SELECT gf.*, row_number() OVER (PARTITION BY mint, labelled, runmax ORDER BY {order}) AS rn
  FROM gf),
kept AS (SELECT * FROM ranked WHERE rn = 1)
SELECT u.mint, u.grad_time, c.funder, c.labelled, c.recipients, {mdays} AS window_days,
       c.n_buyers, c.buy_s, c.n_early, c.n_funded, c.n_groups, c.n_pairs, c.n_max_any,
       c.v AS vqr, c.ev_no_slot, c.ev_no_txi,
       c.t_first_s, c.q_first, c.b_first,
       -- one element per entry lag, 15 / 60 / 240 minutes: the first member sell at or after the lag
       {arr("c.t1_{g}")} AS t1_s,
       {arr("c.q1_{g}")} AS q_t1,
       {arr("c.b1_{g}")} AS b_t1,
       {arr("(c.q1_{g} + c.v) / NULLIF(c.b1_{g}, 0)")} AS px_t1,
       {arr("c.hold_{g}")} AS net_base,
       {arr("c.nsp_{g}")} AS n_sold_pre
FROM u LEFT JOIN kept c ON c.mint = u.mint"""


if __name__ == "__main__":
    out = ROOT / "recon" / "sql"
    for tag, (a, b) in {"day": ("2025-06-09", "2025-06-09"), "week": ("2025-06-09", "2025-06-15"),
                      "pbday": ("2026-09-01", "2026-09-01")}.items():
        (out / f"heavy_b2_auth_{tag}.sql").write_text(auth(a, b), encoding="utf-8")
        (out / f"heavy_b3_cluster_{tag}.sql").write_text(cluster(a, b), encoding="utf-8")
    print("written: heavy_b2_auth_{day,week,pbday}.sql, heavy_b3_cluster_{day,week,pbday}.sql")
