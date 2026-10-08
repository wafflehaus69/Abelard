"""Organic takers, v1 and v2, as counts by cell (chunk kind org2; org2_n_7d, org2_t_s, H5's first-hour marker).

Generates recon/sql/org2_{day,week,pbday}.sql; gen_chunks.py calls build() for the monthly files.

One output row per (token, cell). No wallet is in the output. A cell is the set of takers of one token that
share four keys, each of them FIXED for a (token, wallet) pair:

  cls   C = the creator, M = the migration authority, O = anyone else (a CASE with that priority). The
        creator is the one the event query finds (a 70-day lookback from the chunk's FIRST day), so that its
        org1 can be rebuilt exactly; has_creator says whether the creation lies within 70 days before the
        token's OWN graduation day, and only then is the creator set apart locally. That keeps the answer
        the same wherever the token falls in a chunk.
  fsx  the wallet's fee-share code on that token, from the run-time list __FS_CODES__ (decided locally from
        the fee-share export: E in force at graduation + 240 min, L becomes a recipient later within 7 days,
        P only before); NULL = not a recipient
  bn    how many creation-slot traders of the token share this wallet's funder, itself included: b1a's
        bundle_n, role B of gen_heavy_b1's `hit` and nothing else. NULL = the wallet has no such count
        (not a creation-slot trader, or no funder found in the 24 h, or the token is outside s8_scope)
  gs    the wallet swapped in the graduation slot (what the event query's org1 leaves out)

Because every key is fixed per pair, each wallet of a token is in exactly one cell: distinct counts ADD across
cells and first-swap times compose by minimum. So the bundle cut of five (a verdict, MR-14 ruling 5) and the
choice of which fee-share codes exclude are applied locally, in recon/organic.py, never here.

Definitions (what is counted; MR-6 and MR-7, RULINGS_2026-09-22_B / _C)
  taker      the user of any PumpSwap buy or sell on the graduation pool in [grad_time, grad_time + 7 x 24 h),
             as the event query's org1. Owner wallets are removed before any count (MR-3.4).
  n1         takers in the cell: the v1 base.
  n2         takers with at least one swap that is AGED and NOT BOT-SHAPED at that swap (v2 before the local cuts).
  aged       the wallet has a PumpSwap event, on any pool, at least 24 hours before the swap, found in the
             60 calendar days before the swap day. This stands in for "first on-chain signature", which no
             table here carries. A lookback is a per-swap window, so it does not vary with the chunk.
  bot-shaped judged on the 7 calendar days BEFORE the swap day (UTC), never on anything at or after it:
             more than 200 distinct pools traded, or at least half of that week's round trips held under
             60 seconds. A round trip is one (wallet, pool, day) with a buy and a sell where the first sell is
             not before the first buy; its hold is first sell minus first buy (the readiness query's rule,
             taken per day so that a week is a sum of days). No round trip in the week = not judged on hold.
             The H3 sandwich leg is not here: hold time and pool count alone (H3 spec section 3 allows it).
  first hour n1_1h / n2_1h: the same two counts with the swap inside [grad_time, grad_time + 1 h).
  t1_s, t2_s seconds from graduation to the cell's first swap and first qualifying swap.

Two defects of the readiness query (census_organic_v2_week.sql) are not ported: its bot and first-seen windows
were sample-wide and read activity AFTER the swap; here every window ends before the swap and belongs to it.

Effective scans. A WITH block is executed at every reference (85 credits against 14.65, HEAVY_TIER_UNITS.md),
so each block that reads a large table has ONE consumer:
  PumpSwap buy events   2   tk  (graduation pools, gd0 .. gd1+8)  ->  sw
                            wpd (every pool, gd0-60 .. gd1+8)     ->  wd -> wf -> sw
  PumpSwap sell events  2   the same two blocks
  bonding-curve trades  1   slot0 (gd0-3 .. gd1) -> hitb
  SOL transfers         1   hitb (gd0-4 .. gd1)  -> cand
The small decoded tables are read more than once: tok has three consumers (slot0, sw, the final SELECT) and
u is also the subquery of cr and of both halves of tk.

Engine load is the wallet-by-day history, so it is kept as small as the definitions allow: the pool grain is
kept only from 7 days before the chunk's first day (older days are needed for wallet age alone and collapse
to one row per wallet and day); a pool travels as an 8-byte hash, and not at all on a day with more than 200
pools, which settles the week by itself; window functions run over the wallet-day table only; the distinct
count is taken only where the sum of the daily counts can exceed 200; and only the days a swap can fall on
leave the block, as scalars.
"""
import datetime as dt
import pathlib
import re
import sys

sys.path.insert(0, str(pathlib.Path(__file__).parent))
import verdict_constants

ROOT = pathlib.Path(__file__).resolve().parents[1]
WSOL = "So11111111111111111111111111111111111111112"

# The ruled definition of who is counted (MR-7). None of these decides a hypothesis; they are written in the query.
AGE_HOURS = 24          # wallet age at swap time
HOLD_SECONDS = 60       # a round trip held under this is fast
POOLS_MAX = 200         # more than this many distinct pools in the week is bot-shaped
BOT_DAYS = 7            # "the trailing 7 days": the 7 calendar days before the swap day
# Builder's choice, not ruled: how far back a wallet's first PumpSwap event is looked for. 60 days is the
# readiness lookback, the only measured point (43.6 credits for a week, five references). The scan is paid per
# day, but the older days collapse to (wallet, day), so the length costs rows read and little engine memory.
LOOKBACK_DAYS = 60

BUY = "pumpdotfun_solana.pump_amm_evt_buyevent"
SELL = "pumpdotfun_solana.pump_amm_evt_sellevent"
TRADE = "pumpdotfun_solana.pump_evt_tradeevent"
SOL = "tokens_solana.sol_transfers"
DESIGNED = {BUY: 2, SELL: 2, TRADE: 1, SOL: 1}      # textual references in build(); gen_chunks.LARGE must say the same
NO_WALLET = "-- One row per (token, cell); no wallet in the output. The bundle cut and the fee-share rule are applied locally."
WALLETS = "-- Rows name wallets: run with --private-rows. No threshold, no classification, no verdict here."


def _day(d: dt.date, n: int) -> str:
    return (d + dt.timedelta(days=n)).isoformat()


def head(gd0: str, gd1: str, history: bool = True) -> tuple[str, dict]:
    """The taker set with its cell keys: every block up to pk, one row per (token, wallet) with cls, fsx, bn,
    gs, the first swap (t1) and its slot (slot1), and the first qualifying swap (t2).

    Shared, as head() is in gen_heavy_b1: a later query that collapses organic counts by funder must collapse
    exactly this set, and the ratified funder rule counts funding through the slot of first action, which is
    slot1. history=False leaves the wallet-history scan out (t2 and the v2 flags are then typed NULLs): the
    same takers and keys, one reference to each swap-event table instead of two."""
    d0, d1 = dt.date.fromisoformat(gd0), dt.date.fromisoformat(gd1)
    p = dict(gd0=gd0, gd1=gd1,
             pool_end=_day(d1, 1),             # a pool can be created up to a day after the chunk's last completion,
             ev1=_day(d1, 8),                  # so the last graduation is before the end of gd1+1 and its 7 days end on gd1+8
             cr70=_day(d0, -70),               # the event query's creator lookback: the same creator, so the same org1
             cr0=_day(d0, -3),                 # b1a's scope: created within 3 days before the graduation day
             st0=_day(d0, -4),                 # funding of a creation-slot trader: the 24 h before creation
             st1=_day(d1, 1),                  # exclusive; a token of the chunk is never created after gd1
             h0=_day(d0, -LOOKBACK_DAYS),      # the wallet-age lookback of the first day a swap can fall on
             pg0=_day(d0, -BOT_DAYS))          # the bot week of that day: the pool grain is kept from here
    w7 = f"PARTITION BY w ORDER BY d RANGE BETWEEN INTERVAL '{BOT_DAYS}' DAY PRECEDING AND INTERVAL '1' DAY PRECEDING"
    hist = f"""
wpd AS (
  -- Swap-event reference 2, the last: every pool, grouped at once to (wallet, pool, day). No owner filter is
  -- needed here: this block is only ever joined to takers, and tk has already dropped owner wallets.
  -- Before {p['pg0']} the pool is dropped from the key: those days serve wallet age alone, and the bot week of
  -- every swap of this chunk starts on or after that date, so no token's answer depends on the cut.
  SELECT w, pool, d, min(t) AS tf, min(t) FILTER (WHERE side = 'b') AS fb, min(t) FILTER (WHERE side = 's') AS fs
  FROM (
    SELECT "user" AS w, CASE WHEN evt_block_date >= DATE '{p['pg0']}' THEN pool END AS pool, evt_block_date AS d,
           evt_block_time AS t, 'b' AS side
    FROM {BUY}
    WHERE evt_block_date BETWEEN DATE '{p['h0']}' AND DATE '{p['ev1']}'
    UNION ALL
    SELECT "user", CASE WHEN evt_block_date >= DATE '{p['pg0']}' THEN pool END, evt_block_date, evt_block_time, 's'
    FROM {SELL}
    WHERE evt_block_date BETWEEN DATE '{p['h0']}' AND DATE '{p['ev1']}')
  WHERE w IS NOT NULL
  GROUP BY 1, 2, 3),
wd AS (
  -- One row per (wallet, day). A round trip is a pool with a buy and a sell that day, first sell not before
  -- first buy; fast = held under {HOLD_SECONDS} seconds. The day's pools travel as 8-byte hashes (two pools of
  -- one wallet-week colliding on 64 bits is not a case to plan for). A day with more than {POOLS_MAX} pools
  -- settles every week it falls in by its count alone, so its pools do not travel: no array is ever long.
  SELECT w, d, min(tf) AS tf, count(pool) AS np,
         count_if(pool IS NOT NULL AND fs >= fb) AS rt,
         count_if(pool IS NOT NULL AND fs >= fb AND fs < fb + INTERVAL '{HOLD_SECONDS}' SECOND) AS fast,
         CASE WHEN count(pool) > {POOLS_MAX} THEN CAST(ARRAY[] AS array(varbinary))
              ELSE coalesce(array_agg(xxhash64(to_utf8(pool))) FILTER (WHERE pool IS NOT NULL), CAST(ARRAY[] AS array(varbinary))) END AS ph
  FROM wpd
  GROUP BY 1, 2),
wf AS (
  -- The only window functions on the history side, over the wallet-day table. Each frame ends the day BEFORE
  -- the row's day, so nothing at or after a swap is ever read. t_seen = the wallet's earliest event in the
  -- {LOOKBACK_DAYS} days before the day. many = more than {POOLS_MAX} distinct pools in the {BOT_DAYS} days before it:
  -- one day above the line is enough; otherwise the sum of the daily counts is an upper bound, and the
  -- distinct count is taken only where that sum can pass the line (every day's pools are then in its array).
  -- Only the days a swap can fall on leave this block, and only scalars.
  SELECT w, d, t_seen, rt7, fast7, many
  FROM (
    SELECT w, d,
           min(tf) OVER (PARTITION BY w ORDER BY d RANGE BETWEEN INTERVAL '{LOOKBACK_DAYS}' DAY PRECEDING AND INTERVAL '1' DAY PRECEDING) AS t_seen,
           sum(rt) OVER ({w7}) AS rt7,
           sum(fast) OVER ({w7}) AS fast7,
           CASE WHEN max(np) OVER ({w7}) > {POOLS_MAX} THEN true
                WHEN sum(np) OVER ({w7}) > {POOLS_MAX}
                THEN cardinality(array_distinct(flatten(array_agg(ph) OVER ({w7})))) > {POOLS_MAX}
                ELSE false END AS many
    FROM wd)
  WHERE d >= DATE '{gd0}'),"""
    flags = (f"""(h.w IS NULL) AS nohist,
         -- aged: an event at least {AGE_HOURS} hours before THIS swap, inside the lookback
         coalesce(h.t_seen <= e.t - INTERVAL '{AGE_HOURS}' HOUR, false) AS aged,
         coalesce(h.many, false) AS many,
         -- quick: at least half of the week's round trips were fast (median hold under {HOLD_SECONDS} s); none = not judged
         coalesce(h.rt7 > 0 AND h.fast7 + h.fast7 >= h.rt7, false) AS quick
  FROM tk e JOIN tok k ON k.pool = e.pool
  LEFT JOIN wf h ON h.w = e.w AND h.d = e.d""" if history else
             """CAST(NULL AS boolean) AS nohist, CAST(NULL AS boolean) AS aged,
         CAST(NULL AS boolean) AS many, CAST(NULL AS boolean) AS quick
  FROM tk e JOIN tok k ON k.pool = e.pool""")
    sql = f"""WITH comp AS (
  SELECT mint, evt_block_time AS ct
  FROM pumpdotfun_solana.pump_evt_completeevent
  WHERE evt_block_date BETWEEN DATE '{gd0}' AND DATE '{gd1}'),
pc AS (
  SELECT base_mint AS mint, pool, quote_mint, creator AS migrator, evt_block_time AS pt, evt_block_slot AS ps,
         row_number() OVER (PARTITION BY base_mint ORDER BY evt_block_time) AS rn
  FROM pumpdotfun_solana.pump_amm_evt_createpoolevent
  WHERE evt_block_date BETWEEN DATE '{gd0}' AND DATE '{p['pool_end']}'),
u AS (   -- the event query's universe, so rows join on mint and its org1 is the reconciliation target
  SELECT c.mint, p.pool, p.migrator, p.pt AS grad_time, p.ps AS grad_slot
  FROM comp c JOIN pc p ON p.mint = c.mint AND p.rn = 1 AND p.pt >= c.ct AND p.pt < c.ct + INTERVAL '1' DAY
  WHERE p.quote_mint = '{WSOL}'),
cr AS (
  SELECT mint, arbitrary(COALESCE(creator, "user")) AS creator, min(evt_block_time) AS t0, min(evt_block_slot) AS s0
  FROM pumpdotfun_solana.pump_evt_createevent
  WHERE evt_block_date BETWEEN DATE '{p['cr70']}' AND DATE '{gd1}' AND mint IN (SELECT mint FROM u)
  GROUP BY 1),
tok AS (
  -- LEFT JOIN, as the event query: a token created before the 70 days has no creator here and none is set apart.
  -- The two flags are per token, measured from its own graduation day. s8_scope is the aligned-set query's
  -- bound (created within 3 days before it): outside it the same-funder count is not measured, bn is NULL and
  -- the flag says why. cr_own: created within 70 days before it. The lookback above starts at the chunk's
  -- first day, so a late token of a long chunk can have a creator that the same token alone would not.
  SELECT u.mint, u.pool, u.migrator, u.grad_time, u.grad_slot, cr.creator, cr.t0, cr.s0,
         coalesce(cr.t0 >= date_trunc('day', u.grad_time) - INTERVAL '3' DAY, false) AS s8_scope,
         coalesce(cr.t0 >= date_trunc('day', u.grad_time) - INTERVAL '70' DAY, false) AS cr_own
  FROM u LEFT JOIN cr ON cr.mint = u.mint),
slot0 AS (
  -- creation-slot traders, as b1a's buyers0. No is_buy filter: NULL on every 2025 trade event; a
  -- creation-slot seller acquired in that slot
  SELECT DISTINCT k.mint, t."user" AS w, k.t0
  FROM {TRADE} t JOIN tok k ON t.mint = k.mint AND t.evt_block_slot = k.s0
  WHERE t.evt_block_date BETWEEN DATE '{p['cr0']}' AND DATE '{gd1}' AND k.s8_scope),
hitb AS (
  -- The one SOL-transfer reference: senders of at least 0.001 SOL to a creation-slot trader in the 24 h before
  -- creation. Not widened to every taker: that is the join that was cancelled at its cap on b1a.
  SELECT k.mint, k.w, s.from_owner AS funder
  FROM {SOL} s JOIN slot0 k ON k.w = s.to_owner
  WHERE s.block_time >= TIMESTAMP '{p['st0']} 00:00:00' AND s.block_time < TIMESTAMP '{p['st1']} 00:00:00'
    AND CAST(s.amount AS double) >= 1e6
    AND s.block_time BETWEEN k.t0 - INTERVAL '24' HOUR AND k.t0
  GROUP BY 1, 2, 3),
cand AS (
  -- bn = creation-slot traders sharing this wallet's funder, itself included; the largest over its funders.
  -- Whether that makes a bundle is decided locally (MR-14 ruling 5).
  SELECT mint, w, max(nsf) AS bn
  FROM (SELECT mint, w, count(*) OVER (PARTITION BY mint, funder) AS nsf FROM hitb)
  GROUP BY 1, 2),
fsc AS (
  -- Fee-share recipients come as a run-time list, one code per (token, wallet). Should a list ever carry two,
  -- the earlier letter is kept, so a duplicate can never count a taker twice.
  SELECT mint, w, min(code) AS fsx
  FROM (VALUES __FS_CODES__) AS t(mint, w, code)
  WHERE mint IS NOT NULL
  GROUP BY 1, 2),
tk AS (
  -- Swap-event reference 1: every swap on a graduation pool, buy or sell. Owner wallets leave here (MR-3.4),
  -- before any count; the event query does not drop them, so its org1 can be higher on a token the owner traded.
  SELECT pool, w, d, t, slot
  FROM (
    SELECT pool, "user" AS w, evt_block_date AS d, evt_block_time AS t, evt_block_slot AS slot
    FROM {BUY}
    WHERE evt_block_date BETWEEN DATE '{gd0}' AND DATE '{p['ev1']}' AND pool IN (SELECT pool FROM u)
    UNION ALL
    SELECT pool, "user", evt_block_date, evt_block_time, evt_block_slot
    FROM {SELL}
    WHERE evt_block_date BETWEEN DATE '{gd0}' AND DATE '{p['ev1']}' AND pool IN (SELECT pool FROM u))
  WHERE w IS NOT NULL AND __NOT_OWNER(w)__),{hist if history else ''}
sw AS (
  -- one row per swap in the token's own 7 x 24 h; the flags are those of this swap's wallet on this swap's day
  SELECT k.mint, e.w, e.t, e.slot, k.grad_time,
         CASE WHEN e.w = k.creator THEN 'C' WHEN e.w = k.migrator THEN 'M' ELSE 'O' END AS cls,
         (e.slot = k.grad_slot) AS gs,
         {flags}
  WHERE e.t >= k.grad_time AND e.t < k.grad_time + INTERVAL '7' DAY),
pw AS (
  -- one row per (token, wallet). t2 = its first swap that is aged and not bot-shaped at that swap
  SELECT mint, w, cls, grad_time, bool_or(gs) AS gs, min(t) AS t1, min(slot) AS slot1,
         min(t) FILTER (WHERE aged AND NOT many AND NOT quick) AS t2,
         bool_or(aged) AS any_aged, bool_or(NOT many AND NOT quick) AS any_notbot,
         bool_or(many) AS any_many, bool_or(quick) AS any_quick, bool_or(nohist) AS any_nohist
  FROM sw
  GROUP BY 1, 2, 3, 4),
pk AS (
  SELECT p.mint, p.w, p.cls, f.fsx, c.bn, p.gs, p.grad_time, p.t1, p.slot1, p.t2,
         p.any_aged, p.any_notbot, p.any_many, p.any_quick, p.any_nohist
  FROM pw p
  LEFT JOIN fsc f ON f.mint = p.mint AND f.w = p.w
  LEFT JOIN cand c ON c.mint = p.mint AND c.w = p.w)"""
    return sql, p


def build(gd0: str, gd1: str) -> str:
    """The chunk query: one row per (token, cell), and one row with a NULL cell for a token with no taker,
    so that 'no taker' is a row and not an absence."""
    h, _p = head(gd0, gd1)
    return f"""-- Organic takers v1 and v2 by cell (org2_n_7d, org2_t_s, first-hour marker), graduations {gd0} .. {gd1}.
{NO_WALLET}
{h},
cell AS (
  SELECT mint, cls, fsx, bn, gs,
         count(*) AS n1, count(t2) AS n2,
         count_if(t1 < grad_time + INTERVAL '1' HOUR) AS n1_1h, count_if(t2 < grad_time + INTERVAL '1' HOUR) AS n2_1h,
         min(t1) AS t1, min(t2) AS t2,
         count_if(any_aged) AS n_aged, count_if(any_notbot) AS n_notbot,
         count_if(any_many) AS n_many, count_if(any_quick) AS n_quick, count_if(any_nohist) AS n_nohist
  FROM pk
  GROUP BY 1, 2, 3, 4, 5)
SELECT k.mint, c.cls, c.fsx, c.bn, c.gs,
       coalesce(c.n1, 0) AS n1, coalesce(c.n2, 0) AS n2, coalesce(c.n1_1h, 0) AS n1_1h, coalesce(c.n2_1h, 0) AS n2_1h,
       date_diff('second', k.grad_time, c.t1) AS t1_s, date_diff('second', k.grad_time, c.t2) AS t2_s,
       -- why v1 and v2 differ: takers with an aged swap, with a swap on a day they were not bot-shaped, and
       -- with a swap on a day each bot leg held. n_nohist is a check: a swap with no history row (must be 0)
       coalesce(c.n_aged, 0) AS n_aged, coalesce(c.n_notbot, 0) AS n_notbot,
       coalesce(c.n_many, 0) AS n_many, coalesce(c.n_quick, 0) AS n_quick, coalesce(c.n_nohist, 0) AS n_nohist,
       k.s8_scope, k.cr_own AS has_creator, {LOOKBACK_DAYS} AS lb_days, __FS_SUPPLIED__ AS fs_supplied
FROM tok k LEFT JOIN cell c ON c.mint = k.mint"""


def bundle_takers(gd0: str, gd1: str) -> str:
    """Proving aid, not a chunk kind and not written to disk here: the takers that carry a same-funder count,
    by name, so that bn can be held against the aligned-set member rows of the same day wallet by wallet
    (organic.bundle_takers_check). No history scan."""
    h, _p = head(gd0, gd1, history=False)
    return f"""-- Organic takers with a creation-slot same-funder count (proving aid), graduations {gd0} .. {gd1}.
{WALLETS}
{h}
SELECT mint, w, cls, fsx, bn, gs, slot1, __FS_SUPPLIED__ AS fs_supplied
FROM pk
WHERE bn IS NOT NULL"""


def _refs(sql: str, name: str) -> int:
    return len(re.findall(r"\b(?:FROM|JOIN)\s+" + re.escape(name) + r"\b", sql))


def self_check(sql: str, gd0: str, gd1: str, designed: dict | None = None) -> list[str]:
    """What can be checked by reading the text: the house rules gen_chunks.review enforces, in the spelling it
    asks for, plus that every block on the path of a large table has exactly one consumer."""
    designed = DESIGNED if designed is None else designed
    out = [f"verdict constant: line {no}: {text}" for _n, no, text in verdict_constants.hits(sql)]
    for table, n in designed.items():
        found = [m.end() for m in re.finditer(re.escape(table) + r"\b", sql)]
        if len(found) != n:
            out.append(f"{table}: {len(found)} references, designed {n}")
        for end in found:
            # stricter than the review's 900 characters, which a neighbouring table's bound can satisfy:
            # the bound must stand in this table's own WHERE, before any other FROM
            tail = sql[end:end + 900].split("FROM ")[0]
            ok = (re.search(r"s\.block_time >= TIMESTAMP '\d{4}-\d\d-\d\d 00:00:00' AND s\.block_time < TIMESTAMP '\d{4}-\d\d-\d\d 00:00:00'", tail)
                  if table == SOL else re.search(r"evt_block_date BETWEEN DATE '\d{4}-\d\d-\d\d' AND DATE '\d{4}-\d\d-\d\d'", tail))
            if not ok:
                out.append(f"{table}: no literal bound in the required spelling after the reference at {end}")
    blocks = set(re.findall(r"^(\w+) AS \(", sql.replace("WITH ", ""), flags=re.M))
    for name in ("tk", "wpd", "wd", "wf", "slot0", "hitb", "cand", "fsc", "sw", "pw", "pk", "cell"):
        if name in blocks and _refs(sql, name) != 1:
            out.append(f"{name}: {_refs(sql, name)} consumers, designed 1")
    if f"DATE '{gd0}'" not in sql or f"DATE '{gd1}'" not in sql:
        out.append("chunk bounds not found in the query text")
    if "__NOT_OWNER(w)__" not in sql:
        out.append("no owner filter on the taker wallet")
    if sql.count("__FS_CODES__") != 1 or sql.count("__FS_SUPPLIED__") != 1:
        out.append("fee-share placeholders: each must appear exactly once")
    if re.search(r"\b(CREATE|INSERT|DELETE|UPDATE)\b", sql):
        out.append("statement other than SELECT")
    if re.search(r"block_date[^\n]*\bOR\b|\bOR\b[^\n]*block_date", sql):
        out.append("OR on a partition column")
    if "{" in sql or "}" in sql:
        out.append("an unfilled brace")
    code = verdict_constants.strip(sql)
    if code.count("(") != code.count(")"):
        out.append(f"parentheses do not balance: {code.count('(')} opened, {code.count(')')} closed")
    if sql.split("\n")[1] not in (NO_WALLET, WALLETS):
        out.append("line 2 does not say whether rows name wallets")
    return out


if __name__ == "__main__":
    out = ROOT / "recon" / "sql"
    for tag, (a, b) in {"day": ("2025-06-09", "2025-06-09"), "week": ("2025-06-09", "2025-06-15"),
                      "pbday": ("2026-09-01", "2026-09-01")}.items():
        sql = build(a, b)
        bad = self_check(sql, a, b)
        if bad:
            raise SystemExit(f"org2_{tag}.sql not written: " + "; ".join(bad))
        (out / f"org2_{tag}.sql").write_text(sql, encoding="utf-8")
    print("written: org2_{day,week,pbday}.sql")
