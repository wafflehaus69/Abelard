"""Heavy tier, group B1: aligned-set members with funding records, and holder concentration.

Generates, for one graduation day and for the calibration week:
  recon/sql/heavy_b1a_members_{day,week}.sql   one row per aligned-set member that matters
  recon/sql/heavy_b1b_c06_{day,week}.sql       one row per token: c06 top-10 share, supply at entry

B1a is the input to c07, c08, seta_* and to the CONSENSUS crossover items (MR-11): each
member's funder, the funder's fan-out, the time from funding to first acquisition, and the
creator's funder for block_id. Classification and collapse are NOT done here: they are pure
functions applied locally (recon/actors.py), so no threshold is in any query.

B1a output rows name trader wallets. The runner writes them to barrel/private/ (--private-rows),
never to the repo.

Candidates are the creator, wallets the creator funded, and creation-slot traders with their
same-funder count; whether that count makes a bundle is decided locally (MR-14 ruling 5).
A member "matters" when it is the creator or it ever held the token. Recipients of creator SOL
that never touch the token (fee accounts, tip accounts, rent for new accounts) are counted in
n_all and otherwise dropped; the owner that received the initial mint (the bonding curve) is
dropped. Without this every token shows a "funded" set of four or more (a3_seta_day, 2026-10-01).

Funder of a member = sender of the last SOL transfer of at least 0.001 SOL received strictly
before the member's first action; the latency runs from that funder's FIRST transfer (MR-12.2) (creation time for the creator, first acquisition of the token
for everyone else), inside the scanned window. No such transfer = funding unknown.
"""
import datetime as dt
import pathlib

ROOT = pathlib.Path(__file__).resolve().parents[1]
WSOL = "So11111111111111111111111111111111111111112"


def head(gd0: str, gd1: str) -> tuple[str, dict]:
    d0, d1 = dt.date.fromisoformat(gd0), dt.date.fromisoformat(gd1)
    p = dict(gd0=gd0, gd1=gd1,
             cr0=(d0 - dt.timedelta(days=3)).isoformat(),
             st0=(d0 - dt.timedelta(days=4)).isoformat(),
             st1=(d1 + dt.timedelta(days=2)).isoformat(),
             tr1=(d1 + dt.timedelta(days=9)).isoformat(),
             pool_end=(d1 + dt.timedelta(days=1)).isoformat())
    sql = f"""WITH comp AS (
  SELECT mint, evt_block_time AS ct FROM pumpdotfun_solana.pump_evt_completeevent
  WHERE evt_block_date BETWEEN DATE '{p['gd0']}' AND DATE '{p['gd1']}'),
pc AS (
  SELECT base_mint AS mint, quote_mint, pool, evt_block_time AS pt,
         row_number() OVER (PARTITION BY base_mint ORDER BY evt_block_time) AS rn
  FROM pumpdotfun_solana.pump_amm_evt_createpoolevent
  WHERE evt_block_date BETWEEN DATE '{p['gd0']}' AND DATE '{p['pool_end']}'),
u AS (
  SELECT c.mint, p.pt AS grad_time, p.pool
  FROM comp c JOIN pc p ON p.mint = c.mint AND p.rn = 1 AND p.pt >= c.ct AND p.pt < c.ct + INTERVAL '1' DAY
  WHERE p.quote_mint = '{WSOL}'),
cr AS (
  SELECT mint, arbitrary(COALESCE(creator, "user")) AS creator, min(evt_block_time) AS t0, min(evt_block_slot) AS s0
  FROM pumpdotfun_solana.pump_evt_createevent
  WHERE evt_block_date BETWEEN DATE '{p['cr0']}' AND DATE '{p['gd1']}' AND mint IN (SELECT mint FROM u)
  GROUP BY 1),
base AS (SELECT u.mint, u.grad_time, u.pool, cr.creator, cr.t0, cr.s0 FROM u JOIN cr ON cr.mint = u.mint),
bal AS (   -- token ledger, one pass: every owner's balance at each entry lag, plus chain supply
  SELECT t.token_mint_address AS mint, x.w,
    sum(x.d) FILTER (WHERE t.block_time <= b.grad_time + INTERVAL '15' MINUTE) AS b15,
    sum(x.d) FILTER (WHERE t.block_time <= b.grad_time + INTERVAL '60' MINUTE) AS b60,
    sum(x.d) FILTER (WHERE t.block_time <= b.grad_time + INTERVAL '240' MINUTE) AS b240,
    sum(x.d) FILTER (WHERE t.block_time > b.grad_time + INTERVAL '240' MINUTE AND t.block_time < b.grad_time + INTERVAL '8' DAY) AS net_after,
    sum(x.d) FILTER (WHERE t.block_time >= b.grad_time + INTERVAL '0' DAY AND t.block_time < b.grad_time + INTERVAL '1' DAY) AS f0,
    sum(x.d) FILTER (WHERE t.block_time >= b.grad_time + INTERVAL '1' DAY AND t.block_time < b.grad_time + INTERVAL '2' DAY) AS f1,
    sum(x.d) FILTER (WHERE t.block_time >= b.grad_time + INTERVAL '2' DAY AND t.block_time < b.grad_time + INTERVAL '3' DAY) AS f2,
    sum(x.d) FILTER (WHERE t.block_time >= b.grad_time + INTERVAL '3' DAY AND t.block_time < b.grad_time + INTERVAL '4' DAY) AS f3,
    sum(x.d) FILTER (WHERE t.block_time >= b.grad_time + INTERVAL '4' DAY AND t.block_time < b.grad_time + INTERVAL '5' DAY) AS f4,
    sum(x.d) FILTER (WHERE t.block_time >= b.grad_time + INTERVAL '5' DAY AND t.block_time < b.grad_time + INTERVAL '6' DAY) AS f5,
    sum(x.d) FILTER (WHERE t.block_time >= b.grad_time + INTERVAL '6' DAY AND t.block_time < b.grad_time + INTERVAL '7' DAY) AS f6,
    sum(x.d) FILTER (WHERE t.block_time >= b.grad_time + INTERVAL '7' DAY AND t.block_time < b.grad_time + INTERVAL '8' DAY) AS f7,
    min(t.block_time) FILTER (WHERE x.d > 0) AS first_in,
    bool_or(x.tag = 'to' AND t.from_owner IS NULL) AS got_mint
  FROM tokens_solana.transfers t
  JOIN base b ON b.mint = t.token_mint_address
  CROSS JOIN UNNEST(
    ARRAY[t.to_owner, t.from_owner, '#SUPPLY'],
    ARRAY[CAST(t.amount AS double), -CAST(t.amount AS double),
          CASE WHEN t.from_owner IS NULL THEN CAST(t.amount AS double)
               WHEN t.to_owner IS NULL THEN -CAST(t.amount AS double) ELSE 0e0 END],
    ARRAY['to', 'from', 'sup']) AS x(w, d, tag)
  WHERE t.block_date BETWEEN DATE '{p['cr0']}' AND DATE '{p['tr1']}'
    AND t.token_mint_address IN (SELECT mint FROM base) AND x.w IS NOT NULL
  GROUP BY 1, 2)"""
    return sql, p


def members(gd0: str, gd1: str) -> str:
    h, p = head(gd0, gd1)
    bounds = (f"s.block_time >= TIMESTAMP '{p['st0']} 00:00:00' AND s.block_time < TIMESTAMP '{p['st1']} 00:00:00'\n"
              f"    AND CAST(s.amount AS double) >= 1e6")
    return f"""-- Heavy tier B1a: aligned-set members with funding records, graduations {gd0} .. {gd1}.
-- Rows name wallets: run with --private-rows. No threshold, no classification, no verdict here.
{h},
buyers0 AS (
  -- no is_buy filter: NULL on every 2025 trade event; a creation-slot seller acquired in that slot
  SELECT DISTINCT b.mint, t.user AS w, b.t0
  FROM pumpdotfun_solana.pump_evt_tradeevent t JOIN base b ON t.mint = b.mint AND t.evt_block_slot = b.s0
  WHERE t.evt_block_date BETWEEN DATE '{p['cr0']}' AND DATE '{p['gd1']}'),
keys AS (
  SELECT mint, creator AS k, 'F' AS role, t0 FROM base
  UNION ALL
  SELECT mint, w, 'B', t0 FROM buyers0),
hit AS (
  SELECT k.mint, k.role,
         CASE k.role WHEN 'F' THEN s.to_owner ELSE k.k END AS w,
         CASE k.role WHEN 'B' THEN s.from_owner END AS funder,
         sum(CAST(s.amount AS double)) / 1e9 AS sol
  FROM tokens_solana.sol_transfers s
  CROSS JOIN UNNEST(ARRAY[s.from_owner, s.to_owner], ARRAY['F', 'B']) AS x(addr, role)
  JOIN keys k ON k.k = x.addr AND k.role = x.role
  WHERE {bounds}
    AND ((k.role = 'F' AND s.block_time BETWEEN k.t0 - INTERVAL '24' HOUR AND k.t0 + INTERVAL '24' HOUR)
      OR (k.role = 'B' AND s.block_time BETWEEN k.t0 - INTERVAL '24' HOUR AND k.t0))
  GROUP BY 1, 2, 3, 4),
memb AS (
  SELECT mint, role, w, sol, count(*) OVER (PARTITION BY mint, role, funder) AS n_same_funder FROM hit),
seta AS (
  -- bundle_n = how many creation-slot traders share this wallet's funder (itself included).
  -- Whether that makes a bundle is decided locally (MR-14 ruling 5); every candidate is returned.
  SELECT mint, w, bool_or(src = 'C') AS is_creator, bool_or(src = 'F') AS is_funded,
         max(nsf) FILTER (WHERE src = 'B') AS bundle_n,
         sum(sol) FILTER (WHERE src = 'F') AS cr_sol
  FROM (SELECT mint, creator AS w, 'C' AS src, CAST(NULL AS double) AS sol, CAST(NULL AS bigint) AS nsf FROM base
        UNION ALL
        SELECT mint, w, role, sol, n_same_funder FROM memb)
  WHERE w IS NOT NULL AND __NOT_OWNER(w)__
  GROUP BY 1, 2),
mm0 AS (
  SELECT s.mint, s.w, s.is_creator, s.is_funded, s.bundle_n, s.cr_sol,
         bs.grad_time, bs.t0, bs.creator, b.first_in, b.b15, b.b60, b.b240, b.net_after, b.got_mint,
         b.f0, b.f1, b.f2, b.f3, b.f4, b.f5, b.f6, b.f7,
         CASE WHEN s.is_creator THEN bs.t0 ELSE b.first_in END AS t_act,
         count(*) OVER (PARTITION BY s.mint) AS n_all,
         count_if(s.is_funded) OVER (PARTITION BY s.mint) AS n_funded_all,
         max(s.bundle_n) OVER (PARTITION BY s.mint) AS bundle_n_max
  FROM seta s JOIN base bs ON bs.mint = s.mint
  LEFT JOIN bal b ON b.mint = s.mint AND b.w = s.w),
mm AS (SELECT * FROM mm0 WHERE (is_creator OR first_in IS NOT NULL) AND NOT coalesce(got_mint, false)),
inb0 AS (   -- one row per (member, sender): first and last transfer before the member's first action
  SELECT m.mint, m.w, s.from_owner AS sender, min(s.block_time) AS t_first, max(s.block_slot) AS slot_last,
         max_by(CAST(s.amount AS double), s.block_slot) / 1e9 AS last_sol
  FROM tokens_solana.sol_transfers s JOIN mm m ON s.to_owner = m.w
  WHERE {bounds}
    AND s.from_owner <> m.w AND s.block_time < m.t_act
  GROUP BY 1, 2, 3),
inb AS (    -- funder = the last sender before the first action; funded_ts = that funder's FIRST transfer (MR-12.2).
            -- "Last" is by slot, then by address: block_time is whole seconds and two senders can share one
            -- (found on 2025-06-09: two transfers in the same second, one slot apart, gave two different funders).
  SELECT mint, w, sender AS funder, t_first AS funded_ts, last_sol AS funded_sol, n_senders
  FROM (SELECT *, row_number() OVER (PARTITION BY mint, w ORDER BY slot_last DESC, sender DESC) AS rn,
               count(*) OVER (PARTITION BY mint, w) AS n_senders
        FROM inb0)
  WHERE rn = 1),
fan AS (
  SELECT s.from_owner AS a, approx_distinct(s.to_owner) AS fan_out, count(*) AS n_out
  FROM tokens_solana.sol_transfers s
  WHERE {bounds}
  GROUP BY 1)
SELECT m.mint, m.w, m.is_creator, m.is_funded, m.bundle_n, m.cr_sol, m.n_all, m.n_funded_all, m.bundle_n_max,
       m.grad_time, m.t0, m.first_in, m.b15, m.b60, m.b240, m.net_after,
       i.funder, i.funded_ts, i.funded_sol, i.n_senders, f.fan_out, f.n_out,
       date_diff('second', i.funded_ts, m.first_in) AS fund_to_first_buy_s,
       sup.b15 AS supply15, sup.b60 AS supply60, sup.b240 AS supply240
FROM mm m
LEFT JOIN inb i ON i.mint = m.mint AND i.w = m.w
LEFT JOIN fan f ON f.a = i.funder
LEFT JOIN bal sup ON sup.mint = m.mint AND sup.w = '#SUPPLY'"""


def members_grouped(gd0: str, gd1: str) -> str:
    """Build form of B1a: one row per (token, funder) instead of one per member, so a set of
    thousands of wallets behind one funder is one row. Everything the local collapse, the block
    and the registry need survives the grouping: members per funder, the funder's fan-out, summed
    balances and flows, and whether the creator is in the group. Members with no funder found
    are the group with funder NULL. NEW PATTERN: not yet run; one-day scope first."""
    full = members(gd0, gd1)
    fsum = ", ".join(f"sum(m.f{k}) AS f{k}" for k in range(8))
    cut = full.index("SELECT m.mint, m.w, m.is_creator")
    return (full[:cut].replace("-- Heavy tier B1a: aligned-set members with funding records",
                               "-- Heavy tier B1a (build form): aligned-set members grouped by funder")
            + f"""SELECT m.mint, i.funder,
       -- b_only_n: NULL for the creator and wallets the creator funded (always in the set); for a
       -- wallet that is a candidate only as a creation-slot trader, its same-funder count
       CASE WHEN m.is_creator OR m.is_funded THEN NULL ELSE m.bundle_n END AS b_only_n,
       arbitrary(m.creator) AS creator,
       count(*) AS n_members, count_if(m.is_creator) AS n_creator, count_if(m.is_funded) AS n_funded,
       max(m.n_all) AS n_all, max(m.n_funded_all) AS n_funded_all, max(m.bundle_n_max) AS bundle_n_max,
       max(f.fan_out) AS fan_out, max(i.n_senders) AS n_senders_max,
       sum(m.b15) AS b15, sum(m.b60) AS b60, sum(m.b240) AS b240, sum(m.net_after) AS net_after,
       {fsum},
       approx_percentile(date_diff('second', i.funded_ts, m.first_in), 0.5) AS lat_p50,
       count(i.funded_ts) FILTER (WHERE m.first_in IS NOT NULL) AS n_lat,
       min(m.first_in) AS first_in_min, arbitrary(m.grad_time) AS grad_time, arbitrary(m.t0) AS t0,
       arbitrary(sup.b15) AS supply15, arbitrary(sup.b60) AS supply60, arbitrary(sup.b240) AS supply240
FROM mm m
LEFT JOIN inb i ON i.mint = m.mint AND i.w = m.w
LEFT JOIN fan f ON f.a = i.funder
LEFT JOIN bal sup ON sup.mint = m.mint AND sup.w = '#SUPPLY'
GROUP BY 1, 2, 3""")


def c06(gd0: str, gd1: str) -> str:
    h, p = head(gd0, gd1)
    return f"""-- Heavy tier B1b: c06 (top-10 owners ex-pool, ex-curve, ex-burn, share of chain supply at entry),
-- graduations {gd0} .. {gd1}. Token ledger only. One row per token; no wallet in the output.
{h},
rk AS (
  SELECT bl.mint, bl.w, bl.b15, bl.b60, bl.b240,
         (bl.w = '#SUPPLY') AS is_sup,
         (bl.w = b.pool OR coalesce(bl.got_mint, false)) AS excluded,
         row_number() OVER (PARTITION BY bl.mint, (bl.w = '#SUPPLY' OR bl.w = b.pool OR coalesce(bl.got_mint, false)) ORDER BY bl.b15 DESC NULLS LAST) AS r15,
         row_number() OVER (PARTITION BY bl.mint, (bl.w = '#SUPPLY' OR bl.w = b.pool OR coalesce(bl.got_mint, false)) ORDER BY bl.b60 DESC NULLS LAST) AS r60,
         row_number() OVER (PARTITION BY bl.mint, (bl.w = '#SUPPLY' OR bl.w = b.pool OR coalesce(bl.got_mint, false)) ORDER BY bl.b240 DESC NULLS LAST) AS r240
  FROM bal bl JOIN base b ON b.mint = bl.mint)
SELECT mint,
       max(b15) FILTER (WHERE is_sup) AS supply15, max(b60) FILTER (WHERE is_sup) AS supply60, max(b240) FILTER (WHERE is_sup) AS supply240,
       sum(b15) FILTER (WHERE NOT is_sup AND NOT excluded AND r15 <= 10 AND b15 > 0) AS top10_15,
       sum(b60) FILTER (WHERE NOT is_sup AND NOT excluded AND r60 <= 10 AND b60 > 0) AS top10_60,
       sum(b240) FILTER (WHERE NOT is_sup AND NOT excluded AND r240 <= 10 AND b240 > 0) AS top10_240,
       count_if(NOT is_sup AND NOT excluded AND b240 > 0) AS holders240,
       count_if(excluded) AS n_excluded_owners,
       count(b15) AS n_owner_rows_filled
FROM rk GROUP BY 1"""


if __name__ == "__main__":
    out = ROOT / "recon" / "sql"
    for tag, (a, b) in {"day": ("2025-06-09", "2025-06-09"), "week": ("2025-06-09", "2025-06-15"),
                      "pbday": ("2026-09-01", "2026-09-01")}.items():
        (out / f"heavy_b1a_members_{tag}.sql").write_text(members(a, b), encoding="utf-8")
        (out / f"heavy_b1b_c06_{tag}.sql").write_text(c06(a, b), encoding="utf-8")
        (out / f"heavy_b1a_grouped_{tag}.sql").write_text(members_grouped(a, b), encoding="utf-8")
    print("written: heavy_b1a_members_{day,week}.sql, heavy_b1b_c06_{day,week}.sql")
