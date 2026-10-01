"""Heavy tier, groups B2 (c01-c04 authority and extension history) and B3 (cluster_* for H2).

  recon/sql/heavy_b2_auth_{day,week}.sql      one row per token; no wallet in the output
  recon/sql/heavy_b3_cluster_{day,week}.sql   one row per (token, common funder) with >= 3 early
                                              buyers; names funders, so run with --private-rows

B2 returns the raw history facts as-of graduation + 240 min, plus the fill counts E35 asks for.
PASS / FAIL / UNKNOWN is assigned locally, never here.

B3: early buyers = wallets with a PumpSwap buy on the graduation pool within 30 minutes of
graduation. Funder = any sender of at least 0.001 SOL to that wallet in the 7 days before
graduation. A (token, funder) row counts the early buyers that funder paid, with the funder's
fan-out over the scanned window, the time of the group's first sell and the price at it.
Which funder kinds may define a cluster (MR-11: an exchange or unknown funder never links) is
decided locally by recon/actors.py.
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
    h, p = universe(gd0, gd1)
    bounds = (f"s.block_time >= TIMESTAMP '{p['st0']} 00:00:00' AND s.block_time < TIMESTAMP '{p['st1']} 00:00:00'\n"
              f"    AND CAST(s.amount AS double) >= 1e6")
    return f"""-- Heavy tier B3: cluster_* inputs (H2), graduations {gd0} .. {gd1}. Rows name funders: --private-rows.
{h},
early0 AS (  -- wallets that bought on the graduation pool within 30 minutes
  SELECT u.mint, u.pool, u.grad_time, b."user" AS w, min(b.evt_block_time) AS t_buy
  FROM pumpdotfun_solana.pump_amm_evt_buyevent b JOIN u ON u.pool = b.pool
  WHERE b.evt_block_date BETWEEN DATE '{gd0}' AND DATE '{p['pool_end']}'
    AND b.evt_block_time >= u.grad_time AND b.evt_block_time <= u.grad_time + INTERVAL '30' MINUTE
  GROUP BY 1, 2, 3, 4),
early AS (SELECT * FROM early0 WHERE __NOT_OWNER(w)__),
fund AS (    -- every sender of SOL to an early buyer in the 7 days before graduation
  SELECT e.mint, e.w, s.from_owner AS funder
  FROM tokens_solana.sol_transfers s JOIN early e ON s.to_owner = e.w
  WHERE {bounds}
    AND s.from_owner <> e.w
    AND s.block_time < e.grad_time AND s.block_time >= e.grad_time - INTERVAL '7' DAY
  GROUP BY 1, 2, 3),
grp AS (SELECT mint, funder, count(*) AS n_buyers FROM fund GROUP BY 1, 2 HAVING count(*) >= 3),
sells AS (   -- first sell by any member of the group, and the price in that event
  SELECT g.mint, g.funder, min(sv.evt_block_time) AS t1,
         min_by(CAST(sv.pool_quote_token_reserves AS double) / CAST(sv.pool_base_token_reserves AS double), sv.evt_block_time) AS px_t1
  FROM grp g JOIN fund f ON f.mint = g.mint AND f.funder = g.funder
  JOIN u ON u.mint = g.mint
  JOIN pumpdotfun_solana.pump_amm_evt_sellevent sv ON sv.pool = u.pool AND sv."user" = f.w
  WHERE sv.evt_block_date BETWEEN DATE '{gd0}' AND DATE '{p['ev1']}' AND sv.evt_block_time >= u.grad_time
  GROUP BY 1, 2),
fan AS (
  SELECT s.from_owner AS a, approx_distinct(s.to_owner) AS fan_out
  FROM tokens_solana.sol_transfers s
  WHERE {bounds}
  GROUP BY 1),
ne AS (SELECT mint, count(*) AS n_early FROM early GROUP BY 1)
SELECT g.mint, g.funder, g.n_buyers, ne.n_early, f.fan_out, sl.t1, sl.px_t1
FROM grp g JOIN ne ON ne.mint = g.mint LEFT JOIN fan f ON f.a = g.funder
LEFT JOIN sells sl ON sl.mint = g.mint AND sl.funder = g.funder"""


if __name__ == "__main__":
    out = ROOT / "recon" / "sql"
    for tag, (a, b) in {"day": ("2025-06-09", "2025-06-09"), "week": ("2025-06-09", "2025-06-15")}.items():
        (out / f"heavy_b2_auth_{tag}.sql").write_text(auth(a, b), encoding="utf-8")
        (out / f"heavy_b3_cluster_{tag}.sql").write_text(cluster(a, b), encoding="utf-8")
    print("written: heavy_b2_auth_{day,week}.sql, heavy_b3_cluster_{day,week}.sql")
