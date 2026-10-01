"""MR-12 ruling 4 / order 2: do unlabelled high-fan-out senders split into hubs and factories?

For every sender that paid 400 or more distinct recipients in the window: take each recipient's
FIRST bonding-curve trade within 24 hours of first being paid, and ask how concentrated those
first trades are on one token. A hub's recipients scatter; a factory's land on the same token.

Output is a histogram only (fan-out bucket x recipients-that-traded bucket x share of them whose
first trade was the sender's most common token), with labelled exchange wallets counted beside
the unlabelled ones as the reference population. No wallet and no class is in the output; the
400 is the provisional line of MR-12, used here to select senders, not to decide anything.

Scope limits, stated: first action = first pump.fun bonding-curve trade (PumpSwap buys are not
scanned); fan-out is measured inside the window.
"""
import pathlib

ROOT = pathlib.Path(__file__).resolve().parents[1]


def build(t0: str, t1: str, d0: str, d1: str) -> str:
    bounds = (f"s.block_time >= TIMESTAMP '{t0} 00:00:00' AND s.block_time < TIMESTAMP '{t1} 00:00:00'\n"
              f"    AND CAST(s.amount AS double) >= 1e6")
    return f"""-- Factory-class measurement (MR-12): first-trade concentration among recipients of high-fan-out senders.
-- SOL transfers {t0} .. {t1} (exclusive), bonding-curve trades {d0} .. {d1}. Histogram only.
WITH fan AS (
  SELECT s.from_owner AS a, approx_distinct(s.to_owner) AS fan_out
  FROM tokens_solana.sol_transfers s
  WHERE {bounds}
  GROUP BY 1 HAVING approx_distinct(s.to_owner) >= 400),
pay AS (
  SELECT s.from_owner AS a, s.to_owner AS r, min(s.block_time) AS t_f, arbitrary(f.fan_out) AS fan_out
  FROM tokens_solana.sol_transfers s JOIN fan f ON f.a = s.from_owner
  WHERE {bounds}
    AND s.to_owner <> s.from_owner AND __NOT_OWNER(s.to_owner)__
  GROUP BY 1, 2),
ft AS (
  SELECT p.a, p.r, arbitrary(p.fan_out) AS fan_out, min_by(t.mint, t.evt_block_time) AS m1
  FROM pay p JOIN pumpdotfun_solana.pump_evt_tradeevent t ON t."user" = p.r
  WHERE t.evt_block_date BETWEEN DATE '{d0}' AND DATE '{d1}'
    AND t.evt_block_time >= p.t_f AND t.evt_block_time < p.t_f + INTERVAL '24' HOUR
  GROUP BY 1, 2),
pm AS (SELECT a, m1, arbitrary(fan_out) AS fan_out, count(*) AS n FROM ft GROUP BY 1, 2),
ps AS (SELECT a, arbitrary(fan_out) AS fan_out, sum(n) AS n_traded, max(n) AS top1, count(*) AS n_mints FROM pm GROUP BY 1),
lab AS (SELECT DISTINCT address FROM cex_solana.addresses)
SELECT CAST(floor(log2(ps.fan_out)) AS integer) AS fan_log2,
       CASE WHEN ps.n_traded < 5 THEN '1-4' WHEN ps.n_traded < 20 THEN '5-19'
            WHEN ps.n_traded < 100 THEN '20-99' ELSE '100+' END AS traded_bucket,
       CAST(floor(10e0 * ps.top1 / ps.n_traded) AS integer) AS top1_share_decile,
       count(*) AS senders, count(l.address) AS labelled_cex,
       sum(ps.n_traded) AS recipients_traded, sum(ps.top1) AS recipients_on_top_token,
       approx_percentile(ps.n_mints, 0.5) AS mints_p50
FROM ps LEFT JOIN lab l ON l.address = ps.a
GROUP BY 1, 2, 3"""


if __name__ == "__main__":
    out = ROOT / "recon" / "sql"
    (out / "factory_overlap_day.sql").write_text(build("2025-06-09", "2025-06-10", "2025-06-09", "2025-06-10"), encoding="utf-8")
    (out / "factory_overlap_week.sql").write_text(build("2025-06-05", "2025-06-17", "2025-06-05", "2025-06-17"), encoding="utf-8")
    print("written: factory_overlap_day.sql, factory_overlap_week.sql")
