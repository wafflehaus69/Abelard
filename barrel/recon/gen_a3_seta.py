"""Burn-down item 4a: aligned-set net flow, days 0-7 (the RUG-A input), per token.

Generates recon/sql/a3_seta_day.sql and a3_seta_week.sql. Distributions only; no threshold.
Unsaved ad-hoc queries (never a saved query: the logic stays out of Dune).

Aligned set, as buildable today:
  creator
  + wallets the creator sent SOL to within +/-24h of creation        (S7, 1-hop)
  + creation-slot buyers on the bonding curve, when >= 5 of them share one funder
    in the 24h before creation                                        (S8 bundle)
  (fee-share recipients, S7b, are NOT included: extraction is a paid-month item)

Holdings and flows come from the token transfer ledger, so bonding-curve purchases
before graduation are counted:
  hold_entry = net balance of the set at graduation + 240 min
  f0..f7     = net token flow of the set in day k after graduation (sells negative)
  sf_7d      = net amount the set shed between entry and graduation + 8d, over hold_entry

Scope limit: creations within 3 days before graduation (95.6% of the sample week).
Tokens created earlier are returned with seta_n NULL and reason 'created_outside_window'.
"""
import datetime as dt
import pathlib

ROOT = pathlib.Path(__file__).resolve().parents[1]
WSOL = "So11111111111111111111111111111111111111112"


def build(gd0: str, gd1: str) -> str:
    d0, d1 = dt.date.fromisoformat(gd0), dt.date.fromisoformat(gd1)
    cr0 = (d0 - dt.timedelta(days=3)).isoformat()
    st0 = (d0 - dt.timedelta(days=4)).isoformat()
    st1 = (d1 + dt.timedelta(days=2)).isoformat()
    tr1 = (d1 + dt.timedelta(days=9)).isoformat()
    pool_end = (d1 + dt.timedelta(days=1)).isoformat()
    days = ",\n".join(
        f"    sum(d) FILTER (WHERE bt >= grad_time + INTERVAL '{k}' DAY AND bt < grad_time + INTERVAL '{k + 1}' DAY) AS f{k}"
        for k in range(8))
    return f"""-- A3 / RUG-A input: aligned-set net flow days 0-7, stratum P, graduations {gd0} .. {gd1}.
WITH comp AS (
  SELECT mint, evt_block_time AS ct FROM pumpdotfun_solana.pump_evt_completeevent
  WHERE evt_block_date BETWEEN DATE '{gd0}' AND DATE '{gd1}'),
pc AS (
  SELECT base_mint AS mint, quote_mint, evt_block_time AS pt,
         row_number() OVER (PARTITION BY base_mint ORDER BY evt_block_time) AS rn
  FROM pumpdotfun_solana.pump_amm_evt_createpoolevent
  WHERE evt_block_date BETWEEN DATE '{gd0}' AND DATE '{pool_end}'),
u AS (
  SELECT c.mint, p.pt AS grad_time
  FROM comp c JOIN pc p ON p.mint = c.mint AND p.rn = 1 AND p.pt >= c.ct AND p.pt < c.ct + INTERVAL '1' DAY
  WHERE p.quote_mint = '{WSOL}'),
cr AS (
  SELECT mint, arbitrary(COALESCE(creator, "user")) AS creator, min(evt_block_time) AS t0, min(evt_block_slot) AS s0
  FROM pumpdotfun_solana.pump_evt_createevent
  WHERE evt_block_date BETWEEN DATE '{cr0}' AND DATE '{gd1}' AND mint IN (SELECT mint FROM u)
  GROUP BY 1),
base AS (SELECT u.mint, u.grad_time, cr.creator, cr.t0, cr.s0 FROM u JOIN cr ON cr.mint = u.mint),
st AS (   -- literal bounds on block_time: partition pruning (rule from the S8 watchdog event)
  SELECT from_owner, to_owner, block_time FROM tokens_solana.sol_transfers
  WHERE block_time >= TIMESTAMP '{st0} 00:00:00' AND block_time < TIMESTAMP '{st1} 00:00:00'
    AND CAST(amount AS double) >= 1e6),
funded AS (
  SELECT DISTINCT b.mint, s.to_owner AS w
  FROM base b JOIN st s ON s.from_owner = b.creator
  WHERE s.block_time BETWEEN b.t0 - INTERVAL '24' HOUR AND b.t0 + INTERVAL '24' HOUR),
buyers0 AS (
  SELECT DISTINCT b.mint, t.user AS w
  FROM pumpdotfun_solana.pump_evt_tradeevent t JOIN base b ON t.mint = b.mint AND t.evt_block_slot = b.s0
  WHERE t.evt_block_date BETWEEN DATE '{cr0}' AND DATE '{gd1}' AND t.is_buy),
bf AS (
  SELECT x.mint, x.w, s.from_owner AS funder
  FROM buyers0 x JOIN base b ON b.mint = x.mint JOIN st s ON s.to_owner = x.w
  WHERE s.block_time BETWEEN b.t0 - INTERVAL '24' HOUR AND b.t0),
bigf AS (SELECT mint, funder FROM bf GROUP BY 1, 2 HAVING count(DISTINCT w) >= 5),
bundle AS (SELECT DISTINCT bf.mint, bf.w FROM bf JOIN bigf ON bigf.mint = bf.mint AND bigf.funder = bf.funder),
seta AS (
  SELECT mint, creator AS w FROM base
  UNION SELECT mint, w FROM funded
  UNION SELECT mint, w FROM bundle),
tr AS (
  SELECT token_mint_address AS mint, from_owner, to_owner, CAST(amount AS double) AS amt, block_time AS bt
  FROM tokens_solana.transfers
  WHERE block_date BETWEEN DATE '{cr0}' AND DATE '{tr1}' AND token_mint_address IN (SELECT mint FROM base)),
flows AS (
  SELECT mint, to_owner AS w, amt AS d, bt FROM tr WHERE to_owner IS NOT NULL
  UNION ALL
  SELECT mint, from_owner, -amt, bt FROM tr WHERE from_owner IS NOT NULL),
af AS (
  SELECT f.mint, f.d, f.bt, b.grad_time
  FROM flows f JOIN seta s ON s.mint = f.mint AND s.w = f.w JOIN base b ON b.mint = f.mint),
agg AS (
  SELECT mint,
    sum(d) FILTER (WHERE bt <= grad_time + INTERVAL '240' MINUTE) AS hold_entry,
    sum(d) FILTER (WHERE bt > grad_time + INTERVAL '240' MINUTE AND bt < grad_time + INTERVAL '8' DAY) AS net_after_entry,
{days}
  FROM af GROUP BY 1),
sn AS (
  SELECT s.mint, count(*) AS seta_n,
         count_if(s.w IN (SELECT w FROM funded f WHERE f.mint = s.mint)) AS n_funded,
         count_if(s.w IN (SELECT w FROM bundle x WHERE x.mint = s.mint)) AS n_bundle
  FROM seta s GROUP BY 1)
SELECT u.mint,
       CASE WHEN b.mint IS NULL THEN 'created_outside_window' END AS reason,
       sn.seta_n, sn.n_funded, sn.n_bundle,
       a.hold_entry, a.net_after_entry,
       CASE WHEN a.hold_entry > 0 THEN -coalesce(a.net_after_entry, 0) / a.hold_entry END AS sf_7d,
       a.f0, a.f1, a.f2, a.f3, a.f4, a.f5, a.f6, a.f7
FROM u LEFT JOIN base b ON b.mint = u.mint LEFT JOIN sn ON sn.mint = u.mint LEFT JOIN agg a ON a.mint = u.mint"""


if __name__ == "__main__":
    out = ROOT / "recon" / "sql"
    (out / "a3_seta_day.sql").write_text(build("2025-06-09", "2025-06-09"), encoding="utf-8")
    (out / "a3_seta_week.sql").write_text(build("2025-06-09", "2025-06-15"), encoding="utf-8")
    print("written: a3_seta_day.sql, a3_seta_week.sql")
