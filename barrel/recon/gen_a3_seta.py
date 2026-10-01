"""Burn-down item 4a: aligned-set net flow, days 0-7 (the RUG-A input), per token.

Generates recon/sql/a3_seta_day.sql and a3_seta_week.sql. Distributions only; no threshold.
Unsaved ad-hoc queries (never a saved query: the logic stays out of Dune).

Aligned set, as buildable today:
  creator
  + wallets the creator sent SOL to within +/-24h of creation        (S7, 1-hop)
  + creation-slot traders on the bonding curve, when >= 5 of them share one funder
    in the 24h before creation                                        (S8 bundle)
  (fee-share recipients, c07b, are NOT included: cut from M0 by MR-10)

Holdings and flows come from the token transfer ledger, so bonding-curve purchases
before graduation are counted:
  hold_entry = net balance of the set at graduation + 240 min
  f0..f7     = net token flow of the set in day k after graduation (sells negative)
  sf_7d      = net amount the set shed between entry and graduation + 8d, over hold_entry

Scope limit: creations within 3 days before graduation (95.6% of the sample week).
Tokens created earlier are returned with seta_n NULL and reason 'created_outside_window'.

v2 (2026-10-01). The first version referenced its SOL-transfer CTE and its token-ledger CTE
several times each. The engine inlines a CTE at every reference, so each large table was
scanned once per reference; the one-day run billed 85 credits. Here every large table is
referenced exactly once: SOL transfers are unpivoted to (address, role) and hash-joined to
one key table; ledger rows are unpivoted to (wallet, signed amount); set membership and
flows are aggregated in one pass. Owner wallets are removed from the set (MR-3.4).
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
-- v2: each large table referenced once (see generator docstring).
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
buyers0 AS (
  SELECT DISTINCT b.mint, t.user AS w, b.t0
  FROM pumpdotfun_solana.pump_evt_tradeevent t JOIN base b ON t.mint = b.mint AND t.evt_block_slot = b.s0
  -- no is_buy filter: that column is NULL on every 2025 trade event (tradeevent_layout_probe.sql).
  -- Nothing is lost: a wallet that sells in the creation slot had to acquire in that slot first.
  WHERE t.evt_block_date BETWEEN DATE '{cr0}' AND DATE '{gd1}'),
keys AS (   -- role F: the creator as sender; role B: a creation-slot buyer as receiver
  SELECT mint, creator AS k, 'F' AS role, t0 FROM base
  UNION ALL
  SELECT mint, w, 'B', t0 FROM buyers0),
hit AS (    -- the only reference to sol_transfers; literal block_time bounds (S8 watchdog rule)
  SELECT k.mint, k.role,
         CASE k.role WHEN 'F' THEN s.to_owner ELSE k.k END AS w,
         CASE k.role WHEN 'B' THEN s.from_owner END AS funder
  FROM tokens_solana.sol_transfers s
  CROSS JOIN UNNEST(ARRAY[s.from_owner, s.to_owner], ARRAY['F', 'B']) AS x(addr, role)
  JOIN keys k ON k.k = x.addr AND k.role = x.role
  WHERE s.block_time >= TIMESTAMP '{st0} 00:00:00' AND s.block_time < TIMESTAMP '{st1} 00:00:00'
    AND CAST(s.amount AS double) >= 1e6
    AND ((k.role = 'F' AND s.block_time BETWEEN k.t0 - INTERVAL '24' HOUR AND k.t0 + INTERVAL '24' HOUR)
      OR (k.role = 'B' AND s.block_time BETWEEN k.t0 - INTERVAL '24' HOUR AND k.t0))
  GROUP BY 1, 2, 3, 4),
memb AS (
  SELECT mint, role, w, count(*) OVER (PARTITION BY mint, role, funder) AS n_same_funder FROM hit),
seta AS (
  SELECT mint, w, bool_or(src = 'F') AS is_funded, bool_or(src = 'B') AS is_bundle
  FROM (SELECT mint, creator AS w, 'C' AS src FROM base
        UNION ALL
        SELECT mint, w, role FROM memb WHERE role = 'F' OR n_same_funder >= 5)
  WHERE w IS NOT NULL AND __NOT_OWNER(w)__
  GROUP BY 1, 2),
flows AS (  -- the only reference to the token ledger
  SELECT t.token_mint_address AS mint, x.w, x.d, t.block_time AS bt
  FROM tokens_solana.transfers t
  CROSS JOIN UNNEST(ARRAY[t.to_owner, t.from_owner],
                    ARRAY[CAST(t.amount AS double), -CAST(t.amount AS double)]) AS x(w, d)
  WHERE t.block_date BETWEEN DATE '{cr0}' AND DATE '{tr1}'
    AND t.token_mint_address IN (SELECT mint FROM base) AND x.w IS NOT NULL),
j AS (
  SELECT s.mint, s.w, s.is_funded, s.is_bundle, f.d, f.bt, b.grad_time
  FROM seta s JOIN base b ON b.mint = s.mint
  LEFT JOIN flows f ON f.mint = s.mint AND f.w = s.w),
agg AS (
  SELECT mint,
    count(DISTINCT w) AS seta_n,
    count(DISTINCT w) FILTER (WHERE is_funded) AS n_funded,
    count(DISTINCT w) FILTER (WHERE is_bundle) AS n_bundle,
    count(DISTINCT w) FILTER (WHERE d IS NOT NULL) AS n_with_flow,
    sum(d) FILTER (WHERE bt <= grad_time + INTERVAL '240' MINUTE) AS hold_entry,
    sum(d) FILTER (WHERE bt > grad_time + INTERVAL '240' MINUTE AND bt < grad_time + INTERVAL '8' DAY) AS net_after_entry,
{days}
  FROM j GROUP BY 1)
SELECT u.mint,
       CASE WHEN b.mint IS NULL THEN 'created_outside_window' END AS reason,
       a.seta_n, a.n_funded, a.n_bundle, a.n_with_flow,
       a.hold_entry, a.net_after_entry,
       CASE WHEN a.hold_entry > 0 THEN -coalesce(a.net_after_entry, 0) / a.hold_entry END AS sf_7d,
       a.f0, a.f1, a.f2, a.f3, a.f4, a.f5, a.f6, a.f7
FROM u LEFT JOIN base b ON b.mint = u.mint LEFT JOIN agg a ON a.mint = u.mint"""


if __name__ == "__main__":
    out = ROOT / "recon" / "sql"
    (out / "a3_seta_day.sql").write_text(build("2025-06-09", "2025-06-09"), encoding="utf-8")
    (out / "a3_seta_week.sql").write_text(build("2025-06-09", "2025-06-15"), encoding="utf-8")
    print("written: a3_seta_day.sql, a3_seta_week.sql")
