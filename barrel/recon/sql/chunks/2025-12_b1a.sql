-- Heavy tier B1a (build form): aligned-set members grouped by funder, graduations 2025-12-01 .. 2025-12-31.
-- Rows name wallets: run with --private-rows. No threshold, no classification, no verdict here.
WITH comp AS (
  SELECT mint, evt_block_time AS ct FROM pumpdotfun_solana.pump_evt_completeevent
  WHERE evt_block_date BETWEEN DATE '2025-12-01' AND DATE '2025-12-31'),
pc AS (
  SELECT base_mint AS mint, quote_mint, pool, evt_block_time AS pt,
         row_number() OVER (PARTITION BY base_mint ORDER BY evt_block_time) AS rn
  FROM pumpdotfun_solana.pump_amm_evt_createpoolevent
  WHERE evt_block_date BETWEEN DATE '2025-12-01' AND DATE '2026-01-01'),
u AS (
  SELECT c.mint, p.pt AS grad_time, p.pool
  FROM comp c JOIN pc p ON p.mint = c.mint AND p.rn = 1 AND p.pt >= c.ct AND p.pt < c.ct + INTERVAL '1' DAY
  WHERE p.quote_mint = 'So11111111111111111111111111111111111111112'),
cr AS (
  SELECT mint, arbitrary(COALESCE(creator, "user")) AS creator, min(evt_block_time) AS t0, min(evt_block_slot) AS s0
  FROM pumpdotfun_solana.pump_evt_createevent
  WHERE evt_block_date BETWEEN DATE '2025-11-28' AND DATE '2025-12-31' AND mint IN (SELECT mint FROM u)
  GROUP BY 1),
base AS (   -- per-token creation bound: the same 3 days for every token, wherever it falls in the chunk
  SELECT u.mint, u.grad_time, u.pool, cr.creator, cr.t0, cr.s0 FROM u JOIN cr ON cr.mint = u.mint
  WHERE cr.t0 >= date_trunc('day', u.grad_time) - INTERVAL '3' DAY),
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
    -- first acquisition, within the same horizon for every token (9 days after graduation)
    min(t.block_time) FILTER (WHERE x.d > 0 AND t.block_time < b.grad_time + INTERVAL '9' DAY) AS first_in,
    min(t.block_slot) FILTER (WHERE x.d > 0 AND t.block_time < b.grad_time + INTERVAL '9' DAY) AS first_in_slot,
    -- the transaction of that first acquisition (earliest by slot, then by position in the slot)
    min_by(t.tx_id, t.block_slot * 100000 + coalesce(t.tx_index, 0))
      FILTER (WHERE x.d > 0 AND t.block_time < b.grad_time + INTERVAL '9' DAY) AS first_in_tx,
    bool_or(x.tag = 'to' AND t.from_owner IS NULL) AS got_mint
  FROM tokens_solana.transfers t
  JOIN base b ON b.mint = t.token_mint_address
  CROSS JOIN UNNEST(
    ARRAY[t.to_owner, t.from_owner, '#SUPPLY'],
    ARRAY[CAST(t.amount AS double), -CAST(t.amount AS double),
          CASE WHEN t.from_owner IS NULL THEN CAST(t.amount AS double)
               WHEN t.to_owner IS NULL THEN -CAST(t.amount AS double) ELSE 0e0 END],
    ARRAY['to', 'from', 'sup']) AS x(w, d, tag)
  WHERE t.block_date BETWEEN DATE '2025-11-28' AND DATE '2026-01-10'
    AND t.token_mint_address IN (SELECT mint FROM base) AND x.w IS NOT NULL
  GROUP BY 1, 2),
buyers0 AS (
  -- no is_buy filter: NULL on every 2025 trade event; a creation-slot seller acquired in that slot
  SELECT DISTINCT b.mint, t.user AS w, b.t0
  FROM pumpdotfun_solana.pump_evt_tradeevent t JOIN base b ON t.mint = b.mint AND t.evt_block_slot = b.s0
  WHERE t.evt_block_date BETWEEN DATE '2025-11-28' AND DATE '2025-12-31'),
keys AS (
  SELECT mint, creator AS k, 'F' AS role, t0 FROM base
  UNION ALL
  SELECT mint, w, 'B', t0 FROM buyers0),
hit AS (
  SELECT k.mint, k.role,
         CASE k.role WHEN 'F' THEN s.to_owner ELSE k.k END AS w,
         CASE k.role WHEN 'B' THEN s.from_owner END AS funder,
         sum(CAST(s.amount AS double)) / 1e9 AS sol,
         array_agg(DISTINCT s.tx_id) FILTER (WHERE k.role = 'F') AS cr_txs   -- transactions in which the creator paid this wallet
  FROM tokens_solana.sol_transfers s
  CROSS JOIN UNNEST(ARRAY[s.from_owner, s.to_owner], ARRAY['F', 'B']) AS x(addr, role)
  JOIN keys k ON k.k = x.addr AND k.role = x.role
  WHERE s.block_time >= TIMESTAMP '2025-11-27 00:00:00' AND s.block_time < TIMESTAMP '2026-01-02 00:00:00'
    AND CAST(s.amount AS double) >= 1e6
    -- a creator's transfer to itself (wrapping SOL, moving between its own accounts) is not a wallet it paid:
    -- without the guard the creator's own row came back creator-funded with a link type (13 of 2,846 tokens measured)
    AND ((k.role = 'F' AND s.to_owner <> s.from_owner AND s.block_time BETWEEN k.t0 - INTERVAL '24' HOUR AND k.t0 + INTERVAL '24' HOUR)
      OR (k.role = 'B' AND s.block_time BETWEEN k.t0 - INTERVAL '24' HOUR AND k.t0))
  GROUP BY 1, 2, 3, 4),
memb AS (
  SELECT mint, role, w, sol, cr_txs, count(*) OVER (PARTITION BY mint, role, funder) AS n_same_funder FROM hit),
seta AS (
  -- bundle_n = how many creation-slot traders share this wallet's funder (itself included).
  -- Whether that makes a bundle is decided locally (MR-14 ruling 5); every candidate is returned.
  SELECT mint, w, bool_or(src = 'C') AS is_creator, bool_or(src = 'F') AS is_funded,
         max(nsf) FILTER (WHERE src = 'B') AS bundle_n,
         sum(sol) FILTER (WHERE src = 'F') AS cr_sol,
         arbitrary(cr_txs) FILTER (WHERE src = 'F') AS cr_txs
  FROM (SELECT mint, creator AS w, 'C' AS src, CAST(NULL AS double) AS sol, CAST(NULL AS bigint) AS nsf, CAST(NULL AS array(varchar)) AS cr_txs FROM base
        UNION ALL
        SELECT mint, w, role, sol, n_same_funder, cr_txs FROM memb)
  WHERE w IS NOT NULL AND __NOT_OWNER(w)__
  GROUP BY 1, 2),
mm0 AS (
  SELECT s.mint, s.w, s.is_creator, s.is_funded, s.bundle_n, s.cr_sol,
         bs.grad_time, bs.t0, bs.creator, b.first_in, b.b15, b.b60, b.b240, b.net_after, b.got_mint,
         CASE WHEN s.is_creator THEN bs.s0 ELSE b.first_in_slot END AS slot_act, n0.n_slot0, b.first_in_tx,
         -- How the creator is tied to a wallet it paid (MR-16). token_delivery_by_creator: the creator's SOL reached
         -- the wallet inside the wallet's own first-acquisition transaction (the creator opened its token account and
         -- handed it tokens; the SOL is rent). sol_funding: the creator paid it in some other transaction. Both can hold.
         CASE WHEN NOT s.is_funded THEN NULL
              WHEN coalesce(contains(s.cr_txs, b.first_in_tx), false)
                   THEN CASE WHEN cardinality(s.cr_txs) > 1 THEN 'both' ELSE 'token_delivery_by_creator' END
              ELSE 'sol_funding' END AS link,
         b.f0, b.f1, b.f2, b.f3, b.f4, b.f5, b.f6, b.f7,
         CASE WHEN s.is_creator THEN bs.t0 ELSE b.first_in END AS t_act,
         count(*) OVER (PARTITION BY s.mint) AS n_all,
         count_if(s.is_funded) OVER (PARTITION BY s.mint) AS n_funded_all,
         max(s.bundle_n) OVER (PARTITION BY s.mint) AS bundle_n_max
  FROM seta s JOIN base bs ON bs.mint = s.mint
  LEFT JOIN bal b ON b.mint = s.mint AND b.w = s.w
  -- creation-slot traders per token, so S8 can say UNKNOWN (none decoded) apart from 'no'
  LEFT JOIN (SELECT mint, count(*) AS n_slot0 FROM buyers0 GROUP BY 1) n0 ON n0.mint = s.mint),
mm AS (SELECT * FROM mm0 WHERE (is_creator OR first_in IS NOT NULL) AND NOT coalesce(got_mint, false)),
inb0 AS (   -- one row per (member, sender): first and last transfer before the member's first action
  SELECT m.mint, m.w, s.from_owner AS sender, min(s.block_time) AS t_first, max(s.block_slot) AS slot_last,
         max_by(CAST(s.amount AS double), s.block_slot) / 1e9 AS last_sol
  FROM tokens_solana.sol_transfers s JOIN mm m ON s.to_owner = m.w
  WHERE s.block_time >= TIMESTAMP '2025-11-27 00:00:00' AND s.block_time < TIMESTAMP '2026-01-11 00:00:00'
    AND CAST(s.amount AS double) >= 1e6
    -- per-token window: 4 days before the token's graduation day, up to and including the slot of the
    -- member's first action (funding and acting in one slot is the bundle pattern; seconds cannot order it)
    AND s.from_owner <> m.w AND s.block_slot <= m.slot_act
    -- MR-16: a transfer inside the member's own first-acquisition transaction is rent for the account that
    -- received the tokens, not funding. Same-slot funding in a SEPARATE transaction still counts.
    -- NULL-safe: only a transfer KNOWN to be in that transaction is excluded; an empty tx_id excludes nothing
    AND (m.first_in_tx IS NULL OR s.tx_id IS NULL OR s.tx_id <> m.first_in_tx)
    AND s.block_time >= date_trunc('day', m.grad_time) - INTERVAL '4' DAY
  GROUP BY 1, 2, 3),
inb AS (    -- funder = the last sender before the first action; funded_ts = that funder's FIRST transfer (MR-12.2).
            -- "Last" is by slot, then by address: block_time is whole seconds and two senders can share one
            -- (found on 2025-06-09: two transfers in the same second, one slot apart, gave two different funders).
  SELECT mint, w, sender AS funder, t_first AS funded_ts, last_sol AS funded_sol, n_senders
  FROM (SELECT *, row_number() OVER (PARTITION BY mint, w ORDER BY slot_last DESC, sender DESC) AS rn,
               count(*) OVER (PARTITION BY mint, w) AS n_senders
        FROM inb0)
  WHERE rn = 1),
mf AS (
  SELECT m.mint, m.w, m.creator, m.is_creator, m.is_funded,
         -- bo: NULL for the creator and wallets the creator funded (always in the set); for a wallet that
         -- is a candidate only as a creation-slot trader, its same-funder count (the cut is local)
         CASE WHEN m.is_creator OR m.is_funded THEN NULL ELSE m.bundle_n END AS bo, m.link,
         m.n_all, m.n_funded_all, m.bundle_n_max, m.n_slot0, m.b15, m.b60, m.b240, m.net_after, m.f0, m.f1, m.f2, m.f3, m.f4, m.f5, m.f6, m.f7,
         m.first_in, m.grad_time, m.t0, i.funder, i.funded_ts, i.n_senders,
         sup.b15 AS supply15, sup.b60 AS supply60, sup.b240 AS supply240
  FROM mm m
  LEFT JOIN inb i ON i.mint = m.mint AND i.w = m.w
  LEFT JOIN bal sup ON sup.mint = m.mint AND sup.w = '#SUPPLY')
SELECT CASE WHEN grouping(x.key) = 0 THEN 'member_funder' ELSE 'group' END AS row_kind,
       mf.mint, mf.funder, mf.bo AS b_only_n, mf.link,
       x.key AS member,
       max(mf.funder) FILTER (WHERE x.kind = 'M') AS member_own_funder, max(mf.bo) FILTER (WHERE x.kind = 'M') AS member_b_only_n, max(mf.link) FILTER (WHERE x.kind = 'M') AS member_link,
       count(*) FILTER (WHERE x.kind = 'F') AS n_funded_by_member,
       arbitrary(mf.creator) FILTER (WHERE x.kind = 'M') AS creator,
       count(*) FILTER (WHERE x.kind = 'M') AS n_members,
       count(*) FILTER (WHERE x.kind = 'M' AND mf.is_creator) AS n_creator,
       count(*) FILTER (WHERE x.kind = 'M' AND mf.is_funded) AS n_funded,
       max(mf.n_all) FILTER (WHERE x.kind = 'M') AS n_all, max(mf.n_funded_all) FILTER (WHERE x.kind = 'M') AS n_funded_all,
       max(mf.bundle_n_max) FILTER (WHERE x.kind = 'M') AS bundle_n_max, max(mf.n_slot0) FILTER (WHERE x.kind = 'M') AS n_slot0,
       max(mf.n_senders) FILTER (WHERE x.kind = 'M') AS n_senders_max,
       sum(mf.b15) FILTER (WHERE x.kind = 'M') AS b15, sum(mf.b60) FILTER (WHERE x.kind = 'M') AS b60, sum(mf.b240) FILTER (WHERE x.kind = 'M') AS b240, sum(mf.net_after) FILTER (WHERE x.kind = 'M') AS net_after,
       sum(mf.f0) FILTER (WHERE x.kind = 'M') AS f0, sum(mf.f1) FILTER (WHERE x.kind = 'M') AS f1, sum(mf.f2) FILTER (WHERE x.kind = 'M') AS f2, sum(mf.f3) FILTER (WHERE x.kind = 'M') AS f3, sum(mf.f4) FILTER (WHERE x.kind = 'M') AS f4, sum(mf.f5) FILTER (WHERE x.kind = 'M') AS f5, sum(mf.f6) FILTER (WHERE x.kind = 'M') AS f6, sum(mf.f7) FILTER (WHERE x.kind = 'M') AS f7,
       -- every member's seconds from its funder's first transfer to its first acquisition (no wallet in it):
       -- the per-token median of column 86 is taken locally across members (MR-12.2)
       array_agg(date_diff('second', mf.funded_ts, mf.first_in))
         FILTER (WHERE x.kind = 'M' AND mf.funded_ts IS NOT NULL AND mf.first_in IS NOT NULL) AS lat_s,
       min(mf.first_in) FILTER (WHERE x.kind = 'M') AS first_in_min, arbitrary(mf.grad_time) FILTER (WHERE x.kind = 'M') AS grad_time, arbitrary(mf.t0) FILTER (WHERE x.kind = 'M') AS t0,
       arbitrary(mf.supply15) FILTER (WHERE x.kind = 'M') AS supply15, arbitrary(mf.supply60) FILTER (WHERE x.kind = 'M') AS supply60, arbitrary(mf.supply240) FILTER (WHERE x.kind = 'M') AS supply240
FROM mf
CROSS JOIN UNNEST(ARRAY['M', 'F'], ARRAY[mf.w, mf.funder]) AS x(kind, key)
WHERE x.key IS NOT NULL
GROUP BY GROUPING SETS ((mf.mint, mf.funder, mf.bo, mf.link), (mf.mint, x.key))
HAVING grouping(x.key) = 1 OR (bool_or(x.kind = 'M') AND bool_or(x.kind = 'F'))