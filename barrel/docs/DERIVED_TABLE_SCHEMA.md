# Readiness 1.3 — derived trade table, schema (frozen 2026-09-22, builder draft)

Per H3 Amendment v1.1 R4 and MR-4.5, with the Dune finding that the table lives **inside Dune**
as a materialized view (`dune.<user>.result_barrel_trades`) and only aggregates leave.
Column list is exactly R4's, made concrete against Dune's decoded PumpSwap events. **Nothing
else.** Any addition is an amendment, not an edit.

| # | Column | Type | Source (stratum P) | Note |
|---|---|---|---|---|
| 1 | `signature` | varchar | `evt_tx_id` | |
| 2 | `slot` | bigint | `evt_block_slot` | |
| 3 | `block_timestamp` | timestamp | `evt_block_time` | |
| 4 | `tx_index` | integer | `evt_tx_index` | in-slot order; sandwich test (R5) |
| 5 | `program` | varchar | constant `pAMMBay6…` for P; venue program for R/N | |
| 6 | `pool` | varchar | `pool` | |
| 7 | `mint` | varchar | base mint via `pump_amm_evt_createpoolevent.base_mint` | join key to graduation |
| 8 | `quote_mint` | varchar | `createpoolevent.quote_mint` (authoritative) | P = WSOL |
| 9 | `side` | varchar | `'buy'` / `'sell'` from the event table | |
| 10 | `wallet` | varchar | `user` | owner-wallet exclusion applied here |
| 11 | `gross_quote` | uint256 | buy: `user_quote_amount_in` / `quote_amount_in` per `ix_name` variant; sell: `quote_amount_out` | trader's gross side, variant-aware (see `decode_pumpswap_events.BUY_SIDES`) |
| 12 | `net_quote` | uint256 | the other side of the same pair | pool's side |
| 13 | `base_amount` | uint256 | buy: `base_amount_out`; sell: `base_amount_in` | |
| 14 | `lp_fee` | uint256 | `lp_fee` | |
| 15 | `protocol_fee` | uint256 | `protocol_fee` | |
| 16 | `coin_creator_fee` | uint256 | `coin_creator_fee` | |
| 17 | `era` | varchar | `'pre_boost'` if `block_timestamp < 2026-07-21`, else `'post_boost'` | MR-4 |
| 18 | `stratum` | varchar | `'P'` / `'P_alt'` / `'R'` / `'N'` | MR-4, 1.2 rule |
| 19 | `degraded` | boolean | `block_date BETWEEN 2025-08-05 AND 2025-08-11` | MR-5.6 |

**Deliberate exclusions:** pool reserves after the swap (available in the event; needed for
slippage and the depth floor — computed at query time from the event, not stored, because
storage is the constrained resource); `coin_creator_fee_basis_points` (derivable); any
carve-out leg (buyback/cashback/holder — not trader cost, and absent from Dune's pinned
layout anyway); anything from `pump_evt_createevent` (name/symbol/uri are quarantined and
never enter the table).

**Open before the one-day build:** `ix_name` is present on buy events only; the variant
mapping for `gross_quote` must be confirmed against Dune's rows the same way it was against
chain events (conservation: gross − net = lp + protocol + creator). R and N source columns
depend on the Raydium/Meteora table discovery.

**Export contract (1.3, third bullet) — draft, to be filled with counts:** per-token
aggregate (one row per mint × era), per-token-day aggregate for the calibration slice only,
the H1–H4 result grid (~108 rows), and the 20-token reconciliation rows. Row-level never.
