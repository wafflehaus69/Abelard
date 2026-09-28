# Readiness 1.3 — derived trade table schema — **FROZEN 2026-09-22 (MR-7), 20 + 3 columns**

The Architect's sign-off (MR-6) is conditional: frozen if the columns are the R4 list plus `quote_mint`, a stored `trader_cost = gross − net`, a stored `residual = gross − net − (lp + protocol + creator)` kept as an unnamed leg, and the cost-identity flag. **The v1 draft differed** (it carried `base_amount` and `degraded`, and computed rather than stored the cost columns), so per the ruling this file goes back rather than being frozen by the builder.

Lives inside Dune as a materialized view (`dune.<user>.result_barrel_trades`); only aggregates leave (A1_DUNE_FITNESS §4).

## Columns that match the condition (20)

| # | Column | Type | Source (P) | R4 item |
|---|---|---|---|---|
| 1 | `signature` | varchar | `evt_tx_id` | signature |
| 2 | `slot` | bigint | `evt_block_slot` | slot |
| 3 | `block_timestamp` | timestamp | `evt_block_time` | block_timestamp |
| 4 | `tx_index` | integer | `evt_tx_index` | in-slot tx index |
| 5 | `program` | varchar | venue program id | program |
| 6 | `pool` | varchar | `pool` | pool |
| 7 | `mint` | varchar | via `createpoolevent.base_mint` | mint |
| 8 | `side` | varchar | `'buy'`/`'sell'` | side |
| 9 | `wallet` | varchar | `user` | wallet |
| 10 | `gross_quote` | uint256 | variant-aware (see conservation note) | gross amount |
| 11 | `net_quote` | uint256 | the other side of the pair | net amount |
| 12 | `lp_fee` | uint256 | `lp_fee` | fee leg |
| 13 | `protocol_fee` | uint256 | `protocol_fee` | fee leg |
| 14 | `coin_creator_fee` | uint256 | `coin_creator_fee` | fee leg |
| 15 | `era` | varchar | `pre_boost` / `post_boost` at 2026-07-21 | era |
| 16 | `stratum` | varchar | `P` / `P_alt` / `R` / `N` | stratum |
| 17 | `quote_mint` | varchar | `createpoolevent.quote_mint` (authoritative) | + quote_mint |
| 18 | `trader_cost` | uint256 | `gross_quote − net_quote`, **stored** | + cost column |
| 19 | `residual` | int256 | `trader_cost − (lp + protocol + creator)`, **stored, never folded** | + residual |
| 20 | `cost_identity_ok` | boolean | `abs(residual) ≤ 1` | + identity flag |

## Additions — approved MR-7 (3)

| # | Column | Why | Cost of omitting |
|---|---|---|---|
| 21 | `base_amount` | FIFO lot accounting (MR-4.4) and slippage need the token quantity per swap | lots and slippage would re-read the event tables per query |
| 22 | `degraded` | MR-5.6 requires the flag on every token trading inside 2025-08-05→11 | recomputable from `block_timestamp`; storing it keeps the with/without reporting a filter, not a join |
| 23 | `birth_is_proxy` | MR-6 ruling: flag on N rows whose pool birth is Meteora first-trade | required by the ruling; NULL on P/R |

**Nothing else.** Pool reserves after the swap (needed for slippage and the depth floor) are read from the event tables at query time, not stored.

## Conservation note (validated on Dune's rows, 2026-09-01)
Buy variants: `buy` → gross = `user_quote_amount_in`, net = `quote_amount_in`; `buy_exact_quote_in` → the reverse; sells → gross = `quote_amount_out`, net = `user_quote_amount_out`. The identity holds on 84% of rows (9,594 mints, zero failures); the residual is positive and clusters in 264 + 3,500 mints — a real unnamed leg, kept as column 19.

Deliberate exclusions: any carve-out leg (absent from Dune's pinned layout and not trader cost); anything from `pump_evt_createevent` (name/symbol/uri quarantined).


## Export contract (readiness 1.3, third bullet) — filled 2026-09-28

Only aggregates leave Dune. Datapoint costs at **1 credit / 1,000 datapoints** (Free, Analyst).

| Export | Rows × columns | Datapoints | Credits |
|---|---|---|---|
| Per-token aggregate, stratum P (one row per mint; era, stratum, entry marks at G=15/60/240, gate S1–S11 verdicts, RUG label, expectancy inputs) | 180,543 × 23 | 4,152,489 | **~4,152** |
| H1–H4 result grid (cells × metrics) | 108 × 12 | 1,296 | ~1.3 |
| 20-token hand-reconciliation rows | 20 × 23 | 460 | <1 |
| Calibration-slice per-token-day rows (A3 distributions; first 20% of weeks, 7 days each) | ~36,108 × 7 × 30 | 7,582,680 | **~7,583** |
| Row-level trades | never | — | — |

The per-token aggregate is the one export that matters; the calibration-slice rows are avoidable if the A3 distributions are computed in-warehouse and only their percentiles exported (the recommendation). **Projected export budget: ~4,152 credits with in-warehouse distributions, ~11,735 without.** This is stratum P only; R and N add their own rows.
