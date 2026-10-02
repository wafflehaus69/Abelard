# Readiness 1.2 — graduation-event query, validated (2026-09-22)

**Query:** `recon/sql/graduations_day.sql` on `pumpdotfun_solana.pump_evt_completepumpammmigrationevent`, sample day 2026-09-01.
**Result:** 1,209 migrations, 1,209 distinct mints, 1,209 distinct pools, all emitted as inner events.

## Reconciliation, four ways

| Check | Source | Result |
|---|---|---|
| Raw instruction calls, same day, pump program, by exact 8-byte discriminator from the pinned IDL | Dune `solana.instruction_calls` (`recon/sql/migration_instructions_day.sql`) | `migrate` 667 (250 outer / 417 inner), `migrate_v2` 2,026, `migrate_bonding_curve_creator` 3,182 (creator-field migration, **not** a graduation) |
| Independent count, top-level `migrate` only | BigQuery `Instructions` (top-level only), same discriminator | **250 = Dune outer 250, exact** |
| `quote_mint` rule | join to `pump_amm_evt_createpoolevent.quote_mint` (`recon/sql/graduations_quote_confirm_day.sql`) | placeholder `1111…1111` → WSOL pool **1,154 / 1,154**; USDC → USDC pool 55 / 55 |
| Chain | `recon/chain_check_graduations.py`, 6 samples (3 SOL, 3 USDC) via public RPC | bonding curve owned by pump, `complete = true`, `real_token_reserves = 0`; pool owned by PumpSwap — **6 / 6** |

**Why calls exceed events:** `migrate` is permissionless and idempotent; bots race it, and only the first call on a curve migrates and emits the event. 2,693 graduation-capable calls → 1,209 events with 1,209 distinct mints is that race. **The decoded event is the admission signal; instruction calls are an upper bound.**

**Rules established for the universe layer**
1. Admission to stratum P is one `CompletePumpAmmMigrationEvent` per mint.
2. On that event, `quote_mint = 11111111111111111111111111111111` means SOL-quoted → **P**; any other value → **P-alt**, reported separately, no conversion. Confirmed at the pool-creation event on every row of the sample day.
3. Older on-chain accounts and events carry shorter layouts than the current IDL; decoders stop at end-of-bytes and record absent fields as absent, never zero (the bonding-curve check hit this on the first run).

**Not yet done from 1.2:** the R (pre-2025-03-20 Raydium graduates) and N (Raydium/Meteora native) admission queries, era and DEGRADED labels as columns, and owner-wallet exclusion (wallets not yet supplied).

## Stratum R (pre-2025-03-20, pump.fun → Raydium v4) — validated 2026-09-22

Sample day 2025-02-15: **339** `CompleteEvent`s. Either-side join to `raydium_amm_call_initialize2` within ±3 days matches **339 / 339**, all signed by **one** address, `39azUYFWPz3VHgKCf3VChUwbpURdCHRxjWVowf5jUJjg` (pump.fun's migration authority), median complete→pool lag **98 s**. The official migration places the pump mint on the **pc** side (`account_pcMint`) with WSOL as `account_coinMint`; a coin-side-only join returned 0, which is how this was found. Independent count: BigQuery top-level Raydium v4 inits referencing a `…pump` mint that day = **606 = Dune 297 coin + 309 pc**, exact; the other 267 are third-party pools and are **not** R.

**R admission rule:** `CompleteEvent` for the mint, then `initialize2` with `call_tx_signer = 39azUYFW…`, `account_pcMint = mint`, `account_coinMint = WSOL`, pool time ≥ complete time. Signer is the discriminator; pool creation alone is not.

## Labelled universe query and stratum N — status 2026-09-22

`recon/sql/universe_day.sql` on 2026-09-01: **P 1,154 · P_alt 55 · R 0**, era `post_boost`, `degraded = false`. R correctly empty on a post-cutover day. Era and DEGRADED columns are CASE expressions on graduation time and need no external data.

**N is multi-venue.** Raydium v4 saw **2** pool inits on 2026-09-01; the legacy AMM is negligible in the P window. Native pools now appear on Raydium CP-swap, Raydium CLMM, and Meteora. The N rule (venue pool whose base mint has no pump `CreateEvent`, no LaunchLab pool-create, and is not signed by the pump migration authority) is written for v4 in `recon/sql/stratum_n_day.sql` and extends to the other venues once their creation tables are confirmed; Meteora may lack a decoded pool-creation event on Dune, in which case first trade is the pool-birth proxy and is labelled as such.

> **Flag, 2026-10-01 (review).** These three stratum-N queries read `base_mint_param` from the LaunchLab pool-create event and compare it to a mint. In the LaunchLab IDL that field may be the mint *parameters* (decimals, name, symbol, uri), not the mint. Not confirmed either way. If it is, the LaunchLab exclusion never matched and the queries touched quarantined metadata. **"LaunchLab-origin removals: 0" below is struck as unmeasured**, and the three queries are not to be run until a `LIMIT 0` column probe settles the column. Stratum N is outside the paid build.

**N measured, 2026-09-01** (`recon/sql/stratum_n_multivenue_day.sql`): **197 admitted** = Raydium CP 143 (of 152 inits; 8 non-SOL pairs, 1 pump-origin removed) + Meteora DLMM 52 (of 177 first-trade pools; **94 pump-origin removed**, 36 non-SOL) + Raydium v4 2. LaunchLab-origin removals: 0 that day. **CLMM: `amm_v3_evt_poolcreatedevent` has 0 rows for 2026-09-01→21** — stale decoded coverage on Dune, dated 2026-09-22 per [E15]; CLMM is excluded from N until re-checked. Meteora's leg is a first-trade proxy because Dune has no Meteora pool-creation table; every N row from Meteora carries `birth_source = 'first_trade'`. **The N rule and the proxy need Architect confirmation before the window run.**

## Correction (2026-09-22): current-era pump.fun mints are mostly Token-2022

The S1/S2 sample of five 2026-09-01 P graduates: **4 Token-2022, 1 legacy SPL** (`pump_evt_createevent.token_program`). M0_RECON R4's "pump.fun mints are legacy SPL" held for the 2025 sample and is wrong for the current era (pump.fun `create_v2`). Consequences: S3/S4 are live checks in stratum P; S1/S2 read both `spl_token_solana` and `spl_token_2022_solana` tables; the derived table's `mint` join must not assume a token program.

## Stratum P, pre-event era (2025-03-20 → 2026-04-30) — source and validation, 2026-09-28

`pump_evt_completepumpammmigrationevent` carries **no rows before May 2026** (`universe_p_by_month.sql`: 0 for every month 2025-03 → 2026-04; 2,048 in 2026-05; full volume from June). `pump_amm_evt_createpoolevent` is complete for the whole window (6,555 pools in 2025-03 → 100k+/month; `createpool_by_month.sql`). Whether the event was introduced in May 2026 or its decoding began then is not determined and does not change the fix.

**Pre-event P admission rule:** `CompleteEvent` (bonding curve complete) for the mint, then a PumpSwap `createpoolevent` with `base_mint = mint`, `quote_mint = WSOL`, `evt_block_time ≥ complete_time`, within 1 day. **Validated on 2025-09-15 (`stratum_p_preevent_day.sql`): 321 completes → 321 with a pool → 321 WSOL-quoted; lag p50 1 s, p90 2 s.** The migration is permissionless, so pool `creator` is not a discriminator (323 distinct creators across 321 mints; the extra two are duplicate pool rows on a mint and are handled by taking the earliest pool).

**Consequence:** the pre-registered P window (from 2025-03-20), the calibration slice (to 2025-07-10) and the 1.7 rebalance date (2026-03-23) are all recoverable. From May 2026 the decoded migration event is used and cross-checked against this rule on the overlap month; before it, this rule is the source. `universe_day.sql` is updated to take the union.


## P universe over the window (2026-09-28) — `universe_p_preevent_by_month.sql`, 3.3 credits

| month | completes | with PumpSwap pool ≤ 1 d | P (WSOL) |
|---|---|---|---|
| 2025-03 | 2,721 | 2,562 | 2,549 |
| 2025-04 | 9,059 | 8,732 | 8,708 |
| 2025-05 | 8,447 | 8,411 | 8,394 |
| 2025-06 | 6,761 | 6,748 | 6,729 |
| 2025-07 | 2,801 | 2,797 | 2,790 |
| 2025-08 | 4,759 | 4,754 | 4,739 |
| 2025-09 | 4,373 | 4,368 | 4,356 |
| 2025-10 | 2,920 | 2,918 | 2,914 |
| 2025-11 | 3,568 | 3,566 | 3,560 |
| 2025-12 | 4,337 | 4,331 | 4,323 |
| 2026-01 | 6,473 | 6,467 | 6,464 |
| 2026-02 | 8,369 | 8,359 | 8,349 |
| 2026-03 | 11,430 | 11,396 | 11,390 |
| 2026-04 | 8,193 | 8,187 | 8,187 |
| 2026-05 | 7,006 | 7,003 | 6,949 |
| 2026-06 | 7,417 | 7,412 | 7,212 |

**Cross-check on the overlap:** event table June 2026 = 7,215 P vs rule = 7,212 (**0.04%**); event table May 2026 = 2,048 vs rule = 6,949 — the event (or its decoding) begins mid-May, and the rule is the source before it. March 2025 shows 94% with a PumpSwap pool (the cutover month; the rest went to Raydium and are R), 96% in April, ≥ 99.7% after.

**Stratum P size:** pre-event era (2025-03 → 2026-06, rule) **97,613**; post (2026-07 → 09, event table) **82,930**; **total ≈ 180,543 tokens.** BOOST is visible as June 7,212 → July 21,666.
