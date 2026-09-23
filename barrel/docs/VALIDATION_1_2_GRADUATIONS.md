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

**N measured, 2026-09-01** (`recon/sql/stratum_n_multivenue_day.sql`): **197 admitted** = Raydium CP 143 (of 152 inits; 8 non-SOL pairs, 1 pump-origin removed) + Meteora DLMM 52 (of 177 first-trade pools; **94 pump-origin removed**, 36 non-SOL) + Raydium v4 2. LaunchLab-origin removals: 0 that day. **CLMM: `amm_v3_evt_poolcreatedevent` has 0 rows for 2026-09-01→21** — stale decoded coverage on Dune, dated 2026-09-22 per [E15]; CLMM is excluded from N until re-checked. Meteora's leg is a first-trade proxy because Dune has no Meteora pool-creation table; every N row from Meteora carries `birth_source = 'first_trade'`. **The N rule and the proxy need Architect confirmation before the window run.**

## Correction (2026-09-22): current-era pump.fun mints are mostly Token-2022

The S1/S2 sample of five 2026-09-01 P graduates: **4 Token-2022, 1 legacy SPL** (`pump_evt_createevent.token_program`). M0_RECON R4's "pump.fun mints are legacy SPL" held for the 2025 sample and is wrong for the current era (pump.fun `create_v2`). Consequences: S3/S4 are live checks in stratum P; S1/S2 read both `spl_token_solana` and `spl_token_2022_solana` tables; the derived table's `mint` join must not assume a token program.
