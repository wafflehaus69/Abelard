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
