# Readiness 1.5 — S5 (LP status) and S7b (fee-share recipients): sources established (2026-09-22)

## S5 — LP status
* **Stratum P:** LP is burned by the `migrate` instruction itself (M0_RECON R3; pump-fun program README). S5 = PASS by construction; to be confirmed per token by the LP-mint burn in the migration transaction once the derived table exists.
* **Strata R/N:** LP tokens exist and S5 is live. "Verifiable locker" = a program Dune has decoded lock instructions for. **Decoded on Dune (Solana):** Meteora pools lock-escrow (`meteora_pools_solana.amm_call_lock`, `amm_evt_lock`, `createlockescrow`); Meteora cp-amm (`cp_amm_call_lock_position`, `permanent_lock_position`, their events); Meteora DBC (`dynamic_bonding_curve_call_migrate_meteora_damm_lock_lp_token`, `create_locker`); Raydium-side lockers via launchpad wrappers (`boopdotfun_solana.boop_call_lock_raydium_liquidity` + event; `ego_solana.ego_one_call_lock_liquidity`; `basedbid_solana.*_lockcpliquidity`, `*_lockclmmpositionnft`).
* **Not decoded on Dune, dated 2026-09-22 [E15]:** Streamflow, Jupiter Lock, Raydium's own lock program. LP sent to any of these, or to any program not on the list above, is **UNKNOWN** (strict FAIL, loose PASS, counted). A burn (transfer to a known burn address or SPL `burn` of the LP mint) is PASS regardless of program.
* Query design: LP mint from the pool-creation row (CP: `account_lpMint`; v4: `account_lpMint`), then the LP mint's transfer/burn history in `tokens_solana.transfers` to the locker program's escrow or a burn, as-of entry.

## S7b — creator fee-share recipients
* The pump fee program `pfeeUxB6jkeY1Hxd7CsFCAjcbHA9rWtchMGdZ6VojVZ` has **no decoded tables on Dune** (2026-09-22). `SharingConfig` state (`shareholders` vec) must be reconstructed from **raw** `solana.instruction_calls` to that program plus its `Program data:` events in `solana.transactions.log_messages`, decoded with the pinned `recon/idl/pump_fees.json` (`CreateFeeSharingConfigEvent`, `UpdateFeeSharesEvent`, `ResetFeeSharingConfigEvent`). As-of-T per MR-4 R7.
* Feasibility probe: `recon/sql/s7b_pfee_probe.sql` (raw call volume and discriminator count on one day). Build follows in the paid month if the volume makes it a materialization job; the readiness deliverable is the query written and dry-run on one day.

## S7b sizing (2026-09-01, `recon/sql/s7b_pfee_by_discriminator.sql`)
38.84M `get_fees` calls (one per swap) vs **3,288 `create_fee_sharing_config` + 2,516 `update_fee_shares_v2` + 751 `update_fee_shares`** — ~6.5k sharing-config calls a day. Two discriminators absent from the pinned `pump_fees.json` (`E445A52E…`, 10,193 inner calls; `0A02B65F…`, 3) — logged; the IDL is re-pinned before the window run and any still-unknown discriminator is counted, never dropped.

**Design:** the sharing-config calls (raw `data`, ~6.5k rows/day) are materialised inside Dune with the `tx_id`, slot and mint reference; the `shareholders` vector is Borsh-decoded **per token, on demand, in Python** for the gate sample only, from the exported rows of those tokens — a few hundred KB, not a window-wide export. As-of-T = last config call at or before T (MR-4 R7).

## S7b extraction validated on one day (2026-09-28) — `recon/sql/s7b_sharing_config_day.sql`
Raw `create_fee_sharing_config` / `update_fee_shares(_v2)` calls filtered by discriminator; args Borsh-decoded in Python with the pinned `pump_fees.json`: `shareholders = [{address, share_bps}]` (every sample row a single 10,000-bps shareholder). The mint is in `account_arguments` (position per the IDL's account list). **Cost: 11.6 credits for one day even with `LIMIT 20`** — the scan reads the pump-fee partition regardless. Window extraction ≈ 611 × 11 ≈ **~7,000 credits: a paid-month materialization line**, not a readiness run.
