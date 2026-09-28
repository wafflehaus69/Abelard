# Readiness 1.5 — S6 holder reconstruction (in progress, 2026-09-28)

**Test mint:** `7C5mqYVj…` (P, created 2026-08-27). Full SPL ledger from `tokens_solana.transfers` (`recon/sql/s6_holders_one_mint.sql`), net balance per owner.

**Finding — the curated table records the mint twice.** Per-action totals (`s6_actions_one_mint.sql`): `mint` **2 rows, 2.0e15** against a nominal pump.fun supply of 1.0e15 (1B × 10⁶); `transfer` 423 rows; `burn` 2 rows, 5.9e8. The naive reconstruction therefore gave the pool ~2× supply. **Rule:** mint rows are de-duplicated per (tx, amount) — or, cleaner, minted supply is taken from the chain's `getTokenSupply` plus burns, and `transfers` supplies only the `transfer` action. Reconciliation against `getTokenLargestAccounts` (owner-resolved) is pending an RPC that is currently rate-limiting the heavy call.

**S6 definition as it will be coded:** top-10 holders by owner, excluding the pool's own vault (`createpoolevent` accounts), the bonding curve PDA, known burn addresses, and — per MR-3.4 — owner wallets (`recon/owner_wallets.py`, read from `barrel/private/`). Threshold 40% pre-registered, sensitivity at 30/50. UNKNOWN when the ledger's net supply disagrees with chain supply by > 0.5% after the dedupe rule (coverage gap in `transfers`), strict FAIL / loose PASS.
