# Readiness 1.5 — S6 holder reconstruction (in progress, 2026-09-28)

**Test mint:** `7C5mqYVj…` (P, created 2026-08-27). Full SPL ledger from `tokens_solana.transfers` (`recon/sql/s6_holders_one_mint.sql`), net balance per owner.

**Reconciliation to chain supply: exact.** Per-action totals (`s6_actions_one_mint.sql`): `mint` 2 rows = 2,000,000,000,000,000; `burn` 2 rows = 593,902,297; `transfer` 423 rows. Chain `getTokenSupply` = **1,999,999,406,097,703** = mint − burn **to the lamport**. The ledger is complete for this mint.

**Correction, recorded because it was briefly committed as a finding:** an earlier version of this file claimed the curated table "records the mint twice" against a "nominal 1B supply". The supply is 2B; the token was minted twice by its creator. The table was right and the assumption was wrong. Rule that survives: **minted supply is never assumed from the launchpad's convention — it is read from the chain and reconciled against the ledger's `mint − burn`; a mismatch > 0.5% is the UNKNOWN condition.**

**Ledger result (top owners by net balance):** pool vault `CYZkgb…` 1,999,160,558,541,364 (~99.96% of supply), `27HFmP…` 838,117,412,742, `5uH885…` 730,143,597. Reconciliation of these against `getTokenLargestAccounts` (owner-resolved) is pending an RPC that rate-limits the heavy call; result appended when it lands.

**S6 definition as it will be coded:** top-10 holders by owner, excluding the pool vault (from `createpoolevent`), the bonding-curve PDA, known burn addresses, and owner wallets (`recon/owner_wallets.py`). Threshold 40% pre-registered, sensitivity at 30/50. Denominator = chain supply at entry (mint − burns to that slot). UNKNOWN when ledger net ≠ chain supply by > 0.5%.

## Chain reconciliation — done 2026-09-28 (`recon/out/s6_chain_owner_balances_7C5m.json`)
`getTokenLargestAccounts` is blocked on the public RPC under load, so the reconciliation used `getTokenAccountsByOwner` for the ledger's top three owners:

| owner | ledger net balance | chain balance now | diff |
|---|---|---|---|
| pool vault `CYZkgb…` | 1,999,160,558,541,364 | 1,999,160,558,541,364 | **0** |
| `27HFmP…` | 838,117,412,742 | 838,117,412,742 | **0** |
| `5uH885…` | 730,143,597 | 730,143,597 | **0** |

Supply reconciled earlier to the lamport. **Holder reconstruction from `tokens_solana.transfers` is validated end to end on this mint**; the as-of-entry cut is the same ledger truncated at the entry slot (used for S7 above).
