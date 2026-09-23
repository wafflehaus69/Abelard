# Readiness 1.5 — S1/S2 (mint / freeze authority as-of entry), reconstructed and reconciled (2026-09-22)

**Query:** `recon/sql/s1s2_authority_history.sql` — every `initializeMint2` and `setAuthority` on the mint, from **both** token programs (`spl_token_solana`, `spl_token_2022_solana`), bounded from the mint's pump `CreateEvent` date to graduation + 1 day. **Chain:** `recon/out/s1s2_chain_now.json` (`getAccountInfo`, authorities and extensions only; no metadata fields).

## Method, stated plainly
A public RPC cannot read account state at a historical slot (R8). The as-of state is therefore **reconstructed**: initial authorities from `initializeMint2`, then every `setAuthority` at or before the entry slot applied in slot order (`newAuthority = NULL` is a revocation). The reconstruction is then **reconciled against today's chain state under one condition: no `setAuthority` on the mint after the latest entry lag**. When that holds, today's state *is* the as-of state and the comparison is exact. When it does not, the token is flagged and the comparison is not claimed. This is not a historical-slot read and is not presented as one.

## Result: 5 / 5 reconciled
| mint | program | create slot | events | mint auth after create tx | freeze auth initial | as-of entry (G=15/60/240) | chain now | setAuthority after entry? | S1 | S2 | reconciled |
|---|---|---|---|---|---|---|---|---|---|---|---|
| `GfYX7XWm…` | SPL-Token | 443282872 | 1 init + 1 setAuth (same tx: True) | None | None | None/None, None/None, None/None | None/None | no | PASS | PASS | yes |
| `FBmPBhgQ…` | Token-2022 | 443283131 | 1 init + 1 setAuth (same tx: True) | None | None | None/None, None/None, None/None | None/None | no | PASS | PASS | yes |
| `AjdE84dG…` | Token-2022 | 443439092 | 1 init + 1 setAuth (same tx: True) | None | None | None/None, None/None, None/None | None/None | no | PASS | PASS | yes |
| `AaTwXAnM…` | Token-2022 | 443553744 | 1 init + 1 setAuth (same tx: True) | None | None | None/None, None/None, None/None | None/None | no | PASS | PASS | yes |
| `7C5mqYVj…` | Token-2022 | 442020435 | 1 init + 1 setAuth (same tx: True) | None | None | None/None, None/None, None/None | None/None | no | PASS | PASS | yes |
**Structural finding, now measured rather than assumed (R4 was n=1):** pump.fun's `create` initialises the mint with `freezeAuthority = NULL` and, **in the same transaction**, sets `MintTokens` authority to `NULL`. So for every pump.fun-created mint, S1 and S2 are PASS at every entry lag by construction, and both checks carry zero discriminating power on stratum P. Their power lives in strata R (same mechanism, same result expected) and N, where a venue-native mint can keep either authority live. Gate results must report S1/S2 pass rates per stratum so this is visible rather than averaged away.

## UNKNOWN path (required by 1.5)
* No `initializeMint2` row for the mint in either program's table within [create date, graduation + 1 d] → **UNKNOWN** (decoded coverage gap or a mint created by a path Dune has not decoded). Strict: FAIL. Loose: PASS. Counted and reported.
* A `setAuthority` **after** the latest entry lag → as-of state is reconstructed as usual, but the chain reconciliation is **not claimed** for that token; flagged `post_entry_authority_change`.
* `authorityType` outside {MintTokens, FreezeAccount} (e.g. AccountOwner, CloseAccount) is ignored for S1/S2 and logged.

## S3/S4 groundwork from the same sample
4 of 5 mints are **Token-2022**, carrying only `metadataPointer` + `tokenMetadata`. Dune's `spl_token_2022_call_transferfeeextension` decodes **no arguments** (call metadata only), so the S4 fee *rate* comes from the chain (`transferFeeConfig` on the mint). A transfer-fee config is set at mint initialisation and cannot be removed, so as-of-entry equals as-of-now for S4; `transferHook`, `permanentDelegate`, `nonTransferable` likewise are initialisation-time extensions and have dedicated decoded call tables (`transferhookextension`, `initializepermanentdelegate`, `initializenontransferablemint`) for the as-of check.

## Differential acceptance test (MR-7) — stratum N, post-entry revocations

Candidates: stratum-N mints (CP-swap inits, Aug 2026) with a `setAuthority` on MintTokens or FreezeAccount **after** pool init + 240 min (`recon/sql/s1s2_differential_candidates.sql`). Seven found. For each: full history from both programs (`s1s2_differential_history.sql`), reconstruction at the entry slot must show the **pre-change authority still set**, reconstruction after must equal **today's chain state** (`s1s2_differential_chain_now.json`). A pass here cannot be produced by reconciliation-to-current-state alone.

| mint | program | init found | entry slot (+240 min) | state at entry (mint / freeze) | post-entry changes | state after | chain now | differential |
|---|---|---|---|---|---|---|---|---|
| `p4UmanYq…` | SPL-Token | yes | 437105621 | 3xTeG8… / None | mint→None | None / None | None / None | PASS |
| `2fEvrJjY…` | SPL-Token | yes | 437478705 | YTnFYa… / None | mint→None | None / None | None / None | PASS |
| `AWdvQXYA…` | SPL-Token | yes | 437690838 | 2S7Rz7… / None | mint→None | None / None | None / None | PASS |
| `ALr4PvU7…` | Token-2022 | yes | 437920525 | None / 6tJwEX… | freeze→None | None / None | None / None | PASS |
| `8qcRRjnN…` | Token-2022 | yes | 437625692 | 51FP99… / 51FP99… | freeze→None, mint→None | None / None | None / None | PASS |
| `REDUTAE1…` | Token-2022 | NO | 436798154 | None / None | mint→None, freeze→None | None / None | None / None | FAIL |
| `HUvgiKD7…` | SPL-Token | yes | 439316442 | 5Mk5RT… / 5Mk5RT… | freeze→None, mint→None | None / None | None / None | PASS |

**6/7 PASS**
