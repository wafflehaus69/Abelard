# Readiness 1.5 — S3/S4 (Token-2022 extensions, transfer fee) as-of, reconciled 10/10 (2026-09-22)

**Sample:** five stratum-P graduates (4 Token-2022, 1 legacy SPL) and five Token-2022 stratum-N mints (CP-swap inits, 2026-09-01). **Chain:** `recon/out/s1s2_chain_now.json`, `s3s4_chain_now_N.json`.

## Method
Extensions are initialised at mint creation and cannot be removed, so **as-of entry = as-of now for presence**. Two sources on Dune:
* **Decoded tables with a mint column:** `initializepermanentdelegate`, `initializenontransferablemint` (`recon/sql/s3_decoded_10mints.sql`).
* **Decoded tables WITHOUT a mint column** (`transferfeeextension`, `transferhookextension`, `confidentialtransferextension` carry call metadata only): reconstructed from **raw `solana.instruction_calls`** to the Token-2022 program with the mint in `account_arguments`, classified by the **first data byte = Token-2022 instruction index** (`recon/sql/s3s4_raw_calls*.sql`). Index map used: 0 InitializeMint · 20 InitializeMint2 · 26 TransferFeeExtension · 27 ConfidentialTransferExtension · 32 InitializeNonTransferableMint · 35 InitializePermanentDelegate · 36 TransferHookExtension · 39 MetadataPointerExtension. Values ≥ 200 seen on P mints (210, 215) are the 8-byte discriminators of the **token-metadata interface**, not Token-2022 indices, and are classified as "interface instruction, not an extension init".

## Result

| Stratum | Mint | Raw-call extension indices (creation) | Decoded delegate / non-transferable | Chain now | S3 | S4 | Reconciled |
|---|---|---|---|---|---|---|---|
| P | `GfYX7XWm…` (legacy SPL) | n/a | none | no extensions possible | PASS | PASS | yes |
| P | `FBmPBhgQ…` | 39 | none | metadataPointer, tokenMetadata | PASS | PASS | yes |
| P | `AjdE84dG…` | 39 | none | metadataPointer, tokenMetadata | PASS | PASS | yes |
| P | `AaTwXAnM…` | 39 | none | metadataPointer, tokenMetadata | PASS | PASS | yes |
| P | `7C5mqYVj…` | 39 (create day 08-27) | none | metadataPointer, tokenMetadata | PASS | PASS | yes |
| N | `B6tBzcGX…` | 0, 39 | none | metadataPointer, tokenMetadata | PASS | PASS | yes |
| N | `D4LLPan8…` | 0, 39 | none | metadataPointer, tokenMetadata | PASS | PASS | yes |
| N | `DGojxfuX…` | 0, 39 | none | metadataPointer, tokenMetadata | PASS | PASS | yes |
| N | `GbRs1rEF…` | 0, 39 | none | metadataPointer, tokenMetadata | PASS | PASS | yes |
| N | `H2jDWfVq…` | 20, **26 ×3** | none | **transferFeeConfig, 500 bps** | PASS | **FAIL** | yes |

**First live discriminating case:** a 5% transfer tax on a venue-native mint, invisible to S1/S2, caught by S4 — in stratum N, where the S1/S2 validation predicted the gate's power would sit. On stratum P, pump.fun's `create_v2` attaches only the metadata pointer; S3/S4 are PASS by construction there as far as this sample shows.

## S4 rate as-of entry — a limit, stated
Presence is as-of by construction; the **rate is not**: `TransferFeeExtension` (26) has sub-instructions (second byte: 0 InitializeTransferFeeConfig, 5 SetTransferFee) and the fee can be raised after entry. The three index-26 calls on `H2jDWfVq…` are exactly that shape. The chain exposes only current and previous fee with epochs. For the window, the as-of rate must be decoded from the raw call's second byte and arguments — written as the next step, not done here. Until then S4 is evaluated on **presence and the chain's `olderTransferFee`/`newerTransferFee` pair**, and any entry between two fee changes older than that pair is UNKNOWN.

## UNKNOWN path
* Token-2022 mint with **no raw Token-2022 call referencing it** inside [creation date − 1 d, graduation + 1 d] → UNKNOWN (coverage gap). Strict FAIL, loose PASS, counted.
* Index-26 calls present but sub-instruction undecodable → S4 UNKNOWN on rate, FAIL on presence in strict.
* Legacy SPL mint → S3/S4 not applicable, recorded as PASS-by-construction with `token_program = legacy`.

## M0 rule for S4 (MR-7)
Rate = the rate at initialisation. If **any** TransferFeeExtension (index 26) call referencing the mint exists **after the entry slot**, the row is **FLAG/UNKNOWN** (strict FAIL, loose PASS). The sub-instruction decode of the as-of rate is an M1 refinement. `H2jDWfVq…`'s three index-26 calls all sit in its creation slot, so it is FAIL on rate (500 bps), not UNKNOWN.
