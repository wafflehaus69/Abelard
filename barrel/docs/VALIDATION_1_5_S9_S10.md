# Readiness 1.5 — S9 (wash flag) and S10 (retro sellability), one day (2026-09-28)

`recon/sql/s9_s10_day.sql` on the 1,154 stratum-P graduates of 2026-09-01, first 24 h / 30 min after graduation. 2.8 credits.

| Check | Rule as pre-registered | Result on the day |
|---|---|---|
| S9a | 24 h volume ÷ distinct takers above the 95th percentile of the universe | p95 = 20.4 SOL per taker; **59 flagged** (by construction ~5%) |
| S9b | > 30% of 24 h volume from wallets that both bought and sold within 24 h | round-trip share **p50 0.89, p90 0.99; 1,116 / 1,154 flagged** |
| S10 | no successful sell by a non-creator, non-migrator wallet in the first 30 min | **1,151 sellable, 3 not** |

**Findings.** S9b as written is vacuous: day-one volume on nearly every graduate is round-trip by construction (bots buy and sell within the day), so a 30% threshold separates nothing. It is an [E8] case — a constant set before its distribution — and is referred to the calibration slice for re-registration in v1.2 (candidates: a *higher* cut set at a slice percentile, or same-wallet round-trips inside a much shorter window). S10 is near-vacuous, as §4 anticipated ("the weakest check"); its three failures are worth reading individually since they are the only ones. S9a is a relative rule and behaves as designed.

## UNKNOWN paths (S5–S10)
* **S5:** LP mint's transfer/burn history absent from `tokens_solana.transfers` for the pool's lifetime → UNKNOWN; LP held by a program not in the decoded locker list (`VALIDATION_1_5_S5_S7B.md`) → UNKNOWN.
* **S6:** ledger `mint − burn` ≠ chain supply at entry by > 0.5% → UNKNOWN (coverage gap in transfers).
* **S7:** creator not found in `createevent` for the mint → UNKNOWN; `sol_transfers` window for the creator returns no rows at all (not even the creation-fee transfers) → UNKNOWN.
* **S7b:** no sharing-config call for the mint before T and the mint post-dates the fee program → not applicable (PASS, logged); an unknown fee-program discriminator on the mint → UNKNOWN.
* **S8:** creation slot has no `tradeevent` rows (decoded gap) → UNKNOWN; buyers present but zero `sol_transfers` rows for all of them in the prior 24 h → UNKNOWN on funding, S8 = no on count alone, flagged.
* **S9:** fewer than 5 distinct takers in 24 h → S9a/S9b not computed, UNKNOWN (too thin to characterise).
* **S10:** no sell events at all in 30 min → FAIL by definition, not UNKNOWN; missing `createevent` (creator unknown) → the migrator-only exclusion applies and the row is flagged `creator_unknown`.
