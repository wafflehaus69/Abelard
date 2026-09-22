# BARREL M0 — Dune Readiness Orders (pre-purchase)
**Author:** Architect · **Date:** 2026-09-22 · **Governs:** everything before the Dune Analyst purchase (MR-5.0)
**Rule:** no capital until every item in §1 is checked. The purchase day is the first execution day.

> Committed verbatim as relayed by Mando, 2026-09-22. Progress is tracked in
> `DUNE_READINESS_STATUS.md`, never by editing this file.

## 0. Expense doctrine (new, applies to all projects)
Every purchase carries, in writing before the money moves:
1. Deliverable — the named artifact it produces.
2. Kill criterion — the observable condition under which the spend is stopped or not renewed.
3. Decision fed — the specific ruling the deliverable enables.

For the Dune Analyst month:
* Deliverable: `census.md` (Gate 0 on stratum P) and `cost_trial.md` (§6 one-date trial, measured credits per unit).
* Kill criterion: by day 10 of the month, projected credits for the full P window exceed the allowance, and no sampled-window plan fits either → no renewal; M0 re-scoped.
* Decision fed: v1.2 (rug thresholds X/Y/D, routing X, depth floor F, cohort cut) and the H3 go/defer ruling.

## 1. Readiness checklist (all on the free tier, $0)
Every query below is written, dry-run, and validated on a ≤ 1-day sample before purchase. "Validated" means its output on the sample was reconciled against the chain or against an independent count, and the reconciliation is in the repo.

### 1.1 Environment
* [ ] Dune API key stored in the gitignored `barrel/private/`, key-leak guard extended to the Dune key pattern, guard run passes.
* [ ] Query authoring done as version-controlled `.sql` files in the repo, not only in Dune's editor. Dune's saved queries are a cache; the repo is the source.
* [ ] Credit meter: a script that reads remaining credits from the API before every run and refuses to start any execution projected to exceed the remaining balance minus a 15% reserve.

### 1.2 Universe (stratum P)
* [ ] Graduation-event query: every pump.fun → PumpSwap migration, with mint, pool, slot, timestamp, quote mint. Validated on one day against a chain-side count.
* [ ] Stratum labelling: P (SOL-quoted), P-alt (non-SOL-quoted, reported separately, no conversion), R, N. Era label (pre/post BOOST 2026-07-21). DEGRADED flag for 2025-08-05 → 08-11.
* [ ] Owner-wallet exclusion applied at the universe layer, reading addresses from `barrel/private/` only.

### 1.3 Derived trade table (materialized view spec)
* [ ] Schema frozen: signature, slot, block_timestamp, in-slot tx index, program, pool, mint, quote_mint, side, wallet, gross/net amounts, lp_fee, protocol_fee, coin_creator_fee, era, stratum. Nothing else.
* [ ] Built and validated as a plain query on one day. Materialized-view creation is deferred to the paid month (Analyst's MV cap is unknown until the allowance is read).
* [ ] Export contract: only aggregate rows leave Dune. A list of exactly which aggregates, with their expected row counts, so export credits are projected before any export runs.

### 1.4 Gate 0 census queries
* [ ] Launches/week, graduations/week, % of graduated tokens with ≥ 1 swap in 7 days, by stratum and era.
* [ ] Calibration-slice distributions (A3): reserve decay curves, aligned-cluster sell fraction days 0–7, exit-adjusted/spot ratio for the depth floor F, per-pool volume share for routing X.
* [ ] Twenty-token hand reconciliation list drawn (random, seeded, seed recorded) so it runs on day one.

### 1.5 Safety gate S1–S11 as-of reconstruction
* [ ] S1/S2 (mint/freeze authority as of entry block) reconstructed from instruction history on the sample; reconciled against `getAccountInfo` at a historical slot for 5 tokens.
* [ ] S3/S4 (Token-2022 extensions, transfer fee) same treatment.
* [ ] S6/S7/S7b/S8 holder and cluster queries written; S7b built as-of T from SharingConfig history.
* [ ] S9 wash flag and S10 retro-sellability written.
* [ ] Each check has a documented UNKNOWN path.

### 1.6 Cost model
* [ ] Fee-leg pricing from actual charged fees (MR-3.3 principle) sourced from Dune's decoded events, per era, SOL-quoted only.
* [ ] Fee samples re-run with the version-1 transaction fix; v0-only caveat removed from `M0_FEE_LEGS.md`.
* [ ] Slippage computed from pool reserves at entry block for a $20 buy and full exit; validated on 10 swaps against executed amounts.

### 1.7 §6 cost trial staged
* [ ] One rebalance date chosen (pre-BOOST era, outside the calibration slice, outside DEGRADED), seeded and recorded.
* [ ] Full funnel (winners → early participants → candidate replay → scoring with and without surfacing trades) written and dry-run on the free tier against a 2-day window.
* [ ] Credit projection template ready to be filled with measured units.

### 1.8 Personal-history replay (separate deliverable)
* [ ] Query written against owner wallets; runs entirely from `barrel/private/`; output goes to `barrel/private/` only. Produces Mando's manual-era expectancy net of every ticket, FIFO lots, and seeds the ledger schema.

## 2. Purchase-day sequence (day 1 of the paid month)
1. Mando reads the Analyst credit allowance off the purchase screen → ClaudeCode re-projects → Mando authorizes → purchase.
2. Credit meter armed. Materialized view created; its credit cost measured and logged.
3. §6 cost trial runs first. Credits per unit measured, full-window projection issued, stop and report.
4. Architect and Mando rule: proceed with full Gate 0, sampled window, or kill. Nothing else runs before that ruling.

## 3. Free-tier ledger
ClaudeCode maintains `credits.md`: every free-tier execution, credits consumed, cumulative. Target: readiness complete inside the remaining ~2,400 free credits. If the free tier runs out before §1 is done, that is reported, not worked around by buying early.

## 4. Mando's pre-purchase actions
* [ ] Confirm no billing account is attached to the GCP project (MR-5.3).
* [ ] Set the GCP budget alert anyway.
* [ ] Do not buy Dune Analyst until ClaudeCode reports §1 complete and names the day.
* [ ] On purchase day: read and relay the credit allowance and the materialized-view cap before completing checkout.
