# BARREL — Trial Burn-Down Orders
**Author:** Architect · **Date:** 2026-09-30 · **Deadline:** trial ends 2026-10-06 (account goes view-only)
**Purpose:** convert the ~1,199 remaining trial credits into measured numbers so checkout is a decision, not a projection. Nothing here is analysis; everything is calibration of cost and design.

> Committed verbatim as relayed by Mando, 2026-09-30. Execution status is tracked at the end of this file, below the orders, never by editing them.

## 0. Ground rules
* Watchdog on for every run; `--expect > 25` still requires a one-partition run first.
* Hard reserve: 180 credits (15%) untouched. If the balance hits 180, stop regardless of what's mid-list.
* Every result exported to the repo the same day it's produced. On Oct 6 the warehouse is view-only; anything not in the repo is gone.
* Items run in the order below. A later item never pre-empts an earlier one.
* Nothing under H3 runs. H3 is M0b.

## 1. Fee pricing per era (1.6 — the last open §1 item) — budget ≤ 60 credits
Actual charged fee legs from decoded PumpSwap events, SOL-quoted pools only, one bounded sample day per era (pre-2026-01-10, 2026-01-10 → 2026-07-21, post-BOOST). Output: fee-leg distribution per era, into `M0_FEE_LEGS.md` Part 4. Closes §1 complete-as-amended.

## 2. Per-token aggregate schema — 0 credits
Draft the frozen column list for the M0 foundation table (per mint × era): entry marks at G ∈ {15, 60, 240}; price and reserves at 1h / 4h / 24h / 7d; peak and max drawdown from each G; gate verdicts S1–S11 as-of entry with UNKNOWN paths; aligned-cluster net flow days 0–7 (RUG-A); syndicate cluster size, common funder, first-distribution time (H2); organic-v1 and v2 taker counts; cost legs and bot-layer markup at each G; stratum, era, degraded, admission-source, birth_is_proxy flags. Send to Architect for sign-off before item 3 runs; item 3 materializes this schema, so the schema is frozen first.

## 3. Measure the materialization unit — budget ≤ 400 credits total
This is the number the whole purchase decision rests on.
* 3a. Per-token aggregate, one week of P (a week inside the calibration slice, not DEGRADED): materialize it, record (i) credits to build, (ii) stored bytes, (iii) credits for one full-table read, (iv) credits for one export of the result rows. Budget ≤ 250.
* 3b. Per-swap slice, one day of P: same four measurements. Budget ≤ 150.
* Extrapolate both to the full P window (~180k tokens, ~610 days) and to the two bounded slices (cost-trial 60-day window, calibration slice). Report against Analyst's 4,000 credits/month and the storage cap once it's known.
* Confirm from Dune's docs or support whether materialized views survive a plan change (trial → Analyst). Hypothesis: they don't; treat as disposable until confirmed.

## 4. Calibration-slice distributions, if credits remain — budget ≤ 250 combined
Run against the week materialized in 3a, not against raw tables:
* 4a. Aligned-cluster sell fraction days 0–7 (the RUG-A input). ≤ 150.
* 4b. Venue share per token (routing X input). ≤ 100.
Both outputs are distributions only; no thresholds chosen. Into `VALIDATION_A3.md`.

## 5. Re-projection and checkout recommendation — 0 credits
Before Oct 6, deliver `PURCHASE_DECISION.md`: measured units from item 3, projected credits for M0 ex-H3 by phase (materialize P, Gate 0, H1/H2/H4 passes, exports), months of Analyst required, the kill criterion restated against those numbers, and one line: buy / don't buy / buy with a scope cut. Architect reviews; Mando decides.

## 6. Explicitly not run on the trial
* §6 cost trial (H3 funnel) — M0b.
* Personal-history replay — paid month, against the per-token table plus a scoped per-swap slice for the owner wallets.
* S7b window extraction — paid month, first item after the P materialization.
* Any full-window scan.

## Mando
* Nothing to buy until item 5 arrives.
* GCP billing check still open.

---

## Execution status (ClaudeCode)
* **Item 1 — DONE 2026-09-28, 1.4 credits** (under the 60 budget), before these orders arrived: `M0_FEE_LEGS.md` Part 4, three eras. It also corrected Part 2: creator fees were near-universal (~82 bps median) in October 2025. §1 is complete-as-amended.
* **Mando / GCP billing — CLOSED 2026-09-30:** no billing account is attached (confirmed by Mando; `DUNE_READINESS_STATUS.md` §4). With no account, usage past the free tier fails rather than bills, and no budget alert can exist.
* **Item 2 — drafted 2026-09-30, sent for sign-off** (`PER_TOKEN_SCHEMA.md`, **78 columns**, 0 credits; the previous commit message said 76, which was a miscount). Items 3–4 wait on it.
* **Item 3 prerequisites found in Dune's docs (0 credits), before any spend:** (a) creating a materialized view or a saved query needs a **Read/Write** API key, and the key on file is Read, so Mando must create one; (b) a view requires a refresh cron (15 min to weekly, each refresh billed), so views are created weekly with `expires_at` and deleted once measured; (c) **Analyst storage is 1 GB**, shared by views and uploads: the per-token table fits, but per-swap slices beyond about one day do not; (d) on Analyst, saved queries (and therefore views) are **public**, so owner wallets never enter a saved query; (e) whether views survive trial → Analyst is undocumented, so they are treated as disposable.
* **Balance at order receipt:** 1,301.4 used / 2,500; 1,198.6 remaining; hard reserve 180 leaves **1,018.6 spendable**. Items 3–4 budget 650.
* **Sign-off received 2026-09-30** (`RULINGS_2026-09-30.md`). **Order 1 DONE:** schema frozen at 85 columns under neutral names (`PT_FEATURES_SCHEMA.md`; mapping in `recon/pt_features_schema.json`, generated by `recon/gen_pt_features_schema.py`). Item 3a ≤ 250 plus one measured refresh; 3b cut to ≤ 100; item 4 only if ≥ 400 credits remain above reserve. **Waiting on Mando:** the Read/Write key (`DUNE_API_KEY_RW=`) and the public-table ruling (Architect recommends accept).
