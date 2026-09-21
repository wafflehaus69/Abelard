# BARREL — H3 Wallet Discovery, Amendment v1.1 + Next Orders

**Responds to:** H3_WALLET_DISCOVERY_REVIEW.md (ClaudeCode, 2026-09-21) · **Amends:** H3_WALLET_DISCOVERY_v1.0.md
**Author:** Architect · **Date:** 2026-09-21

> Committed verbatim as relayed by Mando, 2026-09-21. The consolidated spec with these
> rulings applied is `H3_WALLET_DISCOVERY_v1.1.md` (builder edits, signed by this document).

Review accepted. Item 1 was an architect error — the two sentences cannot both hold and the builder read it correctly. Rulings per item, then orders.

## R1 — Surfacing tokens (BLOCKING → resolved)

The look-ahead sentence governs. For each candidate at T:

* Its surfacing token(s) are excluded from the §2 score and from the ≥ 15-distinct-tokens count. A wallet must show ≥ 15 other tokens in-window or it is dropped.
* For one rebalance date, report scores and the resulting top-30 both with and without the surfacing trades, so the size of the ranking bias is a number.

Rationale recorded as the builder stated it: the bias cannot manufacture an out-of-sample edge, but it fills the cohort with one-hit wallets, which is the exact failure step 3 exists to prevent.

## R2 — Dependency on X (v1.2)

Ratified: the §6 cost trial runs on a provisional X; its cohort is labelled PROVISIONAL and discarded; only bytes billed are kept.

Ratified: H3 evaluation excludes the calibration slice, same as H1. Any threshold set on the slice — rug X/Y/D, routing X, depth floor, cohort cut — contaminates every hypothesis evaluated on it. **Standing rule from here: the calibration slice is excluded from evaluation of every hypothesis, not just the one whose threshold was set there.**

## R3 — Undefined constants

* **Lot accounting: FIFO.** Ratified, so the backtest and the M1 tax ledger agree by construction.
* **Depth floor:** defined semantically now, valued in v1.2. An open position is marked at zero when its exit-adjusted value (full exit against pool depth at T) is below F% of its spot-marked value. Gate 0 reports the distribution of that ratio; F is set in v1.2 on the calibration slice. Provisional F for the cost trial only: 20%.

## R4 — Cost: derived trade table + budget

**Derived trade table: ratified, and elevated.** One pass over the P window plus 60-day lead-in extracts every DEX swap by program ID into a dataset in Mando's project; every rebalance date, and every H1/H2/H4 query, reads from it. This is consistent with A1 (raw tape stays server-side) and it is not just an H3 optimisation — it becomes the M0 data foundation and the seed of M1's live store. Schema to include: signature, slot, block_timestamp, in-slot tx index (see R5), program, pool, mint, side, wallet, gross/net amounts, each fee leg, era label, stratum label.

**Budget:** the free tier will not hold M0 under the builder's estimates, and the free-tier ceiling in MR-3.1 is therefore superseded conditionally — see Mando's decisions below. Architect recommendation: authorize paid scanning up to $150 for M0 (estimate $90–120 for the full P window; the trial replaces the estimate). If Mando rules free-only, fallback is a sampled window: one contiguous 90-day block per era, chosen before any results are seen, with every cell labelled SAMPLED and power reassessed. Do not budget off the estimates; the §6 trial reports bytes billed first.

## R5 — Sandwich test needs `Transactions`

Ratified as a line item in the §6 trial: signature-filtered `Transactions` read restricted to the candidate union, and `Transactions.index` is carried into the derived table for those rows. If the trial shows this leg dominates cost, the sandwich criterion becomes optional in v1 and bot-shaped is decided on hold time and token count alone; report which variant ran.

## R6 — Fame proxy confounded by heat

Ratified: fame = (distinct buyers in the 2 min after the wallet's buy) ÷ (distinct buyers in the 2 min before). Measures following, not heat. Diagnostic only.

## R7 — Minor

* Echo scope: comparing only against other candidates is deliberate; bot-shaped exclusion covers the shadow-a-bot case. Written into the spec.
* S7b built as of T from `SharingConfig` history, not today's state. Ratified; applies equally to S7b's use in RUG-A.

## Follow-ups the builder flagged on MR-3.2 (routing)

* "Deepest" = quote-side reserves at the entry block. Ratified.
* Exit when the entry pool has left the qualifying set: exit against the deepest pool that qualifies at the exit block; if no pool qualifies (token effectively dead), the position marks to zero. This is the same rule as the depth floor applied at exit time.

## Orders (ClaudeCode), in sequence

1. Apply R1–R7 to the H3 spec as v1.1 (builder edits, architect-signed by this document).
2. On receipt of the GCP project ID: free freshness and coverage check (A1 step 1), report.
3. §6 cost trial for one rebalance date on provisional X and F, including the R5 `Transactions` leg. Report bytes billed per leg, and the with/without-surfacing ranking comparison from R1. Cohort output discarded.
4. Extrapolate to the derived-table design (one pass, not per-date). Report the projected total and stop. Mando rules on the budget with real numbers.
5. Nothing else runs until step 4 is ruled.

## Decisions (Mando)

1. GCP project ID. The connector can't list projects; it has to come from you.
2. Dataset creation permission in that project, for the derived trade table. Recommendation: yes — it is the single largest cost reduction available and it becomes M1's foundation.
3. Paid-scan authorization. Recommendation: up to $150 for M0, ruled after step 4 returns real numbers. Free-only is a legitimate ruling; the sampled-window fallback is pre-defined above so it doesn't get chosen after seeing results.
4. Set a BigQuery budget alert yourself in the GCP console at whatever cap you choose. Hypothesis, flagged: paid scanning past the free tier requires a billing account attached to the project, and the alert is the only thing that stops a mis-estimated query from billing silently. Neither I nor ClaudeCode can set it; it must be you.

---

## Mando's response (recorded by ClaudeCode, 2026-09-21)

* **Decision 1 — project ID supplied:** `project-1602caf0-d9ea-4ac6-b0b`.
* Decisions 2, 3 and 4 — **not yet ruled.** Until they are, MR-3.1 stands unmodified:
  free 1 TiB this month, confirm-before-run above 50 GB (on the dry-run figure), no
  unclustered `Transactions`, and no dataset is created.
