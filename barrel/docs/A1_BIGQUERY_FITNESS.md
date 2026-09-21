# BARREL M0 — A1: is BigQuery public Solana fit to be the M0 source?

**By:** ClaudeCode · **Date:** 2026-09-21 · **Orders:** H3 Amendment v1.1, orders 2–4
**Billing project:** `project-1602caf0-d9ea-4ac6-b0b` · **Spent this session:** ~45 GB billed (~4% of the free 1 TiB). Every query was dry-run first, and all were under the 50 GB confirm line.
**Verdict: it passes the freshness and coverage checks, and FAILS as a source for swap-level data.** The derived trade table ratified in R4 / MR-4.5 cannot be built from this dataset within any budget ruled so far. **Per order 5, work is stopped pending a ruling.**

---

## 1. Order 2 — freshness and coverage (v1.1 A1 step 1): PASS, with one flagged span

| Check | Result |
|---|---|
| Freshness | Newest block **0 minutes** behind head (2026-09-21 ~22:25 UTC). Limit: 48 h. **PASS** |
| Missing days | None. 629 of 629 calendar days from 2025-01-01 → today |
| Duplicate / overlapping blocks | None |
| Gate 0 weekly gap ≤ 5% | **PASS on a strict upper bound.** Chain-skipped slots are counted as dataset loss, and the worst of 90 weeks is still ≤ 3.65% (the week of 2025-08-04) |

**Flag — August 2025 is degraded.** Checked against the chain (`getBlocks`), the dataset is
missing **10.2% / 5.5% / 8.0% / 10.6%** of blocks on **2025-08-06 / 07 / 10 / 11**. The chain
itself filled 99.9% of slots on those days, so these are dataset holes, not chain skips.
2025-08-05 is also missing 0.9%, and the slot-span method didn't flag it, so that method
under-detects. Recommendation: label that span DEGRADED, flag tokens that graduate or trade
inside it, and report results with and without it. Whether partial-day loss counts as a
"multi-day hole" under A1 is **a ruling, not a builder call.**

Evidence: `recon/out/a1_daily_block_coverage.csv` (every day, every column).

## 2. Orders 3–4 — what the dataset actually contains, established by direct test

One direct PumpSwap transaction and one routed through Jupiter, both captured live, were
looked up in BigQuery and compared against the chain's own copy (`getTransaction`).

| Needed for M0 | Where it would have to come from | Test result |
|---|---|---|
| Direct swaps (PumpSwap as top-level instruction) | `Instructions` by `program_id` | **Present.** 29.3M rows on 2026-09-01, consistent with the live rate. Cheap: about 1.5–4 GB/day, because clustering on `program_id` prunes |
| **Routed swaps** (PumpSwap called inside Jupiter; 22–30% of flow in samples) | `Instructions` | **ABSENT.** The routed tx has 6 rows (ComputeBudget, token-account setup, Jupiter, Token) and **zero PumpSwap rows**. On the chain it calls PumpSwap 6 times. `parent_index` is null on every row checked (7.5M rows). **`Instructions` holds top-level instructions only** |
| **Swap events** (executed amounts, reserves, every fee leg) | `Instructions`, or `Transactions.log_messages` | **ABSENT from both.** PumpSwap emits events as `Program data:` log lines (verified on chain), not as self-CPIs. `log_messages` for a verified transaction is **a single empty string**, while the chain holds 75 lines including the `SellEvent`. **0 of 188,190** PumpSwap transactions in a 10-minute window carry any `Program data:` line |
| Token movements inside swaps; holder tables for S6/S7 | `Token Transfers` | **ABSENT.** Zero rows for both test transactions. On the chain they carry 1 and 10 inner SPL transfers |
| Executed amounts via balances, direct **and** routed | `Transactions.pre/post_token_balances` | **Present**, along with `balance_changes`, `fee` and the in-slot `index`. **But:** no filter prunes it except a *literal* signature (9.6 MB). A signature *join* does not prune: it measured **34.9 GB billed for a 10-minute window**. Reading balances for a day is **248 GB** (dry run) |

## 3. Projected cost, as order 4 asked, for both feasible designs

| Design | What it contains | Projected cost (P window + 60-day lead-in, ~610 days) | Within rulings? |
|---|---|---|---|
| **A. Top-level only** (`Instructions` by program id) | Direct swaps' **instruction arguments** (limits such as `min_amount_out`), **not executed amounts or prices**; no routed swaps, no fees, no holders | ~1–3 TiB, **~$5–20** | Budget yes. **Unfit:** it can't price a trade, compute PnL, or build a holder table |
| **B. Full balances** (`Transactions` pre/post token balances, content-filtered by day) | Every swap, direct and routed, with executed amounts by owner and in-slot order; fee legs are *not* broken out, only their sum | ~150 TiB, **~$900+**. Early-2025 days are smaller, so this is an upper-leaning estimate | **No.** About 6× the $150 cap, and it's an unclustered `Transactions` scan, which MR-3.1 forbids |

Neither design fits. A is affordable but can't answer H1–H4. B would work but is roughly
six times over budget and against the standing rule.

## 4. Records this overturns (corrected here, originals left in place)

1. **MR-2.3 said dry runs report the unclustered upper bound.** Wrong. On literal
   clustered-column filters, dry runs already include pruning, and billed matched processed
   to within 0.02% on every query run. What dry runs can't show is *dynamic* pruning through
   joins, and none happened: the join billed its full estimate.
2. **MR-3.3 ratified pricing §5 from swap events "via BigQuery `Instructions`".** That rested
   on the builder's assumption that events were in `Instructions`, which is **false**, and
   they aren't in `Transactions` either. The *principle* (price from fees actually charged)
   stands. The *source* it named cannot provide it.
3. **The fee-leg decoder (`decode_pumpswap_events.py`) and every earlier probe requested
   `maxSupportedTransactionVersion: 0`.** Mainnet now carries **version-1 transactions**,
   which those requests fail on. That's where the unusually high "RPC failures" in the last
   fee runs came from (**72 of 312**). They weren't random dropouts: the fee samples silently
   excluded a whole transaction version. Fixed in all four scripts, but the published fee
   numbers (`M0_FEE_LEGS.md` Part 2) were drawn from **version-0 transactions only**, and
   that caveat now applies to them.

Also established, and not an error: **PumpSwap emits events on the log path**
(`Program data:`). That makes the forgery check added to the decoder (accept log events
only inside a PumpSwap invocation frame) **load-bearing, not precautionary**.

## 5. What this means, and the decision it returns to Mando

v1.1 A1's fallback was written for "freshness fails or the window has holes". Neither
happened. What happened is the [E6] case: **a convenience layer that silently excludes most
of what is needed.** It has no inner instructions and no logs, yet the tables look complete
and return no errors. D1 therefore comes back for a ruling on real numbers:

* **Option 1 — Dune (the original D1 fallback).** Its decoded Solana tables are built from
  full transaction data, including inner instructions, which is the property BigQuery lacks.
  It needs the same verification BigQuery just got (E4: calibrate before wiring), starting
  with one known routed swap looked up by signature. That's cheap to do on Analyst ($75/mo).
* **Option 2 — BigQuery design B with a paid cap of roughly $900–1,000.** Complete data,
  but it requires lifting MR-3.1's unclustered-`Transactions` rule, and fee legs would come
  back as a sum, not by name.
* **Option 3 — BigQuery design B on the pre-defined sampled window** (one contiguous 90-day
  block per era, chosen before results, every cell labelled SAMPLED). About 270 days × 248 GB
  ≈ 65 TiB, ~$400. Still over the $150 recommendation, and it still needs MR-3.1 lifted.

**Builder's recommendation: Option 1, gated on the same routed-swap test.** It's the only
path that plausibly gives complete swap data under $150. The test that disqualified BigQuery
takes a few minutes to run against Dune before any commitment.

What BigQuery remains good for, and is kept for: block-level coverage and freshness checks,
top-level census counts, and cross-checking Dune's totals ([E6]: know what each layer drops).
