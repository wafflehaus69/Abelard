# BARREL M0 — A1: Dune, known-transaction round-trip ([E34])

**By:** ClaudeCode · **Date:** 2026-09-22 · **Orders:** MR-5 orders 2–3
**Tier:** Free. **Credits consumed, all runs: ~36.9 of 2,500.** No money spent.
**Verdict: PASS on every item BigQuery failed.** Order 3 applies: projection below, then stop.

Reproduce, in order: `dune_roundtrip.py`, `dune_roundtrip_followup.py`, `dune_meta.py`,
`dune_probe_columns.py`, `dune_decoded_check2.py`, `dune_quote_mix.py` (all under
`recon/`, all keyless except for `DUNE_API_KEY` in the gitignored `barrel/.env`).
Raw results with execution IDs and Dune-reported credits: `recon/out/dune_*.json`.

---

## 1. Specimens

Ground truth was fetched live from the chain for every specimen, never reused.

| Specimen | Boundary it exercises | Chain: PumpSwap outer/inner, events, SPL transfers |
|---|---|---|
| `direct_2026-09-21` | plain top-level swap | 1/1, 1, 4 |
| `routed_2026-09-21` | Jupiter-routed; PumpSwap only as inner CPI | 0/6, 2, 11 |
| `direct_2026-09-01` | three weeks old | 1/1, 1, 4 |
| `v1_swap` (2026-09-22) | version-1 transaction, contains a swap | 0/2, 1, 5 |

## 2. Results per item

| Item | Table | Result |
|---|---|---|
| **Routed swaps** | `solana.instruction_calls` | **PASS 3/3.** Routed specimen: 0 outer / 6 inner, exactly as on chain. This is the item BigQuery failed. |
| **Swap events in logs** | `solana.transactions.log_messages` | **PASS 3/3.** `Program data:` lines present at chain count or above. BigQuery's copy was an empty string. |
| **Swap events decoded, fee legs by name** | `pumpdotfun_solana.pump_amm_evt_{buy,sell}event` | **PASS 4/4, field-for-field.** Dune's `lp_fee`, `protocol_fee`, `coin_creator_fee` equal this repo's independent IDL decode of the chain's bytes, on every specimen including both events inside the routed swap and the version-1 swap. |
| **Inner SPL transfers, row-level** | `tokens_solana.transfers` | **PASS on the fully-ingested specimen** (09-01: 4 chain rows, 0 missing, 3 extra). The 09-21 specimens are on a partition the curated table has only partly reached (see §3). Not a completeness failure; re-check below. |

Also present and decoded, with columns confirmed from the pinned IDL:
`pump_evt_createevent`, `pump_evt_completeevent`, `pump_evt_completepumpammmigrationevent`
(the graduation event), `pump_evt_tradeevent`, `pump_amm_evt_createpoolevent`. Gate 0's
census can be written against these directly.

## 3. Findings that shape the design

**3a. Curated tables lag raw tables by up to a day.** Newest partition on 2026-09-22:
`solana.instruction_calls` = 09-22, `tokens_solana.transfers` = 09-21, and the 09-21
partition was still partial at 01:30 UTC. Irrelevant for a historical backtest; relevant
for M1's live store. Re-check to close: rerun `dune_meta2.py` after 2026-09-23 and expect
the 09-21 specimens to pass with 0 missing.

**3b. Dune's decoder pins an older IDL.** `pump_amm_evt_buyevent` stops at the 473-byte
layout: no `buyback_fee`, `cashback`, `holder_rewards`, `virtual_quote_reserves`. The three
additive legs are all present, which is what trader cost needs. Anything from the newer
fields would have to come from a raw-log decode.

**3c. Not every pool is SOL-quoted, and the non-SOL pools carry half the flow.** On
2026-09-01: 21,089 pools (82%) SOL-quoted with 12.9M events; 4,761 pools (18%) quoted in
another mint with 13.4M events. Summing `quote_amount` across both produced "288 billion
SOL/day"; SOL-quoted alone is 4.18M SOL/day, which is plausible. This was the builder's
error, not a data defect: `quote_amount` is in the pool's quote-mint units.
**Rule for v1.2: carry `quote_mint` everywhere; stratum P and the SOL-denominated cost model
use SOL-quoted pools, or a ruled conversion.** pump.fun's `CreateEvent` also carries
`quote_mint`, so non-SOL bonding curves exist too.

**3d. Fee proportions hold across 26M events.** With the SOL-quoted filter, LP is exactly
25.0 bps of quote and protocol exactly 5.0 bps on the whole day, both sides. A structural
check on the decoded table that passes at scale.

## 4. Cost, measured units and a projection (order 3)

**Measured on the free tier** (Dune-reported `execution_cost_credits`):

| Unit | Credits |
|---|---|
| One full day, all decoded swap events, aggregated over ~5 columns | **0.52 – 0.75** (three measurements) |
| Signature lookup across 1–3 days with `tx_id IN (...)`, decoded table | 0.78 |
| Same lookup in `solana.instruction_calls` (raw, 3 days) | 14.8 |
| Metadata queries | ≤ 0.07 each |

**Export pricing, documented:** 1 credit = 1,000 datapoints on Free and Analyst; 1 credit =
5,000 on Plus.

**Consequence, by arithmetic:** 26M swaps/day × 15 columns ≈ 390,000 credits per day to
export row-level. **The derived trade table (R4 / MR-4.5) cannot be exported from Dune at
this scale on any plan. It lives inside Dune as a materialized view, and only aggregates
leave.** That is the R7 rule enforced economically. Materialized views are referenced as
`dune.<user>.result_<name>`; storage is capped per plan (Free 1 MB, Plus 15 GB/month,
**Analyst not documented**).

**Projection**, labelled: extrapolated from the units above, over the P window plus 60-day
lead-in (~611 days). The row-level unit is **unmeasured** and is the first thing to measure
once a month is bought.

| Work | Basis | Estimate |
|---|---|---|
| Gate 0 census, window-wide daily aggregates, 3–5 passes | 0.6 × 611 per pass | **~1,100 – 1,900** |
| Derived table materialization, one pass, SOL-quoted graduate pools only | 2–5× the aggregate unit, a guess | **~750 – 1,900** |
| H1/H2/H4 passes over the materialized table | several passes at a fraction of the above | ~500 – 1,500 |
| Per-token aggregate **export** | ~150–300k tokens × 30 cols ÷ 1,000 | **~4,500 – 9,000** |
| H3 funnel | its own §6 trial; not projected here | — |

Two things follow. **The free tier (2,500/month) cannot hold M0.** And **export, not
compute, is the largest line item** — so the recommendation is to compute the H1–H4
verdicts inside Dune and export only the result grid and the per-token rows the reports
actually cite, which collapses that line to a few hundred credits. Whether Analyst's
allowance covers the rest is unknown to the builder: **its monthly credits are not on any
page that can be fetched here; Mando sees the figure on the purchase screen.** Two months
of Analyst ($150) is inside the cap already recommended.

## 5. What this returns to Mando

Per MR-5.0, no spend without authorization. The decision is: **buy one month of Dune
Analyst ($75)** as the scoping month, with its first job the §6 cost trial (one rebalance
date, row-level unit measured, bytes and credits reported) before anything else runs.
Please note Analyst's monthly credit figure from the purchase page so the projection above
can be checked against it.
