# BARREL M0 — Paid-Month Launch Runbook (Architect's launch order, verbatim)

> Committed verbatim as relayed by Mando on 2026-10-01. Recorded in `M0_TASKING.md` as **MR-13**. The builder's operational version is `RUNBOOK_v1.md`. Seeded selections that this document requires to be recorded before anything runs are in the section after the verbatim text.

# BARREL M0 — Paid-Month Launch Runbook
**Author:** Architect · **Date:** 2026-10-01 · **Status:** architect's launch order; ClaudeCode fills in the operational detail (query names, chunk list, daily ledger) and returns it as `RUNBOOK_v1.md` before purchase.
**Plan assumed:** Analyst, month-to-month, 4,000 credits/month, two months authorized (8,000). If the Plus rule from RULINGS_2026-10-01 fires at checkout, the same phases run uncut inside one month; budgets scale by the credits bought, the gates don't move.

## 0. Standing rules (every day of the month)
- Expected cost is the **hard cap** on any query whose pattern hasn't run at one-day scope. New patterns run at one-day scope first, no exceptions.
- Every input column null-counted per era before it is joined or filtered on.
- Cost runs do not fetch rows. Exports happen once per chunk and are checksummed into the repo the same day.
- Daily entry in `credits.md`: spent today, cumulative, remaining, phase projection. Posted to Mando daily.
- **Reserve: 15% of each month's allowance, untouchable.** Hitting it ends the phase, not the reserve.
- Owner wallets never enter a saved query. Verdicts and thresholds never enter Dune at all.

## 1. Pre-purchase (trial credits, this week)
- Post-BOOST price-path week (burned week chosen by seed and recorded first). ≤ 50 credits.
- Revised `PURCHASE_DECISION.md` with both plan paths costed.
- Full chunk list written: 20 monthly chunks for stratum P, each with its event-column query and heavy-tier queries pre-generated and dry-run-reviewed. Zero credits.
- `RUNBOOK_v1.md` returned to Architect. **Purchase waits for it.**

## 2. Month 1 phases, with budgets and gates

| Phase | Days | Credits (plan / ceiling) | Deliverable | Gate to continue |
|---|---|---|---|---|
| **A. Scaling check** | 1 | 150 / 250 | One monthly chunk, event columns + heavy tier, post-BOOST month | Measured cost per chunk ≤ 1.3× projection. If not → C1. |
| **B. Event columns, full window** | 2–8 | 1,000 / 1,500 | 60 event-derived columns, 20 chunks, each exported on landing | Cumulative ≤ 1,500 by day 8. If not → C1/C4. |
| **C. Heavy tier** | 9–22 | 1,800 / 2,000 (month 1) + up to 3,000 (month 2) | Aligned set, funding, concentration, authorities, blocks, 20 chunks | Day-10 check: B complete and total ≤ 3,000 or → C2. Month-2 purchase ruled on measured per-chunk cost. |
| **D. Census + exports** | throughout | 200 / 300 | Gate 0 census, 20-token reconciliation, final export | — |
| **E. Cancel** | last day | 0 | Plan cancelled, views deleted, storage 0, all exports checksummed | — |

Month 1 plan total ≈ 3,150 of 3,400 spendable. Month 2 is for phase C's remainder only, and is bought only if phase A's measured unit says it fits.

## 3. Contingencies (pre-registered; whoever is on shift applies them without a new ruling)
- **C1 — Build cost overruns projection by > 30%.** Switch from the full window to the pre-defined sampled window: one contiguous 90-day block per era, chosen now by seed (recorded in RUNBOOK_v1), every cell labelled SAMPLED. Power reassessed on blocks.
- **C2 — Heavy tier projects above the remaining budget.** Drop column groups in this fixed order, never any other: (1) `cluster_*` early-buyer groups, (2) organic-v2, (3) `seta_lat_s`, (4) authority history on P (pass-by-construction anyway). Aligned set + funding + concentration + blocks are never dropped; they are H1.
- **C3 — Analyst export rate is the documented 5×.** Switch to in-warehouse analysis: table stays as a materialized view (RW key created then), H1/H4 passes run as ad-hoc queries, only result grids exported. Public-table mitigations already in place.
- **C4 — A chunk exceeds engine limits.** Halve the chunk (fortnight), re-run; budget line unchanged, chunk count doubles for that span only.
- **C5 — Data defect found mid-build** (an empty column, a decoder gap). Halt the phase, null-count the column across all eras, patch the generator, re-run only affected chunks. Chunks already exported are not rebuilt unless the defect touches their columns.
- **C6 — Credits exhausted before phase C completes.** Stop. Export what exists. Evaluate H1 on the chunks built, labelled PARTIAL with block counts. Month 2 ruled on that partial result, not bought by default.
- **C7 — Runaway detected** (in-flight cost > cap). Cancel, log, no re-run of that pattern until it has passed one-day scope again. Two runaways in a month → phase paused for an architect review.

## 4. After export — local, zero credits (weeks 5–6)
1. v1.2 thresholds frozen on the calibration slice: RUG-A X/Y/D, routing X, depth floor F, fan-out threshold, factory class.
2. H1 gate recall and false-block rate against RUG-A and slow-bleed, per era, per lag, raw n and block n.
3. Naive expectancy at each lag net of real fees and depth fill; H4 cost model per era.
4. **H5 — winners' study** (new, Mando's ask). Pre-registered now: winner = 24h peak ≥ 3× entry at G240 *and* 7-day price ≥ entry. Exploratory feature comparison on the calibration slice only; any marker that separates there is tested on the holdout as a one-sided hypothesis. Candidate markers named in advance: creator track record (prior tokens' RUG-A rate, from the actor registry), aligned-set state at entry, concentration, pool depth, bundle, organic-v2 takers in first hour, factory funding. No marker not on the list is tested without re-registration.
5. **Actor registry v0** (new, Mando's ask). Built from the exported aligned-set and funding records: persistent actor ID per collapsed funder; per actor, tokens deployed/funded/bundled, launch cadence, hold time, dump timing, RUG-A rate. 1-hop only in M0. Output: a hazard map — which actors are live, what they did last time. This is the daisy chain's first link and it costs nothing after export.
6. Verdict document; architect adversarial review; M1 go/no-go.

## 5. Out of scope for the paid month, and where each lands
- **Multi-hop daisy chains** (funder of the funder, 7+ day lookbacks): M0b, own budget, after the registry shows which actors are worth the scan. Multi-hop over `sol_transfers` is the most expensive pattern we have.
- **Live syndicate tracking**: M1, Helius address webhooks on registry wallets. Near-free on the free tier; tracks what they do from the day the registry exists.
- **Pre-listing chatter / noise**: cannot be backtested — there is no graveyard of deleted tweets and Telegram posts, and the historical feed can't be bought. It is a **forward collector** (M1/M2): X/Telegram/Discord mentions timestamped against graduation, joined to the on-chain table. Its hypothesis is pre-registered now so the collector is built to test it: *tokens with measurable chatter before graduation have higher 7-day survival and higher winner rate than matched tokens without.* Also expect the opposite tail: chatter is where paid promotion lives. Compliance and ToS check on the data sources before any collector runs.
- **H3 whale copy**: M0b, pending a storage design.

---

## Seeded selections, recorded before any query was run (ClaudeCode, 2026-10-01)

Method for both: `sha256("20261001|" + key)`, seed **20261001**. Reproduce with `recon/seeded_selections.py`.

**Burned week (runbook §1).** Candidates: the eight Monday-start weeks of post-BOOST graduations that already have nine days of follow-up on Dune (2026-07-27 … 2026-09-14). Order by hash of the week's start date, take the smallest.

| Rank | Week start | Hash (first 16 hex) |
|---|---|---|
| **1** | **2026-08-10** | `10bfae60df97950a` |
| 2 | 2026-09-14 | `243f711ca7a37810` |
| 3 | 2026-08-03 | `354524f0fb34c310` |

**Burned week: graduations 2026-08-10 → 2026-08-16.** It is excluded from every hypothesis evaluation from this commit on, in the same way as the calibration slice, and every table and cell that could include it says so.

**Sampled window for contingency C1 (runbook §3).** Start day = first eligible day + (hash of `"C1|" + era` mod number of eligible start days).

* **Pre-BOOST:** eligible starts 2025-07-11 (the day after the calibration slice) → 2026-04-22, 286 days. **Block: 2025-10-30 → 2026-01-27.** It crosses the 2026-01-10 fee-era boundary; cells keep their fee-era label.
* **Post-BOOST:** the era inside the window is 2026-07-21 → 2026-09-20, 62 days. **No 90-day block exists.** The post-BOOST sample is therefore the whole era, less the burned week: 55 days. This departs from "one contiguous 90-day block per era" and needs the Architect's acknowledgement.
