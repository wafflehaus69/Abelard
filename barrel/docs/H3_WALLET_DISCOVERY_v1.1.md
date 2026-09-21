# BARREL — H3 Wallet Discovery Spec (v1.1)

**Author:** Architect · **Builder edits:** ClaudeCode · **Date:** 2026-09-21
**Supersedes:** H3_WALLET_DISCOVERY_v1.0.md (retained unedited)
**Signed by:** H3_AMENDMENT_v1.1.md (Architect, 2026-09-21), rulings R1–R7 and the MR-3.2 routing follow-ups.
**Amends:** M0_TASKING §6 H3, Amendment v1.1 A6
**Status:** Pre-registered. Applies to the M0 backtest cohort; the same rules govern live discovery in M3+.

> Every change from v1.0 is marked **[R#]** with the ruling that made it. Text without a
> mark is v1.0 unchanged. Constants marked *provisional* are valid for the §6 cost trial only
> and are set for real in v1.2 on the calibration slice.

## 0. Principle

The cohort is discovered by our own on-chain measurement, never imported from a public leaderboard or "smart money" label. Public lists (GMGN, Cielo, Nansen, Birdeye top traders, Solscan tags) are the copy-bot herd's input and partly deployer marketing; a wallet's presence on one is treated as a negative feature (see §4), not a source.

Owner wallets (Mando's) are excluded from every cohort, every universe statistic, and the repo. They exist only in the gitignored personal-history replay (`barrel/private/`, ignored before any file existed there).

**Calibration slice [R2].** The calibration slice (the first 20% of weeks in stratum P, per M0 v1.1 A3) is excluded from the evaluation of **every** hypothesis, H3 included, because thresholds used here (routing X, depth floor F, cohort cut) are set on it.

## 1. Cost-controlled discovery funnel

Full-universe per-wallet PnL reconstruction is the expensive item flagged in R7/A6. Discovery funnels from the winners' side instead:

1. **Winner tokens.** For each weekly rebalance date T, take stratum-P tokens that graduated in the trailing 60 days before T and reached ≥ 5× from graduation price at any point before T, measured on the **price reference**: volume-weighted across the pools that qualify under the X% volume-share rule (MR-2.2, MR-3.2). This set is small. *X is provisional for the cost trial [R2].*
2. **Early participants.** Enumerate wallets that bought each winner within the first 60 minutes post-graduation and had sold ≥ 50% of that position before T. Union across winners. The winner(s) through which a wallet entered the union are its **surfacing tokens**.
3. **Full replay, only for that union.** Reconstruct each candidate wallet's complete DEX history over the trailing 60 days before T — all tokens — **for classification and exclusion (§3)**. **The §2 score and token count use every token except the wallet's surfacing tokens [R1].**

Look-ahead check: step 1 uses only price history before T. The cohort selected at T is evaluated on trades after T. **A wallet is never scored on the tokens that surfaced it [R1] — this sentence governs where any other text appears to conflict.**

**Data source [R4].** All of §1–§3 reads the **derived trade table** (one pass over the P window plus a 60-day lead-in, every DEX swap by program ID, held in Mando's GCP project), never re-scans the raw tape per rebalance date.

## 2. Scoring (point-in-time, exit-adjusted)

For each candidate wallet at T, over the trailing 60 days, **excluding its surfacing tokens [R1]:**

* Realized PnL in SOL from closed positions, with **FIFO lot accounting** [R3], so the backtest and the M1 tax ledger agree by construction.
* Exit-adjusted unrealized: open positions marked at what a full exit against pool depth at T would fetch, not spot. **Depth floor [R3]:** a position is marked at zero when its exit-adjusted value is below **F%** of its spot-marked value. Gate 0 reports the distribution of that ratio; F is set in v1.2. *Provisional F = 20%, cost trial only.*
* Score = realized + exit-adjusted unrealized, divided by total SOL deployed (a return, not a dollar figure, so a whale and a $500 wallet are comparable).
* Require **≥ 15 distinct tokens other than its surfacing tokens** in-window [R1]. Fewer = insufficient sample, excluded.
* Report the score distribution before any threshold is chosen; the cohort cut (top 30 in v1.0) is confirmed or amended in v1.2 under the calibration-slice rule.
* **Bias report [R1]:** for one rebalance date, report scores and the resulting top-30 both **with and without** surfacing trades, so the size of the ranking bias is a number.

## 3. Exclusions (hard)

A wallet is dropped, whatever its score, if it is:

* **Deployer-aligned:** a deployer, 1-hop deployer-funded (S7), creator fee-share recipient (S7b), or a same-slot bundle member (S8) on ≥ 2 tokens in-window. **S7b is built as of T from `SharingConfig` history, never today's state [R7]**; the same as-of-T rule applies to S7b's use in RUG-A.
* **Syndicate member:** in any H2 cluster.
* **Bot-shaped:** median hold time < 60 seconds, or > 30% of trades sandwich another wallet's swap in the same slot, or trades in > 200 tokens/week. Those wallets may be profitable; their edge is latency and it is not copyable at any lag we can execute.
  **Sandwich data [R5]:** in-slot order comes from `Transactions.index`, read by signature and restricted to the candidate union (MR-3.1), and carried into the derived table for those rows. If the §6 trial shows this leg dominates cost, the sandwich criterion becomes optional in v1 and bot-shaped is decided on hold time and token count alone. **Report which variant ran.**
* **Echo:** > 50% of its buys occur within 5 minutes after another candidate's buy of the same token. Keep the earlier wallet, drop the follower (CONSENSUS dedup rule). **The comparison is against other candidates only, deliberately [R7]:** a wallet shadowing a non-candidate (e.g. a bot) is covered by the bot-shaped exclusion instead.
* **Owner:** Mando's addresses.

## 4. Fame feature

For each cohort wallet, log a fame proxy, **fame = (distinct buyers of the same token in the 2 minutes after the wallet's buy) ÷ (distinct buyers in the 2 minutes before) [R6]**, averaged over its buys and trended over time. The ratio measures *following* rather than token heat. Hypothesis (architect, flagged): copy-edge decays as fame rises. This is measured, not assumed, and reported alongside H3 results. If it holds, live discovery should weight unfamous qualifying wallets. Diagnostic only.

## 5. Output and execution

Per rebalance date T: cohort list with scores, exclusion reasons for dropped candidates, and fame proxy. Feeds H3 exactly as pre-registered (copy at next block + lag G, mirror exit + lag G, 24h time stop), under the routing rules of MR-3.2:

* **Marks and returns** use the price reference (volume-weighted across qualifying pools).
* **Entry fill** is the single deepest qualifying pool at the entry block. **Deepest = quote-side reserves at that block.**
* **Exit fill:** if the entry pool still qualifies, exit there. If it has left the qualifying set, exit against the deepest pool that qualifies at the exit block. If no pool qualifies, the position marks to zero, which is the depth-floor rule applied at exit.

## 6. Cost gate

Before running §1–§3 across the full window, run it for one rebalance date on provisional X and F, including the R5 `Transactions` leg, and report **bytes billed per leg** and the R1 with/without-surfacing comparison. The trial's cohort is labelled PROVISIONAL and discarded; only bytes billed are kept [R2].

Extrapolate to the **derived-table design** (one pass over the window, not per-date) [R4]. Report the projected total and stop; Mando rules on the budget with real numbers. If the ruling is free-only, the pre-defined fallback is a sampled window: one contiguous 90-day block per era, chosen before any results are seen, every cell labelled SAMPLED and power reassessed.
