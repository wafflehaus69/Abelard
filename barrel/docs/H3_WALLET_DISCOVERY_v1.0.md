# BARREL — H3 Wallet Discovery Spec (v1.0)

**Author:** Architect · **Date:** 2026-09-21 · **Amends:** M0_TASKING §6 H3, Amendment v1.1 A6
**Status:** Pre-registered. Applies to the M0 backtest cohort; the same rules govern live discovery in M3+.

> Committed verbatim as relayed by Mando, 2026-09-21. Frozen: amendments land as dated
> numbered blocks, never as silent edits. Builder's review is in
> `H3_WALLET_DISCOVERY_REVIEW.md`, not in this file.

## 0. Principle

The cohort is discovered by our own on-chain measurement, never imported from a public leaderboard or "smart money" label. Public lists (GMGN, Cielo, Nansen, Birdeye top traders, Solscan tags) are the copy-bot herd's input and partly deployer marketing; a wallet's presence on one is treated as a negative feature (see §4), not a source.

Owner wallets (Mando's) are excluded from every cohort, every universe statistic, and the repo. They exist only in the gitignored personal-history replay.

## 1. Cost-controlled discovery funnel

Full-universe per-wallet PnL reconstruction is the expensive item flagged in R7/A6. Discovery funnels from the winners' side instead:

1. **Winner tokens.** For each weekly rebalance date T, take stratum-P tokens that graduated in the trailing 60 days before T and reached ≥ 5× from graduation price at any point before T (measured on the price reference, not a single pool). This set is small.
2. **Early participants.** Enumerate wallets that bought each winner within the first 60 minutes post-graduation and had sold ≥ 50% of that position before T. Union across winners.
3. **Full replay, only for that union.** Reconstruct each candidate wallet's complete DEX history over the trailing 60 days before T — all tokens, not just the winners that surfaced them. This is what turns "was early once" into a testable skill claim.

Look-ahead check: step 1 uses only price history before T. The cohort selected at T is evaluated on trades after T. A wallet is never scored on the tokens that surfaced it.

## 2. Scoring (point-in-time, exit-adjusted)

For each candidate wallet at T, over the trailing 60 days:

* Realized PnL in SOL from closed positions.
* Exit-adjusted unrealized: open positions marked at what a full exit against pool depth at T would fetch, not spot. Positions in pools below a depth floor are marked at zero.
* Score = realized + exit-adjusted unrealized, divided by total SOL deployed (a return, not a dollar figure, so a whale and a $500 wallet are comparable).
* Require ≥ 15 distinct tokens traded in-window. Fewer = insufficient sample, excluded.
* Report the score distribution before any threshold is chosen; the cohort cut (top 30 in v1.0) is confirmed or amended in v1.2 under the calibration-slice rule.

## 3. Exclusions (hard)

A wallet is dropped, whatever its score, if it is:

* **Deployer-aligned:** a deployer, 1-hop deployer-funded (S7), creator fee-share recipient (S7b), or a same-slot bundle member (S8) on ≥ 2 tokens in-window.
* **Syndicate member:** in any H2 cluster.
* **Bot-shaped:** median hold time < 60 seconds, or > 30% of trades sandwich another wallet's swap in the same slot, or trades in > 200 tokens/week. Those wallets may be profitable; their edge is latency and it is not copyable at any lag we can execute.
* **Echo:** > 50% of its buys occur within 5 minutes after another candidate's buy of the same token. Keep the earlier wallet, drop the follower (CONSENSUS dedup rule).
* **Owner:** Mando's addresses.

## 4. Fame feature

For each cohort wallet, log a fame proxy: number of distinct wallets that bought the same token within 2 minutes after it, averaged over its buys, trended over time. Hypothesis (architect, flagged): copy-edge decays as fame rises. This is measured, not assumed, and reported alongside H3 results. If it holds, live discovery should weight unfamous qualifying wallets.

## 5. Output

Per rebalance date T: cohort list with scores, exclusion reasons for dropped candidates, and fame proxy. Feeds H3 exactly as pre-registered (copy at next block + lag G, mirror exit + lag G, 24h time stop).

## 6. Cost gate

Before running §1–§3 across the full window, run it for one rebalance date and report bytes scanned. Extrapolate to all weekly dates; if the total exceeds ~40% of the M0 BigQuery budget, H3 splits to M0b per A6 and M0 ships without it.
