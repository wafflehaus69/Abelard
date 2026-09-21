# H3 Wallet Discovery Spec v1.0 — builder's review

**Reviews:** `H3_WALLET_DISCOVERY_v1.0.md` (Architect, 2026-09-21) · **By:** ClaudeCode · **Date:** 2026-09-21
**Status:** Item 1 blocks execution; items 2–3 block the full run but not the §6 cost trial; items 4–7 are design notes.

The spec is sound in its shape. Funnelling from the winners' side is the right answer to
R7/A6, and the out-of-sample rule (select at T, evaluate on trades after T) is what makes
H3 a real test. What follows is where the text as written would either contradict itself,
depend on something not yet ruled, or cost more than its own cost gate assumes.

---

## 1. BLOCKING — the spec contradicts itself on whether surfacing tokens are scored

§1 step 3: replay each candidate's *"complete DEX history over the trailing 60 days before T
— **all tokens**, not just the winners that surfaced them."*

§1 look-ahead check: *"A wallet is **never scored on the tokens that surfaced it**."*

Both cannot hold. The surfacing trade is inside the trailing 60 days by construction, and
step 2 requires the wallet to have **sold ≥ 50% before T**, so it is a *closed* position and
lands in §2's realized PnL. Read literally, step 3 scores every candidate on a trade that
was selected *because* it was a 5× winner entered in the first hour.

**Why it matters, stated precisely.** It does **not** invalidate the H3 result. H3 is
evaluated on trades *after* T, so a biased ranking cannot manufacture an out-of-sample
edge. What it corrupts is the **ranking**: every candidate carries a guaranteed early-winner
trade, and that trade dominates the score of a wallet with little else going on. So the
top 30 fills with one-hit wallets, which is exactly the "was early once" failure step 3
exists to prevent.

**Builder's recommendation:** the look-ahead sentence governs. Exclude each wallet's
surfacing token(s) from its §2 score, **and from the ≥ 15-distinct-tokens count**. Report
scores both with and without them for one rebalance date, so the size of the bias is
visible rather than argued. **Needs an Architect ruling before the funnel runs.**

## 2. Dependency — the winner test needs a number that is not set yet

§1 step 1 measures "≥ 5×" **on the price reference**. Per MR-3.2 the price reference is
volume-weighted across pools that qualify under the MR-2.2 **X%** rule, and X is set in
v1.2 from the calibration slice. So **the funnel cannot run for real until v1.2 exists.**

**Proposal:** the §6 cost trial may run on a provisional X, because bytes scanned barely
depend on it. Its *cohort* output is labelled PROVISIONAL and discarded. Only its byte
count is kept.

**Related, for v1.2:** v1.1 A3 excludes the calibration slice (the first 20% of weeks of P)
from H1 evaluation because thresholds were set on it. X will now be set on that same slice
and used in H3's winner test, so **H3 evaluation should exclude it too.** The spec is
silent on this.

## 3. Constants §2 uses but does not define ([E8])

* **The depth floor** below which open positions are marked at zero. It needs an observed
  distribution of pool depth before it gets a value.
* **Lot accounting** for realized PnL: FIFO, average cost, or specific lot. The choice moves
  scores for wallets that scale in and out, and M1's tax ledger (§9.7) will need one anyway.
  **Recommendation:** FIFO. It is conventional, and it is what the ledger will use, so the
  backtest and the ledger agree.

## 4. Cost — linear extrapolation overstates, and the free tier likely cannot hold H3 anyway

**§6 extrapolates one date linearly to all weekly dates.** The 60-day trailing windows at
weekly T overlap about 8.5×, so per-date runs would re-scan the same days roughly eight
times. **Better design:** one pass over the window (plus the 60-day lead-in) extracts every
DEX swap by `program_id` into a **derived trade table** in Mando's project. Every rebalance
date then queries that small table. Cost then tracks the window length, not the number of
dates. This needs write access (a dataset in the billing project), which should be ruled
alongside the project ID. It is consistent with v1.1 A1: the raw tape stays server-side,
and only the derived table is used.

**A pre-measurement estimate, labelled as one ([E8]).** Every number here is an assumption
until the §6 trial measures bytes billed. The assumptions:

* PumpSwap runs about 29M swaps/day (measured live on 2026-09-04, and not a window average).
* That gives about 2 `Instructions` rows per swap (outer instruction + event self-CPI) in
  PumpSwap's cluster, so about 58M rows/day.
* The columns scanned are `data` + `tx_signature` + `block_timestamp`, at about 0.5 KB per
  row, so about **25–35 GB/day** for PumpSwap alone.

Under those assumptions, one 60-day window is about **1.5–2 TiB**, and the full P window
(2025-03-20 to now, about 18 months) is about **14–19 TiB**. That is over the 1 TiB monthly
free allowance. In dollars it is about **$90–$120** at $6.25/TiB. That is small next to
Dune's plans, but it is not free.

**Implication, flagged now so it is not a surprise later:** under MR-3.1's ceiling, H3 very
likely trips its own §6 gate. Gate 0's census may too, because it scans the pump.fun
bonding-curve cluster, which runs at comparable volume. That outcome is not a failure: it is
the decision MR-1 said D1 would get, re-ruled on numbers. The likely fork is either to
authorize a dollar cap on paid scanning, or to stay free and cut scope, e.g. a sampled
window or H3 → M0b. **The trial will replace these estimates; do not budget off them.**

## 5. The bot-shaped sandwich test needs `Transactions`

">30% of trades sandwich another wallet's swap in the same slot" needs the order of
transactions **within** a slot. `Instructions` has no in-block transaction position (its
`index` is the instruction's position inside one transaction). `Transactions.index` does
have it, and under MR-3.1 `Transactions` is read only by signature. So the sandwich test
costs a signature-filtered `Transactions` read, restricted to the candidate union. That is
feasible, but it is a line item the §6 trial should include rather than discover.

## 6. The fame proxy is confounded by how hot the token is

"Distinct wallets that bought the same token within 2 minutes after it" rises for **any**
wallet trading hot tokens, famous or not. A wallet that only trades during frenzies will
read as famous. **Proposal:** normalize by the token's own buyer arrival rate, i.e. buyers
in the 2 minutes *after* divided by buyers in the 2 minutes *before*. That way the proxy
measures *following*, not *heat*. It is diagnostic only, so this does not block anything.

## 7. Minor

* **Echo scope.** The echo rule compares only against other *candidates*. A wallet that
  shadows a non-candidate, say a known bot, passes. That is acceptable in practice, because
  bot-shaped wallets are excluded anyway, but the spec should say it is deliberate.
* **S7b depends on a mechanism with history.** Creator fee-share recipients come from the
  pump fee program's `SharingConfig`. The fee-schedule history (`M0_FEE_LEGS.md` Part 2)
  shows creator-fee mechanics changing materially through the window, so the S7b set must
  be built as of T, not as of today.
