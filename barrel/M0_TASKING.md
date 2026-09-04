# Project 4 — "BARREL" — M0 Tasking Document
**Milestone:** M0 — Historical survival + expectancy backtest
**Author:** Architect (Claude) · **Owner/decision authority:** Mando · **Builder:** ClaudeCode
**Version:** 1.0 · 2026-09-03
**Status:** Pre-registered. No live code, no wallet, no keys, no execution in this milestone.

> **Frozen artifact.** This file is the pre-registration of record, committed verbatim as
> issued. It is never silently edited. Amendments are proposed in `docs/M0_RECON.md`,
> ruled by Mando, and then applied here as a dated, numbered amendment block that leaves
> the superseded text in place. A pre-registration that can be edited is not one.

---

## 0. Purpose

Before building any live detector, answer three questions on archival data:

1. **Survival thesis.** What fraction of post-graduation Solana memecoin entries would a deterministic safety gate have blocked, and what fraction of those blocked tokens actually rugged? (Gate precision/recall on rugs.)
2. **Naive expectancy.** If Mando had put a fixed $20 ticket on every token that passed the gate, what was the net return distribution, by regime, after realistic DEX costs?
3. **Overlay hypotheses.** Does either the syndicate-timing overlay (H2) or the whale-copy overlay (H3) improve expectancy over the gate-only baseline, at Mando's realistic entry price, out of sample?

Standing discipline from CONSENSUS and the BTC project applies in full: hypothesis-first, pre-registered, walk-forward, expectancy not win rate, adversarial data assumed, "NO" is a valid finding.

---

## 1. Non-goals (hard)

- No live trading, no wallet creation, no private keys anywhere in the repo or env.
- No LLM stage in the scoring path. All token metadata (name, symbol, socials, description, URI JSON) is **quarantined display-only data**, stored but never parsed for logic and never passed into any prompt. This is a first-class threat (prompt injection via metadata targeting AI agents).
- No live API polling (Birdeye/DexScreener/Helius). M0 is archival only.
- No airdrop work. Separate track, post-M2.
- No Coinbase / CEX data.

---

## 2. Data sources

Primary: **Dune** (Solana decoded tables) or **Flipside** — ClaudeCode to pick whichever gives complete pump.fun + Raydium/Meteora coverage for the window. The critical requirement is the **graveyard**: every token that launched in-window must be in the universe whether or not it still exists. DexScreener/Birdeye historical endpoints drop dead tokens and are therefore disqualified as the universe source (they may be used for spot-checking only).

Architect's hypothesis on table availability (ClaudeCode must verify, not assume): Dune has community-decoded `pumpdotfun_solana` create/trade/complete events and `raydium_solana` pool-creation + swap tables, and `tokens_solana` metadata. If the graduation event ("complete"/migration) isn't decoded, reconstruct from Raydium pool creation where one side is the token mint and the initial LP comes from the pump.fun migration authority.

Secondary (for SOL/USD at block time): a daily/hourly SOL price series from any reputable source; document it.

**Data-quality gate (Gate 0):** before any analysis, ClaudeCode delivers a census: tokens launched per week, graduation rate per week, % of graduated tokens with ≥1 swap in the following 7 days, and a reconciliation of 20 randomly sampled tokens against Solscan by hand. If the census shows gaps >5% in any week, stop and report.

---

## 3. Universe & window

- **Chain:** Solana only.
- **Universe:** tokens that **graduated** from pump.fun (or equivalent launchpad) to a Raydium or Meteora pool. Launch-stage (bonding-curve) tokens are excluded — speed snipers dominate there and that is not a game an advisory system wins.
- **Entry point (fixed for M0):** graduation time + **G minutes**, with G ∈ {15, 60, 240}. Three fixed lags, no optimization beyond these three. Lag exists because Mando is advisory/manual at first; the 240-minute lag is the honest current-state number.
- **Window:** as far back as the data source cleanly covers, target ≥ 12 months, must include at least one hot regime and one cold regime (see §7).
- **Ticket:** $20 USD notional at entry, converted to SOL at block-time price.

---

## 4. Safety gate (retroactive, deterministic)

Applied as-of entry time using only on-chain state observable at that block. Each check produces PASS / FAIL / UNKNOWN. UNKNOWN is logged separately and is treated as FAIL for the strict variant and PASS for the loose variant; report both.

| # | Check | Fail condition | Notes |
|---|-------|----------------|-------|
| S1 | Mint authority | not revoked | |
| S2 | Freeze authority | not revoked | |
| S3 | Token-2022 extensions | any of: transfer hook, permanent delegate, non-transferable, confidential transfer | Auto-fail; no discretion |
| S4 | Transfer fee extension | fee > 0 | log the rate; anything > 0 fails in strict |
| S5 | LP status | LP tokens not burned AND not in a verifiable locker | "Verifiable" = known locker program; unknown lockers = UNKNOWN |
| S6 | Top-holder concentration | top 10 holders ex-LP ex-known-burn > 40% | threshold pre-registered; report sensitivity at 30/50 |
| S7 | Deployer-linked supply | deployer + wallets funded by deployer (1-hop) hold > 15% | 1-hop only in M0 |
| S8 | Bundle at launch | ≥ 5 distinct wallets buying in the same slot as creation, funded from a common source within 24h prior | detection, not verdict — feeds H2 too |
| S9 | Wash-volume flag | (24h volume / unique taker wallets) above 95th pct of universe, or > 30% of volume is same-wallet round-trips | flag; strict fails, loose warns |
| S10 | Sellability (retro) | no successful sell by a non-deployer-linked wallet in the first 30 min post-graduation | Retro proxy for the live Jupiter round-trip sim; log as the weakest check |
| S11 | Copycat | name/symbol matches a token with ≥ 10x the volume launched within prior 72h | classify, don't fail — separate stratum |

Deployer forensics beyond 1-hop (prior rugs by same funder) is **M1**, not M0.

**Rug definition (outcome label, applied after the fact):** any of — LP removed/drained ≥ 80% within 7 days; price −95% from entry within 7 days with LP drawdown; transfer fee or freeze activated post-entry. Everything else is "not rugged" even if it went to zero slowly. Keep "went to zero slowly" as its own label — it matters for expectancy but is not a gate failure.

**Gate metrics to report:** rug recall (rugs caught / all rugs), rug precision, false-block rate (non-rug tokens blocked), and — the number that matters — **expectancy of the passed set vs. the full set**.

---

## 5. Cost model (mandatory, applied to every simulated fill)

- DEX swap fee: pool-specific (Raydium 0.25% standard; Meteora dynamic — use actual).
- Slippage: computed from the constant-product pool state at the entry block for a $20 buy; same for the exit. No fixed-percentage assumption.
- Priority fee / Jito tip: 0.001 SOL per leg (assumption; flag as such).
- Token transfer tax: from S4, if any.
- Exit fill: at the **worse** of the next-block price and the pool-depth-implied price for a full exit of the position.
- Round-trip cost is reported as a distribution, not a mean. If median round-trip on the passed set exceeds 8%, that is itself a finding.

---

## 6. Pre-registered hypotheses

**H1 — Survival thesis (primary).**
Gate-passed tokens have higher net expectancy per $20 ticket than the unfiltered universe, and the gate catches ≥ 70% of rugs with a false-block rate ≤ 40%.
Decision rule: GO if both hold in-sample AND expectancy improvement holds out-of-sample by walk-forward. Otherwise NO-GO on the gate design (not on the project — a bad gate gets redesigned, that's expected).

**H2 — Syndicate timing overlay.**
Definition: a syndicate is a cluster of ≥ 5 wallets, funded from a common source within 7 days, that accumulate the same token within a 30-min window post-graduation.
Entry: first observable moment the cluster is detected + lag G. Exit: first block in which cluster net flow turns negative for 2 consecutive minutes, OR LP drawdown ≥ 20%, OR time stop 24h — whichever first.
Claim: H2 entries have higher net expectancy than gate-only entries on the same tokens.
Architect prior: skeptical. Followers are the syndicate's exit by design; the on-chain visibility of their distribution is the only reason this is worth testing. Expect the result to be highly lag-sensitive — report at G = 15/60/240 separately. If it only works at G = 15, it is not a manual-execution strategy and gets parked until the semi-auto rung.

**H3 — Whale-copy overlay.**
Cohort construction must be **point-in-time**: at each weekly rebalance date T, rank wallets by **exit-adjusted realized PnL** over the trailing 60 days using only trades that closed before T. Exit-adjusted = mark unrealized positions at what a full exit against pool depth at T would fetch, not at spot. Exclude wallets in any detected syndicate cluster (S8/H2) and wallets with > 50% of trades in the same tokens as another cohort member within 5 minutes (echo/copycat dedup — the CONSENSUS fix). Cohort = top 30 by that metric.
Entry: copy each cohort buy at next-block + lag G. Exit: mirror the cohort wallet's exit + lag G, or 24h time stop.
Claim: net expectancy > gate-only baseline.
Architect prior: this is the same observation-edge class that returned 4× NO-GO on Polymarket. Prior is NO. It's included because Mando ranks it highly and a clean NO closes it; a clean YES would be a real finding.

**H4 (diagnostic, not a trade).** Regime dependence. All of H1–H3 reported per regime bucket (§7). The system will ship with a regime gate regardless; H4 tells us the parameters.

No hypothesis beyond H1–H4 is tested in M0. Anything else that looks interesting gets logged in `ideas.md` and waits for its own pre-registration.

---

## 7. Regime buckets

Define weekly regime from three series: (a) pump.fun launches/week, (b) graduations/week, (c) net new wallets making a first DEX swap/week. Bucket into COLD / WARM / HOT by terciles over the window. Report every metric by bucket. Mando's thesis is that HOT is "fish in a barrel"; M0 measures whether that was true in the last HOT stretch net of costs.

---

## 8. Validation

- Walk-forward: build any parameter (thresholds in S6/S7/S9, cohort size in H3) on the first 60% of weeks, evaluate on the remaining 40%. Report both.
- Multiple comparisons: three lags × three hypotheses × two gate variants × three regimes is 54 cells. Any cell that "wins" alone is noise; a hypothesis only passes if it wins across lags within a regime.
- Power labeling: any cell with < 100 entries is labeled UNDERPOWERED and cannot support a GO.
- Metrics: expectancy per ticket (USD), median return, hit rate ≥ 2× (informational only), Sortino, max drawdown of a running $2,000 bankroll at $20/ticket, and **tickets-to-ruin** under the observed distribution.

---

## 9. Deliverables

1. `census.md` — Gate 0 data-quality report with the 20-token hand reconciliation.
2. `gate_results.md` — S1–S11 pass/fail rates, rug recall/precision, expectancy passed vs. full, strict and loose.
3. `hypotheses.md` — H1–H4 results in the 54-cell grid with UNDERPOWERED labels, plus a one-paragraph plain-English verdict per hypothesis.
4. `cost_model.md` — round-trip cost distribution and assumptions.
5. `ideas.md` — parked observations, no analysis.
6. Reproducible pipeline: one command re-runs everything from raw pulls. Cache raw pulls; never hand-edit intermediates.
7. Ledger schema (forward-looking, not populated in M0): per-swap record with mint, side, qty, SOL amount, SOL/USD at block, USD cost basis, lot id, fee breakdown. This becomes the tax ledger in M1 and the calibration source for later backtests.

---

## 10. Acceptance criteria for M0 → M1

- Gate 0 passed (census gaps ≤ 5%/week, hand reconciliation ≥ 19/20).
- H1 reported with a GO/NO-GO and the gate redesign notes if NO-GO.
- H2 and H3 reported with GO/NO-GO per regime, or UNDERPOWERED. A NO-GO on both is an acceptable outcome; it narrows M1 to gate + unusual-activity detector.
- Cost model documented; assumptions flagged.
- Architect adversarial review completed on the pipeline (expect findings; expect them fixed).

---

## 11. What happens after

- **M1:** live safety gate + tax ledger + regime monitor, advisory-only, Helius webhooks + Jupiter quote sellability simulation (the real S10). Paper-trade begins.
- **M2:** Detector B port — unusual activity on gate-passed tokens (volume z-score vs. own baseline, new-unique-buyer inflow, whale net inflow net of deployer-linked wallets), quiet-week FP rate as acceptance criterion, labeled log from day one.
- **M3+:** whichever of H2/H3 survived M0, if any. Execution ladder: advisory → semi-auto (staged swap, Mando confirms on phone) → full auto with hard caps. Each rung requires a documented paper/live record on the rung below.

---

## 12. Open items for Mando

- Any specific wallets or trackers he followed in the past: they seed an H3 cohort variant ("Mando's list") tested alongside the algorithmic cohort.
- Confirm $2,000 as the bankroll for the drawdown/ruin sim, or give the real number.
- Keep a dated copy of the Ameriprise policy language excluding crypto on file (his call already made; this is record-keeping).

---

## Amendments

*None yet. Proposed amendments are in `docs/M0_RECON.md` §Decisions, pending Mando's ruling.*
