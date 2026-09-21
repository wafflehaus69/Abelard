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

### Index

| Amendment | Author | Date | Scope |
|---|---|---|---|
| A1 (below) | ClaudeCode, recording Mando | 2026-09-04 | Rulings on D1, D2, D3 as given in session |
| **v1.1** — [`docs/M0_AMENDMENT_v1.1.md`](docs/M0_AMENDMENT_v1.1.md) | Architect | 2026-09-04 | **Operative.** Formalizes and extends A1; adds A2 strata, A3 rug direction + snooping guard, A4 BOOST era split, A5 per-era fee model, A6 H3 costing gate, A7 quarantine standard |
| MR-2 (below) | ClaudeCode, recording Mando | 2026-09-21 | D5 bankroll; price-reference pool rule; BigQuery access + what metadata already settles |
| MR-3 (below) | ClaudeCode, recording Mando / Architect | 2026-09-21 | BigQuery spend ceiling + Transactions rule; price reference vs fill; realized-fee cost model ratified; owner wallets; H3 discovery spec adopted |
| MR-4 (below) + [`docs/H3_AMENDMENT_v1.1.md`](docs/H3_AMENDMENT_v1.1.md) | Architect; ClaudeCode recording | 2026-09-21 | M0-wide rulings arising from the H3 review; project ID; standing ceiling unchanged |

*Numbering note.* v1.1 numbers its own sections A1–A7, which collides with the A1 below.
Builder-recorded Mando rulings therefore use the **MR-** prefix from here on; the A1 block
below is MR-1 under that scheme and is not renamed, so that references to it stay valid.

**A1 below is superseded by v1.1 where they overlap** and is retained because it is the
contemporaneous record of what Mando actually ruled, before the Architect formalized it.
Where the two differ in force, v1.1 governs. Two of A1's notes are *not* superseded and
still stand, because v1.1 does not address them: the BigQuery cost caveat (petabyte-scale
dataset, 1 TiB/month free allowance, ~$6.25/TiB after — so A1's outcome is reported with
scan estimates attached), and the E16 obligation to enumerate and freeze the launchpad
admission set before Gate 0 runs rather than during it.

### A1 — Rulings on D1, D2, D3 (Mando, 2026-09-04)

Ruled by Mando on the findings in `docs/M0_RECON.md`. Superseded text above is left in
place. D4 and D5 remain unruled.

---

**A1.1 — D1, data source. RULED: verify the BigQuery public Solana dataset before any
spend.** No vendor purchase is authorized yet.

Supersedes §2's "Dune or Flipside" only as to sequence: BigQuery
(`bigquery-public-data.crypto_solana_mainnet_us`) is evaluated first, free, and Dune
becomes the fallback if it fails. Flipside is struck as a candidate — it ceased to exist
2026-06-17 (R1).

The evaluation is not "does it have data." It is three questions, in `recon/bigquery_gate0_probe.sql`:

1. **Freshness** — how far behind head is the newest block, and is the lag stable across
   the window or does it have holes? R1 established the public record here is misleading
   in both directions: the 2025-03-31 outage was resolved in April 2025, and a separate
   multi-day lag was reported in November 2025 with no official response.
2. **Coverage of the window** — per-week transaction counts back to 2025-03-20, looking
   for gaps. §2's Gate 0 tolerance is >5% in any week = stop and report.
3. **Whether `accounts` actually answers R8** — the state-snapshot table is the entire
   reason this candidate is not dismissed. If it does not carry mint/freeze authority at
   a slot, its main advantage over Dune evaporates and the S1–S7 as-of reconstruction
   cost returns in full.

**Blocked on access.** This session has no `gcloud`/`bq` and no GCP credentials, and the
BigQuery connector is unauthorized. A public dataset still requires an authenticated
project to query. The queries are written and waiting.

---

**A1.2 — D2, universe. RULED: all venues — PumpSwap, Raydium (legacy and current), and
Meteora.** Supersedes §3's "graduated from pump.fun (or equivalent launchpad) to a
Raydium or Meteora pool" by widening it rather than replacing it: PumpSwap is added as
the dominant destination, and the pre-2025-03-20 Raydium era is retained in the main
universe rather than being stratified out.

Builder recommended the narrower PumpSwap-only universe (`docs/M0_RECON.md` D2); Mando
ruled wider. Recorded because the ruling carries obligations the narrow version did not,
and they are not optional:

* **Per-venue composition on every aggregate, always co-presented [E14].** No pooled
  headline number ships without its venue decomposition. A pooled expectancy across
  venues with different LP mechanics, fee schedules and launchpad populations is exactly
  the kind of aggregate E14 exists to forbid.
* **`(or equivalent launchpad)` now binds and must be enumerated before the census.**
  Widening past pump.fun admits Raydium LaunchLab, Meteora's own launch paths, and the
  other current launchpads. The admitted set is written down and frozen *before* Gate 0
  runs, not discovered during it — [E16], admission bugs masquerade as match failures.
* **The gate's structural constants are now venue-dependent, so no check may be assumed
  constant globally.** S1–S5 pass/fail rates get reported per venue.
* **Per-venue cost model.** §5 gains a fee schedule per venue and per era rather than one
  rate: PumpSwap 0.25% (0.20 LP / 0.05 protocol), Raydium v4 0.25%, Raydium CLMM tiered,
  Meteora DLMM dynamic. Plus the creator-fee leg from R6.

**The ruling has a genuine upside the narrow option did not, and it partly repairs D3.**
R3's problem was that LP burn is structural on PumpSwap, making S5 constant and the rug
definition's lead limb unable to fire. That is a *PumpSwap* property, not a universal one.
On venues where LP is not burned by construction, S5 regains discriminating power and
"LP removed ≥80%" fires as written. The cost is that the rug base rate now differs by
venue **by construction** — so rug recall and precision are reported per venue and never
pooled, or the mix alone will move the headline.

---

**A1.3 — D3, rug definition. RULED: measure first, then pre-register.** Supersedes §4's
rug definition as to *timing*: the definition above stands as the draft, and the
threshold is not final until amended.

Binding sequence, no step skippable:

1. Gate 0 census completes.
2. The observed distribution of post-graduation reserve decay is measured and published —
   **per venue**, per A1.2.
3. The rug threshold is pre-registered in a numbered amendment to this file.
4. Only then is any gate metric (recall, precision, false-block rate) computed.

Computing a gate metric before step 3 is a protocol violation, not a shortcut. [E8]:
no spec constant ships without an observed distribution behind it.

---

**Still unruled:** D4 (BOOST as a pre-registered era split vs. a §7 tercile) and D5
(§12 bankroll confirmation, and the "Mando's list" wallet set for the H3 cohort variant).

---

### MR-2 — Rulings and access (Mando, 2026-09-21)

Recorded by ClaudeCode in the session they were given. v1.1 remains operative; this block
adds to it and supersedes nothing in it.

**MR-2.1 — D5 bankroll. RULED: $2,000.** §8's ruin and drawdown simulation uses a $2,000
bankroll at $20/ticket. The placeholder becomes the pre-registered value. D5's second half —
the "Mando's list" wallet set for the H3 cohort variant — is offered and pending receipt.

**MR-2.2 — Price reference under routing. RULED (direction), threshold not yet set.**
Put to Mando: 22–30% of sampled PumpSwap swaps were routed or multi-hop
(`docs/M0_FEE_LEGS.md`), while §5 models a fill against a single constant-product pool.
Mando's ruling, verbatim: *"pools need to hit a certain % of volume for price action. it's
possible for this to be quite fluid."*

Builder's reading, **to be confirmed by Mando or the Architect before v1.2 freezes it**:

* A token's price-setting venue set is **every pool carrying at least X% of that token's
  volume** over a trailing window, not one fixed pool.
* The set is **recomputed through time**, because pool shares migrate — "fluid" is part of
  the rule, not a caveat on it. A pool can enter and leave the set within a token's life.
* Entry and exit marks come from the qualifying set; pools below X% are ignored for price
  but their volume is still counted in the denominator.

What is **not** ruled and must not be invented by the builder:

* **X.** Per [E8] the threshold is measured before it is mandated: Gate 0 outputs the
  distribution of per-pool volume share per token, and X is pre-registered in v1.2 from the
  **v1.1 A3 calibration slice** (first 20% of weeks in stratum P), under the same snooping
  guard as the rug thresholds.
* **The trailing window length** for "share of volume". Same treatment as X.
* **Whether the rule sets the mark or the fill.** "Price action" most naturally reads as the
  *price reference* — the mark entries and exits are measured against. The *fill model*
  (what Mando's $20 actually pays) is a separate question: an aggregator splits a small order
  across the same qualifying pools, so the two may coincide, but that is an assumption until
  ruled.

**MR-2.3 — BigQuery access. CONNECTED 2026-09-21.** The connector reaches
`bigquery-public-data.crypto_solana_mainnet_us`. Every result below came from **table
metadata only — zero bytes scanned, zero cost.** No scanning query has run: a billing
project ID is needed first, and it has not been supplied.

*Freshness (v1.1 A1 step 1) — provisionally PASS.* Every table was last modified
2026-09-21 between 15:37 and 15:40 UTC, and every table has an active streaming buffer
opened in that same minute. The dataset is being written live, not backfilled. Two
cautions keep this at *provisional*: last-modified time is not `MAX(block_timestamp)`, and
freshness at head says nothing about holes inside the window. Both need the scanning query.

*`Accounts` (v1.1 A1 step 2) — FAILS as an as-of source, on its schema alone.* This was
the one reason the dataset was preferred over Dune, so it is recorded carefully:

* It has **no mint-authority or freeze-authority column.** S1/S2 cannot be a lookup.
* It holds **169,239,290 rows across its whole history**, far too few to be a per-slot
  snapshot of a chain whose pump.fun venue alone creates mints by the thousand daily.
* Its own column documentation settles it: `state` is *"the state of the account at the
  time the data was requested from the node,"* keyed by a separate `retrieval_timestamp`.
  That is neither latest-state nor as-of-slot — it is **as-of-whenever-it-was-fetched.**
  v1.1 warned that a latest-state table mistaken for as-of is worse than useless. This one
  is the same hazard in a less obvious form, because its rows carry a `block_slot` that
  invites exactly that mistake.

**Consequence:** R8's reconstruction burden is **not** lifted. S1–S7 as-of state must be
rebuilt from instruction history. The dataset can support that — `Instructions` carries
parsed SPL instructions with named parameters (`initializeMint2`, `setAuthority`), and
`Token Transfers` carries per-transfer Token-2022 `fee` — so this is a cost problem, not an
availability problem.

*Scale and cost, from metadata:*

| Table | Logical size | Partitioning | Clustering | Avg per partition |
|---|---|---|---|---|
| `Transactions` | 885 TiB | DAY, partition filter **required** | `signature` | ~447 GB |
| `Instructions` | 879 TiB | DAY, partition filter **required** | **`program_id`** | ~444 GB |
| `Token Transfers` | 41 TiB | DAY, partition filter **required** | none | ~21 GB |
| `Accounts` | 0.1 TiB | MONTH | none | ~2 GB |
| `Blocks` | 0.1 TiB | MONTH | none | ~1 GB |

Three consequences, each binding on the pipeline:

1. **One unclustered day of `Transactions` is ~40% of the free monthly allowance**
   (1 TiB). A full-history scan of either large table is ~$5,500. The "free" framing in
   MR-1 was already corrected; this puts a number on it.
2. **`Instructions` clustered on `program_id` is what makes M0 affordable at all.**
   Filtering on PumpSwap's or pump.fun's program id reads only those clusters, not the day.
   Caveat, and it matters for budgeting: **a dry run reports the unclustered upper bound**,
   so dry-run estimates on `Instructions` will overstate. Real cost must be measured from
   bytes billed on a small live query, then extrapolated — never read off the dry run.
3. **`Transactions` is clustered on `signature`, not program**, so "all PumpSwap
   transactions for a day" cannot be pruned there directly. The affordable shape is:
   select from `Instructions` by `program_id`, and touch `Transactions` only for the
   pre/post token balances that fee and holder work need, with the smallest column set.

*A7 quarantine applies to a specific table.* `Tokens` stores `name`, `symbol` and `uri` —
precisely the metadata §1 quarantines. Queries against `Tokens` name their columns
explicitly and never select those three. `SELECT *` is now forbidden on two independent
grounds: cost, and the quarantine.

*Correction to `recon/bigquery_gate0_probe.sql`.* It assumed lowercase table names. The real
tables are `Accounts`, `Blocks`, `Transactions`, `Instructions`, `Token Transfers`, `Tokens`
— capitalized, and one containing a space. Fixed in the file.

---

### MR-3 — Relayed orders (Mando / Architect, 2026-09-21)

Recorded by ClaudeCode in the session they were relayed.

**MR-3.1 — BigQuery spend ceiling. RULED.**

* Stay inside the **free 1 TiB of query processing this month.**
* **Confirm before running** any single query whose estimate exceeds **50 GB**.
* **No query touches `Transactions` unclustered.** `Transactions` is clustered on `signature`
  only, so the rule's working form is: `Transactions` is read only with a `signature` filter
  derived from a `program_id`-filtered `Instructions` query, never by date alone. Builder's
  note: clustering pruning is weak under large join or `IN` lists, so the preferred design
  avoids `Transactions` entirely wherever `Instructions` suffices. Fee work does: realized
  fees come from PumpSwap swap events, which are self-CPI rows in `Instructions` under
  PumpSwap's own `program_id`.
* Budgeting caveat (MR-2.3): a dry run on a clustered table reports the unclustered upper
  bound. The 50 GB confirm rule is applied to that dry-run figure, conservatively, until
  real bytes-billed on a first small query gives a measured ratio.

**MR-3.2 — Price reference vs fill. RULED: they are different.** Resolves the question MR-2.2
left open.

* **Price reference** = **volume-weighted** price across the pools that qualify under the
  MR-2.2 X% volume-share rule. Used for marks, returns, the H3 "≥ 5×" winner test, and
  anything else that asks "what was the price".
* **Fill** = the **single deepest qualifying pool at the entry block**. A $20 ticket does not
  split across routes, and the backtest pays what Mando would actually pay, not an average.
  Exit fill follows §5 as written (worse of next-block price and depth-implied price), in
  that same pool, unless it dropped out of the qualifying set by then. **That case is not
  yet ruled** and must not be defaulted: builder flags it for v1.2.
* "Deepest" needs a definition before it is computed. Builder's proposal for v1.2: quote-side
  reserves at the entry block. Not assumed until ruled.

**MR-3.3 — Cost model pricing. RATIFIED for v1.2.** §5 fees are priced from **fees actually
charged in historical swap events**, with config history used only to date era boundaries.
Architect's note carried in: at a median 104 bps per leg when a creator fee is set (cap 300),
fees alone can be **2–6% round trip before slippage on a hot token**, and hot tokens are
where creator fees are set. That is a hurdle the expectancy has to clear, not a reason to
stop.

**MR-3.4 — Owner wallets.** The wallets Mando offered are **his own**. They are excluded from
every cohort, every universe statistic, and the repo (H3 spec §0). They live only in
`barrel/private/`, which is **gitignored before any file exists there**
(`barrel/.gitignore`, verified with `git check-ignore`). No owner address appears in any
committed file, query result, or log.

**MR-3.5 — H3 wallet discovery. ADOPTED:** `docs/H3_WALLET_DISCOVERY_v1.0.md` (Architect,
2026-09-21), committed verbatim. It replaces §6 H3's cohort construction and A6's costing
path with a winners-side funnel and its own cost gate. The builder's review is in
`docs/H3_WALLET_DISCOVERY_REVIEW.md`. **One contradiction in it needs a ruling before
anything runs** (review item 1).

**Still blocked:** no scanning query can run until Mando supplies the **GCP billing project
ID**. It was relayed as "yours to give", which is Mando's to supply. The builder cannot
discover it: the connector exposes no project listing.

---

### MR-4 — M0-wide rulings from the H3 amendment (Architect, 2026-09-21)

`docs/H3_AMENDMENT_v1.1.md` is H3-specific, but five of its rulings bind all of M0. They are
restated here so this file stays the single place M0's rules can be read from.

1. **Calibration slice — standing rule.** The calibration slice is excluded from the
   evaluation of **every** hypothesis, not only the one whose threshold was set on it.
   Thresholds set there so far or pending: rug X/Y/D, routing X, depth floor F, cohort cut.
2. **Routing, completing MR-3.2.** "Deepest" = quote-side reserves at the entry block. Exit:
   in the entry pool if it still qualifies; otherwise the deepest pool qualifying at the exit
   block; if none qualifies, mark to zero.
3. **Depth floor, semantic definition.** A position is marked at zero when its exit-adjusted
   value is below F% of its spot-marked value. Gate 0 reports the distribution of that ratio;
   F is set in v1.2. Provisional F = 20%, cost trial only.
4. **FIFO lot accounting**, M0-wide, so backtest and the M1 tax ledger agree.
5. **The derived trade table is the M0 data foundation**, not an H3 optimisation. Every
   H1–H4 query reads it. It is the seed of M1's live store. Minimum schema: signature, slot,
   block_timestamp, in-slot tx index, program, pool, mint, side, wallet, gross/net amounts,
   each fee leg, era label, stratum label.

**Project ID supplied by Mando:** `project-1602caf0-d9ea-4ac6-b0b`.

**Not yet ruled** (H3 amendment, Mando decisions 2–4): dataset-creation permission, paid-scan
authorization, budget alert. **Until they are ruled, MR-3.1 stands unmodified**: free 1 TiB
this month, confirm-before-run above 50 GB on the dry-run figure, no unclustered
`Transactions`, no dataset created. The Architect's conditional supersession of MR-3.1 takes
effect only when Mando rules decision 3.
