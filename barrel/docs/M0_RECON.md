# BARREL M0 — Source & Premise Recon

**Workstream:** BARREL-M0-RECON · **Builder:** ClaudeCode · **Date:** 2026-09-03
**Governing artifact:** [`../M0_TASKING.md`](../M0_TASKING.md) v1.0 (frozen)
**Status:** Gate 0 **BLOCKED**. Seven findings, five of which change the pre-registration.
No pipeline code written — by design, see "Why nothing was built yet" below.

---

## Why nothing was built yet

[E3] recon-first, disk-and-live-surface is canonical; [E4] calibration-first for any new
source. The tasking document carries an explicit instruction in §2 — *"ClaudeCode must
verify, not assume"* — and the verification came back against several load-bearing premises.
Building the S1–S11 gate against §4 as written would have produced a working pipeline
measuring the wrong universe with an inapplicable outcome label. The findings below are
what the recon was for.

Everything here is dated. Per [E15], the negative verdicts expire and carry a re-check
obligation.

---

## Evidence tiers used

| Tier | Meaning | Used for |
|---|---|---|
| **LIVE** | Observed directly against Solana mainnet RPC by this session, 2026-09-03/04 UTC | R2 (partial), R4, R7 |
| **PRIMARY-DOC** | Protocol's own published documentation | R3, R6 (partial) |
| **SECONDARY** | Press/vendor reporting, cross-read across ≥2 outlets | R1, R2, R5, R6 |

Where a finding is SECONDARY only, it is marked as needing confirmation before it is
allowed to move a threshold.

---

## R1 — Flipside is gone. The §2 source choice is resolved by elimination, not comparison.

**SECONDARY.** Flipside Crypto sold its blockchain data business to SonarX (announced
May 2026) and pivoted the company to an AI product (Edisyl). The Flipspace analytics
platform accepted data export until **2026-06-17** and is shut down.

§2 frames the decision as "Dune or Flipside — ClaudeCode to pick whichever gives complete
coverage." There is no comparison to make. One of the two candidates ceased to exist
roughly ten weeks before this tasking document was written.

**Consequence.** The source decision must be re-opened with a live candidate set, and it
is a spending decision, so it is Mando's, not mine. Candidates worth pricing:

| Candidate | Shape | Known blocker |
|---|---|---|
| **Dune** | SQL over decoded Solana tables | Credit-metered; free tier unusable (below) |
| **Bitquery** | Solana + explicit pump.fun/PumpSwap endpoints | Paid; vendor-curated layer, [E6] exposure |
| **Allium / SonarX** | Enterprise warehouse | Enterprise pricing, likely out of scale |
| **Self-indexed archival** | Own indexer over archival RPC | Highest fidelity, by far the highest cost (see R7) |
| **BigQuery public Solana** | `bigquery-public-data.crypto_solana_mainnet_us` | Community-maintained, no SLA — see note below. The only candidate carrying an `accounts` state-snapshot table (see R8), which is why it is not dismissed. |

**On the BigQuery dataset specifically**, because the obvious reading of the public record
is wrong: the widely-cited "stopped updating 2025-03-31" outage **was resolved** in early
April 2025. A *separate* multi-day lag was reported in late November 2025 with no official
Google response. So the correct characterisation is not "dead" but "community-maintained,
recurring unannounced lags, no SLA, current freshness unverifiable without a GCP project."
That is a real disqualifier for a pipeline that must be re-runnable, but it is a different
disqualifier from the one the search results suggest, and it would have been wrong to
retire the candidate on the stale headline. Verifying current freshness costs one query
against a GCP project and should happen before D1 is ruled.

**Dune free tier is disqualified on the record as published:** 2,500 credits, 120-second
query timeout, no automated/API executions, exports at 20 credits/MB — a ~125 MB lifetime
export budget. §9.6 requires "one command re-runs everything from raw pulls." That cannot
run on the free tier. Paid tiers are Analyst ($75/mo) and Plus ($399/mo). Which of these
is sufficient depends on R7 and cannot be answered without a trial query.

---

## R2 — The universe destination is PumpSwap, not Raydium. §3 is wrong for the window.

**SECONDARY (dates) + LIVE (current state).** pump.fun launched its own AMM, **PumpSwap**,
on **2025-03-20**, and graduated tokens have migrated there ever since. Raydium migration
is a pre-March-2025 legacy path.

Live confirmation of relative liquidity of the two venues. Two independent samples, six
minutes apart, are shown deliberately — see the caveat under R7 for why one would have
been misleading. Each is the 1,000 most-recent signatures for that program and the
wall-clock span of that window. Reproduce with `barrel/recon/probe_onchain.py`.

| Program | Address | 23:59Z | 00:05Z |
|---|---|---|---|
| PumpSwap AMM | `pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA` | ~20,000 tx/min | ~20,000 tx/min |
| pump.fun bonding curve | `6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P` | ~12,000 tx/min | ~30,000 tx/min |
| Raydium AMM v4 | `675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8` | ~1,538 tx/min | ~1,395 tx/min |

The **relative** ordering — PumpSwap an order of magnitude above Raydium v4 — is stable
across both samples and is the claim this finding rests on. The absolute rates are not
stable and are not used as such.

**Consequence.** §3's universe ("graduated … to a Raydium or Meteora pool") and §2's
fallback reconstruction ("reconstruct from Raydium pool creation where … the initial LP
comes from the pump.fun migration authority") would, applied literally to a 12-month
window ending 2026-09, select **the legacy tail and miss essentially the entire
population.** This is the single most consequential finding in this document: the
tasking doc as written does not describe the market it intends to study.

**Note on §5 too:** the cost model prices "Raydium 0.25% standard; Meteora dynamic."
PumpSwap charges 0.25% (0.20% LP / 0.05% protocol) — coincidentally the same headline
number, but arrived at for a different venue, and it is not the only fee leg (see R6).

---

## R3 — LP burn is structural. The rug definition's primary limb cannot fire.

**PRIMARY-DOC** (pump.fun's own program README). The `migrate` instruction is
permissionless and idempotent; it creates the PumpSwap pool and **the LP tokens received
from that pool are then burnt.**

Two things follow, and both are load-bearing.

**(a) S5 is a structural constant on this universe.** "LP tokens not burned AND not in a
verifiable locker" is the fail condition. For every pump.fun graduate the LP is burned by
the migration instruction itself. S5 will read PASS on ~100% of the universe and
contribute nothing to gate precision or recall.

**(b) §4's rug definition is partly inapplicable — and this is the outcome variable.**
The definition's first limb is "LP removed/drained ≥ 80% within 7 days." Liquidity in
this universe cannot be *removed*; the LP tokens do not exist. Reserves can only be
drained *through trading* — which is what an ordinary decline also looks like. The second
limb ("price −95% … **with LP drawdown**") inherits the same problem: LP drawdown here is
a consequence of selling, not an independent signal of malice.

So the H1 claim "the gate catches ≥ 70% of rugs" is currently defined against a label
that, on the correct universe, will fire mostly on its third limb (freeze/transfer-fee
activated post-entry) — which R4 suggests is rare — and otherwise be indistinguishable
from "went to zero fast."

**This needs a ruling before any measurement.** A pre-registration whose outcome variable
is undefined on its own universe cannot produce a GO or a NO-GO. See D3.

---

## R4 — S1/S2 look near-constant; S3/S4 are NOT vacuous. Do not assume either way.

**LIVE.** Direct `getAccountInfo` on a known graduated pump.fun mint
(`9BB6NFEcjBCtnNLFko2FqVQBq8HHM13kCyYcdQbgpump`), 2026-09-03:

```
owner program  : TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA   (SPL Token, legacy)
mintAuthority  : None      -> S1 PASS
freezeAuthority: None      -> S2 PASS
extensions     : (none)    -> S3/S4 not applicable to this mint
decimals       : 6
supply         : 999,974,449.529569
```

Only account-state fields were read. Name, symbol and URI were deliberately not
requested, per §1's metadata quarantine — the quarantine is enforced at the fetch, not
downstream of it.

**SECONDARY** reporting is consistent: pump.fun revokes mint authority at creation by
construction.

**But n = 1, and the counter-evidence is already in hand.** Two samples of 20 recent
PumpSwap transactions each, six minutes apart, split their token-program invocations:

| Sample | SPL-Token | Token-2022 | Token-2022 share |
|---|---|---|---|
| 23:59Z | 133 | 10 | 7.0% |
| 00:05Z | 147 | 48 | 24.6% |

Token-2022 mints unambiguously trade on PumpSwap, at a share that is both non-trivial and
unstable across samples. Whether any of those are *pump.fun graduates* versus routed
third-party tokens is not established, and that is exactly the question S3/S4 exist to
answer. **S3 and S4 must not be written off as vacuous** — which is the trap the S1/S2
result invites, and the reason both are reported here together.

**Failed probe, reported as a non-finding.** An attempt to sample fresh `create`
instructions from the pump program returned 0 creates in 413 signatures. That is *not*
evidence that creates have moved elsewhere: at the measured rate (R2) 413 signatures is
**about two seconds** of pump.fun activity. Reporting "creates have moved to a new
program" off that sample would have been a fabricated finding. The real measurement is
the Gate 0 census and it needs the data source.

**Consequence for H1.** If S1, S2 and S5 are near-constant PASS — which R3 establishes for
S5 and this finding suggests for S1/S2 — then three of eleven checks carry zero
discriminating power, and H1's "≥ 70% of rugs caught" rests entirely on **S6–S11**.
That should be stated in the pre-registration up front rather than discovered in
`gate_results.md`, because it changes what a NO-GO on H1 would mean.

---

## R5 — BOOST (2026-07-21) is a structural break inside the target window.

**SECONDARY.** pump.fun made "BOOST mode" the default for all tokens launched after
**2026-07-21**. Two mechanical effects, both hostile to the M0 design as written:

**(a) The graduated population changes composition discontinuously.** Reported graduation
rate moved from sub-1% to **6.7%**, roughly 8× the June average. A ~8× wider graduation
funnel is a *different population* — the marginal graduate after 2026-07-21 cleared a much
lower bar. Pooling pre- and post-BOOST graduates in one expectancy estimate mixes two
populations.

**(b) BOOST fires inside the G = 15 entry lag.** BOOST directs ~20% of migrated liquidity
into buying and burning the token **within five minutes of migration**. The G = 15 entry
therefore lands immediately downstream of a mechanical, protocol-funded buy that did not
exist before 2026-07-21. Pre- and post-BOOST G = 15 fills are not the same experiment.

**(c) §7's regime bucketing will misread it.** Terciles over "graduations/week" will
classify the post-BOOST weeks as HOT. They are not hot; a rule changed. Mando's thesis is
that HOT is "fish in a barrel" — if the HOT tercile is substantially a BOOST artifact,
H4 answers a question nobody asked. [E17]: the aggregation window can invert the sign of
the conclusion it feeds.

**Recommendation:** BOOST is a **pre-registered regime split** (a named era boundary),
never a tercile. See D4.

---

## R6 — Fee legs changed on 2026-01-10 and the cost model has no creator-fee leg.

**SECONDARY.** pump.fun overhauled creator fees on **2026-01-10**, introducing fee sharing
across up to 10 wallets per token. **PRIMARY-DOC** (the program README) still states the
creator fee "is set to 0 and is not used" — the two are in direct conflict, and the README
is the more likely stale of the pair. A live fee program is deployed and active
(`pfeeUxB6jkeY1Hxd7CsFCAjcbHA9rWtchMGdZ6VojVZ`, observed executing 2026-09-03).

**Two separate consequences:**

1. **Cost model (§5).** There is no creator-fee leg in the list. Whatever the resolution,
   the fee schedule is **time-varying across the window** and must be modelled per-era,
   not as a constant. [E8]: no spec constant ships without an observed distribution
   behind it.
2. **S7 confound.** S7 measures "deployer + wallets funded by deployer (1-hop)." Creator
   fee-share wallets are funded by *the protocol*, not by the deployer — so a set of
   economically deployer-aligned wallets can be structurally invisible to a 1-hop
   funding trace. S7 as specified will under-count aligned supply on post-2026-01-10
   tokens specifically. Worth logging the fee-share set as a separate stratum.

---

## R7 — Scale: the data volume forbids swap-level export.

**LIVE**, sampled 2026-09-03T23:59Z. Extrapolating the R2 rate table:

| Program | Implied tx/day (across both samples) | 12-month order of magnitude |
|---|---|---|
| PumpSwap AMM | ~29 M | ~10 ×10⁹ |
| pump.fun bonding curve | ~17–43 M | ~6–16 ×10⁹ |

**Caveat, stated because it matters:** these are instantaneous rates from two samples six
minutes apart, not window averages — and the bonding-curve rate moved 2.5× between them,
which is precisely why a single sample would have been reported with false precision.
Treat these as an order-of-magnitude floor on *current* activity, not as a window
estimate. The window average is a Gate 0 output, not a recon output.

**Design consequences, which hold at any plausible correction to those numbers:**

- **No raw swap export is viable.** Every per-token aggregate (first-7-day OHLC, unique
  takers, wash round-trips, cluster flow) must be computed **in the warehouse** and only
  the aggregate exported. The local cache holds per-token aggregate rows, never raw swaps.
- §9.6 ("cache raw pulls; never hand-edit intermediates") should be read as *cache the
  aggregate pulls* — the raw layer stays server-side. Worth writing into the doc so a
  later reader does not think the raw tape was skipped by accident.
- H3's point-in-time wallet cohort is the most expensive item in M0 by a wide margin: it
  requires per-wallet realized-PnL reconstruction across the whole universe at every
  weekly rebalance date. That should be costed **before** it is committed to, not after.

---

## R8 — As-of account state is the largest unpriced engineering item.

S1–S7 are **account-state** questions asked as-of a specific block. Dune's Solana surface
is transaction / instruction / account-activity shaped; it is not a historical account-state
snapshot store. So:

- **S1/S2 as-of entry** must be reconstructed from instruction history
  (`initializeMint2` + any subsequent `SetAuthority`) — not read.
- **S3/S4 as-of entry** must be reconstructed from Token-2022 extension-initialization
  and `TransferFeeConfig` update instructions.
- **S6/S7 as-of entry** require **replaying the full transfer set per token** to
  reconstruct the holder table at that block, per token, at three lags.

This is the bulk of the M0 build and it is not costed anywhere in the tasking document.
It is also the place where [E6] bites hardest: a curated convenience table that "has
holders" is an aggregation layer with its own exclusions, and using one without knowing
what it drops is the Meta-VIE failure again in a new venue.

The one candidate that would sidestep most of this is the BigQuery public Solana dataset's
`accounts` state-snapshot table — which is why R1 flags it as worth verifying rather than
dismissing on a single stale-data report.

---

## Decisions required from Mando

Nothing further should be built until D1–D3 are ruled. D4–D5 can be ruled in parallel.

**D1 — Data source and budget.** Flipside is gone (R1). Dune's free tier cannot run the
§9.6 pipeline. Ruling needed on: which vendor, and what monthly spend is authorized.
My recommendation: authorize a **one-month Dune Analyst ($75)** as a *scoping* purchase
whose only deliverable is the Gate 0 census and a measured credit-burn rate — i.e. buy the
answer to "what does the full M0 cost", not the full M0. If the census comes back clean
and the burn rate implies M0 fits inside Analyst, continue there; if it implies Plus,
that becomes a second, informed decision rather than a guess made today.

**D2 — Universe destination.** Recommend: **PumpSwap graduates are the universe**, with
the pre-2025-03-20 Raydium era either excluded outright or carried as a separate,
explicitly labelled stratum that is never pooled with the main result. Note the
consequence: an all-PumpSwap window starts 2025-03-20, which still comfortably clears
§3's "≥ 12 months" requirement.

**D3 — Rug definition.** The current definition's primary limb cannot fire on this
universe (R3). This is a redefinition of the study's outcome variable and is squarely
the Architect's and Mando's call, not the builder's. What I can offer is that the
distribution should be measured before the threshold is set, per [E8] — i.e. the honest
sequence is: Gate 0 census → measure the observed shapes of post-graduation reserve decay
→ *then* pre-register the rug threshold, in a dated amendment, before any gate metric is
computed.

**D4 — BOOST.** Recommend: pre-registered era split at 2026-07-21, reported alongside but
never merged into the §7 terciles, and G = 15 results reported separately per era.

**D5 — Standing open items from §12**, still unanswered: bankroll confirmation ($2,000 or
the real number), and the "Mando's list" wallet set for the H3 cohort variant.

---

## Re-check obligations ([E15])

| Item | Claim | Re-check by |
|---|---|---|
| R1 | Flipside/SonarX has no self-serve tier | 2026-12-03 |
| R1 | BigQuery public Solana freshness | **before D1 is ruled** — needs one query from a GCP project; the "dead since 2025-03-31" headline is wrong |
| R4 | S1/S2 constant across the universe | Gate 0 census (n=1 today) |
| R4 | Token-2022 share among pump.fun *graduates* specifically | Gate 0 census — today's samples are of PumpSwap flow, not of graduates |
| R5 | BOOST graduation-rate figures | Gate 0 census — these are press numbers, not measured |
| R6 | README "creator fee = 0" vs 2026-01-10 fee-sharing reporting | before `cost_model.md` — resolve against the live fee program, not the docs |
| R7 | Activity rates | Gate 0 census — instantaneous rates, observed unstable across 6 minutes |
