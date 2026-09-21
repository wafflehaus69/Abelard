# BARREL M0 — A5 fee legs, measured

> **SUPERSEDED IN PART — 2026-09-21.** The balance-delta method below was replaced by
> decoding PumpSwap's own swap events, which name every fee leg. The "unidentified variable
> leg" is **identified: it is `coin_creator_fee`.** See **Part 2** at the end of this file.
> Part 1 is kept unedited because its two method defects are the reason Part 2 exists.
> **One Part 1 claim is now contradicted:** its explanation for zero-leg sells ("PumpSwap
> can take its fee on the output token") is wrong — every fee in the events is quote-
> denominated in both directions. The zero-leg sells were a blind spot of the balance-delta
> probe, not base-token fees.


**Workstream:** BARREL-M0-RECON · **Builder:** ClaudeCode · **Date:** 2026-09-04
**Serves:** Amendment v1.1 §A5 ("resolved against the live fee program, not the README")
**Closes:** R6's README-versus-reporting conflict, **for the current era only**
**Reproduce:** `python barrel/recon/probe_fee_legs.py 60`

---

## What was measured and why

R6 recorded a direct conflict with documentation on both sides and measurement on
neither: pump.fun's program README says the creator fee "is set to 0 and is not used";
2026-01-10 reporting describes a creator fee-sharing overhaul across up to ten wallets.
§5 needs a fee schedule, and [E8] forbids shipping a spec constant without an observed
distribution behind it.

So the schedule was read off executed swaps rather than off either document: take recent
PumpSwap transactions, compute the quote-side (SOL + WSOL) value change per **owner**,
call the largest gain the principal, and treat every other owner that gained quote-side
value as a fee leg expressed in bps of the principal.

**n = 60 single-hop swaps**, sampled 2026-09-04T00:47Z.

---

## Two method defects found and fixed before reporting

Recorded because the wrong numbers were superficially plausible, and a fee schedule that
feeds §5 expectancy is exactly the kind of constant that gets believed once written down.

**Defect 1 — multi-hop contamination.** The first run took every PumpSwap transaction.
On a routed or arbitrage transaction the largest quote-side gain is an intermediate hop,
so the other hops read as fee legs. That run reported a median fee load of **94.8 bps and
a maximum of 7,429 bps** — i.e. a 74% fee. Fixed by requiring PumpSwap to be the only
value-moving program in the transaction.

**Defect 2 — wrap/unwrap read as a payment.** The second run still keyed deltas by
*account*. A trader wrapping SOL moves value from their wallet into a temporary WSOL
account they own; closing it moves the lamports back. Keyed by account, one trader's own
wrap looks like a stranger receiving a large payment — which produced a phantom "fee
recipient" collecting **1,132 bps across three swaps**. Fixed by aggregating deltas by
**owner**, which makes a wrap net to zero, because that is what it is.

Neither defect announced itself. Both were caught because the resulting number was
implausible on its face — which is not a reliable detector, and is the argument for the
IDL-based confirmation named at the bottom of this page.

---

## Result

### Leg count is bimodal by direction, and that is a measurement limit, not a finding about fees

| Direction | n | 0 quote-side legs | ≥1 leg |
|---|---|---|---|
| `Sell` | 32 | 25 | 7 |
| `BuyExactQuoteIn` | 18 | 5 | 13 |
| `Buy` | 10 | 2 | 8 |

A swap showing zero quote-side legs is **not fee-free**. PumpSwap can take its fee on the
output token, and a quote-side probe cannot see a fee paid in the base token. Stated
rather than inferred away: this probe measures the fee schedule *on the quote side*, and
`Sell` is the direction where it is largely blind.

### Where legs are visible, the shape is consistent: a fixed matched pair plus a variable leg

Every one of the 28 swaps with visible legs had **exactly three**. The two smallest are
always a matched pair, at either 2.5 + 2.5 bps or 5.0 + 5.0 bps. The third varies by two
orders of magnitude.

```
  7 x    5.0 + 2.5 + 2.5 bps        <- the clean case, 10 bps total
  4 x  182.1 + 5.1 + 5.1 bps
  4 x   94.8 + 2.5 + 2.5 bps
  4 x   89.8 + 2.5 + 2.5 bps
  1 x  141.4 + 5.1 + 5.0 bps
  1 x   69.9 + 2.5 + 2.5 bps
  1 x   59.9 + 2.5 + 2.5 bps
  1 x   28.0 + 2.5 + 2.5 bps
  1 x   23.0 + 2.5 + 2.5 bps
  1 x   10.0 + 5.0 + 5.0 bps
```

The recurring-recipient table splits cleanly into two populations, which is the strongest
structural evidence here: recipients that appear often and *always small*
(`62qc2CNX…` 20 swaps, median 2.5 bps; `A7hAgCzF…` 7 swaps, median 2.5 bps), and
recipients that appear a few times and *always large* (`Byjq9v52…` median 182.1 bps;
`7b6RdxS2…` median 94.8 bps; `DD4qZDhg…` median 89.8 bps). Fixed infrastructure and
per-token destinations behave differently, and they behave differently here.

### What this establishes

1. **The README is wrong for the current era, and R6 resolves against it.** There are
   three distinct value-receiving legs on a visible swap, not zero. "Creator fee is set
   to 0 and is not used" does not describe what is executing on mainnet today.
2. **The 5.0 bps leg matches PumpSwap's documented 0.05% protocol fee exactly.** That is
   the method's own calibration check, and it passes.
3. **The 0.25% headline is not a single leg and must not be modelled as one.** The 0.20%
   LP portion stays inside the pool, so it sits *within* the principal and is invisible as
   a leg — consistent with the documented 0.20/0.05 split.

### What this does NOT establish — the variable third leg

The 23–182 bps leg is **unidentified**. The leading hypothesis is that it is the
**per-token creator fee** under the post-2026-01-10 fee-sharing regime: it recurs per
recipient, is stable per recipient across swaps, and varies across recipients — exactly
how a creator-configured rate would behave. That is a hypothesis consistent with the
evidence, not a finding, and it is not written into §5 on this basis.

**If it is confirmed, it is a first-order cost-model result**, because a per-token fee
reaching ~1.8% *per leg* dwarfs every other cost in §5 and cannot be carried as a
constant. §5's round-trip budget ("if median round-trip on the passed set exceeds 8%,
that is itself a finding") would be decided largely by this term.

**Confirmation path**, neither of which needs the blocked archival source: parse the
PumpSwap IDL swap event, which carries the fee fields by name, or read the pool account's
creator field and match it against the large-leg recipient.

---

## Scope limits

- **Current era only.** A5 wants a schedule across three eras (pre-2026-01-10,
  2026-01-10 → BOOST, post-BOOST). Historical eras need the archival source blocked at A1.
- **n = 60**, one sampling moment. Adequate to establish the leg *structure*; not adequate
  for the *distribution* of the variable leg, which is the number §5 actually needs.
- **Quote side only**, so `Sell` fees are largely unmeasured (see the direction table).
- Recurring-recipient identities are on-chain addresses and are treated as data. No token
  metadata was fetched at any point, per §1 / v1.1 §A7 — the quarantine holds at the fetch.

## A finding for §5 that is not about fees

**22–30% of PumpSwap swaps sampled were routed or multi-hop**, across two runs. §5 models
a fill as a single constant-product swap against one pool. Mando's own fills would route
through an aggregator like anyone else's, which means the realistic entry is not the
model's entry. Worth an explicit decision in v1.2: either model the routed fill, or
pre-register single-pool execution as a stated simplification with its direction of bias
declared.


---

# Part 2 — fee legs read from PumpSwap's own events (2026-09-21)

**Reproduce:** `python barrel/recon/decode_pumpswap_events.py 200` ·
`python barrel/recon/fee_schedule_history.py`
**IDLs:** pinned in `recon/idl/` with sha256 in `PROVENANCE.json` (fetched 2026-09-21).

## Method

PumpSwap emits an Anchor `BuyEvent` / `SellEvent` on every swap carrying each fee leg by
name with its configured rate. Reading it is a lookup, not an inference. It also works on
routed swaps, so Part 1's single-hop filter, and the 22–30% of flow it discarded, are no
longer needed.

Three safeguards, each added because the naive version would have been wrong:

1. **Truncation-tolerant decoding.** Older swaps emit shorter events. Fields past the end of
   the bytes are recorded **absent**, never zero ([E1]).
2. **Forgery resistance.** A `Program data:` log line can be printed by *any* program in a
   transaction; on a routed swap a hostile program could print a fake `BuyEvent`. Log-path
   events are now accepted only inside a PumpSwap invocation frame; the self-CPI path is
   unforgeable by construction. The first version read every log line. No forgery was
   observed, but that is an absence of evidence, and §0 assumes the data is adversarial.
3. **Conservation, not field names, decides what the trader pays.** See below.

## Result — which legs the trader pays

| Leg | Trader pays it? | Evidence |
|---|---|---|
| `lp_fee` | **yes** | conservation |
| `protocol_fee` | **yes** | conservation |
| `coin_creator_fee` | **yes** | conservation |
| `buyback_fee` | no — 50% carve-out *of* protocol fee | `buyback_basis_points = 5000` in GlobalConfig; realizes 2.5 bps against a 5 bps protocol fee |
| `holder_rewards`, `cashback` | no — carve-outs / rebates | conservation excludes them |

The conservation identity `gross − net = lp + protocol + creator` held on **168 of 171**
swaps (3 failures excluded, not estimated), and **40 of 40** after the forgery fix.

**A field-naming trap worth recording.** In the `buy` variant `user_quote_amount_in` is what
the user paid and `quote_amount_in` is what reached the pool; in `buy_exact_quote_in` the
**same two names mean the opposite.** Read naively, 43 of 63 buys failed conservation. The
decoder now maps each variant explicitly and **refuses** an unknown variant rather than
guessing.

## Result — current-era trader cost, per leg (n=168, 2026-09-21)

| | n | median | p90 |
|---|---|---|---|
| no creator fee | 44 | **30 bps** (flat) | 30 bps |
| creator fee charged | 124 | **104 bps** | — |
| all | 168 | 79 bps | 125 bps |

Round trip is two legs, before slippage. Configured creator rates seen: 5–250 bps;
GlobalConfig caps them at **300 bps per leg** (`max_configurable_creator_fee_bps`).
Max values of 2,500 bps in the raw output are **dust swaps** (a 4-lamport trade paying a
1-lamport rounded fee) and are irrelevant at a $20 ticket.

**Prevalence is NOT estimated here and must not be.** The share of swaps carrying a
creator fee was **30% in one sample and 73% in another** taken minutes apart. A few seconds
of flow is dominated by whichever tokens are hot, so prevalence is a property of the token
mix, not of the protocol. It has to be measured per token over the window.

## Result — the fee schedule has a history, and it moves the M0 window

Every schedule change is an admin transaction that emits a dated event. The current admin's
full history — 6,422 signatures, **75 successful**, 2025-02-19 → 2026-09-16 — decodes to:

| Date (UTC) | Event | Change |
|---|---|---|
| 2025-03-17 | UpdateFeeConfig | lp 20 / protocol 5; **no creator field in this event version** |
| 2025-05-12 | UpdateFeeConfig | lp 20 / protocol 5 / **creator 5** — first appearance of a creator leg |
| 2025-07-18 | UpdateFeeConfig | unchanged rates; creator-authority field added |
| 2025-09-02 | UpdateFeeConfig ×2 | **undecodable** — values ~10¹⁹ bps, i.e. a different layout at that time. Recorded as unknown, NOT as fees |
| 2025-11-14 | ReservedFeeRecipients | recipient change only |
| **2026-09-09** | UpdateCreatorFeeConfig | **creator fee becomes configurable, max 100 bps** |
| **2026-09-12** | UpdateCreatorFeeConfig | max raised to **300 bps** |

**If this holds, it is the most consequential cost-model finding so far:** per-token
creator fees of 30–250 bps exist only since **2026-09-09** — twelve days — and essentially
none of the M0 window is priced like today. The current-era sample in the table above would
**overstate** window-era costs badly if used as the §5 constant.

**Why it is marked provisional, not established:**

1. **Admin handovers.** Control passed from `8LWu7QM2…` to the current admin on 2025-02-24
   and *again* on 2025-08-28, so it returned to `8LWu7QM2…` in between. Changes signed by
   that key are missing. No `CreateConfigEvent` is in this history either, so the
   **launch-day regime is unobserved.**
2. **Two undecodable events on 2025-09-02.** Something changed that day; what, is unknown.
3. **Tiering lives in a second program.** Every swap calls `get_fees` on the pump fee
   program (`pfeeUxB6…`), whose `FeeConfig` holds **fee tiers keyed on market cap**, each
   setting its own lp / protocol / creator bps, plus `flat_fees` and `exotic_flat_fees`.
   That explains what GlobalConfig alone does not (lp 25 vs the global 20; an lp 2 /
   protocol 93 tier). Its own admin history (`UpsertFeeTiersEvent`, `UpdateFeeConfigEvent`,
   `SetExoticFlatFeesEvent`) is **not yet read.**

## What this means for §5

The fee a trader paid is a function of **era × market cap at the moment of the swap × the
token's own creator setting**. Reconstructing that from config history is a model, and
[E6] warns what models of an aggregation layer do. The ground truth is the **realized fee
in each historical swap event**, which already bakes all three in.

**Recommendation for v1.2:** price §5 fees from realized swap events decoded out of BigQuery
`Instructions` (clustered on `program_id`, so affordable), and use the config history only
as the cross-check that dates the era boundaries. Blocked on a billing project ID.

## Open

- `8LWu7QM2…` admin history (closes gap 1).
- Pump fee program tier history (closes gap 3).
- The 2025-09-02 layout (gap 2): needs the IDL as it stood that day, or a byte-level read.
- One zero-fee `buy` swap, unexplained. **Not** the BOOST buy-and-burn — per the IDL that
  is its own instruction emitting `BoostBuyAndBurnEvent`.

## Caveat added 2026-09-21, after publication

**Every Part 2 figure was drawn from version-0 transactions only.** The scripts requested
`maxSupportedTransactionVersion: 0`, mainnet now carries version-1 transactions, and those
requests failed on them. They were logged as "RPC failures" (72 of 312 fetches in the
200-swap run). That's a silent exclusion of a whole transaction version, and it may not be
random with respect to who trades. Fixed in all scripts, and the figures above stand only
with this caveat attached. See `A1_BIGQUERY_FITNESS.md` §4.

Also verified on chain: PumpSwap emits its swap events as `Program data:` **log lines**, not
as self-CPIs. The decoder's in-frame check on log events is therefore the only thing standing
between a hostile program in a route and a forged event.

---

# Part 3 — re-run including version-1 transactions (2026-09-21, MR-5 order 5)

**Reproduce:** `python barrel/recon/decode_pumpswap_events.py 250` · raw output
`recon/out/fee_legs_v1_2026-09-21.txt`

This replaces the version-0-only caveat on Part 2. The decoder now requests version-1
transactions and tallies the version of every transaction it fetches, so the claim that
current flow is covered can be checked rather than assumed.

| | Part 2 (version-0 only) | Part 3 (all versions) |
|---|---|---|
| Transactions fetched | 312, with 72 silently dropped (23%) | **419**: 220 v0, 138 legacy, **61 v1 (15%)**, 27 dropped for rate limits (6%) |
| Conservation holds | 168 / 171 | **187 / 188** |
| Per leg, no creator fee | 30 bps (flat) | **30.0 bps (flat)**, n = 78 |
| Per leg, creator fee charged | median 104 bps | **median 99.1 bps, max 138**, n = 109 |

**The flat 30 bps is stable across every sample and both transaction-version regimes. It is
the number §5 can rely on for tokens without a creator fee.** The creator-fee leg moved
between samples (median 104 → 119 → 99 bps across three runs), which is the expected
behaviour of a per-token setting drawn from whichever tokens were hot at the moment.

Still true, and still binding:

* **Creator-fee prevalence is not estimable from live snapshots.** It was 30%, 73%, 56% and
  58% in four samples. It is a property of the token mix and must be measured per token over
  the window.
* **This is the current era only.** The schedule history (Part 2) indicates per-token creator
  fees are configurable only since 2026-09-09, so these figures must not be projected back
  over the M0 window. The window's own realized fees need an archival source that carries
  program logs. BigQuery does not (`A1_BIGQUERY_FITNESS.md`), and Dune is under test.
* **Limit of this run:** it counts transaction versions *fetched*, but the process was
  started before the per-version *event* breakdown was added, so how many of the 188 swap
  events came from the 61 version-1 transactions is not reported. Every fetched transaction
  went through the same decoder, so version-1 swaps are in the sample. Their share is not.
