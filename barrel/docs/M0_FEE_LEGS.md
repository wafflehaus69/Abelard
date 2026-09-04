# BARREL M0 — A5 fee legs, measured

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
