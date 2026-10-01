# Factory class — do high-fan-out senders split into hubs and factories? (MR-12 ruling 4, order 2)

**Date:** 2026-10-01 · **Spent:** 39.2 of 60 credits (one-day validation 5.1, calibration week 34.0)
**Query:** `recon/sql/factory_overlap_week.sql` (generator `recon/gen_factory_overlap.py`). **Evidence:** `recon/out/run_factory_overlap_week_*.json`. Histogram output only; no wallet in it.

## Answer

**Yes, where there is enough to look at.** Among senders with 100 or more recipients that went on to trade, the population is two clusters with almost nothing between them, and every labelled exchange sits in one of them.

## What was measured

Window: SOL transfers 2025-06-05 → 06-16 (the calibration week's scan), transfers of 0.001 SOL or more. For every sender that paid 400 or more distinct recipients, each recipient's **first pump.fun bonding-curve trade within 24 hours of first being paid** was taken, and the share of those recipients whose first trade was on the sender's single most common token was computed. 6,049 senders had at least one such recipient; 13 of them are labelled exchange wallets.

Senders by that share, grouped by how many of their recipients traded:

| Recipients that traded | Senders | Share under 10% | 10–90% | 90–99% | **100%** | Labelled exchanges |
|---|---|---|---|---|---|---|
| 100 or more | 234 | **130** | 15 | 5 | **84** | 12, all under 10% |
| 20–99 | 135 | 93 | 33 | 4 | 5 | 1, under 10% |
| 5–19 | 584 | 80 | 486 | 4 | 14 | 0 |
| 1–4 | 5,096 | — | — | — | — | 0 (too few recipients to read a share) |

In the top row, 214 of 234 senders are at one extreme or the other. **84 senders paid 100 or more wallets whose first trade, within a day, was every one on the same token.** That is the factory the ruling describes. The 130 at the other end, with all 12 labelled exchanges among them, are hubs.

By fan-out, the 100%-concentration senders with 20 or more trading recipients are small-to-mid: 12 at fan-out 256–511, 52 at 512–1,023, 18 at 1,024–2,047, 5 at 2,048–4,095, and 2 above that. The hubs run all the way up to fan-out above 100,000.

## What this supports, and what it does not

* **Supports shipping the class in v1.2**, defined on a sender's recipients' first trades. A rule of the form "at least N trading recipients and at least X of them on one token" separates cleanly at N = 100. N and X are not chosen here.
* **Below 100 trading recipients the split blurs.** At 5–19 most senders are in the middle, which is what small counts do to a share. The class needs a floor on recipients, and that floor decides how many factories it catches.
* **First action means a bonding-curve trade.** A factory whose wallets first act on PumpSwap after graduation is not seen. That scan was left out to stay inside the budget.
* **One week, one era.** Not repeated post-BOOST.
* **The readiness run's 1,128-wallet funder was a creator funding its own wallets**, and the week has one like it (1,391 wallets). Those are already in the aligned set by the creator-funded rule. The factory class adds the case where the funder is *not* the creator.

## Effect on collapse today

None. `actors.classify_funder` takes a `factories` list and treats its members as linking; the list is empty until v1.2 defines the rule. With it empty, a factory is classed `hub` and does not link, so actor counts remain over-stated for these senders.
