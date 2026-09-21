# CD-FRONTIER-VERIFY — an aggregate's frontier is where its membership is

Built 2026-09-21 on `cd-frontier`. **369 tests pass.** Not merged, not deployed —
held for Mando's word.

## The defect, live since 2026-09-12

Oracle's fiscal quarter ended 2026-08-31, which aligns to calendar 2026Q3 about
six weeks before the calendar-year filers report Q3. Every matched-membership sum
published "the newest quarter any member has reached", so from Sep 12 every one
of them led with ORCL alone:

| surface | what it published |
|---|---|
| thesis line | "Panel capex TTM **$75.66B is falling**" — was $614.18B |
| total panel | 2026Q3, **1 member**, YoY **+176.0%** (ORCL's own growth) |
| hyperscaler bucket | INSUFFICIENT-MEMBERSHIP — the whole bucket dark |
| composition events | "2026Q3 **AMZN, GOOGL, META, MSFT left**" — four phantom exits |
| supplier cross-check | **367.2%** — NVDA's $277.8B over ORCL's $75.7B |
| A3 one-quarter-ahead | **silenced** — ORCL moved the demand frontier to Q3 |

Nine days. It recurs every quarter ORCL files: its quarters end in Aug, Nov, Feb
and May. No false alert reached the queue (0 capex items pending) and nothing was
emailed (morning-briefs is not loaded).

## The ruling — Mando, 2026-09-21

> A trailing quarter joins an aggregate only once the members who have reported
> it cover **`COVERAGE_FLOOR` (0.95)** of the prior quarter's dollars.

The already-ratified constant, reused. In code it is an **alias**,
`FRONTIER_COVERAGE_FLOOR = COVERAGE_FLOOR`, never a second literal — pinned by
`test_the_floor_is_the_ratified_coverage_floor_not_a_copy`.

**Measured before building (E8).** Coverage of the prior quarter's dollars,
every aggregate, every recent quarter pair: **100%**. The partial frontier:
**9.1%** (total), **9.8%** (hyperscaler). Arrivals cannot lower coverage by
construction, so the knee is absolute — any floor from ~0.10 to ~0.99 trims the
same single quarter and nothing else.

## Two things added beyond what was approved — for your word

**1. A 90-day escape.** Coverage also dips when a member genuinely *leaves* —
measured, hyperscaler 2017Q1 at 78% was Amazon dropping out of the concept — and
a gate with no end would freeze that aggregate on its last complete quarter
forever. So a quarter publishes regardless once **90 days** have passed since its
calendar end, with the missing names listed as behind on filing. 90 is not fitted:
it is the latest regular deadline for any periodic report (Form 10-K,
non-accelerated filer). `FRONTIER_MAX_WAIT_DAYS`. Strike it and the gate still
fixes the defect; it just has no exit for a real departure.

**2. The same gate on every matched sum, not only the headline.** Total, all
three buckets, the credit leg, and the supplier DC-revenue combined series. The
cross-check and A3 were broken by the hyperscaler series, so gating it fixes them
for free; the supplier combined series is gated for consistency (NVDA alone at Q3
covers 75.5% and is held — its early read already has its own home in A3).

## A bug the tests caught in my first cut

It walked back comparing each trailing quarter with its immediate predecessor.
With ORCL alone at **both** 2026Q3 and 2026Q4, Q4 was judged against Q3 — itself
partial — and ORCL covers 100% of ORCL, so the one-member tail published anyway.
Rewritten as a forward pass: each quarter is measured against the last
**accepted** quarter, and once one is held the whole tail is held, so the
published series is always a prefix with no holes.
`test_two_trailing_partial_quarters_are_both_held` pins it.

## Verified against live data, not fixtures

Production code and branch code run on two identical copies of the live DB, same
fixed instant, alerts routed to a scratch queue:

| | production | branch |
|---|---|---|
| total | 2026Q3 · $75.66B · 1 member · +176.0% | **2026Q2 · $614.18B · 22 · +83.1%** |
| hyperscaler | INSUFFICIENT-MEMBERSHIP (1) | **PLATEAU (5)** |
| builder / landlord | ACCELERATING (12) / PLATEAU (5) | unchanged |
| phantom exits | AMZN, GOOGL, META, MSFT | **none** |
| cross-check | 2026Q3 · 367.2% | **2026Q2 · 53.8%** |
| A3 | silent | **NVDA 2026Q3** |

Thesis line after:

> Panel capex TTM $614.18B is rising and reads ACCELERATING; credit issuance is
> rising; the forward-commitment total is refused, so it has no direction. The
> supplier cross-check reads 53.8% from 50.7% a quarter earlier, against no
> pre-registered band. Hyperscalers: 2 ACCELERATING, 2 PLATEAU, 1 DECELERATING.
> The panel commitment total is REFUSED-MIXED-BASIS. **The panel stands at
> 2026Q2; 2026Q3 is partial — 1 of 22 members have reported, 9% of 2026Q2's
> dollars.**

The last clause is new and always present — it reads "no later quarter is
partially reported" when nothing is held, so a reader sees the clause change
rather than having to notice it appear.

Production's queue timestamp was unchanged by the test, and the scratch tree was
removed.

## When it clears on its own

The calendar filers report Q3 by early November. Once their 10-Qs land, coverage
passes 95% and 2026Q3 joins every aggregate without anyone touching it. Worst
case, 2026-12-29.
