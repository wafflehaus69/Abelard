# CD-GAP2-P3-VERIFY — the cross-check on fixed membership, and MU's break

Built 2026-09-22 on `cd-gap2-p3`, off `main` at `883baf7`. Measured against the live
snapshot (Basilic, generated 2026-09-21). **The 44–48% band decision is Mando's; this
document supplies the data it was waiting on, and states a recommendation.**

## The finding in one line

**The published cross-check changes its numerator membership three times and says so
only in a column.** It is a matched sum — whoever reports a quarter is in it that
quarter — which is the right rule for currency and the wrong one for a level. The ratio
is read as a level.

## What that has already done

Between 2025Q2 and 2025Q3 the published series rose **46.73% → 51.08%**, and reads as
the supplier side pulling ahead of the buyers. It was Micron arriving.

| 2025Q3, read three ways | value |
|---|---|
| published matched series (2 members → 3) | 51.08% |
| the same quarter on the two names that span both quarters | **45.26%** |
| the prior quarter, same two names | 46.73% |

The underlying ratio **fell 1.47pp**. The line a reader watched **rose 4.35pp**. Nothing
was wrong with either number; they are not the same measurement, and only one column of
member counts distinguished them.

The earlier entry is larger still: NVDA joining AMD at 2024Q1 stepped the published
series **+29.05pp** (4.43% → 33.49%).

## The two legs, on constant membership

Denominator: the hyperscaler bucket TTM, **5 members throughout both windows** — no
denominator break inside either leg (`capex_members_changed` is false on both).

### Two-name — NVDA + AMD, 2024Q1 … 2026Q2 (10 quarters)

| quarter | ratio | DC revenue TTM | hyperscaler capex TTM |
|---|---|---|---|
| 2024Q1 | 33.49% | $54.78B | $163.57B |
| 2024Q2 | 40.31% | $74.13B | $183.89B |
| 2024Q3 | **44.55%** | $92.12B | $206.79B |
| 2024Q4 | **46.25%** | $110.59B | $239.09B |
| 2025Q1 | **47.66%** | $129.10B | $270.89B |
| 2025Q2 | **46.73%** | $146.06B | $312.56B |
| 2025Q3 | **45.26%** | $161.67B | $357.17B |
| 2025Q4 | **44.62%** | $183.64B | $411.53B |
| 2026Q1 | **44.07%** | $212.47B | $482.15B |
| 2026Q2 | **44.51%** | $252.09B | $566.37B |

**Eight consecutive quarters inside 44–48%**, 2024Q3 through 2026Q2. The two quarters
outside it are the first two, while NVIDIA's datacenter revenue was still ramping into
the window. Range across the eight: **44.07–47.66%**, a spread of 3.59pp.

### Three-name — NVDA + AMD + MU, 2025Q3 … 2026Q2 (4 quarters)

| quarter | ratio | DC revenue TTM |
|---|---|---|
| 2025Q3 | 51.08% | $182.43B |
| 2025Q4 | 50.33% | $207.11B |
| 2026Q1 | 50.73% | $244.61B |
| 2026Q2 | 53.78% | $304.60B |

MU's contribution is widening: the step it adds to the same quarter runs **+5.81pp,
+5.70pp, +6.66pp, +9.27pp**. Micron's HBM is growing faster than NVDA and AMD's
datacenter revenue combined, so the three-name leg is not the two-name leg plus a
constant.

## Recommendation for the band decision

1. **Register 44–48% on the TWO-NAME leg only, effective 2024Q3** — the leg and the
   window it was observed on. It has held eight consecutive quarters with 3.59pp of
   spread inside a 4pp band, through a period when the denominator itself nearly
   tripled ($206.79B → $566.37B TTM). That is the strongest claim the data supports.
2. **Register nothing on the three-name leg.** Four observations is not a distribution
   (E8), and its entrant's contribution is still widening quarter over quarter. Revisit
   when it has eight — 2027Q3 on current filing cadence.
3. **The band is a LEVEL band and is not the dcrev dead-band.** CD-3b measured 9pp for
   `dcrev:supplier`, which governs quarter-to-quarter MOVES in the phase ladder. Two
   different quantities; neither substitutes for the other.
4. **A breach announces nothing until Mando rules what it means.** The leg publishes;
   no alert is wired to it in this unit.

## What this unit built

* `snapshot.crosscheck_cohorts` — one series per fixed membership, cohorts built from
  entry order rather than named, so a fourth admitted supplier produces its own leg and
  its own dated break with no code change.
* `breaks` — each entry dated, measured as the same quarter read with and without the
  entrant, which is the only comparison that isolates it.
* Both rendered: the dashboard's supplier view and the PDF's supplier section, each
  carrying the break callout beside the legs.
* `tests/test_crosscheck_cohorts.py` — seven tests, including the live shape in
  miniature: an entrant large enough that the matched line rises while the ratio falls.

## What it did not do

The published matched series is **unchanged** and still leads the page: it is the right
series for "is the supplier side current", and P3's legs sit beneath it rather than
replacing it. The thesis line still reads the matched series and still says no band is
registered — wiring it to a ratified band is a separate unit, and belongs after the
ruling, not before it.
