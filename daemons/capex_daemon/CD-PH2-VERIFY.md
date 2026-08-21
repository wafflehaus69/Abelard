# CD-PH2-VERIFY — View 0, native SVG views, the phase page

Produced 2026-08-21 on `cd-ph2` (branched from `cd-ph1`). **165 tests pass.** Nothing merged,
nothing pushed. Every figure below computed by the built code against live SEC data.

New modules: `svgcharts.py` (charts), `brief.py` (PDF phase page). `charts.py` — the CD-2 matplotlib
pipeline — is **unchanged and still running**; see §7 for why that sentence needs writing down.

---

## 1. CRWV bucket — the order's premise did not hold, and the slip was mine

The order asked me to resolve a conflict between "the §3 'hyperscaler bucket' label" and the ruled
builder placement. There is no such label in CD-1-SPEC §3. The §3 references to CRWV (lines 64, 78,
92, 99) are all about its **tier** — CORE, and the deleted DEGRADED-SHORT — not its bucket, and
line 337 places it under "Leveraged builders". Config agrees: `universe.csv` has `CRWV,builder`,
and the loader returns `builder`.

The "hyperscaler" pairing came from **CD-PH1-VERIFY §3**, which I wrote: I ran the matched-membership
graduation backtest against the hyperscaler bucket because it was the longest clean series, and
captioned it as if CoreWeave belonged there. Config was never wrong. The doc was.

**Corrected in place**, re-run against the builder bucket, and the corrected version is the stronger
demonstration:

| quarter | without CRWV | with CRWV | result |
|---|---|---|---|
| 2025Q2 | +347.9% (n=6) | +347.9% (n=6) | guard held — no year-ago TTM window |
| 2025Q3 | +353.3% (n=6) | +353.3% (n=6) | guard held — no year-ago TTM window |
| 2025Q4 | +229.6% (n=7) | **+45.8%** (n=8) | legitimately included |
| 2026Q1 | +232.4% (n=7) | **+122.1%** (n=8) | legitimately included |
| 2026Q2 | +269.5% (n=7) | +269.5% (n=7) | guard held — series ends 2026Q1 |

CoreWeave's entry moves the builder sum **down 184pp**, because it is large in dollars ($16.60B TTM)
and growing slower than the small builders around it. The hyperscaler version moved under 2pp and
made the guard look like a rounding detail.

## 2. The defect View 0 surfaced — a level is not a comparison

Matched membership makes a *YoY* safe: both sides taken over names present on both sides. It does
nothing for a plotted **level**, and View 0 is levels. Measured on the live panel:

```
total capex, matched membership, member count per quarter across 66 quarters:
122222232555566666666667778878888777899888887777777789999999999999
                                              1 -> 12 members
```

Drawn as-is, most of the hero line's rise is name arrival. Three candidate rules were measured
against the live frontier rather than argued:

| start | qtrs | names | $ share of reported | members |
|---|---|---|---|---|
| 2011Q3 | 60 | 2 | 34.9% | MSFT, ORCL |
| 2016Q1 | 42 | 4 | 63.0% | + EQIX, GOOGL |
| **2018Q2** | **33** | **5** | **98.2%** | **+ AMZN** |
| 2022Q4 | 15 | 9 | 99.6% | + APLD, CORZ, MARA, WULF |
| 2024Q3 | 8 | 11 | 99.7% | + CLSK, HUT |

The knee is sharp. Reaching past 2018Q2 drops Amazon and costs **35pp of coverage in one step**;
stopping short of it buys 1.4pp for fifteen fewer quarters. So the rule is **coverage first, history
second**: the longest constant-membership window whose members still cover 95% of the dollars
reported at its end. Any floor between roughly 0.66 and 0.98 selects the same window, which is why
`COVERAGE_FLOOR` is a threshold and not a tuned parameter.

Maximising *members × quarters* was tried first and selected 4 names over 44 quarters — **dropping
Amazon and Meta from an AI-capex chart.** Rejected on that output.

**Chosen, live:** 5 names (AMZN, EQIX, GOOGL, MSFT, ORCL), 2018Q2–2026Q2, 98.2% coverage.

## 3. Filing lag is named, not silently absorbed

98.2% is a share of issuers *reporting through 2026Q2*. Six names have not filed it yet and are
therefore absent from the last point — including **META at $75.7B**, roughly 15% of the panel. That
is not a contraction and must not read as one, so the constant panel returns its `lagging` list and
both the dashboard and the phase page print it:

> Behind on filing, so absent from the last point: META (last 2026Q1, $75.7B), CRWV (2026Q1,
> $16.6B), DLR ($3.3B), IREN ($1.9B), GLXY ($1.4B), WYFI ($0.4B).

## 4. The jaws — the second thing View 0 got wrong

First build rebased panel credit onto panel capex. Both were "the panel", but they are **not the
same names**: capex coverage selects 5, issuance coverage selects 2 (AMZN, GOOGL). The chart was
drawing a gap between different companies and calling it a divergence. The rebase factor was ×30.

Fixed by computing the jaws over the **intersection** — same names, same window, both legs:

| leg | 2019Q4 | 2026Q2 | multiple |
|---|---|---|---|
| capex, AMZN+GOOGL | $40.4B | $305.4B | **×7.6** |
| credit, AMZN+GOOGL | $1.9B | $192.1B | **×100.6** |

**That is the Hayes claim, live and on matched membership:** for the same two companies over the
same 27 quarters, credit issuance grew thirteen times faster than the capex it funds.

## 5. Log scale, and why the rebase is gone entirely

Rebasing then broke the axis. Credit rescaled ×21 pushed the y-maximum to $4T while the panel is
$491B, flattening every real line to the floor — the builder bucket ($8.9B) and REIT were invisible
against hyperscaler ($477B). Verified by rendering the PDF and looking at it.

The composite is now **log-scaled and unrescaled**. On a log axis a rebase is only a vertical shift,
so both legs are drawn in true dollars and the divergence is read off the **slopes**. This fixes the
jaws distortion and the four-orders-of-magnitude legibility problem at once, and removes the scale
factor the reader previously had to divide back out.

## 6. Forward commitments — the leg is REFUSED, and says so

The order asks for commitments as a faint area. It cannot be drawn honestly. Disclosure is
event-driven and ragged (Oracle: 13 gaps in 16 observations; Amazon's series stops at 2024Q2), and
**no constant-membership window exists at any tested setting** — min-quarters 4/5/6/8 × floor
0.95/0.80/0.50, all NONE. The varying-membership sum is visibly an artifact:

```
2026Q1  $255.5B  n=7  CORZ,EQIX,FRMI,MARA,META,ORCL,WULF
2026Q2  $ 45.2B  n=6  CORZ,EQIX,MARA,ORCL,SPCX,WULF     <- META's $237.7B stops being disclosed
```

An 82% "collapse" that never happened. So the panel line is refused, the refusal and its live
citation are printed where the line would have been, and the underlying series — which are perfectly
readable — are plotted **per issuer** instead, with gaps dashed so a non-disclosure quarter cannot
read as a flat one. What fails is adding them, not observing them.

## 7. A mistake: I overwrote `charts.py`

`charts.py` already existed — the committed CD-2 matplotlib module holding `BUCKET_COLORS`,
`chart_hayes_panel`, `render_all` and `build_pdf`, the last of which produces `cd2_thesis_layer.pdf`
and is called by `scan.py:199`. I wrote the new SVG module straight over it without reading it
first. `test_cd2.py::test_bucket_colors_cover_every_published_bucket` caught it.

Restored from HEAD; the new module is `svgcharts.py`. Both pipelines verified running in the same
process: 14 SVGs across six views, plus 4 PNGs and `cd2_thesis_layer.pdf`.

Two things follow. First, my statement in `brief.py` that the thesis PDF came from an uncommitted
script was **wrong** — it is committed, in the file I had overwritten; corrected. Second, the house
now has two chart stacks. **Whether the matplotlib pipeline should be retired is yours to rule**, and
it is coupled to the standing Flask-vs-stdlib question (CD-PH1 §9): if the answer there is stdlib,
retiring matplotlib drops a heavy dependency the SVG path does not need.

## 8. What shipped

**View 0 — `/`, the front page.** One composite: panel TTM capex as the hero line on its phase
shading, bucket sums beneath at reduced weight, the two jaws legs, the commitments refusal, and the
breadth strip along the bottom. Every point carries its member count in a native `<title>`.

**Views 1–5, phase-shaded native SVG.** Hayes panel: three legs at true magnitude, unrebased,
shaded on leg 1 only because only capex is classified. Phase board: the state grid — every
classified series × every quarter it classifies, blanks left uncoloured so absence does not read as
a fifth state — plus the panel YoY chart. Divergence: the credit-to-capex ratio over time, published
in the snapshot rather than divided in a renderer. Buckets: YoY and level per bucket. Commitments:
per-issuer, dashed across gaps.

**The phase page — `brief.phase_page`.** Vector composite plus aggregate and issuer tables, house
`abelard_common.render.pdf` helpers, refusals in their own section. Drawn from
`svgcharts.composite_model`, the same model the dashboard uses — `test_both_renderers_place_a_point_identically`
pins the shared transform so the brief cannot drift from the front page.

**A CD-PH1 defect fixed in passing.** The one-member REIT bucket published `INSUFFICIENT-MEMBERSHIP`
as its *state* but the TTM and YoY **cells kept printing +56.7%** — the same defect one column to
the right of where CD-PH1 fixed it. Withheld now, with the reason on hover.

No LLM calls. Read-only dashboard, `mode=ro` per request. No new runtime dependencies: the SVG path
is stdlib, the PDF path uses the ReportLab the monorepo already declares.

## 9. Open

1. **Flask vs stdlib** — still yours (CD-PH1 §9), now coupled to §7.
2. **Retire the matplotlib pipeline?** — §7. Two stacks is the current state, deliberately, pending
   your word.
3. **Band re-measurement** — unchanged, after two more filed quarters.
4. **REIT bucket** — still one member, so it has no constant panel and appears on no level chart.
   Two names remains one late filing from unusable.
5. **`COVERAGE_FLOOR` will need re-checking** when CRWV and META clear their filing lag: a 12-name
   window may then clear 95% and the chart would legitimately lengthen.
