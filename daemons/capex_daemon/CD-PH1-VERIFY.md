# CD-PH1-VERIFY — phase classifier, aggregate trend, dashboard

Produced 2026-08-18 on `cd-ph1`. **144 tests pass.** Nothing merged, nothing pushed.
Every figure computed by the built classifier against live SEC data.

Dead-bands ratified 2026-08-18 (Abelard), all eight as measured: per-issuer hyperscaler 6pp,
builder 27pp, REIT 2pp, host 5pp; bucket-sum hyperscaler 4pp, builder 27pp, REIT 6pp; total panel
5pp. Stamped in `config.DEAD_BANDS` with a re-measurement obligation after two more filed quarters.

---

## 1. Hand-verified state histories — three issuers

### MSFT — a plateau that resolved upward

| quarter | TTM capex | TTM YoY | Δ | state | qtrs | flags |
|---|---|---|---|---|---|---|
| 2025Q1 | 61,345,000,000 | +55.1% | −2.7pp | PLATEAU | 3 | |
| 2025Q2 | 64,551,000,000 | +45.1% | **−10.0pp** | PLATEAU | 4 | **SOFTENING** |
| 2025Q3 | 69,022,000,000 | +39.5% | −5.6pp | PLATEAU | 5 | |
| 2025Q4 | 83,094,000,000 | +49.6% | +10.1pp | PLATEAU | 6 | |
| 2026Q1 | 97,225,000,000 | +58.5% | +8.9pp | **ACCELERATING** | 1 | |
| 2026Q2 | 115,948,000,000 | +79.6% | +21.1pp | ACCELERATING | 2 | CONFIRMED |

Hand-checked: 2025Q2's −10.0pp exceeds the 6pp band, so SOFTENING raises — but a **single** move
does not make a state, so PLATEAU holds. 2025Q3's −5.6pp is *inside* the band, so the decline never
reaches N=2 and the softening resolves without a DECELERATING call. Then two consecutive out-of-band
rises (+10.1, +8.9) enter ACCELERATING, and a third (+21.1) raises CONFIRMED.

**This is the ladder earning its keep.** A naive sign-reader would have called MSFT decelerating in
2025Q2 and reversed itself twice.

### META — a confirmed acceleration rolling over

| quarter | TTM capex | TTM YoY | Δ | state | qtrs | flags |
|---|---|---|---|---|---|---|
| 2025Q3 | 62,733,000,000 | +106.2% | +24.2pp | ACCELERATING | 5 | CONFIRMED |
| 2025Q4 | 69,691,000,000 | +87.1% | **−19.1pp** | ACCELERATING | 6 | **SOFTENING** |
| 2026Q1 | 75,747,000,000 | +73.0% | −14.1pp | **DECELERATING** | 1 | |

The full sequence the order asked to be demonstrated, on the largest name that shows it live:
**CONFIRMED acceleration → SOFTENING → DECELERATING**. Note capex is still *rising* in dollars
($62.7B → $75.7B); what decelerates is the growth rate. That distinction is the entire point of
classifying TTM YoY rather than the level.

### CCOI — deceleration completing into contraction

| quarter | TTM capex | TTM YoY | Δ | state | qtrs |
|---|---|---|---|---|---|
| 2025Q2 | 219,636,000 | +38.5% | −5.6pp | DECELERATING | 2 |
| 2025Q3 | 196,642,000 | +2.2% | −36.3pp | DECELERATING | 3 |
| 2025Q4 | 187,569,000 | **−3.8%** | −6.0pp | **CONTRACTING** | 1 |
| 2026Q1 | 175,720,000 | −17.2% | −13.4pp | CONTRACTING | 2 |
| 2026Q2 | 158,055,000 | −28.0% | −10.8pp | CONTRACTING | 3 |

CONTRACTING is the **sole level-based state** and takes over the moment TTM YoY crosses zero —
2025Q4, at −3.8%. No N-window applies to it, because a series shrinking against its year-ago self is
contracting whatever the growth rate did to get there.

## 2. SOFTENING → DECELERATING → CONFIRMED, on real history

Best historical instance in the panel — **MSFT, 2019**:

| quarter | TTM YoY | Δ | direction | state | flags |
|---|---|---|---|---|---|
| 2019Q1 | +39.4% | −24.1pp | down | PLATEAU | **SOFTENING** |
| 2019Q2 | +19.7% | −19.7pp | down | **DECELERATING** | |
| 2019Q3 | +4.6% | −15.1pp | down | DECELERATING | **CONFIRMED** |

Three quarters, three stages, no hand-tuning: the flag on the first out-of-band decline, the state on
the second, confirmation on the third.

## 3. Matched-membership guard — CRWV graduation backtest

CoreWeave carries 9 quarters (2024Q1–2026Q1). Hyperscaler bucket-sum YoY computed with and without
it:

| quarter | without CRWV | with CRWV | result |
|---|---|---|---|
| 2025Q3 | +72.7% (n=5) | +72.7% (n=5) | **guard held** — CRWV excluded, no prior-year window |
| 2025Q4 | +72.1% (n=5) | +70.2% (n=6) | legitimately included, both windows complete |
| 2026Q1 | +78.0% (n=5) | +78.6% (n=6) | legitimately included |
| 2026Q2 | +83.2% (n=4) | +83.2% (n=4) | **guard held** — CRWV series ends 2026Q1 |

The demonstration is 2025Q3: CRWV **has data** for that quarter and is still excluded, because it
lacks the year-ago window the comparison needs. When it does enter, the aggregate moves by under
2pp — its real contribution, not its arrival. A naive sum would have booked CoreWeave's entire
$16.6B as bucket growth.

## 4. SNOW — the calibration ghost

| quarter | TTM YoY | Δ | state |
|---|---|---|---|
| 2025Q4 | +101.4% | +2.3pp | PLATEAU |
| 2026Q1 | +119.6% | +18.2pp | PLATEAU |
| 2026Q2 | **−10.2%** | **−129.8pp** | **CONTRACTING** |

SNOW is MIRROR: it classifies, it is visible on the phase board, and it **never alerts**
(`test_mirror_names_never_alert`). It is retained precisely because it is the one name in the panel
currently showing a real, unambiguous contraction — live proof the classifier fires on a genuine
decline rather than only on synthetic fixtures.

## 5. Dead-band suppression of real wiggles

Moves large enough to be visible, small enough to be noise — state held in every case:

| issuer | band | quarter | Δ | state held |
|---|---|---|---|---|
| EQIX | 2pp | 2026Q2 | +1.8pp | PLATEAU |
| EQIX | 2pp | 2024Q1 | +1.4pp | PLATEAU |
| DLR | 2pp | 2022Q4 | +1.1pp | PLATEAU |
| AMZN | 6pp | 2026Q1 | +3.4pp | PLATEAU |
| AMZN | 6pp | 2023Q1 | −5.8pp | CONTRACTING |

AMZN 2023Q1 is the instructive one: a −5.8pp move, close to the 6pp band and larger than several
moves that *did* change state elsewhere — suppressed here because hyperscaler noise is measured at
that scale. On the REIT band it would have been decisive. Per-bucket bands are why.

## 6. Two defects the build surfaced

**A one-member bucket sum was published as a bucket trend.** DLR's series ends 2026Q1 while EQIX
reaches 2026Q2, so matched membership correctly refused to mix them — and then published the REIT
bucket at **+56.7%**, which was simply EQIX wearing a bucket label. Matched membership guards the
*comparison*; nothing guarded the *aggregate*. Added `MIN_BUCKET_MEMBERS = 2`; the bucket now
publishes `INSUFFICIENT-MEMBERSHIP` and the dashboard renders a warning naming the count.

**Aggregate transitions never reached `phase_events`.** The scan recorded issuer transitions only,
while the alert path reads bucket and total-panel transitions too — so every aggregate transition
would have re-alerted on **every scan, forever**. Now all transitions from the snapshot are recorded.

Also added: a first run backfills the whole history (240 transitions) and **alerts none of it**,
reporting a count instead. Rediscovering history is not news.

## 7. Live end-to-end

```
[capex-scan] updated: 28 of 30 issuers had new filings: ... | first run: 240 transitions backfilled, none alerted
[capex-scan] no-op: 30 issuers checked, none with a new filing since watermark
```

Second run is a clean no-op — the classifier did not disturb scan idempotency. Artifacts written:
`hayes_panel.png`, `bucket_capex_ttm.png`, `divergence_ttm.png`, `forward_commitments.png`,
`cd2_thesis_layer.pdf` (261KB). Dashboard loads the snapshot **read-only** and renders all five
views (Hayes panel 17KB, phase board 18KB, divergence, buckets, commitments).

## 8. Current panel state

| series | state | TTM YoY | members |
|---|---|---|---|
| **TOTAL PANEL** | ACCELERATING | — | 4 matched |
| hyperscaler | ACCELERATING | +83.2% | 4 |
| builder | ACCELERATING | +269.5% | 7 |
| reit | **INSUFFICIENT-MEMBERSHIP** | — | 1 |

**Breadth disagrees with the dollar-weighted read, and that is the finding.** The builder bucket sum
is ACCELERATING at +269.5%, while its breadth is **2 accelerating, 3 decelerating, 2 contracting —
net −1**. The bucket is being carried by its largest members while most of its names are turning.
That divergence is invisible in any single number, which is why R4a requires them co-published.

Live contractions: **CLSK −37.8%**, **MARA −10.1%**, **CCOI −28.0%**, **IRM −4.4%**, **SNOW −10.2%**.

## 9. Stack deviation, flagged not resolved (E3)

The order specified *"Flask on :8788, SM dashboard pattern (same stack)"*. Those conflict. SM's
dashboard is stdlib `ThreadingHTTPServer`, and **Flask appears nowhere in the monorepo** — no
pyproject declares it, no module imports it. Following the literal word would add a dependency the
house has never used, so this follows *"same stack"* and the disk.

Rendering is kept pure — every view is a function from snapshot to HTML — so swapping in a Flask
shim touches only `serve()`. Reversible in minutes if ruled.

## 10. Open

1. **Flask vs stdlib** — §9, yours to rule.
2. **Band re-measurement** — bucket-sum and total-panel bands rest on n=13–14. Obligation stamped
   in config: re-run `tools/measure_deadband.py` after two more filed quarters land panel-wide.
3. **REIT bucket** — will stay `INSUFFICIENT-MEMBERSHIP` until DLR files its 2026Q2. A two-name
   bucket is one late filing away from unusable; worth deciding whether it should carry more names.
4. **Charts are matplotlib PNGs**, not the interactive per-bucket co-plot with phase shading the
   order sketches for view (1). The phase chart ships; interactive shading did not.
