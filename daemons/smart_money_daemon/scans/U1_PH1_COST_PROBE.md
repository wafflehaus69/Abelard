# U1_PH1_COST_PROBE — SM-U1 universal Form 4 ingest gate

Run 2026-07-24 on Basilic. PH1 is a measure-and-STOP gate. Result: **STOP —
the 12-month universal backfill extrapolates to ~42h, 3.5x the ~12h threshold.**

## Measured (30-day walk, anchor 2026-07-23)
- Trading days with an index: 20 of 30 calendar days.
- Form 4s in window: 23,261. **Mean 1,163 / trading day** (max day 4,318).
  This is ~1.8x the 632-on-2026-07-20 prior — recent Form 4 volume is higher.
- Throughput: 0.515 s / filing (sample of 40, all parsed). Each filing = TWO
  EDGAR fetches (index.json to find the ownership XML, then the XML).
- Avg 1.55 non-derivative txns / filing.

## Extrapolation (12-month depth)
- Filings: ~293,000 (1,163 x 252 trading days).
- **Wall-clock: ~41.9 hours** at the measured 0.515 s/filing.
- DB growth: ~454K rows, ~114 MB.

## Why it is slow, and the binding constraint
- The cost is the 2-fetch-per-filing pattern under the 10 req/s EDGAR cap.
- **Hard floor:** 293K filings x 2 requests / 10 req/s = ~16.3h even at zero
  latency. Latency pushes the real number to ~42h.
- Cutting to ONE fetch/filing (pull the submission .txt directly and extract the
  inline XML, skipping the index.json round-trip) ~halves requests to 293K/10 =
  ~8.1h pacing-bound — plausibly UNDER 12h with latency, but not guaranteed.

## Options for Mando (gate decision)
1. **Reduce depth.** 3 months ~= 10h, 6 months ~= 21h. Discovery is recency-bound
   (per the order) — 3-6 months may be enough.
2. **Single-fetch optimization** (fetch .txt, skip index.json). Est ~8-16h for
   12mo. An engineering change to the walk; brings 12mo near the threshold.
3. **Accept the ~42h as a one-time, resume-safe run.** The walk is per-day
   watermarked and accession-idempotent — it can run across days/interruptions
   without redoing work. The 12h gate is about a single sitting, not total cost.
4. **Sample** (e.g. every Nth day, or a size/volume floor) — trades completeness
   for speed; changes the discovery denominator, must be labeled.

No proxy, no silent proceed. PH2-PH5 are NOT run. Machinery is committed
(form4_universal.py) but the backfill is not executed. Awaiting Mando's choice
of depth / optimization / acceptance.

## RE-PROBE (single-fetch optimization applied, 2026-07-24)
Mando chose Option 2 (optimize) then 3 (accept resumable), proceed if under ~20h.
Single-fetch via the submission .txt (inline ownership XML, no index.json
round-trip) is now live and validated (byte-identical parse, 1 request/filing).
- per-filing: 0.264s (was 0.515s — halved).
- **est 12mo wall-clock: 21.5h** (was 41.9h). DB growth unchanged (~454K rows).
- **Marginally OVER the ~20h proceed bar (by ~1.5h).** Depth stays 12mo (3mo
  would leave PH4 with too few gradeable events).

Options at this margin:
- **Proceed as resumable anyway** — it is per-day watermarked and
  accession-idempotent, so 21.5h across sittings is exactly the Option-3 shape.
- **Tighten pace** from 0.15s (6.7 req/s, conservative) toward the 10 req/s cap:
  0.12s -> ~18.7h, clearly under 20, still under the EDGAR cap. Cheap change,
  isolated to the universal path.
Awaiting Mando: proceed at 21.5h, or tighten pace to land under 20 first.
