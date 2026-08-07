# eFD access — MEASURED, not asserted

**Status: NOT DEGRADED.** Determined by measurement, not by assumption.

| field | value |
|---|---|
| **Measured** | 2026-07-24 → 2026-08-06 (Phase W probe window) |
| **Evidence** | `efd_probe_log`, n=36 recorded, **n=22 weekday** (the verdict floor is 20) |
| **Verdict** | **NO WINDOW** — availability does not vary by hour |
| **Re-measure by** | **2026-11-06** (this finding EXPIRES; see *Expiry* below) |
| **Instrument** | `python -m smart_money.waf_probe --map` |

## What the probe measured

Eight hours-of-day sampled from Basilic against the eFD search endpoint via
`efd_session.post_data`:

| hour (local) | 06 | 09 | 12 | 15 | 18 | 20 | 21 | 23 |
|---|---|---|---|---|---|---|---|---|
| attempts | 5 | 4 | 4 | 4 | 6 | 3 | 5 | 5 |
| success | 100% | 100% | 100% | 100% | 100% | 100% | 100% | 100% |
| median ms | 594 | 604 | 757 | 1111 | 750 | 661 | 732 | 842 |

- **weekday 22/22 (100.0%)**, weekend 14/14 (100.0%).
- **10 probes excluded as `no_network`** — Basilic's own DNS/HTTPS outage on 2026-08-04.
  Those ten logged as failures at first and read *exactly* like the weekday throttle this
  map exists to detect. They are excluded from every rate and reported separately.
- Verdict is computed from **weekday rows only**. Day-of-week is a real confound: a
  load-shaped throttle would let weekend probes pass at every hour, so weekend green is
  not evidence of anything. The map refuses a verdict until weekday n ≥ 20.

## The finding

The 2026-07-20 "eFD blocks scripted access" claim is **not supported by measurement.**
There is no closed window because there is no window at all — the endpoint answered
every weekday probe at every sampled hour, sub-second.

Two things had been conflated under the word "WAF":

1. **A caller bug.** A request without the `X-CSRFToken` header + agreement session 403s.
   That is our omission, not a bot filter. `efd_session.post_data` sends both.
2. **Rate shaping.** Rapid unpaced bursts tarpit the source IP. That is a *pacing*
   constraint (`PACE_SECONDS`), not an availability wall, and it is hour-independent.

## Production consequences

- The Senate legs are **un-degraded**. `leg_congress` and `leg_congress_annual` run the
  Senate path as a first-class source, not a best-effort one.
- **No morning window move.** The Phase W order contemplated relocating the Senate legs
  to a window with better availability. No such window exists — the move would buy
  nothing and is explicitly NOT being made.
- **Playwright is not required and is not deployed.** Detail-page GETs never needed it;
  the index no longer does either.
- Pacing stays. `PACE_SECONDS`, one agreement session per burst.
- `senate_fd_ingest` keeps its `soft_block` classification and resume ledger. A block
  being absent today is not a promise about tomorrow — the retry machinery is how the
  return of one gets *detected* rather than silently absorbed.

## Expiry

**This finding is dated and it expires on 2026-11-06.** Source-side behaviour is not
ours to control, and an undated "it works" claim is exactly the kind of assertion this
probe existed to replace. On or before that date, re-run:

```
python -m smart_money.waf_probe --map
```

Until then the probe keeps logging on the nightly scan, so a regression shows up in the
map without anyone re-deciding anything. If the weekday rate falls materially below
100%, this finding is void and the degraded-Senate posture returns **on the new data**,
not on memory of the old.

---

# HISTORY

Kept so the change of belief is legible.

**2026-07-20 — WAF finding (SUPERSEDED, and its central claim was wrong).** The Senate
eFD "503 maintenance" seen from 2026-07-17 was read as a WAF/bot filter that 503'd any
request not issued by the site's own DataTables widget. OPTION 2 (requests replication)
was judged infeasible, and the index was harvested once via a browser DataTables
page-walk (`data/raw/efd/senate_ptr_index_20260720.json`, 1562 uuids). Detail-page GETs
always passed via plain requests.

**2026-07-30 — first contradiction (SM-C2 Phase 0, single probe).** `post_data` returned
HTTP 200 for PTR (recordsTotal 263) and annual (recordsTotal 374) search in ~0.2–0.3s.
The discriminator that passes is the `X-CSRFToken` header plus the agreement session. A
single probe, though — enough to doubt the block, not enough to retire it.

**2026-08-06 — retired on data (Phase W).** 36 probes over 13 days, 22 of them weekday,
100% success at all eight sampled hours. The 2026-07-20 conclusion is withdrawn. What
was actually happening on 2026-07-17 is not known and is not guessed at here; what is
known is that it is not happening now.
