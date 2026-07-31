# eFD access — CURRENT TRUTH 2026-07-30 (supersedes the 2026-07-20 WAF finding)

**The DataTables index/search endpoint is REACHABLE again via plain server-side
`requests`.** The 2026-07-20 WAF block (below, kept for history) is **no longer in
effect** on this path. No browser / Playwright is needed for the index.

## Live re-probe (2026-07-30, SM-C2 Phase 0, from Basilic via `smart_money.efd_session`)
- `bootstrap(probe=False)` (agreement handshake) → OK.
- `post_data(s, {report_types:[11]})` **PTR search → HTTP 200, recordsTotal 263**
  (sample: McCormick, David H. (Senator)), 0.3s.
- `post_data(s, {report_types:[7]})` **ANNUAL search → HTTP 200, recordsTotal 374**
  (sample: Hyde-Smith, Cindy (Senator)), 0.2s.
- The discriminator that passes is the **`X-CSRFToken` header + the agreement session**
  (what `efd_session.post_data` already sends). A request WITHOUT that header 403s — that
  is a caller bug, not the WAF.
- **Outcome (a) from the SM-C2 Phase 0 decision tree: PTR works.** The block is not
  report-type-specific (both 200) and not a hard wall.

## Pacing caveat (rate-shaped, not a wall)
Rapid repeated scripted probes were observed to tarpit/rate-limit the source IP during
this session (hangs, then transient failures) before settling. So: **pace requests**
(the existing `PACE_SECONDS`), use one agreement session per burst, and do not hammer the
data endpoint. Single / paced calls succeed cleanly.

## Production implication
- The Senate PTR **delta leg can un-degrade** — enumerate new PTRs via `post_data`
  (report_type 11) instead of the one-shot browser-harvested index. Closes the standing
  SM-4 blocker.
- The Senate **annual holdings** (report_type 7) index is likewise reachable for SM-C2
  Phase 1 (detail pages `/search/view/annual/{uuid}/`).
- **Playwright is NOT required** for the index anymore. Keep it filed as a fallback only
  if the WAF returns; do not build/deploy it now.

---

# HISTORY — 2026-07-20 WAF finding (NO LONGER CURRENT, kept so the change is legible)

The Senate eFD "503 maintenance" seen from 2026-07-17 was a WAF/bot-filter that 503'd any
request not issued by the site's own DataTables widget — even byte-identical in-browser
fetches with a matching csrftoken. At that time OPTION 2 (requests replication) was judged
infeasible and the index was harvested once via browser DataTables page-walk
(`data/raw/efd/senate_ptr_index_20260720.json`, 1562 uuids); detail-page GETs always
passed via plain requests. As of 2026-07-30 the index endpoint passes via plain requests
again (see above), so that browser-only constraint no longer applies.
