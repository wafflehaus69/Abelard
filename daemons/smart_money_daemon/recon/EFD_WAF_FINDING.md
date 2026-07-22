# eFD WAF finding — 2026-07-20 (supersedes the "maintenance" assumption)

The Senate eFD "503 maintenance" seen since Fri 2026-07-17 ~11am ET is NOT an
outage. The backend is fully alive and serving the browser widget. eFD deployed
a WAF/bot-filter over the weekend that 503s any request not issued by the site's
own DataTables widget.

## Evidence (live, via browser automation against the real site)
- Site search widget (button then DataTables ajax): HTTP 200, full corpus
  recordsTotal 2390 PTRs back to 2015.
- DataTables API driven draw (dt.ajax.reload): HTTP 200 — the widget's own XHR
  path passes.
- In-page fetch, same page, same cookies, same csrftoken, full DataTables body,
  X-Requested-With plus Accept plus charset matched: HTTP 503, repeatable.
- In-page jQuery ajax minimal body: HTTP 503.
- CONTROL: after a real button search populated 25 rows in the DOM (widget XHR
  got 200), an immediate identical in-page fetch still 503'd.

## Conclusion
- OPTION 2 (requests/curl replication) is INFEASIBLE. If an in-browser fetch
  with byte-identical headers/cookies/body cannot pass, a server-side requests
  call never will. The discriminator lives inside the widget execution path
  (a JS-challenge cookie or per-XHR token from DataTables minified code), not in
  any header or body we can reproduce.
- OPTION 1 (browser automation driving the real widget) WORKS. Harvested the
  full index in-browser via the DataTables API page-walk: 2390 rows, 1562
  unique electronic PTR uuids plus paper filings, no errors.

## Production implication (DECISION FOR MANDO)
- 24/7 collection lives on headless Basilic. Option 1 needs a real browser
  engine there, i.e. Playwright / headless Chromium, a NEW production dependency,
  plus a risk the WAF also flags headless. Recommended path: build the Playwright
  harvester, validate headless passes on Orban FIRST, then deploy to Basilic on
  Mando's go. Do NOT install on the production box unilaterally.
- Detail pages (GET /search/view/ptr/{uuid}/): TESTED — they PASS via plain
  requests with an agreement session (HTTP 200, transaction table parses clean).
  Only the DataTables index endpoint is WAF-blocked.

## RESOLVED ARCHITECTURE (implemented 2026-07-20)
- INDEX (list of PTR uuids): browser-only, harvested via DataTables API page-walk.
  Done once this session -> data/raw/efd/senate_ptr_index_20260720.json (1562
  unique electronic PTR uuids, full corpus back to 2012). Refresh for new filings
  needs a browser again (future: Playwright on Basilic, still a Mando decision,
  but NOT on the critical path for the backfill).
- DETAILS (the 1562 transaction tables, the bulk): plain requests, no browser.
  efd_ingest.py --index-file mode + bootstrap(probe=False) light session.
- Backfill running on Basilic now via requests. The Playwright/Basilic infra
  decision is now only about ongoing index refresh, not the historical backfill.
