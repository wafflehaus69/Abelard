# ORDER PA-1.0 — FINANCIAL DEALS UNIT, PHASE 0 RECON REPORT

**Executed:** 2026-08-21 · recon-only · no daemon package, no schema applied, no ledger, no contact path
**Status:** STOPPED at the phase gate. Nothing built. Awaiting Mando's disk review.
**Worktree:** `Abelard-fdu-pa1`, branch `fdu-pa1-recon`, new surface `fdu/` (E25 observed)
**Evidence tags:** `[CURL]` fetched live this session · `[DISK]` read from disk · `[INFERENCE]` reasoning

Doctrine line inherited from SC-R1: **live curl = canonical; anything not curl-verified is marked INFERENCE.**

**LLM calls: 0. FDU model spend: $0.00.** No LLM was needed and none was used.

---

## 0. Bottom line

**T1 PASSES.** A bulk, ToS-permissive, machine-readable source for Form ADV Part 1 data
exists and was retrieved and parsed in full this session: 23,794 firm records, 7.27 MB
compressed, refreshed the morning of the run.

**And the pass is narrower than the order assumes.** The three fields that carry the order's
primary succession thesis — Item 4 (Successions), Schedule A/B (ownership), and the DRP
disclosure pages — are **absent from the bulk product entirely**, measured across all 23,794
records. They exist only in per-firm PDFs at ~929 KB each.

The sharpest finding is not a ToS finding. It is that **ToS permits substantially more than
FDU's own invariants do.** The SEC's governing clause says its adviser data "may be
distributed or copied," and the individual feed is a complete per-person employment history.
Nothing outside FDU stops that being ingested. I-3 and the unresolved firm-facing wall are
the only things that do. That asymmetry should be ruled explicitly, because an engineer
reading "PERMISSIVE" in a verdict table could reasonably wire it.

---

## 1. Premise failures found in the order

Per the standing error hunt. These are findings, not obstacles.

| # | Order's premise | Verdict | Evidence |
|---|---|---|---|
| 1 | "SEC publishes bulk ADV data; verify current location" | **HOLDS, but the location is undiscoverable from the order's map** | The bulk product is not on `www.sec.gov`. It is `reports.adviserinfo.sec.gov/reports/CompilationReports/CompilationReports.manifest.json`, found only by pulling the IAPD SPA's JS bundle and grepping it. `www.sec.gov` returned HTTP 403 on every request including `/robots.txt`. §2.1 |
| 2 | Part 1 fields "AUM, employee counts, ownership, disciplinary" available in bulk | **PARTIAL — and the omissions are the load-bearing ones** | AUM yes, employee counts yes, disciplinary *flags* yes — but **ownership no** and **disciplinary detail no**. `Item4` (Successions): **0 occurrences in 23,794 records**. No `Schedule`, `Owner`, or `DRP` element exists anywhere in the corpus. §2.2 |
| 3 | T1b prior expectation: BrokerCheck "HOSTILE or CONDITIONAL" | **CONDITIONAL — and the nuance inverts the usual reading** | BrokerCheck's ToU *expressly permits* scraping — "including by use of data mining, scraping or harvesting tools (including robots)" — but "solely for investor protection, academic, compliance or regulatory purposes." FDU's purpose is none of the four. Blocked *for purpose*, not blocked outright. §3.2 |
| 4 | T1c: go to state regulators for state IA registration data | **LARGELY REDUNDANT** | The SEC's own `IA_FIRM_STATE_Feed` already carries state-registered IAs nationally, with per-state regulator codes and status dates. Per-state retrieval buys little for registration data. §4 |
| 5 | Manifest file sizes describe the download | **REFUTED — off by 10.7x** | The manifest advertises "78 MB" for the firm feed. That is the **uncompressed** size (82,181,857 bytes, read exactly from the gzip ISIZE trailer). The actual download is **7,270,232 bytes**. A bandwidth or rate plan built on the manifest figure would be wrong by an order of magnitude. §2.1 |
| 6 | "ADV amendments … filing-cadence anomalies" are observable | **REFUTED as stated** | The feed is a **daily snapshot, not an event stream**. Each firm carries one `Filing@Dt` — its most recent. Amendment *detection* requires diffing snapshots, and the publisher retains only an **8-day rolling window** (measured, §2.3). FDU can bootstrap at most 8 days of history and must snapshot from day one thereafter. |
| 7 | Robots posture predicts ToS posture | **REFUTED, twice, in opposite directions** | `brokercheck.finra.org/robots.txt` is `Disallow:` — fully permissive — behind a purpose-limiting ToU. `finra.org` robots is ordinary while its ToU forbids database creation outright. The SC-R1 Algora/Gitcoin lesson reproduces exactly. §3 |

---

## 2. T1a — SEC IAPD / Form ADV bulk. **PASS.**

### 2.1 The surface, as actually found

`[CURL]` `https://reports.adviserinfo.sec.gov/reports/CompilationReports/CompilationReports.manifest.json`

```json
{"files": [{"name": "IA_FIRM_SEC_Feed_08_21_2026.xml.gz",   "size": "78 MB",  "date": "08/21/2026"},
           {"name": "IA_FIRM_STATE_Feed_08_21_2026.xml.gz", "size": "69 MB",  "date": "08/21/2026"},
           {"name": "IA_INDVL_Feed_08_21_2026.xml.zip",     "size": "168 MB", "date": "08/21/2026"}]}
```

| property | measured value |
|---|---|
| Download size, firm/SEC feed | **7,270,232 bytes** (manifest says "78 MB" — that is uncompressed) |
| Uncompressed | 82,181,857 bytes, exact, from the gzip ISIZE trailer |
| `Last-Modified` | Fri, 21 Aug 2026 09:29:07 GMT |
| `Accept-Ranges` | `bytes` — **range requests work** (HTTP 206 confirmed) |
| Format | XML, `ISO-8859-1`, root `<IAPDFirmSECReport GenOn="2026-08-21">` |
| Records | **23,794 firms** — 16,900 Registered/APPROVED · 6,661 ERA/ACTIVE · 233 Registered/APPROVED-120 |

Range support matters operationally: schema can be re-verified with a 256 KB ranged GET
instead of a full pull, and the exact uncompressed size is readable from an 8-byte range on
the trailer. Both are used in this report.

### 2.2 What is in the bulk feed, and what is not

**Present** — `Item1, 2A, 2B, 3A–3C, 5A–5L, 6A, 6B, 7A, 7B, 8A–8I, 9A–9F, 10A, 11, 11A–11H`.

Measured coverage:

| field | meaning | coverage | distribution |
|---|---|---|---|
| `Item5A@TtlEmp` | total employees | 17,132 / 23,794 (72.0%) | median 8 · p90 63 · max 41,879 |
| `Item5F@Q5F2C` | total regulatory AUM | 16,783 / 23,794 (70.5%) | median $439,211,752 · p90 $7,084,560,665 |
| `Item11@Q11="Y"` | any disciplinary disclosure | 955 firms (4.0%) | — |
| `Filing@Dt` | most recent filing | 23,794 | 23,030 in 2026; 534 distinct dates |

The ~28% coverage gap on `5A`/`5F` is **structural, not missing data**: Exempt Reporting
Advisers (6,661 firms) do not complete the full item set. 23,794 − 6,661 = 17,133, matching
the 17,132 observed `TtlEmp` to within one record. Any risk score computed over these fields
must treat ERA absence as *not applicable*, never as *calm* — the scout `_ABSENCE_REASONS`
lesson applies directly.

**Absent — searched across the full 82 MB corpus, zero matches:**

- `Item4` — **Successions**. 0 occurrences.
- `Schedule A` / `Schedule B` — direct and indirect owners. No element.
- DRP pages — disciplinary *detail*. Only the Y/N flags survive.

`[CURL]` The same three sections **are** present in the per-firm PDF. Verified on one firm
(CRD 283882, a corporate entity, not a person),
`reports.adviserinfo.sec.gov/reports/ADV/283882/PDF/283882.pdf`, 929,091 bytes, 22 pages,
containing `Item 4`, `Succession`, `Schedule A`, `Schedule B`, `Direct Owners`,
`Indirect Owners`, and `Disclosure Reporting`. This path is **not** robots-disallowed.

**The cost of that route, stated plainly:** full ownership coverage means 23,794 requests at
~929 KB each ≈ **22 GB**. That is not a "systematic retrieval" any I-1 rate discipline makes
comfortable, and it should be treated as a Phase-1 design fork, not an implementation detail.

### 2.3 Refresh cadence and retention — measured, not assumed

`[CURL]` Probed the dated filename pattern backwards:

```
08_21 206 · 08_20 206 · 08_19 206 · 08_18 206 · 08_17 206 · 08_16 206 · 08_15 206 · 08_14 206
08_13 403 · 08_12 403 · 08_11 403 · 08_10 403 · 08_09 403 · 08_08 403 · 08_07 403
07_31 403 · 07_24 403 · 07_22 403 · 07_21 403
```

**Retention is an 8-day rolling window**, daily including weekends (08_15 Sat and 08_16 Sun
both present). The boundary is sharp between 08_14 and 08_13.

Cadence caveat per [E15]: *daily* is inferred from eight consecutive present dates observed
in one session. It is a dated finding with a re-check obligation, not a permanent property.

**Consequence:** there is no deep history to mine. Every longitudinal signal in T2 requires
FDU to archive its own snapshots starting on day one. Eight days is the entire bootstrap.

---

## 3. T1b — FINRA BrokerCheck. **CONDITIONAL(purpose-limited) → blocked for FDU's purpose.**

### 3.1 What is exposed

`[CURL]` `api.brokercheck.finra.org/search/individual` returns unauthenticated JSON. Per I-3
I record **fields, not values**. One search returned `total: 10,643` and carried, per
individual: source id, first/middle/last/other names, BD and IA scope, disclosure flag,
approved registration count, employment count, `ind_industry_days`, `ind_industry_cal_date`
(industry entry date — a career-stage proxy), and `ind_current_employments[]` with firm id,
firm name, branch city/state/zip, and SEC numbers.

The IAPD `IA_INDVL` feed is richer still. Field names only, extracted from a 128 KB ranged
sample which I then **deleted** rather than retain:

```
Elements:   Indvl · Info · CrntEmp(s) · EmpHs(s) · CrntRgstn(s) · PrevRgstn(s) ·
            DRP(s) · Exm(s) · Dsgntn(s) · OthrBus(s) · OthrNm(s) · BrnchOfLoc(s)
Attributes: firstNm midNm lastNm sufNm indvlPK orgNm orgPK fromDt toDt
            regAuth regCat regBeginDt regEndDt exmCd exmNm exmDt dsgntnNm
            hasBankrupt hasBond hasCivilJudc hasCriminal hasCustComp
            hasInvstgn hasJudgment hasRegAction hasTermination
            city state postlCd str1 str2 actvAGReg
```

`EmpHs` with `fromDt`/`toDt`/`orgNm` is **full employment history**. This directly answers
T4: historical advisor transition data is reconstructable from public filings, completely, in
bulk, without authentication.

### 3.2 The governing clause

FINRA's site-wide ToU (`https://www.finra.org/terms-of-use`, last modified November 9 2023,
*Permitted Uses* and *Restrictions* (d)–(e)) forbids the core activity outright:

> "develop or create a database of data using the FINRA Website … use any process to monitor
> or copy the FINRA Website in bulk, or use any data mining, scraping or harvesting tools
> (including robots), or any similar data-gathering or extraction tools"

But BrokerCheck carries its **own** ToU, presented as a click-through on the SEARCH button
and served in `brokercheck.finra.org/main.2b8c58f556a565c0.js`. Its default is narrow —

> "the data provided through BrokerCheck shall be used ONLY for your own personal or
> professional use … All other uses are strictly prohibited."

— and it then grants a **specific, purpose-limited carve-out** lifting restrictions (e), (f),
(g) and (k):

> "the BrokerCheck data may be copied and compiled, including by use of data mining, scraping
> or harvesting tools (including robots) or similar data gathering or extraction tools, and
> used solely for investor protection, academic, compliance or regulatory purposes"

**Verdict as executed: CONDITIONAL, and the carve-out does not reach deal sourcing or
recruitability assessment.** Retrieval was halted at FINRA surfaces once the terms were read.
Total FINRA-side data fetches this session: 3 probes plus the terms documents. No systematic
retrieval occurred.

> ### RULING R-PA1-1 — superseded, 2026-08-21, by Mando
>
> > "Override on the clause. We aren't selling this product. BrokerCheck is okay'd as a source."
>
> **BrokerCheck is admitted.** Full text and reasoning in `fdu/AGENTS.md`. In brief, because
> the distinction survives the override and governs Phase 1 design:
>
> The clause has **two independent axes**, and the ruling settles one of them cleanly.
> *Permitted Uses* allows "your own personal or professional use" — FDU's use is
> personal/professional research, sold and distributed to no one, so the commercial concern
> animating most of the document does not apply. That is the **use** axis, and it is settled.
>
> The **collection-method** axis is separate. Restriction (e) bars "data mining, scraping or
> harvesting tools (including robots)" *regardless of whether anything is sold*, and is lifted
> only by the four-purpose carve-out. So automation intensity, not commercial intent, is the
> live variable: targeted human-paced lookups sit inside Permitted Uses; bulk automated
> harvesting still loads (e).
>
> **This is mostly moot in practice, and that is the useful part.** The SEC `IA_INDVL` bulk
> feed carries substantially the same individual data — employment history with dates,
> registrations, exam dates, designations, branch locations, disclosure flags — under an
> explicitly permissive SEC clause. **Take bulk from the SEC feed; reserve BrokerCheck for
> targeted lookups.** That honours the ruling, yields better data, and never loads (e).
>
> **Still gated independently:** this is a ToS ruling only. Person-level ingestion remains
> blocked by I-3 and by the pending firm-facing wall artifact. See §10 and Q3.

### 3.3 The robots/ToU divergence, recorded as data

`brokercheck.finra.org/robots.txt` is `User-agent: * / Disallow:` — maximally permissive — and
FINRA additionally publishes **80 sitemaps** of individual report URLs at
`files.brokercheck.finra.org/sitemap.xml` (sampled `sitemap_1.xml`, 413,589 bytes), i.e. it
actively solicits search-engine indexing of the very records its ToU restricts. Robots is not
a compliance check. Doctrine A.1.1 holds.

---

## 4. T1c — State regulators (FL first, plus TX, CA, and NASAA)

| surface | verdict | evidence |
|---|---|---|
| **Florida OFR "REAL"** `real.flofr.gov` | **HOSTILE — bot detection** | 302 → `/datamart/languageChoice.do`; served page (21,198 B) contains **reCAPTCHA**. Recorded, skipped, reported. No bypass attempted. |
| **Texas SSB** `www.ssb.texas.gov` | PERMISSIVE (robots) | Drupal-default robots.txt. No IA registration data API located from static analysis. |
| **California DFPI** `dfpi.ca.gov` | **CONDITIONAL(rate)** | `Crawl-delay: 600` — ten minutes per request for `User-agent: *`. Systematic retrieval is impractical by design, presumably intentionally. |
| **NASAA** `www.nasaa.org` | PERMISSIVE (robots) | `User-agent: * / Disallow:` |

**The premise finding matters more than the census.** State-registered IA registration data is
already published centrally by the SEC in `IA_FIRM_STATE_Feed`, same schema as the SEC feed,
with per-state regulator status:

```xml
<StateRgstn><Rgltrs><Rgltr Cd="VA" St="APPROVED" Dt="2023-01-03"/></Rgltrs></StateRgstn>
```

Ownership and DRP are absent there too, identically. So per-state retrieval is redundant for
registration data and would only be worth it for something the SEC feed does not carry —
succession-plan filings and enforcement actions. **Neither was established as a retrievable
public surface this session** (see S8).

---

## 5. T1d — Form ADV Part 2 brochures. **NOT bulk-available.**

The compilation manifest carries exactly three files. None is Part 2. `[CURL]`

The primary per-firm brochure route is **robots-Disallowed** on the host that serves it —
`adviserinfo.sec.gov/robots.txt` disallows `/firm/brochure/` and
`/IAPD/Content/Common/crd_iapd_Brochure.aspx`. I did not fetch those paths.

So the surface where succession and continuity language actually lives is the one surface
whose primary route the publisher asks robots to stay out of. Recorded as `tos_class` data,
not engineered around. No NLP work was in scope and none was done.

---

## 6. T2 / T4 — Signal inventory

Full table in `signal_inventory.json` (10 signals, marked **necessarily incomplete**,
category-first). No signal is scored, weighted, or shipped.

| id | signal | bulk? | verdict |
|---|---|---|---|
| S1 | Item 4 Successions | no | REAL BUT NOT BULK-AVAILABLE (PDF only) |
| S2 | Schedule A/B ownership change | no | REAL BUT NOT BULK-AVAILABLE (PDF only) |
| S3 | Headcount contraction (5A/5B) | yes | AVAILABLE, UNVALIDATED |
| S4 | AUM / account trajectory (5F) | yes | AVAILABLE, UNVALIDATED |
| S5 | Principal age proxy | person-level | **INFERENCE ONLY — unfalsifiable** |
| S6 | Registration status transition | yes | AVAILABLE, UNVALIDATED |
| S7 | Filing-cadence anomaly | snapshot only | REQUIRES OWN SNAPSHOT ARCHIVE |
| S8 | State succession-plan filings | no | UNRESOLVED — premise not established |
| S9 | Marketplace listings | gated | BLOCKED (account creation) |
| S10 | Part 2 brochure language | no | NOT BULK; primary route robots-Disallowed |

Two entries deserve to be read rather than skimmed.

**S5 (age proxy) cannot be validated.** Date of birth is not public, so there is no ground
truth to measure a registration-year age proxy against. The order already says to encode it as
claim-not-fact; I would go further — an unfalsifiable feature has no validation path, and per
the T2 spec ("a falsifiable validation approach" per row) it therefore **fails the order's own
admission criterion for a signal.** It should be excluded, or ruled in explicitly, not carried
as an ordinary row.

**S3/S4 have a base-rate problem that will masquerade as signal.** AUM moves with markets;
headcount moves with ordinary attrition. Neither is signal until the population base rate is
measured and any threshold is pre-registered against the observed distribution [E8]. Phase 0
deliberately ships no threshold.

---

## 7. T3 — Intermediary and marketplace landscape. **Honest null: the space is saturated.**

The order asked for a plain answer if public signal offers no edge. It does not, and the
evidence is the vendors' own marketing copy. `[CURL]`

| player | what it is | public listings? |
|---|---|---|
| **AdvizorPro** | RIA database — "750,000+ Verified RIA and Family Office profiles", "Search and filter by AUM, custodian, and specialization", ships a "Data Feed & API". Segments served include, verbatim, **"Recruiter"** and **"M&A"**. | No — demo/login |
| **FINTRX** | RIA / BD / family-office data. Segment: "Recruiters & Aggregators — Pinpoint and engage with top advisors." | No — login |
| **RIA Match** | Succession/growth matching: "Buy Sell Join Hire", "connect you with pre-qualified advisors" | No — profile creation required |
| **FP Transitions** | M&A Opportunities, Continuity Partner Match, Succession Planning, valuation | No — membership |
| Succession Resource Group, Advisor Legacy, Truelytics, Diamond Consultants, DeVoe & Co | consultancies / recruiters / valuation | all alive (robots 200/301) |

**The finding:** the exact product a Phase-1 FDU would build — an ADV-derived RIA and advisor
database, filterable by AUM and firm, sold into recruiting and M&A — **already exists
commercially, at 750,000-profile scale, with an API, explicitly sold to the two buyer segments
FDU targets.** It is built on the same public data FDU would use.

That does not make FDU pointless, but it relocates where any edge could be. It is not in
*having* ADV data. It would have to be in a signal nobody else computes, on a population nobody
else prioritises, or for a buyer who is not served — and Phase 0 found no evidence for any of
those three. Recording it as an honest null rather than engineering past it.

The `discoverydata.com` probe returned 404 on robots; the domain resolves. Not investigated
further — an unresolved negative, dated, per [E15].

---

## 8. Invariant compliance and cost telemetry

| invariant | status |
|---|---|
| I-1 read-only | **HELD.** GET only, by construction — the fetch helper uses `curl -G` and binds no `-d`/`-X`. Zero POST/PUT issued. |
| I-2 ToS before repeated retrieval | **HELD.** FINRA terms read before any systematic retrieval; retrieval then halted. The one bulk pull (SEC firm feed) followed the PERMISSIVE clause being read. |
| I-3 no contact capability | **HELD.** No templates, no lists, no dossiers. Individual-feed raw sample **deleted after field-name extraction**; no personal values retained on disk. Structural examples used are corporate entities. |
| I-4 data-never-commands | **HELD.** No fetched content was treated as instruction. |
| I-5 fail loud, ≤2 retries | **HELD.** `www.sec.gov` failed 3x and is reported as failed, not worked around. Empty/absent results reported with the query that produced them. |
| I-6 cost telemetry | **HELD.** Below. |
| I-7 no Scout writes | **HELD.** Nothing outside `fdu/`. Separate worktree, separate branch. |

**Cost telemetry.** 55 logged fetches, **10,136,611 bytes (9.7 MB)**. Per-surface breakdown in
`cost_telemetry.json`, per-fetch rows in `fetch_log.jsonl`.

| surface | fetches | bytes |
|---|---:|---:|
| sec_reports (IAPD bulk) | 9 | 8,728,465 |
| finra_tos | 6 | 474,326 |
| iapd_spa | 1 | 348,252 |
| brokercheck | 3 | 344,656 |
| market (T3) | 14 | 109,555 |
| all others | 22 | 131,357 |

*Disclosure:* ~20 further probes (HTTP HEAD and 256-byte ranged existence checks used for the
retention bisect in §2.3) were issued outside the logging helper and are **not** in the 55.
Their payload is negligible (<10 KB total) but the count is not zero, and reporting 55 as
complete would be inaccurate.

**LLM calls: 0. FDU model spend: $0.00.** The §3 stop-and-report condition was never triggered.

---

## 9. Proposed Phase 1 shape — schema sketch only, no code

Offered because T1 passed. **Not a recommendation to proceed** — §7 and §10 bear on that.

Three tables, mirroring scout's envelope discipline while rejecting its opportunity shape (a
firm is not an opportunity; [E23]):

```
firm_snapshot     -- one row per (firm_crd, snapshot_date). APPEND-ONLY.
                  -- firm_crd, snapshot_date, sec_number, legal_nm, bus_nm,
                  -- rgstn_type, rgstn_status, rgstn_dt, filing_dt, form_vrsn,
                  -- ttl_emp, aum_total, aum_discr, acct_count, item11_any_y,
                  -- state_regulators_json, era_flag, raw_payload_hash

firm_delta        -- derived, never hand-written. one row per detected change.
                  -- firm_crd, from_snapshot, to_snapshot, field, old, new,
                  -- delta_class, absence_reason  <- distinguishes "changed"
                  --                                 from "field not applicable"

source_health     -- per-feed watermark, last_ok, consecutive_failures
```

Four properties are load-bearing:

1. **`firm_snapshot` is append-only.** The feed is a snapshot with an 8-day publisher
   retention; if FDU overwrites, its own history is the only history and it destroys it. This
   is scout's `opportunity_verdicts` lesson learned before the bug rather than after.
2. **`absence_reason` is a first-class column,** not a null. 28% of the population are ERAs for
   whom `ttl_emp` is *not applicable*, not *zero*.
3. **Firm-level only.** No `individual` table is proposed. That is not an omission — see §10.
4. **No score, no weight, no threshold.** Nothing until a distribution exists [E8].

Sizing, measured not guessed: 7.27 MB/day compressed for the SEC firm feed, 5.07 MB/day for the
state feed. A year of daily snapshots is ~4.5 GB compressed at source, far less stored as
parsed deltas.

---

## 10. The thing I most want ruled

**ToS is not the binding constraint on the recruiting side. FDU's own invariants are.**

The SEC's governing clause for adviser data is:

> "Information presented on www.AdviserInfo.sec.gov is considered public information and may be
> distributed or copied."

That is about as permissive as a governing clause gets, and it covers `IA_INDVL` — a complete,
unauthenticated, bulk database of named individuals with full employment history, registration
dates, exam dates, branch locations, and disclosure flags. **Nothing external prevents
ingesting it.** What prevents it is I-3, and the firm-facing wall amendment that §0 of the order
describes as pending a written artifact.

This needs ruling explicitly rather than left to inference, for a specific reason: a verdict
table reading **PERMISSIVE** next to the individual feed is exactly the kind of plausible,
well-evidenced, silently-wrong input the YesWeHack override in SC-R1 exists to warn about. The
correct row is not "permitted." It is "permitted by the publisher, forbidden by us, pending
Mando." I have written it that way in `tos_verdicts.json` (`applies_to_fdu: false`), but a JSON
field is a weak place for a compliance boundary to live.

Second, smaller item in the same class: **the SEC Fair Access policy expects a declared
User-Agent carrying a contact address.** I sent a descriptive UA with no contact address,
because none has been authorized for declaration and I was not going to send Mando's personal
address to a federal system or invent one. `www.sec.gov` returned 403 on all three attempts,
which may or may not be related. Sustained SEC access likely needs this resolved. See Q6.

---

## 11. Open questions for Mando

Q1–Q4 are carried forward from the order unresolved, as instructed. Q5–Q8 are new.

**Q1 — Separate ledger ratification.** FDU rows in own SQLite, never merged into Scout's corpus.
*Carried forward. Nothing in Phase 0 contradicts it; §9's schema assumes it.*

**Q2 — FDU scope order:** practice acquisition first, recruiting second, or parallel? *Carried
forward, with new evidence: the practice-acquisition side is firm-level and clears I-3 cleanly.
The recruiting side is person-level and does not. That asymmetry argues for sequencing rather
than parallel, but the call is yours.*

**Q3 — Written firm-approval artifact:** status, and whether retail-client outreach is in or out
of scope as written. *Carried forward. §10 is the sharpened version of this question.*

**Q4 — 3270 disclosure date.** *Carried forward. Standing raise, both units.*

**Q5 — Does the SEC's "may be distributed or copied" clause govern `reports.adviserinfo.sec.gov`,
or does FINRA's ToU reach it?** IAPD is an SEC property **operated by FINRA** — the SPA is served
with FINRA support widgets and the FINRA support centre is its help desk. FINRA's ToU is scoped
by its own words to "the FINRA.ORG site," so on a plain reading it does not extend to an
`sec.gov` host. I read it that way. It is the single load-bearing legal premise under the T1
PASS and it deserves a ruling rather than my reading.

**Q6 — What contact address, if any, may be declared in the User-Agent for SEC Fair Access?**
*Narrowed by ruling R-PA1-2 (Mando, 2026-08-21): the Phase 0 judgment not to send Mando's
personal address to a federal system is confirmed, and that option is closed.* Remaining: a
provisioned project address, or none — accepting that `www.sec.gov` stays unavailable, which
costs little, since the bulk product is on a different host that works.

**Q7 — Is the 22 GB per-firm PDF route in or out?** Ownership and Item 4 — the order's primary
succession thesis — exist only there. 23,794 requests at ~929 KB. In scope, out of scope, or
scoped to a filtered subpopulation (e.g. only firms whose bulk fields moved first)? The third is
the only version I would build, and it inverts the pipeline: bulk deltas become a *trigger* for
selective PDF retrieval, not a standalone signal.

**Q8 — Given §7, what is FDU's edge thesis?** AdvizorPro sells 750,000 ADV-derived profiles with
an API into "Recruiter" and "M&A" segments today. Phase 0 found no public signal those vendors
do not already have. This is the honest null the order asked for, and it is a strategy question
rather than an engineering one — but Phase 1 should not start before it has an answer, because
every schema decision downstream depends on which edge is being built.

---

## STOP

Phase 0 complete. **No daemon package exists. No ledger, no scraper, no contact path, no schema
applied.** Output is flat files under `fdu/recon/`. Held on branch `fdu-pa1-recon` in worktree
`Abelard-fdu-pa1`; **not pushed**. Awaiting Mando's disk review.
