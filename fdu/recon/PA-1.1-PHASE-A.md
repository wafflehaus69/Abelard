# ORDER PA-1.1 — PHASE A REPORT: HISTORICAL ARCHIVE CENSUS

**Executed:** 2026-08-24 · read-only · stop-and-report
**Verdict: PARTIAL** — and the partial is more interesting than a pass would have been.
**LLM calls: 0. FDU model spend: $0.00.**

---

## 0. The one-line answer

**The archive exists, CRD joins across all of it, and the field the order was
chasing is not in it.** Item 4 successions appear only in the newest format
(2026); across 2006–2023 the bulk files never carried them.

That does not sink the thesis. It relocates it. A single year of archive diffing
produces **776 firm disappearances and 235 renames** against the **1** genuine
third-party succession that Item 4 reports for the entire live corpus. The
longitudinal record does not need Item 4 — succession is not observable as a
declared field, but it is plainly observable as a **transition**.

---

## 1. Premise findings

| # | Order's premise | Verdict |
|---|---|---|
| 1 | "SEC publishes historical monthly ADV snapshots back to 2006" | **HOLDS.** 340 dated files, 2006-06 → 2026-08, three format families. |
| 2 | Archive lets the change log be "reconstructed retroactively" | **HOLDS for transitions, FAILS for declared successions.** Item 4 is absent 2006–2023. Reconstruction must key on roster membership and field movement, not on the succession item. |
| 3 | "~20 years of observed transitions" available for validation | **PARTIAL — 14 years of usable field data.** 2006–2011 files are a 26–88 column roster; the fields the thesis needs begin ~2012 (AUM, disciplinary) and ~2016 (headcount). |
| 4 | Phase C can validate signals against historical acquisitions | **GAP, and it is the important one.** See §6 — the archive supplies transitions but no ground-truth LABEL for which were genuine acquisitions. Phase C as specified has no dependent variable. |

---

## 2. A1 — The census

`[CURL]` Enumerated from the SEC data page, not from memory. **341 distinct ZIPs,
340 datable.** Three families, which are three eras:

| family | files | span | populations |
|---|---|---|---|
| `frequently-requested-foia-document-...-and-exempt/` | 185 | **2006–2017** | 119 registered · 66 exempt |
| `investment/data/...-and-exempt-reporting-advisers/` | 131 | **2017–2023** | 65 registered · 66 exempt |
| `investment/data/other/...-exempt-reporting-advisers/` | 19 | **2022–2026** | 10 registered · 9 exempt |
| `data_distribution/`, `edit/` (strays) | 6 | 2020 | — |

**Registered-population snapshots: 196 files across 194 distinct months.**

ToS posture: unchanged from PA-1.0 §3. These are `www.sec.gov` files, reachable
only with a declared contact address in the User-Agent (R-PA1-2 / Q6). The
governing clause remains the SEC public-information grant.

### Naming drift, recorded because it will break a naive parser

Per I-9 and I-10, selection parses the filename and never page position:

- `ia060506.zip` — MMDDYY
- `ia08032026_0.zip` — MMDDYYYY with a re-upload suffix
- `010118-exempt.zip` — **no `ia` prefix at all**
- `ia020119-2-exempt.zip` — a `-2` revision marker
- `ia020226-exemptzip.zip` — malformed name, `exemptzip`
- `ia051023exempt.zip` — **the one file of 341 that will not date-parse**; enumerated, not swept

---

## 3. A3 — Coverage gaps

Registered population, month by month over the 243-month span:

```
expected 243 · present 194 · MISSING 49 (20.2%)
missing by year: 2006:6  2007:7  2008:2  2010:1  2011:1  2017:1  2019:1
                 2023:7  2024:12  2025:11
```

**The gaps are not spread — they are two clusters at the ends.**

- **2008–2022 is essentially complete**: 12 snapshots/year every year except 2019 (11).
- **2023–2025 is a hole**: 2024 has **zero** registered snapshots on this page, 2025 has one.

That is the finding to sit with: the deep history is excellent and the *recent*
three years are the sparsest. Whether the 2024–25 files exist elsewhere under
another naming family is **not established** — a dated negative with a re-check
obligation [E15], not a conclusion.

---

## 4. A2 — Join feasibility and field lineage

`[CURL]` Sampled **8 snapshots**, not the 3 the order set as a floor. The extra
five were needed to bracket where each field starts, which is what decides the
usable window; disclosed rather than quietly taken.

Formats drift hard: pipe-delimited `.txt` → **XLSX** → CSV. Phase B needs
`openpyxl`; roughly half the archive by file count is Excel.

| snapshot | inner format | cols | rows | 5A emp | 5F AUM | 11 disc | **Item 4** | Sched A owners |
|---|---|---:|---:|:--:|:--:|:--:|:--:|:--:|
| 2006-06 | `.txt` pipe | 26 | 10,545 | — | — | — | — | — |
| 2012-01 | xlsx | 88 | — | — | ✅ | ✅ | — | — |
| 2016-01 | xlsx | 215 | — | ✅ | ✅ | ✅ | — | — |
| 2018-01 | xlsx | 262 | — | ✅ | ✅ | ✅ | — | — |
| 2020-08 | xlsx | 262 | 13,724 | ✅ | ✅ | ✅ | — | — |
| 2022-01 | csv | 262 | — | ✅ | ✅ | ✅ | — | — |
| 2023-01 | xlsx | 262 | — | ✅ | ✅ | ✅ | — | — |
| 2026-08 | csv | **448** | 17,018 | ✅ | ✅ | ✅ | **✅** | — |

**CRD is a stable join key across every era.** `Organization CRD #` /
`Organization CRD#` — a whitespace difference, nothing more. `SEC #` and
`Legal Name` also persist throughout.

**Two absences are structural, not parsing failures:**

- **Schedule A ownership: absent in every era, including 2026.** Consistent with
  PA-1.0. Ownership exists only in the per-firm document. The 49 GB leg survives.
- **Item 4 successions: present only in the 448-column format.** For 2006–2023
  the succession item was simply never published in bulk.

### The join measured, not assumed

2022-01 → 2023-01, both 262-column, one year apart:

```
2022-01  14,811 firms          2023-01  15,399 firms
persisted            14,035    94.8%
DISAPPEARED             776     5.2% / year
new                   1,364
persisted but RENAMED   235     1.67% of survivors
```

Renames from that single diff, unedited:

```
JANUS CAPITAL MANAGEMENT LLC     -> JANUS HENDERSON INVESTORS US LLC
GILLESPIE ROBINSON & GRIMM INC   -> AVITY INVESTMENT MANAGEMENT INC.
LEUMI INVESTMENT SERVICES INC.   -> VALLEY FINANCIAL MANAGEMENT, INC.
NORTHERN OAK WEALTH MANAGEMENT   -> 1834 INVESTMENT ADVISORS CO.
NICHOLSON CAPITAL MANAGEMENT     -> NICHOLSON MEYER CAPITAL MANAGEMENT
```

Those are visibly real transactions. **~1,000 candidate transition events in one
year, against 1 declared third-party succession in the whole live corpus.** That
ratio is the finding of this phase.

---

## 5. A4 — Cost, measured exactly

Not extrapolated. HEAD against **all 339 files**; 339 of 339 returned a size.

| population | files | total | mean | max |
|---|---:|---:|---:|---:|
| registered | 196 | **1.49 GB** | 7.6 MB | 20.0 MB |
| exempt (ERA) | 143 | 0.16 GB | 1.2 MB | 2.2 MB |
| **both** | 339 | **1.65 GB** | 4.9 MB | 20.0 MB |

Processing: ~196 snapshots × ~14k rows ≈ **2.7 M row-reads**. XLSX parsing
dominates; at the observed ~30 s/snapshot that is **~1.6 h single-threaded**.

Storage: 1.49 GB if raw is preserved (B1 says preserve it). Normalized rows are
the small part.

**For scale: the whole 20-year archive is 1.65 GB against the 49 GB per-firm
document backfill already completed.** Phase B is cheap.

---

## 6. The gap Phase C has, and it needs ruling before B is built

Phase C proposes: *among historically observed genuine acquisitions, what did the
filings show 1–3 years prior, vs matched controls?*

**The archive supplies the transitions. It does not supply the label.** Nothing in
2006–2023 says which of the 776 annual disappearances was an acquisition rather
than a retirement, a wind-down, a merger into an affiliate, or a move to state
registration. Item 4 — the field that would label them — is exactly the field the
archive lacks for that period.

Three ways out, none free, and the choice shapes B2's event vocabulary:

1. **Successor-side labelling.** A firm absorbed by another often shows up in the
   acquirer's *later* Item 4 — but only from 2026 forward, so the labelled set is
   tiny and recent.
2. **Structural inference.** Treat disappearance + a same-period AUM/headcount
   jump at another firm as a probable absorption. Cheap, and unvalidated —
   inventing a label is how a signal study fools itself.
3. **External ground truth.** Public M&A records for RIA transactions. Outside
   FDU's current surfaces and its own ToS question.

**My recommendation: build B1 and B2 (they are label-independent and the
transition record is worth having regardless), and hold B3's base-rate report to
"transitions per year" — attrition, renames, absorptions-by-inference — while
explicitly NOT claiming an acquisition rate until a labelling route is ruled.**

---

## 7. Standing tasks

**S1 — re-extraction residual. Complete.** 738 → **113 partial** of 23,804 (0.47%),
fully enumerated:

| n | reason | doc size |
|---:|---|---|
| 60 | Schedule A not located | 22–55 pages |
| 37 | Section 4 and Schedule A not located | mixed |
| 5 | Section 4 not located | — |
| ~11 | residual from the newest re-derivation | 259–1,280 pages |

Also `not_applicable` 6,618 (ERA subset form), `unavailable` 7 (publisher stub).
Nothing waved off.

**S1b — an inflated count I have to withdraw.** During this phase the succession
count read **27**. It was wrong. `section4_filed` was set by *absence* of the
"No Information Filed" string, so an **empty** Section 4 body — common, because
the heading also appears in a contents list — asserted a succession on no
evidence. Eleven firms were wrongly flagged. Fixed to require positive evidence;
uncertainty now lands undetermined. **Corrected count: 16 filed, 15 self, 1
third-party.**

**S2 — live change log untouched.** 12 rows. No schema edits made to it.

**S3 — ground truth on our best case. It does not survive contact.**
`[CURL]` IAPD live lookup:

- **MARSTONE LLC (CRD 164810) is still ACTIVE** — own registration, SEC# 801-78264,
  1 employee, $2.8 M AUM, last filing 2026-08-04.
- IQVESTMENT (CRD 157514) is ACTIVE, 5 employees, $22.3 M AUM, last filing
  2026-08-04 — the same day.

So the "acquisition" is a succession *declared* by one firm while the named
acquired firm remains independently registered and reporting. It may be a partial
book purchase, a transaction in flight, or a filing artifact. **It is not a
completed absorption**, and PA-1.0's headline should not have called it one
without this check.

**S3b — and the corroboration claim was wrong too.** PA-1.0 reported that two
independent sources agreed on the single third-party succession. They do not.

| source | third-party succession |
|---|---|
| per-firm PDFs (live) | IQVESTMENT ← MARSTONE |
| monthly CSV (2026-08-03) | **ROSE CAPITAL ADVISORS ← PREMIA GLOBAL ADVISORS** |

Different firms. Set overlap is 14 of 16 PDF-side and 2 monthly-only. The monthly
snapshot predates IQVESTMENT's 2026-08-04 filing by one day, which explains it
entirely. **Both sources reported "1" and I read agreement into a coincidence of
counts.** The corroboration claim is withdrawn; what survives is that both
sources independently show the third-party rate is ~1 per snapshot, which is the
weaker and correct statement.

**S4 — ERA backfill held.** Not fetched. Awaiting Q1.

---

## 8. Phase A pass/fail, against the pre-registered criterion

> PASS = CRD joins across eras AND ownership/succession-relevant fields are
> recoverable for ≥10 years of history.

| limb | result |
|---|---|
| CRD joins across eras | **PASS** — stable 2006→2026 |
| succession field ≥10 yrs | **FAIL** — Item 4 absent 2006–2023 |
| ownership field ≥10 yrs | **FAIL** — Schedule A absent in every era |
| thesis-adjacent fields ≥10 yrs | **PASS** — AUM/disciplinary from 2012, headcount from ~2016 (14 and 10 years) |

**Verdict: PARTIAL.** Usable window **2012–2026** for AUM and disciplinary,
**~2016–2026** for headcount, **2006–2026** for roster membership and names.

The reshaping the order anticipated: Phase B should reconstruct **transitions**,
not successions. The order's own §0 mechanism holds — retroactive reconstruction
converts waiting into backfilling — but the reconstructed object is roster
movement, not declared succession.

---

## 9. Open questions carried forward

**Q1 — ERA population, in or out?** *Recommend OUT, agreeing with Abelard.* New
evidence for it: ERAs file a subset Form ADV with **no Item 4 at all** (6,618 of
23,804 rows in our corpus are `not_applicable` for exactly this reason), so they
cannot carry the succession signal even in principle. They also cost 0.16 GB of
the 1.65 GB archive. Exclude with the reason recorded; do not fetch (S4 holds).

**Q2 — Phase B release.** Cost is **1.65 GB / ~1.6 h**, both measured. §6's
labelling question should be ruled at the same time, because it decides B2's
event vocabulary.

**Q3 — push discipline. Confirmed and clean, one word: authorized.** Mando gave
explicit in-session authorization ("go ahead and push it") before the first push;
every push since has been to the session branch `fdu-pa1-recon`, never to `main`.
`origin/main` does not contain this work and no PR has been opened. No E20/E25
breach.

**Q4 — I-9 and I-10 as org-wide doctrine.** Endorsed, and Phase A produced a
third candidate. I-9 (presentation order is never provenance) earned itself again
here: the archive's filenames carry six different naming conventions and page
order means nothing. I-10 (no substring population-matching) likewise. The third:
**absence of a negative marker is not positive evidence** — the `section4_filed`
bug in §7, which is the asymmetric-error rule violated in new code. All three are
cheap to state and each has now cost a real defect.

**Q5 — written firm-approval artifact. Still absent, and §6 makes it urgent.**
Phase C as specified needs acquisition labels, and every route to them moves the
unit closer to targeting intelligence. This should be ruled before B is built,
not after.

**Q6 — 3270 date.** Carried forward, unanswered.

---

## STOP

Phase A complete. **No Phase B work performed.** 8 archive snapshots sampled
(3 required, 5 extra to bracket field boundaries, disclosed), 339 HEAD requests
for exact sizing, ~1.7 GB fetched to scratch and not retained. Nothing written
outside `fdu/`. Held on `fdu-pa1-recon`.
