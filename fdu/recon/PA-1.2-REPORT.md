# ORDER PA-1.2 — PHASE B BUILD + LABEL RECON + RECRUITING LANE

**Executed:** 2026-08-24/25 · Phase B built and run · L, R, S recon read-only
**LLM calls: 0. FDU model spend: $0.00.**

---

## 0. The headline

**Phase B is built and the archive is ingested: 196 of 197 snapshots, 2006-06 →
2026-08, 2.47 M rows parsed, 334,169 transition events.** The one miss is
explained and is not a defect (§2.3).

**Phase L returns a hard number that reshapes the unit.** Forward accrual via
Item 4 runs at **~1.6 third-party successions per year**. Five years of patience
buys about eight labels. Phase C cannot be validated on that, which makes human
adjudication (L2) the critical path rather than a nice-to-have. The worksheet is
built and waiting.

**Phase R's byproduct hypothesis is refuted, early, as instructed.** The archive
is **firm-level only** — zero individual-level columns in any era, zero
individual-level files among all 341. The recruiting lane inherits nothing from
the acquisition lane's backfill. One mechanism, one thesis, not two.

---

## 1. Premise findings

| # | Premise | Verdict |
|---|---|---|
| 1 | "roster-membership field survives all eras, meaning we may already own 20 years of rep-movement data as a byproduct of B1" | **REFUTED.** The roster is a **firm** roster. 0 individual columns across 26/88/215/262/448-column eras; 0 individual-level files among 341. Employee data is counts (`5A`, `5B(n)`), control persons is a count. §4 |
| 2 | Archive backfill costs 1.49 GB / ~1.6 h | **HOLDS.** Measured run: 196 files, ~1.5 GB, ingest completed in ~14 minutes wall on Basilic — faster than estimated because CSV eras dominate by row count and XLSX parsing was over-weighted. |
| 3 | ≥90% clean ingest per era | **EXCEEDED.** 196/197 files (99.5%); 2,466,303 rows parsed with **2,542 skipped (0.103%)**, all non-numeric CRD cells. |
| 4 | Coverage holes are 2023–25 and may be fillable | **PARTIALLY REFUTED.** See S5 — nothing on any SEC surface I can reach. |

---

## 2. Phase B — built and run

### 2.1 B1: the snapshot store

Twenty years of format drift is handled as **data, not code**: one resolver plus
a lineage table ([`lineage.py`](../fdu_daemon/lineage.py)) maps canonical fields
onto whatever header a snapshot actually has. Adding an era is a table entry.

Era is labelled from **what the header carries**, never the filename — filenames
drift independently, with **eight distinct name shapes** observed across 341
files (`ia#.zip`, `ia#-exempt.zip`, `#-exempt.zip`, `ia#-#.zip`, `ia#exempt.zip`
…). [I-9]

Ingested, by era:

| era | snapshots | what it carries |
|---|---:|---|
| `roster-only` | 26 | identity + address only (2006–~2008) |
| `part1a-partial` | 6 | + AUM, disciplinary |
| `part1a-full` | 154 | + headcount, client counts |
| `2026-wide` | 10 | + **Item 4 successions** |

### 2.2 B2: the diff engine

334,169 events across a closed vocabulary. **No event type names a commercial
outcome** — a test asserts none contains `acqui`/`sale`/`success`. [I-13]

| event type | count | gap-spanning |
|---|---:|---:|
| `aum_delta` | 189,751 | 14,328 |
| `headcount_delta` | 91,085 | 9,624 |
| `appearance` | 24,939 | 5,587 |
| `disappearance` | **18,508** | 3,804 |
| `rename` | **6,202** | 1,152 |
| `status_change` | 3,684 | 96 |
| **total** | **334,169** | 34,591 (10.4%) |

Two traps are guarded, both of which would have silently inflated every rate:

- **Gap-spanning pairs are flagged, not counted as one-month moves.** 49 of 243
  months are missing; a 13-month interval read as one step corrupts any rate
  built on it. 10.4% of events carry the flag and are excluded from §2.4.
- **A field absent in one era is never a change.** Comparing a 2012 file (no
  headcount column) against a 2016 file (has one) would otherwise manufacture a
  headcount event out of schema drift.

### 2.3 The one file that failed — and it is not a defect

`ia010119.zip` ingests to a 247-byte text file reading, verbatim:

> "The file for 'Registered Investment Advisers, January 2019' is unavailable due
> to the federal government shutdown December 22, 2018, through January 25, 2019."

The publisher could not produce that month. **196/197 with the single miss
externally explained** — enumerated per the acceptance criterion, not waved off.

### 2.4 B3: base rates of observed events — and one that must not be quoted flat

Disappearances per year, **clean intervals only**:

```
2007   144      2012  3,001  <-- see below      2017   787      2022   705
2008   390      2013    864                     2018   798      2023   478*
2009   787      2014    696                     2019   601*     2026   665*
2010   759      2015    666                     2020   775
2011   926      2016    785                     2021   877       * partial-year coverage
```

**The 2012 spike is a regulatory artifact, not market activity, and quoting it
inside a base rate would be a serious error.** The roster tells the story
directly:

```
2012-03  11,771     2012-06  12,476     2012-09  11,144
2012-04  12,622     2012-07  11,622     2012-12  10,870
2012-05  12,598     2012-08  11,308     2013-03  10,643
```

A surge of registrations in April, then a sustained bleed — 920 disappearances in
July 2012 alone. This is the Dodd-Frank switch: mid-sized advisers were pushed
from SEC to state registration while private-fund advisers were pulled in. Those
firms did not stop existing; they changed regulator.

**Excluding 2012 and partial years, the disappearance rate is remarkably stable
at ~700–900/year on a roster of 10,000–15,000 — roughly 5–7% annually.** That is
the honest base rate of *firms leaving the SEC roster*. It is **not** an
acquisition rate and this report does not offer it as one. [I-13]

---

## 3. Phase L — label recon

### L1 — forward accrual, quantified

Measured on the 2026-08 monthly snapshot: 16 successions carried, filing dates
spanning 2025-12-11 → 2026-07-31 (232 days), of which **15 self and 1
third-party**.

| | rate |
|---|---|
| all successions | ~25 / year |
| **third-party** | **~1.6 / year** |

| wait | labelled third-party events |
|---|---|
| 1 year | ~2 |
| 3 years | ~5 |
| 5 years | ~8 |

Caveat stated rather than buried: Item 4 is a momentary flag — a firm reports a
succession once and is instructed not to repeat it — so a standing snapshot shows
only filings recent enough to still be that firm's latest amendment. This is a
**lower bound**.

**L1 alone cannot support Phase C.** Eight labels against a ~1,000-event/year
candidate pool is not a validation set.

### L2 — the adjudication worksheet, built

`fdu-daemon worksheet --n 50` → **50 rows**, stratified across era × AUM band:

```
part1a-full    34      roster-only      8
part1a-partial  4      2026-wide        4
```

Design choices worth stating:

- **No machine-suggested label.** No score, no ranking, no "likely acquisition"
  column. A suggested label is the thing a human then agrees with, and L2's
  entire value is an independent judgement. A test checks the emitted columns.
- **The outcome vocabulary includes the boring answers** — `wound_down`,
  `moved_to_state`, `reorganized_same_owner`, `undetermined`. A worksheet
  offering only interesting outcomes manufactures them. Given §2.4,
  `moved_to_state` is likely to be *common*, and an adjudicator not offered it
  would be forced toward a wrong answer.
- **Gap-spanning disappearances excluded.** 49 of the 50 sampled rows have a
  one-month interval, so "when did it vanish" has a real answer.
- **Firms only.** No person column; a test asserts it. [I-11]

**The worksheet is NOT committed to the repo, and that is deliberate.** This
repository is **public**. A committed file listing 50 named firms under a
"which of these was acquired" heading reads as a target list regardless of intent,
and every value in it is already public SEC data that anyone can re-derive from
the generator. The generator is committed; the instrument lives at
`~/.openclaw/fdu/adjudication/L2_worksheet.csv` on Basilic. If you want it
elsewhere, say where.

**Pre-registered fork stands:** if <20% of the sample is plausibly third-party
acquisition, the deal-sourcing thesis narrows to the forward-accruing labelled
stream plus rename-class events. Given §2.4's Dodd-Frank finding, I would not be
surprised if the number lands well under 20%.

### L3 — external corroboration surfaces, census only

Trade press that covers RIA M&A: **RIABiz** (301), **Citywire** (200),
**InvestmentNews** (200), **WealthManagement.com** (200), **AdvisorHub** (301) —
all live, all reachable. **No ToS was read and no retrieval was performed beyond
`robots.txt`**, because I-2 requires the terms verdict *before* systematic
retrieval and no scraper is authorized. Recorded as: the surface exists and is
plausibly rich; feasibility is unestablished.

---

## 4. Phase R — recruiting lane

**R-1: the byproduct hypothesis is refuted.** Checked first, as instructed.

| | 2006 (26 col) | 2020 (262 col) | 2026 (448 col) |
|---|---|---|---|
| individual / rep identity | **0** | **0** | **0** |
| employee data | — | counts only (`5A`, `5B(n)`) | counts only |
| control persons | — | `10A` (count) | `10A` + count |
| owner names | — | — | — |

And **0 individual-level FILES** among all 341 archive ZIPs. The archive is
firm-level, entirely, for its whole twenty years.

**Consequence:** there is no historical rep-movement record to reconstruct. The
only individual-level surface FDU has found is the live `IA_INDVL` feed, which is
current-state only. **The recruiting lane starts its history at zero and accrues
forward**, exactly as the acquisition lane's live change log does — and unlike
the acquisition lane, it has no archive to backfill from.

**R-2: ToS.** Unchanged from PA-1.0 and R-PA1-1. BrokerCheck is CONDITIONAL with
a purpose limitation, admitted by ruling for targeted lookups, with bulk work
routed to the SEC feed. The SEC individual feed carries the explicit
public-information grant. **No new individual-level surface was probed**, because
R-3's output is an inventory and probing is not censusing.

**R-3: recruiting-signal inventory.** Firm-level only, no names, per I-11.

| signal | mechanism | surface | availability | validation path |
|---|---|---|---|---|
| rep departure/arrival | `EmpHs` from/to dates in the individual feed | SEC `IA_INDVL` | live only, no history | forward accrual; no retrospective test possible |
| firm headcount fall | `5A` drop without a disappearance | archive + live | **20 yrs** | measurable now, but it is a firm signal, not a person one |
| firm disappearance → rep dispersal | reps re-register elsewhere | individual feed | live only | needs both sides; only forward |
| registration lapse | `CrntRgstn`/`PrevRgstn` dates | individual feed | live only | forward |
| designations / exam dates | `Exm`, `Dsgntn` | individual feed | live only | career-stage proxy, unfalsifiable (PA-1.0 S5) |

**The honest read: the recruiting lane is data-poor in exactly the way the
acquisition lane was until the archive turned up, and there is no equivalent
archive.** Its only path is to start recording the individual feed now and wait.
That is a real cost to state before anyone plans around it.

**Nothing person-level was stored.** No individual data was fetched, parsed, or
written during this order. I-11's gate holds.

---

## 5. Standing tasks

**S5 — the 2023–25 coverage hole.** Not fillable from what I can see:

- The SEC index page lists **zero** registered snapshots for 2024 and one for 2025.
- The FOIA/historical page carries no `ia*` files for those years.
- Eight plausible filename variants probed directly under the current family: **all 404**.

Filename probing is weak evidence of absence and is labelled as such; the index
page is the authoritative negative. **A FOIA request is viable** — the cost is a
letter and patience, and it is a human action, not one I can take. Recorded as
the open route.

**S1** closed previously (113/23,804 = 0.47%, enumerated). **S2** — the live
change log remains **untouched**; no schema edits, and Phase B wrote to new
tables (`snapshot`, `transition_events`) rather than near it. **S4** closed by R1.

---

## 6. Invariant compliance

| | |
|---|---|
| I-1 read-only | GET only; archive fetches are downloads, no verb beyond GET exists |
| I-2 ToS-first | No systematic retrieval from any surface whose terms are unread. L3 stopped at `robots.txt` |
| I-3 / I-11 | Zero person-level artifacts created. Worksheet is firm-only and test-asserted |
| I-12 | Nothing placed in `fdu/deliverables/`. Nothing prepared for the firm. No executor-initiated sharing |
| I-13 | Event vocabulary carries no commercial term; §2.4 explicitly refuses to present the disappearance rate as an acquisition rate |
| I-6 | Archive run: ~1.5 GB across 197 fetches. **0 LLM calls, $0.00** |

---

## 7. Open questions

**Q1 — E-entry ratification.** Three rules, each bought with a real defect. This
order produced a fourth candidate: **an event vocabulary must not contain a word
the data cannot support** — the reason `transition_events` says `disappearance`
and never `acquisition`, and the reason §2.4's 5–7% is not offered as a deal rate.

**Q2 — the artifact.** Still not in `fdu/compliance/`. I have specced nothing
person-level and will not. Forward the email and the gate lifts to its written
scope.

**Q3 — L2 adjudication: yours or delegated?** 50 rows, ~5 minutes each with the
IAPD link prebuilt. Worth knowing before I build any Phase C scaffolding, because
label quality is the ceiling on everything downstream.

**Q4 — firm-deliverable boundary.** Confirmed as understood: nothing crosses to
the firm except through you, versioned, from `fdu/deliverables/`. Nothing has.

**Q5 — 3270.** Eighth raise.

**Q6 (new) — the public repo.** `github.com/wafflehaus69/Abelard` is world-readable.
That was fine for doctrine and code. It is a different question now that the unit
produces firm-level research artifacts, and it is why the L2 worksheet stayed off
disk in the repo. Worth a deliberate decision rather than an inherited default.

---

## STOP

Phase B built and run. Phase L and R recon complete, read-only. No Phase C work.
No person-level data fetched, parsed, or stored. Held on `fdu-pa1-recon`.
