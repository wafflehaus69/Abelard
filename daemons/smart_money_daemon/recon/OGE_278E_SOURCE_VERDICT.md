# OGE Form 278e (executive-branch disclosure) — source verdict

Recon date 2026-07-31. Question: can we ingest Kevin Warsh's holdings, and executive-branch
officials generally, the way we ingest congressional FD holdings?

## Answer: technically YES, but a STATUTE restricts use. Decision is Mando's, not a build call.

## What exists

`https://extapps2.oge.gov/201/Presiden.nsf/PAS+Index?OpenView` — the OGE "Presidential
Nominee and Appointee Request System", a Lotus Domino app. HTTP 200 to a plain paced
`curl` with our UA; no WAF, no CSRF dance, no JS requirement for the index itself. It is an
A–Z index of Presidentially Appointed / Senate-confirmed officials.

**Kevin Warsh IS listed**, exactly as:

    Warsh, Kevin, Federal Reserve System Board of Governors, Governor & Chairman

Federal Reserve governors are covered generally (Barr, Bowman, Brainard, Clarida, ... all
present), so this is the right source for Fed principals, not just Warsh.

Two access tiers, per the site's own text:
* Officials at **Executive Schedule Levels 1 and 2** — reports "can be downloaded without
  completing an OGE Form 201". Warsh's listing sits in this index.
* **All other covered executives** — require submitting an **Online OGE Form 201** request,
  which identifies the requester and states a purpose. That is an attributable, signed
  request, not an anonymous fetch.

## The blocker is legal, not technical

The index page gates on an affirmative "I am aware of these prohibitions and wish to
proceed", quoting Title I of the Ethics in Government Act of 1978, 5 U.S.C. app. § 105(c).
It is unlawful to obtain or use a report:

* (A) for any unlawful purpose;
* **(B) for any commercial purpose, other than by news and communications media for
  dissemination to the general public;**
* (C) for determining or establishing the credit rating of any individual; or
* (D) for use, directly or indirectly, in the solicitation of money for any political,
  charitable, or other purpose.

> The Attorney General may bring a civil action against any person who obtains or uses a
> report for any purpose prohibited ... penalty in any amount not to exceed $11,000.

**(B) is the one that bites.** This daemon exists to inform trading decisions. Whether that
is a "commercial purpose" is a genuine legal question with a named civil penalty attached,
and it is the operator's call to make knowingly — not something to route around in code.

## Why this source is DIFFERENT from everything else we ingest

Every other source in this daemon is unrestricted public data:

| Source | Restriction on use |
|---|---|
| SEC EDGAR Form 4 / 13F | none — public disclosure, any use |
| House Clerk FD / Senate eFD (STOCK Act) | none — explicitly public, any use |
| **OGE Form 278e** | **5 U.S.C. app. § 105(c) — no commercial use, $11k civil penalty** |

So the congressional-holdings precedent does NOT carry over. Do not reason "we already
ingest financial disclosures, therefore this one is fine."

## Options for Mando

1. **DROP** — do not ingest OGE 278e. Costs us Warsh's full holdings; keeps the corpus
   uniformly unrestricted, which is also the simplest posture to defend.
2. **MANUAL, NON-COMMERCIAL** — Mando personally pulls a specific report for personal
   research, and it is read by a human rather than piped into the signal pipeline. No
   scraper, no scheduled job, no derived alerts.
3. **SEEK COUNSEL FIRST** — get an actual legal read on whether this use is "commercial"
   before any ingest. Only then decide between 1 and 2 or a broader build.

**Recommendation: 1 or 3.** Not 2-as-a-build — a scraper plus a scheduled job is exactly
the shape that makes a "commercial purpose" reading easy, for a single principal's holdings
that Form 4 already covers in part.

## What we get WITHOUT this source

Warsh is already a `persons` row (CIK 0001555065) from universal Form 4 ingest. Form 4
covers his positions in issuers where he is an officer/director/10% owner — **a subset of
his portfolio, not the whole thing**, and it carries no use restriction. That is the
unrestricted path, and it is what ORDER item (a) backfills.

## STOP

No fetcher written, no report downloaded, no OGE Form 201 submitted. Recon only.
