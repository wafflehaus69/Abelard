# CD-BRIEF1-VERIFY — from state dump to daily read

Executed 2026-09-02/03 on `cd-brief1`. **346 tests pass.** Nothing merged,
nothing deployed — held for Mando's word. Six commits, one per unit except B1–B3
(see below). Every figure measured against live SEC data or the live Basilic
panel.

---

## B4 — threshold ratified, and AVGO cleared

Ratified: **(≥2.0× AND ≥$1B) OR ≥$20B**, frontier-gated.

Above **10×** a move is quarantined as `BASIS-SUSPECT`: it publishes, it lists
separately on page one, and it does **not** alert until a presentation check is
recorded. A 2372× move is not a growth rate until someone has confirmed the two
observations measure the same thing.

### AVGO resolved — and the answer is REAL, not a retag

Both 10-Qs carry the same note 10 "Commitments and Contingencies" table, the
same `Purchase Commitments` line item, the same fiscal-year rows and the same
units:

| Purchase Commitments | 2026 (rem) | 2027 | 2028 | 2029 |
|---|---|---|---|---|
| filed 2026-03-11 | $28M | $12M | $10M | $4M |
| filed 2026-06-09 | $22M | **$55,214M** | **$72,870M** | $4M |

**The concept, the table and the presentation are identical.** What changed is
the obligation. The near-term rows carry the whole move while 2029 and later are
unchanged — the shape of a real multi-year supply commitment, not a scope
change. Corroborated by `RevenueRemainingPerformanceObligation` in the same
filing: **$45.0B → $164.6B**.

So the quarantine did its job by forcing the check, and the check cleared it.
Suppressing this as a tagging artefact would have hidden the largest single
forward-demand event on the panel. **Quarantine is a queue, not a verdict.**

---

## B5 — the commitments total is refused

Measured on the live panel: **3 distinct concepts across 25 disclosing
issuers.**

| concept | issuers |
|---|---|
| `ContractualObligation` | CLSK, IRM, KEEL, MARA, META, WYFI |
| `PurchaseObligation` | CIFR, GOOGL, HUT, NVDA, SMCI |
| `UnrecordedUnconditionalPurchaseObligationBalanceSheetAmount` | AMD, AMZN, APLD, AVGO, CCOI, CORZ, EQIX, FRMI, IREN, MU, ORCL, SNOW, SPCX, WULF |

The front-page row prints `REFUSED-MIXED-BASIS` with the concepts and their
issuers in the detail. This refusal **outranks** the existing membership refusal
because it holds even when membership is perfectly constant — a
constant-membership sum across three bases is still a category error.

Per-issuer figures and the A4 deltas are untouched. Each is internally
consistent and comparable to its own history, which is exactly why the deltas
are computed per issuer and never summed. What fails is adding them. Held until
GAP2 P2 assigns basis classes; a total may then be published **within** one
class and still never across them.

---

## B6 — GAP2 P1 and P4, promoted

### P4 — CONTESTED, and a correction from running it

HUT reads DECELERATING with a latest move of **+228.1pp against a 27.0pp band**.
Both facts are true and the label alone carried one of them.

The first cut fired on *both* mismatches. Running it against the live panel
rather than reading it: **CIFR and DLR came back carrying SOFTENING and
CONTESTED together** — one fact stated twice. `FLAG_SOFTENING` has always meant
"the first out-of-band decline inside a state that is not contracting", which
*is* the ACCELERATING-against-a-fall case in the ladder's existing vocabulary.

CONTESTED now covers only the gap SOFTENING never did: an out-of-band **rise**
inside DECELERATING or CONTRACTING. The two are mirrors; a test walks every
(state, direction) pair and asserts they never co-occur. Live panel after the
fix:

| ticker | state | latest move | flag |
|---|---|---|---|
| HUT | DECELERATING | +228.1pp | **CONTESTED** |
| APLD | DECELERATING | +92.2pp | **CONTESTED** |
| CIFR | ACCELERATING | −46.6pp | SOFTENING |
| DLR | ACCELERATING | −2.1pp | SOFTENING |

Breadth now publishes **both censuses**: states (a run) beside directions (this
quarter). Publishing only the first made a quarter in which most names turned
look identical to a quiet one.

### P1 — the frontier credit pair

The constant-membership jaws are the honest **level**: five names held fixed
across sixty quarters, so a rise is spending rather than arrivals. That
correctness costs currency — the five are chosen for coverage, so the newest and
largest credit issuers are not among them.

So a second pair over **full current membership, trailing 8 quarters**, drawn
lighter and labelled. It is explicitly not a level to be compared with the long
one: membership still moves inside eight quarters, so every entry in the window
is published as a composition event beside it. Two legs, both labelled, neither
pretending to be the other.

---

## B7 — hygiene

The dcrev band note printed verbatim on all five supplier rows. Five identical
sentences is not five facts: the band is a property of the **series class**, so
it sits once under the table in both renderers. If the legs ever disagree on a
band that becomes a stated finding rather than a silently-picked one.

IREN's flag now carries its A5 verdict in the status itself —
`SUSPECT-IDENTITY-VERIFIED-COINCIDENCE`. A flag with no resolution is a question
left open on the page forever, and the status is what a reader sees. Evidence
travels with it: disjoint concepts, no shared fact, 0.07% apart, and the display
rounding of 1.000665 to "+100%" as the thing that made it look like an identity.

---

## B1–B3 — the Brief

**Committed as one unit rather than three, and naming the reason rather than
performing the split.** `BRIEF_SECTIONS` is *defined* as B1's section plus B2's
plus the phase board, and `snapshot.py` carries `since_last_scan` and
`thesis_line` in one module. Three commits would have left two that do not
build.

### B1 — "Since the last scan" is page one of everything

Frontier-gated transitions flagged **NEW**, filings ingested with issuer and
period, commitment moves carrying their B4 basis verdict, the BASIS-SUSPECT
quarantine listed separately, supplier-frontier rows, composition events.

**Every section reports its own emptiness.** A section that vanishes when empty
is indistinguishable from one that failed to run, and on a page whose whole
purpose is "what is new", silence has to be a statement. A snapshot with no
record says it *predates the record* rather than rendering as a quiet night —
absence has two causes here too.

### B2 — the thesis line, and two defects found by running it

One mechanical paragraph, same clause order every day, no adjectives. A test
asserts the absence of "strong", "sharp", "surge", "collapse" and their kin: the
value of a fixed sentence is that a changed clause is visible without reading,
and any word chosen for emphasis destroys that.

Live output:

> Panel capex TTM $606.30B is rising and reads ACCELERATING; credit issuance is
> rising; the forward-commitment total is refused, so it has no direction. The
> supplier cross-check reads 53.8% from 50.7% a quarter earlier, against no
> pre-registered band. Hyperscalers: 2 ACCELERATING, 2 PLATEAU, 1 DECELERATING.
> The panel commitment total is REFUSED-MIXED-BASIS.

**Defect 1 — an invented band, labelled "pre-registered".** The first cut
shipped `CROSSCHECK_BAND = (0.44, 0.48)` with a comment claiming it came from
CD-3b. CD-3b measured the `dcrev:supplier` **dead-band** at 9pp — a band on
quarter-to-quarter *moves* in the ladder, not a registered range for the ratio's
*level*. I invented two numbers and was about to print them as "pre-registered"
on the front page of a document meant to be read daily and trusted
structurally. Now `None`; the clause reports the level against its own prior
quarter and says plainly that no band is registered. The machinery stays, so
registering one later needs no new code — which is what "pre-registered" has to
mean to be worth anything.

**Defect 2 — the line contradicted B5 one screen away.** The commitments clause
read `panel["commitments"]`, the cross-basis total B5 had just refused, and
asserted "forward commitments are rising". It now says the total is refused and
has no direction. Verified by applying B5's clause to the live issuer set: 3
concepts, 25 issuers, refusal fires, clause holds.

### B3 — Brief and Reference

`report --brief` (3 sections) and `report --reference` (22 pages), both emitted
by the nightly.

The Brief was carrying the **Reference cover**: a subtitle claiming "all seven
views" plus three paragraphs on sparklines and landscape layout. That pushed the
since-last-scan page to page two and cost a third of a three-page document. The
explainer is Reference-only now, the Brief starts on its own front page, and the
footer carries the actual title instead of a hardcoded one.

---

## Open

1. **Not deployed.** Held for the merge word. Production still runs the CD-GAP2A
   code, so the live dashboard has no Brief, no CONTESTED marker and no
   mixed-basis refusal yet.
2. **A populated Since page runs to 4 pages, not 3.** The section count is 3 as
   ordered; the page count grows with content. Left as content-driven rather
   than truncated, but worth a word if the Brief must be exactly three pages.
3. **No cross-check band is registered.** B2 now says so honestly rather than
   inventing one. Registering a range is a ruling, and the clause will state
   position against it the moment one exists.
4. **GAP2 P2, P3, P5–P8** remain held. P2 is now load-bearing for B5: basis
   classes are what would let a commitments total be published at all.
5. **The fourth instance.** The freshness fallback, the RIOT ruling and
   `prose.py` were each built and never wired; `dcrev` transitions were computed
   and never routed into `snap["transitions"]`, which is the same shape again.
   The standing check proposed in CD-GAP2A is still unbuilt.
