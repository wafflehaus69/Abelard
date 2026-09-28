# CD-GAP2-P2-VERIFY — what each forward-commitment line actually contains

Produced 2026-09-21 on `cd-gap2-p2`. **Research and proposal only — no code changed,
nothing merged.** Presentation-verified (E23) for all 26 disclosing issuers by nine
read-only agents, each reading the filing behind an issuer's latest tagged figure. The
three claims that would move the thesis were then re-checked by hand against the
filings (§Hand checks).

**Class assignment is Mando's ruling** (P2: "cross-issuer ranking suspended until basis
classes are assigned"). The classes below are PROPOSED.

## The finding in one line

**The XBRL concept predicts nothing about what a figure contains.** Each of the three
concepts in use spans two or three classes. `UnrecordedUnconditional…` alone carries
supply commitments, supply including capex, leases not yet commenced, lessor lease
*receipts*, prepaid rent received, and a recorded liability. B5's refusal to add them
was right, and understated the problem.

## What is wrong with the published leg today

1. **Six published "commitments" are not forward commitments at all.** CORZ ($3.1B) is
   lease payments the company *receives* as a lessor. WULF ($90M) is prepaid rent
   received. KEEL ($1.08B) is ~97.5% convertible-note principal. CCOI is a recorded
   liability. RIOT ($20K) is a 2016 royalty from a predecessor business. WYFI ($2M) is a
   SAFE equity-investment contingency. They flow through the A4 deltas and the Brief's
   since-page as "forward-commitment moves".
2. **Three are real commitments unrelated to the buildout.** GOOGL's $7.7B is content
   licensing. AMZN's tagged line is media content, energy and software. IRM's is mostly
   software maintenance and support. In two of these the buildout figure sits *beside*
   the tagged one: AMZN's $137.2B of leases not yet commenced, and IRM's $1.09B of
   data-center construction commitments, disclosed separately and untagged by this
   concept.
3. **The largest disclosers are stale because companyfacts drops dimensioned facts**,
   the same blindness the supplier leg hit. AMZN (published $32.4B as of **2024Q2**),
   NVDA (4 fiscal quarters stale), MU (11) and CLSK (7) all still disclose; they moved to
   dimensional tags. Fixing that needs the parser leg, as CD-3 built for segment revenue.

## Hand checks — the three claims that would move the thesis

| claim | verdict |
|---|---|
| CORZ $3.1B is lessor receipts | **Confirmed.** 10-Q 0001839341-26-000014: "Operating lease payments expected to be received exclude $3.1 billion in total future noncancellable operating lease payments expected to be received for operating leases that have not yet commenced". |
| GOOGL $7.7B is content licensing | **Confirmed at the tag itself.** 10-K 0001652044-26-000018, Note 10: "certain content licensing agreements with future fixed or minimum guaranteed commitments of $7.7 billion". |
| AMZN is understated at $32.4B | **Number confirmed, meaning corrected.** 10-Q 0001018724-26-000026 carries $130.065B under dimension `…DigitalMediaContentProcureEnergyAndLicenseSoftwareMember`; companyfacts stops at 2024-06-30. But that is media, energy and software, not buildout supply. AMZN's buildout-relevant fact is a second one: **$137.214B of leases not yet commenced**. "$130B of hidden AI commitments" would have been a false thesis signal — the hand check is what caught it. |

## Proposed classes — for ratification

The agents' scheme (purchase / purchase-incl-capex / total-contractual / other) lacks
the axis verification showed matters most: **is the figure about the buildout at all?**

| class | buildout? | meaning |
|---|---|---|
| SUPPLY | yes | unconditional purchase, supply or capacity obligations |
| SUPPLY+CAPEX | yes | as above, explicitly including PP&E or data-center build |
| LEASES-NOT-COMMENCED | yes* | signed leases not yet started (*SNOW's are offices) |
| CONTENT-ENERGY-SOFTWARE | no | real commitments, not infrastructure |
| MIXED | review | several of the above in one total (SPCX includes a spectrum acquisition) |
| NOT-A-COMMITMENT | — | receipts, prepaid rent, debt, liabilities, royalties, SAFEs: excluded everywhere |

## Per issuer

| issuer | concept | latest | as of | proposed | agent class · conf | contains (verified) |
|---|---|---|---|---|---|---|
| META | ContractualObligation | $349.31B | 2026-06-30 | **SUPPLY+CAPEX** | PURCHASE-INCL-CAPEX · high | supply, capex, cloud/services |
| AVGO | Unrecorded… | $126.82B | 2026-08-02 | **SUPPLY** | PURCHASE-OBLIGATIONS · high | supply |
| NVDA | PurchaseObligation | $45.77B | 2025-07-27 | **SUPPLY+CAPEX** | PURCHASE-INCL-CAPEX · medium | supply, capex, cloud/services |
| SMCI | PurchaseObligation | $34.20B | 2026-06-30 | **SUPPLY** | PURCHASE-OBLIGATIONS · high | supply |
| ORCL | Unrecorded… | $34.15B | 2026-08-31 | **SUPPLY+CAPEX** | PURCHASE-INCL-CAPEX · medium | supply, capex |
| AMZN | Unrecorded… | $32.41B | 2024-06-30 | **CONTENT-ENERGY-SOFTWARE** | PURCHASE-OBLIGATIONS · high | supply, cloud/services |
| AMD | Unrecorded… | $30.28B | 2026-06-27 | **SUPPLY** | PURCHASE-OBLIGATIONS · high | supply, cloud/services |
| SPCX | Unrecorded… | $27.95B | 2026-06-30 | **MIXED** | OTHER · high | supply, capex, cloud/services |
| GOOGL | PurchaseObligation | $7.70B | 2025-12-31 | **CONTENT-ENERGY-SOFTWARE** | OTHER · high | — |
| MU | Unrecorded… | $6.70B | 2023-08-31 | **SUPPLY+CAPEX** | PURCHASE-INCL-CAPEX · high | supply, capex, cloud/services |
| CORZ | Unrecorded… | $3.10B | 2026-06-30 | **NOT-A-COMMITMENT** | OTHER · high | — |
| KEEL | ContractualObligation | $1.08B | 2026-06-30 | **NOT-A-COMMITMENT** | OTHER · high | debt, op-leases, fin-leases |
| EQIX | Unrecorded… | $708M | 2026-06-30 | **LEASES-NOT-COMMENCED** | OTHER · high | op-leases, fin-leases |
| CLSK | ContractualObligation | $268M | 2024-09-30 | **SUPPLY+CAPEX** | PURCHASE-INCL-CAPEX · high | supply, capex |
| IRM | ContractualObligation | $234M | 2025-12-31 | **CONTENT-ENERGY-SOFTWARE** | PURCHASE-OBLIGATIONS · high | supply, cloud/services |
| IREN | Unrecorded… | $198M | 2025-09-30 | **LEASES-NOT-COMMENCED** | OTHER · high | fin-leases |
| FRMI | Unrecorded… | $143M | 2026-06-30 | **SUPPLY+CAPEX** | PURCHASE-INCL-CAPEX · medium | supply, capex |
| CIFR | PurchaseObligation | $99M | 2023-12-31 | **SUPPLY+CAPEX** | PURCHASE-INCL-CAPEX · high | supply, capex |
| WULF | Unrecorded… | $90M | 2026-06-30 | **NOT-A-COMMITMENT** | OTHER · high | — |
| SNOW | Unrecorded… | $78M | 2026-07-31 | **LEASES-NOT-COMMENCED** | OTHER · high | op-leases |
| MARA | ContractualObligation | $65M | 2026-06-30 | **SUPPLY+CAPEX** | PURCHASE-INCL-CAPEX · medium | supply, capex |
| HUT | PurchaseObligation | $40M | 2023-10-31 | **SUPPLY+CAPEX** | PURCHASE-INCL-CAPEX · medium | supply, capex |
| APLD | Unrecorded… | $17M | 2025-08-31 | **LEASES-NOT-COMMENCED** | OTHER · high | fin-leases |
| WYFI | ContractualObligation | $2M | 2024-06-30 | **NOT-A-COMMITMENT** | OTHER · high | — |
| CCOI | Unrecorded… | $218K | 2022-03-31 | **NOT-A-COMMITMENT** | OTHER · high | debt |
| RIOT | ContractualObligation | $20K | 2016-12-31 | **NOT-A-COMMITMENT** | OTHER · high | — |

## Staleness, and why (E15: dated findings)

- **NVDA** — Four fiscal quarters stale: the newest periodic filing is the 10-Q for 2026-07-26 (0001045810-26-000075, filed 2026-08-26). NVDA did not stop disclosing. It stopped tagging a non-dimensional total. From the Q3 FY26 10-Q (0001045810-25-000230) onward, it gave the pieces separately, with no table total, and tagged them only with OtherCommitmentsAxis dimensions, which companyfacts leaves out. Q3 FY26 pieces: PurchaseObligation $50.3B supply and $26B cloud, plus OtherCommitment $6.5B investments and $2.1B other. 10-K FY26: OtherCommitment $95.2B, $27B, $11.4B, $3.4B. Q2 FY27 10-Q: tagged OtherComm…
- **AMZN** — Stale only in companyfacts, not in the filings. AMZN presents this row every quarter and still tags it with this concept: 2024Q3 $51,524M, 2025FY $84,772M, 2026Q2 $130,065M (accn 0001018724-26-000026). From the 2024Q3 10-Q the fact carries a dimension (UnrecordedUnconditionalPurchaseObligationByCategoryOfItemPurchasedAxis = amzn:LongTermAgreementsToAcquireAndLicenseDigitalMediaContentProcureEnergyAndLicenseSoftwareMember). Companyfacts publishes only undimensioned facts, so the series appears to stop at 2024-06-30. The basis also changed. Footnote (2) adds 'acquire property and equipment' from…
- **GOOGL** — The latest tag is 2025Q4 (10-K filed 2026-02-05). That is two quarters behind Alphabet's newest periodic filing, the 2026Q2 10-Q (0001652044-26-000071, filed 2026-07-23), and this series will not advance. Alphabet stopped using PurchaseObligation after the 10-K. In the 2026 Q1 and Q2 10-Qs, Note 10 was rewritten to aggregate long-term supply agreements, energy service agreements and content licenses under us-gaap:LongTermPurchaseCommitmentAmount ($232.7B at 2026-03-31; $707.0B at 2026-06-30, 'the significant majority' long-term supply). Content licenses are no longer separately quantified. Tra…
- **MU** — The latest companyfacts fact is FY2023 (2023-08-31), about 11 quarters behind MU's newest periodic filing (10-Q for 2026-05-28). MU did not stop disclosing the figure. Starting with the FY2024 10-K it tags the same Commitments-note figure only with a dimension (UnrecordedUnconditionalPurchaseObligationByCategoryOfItemPurchasedAxis = mu:GoodsServicesAndCapitalAdditionsMember). Companyfacts keeps only undimensioned facts, so these are dropped. The dimensioned values are FY2024 $6.7B at 2024-08-29 (accn 0000723125-24-000027) and FY2025 $5.5B at 2025-08-28 (accn 0000723125-25-000028). Each filing…
- **CLSK** — The tagged figure is 7 quarters older than CLSK's newest periodic filing (10-Q for 2026-06-30). CLSK kept presenting the same off-balance-sheet 'Contractual future payments' table every quarter, but stopped tagging its total as ContractualObligation after the FY2024 10-K. In the FY2025 10-Qs (2024-12-31 to 2025-06-30), every cell including Total was tagged ContractualObligationFutureMinimumPaymentsDueRemainderOfFiscalYear ($82.8M, $159.6M, $57.689M). From the FY2025 10-K on, every cell, including the multi-year columns and the grand total, is tagged ContractualObligationDueInNextTwelveMonths (…
- **IREN** — Stale by 3 quarters (latest periodic is the FY2026 10-K, 2026-06-30). IREN did not stop tagging a series. It used this concept exactly once, in the 2025-09-30 10-Q, for a one-off GPU lease-financing disclosure, so it was never a recurring commitments series. IREN's recurring figure is the Commitments note total, tagged OtherCommitmentDueInNextTwelveMonths every quarter (2025Q3 $1,083.8M; 2025Q4 $8,748.1M; 2026Q1 $11,899.1M; 2026Q2 $13,611.0M within-12-months). At FY2026 there is also a +$199.0M after-12-months piece tagged iren:OtherCommitmentToBePaidAfterTwelveMonths, and the total $13,810.0M…
- **CIFR** — Stale by 10 quarters (the latest periodic filing is the 10-Q for 2026-06-30). CIFR used PurchaseObligation only in FY2021 filings and the FY2023 10-K, never quarterly. In the FY2024 10-K the only related fact is the Bitmain remaining $139M (subsequent-event, counterparty-dimensioned, 2025-01-31), tagged UnrecordedUnconditionalPurchaseObligationBalanceSheetAmount. It is dimensional, so it is not in companyfacts. From 2025Q3, CIFR's forward-commitment figure is 'commitments ... with suppliers, primarily related to construction', tagged us-gaap:OtherCommitment: $326.4M at 2025-09-30, $713.5M at 2…
- **HUT** — The latest fact is dated 2023-10-31 and was last filed in the 10-Q for 2024-06-30. The newest periodic filing is the 10-Q for 2026-06-30 (0001104659-26-090025, filed 2026-08-04), about 11 quarters later. From the 10-Q for 2024-09-30 (0001558370-24-015423) on, the 'Purchase agreements' paragraph is gone from the Commitments note, which now opens with legal matters, so the concept stopped being tagged. It did not move to another us-gaap concept. Later filings tag only custom hut: elements around the Bitmain miner purchase agreement (MinerPurchaseLiability, NumberOfMinersToBePurchased, DepositsFo…
- **APLD** — The latest fact is 2025-08-31, from the Q1 FY26 10-Q. The newest periodic filing is the FY2026 10-K for 2026-05-31 (0001144879-26-000048, filed 2026-07-29), three fiscal quarters later. Starting with the 10-Q for 2025-11-30 (0001144879-26-000006), the not-yet-commenced sentence is gone from the Leases note, so it is no longer tagged. The MD&A footnote (4) in that 10-Q still repeats $16.6M, untagged. Neither appears in the 2026-02-28 10-Q or the FY2026 10-K. It did not move to another concept. The only us-gaap commitment tag in those later filings is ContractualObligation, dimensioned PurchaseC…
- **WYFI** — The tagged end, 2024-06-30, is the SAFE Effective Date, not a balance-sheet date. The issuer tags the same 2024-06-30 instant in every periodic filing, from the Q3 2025 10-Q through the 10-Q for 2026-06-30 (filed 2026-08-12). So the fact is not stale from abandonment; it is dated to the event, and its 2024Q2 panel quarter comes from the context date. Per the 2026-06-30 10-Q, the New SAFE was surrendered and terminated on 2026-08-10, so the $2M should be treated as extinguished from Q3 2026 on.
- **CCOI** — The newest periodic filing is the 10-Q for 2026-06-30 (0001104659-26-091790, filed 2026-08-06), about 17 quarters after 2022Q1. The dimension-free fact stopped because the IPA liability ran off: $218K at 2022-03-31, and the FY2022 10-K (0001104659-23-025123) carries 785 only as the 2021 comparative. CCOI still discloses and tags real unconditional purchase obligations, but only annually in the 10-K and only as dimensional facts that companyfacts omits. The FY2025 10-K (0001104659-26-017968) states: 'Unconditional purchase obligations for equipment and services totaled $ 64.8 million at Decembe…
- **RIOT** — The newest periodic filing is the 10-Q for 2026-06-30 (0001104659-26-093448, filed 2026-08-10), about 38 quarters after FY2016. The concept was never used again after the filer left the animal-health licensing business and became Riot, a bitcoin miner. The $20K belongs to a business that no longer exists. Riot's current forward commitments use different concepts. The FY2025 10-K (0001104659-26-022322) tags us-gaap:PurchaseObligation only on CounterpartyNameAxis members: $29.4M remaining to MicroBT for miners ('which is expected to be paid through the first half of 2026') and $6.7M for the Cors…

## What P2 needs, once classes are ruled

1. **A basis column** on every commitments surface: class, what it contains, and the
   verifying accession and date, so a figure never travels without its meaning.
2. **Cross-issuer ranking stays suspended** until ratification; afterwards it ranks
   within one class only, never across.
3. **NOT-A-COMMITMENT is excluded** from the deltas, the since-page and any alert.
4. **Panel lines sum within one class only** (GAP1 P6 as amended).
5. **A dimensional commitments leg** through the parser, so AMZN, NVDA, MU and CLSK stop
   being frozen at their last undimensioned figure. A build, not a config change.

**Bearing on the open commitment-alert decision.** Wiring B4's alerts before classes
exist would alert on CORZ's lease receipts and WULF's prepaid rent. P2's classes should
land first.

---

# BUILD RECORD — ORDER CD-GAP2-P2-BUILD (C1–C6), 2026-09-22/23

Built on `cd-gap2-p2`, commit per unit, held for the merge word. Every figure below was
measured against the live panel (Basilic snapshot of 2026-09-21) or the filings
themselves, not asserted.

## C1 — the classes, ratified and gating (`f2d03ed`)

`commitment_basis.py` holds the ruled map: 26 tagged lines, each with the class read in
its filing, the accession, and what it contains. Only SUPPLY, SUPPLY+CAPEX and
LEASES-NOT-COMMENCED reach Leg 3, the since-page or an alert.

**What it removed, measured.** CORZ's −$2.70B at 0.53x was publishing as a
forward-commitment MOVE; it is lease payments CORZ expects to RECEIVE. Gone, with WULF's
prepaid rent, CCOI's recorded liability and RIOT's 2016 royalty. The three alerts that
fire today (META +$111.64B, SMCI 3.39x, ORCL +$20.84B) are all buildout classes, so the
gate changes no alert today — it changes which moves CAN alert.

**UNCLASSIFIED is the default and is not buildout**, so a new name cannot enter the
buildout read by existing. That gate has an edge — a genuine jump at a name nobody has
read cannot alert — so those moves publish as `commitment_basis_owed`: work orders on
the since-page, never signals. Dropping them silently would rebuild P2's own failure one
layer up, with the number replaced by a silence.

## C2 — the parser leg (`08d2be9`)

Seventeen verified rules over five issuers; 33 agent verifications, with an adversarial
second reader on every rule that could reach a buildout total.

| issuer | the API's figure | the filing's figure |
|---|---|---|
| **NVDA** | $45.77B (2025Q3) | **$279B** supply and capacity, +$160B in one quarter (2.34x) |
| **MSFT** | nothing (UNCOVERED-UNTAGGED) | **$329.1B** leases not yet commenced, +$132.5B in one quarter |
| **AMZN** | $32.41B (2024Q2) | **$137.21B** leases not yet commenced: 75.0 → 96.4 → 106.3 → 137.2 |
| **MU** | $6.70B (2023Q3) | $5.5B goods, services and capital additions |
| **IRM** | $234.0M | $1.09B datacenter construction (R1(a)) |

Three double-count traps are pinned by test, each found in a real filing: a total tagged
as a sibling member of its own components (NVDA's $366B); one value wrapped under two
contexts (MSFT's $329.1B); a maturity ladder on the same member summing to the same
total (AMZN's six buckets). **The MSFT case changed the design** — the adversarial
re-read found the two nested contexts bind the OPPOSITE way round from the first
reading, so no rule may resolve a nested fact by member name.

**CLSK is refused, not captured.** Its class split exists only under
`ContractualObligationDueInNextTwelveMonths`, which the filing uses for its whole
multi-year table. A twelve-month figure ranked beside other issuers' total stocks inside
one class is the error the classes exist to prevent. The refusal publishes with its
reason.

## C3 — one frontier (`a81dd89`)

E31's alert gate was the package's second definition of "the frontier": arrival order,
not coverage. It fails where it matters — the same Oracle quarter that caused E35 had
dragged the ALERT window to 2026Q2 and silenced every transition the rest of the panel
had there. The gate now reads the total's published quarter. The window moves
2026Q2 → 2026Q1 and admits 14 transitions, and **all 14 are already in `phase_events`,
so widening it announces nothing** — measured, not assumed.

## C4 — the sink (`601ec39`)

`commitment_events` (its own table), `alerts.enqueue_commitment_alerts` (issuer-scoped,
dedupe_key = the event key unmodified, class in the payload), and `scan.run` wiring all
three with its own first-run flag. Acceptance runs against the real `AlertQueue`: a
SUPPLY+CAPEX move arrives once and is a duplicate on re-derivation; the identical move
at CORZ never arrives; a GOOGL content-licensing move cannot alert and is still
recorded. **The first run is silent** (R2), which is why the six moves the parser leg
just found will backfill rather than fire.

## C5 — the ledger (`667a731`)

E36 ("a ratified behaviour is not built until its sink is connected"), E23's third and
largest citation, and E35's closing paragraph corrected — it had recorded E31's
arrival-based frontier as "the one remaining" and called it safe today, which was
already false.

## C6 — KEEL, and what the check found (`eff267f`)

KEEL's $1.079B is debt principal plus lease payments; the credit leg measures issuance,
a flow. Not the same quantity, not double-counted, and now published nowhere — correct.

The check found a live double count in the same issuer: KEEL tags one raise twice, gross
in Note 13 ($458,650K) and net of transaction costs on the cash-flow statement
($445,124K), and the resolver summed them as distinct instruments — **$903,774K for a
$458,650K raise**. Gross/net pairs of one raise are now refused rather than summed. The
first cut misfired on CIFR, which tags the same $167,113,000 under both concepts;
identical values are a double-TAG and collapse handles them better, so identical pairs
fall through to rule (a). Checked across all 40 issuers: KEEL is the only status change.

## Retro-note (E15), dated 2026-09-23

**Where the time went.** The research half took nine agents and a day; the build half
took six units and found three defects nobody had ordered fixed — E31's second frontier,
KEEL's gross/net sum, and a superseded figure ranked as current inside its own class.
All three surfaced from *running the thing and reading the output*, not from tests,
which is the same lesson E36 records from the other side.

**The correction that matters most is against my own earlier work.** P2's research read
AMZN's $130.065B line as content, energy and software, and R1(a) was ruled on that
reading. It was exact for the 2024Q2 figure. It is no longer exact: AMZN added "acquire
property and equipment" to that footnote after, and did not rename the XBRL member, so
the member name now misdescribes the line. Hand-checked at both ends
(0001018724-24-000130 against 0001018724-26-000026). Nothing published changes — both
candidate classes are non-buildout — but the label is wrong, so the ruling is re-opened
rather than quietly kept.

**Expiry.** Every class in `commitment_basis.TAGGED_BASIS` is dated to the filing it was
read in. A class whose issuer re-words its footnote is stale the moment it does so, and
nothing in this build detects that. Re-verification obligation: for any issuer whose
class or capture was read before 2026-09-23, re-read the line when its next annual
report lands. AMZN is the proof that this is not theoretical.

## Open for Mando

1. **AMZN's $130.065B line** — re-classify MIXED-UNSEPARABLE on the live footnote, or
   keep CONTENT-ENERGY-SOFTWARE? No published number turns on it either way.
2. **NVDA's $36B AI-cloud agreements** — classed SUPPLY, and they are CONTINGENT: NVIDIA
   buys the capacity only if the AI cloud fails to sell it. A ruling that contingent
   obligations are not SUPPLY moves $36B out of the buildout read.
3. **NVDA's guarantees** — $105B to SB Energy and $3.5B of land/power/shell guarantees
   are classed NOT-A-COMMITMENT (maximum exposure, not a purchase). They are large
   enough that "excluded, and why" may deserve its own line rather than a table row.
4. **AMZN's RPO disclosures** — $38B of existing AWS–OpenAI commitments and two $100B
   expansions (OpenAI, Anthropic) are revenue obligations owed TO Amazon. Not
   commitments BY Amazon, so excluded here — but they are the demand side of the same
   buildout and nothing in the daemon reads them.
5. **CIFR's collapse** — keeps `ProceedsFromConvertibleDebt` and drops the net-tagged
   series carrying the newer figures ($2.77B for H1 2026 against $1.44B). Whether
   collapse should keep the series with live data rather than the alphabetically-first
   is a separate ruling. Flagged, not touched.
