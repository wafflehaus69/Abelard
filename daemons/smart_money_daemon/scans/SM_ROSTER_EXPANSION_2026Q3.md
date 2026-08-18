# Roster expansion, 2026-08-17 — scouting, vetting, and the rulings

Successor to `SM_P2_FILER_RESOLUTION.md`. Two scouting rounds took the tracked 13F
shelf from **19 filers to 28**. Every candidate below was resolved to a live CIK
and its actual Q2 2026 filing pulled and measured; nothing here rests on a
reputation or a secondary source.

Companion data (same directory):
- `SM_ROSTER_scout_portfolios.csv` — 1,086 position-level rows, 10 round-1
  candidates, Q2 2026 with QoQ deltas and exits.
- `SM_ROSTER_vet_holdings.csv` — 313 rows at/above 0.25% of book, 6 round-2
  candidates. 28,837 sub-floor rows excluded as noise.

---

## Added (9)

| filer | CIK | thesis | positions | book | why |
|---|---|---|---|---|---|
| TCI Fund Management (Hohn) | 0001647251 | activist | 11 | $52.8B | Highest signal-per-row available. 84% in top 5 |
| Himalaya Capital (Li Lu) | 0001709323 | value | 8 | $3.70B | Extreme concentration, 94.8% top-5 |
| Berkshire Hathaway (Buffett) | 0001067983 | value | 29 | $299B | $144B of novel exposure; energy sponsorship read |
| Baupost Group (Klarman) | 0001061768 | contrarian | 23 | $5.42B | Distressed. **Reports in THOUSANDS** |
| Akre Capital | 0001112520 | value | 20 | $5.11B | 0 new / 0 exits QoQ but 15 of 19 resized |
| Kopernik Global (Iben) | 0001599814 | hard_assets | 30 | $1.63B | **100% novel.** Gold, uranium, deep EM |
| Horizon Kinetics (Stahl) | 0001056823 | hard_assets | 351 | $8.82B | 95.7% novel. TPL alone is 48.6% of book. **Floor 0.25%** |
| First Eagle Investment | 0001325447 | value | 424 | $59.9B | 84.8% novel, institutional scale. **Floor 0.25%** |
| Elliott Investment Mgmt (Singer) | 0001791786 | activist | 29 | $22.7B | 56.7% top-5. TFPM 17.6%, PSX 14.4% |

`hard_assets` EXTENDS the thesis vocabulary, as `corporate_strategic` did. Filing
`value` on Kopernik and Horizon Kinetics would have filed them next to Berkshire
and lost the royalties/energy-land distinction that justified adding them.

**Position floors** are applied at the READ layer, never at ingest. All 775 rows
from the two floored filers are stored; 104 sit at or above 0.25% and carry
signal. Dropping the tail at ingest would be irreversible AND would corrupt the
book total that every `pct_of_book` is measured against.

---

## Rejected, with the measurement

| candidate | CIK | positions | top-5 | verdict |
|---|---|---|---|---|
| **Greenlight (Einhorn)** | 0001079114 | — | — | **No 13F-HR since 2023-12-31.** Last filing of any kind an N-PX, Aug 2024. DME Advisors and Greenlight Masters file zero. Nothing to ingest |
| **Point72 (Cohen)** | 0001603466 | 3,923 | 6.7% | Pod shop. Cohen's own decisions are not visible in the filing |
| **Bridgewater (Dalio)** | 0001350694 | 997 | 32.7% | Legible SIZE, illegible CONTENT — 25.5% of book is SPY+IVV, macro beta via ETFs. Dalio ceded control 2022, so a principal label would misattribute |
| **Citadel (Griffin)** | 0001423053 | **13,575** | 15.0% | Prior exclusion holds and was understated — more than double the whole 28-filer corpus |
| **Millennium (Englander)** | 0001273087 | 5,795 | 12.4% | As above |
| **D.E. Shaw** | 0001009207 | 4,833 | 10.6% | As above |
| Donald Smith & Co | 0000814375 | 66 | 24.7% | Diffuse; redundant with Kopernik. Top holding unresolvable (N-prefix foreign) |
| Fundsmith (Smith) | 0001569205 | 41 | 29.8% | UK manager; the 13F sees only its US-listed slice, which flickers. 13 "new" names in one quarter is implausible for the strategy |
| Pershing Square (Ackman) | 0001336528 | — | — | Already on the shelf since SM-P2 |
| Appaloosa (Tepper) | 0001656456 | — | — | Already on the shelf since SM-P2 |

**On the quants.** A 0.25% floor makes them *ingestible* but not *informative*:
52–58 lines survive it, of which only 8–12 are novel, and the survivors are ETFs
and duplicate option legs (SPY / QQQ / IVV / IWM). Across the six round-2
candidates, 121 of the 313 above-floor rows are options — hedges and delta
expressions, not conviction. Recorded in `manager_resolve.EXCLUDED` so the next
person to propose them finds the evidence rather than re-running the vetting.

---

## Resolution traps hit while scouting

Name search returns the WRONG entity for three of these. Verify against the
submissions record, and select on **most recent filing**, never on filing count:

| name | trap |
|---|---|
| Horizon Kinetics | `0001519418` stops at 2018-12-31. Live filer is `0001056823` |
| Baupost | `0001054420` stops at **2002**. Live filer is `0001061768` |
| Bridgewater | a date tie-break silently selected **Punch & Associates**; match on name |
| Point72 | five regional entities; the filer is `0001603466`, the rest stopped 2025-09-30 |

---

## Principals

24 of 28 carry the principal in the display name. **Four are deliberately blank —
Lone Pine, Baker Bros, Akre, First Eagle** — because the founder has stepped back
or no single decision-maker is recorded, and an invented attribution would read as
fact permanently. Awaiting a ruling. The three corporate filers have none by design.

---

## Known defect, not yet fixed

`thirteenf_ingest` reported Kopernik as `error` (JSON decode) while its 240 rows
across 8 periods landed correctly — the failure fires after the commits, so the
status line misreports a successful ingest. Fail-loud violation in the reporting
path, not the data path.
